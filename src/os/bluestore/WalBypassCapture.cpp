// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "WalBypassCapture.hpp"

#include "BlueStore.h"
#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/debug.h"
#include "common/perf_counters.h"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

namespace fs = std::filesystem;

#define dout_context nullptr
#define dout_subsys ceph_subsys_bluestore

namespace {

std::string make_wal_bypass_filename(uint64_t seq)
{
  char name[64] = {0};
  int r = snprintf(name, sizeof(name), "ceph_wal_%010llu.log",
                   static_cast<unsigned long long>(seq));
  ceph_assert(r > 0);
  return std::string(name);
}

bool parse_wal_bypass_seq(const std::string& name, uint64_t& seq)
{
  constexpr std::string_view prefix = "ceph_wal_";
  constexpr std::string_view suffix = ".log";
  if (name.size() <= prefix.size() + suffix.size()) {
    return false;
  }
  if (name.compare(0, prefix.size(), prefix) != 0) {
    return false;
  }
  if (name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
    return false;
  }

  const size_t begin = prefix.size();
  const size_t end = name.size() - suffix.size();
  for (size_t i = begin; i < end; ++i) {
    if (name[i] < '0' || name[i] > '9') {
      return false;
    }
  }

  errno = 0;
  char* parse_end = nullptr;
  unsigned long long value = strtoull(name.c_str() + begin, &parse_end, 10);
  if (errno != 0 || parse_end == nullptr ||
      static_cast<size_t>(parse_end - name.c_str()) != end) {
    return false;
  }

  seq = static_cast<uint64_t>(value);
  return true;
}

class WalBypassSeqState {
private:
  CephContext* m_cct;
  fs::path m_dir;
  fs::path m_state_path;
  uint64_t m_next_seq = 1;

public:
  explicit WalBypassSeqState(CephContext* cct, fs::path dir)
    : m_cct(cct),
      m_dir(std::move(dir)),
      m_state_path(m_dir / "ceph_wal_seq.state") {
  }

  bool initialize() {
    uint64_t state_next = 0;
    const bool has_state = read_state_file(&state_next);
    uint64_t max_existing_seq = scan_max_bypass_seq();
    uint64_t next_seq = max_existing_seq + 1;
    if (has_state && state_next > next_seq) {
      next_seq = state_next;
    }
    if (next_seq == 0) {
      next_seq = 1;
    }
    m_next_seq = next_seq;
    if (!persist_state_file(m_next_seq)) {
      lderr(m_cct) << __func__ << " failed to persist initial next_seq="
                   << m_next_seq << " state_path=" << m_state_path << dendl;
      return false;
    }
    return true;
  }

  uint64_t next_seq() const {
    return m_next_seq;
  }

  void on_file_exists_conflict() {
    ++m_next_seq;
  }

  bool commit_opened_seq(uint64_t opened_seq) {
    if (opened_seq != m_next_seq) {
      lderr(m_cct) << __func__ << " inconsistent opened_seq=" << opened_seq
                   << " expected_next_seq=" << m_next_seq << dendl;
      return false;
    }
    if (opened_seq == std::numeric_limits<uint64_t>::max()) {
      lderr(m_cct) << __func__ << " sequence overflow at " << opened_seq
                   << dendl;
      return false;
    }

    m_next_seq = opened_seq + 1;
    if (!persist_state_file(m_next_seq)) {
      lderr(m_cct) << __func__ << " failed to persist next_seq="
                   << m_next_seq << " after opened_seq=" << opened_seq
                   << dendl;
      return false;
    }
    return true;
  }

  bool ensure_consistent_with_disk() {
    const uint64_t max_existing_seq = scan_max_bypass_seq();
    const uint64_t min_next_seq = max_existing_seq + 1;
    if (m_next_seq < min_next_seq) {
      ldout(m_cct, 5) << __func__ << " correcting next_seq from " << m_next_seq
                      << " to " << min_next_seq << dendl;
      m_next_seq = min_next_seq;
      if (!persist_state_file(m_next_seq)) {
        lderr(m_cct) << __func__ << " failed to persist corrected next_seq="
                     << m_next_seq << dendl;
        return false;
      }
    }
    return true;
  }

private:
  bool persist_state_file(uint64_t value) const {
    std::error_code ec;
    if (!fs::exists(m_dir, ec) || ec) {
      lderr(m_cct) << __func__ << " state dir missing/unavailable: " << m_dir
                   << " ec=" << ec.message() << dendl;
      return false;
    }

    const auto unique_suffix = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
    fs::path tmp = m_state_path;
    tmp += ".tmp." + unique_suffix;

    {
      std::ofstream out(tmp, std::ios::out | std::ios::binary | std::ios::trunc);
      if (!out.is_open()) {
        lderr(m_cct) << __func__ << " failed to open tmp state file " << tmp
                     << dendl;
        return false;
      }
      out << value << "\n";
      out.flush();
      if (!out.good()) {
        out.close();
        fs::remove(tmp, ec);
        lderr(m_cct) << __func__ << " failed to flush tmp state file " << tmp
                     << dendl;
        return false;
      }
    }

    fs::rename(tmp, m_state_path, ec);
    if (ec) {
      fs::remove(tmp, ec);
      lderr(m_cct) << __func__ << " failed to rename " << tmp << " to "
                   << m_state_path << " ec=" << ec.message() << dendl;
      return false;
    }
    return true;
  }

  bool read_state_file(uint64_t* value) const {
    std::ifstream in(m_state_path, std::ios::in | std::ios::binary);
    if (!in.is_open()) {
      ldout(m_cct, 10) << __func__ << " state file not found: "
                       << m_state_path << dendl;
      return false;
    }

    uint64_t parsed = 0;
    in >> parsed;
    if (!in.good() && !in.eof()) {
      lderr(m_cct) << __func__ << " failed reading state file "
                   << m_state_path << dendl;
      return false;
    }
    if (in.fail()) {
      lderr(m_cct) << __func__ << " invalid state content in "
                   << m_state_path << dendl;
      return false;
    }

    *value = parsed;
    return true;
  }

  uint64_t scan_max_bypass_seq() const {
    std::error_code ec;
    if (!fs::exists(m_dir, ec) || ec) {
      return 0;
    }

    uint64_t max_seq = 0;
    for (const auto& entry : fs::directory_iterator(m_dir, ec)) {
      if (ec || !entry.is_regular_file()) {
        continue;
      }
      uint64_t seq = 0;
      if (parse_wal_bypass_seq(entry.path().filename().string(), seq) &&
          seq > max_seq) {
        max_seq = seq;
      }
    }
    return max_seq;
  }
};

class WalBypassCaptureStream {
private:
  CephContext* m_cct;
  fs::path m_dir;
  fs::path m_path;
  uint64_t m_seq;
  bool m_path_exists = false;
  std::ofstream m_out;
  uint64_t m_bytes_written = 0;
  bool m_flush_pending = false;
  std::string m_active_buffer;
  std::string m_flush_buffer;
  std::chrono::steady_clock::time_point m_opened_at =
    std::chrono::steady_clock::now();

public:
  explicit WalBypassCaptureStream(CephContext* cct,
                                  fs::path dir,
                                  uint64_t seq)
    : m_cct(cct),
      m_dir(std::move(dir)),
      m_path(m_dir / make_wal_bypass_filename(seq)),
      m_seq(seq) {
  }

  ~WalBypassCaptureStream() {
    if (m_out.is_open()) {
      m_out.close();
    }
  }

  bool open() {
    std::error_code ec;
    if (fs::exists(m_path, ec) && !ec) {
      m_path_exists = true;
      ldout(m_cct, 15) << __func__ << " path exists: " << m_path << dendl;
      return false;
    }

    m_out.open(m_path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!m_out.is_open()) {
      lderr(m_cct) << __func__ << " failed to open bypass log: "
                   << m_path << dendl;
      return false;
    }

    m_path_exists = false;
    m_bytes_written = 0;
    m_opened_at = std::chrono::steady_clock::now();
    ldout(m_cct, 15) << __func__ << " opened stream seq=" << m_seq
                     << " path=" << m_path << dendl;
    return true;
  }

  bool write(const char* data, size_t len) {
    if (!m_out.is_open()) {
      return false;
    }
    m_out.write(data, len);
    if (!m_out.good()) {
      lderr(m_cct) << __func__ << " failed writing bypass log: "
                   << m_path << dendl;
      return false;
    }
    m_bytes_written += len;
    return true;
  }

  void append_to_active(const char* data, size_t len) {
    m_active_buffer.append(data, len);
  }

  size_t active_size() const {
    return m_active_buffer.size();
  }

  bool has_active_buffer() const {
    return !m_active_buffer.empty();
  }

  bool has_flush_pending() const {
    return m_flush_pending;
  }

  size_t flush_size() const {
    return m_flush_pending ? m_flush_buffer.size() : 0;
  }

  void enqueue_flush() {
    if (m_active_buffer.empty()) {
      return;
    }

    if (m_flush_pending) {
      m_flush_buffer.append(m_active_buffer);
      m_active_buffer.clear();
    } else {
      m_active_buffer.swap(m_flush_buffer);
      m_flush_pending = true;
    }
  }

  bool dequeue_flush(std::string* out) {
    if (!m_flush_pending) {
      return false;
    }
    out->swap(m_flush_buffer);
    m_flush_pending = false;
    return true;
  }

  bool sync_and_close() {
    if (!m_out.is_open()) {
      return true;
    }

    m_out.flush();
    if (!m_out.good()) {
      lderr(m_cct) << __func__ << " failed flushing bypass log: "
                   << m_path << dendl;
      return false;
    }
    m_out.close();
    return true;
  }

  bool path_exists() const {
    return m_path_exists;
  }

  uint64_t seq() const {
    return m_seq;
  }

  uint64_t bytes_written() const {
    return m_bytes_written;
  }

  std::chrono::steady_clock::time_point opened_at() const {
    return m_opened_at;
  }
};

class WalBypassRotatePolicy {
private:
  CephContext* m_cct;
  uint64_t m_rotate_size_bytes = 0;
  std::chrono::seconds m_rotate_interval = std::chrono::seconds(0);

public:
  WalBypassRotatePolicy(CephContext* cct,
                        uint64_t rotate_size_mb,
                        uint64_t rotate_interval_sec)
    : m_cct(cct) {
    if (rotate_size_mb > 0) {
      m_rotate_size_bytes = rotate_size_mb * 1024ULL * 1024ULL;
    }
    if (rotate_interval_sec > 0) {
      m_rotate_interval = std::chrono::seconds(rotate_interval_sec);
    }
  }

  bool enabled() const {
    return m_rotate_size_bytes > 0 || m_rotate_interval.count() > 0;
  }

  bool should_rotate(const WalBypassCaptureStream& stream) const {
    const auto now = std::chrono::steady_clock::now();
    const bool hit_size =
      (m_rotate_size_bytes > 0) && (stream.bytes_written() >= m_rotate_size_bytes);
    const bool hit_time =
      (m_rotate_interval.count() > 0) && (now - stream.opened_at() >= m_rotate_interval);
    if (hit_size || hit_time) {
      ldout(m_cct, 15) << __func__ << " hit_size=" << hit_size
                       << " hit_time=" << hit_time
                       << " bytes=" << stream.bytes_written() << dendl;
    }
    return hit_size || hit_time;
  }
};

}  // anonymous namespace

class WalBypassCapture::Impl {
private:
  CephContext* m_cct;
  ceph::common::PerfCounters* m_logger = nullptr;
  bool m_enabled = false;
  bool m_stopping = false;
  bool m_failed = false;
  uint64_t m_flush_trigger_bytes = 1024 * 1024;
  std::chrono::milliseconds m_flush_interval = std::chrono::milliseconds(100);
  std::unique_ptr<WalBypassRotatePolicy> m_rotate_policy;
  std::chrono::steady_clock::time_point m_last_enqueue = std::chrono::steady_clock::now();
  fs::path m_bypass_dir;
  std::unique_ptr<WalBypassSeqState> m_seq_state;
  std::unique_ptr<WalBypassCaptureStream> m_current_stream;
  std::deque<std::unique_ptr<WalBypassCaptureStream>> m_old_streams;
  std::mutex m_lock;
  std::condition_variable m_cond;
  std::thread m_worker;

public:
  explicit Impl(CephContext* cct, ceph::common::PerfCounters* logger)
    : m_cct(cct),
      m_logger(logger) {
    if (!m_cct->_conf.get_val<bool>("bluerocks_wal_bypass_enable")) {
      return;
    }

    std::string bypass_dir =
      m_cct->_conf.get_val<std::string>("bluerocks_wal_bypass_dir");
    if (bypass_dir.empty()) {
      return;
    }
    m_bypass_dir = fs::path(bypass_dir);

    uint64_t trigger_kb = m_cct->_conf.get_val<uint64_t>("bluerocks_wal_flush_trigger_kb");
    uint64_t interval_ms = m_cct->_conf.get_val<uint64_t>("bluerocks_wal_flush_interval_ms");
    if (trigger_kb == 0 || interval_ms == 0) {
      return;
    }

    uint64_t rotate_size_mb = m_cct->_conf.get_val<uint64_t>("bluerocks_wal_rotate_size_mb");
    uint64_t rotate_interval_sec = m_cct->_conf.get_val<uint64_t>("bluerocks_wal_rotate_interval_sec");
    if (rotate_size_mb == 0 && rotate_interval_sec == 0) {
      return;
    }

    m_flush_trigger_bytes = trigger_kb * 1024;
    m_flush_interval = std::chrono::milliseconds(interval_ms);
    m_rotate_policy = std::make_unique<WalBypassRotatePolicy>(
      m_cct, rotate_size_mb, rotate_interval_sec);
    if (!m_rotate_policy->enabled()) {
      return;
    }

    std::error_code ec;
    fs::create_directories(m_bypass_dir, ec);
    if (ec && !fs::exists(m_bypass_dir)) {
      lderr(m_cct) << __func__ << " failed to create bypass dir "
                   << m_bypass_dir << " ec=" << ec.message() << dendl;
      return;
    }

    m_seq_state = std::make_unique<WalBypassSeqState>(m_cct, m_bypass_dir);
    if (!m_seq_state->initialize()) {
      lderr(m_cct) << __func__ << " failed to initialize sequence state in "
                   << m_bypass_dir << dendl;
      return;
    }

    if (!open_next_stream()) {
      lderr(m_cct) << __func__ << " failed to open initial bypass stream"
                   << dendl;
      return;
    }

    m_enabled = true;
    m_worker = std::thread(&Impl::flush_loop, this);
  }

  ~Impl() {
    shutdown();
  }

  bool enabled() const {
    return m_enabled;
  }

  void append(const char* data, size_t len) {
    if (!m_enabled || m_failed || len == 0 || !m_current_stream) {
      return;
    }

    auto now = std::chrono::steady_clock::now();
    std::unique_lock lk(m_lock);
    m_current_stream->append_to_active(data, len);
    if (m_current_stream->active_size() >= m_flush_trigger_bytes ||
        now - m_last_enqueue >= m_flush_interval) {
      enqueue_flush_locked(now);
    }
    update_backlog_locked();
  }

private:
  void enqueue_flush_locked(std::chrono::steady_clock::time_point now) {
    if (!m_current_stream || !m_current_stream->has_active_buffer()) {
      return;
    }

    m_current_stream->enqueue_flush();
    m_last_enqueue = now;
    m_cond.notify_one();
  }

  bool has_flush_pending_locked() const {
    return m_current_stream && m_current_stream->has_flush_pending();
  }

  bool has_active_buffer_locked() const {
    return m_current_stream && m_current_stream->has_active_buffer();
  }

  bool write_to_current_stream(const char* data, size_t len) {
    if (!m_current_stream) {
      return false;
    }
    return m_current_stream->write(data, len);
  }

  bool open_next_stream() {
    if (!m_seq_state || !m_seq_state->ensure_consistent_with_disk()) {
      lderr(m_cct) << __func__ << " sequence state inconsistent" << dendl;
      return false;
    }

    for (int attempt = 0; attempt < 1024; ++attempt) {
      const uint64_t seq = m_seq_state->next_seq();
      auto stream = std::make_unique<WalBypassCaptureStream>(
        m_cct, m_bypass_dir, seq);
      if (!stream->open()) {
        if (stream->path_exists()) {
          m_seq_state->on_file_exists_conflict();
          continue;
        }
        on_write_error();
        return false;
      }

      if (!m_seq_state->commit_opened_seq(stream->seq())) {
        stream->sync_and_close();
        lderr(m_cct) << __func__ << " failed to commit opened seq="
                     << stream->seq() << dendl;
        on_write_error();
        return false;
      }
      m_current_stream = std::move(stream);
      on_file_opened();
      return true;
    }
    lderr(m_cct) << __func__ << " exhausted retries while opening new stream"
                 << dendl;
    return false;
  }

  bool rotate_stream() {
    if (!m_current_stream) {
      return false;
    }

    m_old_streams.push_back(std::move(m_current_stream));

    if (!drain_old_streams()) {
      lderr(m_cct) << __func__ << " failed draining old streams" << dendl;
      return false;
    }
    if (!open_next_stream()) {
      lderr(m_cct) << __func__ << " failed opening rotated stream" << dendl;
      return false;
    }
    return true;
  }

  bool drain_old_streams() {
    while (!m_old_streams.empty()) {
      auto& old_stream = m_old_streams.front();
      if (!old_stream->sync_and_close()) {
        lderr(m_cct) << __func__ << " failed to close old stream" << dendl;
        return false;
      }
      m_old_streams.pop_front();
    }
    return true;
  }

  void flush_loop() {
    std::string local;

    // 锁的范围比较大，并且这个锁是在关键路径上的，所以需要尽量避免在持锁期间做过多的事情
    // 目前 flush 的频率应该不会太高，所以应该不会有太大影响
    std::unique_lock lk(m_lock);
    while (true) {
      if (!has_flush_pending_locked()) {
        m_cond.wait_for(lk, m_flush_interval, [this] {
          return has_flush_pending_locked() || m_stopping;
        });
        if (!has_flush_pending_locked()) {
          enqueue_flush_locked(std::chrono::steady_clock::now());
        }
      }

      if (m_current_stream && m_current_stream->dequeue_flush(&local)) {
        if (!local.empty()) {
          auto t0 = mono_clock::now();
          const bool write_ok = write_to_current_stream(local.data(), local.size());
          auto flush_lat = mono_clock::now() - t0;
          on_flush_latency(flush_lat);
          if (!write_ok) {
            lderr(m_cct) << __func__ << " bypass write failed, disabling capture"
                         << dendl;
            m_failed = true;
            on_write_error();
          } else {
            on_bytes_written(local.size());
            if (m_rotate_policy &&
                m_current_stream &&
                m_rotate_policy->should_rotate(*m_current_stream) &&
                !rotate_stream()) {
              lderr(m_cct) << __func__ << " rotate failed, disabling capture"
                           << dendl;
              m_failed = true;
              on_write_error();
            }
          }
        }
        update_backlog_locked();
        local.clear();
      }

      if (m_stopping && !has_flush_pending_locked() && !has_active_buffer_locked()) {
        update_backlog_locked();
        break;
      }
    }
  }

  void shutdown() {
    if (!m_enabled) {
      return;
    }

    {
      std::lock_guard lk(m_lock);
      if (has_active_buffer_locked()) {
        enqueue_flush_locked(std::chrono::steady_clock::now());
      }
      m_stopping = true;
      m_cond.notify_one();
    }

    if (m_worker.joinable()) {
      m_worker.join();
    }

    if (m_current_stream) {
      m_current_stream->sync_and_close();
      m_current_stream.reset();
    }
    if (!m_old_streams.empty()) {
      drain_old_streams();
    }
    if (m_logger) {
      m_logger->set(l_bluestore_wal_bypass_backlog_bytes, 0);
    }
    m_enabled = false;
  }

  void on_file_opened() {
    if (m_logger) {
      m_logger->inc(l_bluestore_wal_bypass_files_total);
    }
  }

  void on_bytes_written(size_t len) {
    if (m_logger) {
      m_logger->inc(l_bluestore_wal_bypass_bytes_total, len);
    }
  }

  void on_write_error() {
    if (m_logger) {
      m_logger->inc(l_bluestore_wal_bypass_write_errors_total);
    }
  }

  void on_flush_latency(ceph::timespan lat) {
    if (m_logger) {
      m_logger->tinc_with_max(l_bluestore_wal_bypass_flush_latency, lat);
    }
  }

  void update_backlog_locked() {
    if (!m_logger || !m_current_stream) {
      return;
    }
    // 理论上不是很准，因为 m_old_streams 里可能还有一些待 flush 的数据
    // 但是这个值主要是为了监控和 alert 的，所以不需要非常精准
    const uint64_t backlog = m_current_stream->active_size() +
      m_current_stream->flush_size();
    m_logger->set(l_bluestore_wal_bypass_backlog_bytes, backlog);
  }
};

WalBypassCapture::WalBypassCapture(CephContext* cct,
                                   ceph::common::PerfCounters* logger)
  : m_impl(std::make_unique<WalBypassCapture::Impl>(cct, logger))
{
}

WalBypassCapture::~WalBypassCapture() = default;

bool WalBypassCapture::enabled() const
{
  return m_impl->enabled();
}

void WalBypassCapture::append(const char* data, size_t len)
{
  m_impl->append(data, len);
}
