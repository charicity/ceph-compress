// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "WalBypassCapture.hpp"
#include "WalBypassUtil.h"

#include "BlueStore.h"
#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;

#define dout_subsys ceph_subsys_bluestore

namespace {

// ---------------------------------------------------------------------------
// POSIX fsync helpers
// ---------------------------------------------------------------------------

int fsync_path(const fs::path& p)
{
  int fd = ::open(p.c_str(), O_RDONLY);
  if (fd < 0) {
    return -errno;
  }
  int r = ::fdatasync(fd);
  int save = errno;
  ::close(fd);
  return r < 0 ? -save : 0;
}

int fsync_directory(const fs::path& dir)
{
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    return -errno;
  }
  int r = ::fsync(fd);
  int save = errno;
  ::close(fd);
  return r < 0 ? -save : 0;
}

// ---------------------------------------------------------------------------

} // anonymous namespace

// Use make_wal_bypass_filename() and parse_wal_bypass_seq() from WalBypassUtil.h

namespace {

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

    // fsync the tmp file content before rename
    if (int r = fsync_path(tmp); r < 0) {
      lderr(m_cct) << __func__ << " failed to fsync tmp state file " << tmp
                   << " r=" << r << dendl;
      fs::remove(tmp, ec);
      return false;
    }

    fs::rename(tmp, m_state_path, ec);
    if (ec) {
      fs::remove(tmp, ec);
      lderr(m_cct) << __func__ << " failed to rename " << tmp << " to "
                   << m_state_path << " ec=" << ec.message() << dendl;
      return false;
    }

    // fsync directory to persist the rename (directory entry)
    if (int r = fsync_directory(m_dir); r < 0) {
      lderr(m_cct) << __func__ << " failed to fsync dir " << m_dir
                   << " after rename, r=" << r << dendl;
      // non-fatal: data is written, rename done, just dir entry may not be durable
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
    auto it = fs::directory_iterator(m_dir, ec);
    if (ec) {
      return 0;
    }
    for (auto end = fs::directory_iterator(); it != end; it.increment(ec)) {
      if (ec) {
        break;
      }
      if (!it->is_regular_file(ec) || ec) {
        continue;
      }
      uint64_t seq = 0;
      if (parse_wal_bypass_seq(it->path().filename().string(), seq) &&
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
  int m_fd = -1;
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
    if (m_fd >= 0) {
      ::close(m_fd);
      m_fd = -1;
    }
  }

  bool open() {
    // O_CREAT|O_EXCL provides atomic "create if not exists"
    m_fd = ::open(m_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_TRUNC, 0644);
    if (m_fd < 0) {
      if (errno == EEXIST) {
        m_path_exists = true;
        ldout(m_cct, 15) << __func__ << " path exists: " << m_path << dendl;
        return false;
      }
      lderr(m_cct) << __func__ << " failed to open bypass log: "
                   << m_path << " errno=" << cpp_strerror(errno) << dendl;
      return false;
    }

    m_path_exists = false;
    m_bytes_written = 0;
    m_opened_at = std::chrono::steady_clock::now();

    // fsync directory to persist the new directory entry
    fsync_directory(m_dir);

    ldout(m_cct, 15) << __func__ << " opened stream seq=" << m_seq
                     << " path=" << m_path << dendl;
    return true;
  }

  // write() is ONLY called by the worker thread (no lock needed for fd access)
  bool write(const char* data, size_t len) {
    if (m_fd < 0) {
      return false;
    }
    while (len > 0) {
      ssize_t r = ::write(m_fd, data, len);
      if (r < 0) {
        if (errno == EINTR) {
          continue;
        }
        lderr(m_cct) << __func__ << " failed writing bypass log: "
                     << m_path << " errno=" << cpp_strerror(errno) << dendl;
        return false;
      }
      data += r;
      len -= static_cast<size_t>(r);
      m_bytes_written += static_cast<uint64_t>(r);
    }
    return true;
  }

  // append_to_active is called under m_lock by the front-end thread
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
    if (m_fd < 0) {
      return true;
    }

    int r = ::fdatasync(m_fd);
    if (r < 0) {
      lderr(m_cct) << __func__ << " failed fdatasync bypass log: "
                   << m_path << " errno=" << cpp_strerror(errno) << dendl;
      ::close(m_fd);
      m_fd = -1;
      return false;
    }
    ::close(m_fd);
    m_fd = -1;
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

  // Atomic flags: read by front-end without lock, written by worker/shutdown.
  std::atomic<bool> m_enabled{false};
  std::atomic<bool> m_failed{false};

  bool m_stopping = false;  // only under m_lock
  uint64_t m_flush_trigger_bytes = 1024 * 1024;
  uint64_t m_max_backlog_bytes = 256ULL * 1024 * 1024;
  std::chrono::milliseconds m_flush_interval = std::chrono::milliseconds(100);
  std::unique_ptr<WalBypassRotatePolicy> m_rotate_policy;
  std::chrono::steady_clock::time_point m_last_enqueue = std::chrono::steady_clock::now();
  fs::path m_bypass_dir;

  // m_seq_state is ONLY accessed by the worker thread (and during init/shutdown
  // when worker is not running). No lock needed.
  std::unique_ptr<WalBypassSeqState> m_seq_state;

  // m_current_stream pointer: modified under m_lock.
  // write()/sync_and_close() on the stream: ONLY called by worker thread.
  // active_buffer operations: called under m_lock by front-end.
  std::unique_ptr<WalBypassCaptureStream> m_current_stream;

  // Total bytes appended to the bypass stream (across all files).
  // Updated under m_lock by front-end append().
  // Used by notify_new_wal() to compute 32 KB padding.
  uint64_t m_total_appended_bytes = 0;

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

    uint64_t max_backlog_mb = m_cct->_conf.get_val<uint64_t>("bluerocks_wal_max_backlog_mb");
    if (max_backlog_mb > 0) {
      m_max_backlog_bytes = max_backlog_mb * 1024ULL * 1024ULL;
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

    // Persist the sharding definition so the replay tool can create
    // a skeleton DB with matching Column Family layout.
    persist_sharding_meta();

    {
      // open_next_stream sets m_current_stream; safe without lock since
      // worker thread is not started yet.
      auto stream = try_create_stream();
      if (!stream) {
        lderr(m_cct) << __func__ << " failed to open initial bypass stream"
                     << dendl;
        return;
      }
      m_current_stream = std::move(stream);
      on_file_opened();
    }

    m_enabled.store(true, std::memory_order_release);
    m_worker = std::thread(&Impl::flush_loop, this);
  }

  ~Impl() {
    shutdown();
  }

  bool enabled() const {
    return m_enabled.load(std::memory_order_acquire);
  }

  void append(const char* data, size_t len) {
    if (!m_enabled.load(std::memory_order_acquire) ||
        m_failed.load(std::memory_order_acquire) ||
        len == 0) {
      return;
    }

    auto now = std::chrono::steady_clock::now();
    std::unique_lock lk(m_lock);

    if (!m_current_stream) {
      // During rotation window, silently drop.
      // This window is very short (sub-ms on healthy disks).
      return;
    }

    // Backlog limit check: reject new data when backlog exceeds threshold
    // to prevent OOM if the bypass disk is slower than WAL production rate.
    const uint64_t current_backlog = m_current_stream->active_size() +
      m_current_stream->flush_size();
    if (m_max_backlog_bytes > 0 && current_backlog + len > m_max_backlog_bytes) {
      on_data_dropped(len);
      return;
    }

    m_current_stream->append_to_active(data, len);
    m_total_appended_bytes += len;
    if (m_current_stream->active_size() >= m_flush_trigger_bytes ||
        now - m_last_enqueue >= m_flush_interval) {
      enqueue_flush_locked(now);
    }
    update_backlog_locked();
  }

  /// Pad bypass stream to next 32 KB block boundary.
  /// Called when RocksDB opens a new WAL file.  The zero padding causes
  /// log::Reader to skip to the start of the next block, keeping WAL segment
  /// records aligned.  (RocksDB treats kZeroType records as skip markers.)
  static constexpr uint32_t kWalBlockSize = 32768;  // rocksdb::log::kBlockSize

  void notify_new_wal() {
    if (!m_enabled.load(std::memory_order_acquire) ||
        m_failed.load(std::memory_order_acquire)) {
      return;
    }

    std::unique_lock lk(m_lock);
    if (!m_current_stream) {
      return;
    }

    const uint32_t remainder = static_cast<uint32_t>(
      m_total_appended_bytes % kWalBlockSize);
    if (remainder == 0) {
      // Already aligned, nothing to do.
      return;
    }
    const uint32_t pad_len = kWalBlockSize - remainder;
    // Allocate zero-filled padding.
    std::string zeros(pad_len, '\0');
    m_current_stream->append_to_active(zeros.data(), zeros.size());
    m_total_appended_bytes += pad_len;
    // Force an immediate flush so the padding reaches disk promptly.
    enqueue_flush_locked(std::chrono::steady_clock::now());
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

  /// Persist bluestore_rocksdb_cfs sharding definition to the bypass
  /// directory so the replay tool can create a matching skeleton DB.
  /// Non-fatal: failure only logs a warning.
  void persist_sharding_meta() {
    if (m_bypass_dir.empty()) return;
    bool use_cf = m_cct->_conf.get_val<bool>("bluestore_rocksdb_cf");
    std::string sharding_text;
    if (use_cf) {
      sharding_text = m_cct->_conf.get_val<std::string>("bluestore_rocksdb_cfs");
    }
    fs::path meta_path = m_bypass_dir / "ceph_wal_sharding.meta";

    // Atomic write: tmp → fsync → rename → fsync_directory
    const auto ts = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
    fs::path tmp = meta_path;
    tmp += ".tmp." + ts;
    {
      std::ofstream out(tmp, std::ios::out | std::ios::binary | std::ios::trunc);
      if (!out.is_open()) {
        ldout(m_cct, 1) << __func__ << " cannot write sharding meta to "
                        << tmp << dendl;
        return;
      }
      out << sharding_text;
      out.flush();
      if (!out.good()) {
        std::error_code ec;
        out.close();
        fs::remove(tmp, ec);
        return;
      }
    }
    fsync_path(tmp);
    std::error_code ec;
    fs::rename(tmp, meta_path, ec);
    if (ec) {
      fs::remove(tmp, ec);
      ldout(m_cct, 1) << __func__ << " failed to rename sharding meta: "
                      << ec.message() << dendl;
      return;
    }
    fsync_directory(m_bypass_dir);
    ldout(m_cct, 5) << __func__ << " persisted sharding meta to "
                    << meta_path << " (" << sharding_text.size()
                    << " bytes)" << dendl;
  }

  bool has_flush_pending_locked() const {
    return m_current_stream && m_current_stream->has_flush_pending();
  }

  bool has_active_buffer_locked() const {
    return m_current_stream && m_current_stream->has_active_buffer();
  }

  // Create a new bypass stream file. Only called by worker thread
  // (or during init when worker is not running).
  // Does NOT modify m_current_stream — caller assigns the result.
  std::unique_ptr<WalBypassCaptureStream> try_create_stream() {
    if (!m_seq_state || !m_seq_state->ensure_consistent_with_disk()) {
      lderr(m_cct) << __func__ << " sequence state inconsistent" << dendl;
      return nullptr;
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
        return nullptr;
      }

      if (!m_seq_state->commit_opened_seq(stream->seq())) {
        stream->sync_and_close();
        lderr(m_cct) << __func__ << " failed to commit opened seq="
                     << stream->seq() << dendl;
        on_write_error();
        return nullptr;
      }
      return stream;
    }
    lderr(m_cct) << __func__ << " exhausted retries while opening new stream"
                 << dendl;
    return nullptr;
  }

  // Rotate stream. Called by worker thread only.
  // The lock is NOT held on entry. We acquire/release it for pointer swaps
  // but do all file I/O without the lock.
  bool rotate_stream_unlocked() {
    // Phase 1: Detach old stream under lock.
    // While m_current_stream is null, front-end append() will silently drop.
    std::unique_ptr<WalBypassCaptureStream> old_stream;
    {
      std::unique_lock lk(m_lock);
      old_stream = std::move(m_current_stream);
      // m_current_stream is now nullptr
    }

    // Phase 2: Close old stream without lock (fdatasync + close).
    if (old_stream) {
      if (!old_stream->sync_and_close()) {
        lderr(m_cct) << __func__ << " failed closing old stream" << dendl;
        // non-fatal for rotation; continue opening new stream
      }
      old_stream.reset();
    }

    // Phase 3: Open new stream without lock (file I/O).
    auto new_stream = try_create_stream();

    // Phase 4: Install new stream under lock.
    {
      std::unique_lock lk(m_lock);
      if (new_stream) {
        m_current_stream = std::move(new_stream);
        on_file_opened();
      } else {
        lderr(m_cct) << __func__ << " failed opening rotated stream" << dendl;
        m_failed.store(true, std::memory_order_release);
        on_write_error();
        return false;
      }
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // flush_loop: The worker thread.
  //
  // Design: The lock (m_lock) is ONLY held for buffer management (dequeue,
  // enqueue, pointer swap). All file I/O (write, fsync, open, close) happens
  // WITHOUT the lock, so the front-end append() is never blocked by disk I/O.
  //
  // Safety: m_current_stream->write() is only called by this thread.
  // m_current_stream pointer is stable between lock acquisitions because
  // only this thread (and shutdown after join) modifies it.
  // ---------------------------------------------------------------------------
  void flush_loop() {
    std::string local;

    while (true) {
      bool got_data = false;

      // === Phase 1: Dequeue under lock ===
      {
        std::unique_lock lk(m_lock);
        if (!has_flush_pending_locked()) {
          m_cond.wait_for(lk, m_flush_interval, [this] {
            return has_flush_pending_locked() || m_stopping;
          });
          if (!has_flush_pending_locked()) {
            enqueue_flush_locked(std::chrono::steady_clock::now());
          }
        }

        // Check for termination
        if (m_stopping && !has_flush_pending_locked() && !has_active_buffer_locked()) {
          update_backlog_locked();
          break;
        }

        if (m_current_stream) {
          got_data = m_current_stream->dequeue_flush(&local);
        }
      }
      // Lock released — front-end append() can run freely now.

      // === Phase 2: Write I/O without lock ===
      // m_current_stream pointer is stable here: only this thread modifies it
      // (via rotate_stream_unlocked), and we haven't rotated yet.
      if (got_data && !local.empty() && m_current_stream) {
        auto t0 = mono_clock::now();
        const bool write_ok = m_current_stream->write(local.data(), local.size());
        auto flush_lat = mono_clock::now() - t0;
        on_flush_latency(flush_lat);

        if (!write_ok) {
          lderr(m_cct) << __func__ << " bypass write failed, disabling capture"
                       << dendl;
          m_failed.store(true, std::memory_order_release);
          on_write_error();
          local.clear();
          continue;  // loop back to check m_stopping
        }
        on_bytes_written(local.size());
        local.clear();

        // === Phase 3: Check rotation without lock ===
        // rotate policy + stream stats are read-only here; safe.
        if (m_rotate_policy && m_current_stream &&
            m_rotate_policy->should_rotate(*m_current_stream)) {
          if (!rotate_stream_unlocked()) {
            lderr(m_cct) << __func__ << " rotate failed, disabling capture"
                         << dendl;
            // m_failed already set by rotate_stream_unlocked
          }
        }
      } else {
        local.clear();
      }

      // === Phase 4: Update backlog under lock ===
      {
        std::unique_lock lk(m_lock);
        update_backlog_locked();
      }
    }
  }

  void shutdown() {
    if (!m_enabled.load(std::memory_order_acquire)) {
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
    if (m_logger) {
      m_logger->set(l_bluestore_wal_bypass_backlog_bytes, 0);
    }
    m_enabled.store(false, std::memory_order_release);
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

  void on_data_dropped(size_t /* len */) {
    if (m_logger) {
      m_logger->inc(l_bluestore_wal_bypass_drops_total);
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

void WalBypassCapture::notify_new_wal()
{
  m_impl->notify_new_wal();
}
