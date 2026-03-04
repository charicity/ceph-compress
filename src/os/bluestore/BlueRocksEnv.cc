// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "BlueRocksEnv.h"
#include "BlueFS.h"
#include "common/debug.h"
#include "include/stringify.h"
#include "kv/RocksDBStore.h"
#include "string.h"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <thread>

using std::string_view;
namespace fs = std::filesystem;

#define dout_context nullptr
#define dout_subsys ceph_subsys_bluestore

namespace {

rocksdb::Status err_to_status(int r)
{
  switch (r) {
  case 0:
    return rocksdb::Status::OK();
  case -ENOENT:
    return rocksdb::Status::NotFound(rocksdb::Status::kNone);
  case -EINVAL:
    return rocksdb::Status::InvalidArgument(rocksdb::Status::kNone);
  case -EIO:
  case -EEXIST:
    return rocksdb::Status::IOError(rocksdb::Status::kNone);
  case -ENOLCK:
    return rocksdb::Status::IOError(strerror(r));
  default:
    // FIXME :(
    ceph_abort_msg("unrecognized error code");
    return rocksdb::Status::NotSupported(rocksdb::Status::kNone);
  }
}

std::pair<std::string_view, std::string_view>
split(const std::string &fn)
{
  size_t slash = fn.rfind('/');
  ceph_assert(slash != fn.npos);
  size_t file_begin = slash + 1;
  while (slash && fn[slash - 1] == '/')
    --slash;
  return {string_view(fn.data(), slash),
          string_view(fn.data() + file_begin,
	              fn.size() - file_begin)};
}

bool is_wal_file(const std::string& fname)
{
  // 根据后缀判断是否是 wal
  // 是否过于草率了
  constexpr std::string_view suffix = ".log";
  return fname.size() >= suffix.size() &&
    fname.compare(fname.size() - suffix.size(), suffix.size(), suffix) == 0;
}

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

// WalBypassCapture 系列的分工（按充血模型）:
// - WalBypassSeqState:
//   负责旁路文件序号状态（ceph_wal_seq.state）的读取、持久化、
//   与目录现状的一致性修正（next_seq >= max_existing+1）。
// - WalBypassCaptureStream:
//   负责单个旁路文件实体的生命周期与数据承载（文件身份seq/path、
//   双缓冲、写入、flush/close、已写字节与打开时间等自身状态）。
// - WalBypassRotatePolicy:
//   负责轮转判定策略（size/time），不参与IO执行与调度。
// - WalBypassCapture:
//   仅负责调度编排：初始化依赖、线程与条件变量驱动、flush触发、
//   stream切换/回收与错误熔断，不承载具体文件实体行为和策略细节。
class WalBypassCapture {
private:
  CephContext* m_cct;
  bool m_enabled = false;       // 是否启用bypass捕获
  bool m_stopping = false;      // 正在停止中
  bool m_failed = false;        // 写入失败
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
  explicit WalBypassCapture(CephContext* cct)
    : m_cct(cct) {
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
    m_worker = std::thread(&WalBypassCapture::flush_loop, this);
  }

  ~WalBypassCapture() {
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
        return false;
      }

      if (!m_seq_state->commit_opened_seq(stream->seq())) {
        stream->sync_and_close();
        lderr(m_cct) << __func__ << " failed to commit opened seq="
                     << stream->seq() << dendl;
        return false;
      }
      m_current_stream = std::move(stream);
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

    // WARNING: 我们目前不处理背压问题，但这里确实存在潜在丢数据的风险，压测这里是没问题的
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

    // 这个锁的粒度比较大，但考虑到我们目前的设计（单线程flush，且flush本身就是个重操作），应该是可以接受的
    // 但是如果未来我们想要优化flush性能（比如引入多线程flush，或者允许flush和append并行），我们可能需要重新设计这个锁的粒度和flush的调度机制
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
        if (!local.empty() && !write_to_current_stream(local.data(), local.size())) {
          lderr(m_cct) << __func__ << " bypass write failed, disabling capture"
                       << dendl;
          m_failed = true;
        } else if (!local.empty()) {
          if (m_rotate_policy &&
              m_current_stream &&
              m_rotate_policy->should_rotate(*m_current_stream) &&
              !rotate_stream()) {
            lderr(m_cct) << __func__ << " rotate failed, disabling capture"
                         << dendl;
            m_failed = true;
          }
        }
        local.clear();
      }

      if (m_stopping && !has_flush_pending_locked() && !has_active_buffer_locked()) {
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
    m_enabled = false;
  }
};

}

// A file abstraction for reading sequentially through a file
class BlueRocksSequentialFile : public rocksdb::SequentialFile {
  BlueFS *fs;
  BlueFS::FileReader *h;
 public:
  BlueRocksSequentialFile(BlueFS *fs, BlueFS::FileReader *h) : fs(fs), h(h) {}
  ~BlueRocksSequentialFile() override {
    delete h;
  }

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // REQUIRES: External synchronization
  rocksdb::Status Read(size_t n, rocksdb::Slice* result, char* scratch) override {
    int64_t r = fs->read(h, h->buf.pos, n, NULL, scratch);
    ceph_assert(r >= 0);
    *result = rocksdb::Slice(scratch, r);
    return rocksdb::Status::OK();
  }

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  rocksdb::Status Skip(uint64_t n) override {
    h->buf.skip(n);
    return rocksdb::Status::OK();
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    h->buf.invalidate_cache(offset, length);
    fs->invalidate_cache(h->file, offset, length);
    return rocksdb::Status::OK();
  }
};

// A file abstraction for randomly reading the contents of a file.
class BlueRocksRandomAccessFile : public rocksdb::RandomAccessFile {
  BlueFS *fs;
  BlueFS::FileReader *h;
 public:
  BlueRocksRandomAccessFile(BlueFS *fs, BlueFS::FileReader *h) : fs(fs), h(h) {}
  ~BlueRocksRandomAccessFile() override {
    delete h;
  }

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // Safe for concurrent use by multiple threads.
  rocksdb::Status Read(uint64_t offset, size_t n, rocksdb::Slice* result,
		       char* scratch) const override {
    int64_t r = fs->read_random(h, offset, n, scratch);
    ceph_assert(r >= 0);
    *result = rocksdb::Slice(scratch, r);
    return rocksdb::Status::OK();
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to eachother by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return snprintf(id, max_size, "%016llx",
		    (unsigned long long)h->file->fnode.ino);
  };

  // Readahead the file starting from offset by n bytes for caching.
  rocksdb::Status Prefetch(uint64_t offset, size_t n) override {
    fs->read(h, offset, n, nullptr, nullptr);
    return rocksdb::Status::OK();
  }

  //enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };

  void Hint(AccessPattern pattern) override {
    if (pattern == RANDOM)
      h->buf.max_prefetch = 4096;
    else if (pattern == SEQUENTIAL)
      h->buf.max_prefetch = fs->cct->_conf->bluefs_max_prefetch;
  }

  bool use_direct_io() const override {
    return !fs->cct->_conf->bluefs_buffered_io;
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    h->buf.invalidate_cache(offset, length);
    fs->invalidate_cache(h->file, offset, length);
    return rocksdb::Status::OK();
  }
};


// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class BlueRocksWritableFile : public rocksdb::WritableFile {
  BlueFS *fs;
  BlueFS::FileWriter *h;
  std::unique_ptr<WalBypassCapture> wal_bypass;
 public:
  BlueRocksWritableFile(BlueFS *fs,
                        BlueFS::FileWriter *h,
                        const std::string& fname,
                        bool may_be_wal)
    : fs(fs), h(h)
  {
    // may_be_wal 有点蠢，但是确实很有必要
    if (may_be_wal && is_wal_file(fname)) {
      wal_bypass = std::make_unique<WalBypassCapture>(fs->cct);
    }
  }
  ~BlueRocksWritableFile() override {
    fs->close_writer(h);
  }

  // Indicates if the class makes use of unbuffered I/O
  /*bool UseOSBuffer() const {
    return true;
    }*/

  // This is needed when you want to allocate
  // AlignedBuffer for use with file I/O classes
  // Used for unbuffered file I/O when UseOSBuffer() returns false
  /*size_t GetRequiredBufferAlignment() const {
    return c_DefaultPageSize;
    }*/

  rocksdb::Status Append(const rocksdb::Slice& data) override {
    fs->append_try_flush(h, data.data(), data.size());
    if (wal_bypass && wal_bypass->enabled()) {
      wal_bypass->append(data.data(), data.size());
    }
    return rocksdb::Status::OK();
  }

  // Positioned write for unbuffered access default forward
  // to simple append as most of the tests are buffered by default
  rocksdb::Status PositionedAppend(
    const rocksdb::Slice& /* data */,
    uint64_t /* offset */) override {
    return rocksdb::Status::NotSupported();
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  rocksdb::Status Truncate(uint64_t size) override {
    int r = fs->truncate(h, size);
    if (r < 0) {
      return err_to_status(r);
    }
    return rocksdb::Status::OK();
  }

  rocksdb::Status Close() override {
    fs->fsync(h);
    return rocksdb::Status::OK();
  }

  rocksdb::Status Flush() override {
    fs->flush(h);
    return rocksdb::Status::OK();
  }

  rocksdb::Status Sync() override { // sync data
    fs->fsync(h);
    return rocksdb::Status::OK();
  }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  bool IsSyncThreadSafe() const override {
    return true;
  }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  bool UseDirectIO() const {
    return false;
  }

  void SetWriteLifeTimeHint(rocksdb::Env::WriteLifeTimeHint hint) override {
    h->write_hint = (const int)hint;
  }

  /*
   * Get the size of valid data in the file.
   */
  uint64_t GetFileSize() override {
    return h->file->fnode.size + h->get_buffer_length();;
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return snprintf(id, max_size, "%016llx",
		    (unsigned long long)h->file->fnode.ino);
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    fs->fsync(h);
    fs->invalidate_cache(h->file, offset, length);
    return rocksdb::Status::OK();
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  rocksdb::Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    // round down to page boundaries
    int partial = offset & 4095;
    offset -= partial;
    nbytes += partial;
    nbytes &= ~4095;
    if (nbytes)
      fs->flush_range(h, offset, nbytes);
    return rocksdb::Status::OK();
  }

 protected:
  /*
   * Pre-allocate space for a file.
   */
  rocksdb::Status Allocate(uint64_t offset, uint64_t len) override {
    int r = fs->preallocate(h->file, offset, len);
    return err_to_status(r);
  }
};


// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class BlueRocksDirectory : public rocksdb::Directory {
  BlueFS *fs;
 public:
  explicit BlueRocksDirectory(BlueFS *f) : fs(f) {}

  // Fsync directory. Can be called concurrently from multiple threads.
  rocksdb::Status Fsync() override {
    // it is sufficient to flush the log.
    fs->sync_metadata(false);
    return rocksdb::Status::OK();
  }
};

// Identifies a locked file.
class BlueRocksFileLock : public rocksdb::FileLock {
 public:
  BlueFS *fs;
  BlueFS::FileLock *lock;
  BlueRocksFileLock(BlueFS *fs, BlueFS::FileLock *l) : fs(fs), lock(l) { }
  ~BlueRocksFileLock() override {
  }
};


// --------------------
// --- BlueRocksEnv ---
// --------------------

BlueRocksEnv::BlueRocksEnv(BlueFS *f)
  : EnvWrapper(Env::Default()),  // forward most of it to POSIX
    fs(f)
{

}

rocksdb::Status BlueRocksEnv::NewSequentialFile(
  const std::string& fname,
  std::unique_ptr<rocksdb::SequentialFile>* result,
  const rocksdb::EnvOptions& options)
{
  if (fname[0] == '/')
    return target()->NewSequentialFile(fname, result, options);
  auto [dir, file] = split(fname);
  BlueFS::FileReader *h;
  int r = fs->open_for_read(dir, file, &h, false);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksSequentialFile(fs, h));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewRandomAccessFile(
  const std::string& fname,
  std::unique_ptr<rocksdb::RandomAccessFile>* result,
  const rocksdb::EnvOptions& options)
{
  auto [dir, file] = split(fname);
  BlueFS::FileReader *h;
  int r = fs->open_for_read(dir, file, &h, true);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksRandomAccessFile(fs, h));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewWritableFile(
  const std::string& fname,
  std::unique_ptr<rocksdb::WritableFile>* result,
  const rocksdb::EnvOptions& options)
{
  auto [dir, file] = split(fname);
  BlueFS::FileWriter *h;
  int r = fs->open_for_write(dir, file, &h, false);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksWritableFile(fs, h, fname, true));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::ReuseWritableFile(
  const std::string& new_fname,
  const std::string& old_fname,
  std::unique_ptr<rocksdb::WritableFile>* result,
  const rocksdb::EnvOptions& options)
{
  auto [old_dir, old_file] = split(old_fname);
  auto [new_dir, new_file] = split(new_fname);

  int r = fs->rename(old_dir, old_file, new_dir, new_file);
  if (r < 0)
    return err_to_status(r);

  BlueFS::FileWriter *h;
  r = fs->open_for_write(new_dir, new_file, &h, true);
  if (r < 0)
    return err_to_status(r);
  result->reset(new BlueRocksWritableFile(fs, h, new_fname, true));
  fs->sync_metadata(false);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewDirectory(
  const std::string& name,
  std::unique_ptr<rocksdb::Directory>* result)
{
  if (!fs->dir_exists(name))
    return rocksdb::Status::NotFound(name, strerror(ENOENT));
  result->reset(new BlueRocksDirectory(fs));
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::FileExists(const std::string& fname)
{
  if (fname[0] == '/')
    return target()->FileExists(fname);
  auto [dir, file] = split(fname);
  if (fs->stat(dir, file, NULL, NULL) == 0)
    return rocksdb::Status::OK();
  return err_to_status(-ENOENT);
}

rocksdb::Status BlueRocksEnv::GetChildren(
  const std::string& dir,
  std::vector<std::string>* result)
{
  result->clear();
  int r = fs->readdir(dir, result);
  if (r < 0)
    return rocksdb::Status::NotFound(dir, strerror(ENOENT));//    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::DeleteFile(const std::string& fname)
{
  auto [dir, file] = split(fname);
  int r = fs->unlink(dir, file);
  if (r < 0)
    return err_to_status(r);
  fs->sync_metadata(false);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::CreateDir(const std::string& dirname)
{
  int r = fs->mkdir(dirname);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::CreateDirIfMissing(const std::string& dirname)
{
  int r = fs->mkdir(dirname);
  if (r < 0 && r != -EEXIST)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::DeleteDir(const std::string& dirname)
{
  int r = fs->rmdir(dirname);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetFileSize(
  const std::string& fname,
  uint64_t* file_size)
{
  auto [dir, file] = split(fname);
  int r = fs->stat(dir, file, file_size, NULL);
  if (r < 0)
    return err_to_status(r);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetFileModificationTime(const std::string& fname,
						      uint64_t* file_mtime)
{
  auto [dir, file] = split(fname);
  utime_t mtime;
  int r = fs->stat(dir, file, NULL, &mtime);
  if (r < 0)
    return err_to_status(r);
  *file_mtime = mtime.sec();
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::RenameFile(
  const std::string& src,
  const std::string& target)
{
  auto [old_dir, old_file] = split(src);
  auto [new_dir, new_file] = split(target);

  int r = fs->rename(old_dir, old_file, new_dir, new_file);
  if (r < 0)
    return err_to_status(r);
  fs->sync_metadata(false);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::LinkFile(
  const std::string& src,
  const std::string& target)
{
  ceph_abort();
}

rocksdb::Status BlueRocksEnv::AreFilesSame(
  const std::string& first,
  const std::string& second, bool* res)
{
  for (auto& path : {first, second}) {
    if (fs->dir_exists(path)) {
      continue;
    }
    auto [dir, file] = split(path);
    int r = fs->stat(dir, file, nullptr, nullptr);
    if (!r) {
      continue;
    } else if (r == -ENOENT) {
      return rocksdb::Status::NotFound("AreFilesSame", path);
    } else {
      return err_to_status(r);
    }
  }
  *res = (first == second);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::LockFile(
  const std::string& fname,
  rocksdb::FileLock** lock)
{
  auto [dir, file] = split(fname);
  BlueFS::FileLock *l = NULL;
  int r = fs->lock_file(dir, file, &l);
  if (r < 0)
    return err_to_status(r);
  *lock = new BlueRocksFileLock(fs, l);
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::UnlockFile(rocksdb::FileLock* lock)
{
  BlueRocksFileLock *l = static_cast<BlueRocksFileLock*>(lock);
  int r = fs->unlock_file(l->lock);
  if (r < 0)
    return err_to_status(r);
  delete lock;
  lock = nullptr;
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetAbsolutePath(
  const std::string& db_path,
  std::string* output_path)
{
  // this is a lie...
  *output_path = "/" + db_path;
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::NewLogger(
  const std::string& fname,
  std::shared_ptr<rocksdb::Logger>* result)
{
  // ignore the filename :)
  result->reset(create_rocksdb_ceph_logger());
  return rocksdb::Status::OK();
}

rocksdb::Status BlueRocksEnv::GetTestDirectory(std::string* path)
{
  static int foo = 0;
  *path = "temp_" + stringify(++foo);
  return rocksdb::Status::OK();
}
