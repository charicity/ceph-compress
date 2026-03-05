// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
//
// ceph-bluestore-wal-replay — Replay bypass-captured WAL files onto a
// skeleton RocksDB to recover OSD metadata.
//
// Usage:
//   ceph-bluestore-wal-replay --wal-dir <dir> --mode posix --db-path <dir>
//   ceph-bluestore-wal-replay --wal-dir <dir> --mode bluestore --osd-path <dir>
//   ceph-bluestore-wal-replay --wal-dir <dir> --verify-only
//

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

// RocksDB internal headers MUST come first to define port:: types and ALIGN_AS
// before Ceph headers (ShardedCache.h) can redefine CACHE_LINE_SIZE.
#include "port/port.h"
#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"

// Ceph headers
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "include/ceph_assert.h"

// BlueStore / KV
#include "kv/RocksDBStore.h"
#include "os/bluestore/BlueStore.h"
#include "os/bluestore/WalBypassUtil.h"

// RocksDB public
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"

namespace fs = std::filesystem;

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_bluestore

// ============================================================================
// Structs
// ============================================================================

struct ReplayOptions {
  std::string wal_dir;
  std::string mode;          // "posix" or "bluestore"
  std::string db_path;       // for posix mode
  std::string osd_path;      // for bluestore mode
  std::string checkpoint_file;
  uint64_t    checkpoint_interval = 10000;
  uint64_t    stop_seqno = 0;  // 0 = no limit
  bool        verify_only = false;
};

struct Checkpoint {
  uint64_t file_seq = 0;
  uint64_t file_offset = 0;
  uint64_t batch_count = 0;
  uint64_t bytes_applied = 0;
  uint64_t last_seqno = 0;
};

struct WalFileInfo {
  uint64_t seq;
  fs::path path;
  bool operator<(const WalFileInfo& o) const { return seq < o.seq; }
};

struct ReplayStats {
  uint64_t files_processed = 0;
  uint64_t batches_applied = 0;
  uint64_t bytes_total = 0;
  uint64_t last_seqno = 0;
  uint64_t cf_errors = 0;
  uint64_t zero_skips = 0;
};

// ============================================================================
// Checkpoint persistence
// ============================================================================

static bool write_checkpoint(const fs::path& path, const Checkpoint& cp)
{
  auto ts = std::to_string(
    std::chrono::steady_clock::now().time_since_epoch().count());
  fs::path tmp = path;
  tmp += ".tmp." + ts;

  {
    std::ofstream out(tmp, std::ios::out | std::ios::trunc);
    if (!out.is_open()) return false;
    out << "file_seq=" << cp.file_seq << "\n";
    out << "file_offset=" << cp.file_offset << "\n";
    out << "batch_count=" << cp.batch_count << "\n";
    out << "bytes_applied=" << cp.bytes_applied << "\n";
    out << "last_seqno=" << cp.last_seqno << "\n";
    out.flush();
    if (!out.good()) {
      out.close();
      std::error_code ec;
      fs::remove(tmp, ec);
      return false;
    }
  }

  wal_fsync_path(tmp);
  std::error_code ec;
  fs::rename(tmp, path, ec);
  if (ec) {
    fs::remove(tmp, ec);
    return false;
  }
  wal_fsync_directory(path.parent_path());
  return true;
}

static bool read_checkpoint(const fs::path& path, Checkpoint& cp)
{
  std::ifstream in(path, std::ios::in);
  if (!in.is_open()) return false;

  std::string line;
  while (std::getline(in, line)) {
    auto eq = line.find('=');
    if (eq == std::string::npos) continue;
    std::string key = line.substr(0, eq);
    std::string val = line.substr(eq + 1);
    if (key == "file_seq")       cp.file_seq = strtoull(val.c_str(), nullptr, 10);
    else if (key == "file_offset")  cp.file_offset = strtoull(val.c_str(), nullptr, 10);
    else if (key == "batch_count")  cp.batch_count = strtoull(val.c_str(), nullptr, 10);
    else if (key == "bytes_applied") cp.bytes_applied = strtoull(val.c_str(), nullptr, 10);
    else if (key == "last_seqno")   cp.last_seqno = strtoull(val.c_str(), nullptr, 10);
  }
  return true;
}

// ============================================================================
// Scan & validate bypass files
// ============================================================================

static int scan_wal_files(const std::string& wal_dir,
                          std::vector<WalFileInfo>& files)
{
  files.clear();
  std::error_code ec;
  for (auto it = fs::directory_iterator(wal_dir, ec);
       it != fs::directory_iterator();
       it.increment(ec)) {
    if (ec) {
      std::cerr << "ERROR: directory iteration error: " << ec.message() << std::endl;
      return -EIO;
    }
    if (!it->is_regular_file(ec) || ec) continue;
    wal_bypass_file_t file;
    if (wal_bypass_file_t::from_filename(it->path().filename().string(), &file)) {
      files.push_back({file.seq, it->path()});
    }
  }
  std::sort(files.begin(), files.end());
  return 0;
}

static int validate_continuity(const std::vector<WalFileInfo>& files)
{
  if (files.empty()) {
    std::cerr << "ERROR: no WAL bypass files found" << std::endl;
    return -ENOENT;
  }
  for (size_t i = 1; i < files.size(); i++) {
    if (files[i].seq != files[i - 1].seq + 1) {
      std::cerr << "ERROR: sequence gap detected between seq "
                << files[i - 1].seq << " and " << files[i].seq << std::endl;
      return -EINVAL;
    }
  }
  return 0;
}

// ============================================================================
// CfValidator — checks that CF IDs in a WriteBatch are within range
// ============================================================================

class CfValidator final : public rocksdb::WriteBatch::Handler {
public:
  explicit CfValidator(uint32_t max_cf_id) : m_max_cf_id(max_cf_id) {}
  bool ok() const { return m_ok; }

  rocksdb::Status PutCF(uint32_t cf, const rocksdb::Slice&,
                        const rocksdb::Slice&) override {
    return validate(cf);
  }
  rocksdb::Status DeleteCF(uint32_t cf, const rocksdb::Slice&) override {
    return validate(cf);
  }
  rocksdb::Status SingleDeleteCF(uint32_t cf, const rocksdb::Slice&) override {
    return validate(cf);
  }
  rocksdb::Status MergeCF(uint32_t cf, const rocksdb::Slice&,
                          const rocksdb::Slice&) override {
    return validate(cf);
  }
  rocksdb::Status DeleteRangeCF(uint32_t cf, const rocksdb::Slice&,
                                const rocksdb::Slice&) override {
    return validate(cf);
  }

private:
  rocksdb::Status validate(uint32_t cfid) {
    if (cfid > m_max_cf_id) {
      m_ok = false;
      return rocksdb::Status::Corruption(
        "CF id " + std::to_string(cfid) + " exceeds max " +
        std::to_string(m_max_cf_id));
    }
    return rocksdb::Status::OK();
  }

  uint32_t m_max_cf_id;
  bool m_ok = true;
};

// ============================================================================
// LogReporter for log::Reader
// ============================================================================

class LogReporter : public rocksdb::log::Reader::Reporter {
public:
  uint64_t corruptions = 0;
  void Corruption(size_t bytes, const rocksdb::Status& s) override {
    ++corruptions;
    std::cerr << "WAL corruption: " << bytes << " bytes dropped: "
              << s.ToString() << std::endl;
  }
};

// ============================================================================
// Read sharding metadata from bypass directory
// ============================================================================

static std::string read_sharding_meta(const std::string& wal_dir)
{
  fs::path meta_path = fs::path(wal_dir) / "ceph_wal_sharding.meta";
  std::ifstream in(meta_path, std::ios::in | std::ios::binary);
  if (!in.is_open()) {
    return "";
  }
  std::string content((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
  return content;
}

// ============================================================================
// Compute max CF id from sharding text
// ============================================================================

static uint32_t compute_max_cf_id(const std::string& sharding_text)
{
  if (sharding_text.empty()) {
    return 0;
  }
  std::vector<RocksDBStore::ColumnFamily> sharding_def;
  RocksDBStore::parse_sharding_def(sharding_text, sharding_def);
  uint32_t count = 0;
  for (auto& cf : sharding_def) {
    count += cf.shard_cnt;
  }
  return count;
}

// ============================================================================
// Open target DB — POSIX mode
// ============================================================================

static int open_posix_db(const ReplayOptions& opt,
                         const std::string& sharding_text,
                         std::unique_ptr<RocksDBStore>& store,
                         rocksdb::DB*& raw_db,
                         uint32_t& max_cf_id)
{
  // Create db directory
  std::error_code ec;
  fs::create_directories(opt.db_path, ec);
  if (ec && !fs::exists(opt.db_path)) {
    std::cerr << "ERROR: cannot create db-path " << opt.db_path
              << ": " << ec.message() << std::endl;
    return -EIO;
  }

  store = std::make_unique<RocksDBStore>(g_ceph_context, opt.db_path, std::map<std::string,std::string>{}, nullptr);

  // Minimal options
  std::string rocksdb_options;
  store->init(rocksdb_options);

  std::ostringstream err;
  int r;

  // Check if DB already exists (has CURRENT file). If so, open it directly
  // instead of create_and_open to avoid re-creating existing column families.
  bool db_exists = fs::exists(fs::path(opt.db_path) / "CURRENT");
  if (db_exists) {
    r = store->open(err);
  } else {
    r = store->create_and_open(err, sharding_text);
  }
  if (r < 0) {
    std::cerr << "ERROR: failed to " << (db_exists ? "open" : "create/open")
              << " RocksDB at " << opt.db_path
              << ": " << err.str() << std::endl;
    return r;
  }

  raw_db = store->get_raw_db();
  max_cf_id = compute_max_cf_id(sharding_text);

  std::cout << "Opened POSIX RocksDB at " << opt.db_path
            << " with max_cf_id=" << max_cf_id << std::endl;
  return 0;
}

// ============================================================================
// Open target DB — BlueStore mode
// ============================================================================

static int open_bluestore_db(const ReplayOptions& opt,
                             const std::string& sharding_text,
                             std::unique_ptr<BlueStore>& bluestore,
                             rocksdb::DB*& raw_db,
                             uint32_t& max_cf_id)
{
  bluestore = std::make_unique<BlueStore>(g_ceph_context, opt.osd_path);

  KeyValueDB* db_ptr = nullptr;
  int r = bluestore->open_db_environment(&db_ptr, false, false);
  if (r < 0) {
    std::cerr << "ERROR: failed to open BlueStore DB at " << opt.osd_path
              << ": " << cpp_strerror(r) << std::endl;
    return r;
  }

  auto* rocks_db = dynamic_cast<RocksDBStore*>(db_ptr);
  if (!rocks_db) {
    std::cerr << "ERROR: DB is not RocksDBStore" << std::endl;
    bluestore->close_db_environment();
    return -EINVAL;
  }

  raw_db = rocks_db->get_raw_db();
  max_cf_id = compute_max_cf_id(sharding_text);

  std::cout << "Opened BlueStore DB at " << opt.osd_path
            << " with max_cf_id=" << max_cf_id << std::endl;
  return 0;
}

// ============================================================================
// Replay one file
// ============================================================================

static int replay_one_file(const WalFileInfo& file_info,
                           rocksdb::DB* db,
                           uint32_t max_cf_id,
                           const ReplayOptions& opt,
                           Checkpoint& cp,
                           ReplayStats& stats)
{
  // Open the file using POSIX filesystem
  auto fs = rocksdb::FileSystem::Default();
  std::unique_ptr<rocksdb::FSSequentialFile> fsfile;
  rocksdb::IOStatus ios = fs->NewSequentialFile(
    file_info.path.string(),
    rocksdb::FileOptions(),
    &fsfile,
    nullptr /* dbg */);
  if (!ios.ok()) {
    std::cerr << "ERROR: cannot open " << file_info.path
              << ": " << ios.ToString() << std::endl;
    return -EIO;
  }

  std::unique_ptr<rocksdb::SequentialFileReader> file_reader(
    new rocksdb::SequentialFileReader(
      std::move(fsfile),
      file_info.path.string(),
      /* readahead_size */ 4 * 1024 * 1024));

  LogReporter reporter;
  rocksdb::log::Reader reader(
    nullptr,  // info_log
    std::move(file_reader),
    &reporter,
    true,   // checksum
    0);     // log_num (not recyclable)

  // Skip to checkpoint offset if resuming
  if (file_info.seq == cp.file_seq && cp.file_offset > 0) {
    // Skip forward in the reader. We parse and discard records until we
    // reach or exceed the checkpoint offset.
    rocksdb::Slice record;
    std::string scratch;
    while (reader.LastRecordEnd() < cp.file_offset) {
      if (!reader.ReadRecord(&record, &scratch,
                             rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords)) {
        break;
      }
    }
    std::cout << "  Resumed at offset " << reader.LastRecordEnd() << std::endl;
  }

  rocksdb::Slice record;
  std::string scratch;
  rocksdb::WriteOptions wopts;
  wopts.disableWAL = true;
  wopts.sync = false;

  while (reader.ReadRecord(&record, &scratch,
                           rocksdb::WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    if (record.size() < rocksdb::WriteBatchInternal::kHeader) {
      continue;
    }

    rocksdb::WriteBatch batch;
    rocksdb::Status s = rocksdb::WriteBatchInternal::SetContents(&batch, record);
    if (!s.ok()) {
      std::cerr << "WARNING: SetContents failed: " << s.ToString() << std::endl;
      continue;
    }

    uint64_t seqno = rocksdb::WriteBatchInternal::Sequence(&batch);

    // Stop condition
    if (opt.stop_seqno > 0 && seqno >= opt.stop_seqno) {
      std::cout << "  Stop condition reached at seqno=" << seqno << std::endl;
      // Update checkpoint to the position just before this record
      cp.file_seq = file_info.seq;
      cp.file_offset = reader.LastRecordOffset();
      return 1;  // signal: stopped by condition
    }

    if (opt.verify_only) {
      // Validate CF IDs only
      CfValidator validator(max_cf_id);
      s = batch.Iterate(&validator);
      if (!s.ok() || !validator.ok()) {
        stats.cf_errors++;
      }
    } else {
      // Validate first
      CfValidator validator(max_cf_id);
      s = batch.Iterate(&validator);
      if (!s.ok() || !validator.ok()) {
        stats.cf_errors++;
        // Skip this batch — CF layout mismatch
        continue;
      }

      // Apply
      s = db->Write(wopts, &batch);
      if (!s.ok()) {
        std::cerr << "ERROR: db->Write failed: " << s.ToString() << std::endl;
        return -EIO;
      }
    }

    stats.batches_applied++;
    stats.bytes_total += record.size();
    stats.last_seqno = seqno;

    // Periodic checkpoint
    if (!opt.verify_only &&
        stats.batches_applied % opt.checkpoint_interval == 0) {
      cp.file_seq = file_info.seq;
      cp.file_offset = reader.LastRecordEnd();
      cp.batch_count = stats.batches_applied;
      cp.bytes_applied = stats.bytes_total;
      cp.last_seqno = stats.last_seqno;
      if (!opt.checkpoint_file.empty()) {
        write_checkpoint(opt.checkpoint_file, cp);
      }
      std::cout << "  Checkpoint: batches=" << stats.batches_applied
                << " seqno=" << stats.last_seqno << std::endl;
    }
  }

  stats.zero_skips += reporter.corruptions;
  stats.files_processed++;

  // Update checkpoint for this file completion
  cp.file_seq = file_info.seq;
  cp.file_offset = 0;  // next file starts from beginning
  cp.batch_count = stats.batches_applied;
  cp.bytes_applied = stats.bytes_total;
  cp.last_seqno = stats.last_seqno;

  return 0;
}

// ============================================================================
// Usage
// ============================================================================

static void usage()
{
  std::cout << R"(
Usage: ceph-bluestore-wal-replay [options]

Required:
  --wal-dir <dir>           Directory containing bypass WAL files

Mode (choose one):
  --mode posix              Replay to a standalone POSIX RocksDB
    --db-path <dir>         Output RocksDB directory (created if missing)

  --mode bluestore          Replay to BlueStore's RocksDB (requires prior mkfs)
    --osd-path <dir>        OSD data directory

  --verify-only             Only scan and validate, do not write

Optional:
  --stop-seqno <N>          Stop after reaching this WriteBatch sequence number
  --checkpoint-file <path>  Checkpoint file path (default: <wal-dir>/replay_checkpoint)
  --checkpoint-interval <N> Batches between checkpoints (default: 10000)
)" << std::endl;
}

// ============================================================================
// Argument parsing
// ============================================================================

static int parse_args(int argc, const char** argv, ReplayOptions& opt)
{
  std::vector<const char*> args;
  args.reserve(argc);
  for (int i = 1; i < argc; i++) {
    args.push_back(argv[i]);
  }

  for (size_t i = 0; i < args.size(); i++) {
    std::string a(args[i]);
    if (a == "--wal-dir" && i + 1 < args.size()) {
      opt.wal_dir = args[++i];
    } else if (a == "--mode" && i + 1 < args.size()) {
      opt.mode = args[++i];
    } else if (a == "--db-path" && i + 1 < args.size()) {
      opt.db_path = args[++i];
    } else if (a == "--osd-path" && i + 1 < args.size()) {
      opt.osd_path = args[++i];
    } else if (a == "--stop-seqno" && i + 1 < args.size()) {
      opt.stop_seqno = strtoull(args[++i], nullptr, 10);
    } else if (a == "--checkpoint-file" && i + 1 < args.size()) {
      opt.checkpoint_file = args[++i];
    } else if (a == "--checkpoint-interval" && i + 1 < args.size()) {
      opt.checkpoint_interval = strtoull(args[++i], nullptr, 10);
    } else if (a == "--verify-only") {
      opt.verify_only = true;
    } else if (a == "-h" || a == "--help") {
      usage();
      return 1;
    }
    // Unrecognized args are passed through to global_init
  }

  if (opt.wal_dir.empty()) {
    std::cerr << "ERROR: --wal-dir is required" << std::endl;
    usage();
    return -EINVAL;
  }

  if (!opt.verify_only) {
    if (opt.mode.empty()) {
      std::cerr << "ERROR: --mode (posix|bluestore) is required" << std::endl;
      usage();
      return -EINVAL;
    }
    if (opt.mode == "posix" && opt.db_path.empty()) {
      std::cerr << "ERROR: --db-path is required for posix mode" << std::endl;
      return -EINVAL;
    }
    if (opt.mode == "bluestore" && opt.osd_path.empty()) {
      std::cerr << "ERROR: --osd-path is required for bluestore mode" << std::endl;
      return -EINVAL;
    }
  }

  if (opt.checkpoint_file.empty()) {
    opt.checkpoint_file = opt.wal_dir + "/replay_checkpoint";
  }

  return 0;
}

// ============================================================================
// main
// ============================================================================

int main(int argc, const char** argv)
{
  // Parse our own args first
  ReplayOptions opt;
  int r = parse_args(argc, argv, opt);
  if (r != 0) return (r > 0) ? 0 : EXIT_FAILURE;

  // Build args vector for global_init (pass through unknown args)
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  // ---- Step 1: Scan & validate bypass files ----
  std::cout << "=== Scanning WAL bypass directory: " << opt.wal_dir << std::endl;

  std::vector<WalFileInfo> wal_files;
  r = scan_wal_files(opt.wal_dir, wal_files);
  if (r < 0) return EXIT_FAILURE;

  r = validate_continuity(wal_files);
  if (r < 0) return EXIT_FAILURE;

  std::cout << "Found " << wal_files.size() << " WAL files, seq "
            << wal_files.front().seq << ".." << wal_files.back().seq
            << std::endl;

  // Read sharding metadata
  std::string sharding_text = read_sharding_meta(opt.wal_dir);
  if (!sharding_text.empty()) {
    std::cout << "Sharding metadata: " << sharding_text << std::endl;
  } else {
    std::cout << "No sharding metadata found (default CF only)" << std::endl;
  }

  if (opt.verify_only) {
    std::cout << "=== Verify-only mode ===" << std::endl;
  }

  // ---- Step 2: Open target DB ----
  rocksdb::DB* raw_db = nullptr;
  uint32_t max_cf_id = 0;
  std::unique_ptr<RocksDBStore> posix_store;
  std::unique_ptr<BlueStore> bluestore;

  if (!opt.verify_only) {
    if (opt.mode == "posix") {
      r = open_posix_db(opt, sharding_text, posix_store, raw_db, max_cf_id);
      if (r < 0) return EXIT_FAILURE;
    } else if (opt.mode == "bluestore") {
      r = open_bluestore_db(opt, sharding_text, bluestore, raw_db, max_cf_id);
      if (r < 0) return EXIT_FAILURE;
    }
  } else {
    max_cf_id = compute_max_cf_id(sharding_text);
  }

  // ---- Step 3: Load checkpoint ----
  Checkpoint cp;
  if (!opt.checkpoint_file.empty() && fs::exists(opt.checkpoint_file)) {
    if (read_checkpoint(opt.checkpoint_file, cp)) {
      std::cout << "Loaded checkpoint: file_seq=" << cp.file_seq
                << " offset=" << cp.file_offset
                << " batches=" << cp.batch_count
                << " last_seqno=" << cp.last_seqno << std::endl;
    }
  }

  // ---- Step 4: Replay ----
  std::cout << "=== Starting replay ===" << std::endl;
  auto t_start = std::chrono::steady_clock::now();

  ReplayStats stats;
  stats.batches_applied = cp.batch_count;
  stats.bytes_total = cp.bytes_applied;
  stats.last_seqno = cp.last_seqno;

  bool stopped_by_condition = false;

  for (auto& wf : wal_files) {
    // Skip files before checkpoint
    if (cp.file_seq > 0 && wf.seq < cp.file_seq) {
      continue;
    }
    // If we're at the checkpoint file but offset is 0, that means
    // the file was fully processed. Skip it.
    if (cp.file_seq > 0 && wf.seq == cp.file_seq && cp.file_offset == 0) {
      continue;
    }

    std::cout << "Processing " << wf.path.filename()
              << " (seq=" << wf.seq << ")" << std::endl;

    r = replay_one_file(wf, raw_db, max_cf_id, opt, cp, stats);
    if (r == 1) {
      // Stopped by --stop-seqno condition
      stopped_by_condition = true;
      break;
    }
    if (r < 0) {
      std::cerr << "ERROR: replay failed at " << wf.path << std::endl;
      // Save final checkpoint
      if (!opt.checkpoint_file.empty() && !opt.verify_only) {
        write_checkpoint(opt.checkpoint_file, cp);
      }
      goto cleanup;
    }
  }

  // Final checkpoint
  if (!opt.checkpoint_file.empty() && !opt.verify_only) {
    write_checkpoint(opt.checkpoint_file, cp);
  }

  // ---- Step 5: Flush & verify ----
  if (raw_db && !opt.verify_only) {
    std::cout << "Flushing memtables ..." << std::endl;
    {
      rocksdb::FlushOptions fopts;
      fopts.wait = true;
      fopts.allow_write_stall = true;
      // Flush all column families
      auto s = raw_db->Flush(fopts);
      if (!s.ok()) {
        std::cerr << "WARNING: Flush failed: " << s.ToString() << std::endl;
      }
    }
  }

cleanup:
  {
    auto t_end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();

    std::cout << "\n=== Replay Summary ===" << std::endl;
    std::cout << "  Files processed : " << stats.files_processed << std::endl;
    std::cout << "  Batches applied : " << stats.batches_applied << std::endl;
    std::cout << "  Bytes total     : " << stats.bytes_total << std::endl;
    std::cout << "  Last seqno      : " << stats.last_seqno << std::endl;
    std::cout << "  CF ID errors    : " << stats.cf_errors << std::endl;
    std::cout << "  Corruptions     : " << stats.zero_skips << std::endl;
    std::cout << "  Elapsed time    : " << elapsed << "s" << std::endl;
    if (stopped_by_condition) {
      std::cout << "  Stopped by      : --stop-seqno " << opt.stop_seqno << std::endl;
    }
    std::cout << "  Exit code       : " << (r < 0 ? "FAILURE" : "SUCCESS") << std::endl;
  }

  // Close DB
  if (bluestore) {
    bluestore->close_db_environment();
    bluestore.reset();
  }
  if (posix_store) {
    posix_store->close();
    posix_store.reset();
  }

  return (r < 0) ? EXIT_FAILURE : EXIT_SUCCESS;
}
