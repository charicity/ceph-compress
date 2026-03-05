// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <string_view>
#include <unistd.h>

#include "include/ceph_assert.h"

struct wal_bypass_file_t {
  static constexpr std::string_view PREFIX = "ceph_wal_";
  static constexpr std::string_view SUFFIX = ".log";

  uint64_t seq = 0;

  wal_bypass_file_t() = default;
  explicit wal_bypass_file_t(uint64_t s) : seq(s) {}

  std::string filename() const {
    char name[64] = {0};
    int r = snprintf(name, sizeof(name), "ceph_wal_%010llu.log",
                     static_cast<unsigned long long>(seq));
    ceph_assert(r > 0);
    return std::string(name);
  }

  static bool from_filename(std::string_view name, wal_bypass_file_t* out) {
    if (name.size() <= PREFIX.size() + SUFFIX.size()) {
      return false;
    }
    if (name.compare(0, PREFIX.size(), PREFIX) != 0) {
      return false;
    }
    if (name.compare(name.size() - SUFFIX.size(), SUFFIX.size(), SUFFIX) != 0) {
      return false;
    }

    const size_t begin = PREFIX.size();
    const size_t end = name.size() - SUFFIX.size();
    for (size_t i = begin; i < end; ++i) {
      if (name[i] < '0' || name[i] > '9') {
        return false;
      }
    }

    errno = 0;
    char* parse_end = nullptr;
    unsigned long long value =
      strtoull(name.data() + begin, &parse_end, 10);
    if (errno != 0 || parse_end == nullptr ||
        static_cast<size_t>(parse_end - name.data()) != end) {
      return false;
    }

    if (out) {
      out->seq = static_cast<uint64_t>(value);
    }
    return true;
  }
};

/// Generate bypass WAL filename for a given sequence number.
/// Format: ceph_wal_0000000001.log (10-digit zero-padded)
inline std::string make_wal_bypass_filename(uint64_t seq)
{
  return wal_bypass_file_t(seq).filename();
}

/// Parse a bypass WAL filename and extract the sequence number.
/// Returns true on success.
inline bool parse_wal_bypass_seq(const std::string& name, uint64_t& seq)
{
  wal_bypass_file_t file;
  if (!wal_bypass_file_t::from_filename(name, &file)) {
    return false;
  }
  seq = file.seq;
  return true;
}

/// fdatasync a file by path. Returns 0 on success, -errno on failure.
inline int wal_fsync_path(const std::filesystem::path& p)
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

/// fsync a directory by path. Returns 0 on success, -errno on failure.
inline int wal_fsync_directory(const std::filesystem::path& dir)
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
