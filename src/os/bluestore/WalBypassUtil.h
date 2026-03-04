// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <cstdio>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <string_view>

#include "include/ceph_assert.h"

/// Generate bypass WAL filename for a given sequence number.
/// Format: ceph_wal_0000000001.log (10-digit zero-padded)
inline std::string make_wal_bypass_filename(uint64_t seq)
{
  char name[64] = {0};
  int r = snprintf(name, sizeof(name), "ceph_wal_%010llu.log",
                   static_cast<unsigned long long>(seq));
  ceph_assert(r > 0);
  return std::string(name);
}

/// Parse a bypass WAL filename and extract the sequence number.
/// Returns true on success.
inline bool parse_wal_bypass_seq(const std::string& name, uint64_t& seq)
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
