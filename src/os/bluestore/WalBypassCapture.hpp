// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>
#include <memory>

#include "include/common_fwd.h"

class WalBypassCapture {
public:
  explicit WalBypassCapture(CephContext* cct,
                            ceph::common::PerfCounters* logger = nullptr);
  ~WalBypassCapture();

  bool enabled() const;
  void append(const char* data, size_t len);
  /// Called when a new RocksDB WAL file begins.  Pads the bypass stream
  /// to the next 32 KB block boundary so that log::Reader stays aligned.
  void notify_new_wal();

private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};
