// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>
#include <memory>

#include "include/common_fwd.h"

class WalBypassCapture {
public:
  explicit WalBypassCapture(CephContext* cct);
  ~WalBypassCapture();

  bool enabled() const;
  void append(const char* data, size_t len);

private:
  class Impl;
  std::unique_ptr<Impl> m_impl;
};
