#ifndef CEPH_HRACCOMPRESSOR_H
#define CEPH_HRACCOMPRESSOR_H

#include <immintrin.h>
#include <stdlib.h>
#include <x86gprintrin.h>

#include "Hrac.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/dout.h"
#include "compressor/Compressor.h"
#include "include/buffer.h"
#include "include/encoding.h"

#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix *_dout << "hrac: "

const uint32_t HRAC_BLK = 64;

const uint32_t HRAC_NSBLK = 65536;
const uint32_t HRAC_INNER = 16;
// const uint32_t HRAC_NSBLK = 1024;
// const uint32_t HRAC_INNER = 16384;

#define dtype uint8_t
#define diff_e diff_e8
#define diff_d diff_d8
#define BW 8
#define LBW 3
#define sig_len(x) nonzero_count_u8[x]

#define ROUND_UP_256(x) (((x) + 255) & ~255)

class HracCompressor : public Compressor {
  CephContext *cct;

public:
  HracCompressor(CephContext *cct)
      : Compressor(COMP_ALG_HRAC, "hrac"), cct(cct) {}

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst,
               boost::optional<int32_t> &compressor_message) override {
    ldout(cct, 0) << "HRAC_DEBUG: compress() called! input_size="
                  << src.length() << dendl;

    uint32_t origin_len = src.length();

    if (origin_len < HRAC_BLK) {
      ldout(cct, 0)
          << "HRAC_DEBUG: compress() input too small for compression: "
          << origin_len << " VS " << HRAC_BLK << dendl;
      return -1;
    }

    ceph::buffer::list src_contig = src;
    if (!src.is_contiguous()) {
      src_contig.rebuild();
    }
    const uint8_t *input_ptr = (const uint8_t *)src_contig.c_str();

    size_t max_out_len = (size_t)(origin_len * 1.2) + 2048;
    ldout(cct, 0) << "HRAC_DEBUG: compress() Calling fits_kcomp_u8 as "
                  << origin_len << " " << max_out_len << dendl;
    ceph::buffer::ptr out_ptr = ceph::buffer::create_page_aligned(max_out_len);
    uint8_t *output_raw = (uint8_t *)out_ptr.c_str();

    uint32_t compressed_len =
        fits_kcomp_u8(input_ptr, origin_len, output_raw, max_out_len, HRAC_BLK,
                      HRAC_NSBLK, HRAC_INNER);

    if (compressed_len == 0 || compressed_len >= origin_len) {
      ldout(cct, 0) << "HRAC_DEBUG: compress() Compression not effective: "
                    << compressed_len << " VS " << origin_len
                    << ", input_size=" << origin_len
                    << ", max_out_len=" << max_out_len << dendl;
      return -1;
    }

    ceph::encode(origin_len, dst);
    dst.append(out_ptr, 0, compressed_len);

    ldout(cct, 0) << "HRAC_DEBUG: compress() Compression done. Compressed len="
                  << compressed_len << ", origin_len=" << origin_len << dendl;
    return 0;
  }

  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst,
                 boost::optional<int32_t> compressor_message) override {
    auto i = src.begin();
    return decompress(i, src.length(), dst, compressor_message);
  }

  int decompress(ceph::buffer::list::const_iterator &iter,
                 size_t compressed_len, ceph::buffer::list &dst,
                 boost::optional<int32_t> compressor_message) override {
    ldout(cct, 0) << "HRAC_DEBUG: decompress() called! src_len="
                  << compressed_len << dendl;

    if (compressed_len <= 4) {
      ldout(cct, 0) << "HRAC_ERROR: decompress() compressed_len too small: "
                    << compressed_len << dendl;
      return -1;
    }

    uint32_t origin_len;
    try {
      ceph::decode(origin_len, iter);
    } catch (...) {
      ldout(cct, 0) << "HRAC_ERROR: decompress() Failed to decode origin_len!"
                    << dendl;
      return -1;
    }

    ldout(cct, 0) << "HRAC_DEBUG: decompress() origin_len=" << origin_len
                  << dendl;

    if (origin_len > 100 * 1024 * 1024) {
      ldout(cct, 0) << "HRAC_ERROR: decompress() origin_len too big!" << dendl;
      return -1;
    }

    uint32_t actual_compressed_len = compressed_len - 4;

    // 添加边界检查
    if (iter.get_remaining() < actual_compressed_len) {
      ldout(cct, 0)
          << "HRAC_ERROR: decompress() Not enough data in buffer! remaining="
          << iter.get_remaining() << " needed=" << actual_compressed_len
          << dendl;
      return -1;
    }

    size_t alloc_in_size = ROUND_UP_256(actual_compressed_len + 64);
    void *aligned_input_buffer = aligned_alloc(256, alloc_in_size);

    if (!aligned_input_buffer) {
      ldout(cct, 0) << "HRAC_ERROR: decompress() Alloc failed for input"
                    << dendl;
      return -1;
    }

    memset(aligned_input_buffer, 0, alloc_in_size);

    // iter.copy() 会自动移动 iterator
    iter.copy(actual_compressed_len, (char *)aligned_input_buffer);
    const uint8_t *input_raw = (const uint8_t *)aligned_input_buffer;

    size_t alloc_out_size = ROUND_UP_256(origin_len + 64);
    void *aligned_output_buffer = aligned_alloc(256, alloc_out_size);

    if (!aligned_output_buffer) {
      ldout(cct, 0) << "HRAC_ERROR: decompress() Alloc failed for output"
                    << dendl;
      free(aligned_input_buffer);
      return -1;
    }
    memset(aligned_output_buffer, 0, alloc_out_size);
    uint8_t *output_raw = static_cast<uint8_t *>(aligned_output_buffer);

    ldout(cct, 0) << "HRAC_DEBUG: decompress() Calling fits_kdecomp_u8..."
                  << dendl;

    fits_kdecomp_u8(input_raw, actual_compressed_len, output_raw, origin_len,
                    HRAC_BLK, HRAC_NSBLK, HRAC_INNER);

    ldout(cct, 0) << "HRAC_DEBUG: decompress() Decompression done." << dendl;

    dst.append((char *)aligned_output_buffer, origin_len);
    free(aligned_input_buffer);
    free(aligned_output_buffer);

    return 0;
  }
};

#endif // CEPH_HRACCOMPRESSOR_H
