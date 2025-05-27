#pragma once

#include "common/typedefs.h"

#include <atomic>
#include <bitset>

namespace leanstore::buffer {

/**
 * @brief Run-time context of all DB pages
 */
struct BufferFrame {
  wid_t last_writer;     // the id of last worker who modifies this page since its last flush
  u64 last_written_gsn;  // the last gsn that this extent/page was written to storage

  void Init() {
    last_writer      = 0;
    last_written_gsn = 0;
  }
};

/**
 * @brief Similar to Buffer Frame, but for Extent pages
 */
struct alignas(8) ExtentFrame {
  std::atomic<bool> prevent_evict;
  u64 extent_size : 48;

  void Init() {
    prevent_evict = false;
    extent_size   = 1;
  }
};

static_assert(sizeof(ExtentFrame) == 8);

}  // namespace leanstore::buffer