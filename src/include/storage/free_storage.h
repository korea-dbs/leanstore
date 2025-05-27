#pragma once

#include "storage/extent/extent_list.h"
#include "sync/range_lock.h"

#include "gtest/gtest_prod.h"
#include "tbb/concurrent_map.h"
#include "tbb/concurrent_queue.h"

#include <array>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::storage {

/**
 * @brief At the moment, only support free space management for extents
 * That is, it doesn't support normal B-tree page (will be thrown away upon deletion)
 */
class FreeStorageManager {
 public:
  FreeStorageManager();
  ~FreeStorageManager() = default;

  // Extent APIs
  auto TryLockRange(pageid_t start_pid, u64 page_count) -> bool;
  void PublicFreeExtents(std::span<const storage::ExtentTier> extents);
  void PrepareFreeTier(pageid_t start_pid, u8 tier_index);

 private:
  friend class leanstore::buffer::BufferManager;

  void LogFreePage(bool is_free_op, pageid_t start_pid, extidx_t tier_idx);

  std::unique_ptr<sync::RangeLock> locked_extent_;
};

}  // namespace leanstore::storage