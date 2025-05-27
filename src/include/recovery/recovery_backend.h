#pragma once

#include "common/typedefs.h"
#include "common/utils.h"

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::recovery {

class RecoveryManager;

class RecoveryBackend {
 public:
  explicit RecoveryBackend(buffer::BufferManager *buffer);
  void Connect();
  void RetrieveMasterRecord();
  auto ReadLogBlocks(const std::function<void(u8 *buf, u64 len, u32 signature)> &materialize_fn) -> u64;

 private:
  friend class RecoveryManager;

  /* General */
  buffer::BufferManager *buffer_;
  u32 signature_;

  /* Properties for SSD log backend */
  int wal_fd_;
  std::atomic<u64> wal_offset_;
  const u64 wal_block_size_;
  const u32 retries_per_worker_;
};

}  // namespace leanstore::recovery