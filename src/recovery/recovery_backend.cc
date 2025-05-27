#include "recovery/recovery_backend.h"
#include "common/exceptions.h"
#include "leanstore/leanstore.h"
#include "recovery/io_block.h"

namespace leanstore::recovery {

RecoveryBackend::RecoveryBackend(buffer::BufferManager *buffer)
    : buffer_(buffer),
      wal_block_size_(FLAGS_wal_block_size_mb * MB),
      retries_per_worker_((FLAGS_worker_count + FLAGS_wal_recovery_threads - 1) / FLAGS_wal_recovery_threads) {}

void RecoveryBackend::Connect() {
  wal_fd_ = open(FLAGS_db_path.c_str(), O_RDONLY | O_DIRECT, S_IRWXU);
  Ensure(wal_fd_ > 0);
  wal_offset_ = StorageCapacity(FLAGS_db_path.c_str());
}

void RecoveryBackend::RetrieveMasterRecord() {
  auto buffer = AlignedPtr<BLK_BLOCK_SIZE>(wal_block_size_);

  auto cnt = pread(wal_fd_, buffer.get(), BLK_BLOCK_SIZE, wal_offset_ - BLK_BLOCK_SIZE);
  Ensure(cnt == BLK_BLOCK_SIZE);
  auto master = reinterpret_cast<LogMetaEntry *>(buffer.get());
  Ensure(master->type == LogEntry::Type::MASTER_RECORD);
  if (FLAGS_wal_debug) { master->ValidateChksum(); }
  signature_ = master->rc.signature;
  wal_offset_ -= BLK_BLOCK_SIZE;
}

/**
 * @brief For SSD backend, `offset` is the on-SSD offsets
 * For SLS, `offset` means the SLS's Log sequence numbers
 * For Bookkeeper, `offset` means log entry ID
 */
auto RecoveryBackend::ReadLogBlocks(const std::function<void(u8 *buf, u64 len, u32 signature)> &materialize_fn) -> u64 {
  /* Log entries should always be smaller than `wal_block_size_` with whatever log backend */
  auto buffer = AlignedPtr<BLK_BLOCK_SIZE>(wal_block_size_);

  // Last SSD offset that refers to valid log block (according to signature)
  u64 last_valid_offset = std::numeric_limits<u64>::max();
  auto invalid_blocks   = 0U;
  while (invalid_blocks < retries_per_worker_) {
    auto rd_offset = wal_offset_.fetch_sub(wal_block_size_) - wal_block_size_;
    auto rd        = pread(wal_fd_, buffer.get(), wal_block_size_, rd_offset);
    Ensure(rd == static_cast<i64>(wal_block_size_));
    auto block_metadata = reinterpret_cast<LogMetaEntry *>(&(buffer.get())[rd - sizeof(LogMetaEntry)]);
    spdlog::debug("Worker {} to analyze offset {} MB", LeanStore::worker_thread_id, rd_offset / MB);
    if (block_metadata->rc.signature != signature_) {
      spdlog::debug("Worker {} stops: required signature {}, found {}", LeanStore::worker_thread_id,
                    block_metadata->rc.signature, signature_);
      invalid_blocks++;
      continue;
    }
    last_valid_offset = rd_offset;
    materialize_fn(buffer.get(), wal_block_size_, signature_);
  }

  return last_valid_offset;
}

}  // namespace leanstore::recovery