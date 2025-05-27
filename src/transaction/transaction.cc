#include "transaction/transaction.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"
#include "leanstore/statistics.h"
#include "storage/btree/node.h"
#include "storage/page.h"
#include "transaction/transaction_manager.h"

#include <cstring>

namespace leanstore::transaction {

void SerializableTransaction::Construct(const Transaction &txn) {
  state     = txn.state;
  stats     = txn.stats;
  start_ts  = txn.start_ts;
  commit_ts = txn.commit_ts;
  if (FLAGS_wal_variant != LoggingVariant::VECTOR) {
    needs_remote_flush = txn.needs_remote_flush;
    max_observed_gsn   = txn.max_observed_gsn;
  } else {
    txn.SerializeGSNVector(content);
    vector_size = txn.gsn_vector.size();
  }
  no_write_pages   = txn.to_write_pages_.size();
  no_evict_extents = txn.to_evict_extents_.size();
  no_free_extents  = txn.to_free_extents_.size();
  std::memcpy(&content[OffsetWritePages()], txn.to_write_pages_.data(), no_write_pages * sizeof(storage::LargePage));
  std::memcpy(&content[OffsetEvictExtents()], txn.to_evict_extents_.data(), no_evict_extents * sizeof(pageid_t));
  std::memcpy(&content[OffsetFreeExtents()], txn.to_free_extents_.data(),
              no_free_extents * sizeof(storage::ExtentTier));
  Ensure(MemorySize() == txn.SerializedSize());
}

auto SerializableTransaction::Dependencies() const -> std::span<const wid_t> {
  return {reinterpret_cast<const wid_t *>(content), vector_size};
}

auto SerializableTransaction::DepGSN() const -> std::span<const timestamp_t> {
  return {reinterpret_cast<const timestamp_t *>(&content[sizeof(wid_t) * vector_size]), vector_size};
}

/** Whether the given byte buffer does not store a valid SerializableTransaction */
auto SerializableTransaction::InvalidByteBuffer(const u8 *buffer) -> bool { return buffer[0] == NULL_ITEM; }

/* Should be in-sync with Transaction::SerializedSize() */
auto SerializableTransaction::MemorySize() -> u16 {
  auto vector_mem_size = (FLAGS_wal_variant != LoggingVariant::VECTOR) ? 0 : Transaction::VECTOR_KEY_SIZE * vector_size;
  auto ret = sizeof(SerializableTransaction) + vector_mem_size + no_write_pages * sizeof(storage::LargePage) +
             no_evict_extents * sizeof(pageid_t) + no_free_extents * sizeof(storage::ExtentTier);
  return UpAlign(ret, CPU_CACHELINE_SIZE);
}

// -------------------------------------------------------------------------------------

void Transaction::Initialize(TransactionManager *manager, timestamp_t start_timestamp, Type txn_type,
                             IsolationLevel level, Mode txn_mode) {
  //--------------------------
  manager_      = manager;
  is_read_only_ = true;
  type_         = txn_type;
  mode_         = txn_mode;
  iso_level_    = level;

  //--------------------------
  state     = State::STARTED;
  start_ts  = start_timestamp;
  commit_ts = 0;

  //--------------------------
  max_observed_gsn   = std::numeric_limits<timestamp_t>::max();
  needs_remote_flush = false;

  //--------------------------
  to_write_pages_   = storage::LargePageList();
  to_evict_extents_ = std::vector<pageid_t>();
  to_free_extents_  = storage::TierList();

  //--------------------------
  gsn_vector = std::unordered_map<wid_t, timestamp_t>();
}

/* Should be in-sync with SerializableTransaction::MemorySize() */
auto Transaction::SerializedSize() const -> u64 {
  auto ret = sizeof(SerializableTransaction) + Transaction::VECTOR_KEY_SIZE * gsn_vector.size() +
             to_write_pages_.size() * sizeof(storage::LargePage) + to_evict_extents_.size() * sizeof(pageid_t) +
             to_free_extents_.size() * sizeof(storage::ExtentTier);
  return UpAlign(ret, CPU_CACHELINE_SIZE);
}

auto Transaction::LogWorker() -> recovery::LogWorker & { return manager_->log_manager_->LocalLogWorker(); }

auto Transaction::BufferPool() -> buffer::BufferManager * { return manager_->buffer_; }

/**
 * @brief Serialize the txn's GSN into a buffer
 * Format, assuming the GSN unordered map stores info of W-2, W-9, and W-5:
 *
 * |-----|-----|-----|------------|------------|------------|
 * | W-2 | W-9 | W-5 | GSN of W-2 | GSN of W-9 | GSN of W-5 |
 * |-----|-----|-----|------------|------------|------------|
 *
 * Return the size of the serialized payload
 */
auto Transaction::SerializeGSNVector(u8 *buffer) const -> u64 {
  auto cnt       = gsn_vector.size();
  auto wid_array = reinterpret_cast<wid_t *>(buffer);
  auto ts_array  = reinterpret_cast<timestamp_t *>(&buffer[sizeof(wid_t) * cnt]);
  auto idx       = 0;
  for (auto &[wid, timestamp] : gsn_vector) {
    wid_array[idx] = wid;
    ts_array[idx]  = timestamp;
    idx++;
  }
  return reinterpret_cast<u8 *>(&ts_array[cnt]) - buffer;
}

auto Transaction::SerializedVectorSize() const -> u64 { return VECTOR_KEY_SIZE * gsn_vector.size(); }

auto Transaction::ReadOnly() -> bool { return is_read_only_; }

auto Transaction::IsRunning() -> bool { return state == State::STARTED; }

void Transaction::MarkAsWrite() {
  if (is_read_only_) {
    is_read_only_ = false;
    auto &entry   = LogWorker().ReserveLogMetaEntry();
    entry.type    = recovery::LogEntry::Type::TX_START;
    LogWorker().SubmitActiveLogEntry();
  }
}

auto Transaction::HasBLOB() -> bool {
  return !to_write_pages_.empty() || !to_evict_extents_.empty() || !to_free_extents_.empty();
}

auto Transaction::ToFlushedLargePages() -> storage::LargePageList & { return to_write_pages_; }

auto Transaction::ToEvictedExtents() -> std::vector<pageid_t> & { return to_evict_extents_; }

auto Transaction::ToFreeExtents() -> storage::TierList & { return to_free_extents_; }

}  // namespace leanstore::transaction