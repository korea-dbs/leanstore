#pragma once

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "recovery/log_entry.h"
#include "storage/extent/large_page.h"

#include "gtest/gtest_prod.h"

#include <limits>
#include <memory>
#include <new>
#include <span>
#include <type_traits>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::recovery {
struct LogWorker;
class GroupCommitExecutor;
}  // namespace leanstore::recovery

namespace leanstore::transaction {

class TransactionManager;

enum class IsolationLevel : u8 {
  READ_UNCOMMITTED   = 0,
  READ_COMMITTED     = 1,
  SNAPSHOT_ISOLATION = 2,
  SERIALIZABLE       = 3,
};

/**
 * @brief Commit protocol variants:
 * - BASELINE_COMMIT:       Baseline commit protocol employed in MySQL and PostgreSQL
 *                          One of the workers thread, after acquiring the lock, executes the group commit job
 * - FLUSH_PIPELINING:      Group commit with AsyncIO + fdatasync() to flush data
 * - WORKERS_WRITE_LOG:     The workers pwrite() the log entries after a certain amount.
 *                          Group commit is responsible for fdatasync() and mark transaction committed
 * - AUTONOMOUS_COMMIT:     During log write, workers try to steal another log buffer from another
 */
enum class CommitProtocol : u8 {
  BASELINE_COMMIT   = 0,
  FLUSH_PIPELINING  = 1,
  WORKERS_WRITE_LOG = 2,
  AUTONOMOUS_COMMIT = 3,
};

inline auto operator==(int lhs, CommitProtocol &&rhs) -> bool { return ToUnderlying(rhs) == lhs; }

inline auto operator!=(int lhs, CommitProtocol &&rhs) -> bool { return ToUnderlying(rhs) != lhs; }

struct SerializableTransaction;

class Transaction {
 public:
  static constexpr size_t VECTOR_KEY_SIZE = sizeof(wid_t) + sizeof(timestamp_t);
  enum class Type : u8 { USER, SYSTEM };
  enum class Mode : u8 { OLTP, OLAP };
  enum class State : u8 { IDLE = 0, STARTED = 1, READY_TO_COMMIT = 2, COMMITTED = 3, ABORTED = 4, BARRIER = 5 };

  // Statistics
  struct Statistics {
    timestamp_t arrival_time;
    timestamp_t start;
    timestamp_t precommit;
  } stats;

  // Transaction run-time
  State state{State::IDLE};
  timestamp_t start_ts;
  timestamp_t commit_ts;
  timestamp_t max_observed_gsn;  // The smallest upper-limit of gsn at which all logs of all workers have been flushed
  bool needs_remote_flush;       // whether active_txn can avoid remote flush before commit
  std::unordered_map<wid_t, timestamp_t> gsn_vector;  // Dependency vector

#ifdef ENABLE_TESTING
  void ResetState() { state = State::IDLE; }
#endif
  void Initialize(TransactionManager *manager, timestamp_t start_timestamp, Type txn_type, IsolationLevel level,
                  Mode txn_mode);

  // Misc utilities
  auto SerializedSize() const -> u64;
  auto LogWorker() -> recovery::LogWorker &;
  auto BufferPool() -> buffer::BufferManager *;

  // Txn context - public interfaces
  auto TxnID() -> txnid_t { return start_ts; }

  auto ReadOnly() -> bool;
  auto IsRunning() -> bool;
  void MarkAsWrite();
  auto HasBLOB() -> bool;

  // GSN Vector utility
  auto SerializeGSNVector(u8 *buffer) const -> u64;
  auto SerializedVectorSize() const -> u64;

  // Blob/Extent utility
  auto ToFlushedLargePages() -> storage::LargePageList &;
  auto ToEvictedExtents() -> std::vector<pageid_t> &;
  auto ToFreeExtents() -> storage::TierList &;

 private:
  friend class TransactionManager;
  friend struct recovery::LogWorker;
  friend struct SerializableTransaction;
  friend class recovery::GroupCommitExecutor;
  FRIEND_TEST(TestTransaction, TransactionLifeTime);
  FRIEND_TEST(TestTransaction, AllocateBlobWithLogging);

  // Transaction config
  TransactionManager *manager_;
  bool is_read_only_;
  Type type_;
  Mode mode_;
  IsolationLevel iso_level_;

  // Required async-write these extents
  storage::LargePageList to_write_pages_;
  std::vector<pageid_t> to_evict_extents_;
  storage::TierList to_free_extents_;
};

struct alignas(CPU_CACHELINE_SIZE) SerializableTransaction {
  static constexpr u8 NULL_ITEM = std::numeric_limits<u8>::max();

  Transaction::State state = {Transaction::State::IDLE};

  union {
    // Used for GSN/RFA variants
    struct {
      bool needs_remote_flush : 1;
      timestamp_t max_observed_gsn : 63;
    };

    // Used for VECTOR variant
    size_t vector_size;
  };

  Transaction::Statistics stats;
  timestamp_t start_ts;
  timestamp_t commit_ts;
  u16 no_write_pages   = 0;
  u16 no_evict_extents = 0;
  u16 no_free_extents  = 0;
  u8 content[];

  SerializableTransaction()  = default;
  ~SerializableTransaction() = default;

  void Construct(const Transaction &txn);
  auto MemorySize() -> uoffset_t;

  static auto InvalidByteBuffer(const u8 *buffer) -> bool;

  // -------------------------------------------------------------------------------------
  auto ToFlushedLargePages() const -> std::span<const storage::LargePage> {
    return {reinterpret_cast<const storage::LargePage *>(&content[0]), no_write_pages};
  }

  auto ToEvictedExtents() const -> std::span<const pageid_t> {
    return {reinterpret_cast<const pageid_t *>(&content[OffsetEvictExtents()]), no_evict_extents};
  }

  auto ToFreeExtents() const -> std::span<const storage::ExtentTier> {
    return {reinterpret_cast<const storage::ExtentTier *>(&content[OffsetFreeExtents()]), no_free_extents};
  }

  // -------------------------------------------------------------------------------------
  auto Dependencies() const -> std::span<const wid_t>;
  auto DepGSN() const -> std::span<const timestamp_t>;

 private:
  auto OffsetWritePages() const -> u64 {
    if (FLAGS_wal_variant != LoggingVariant::VECTOR) { return 0; }
    return vector_size * Transaction::VECTOR_KEY_SIZE;
  }

  auto OffsetEvictExtents() const -> u64 { return OffsetWritePages() + no_write_pages * sizeof(storage::LargePage); }

  auto OffsetFreeExtents() const -> u64 { return OffsetEvictExtents() + no_evict_extents * sizeof(pageid_t); }
};

static_assert(sizeof(SerializableTransaction) == CPU_CACHELINE_SIZE);
static_assert(std::is_trivially_destructible_v<SerializableTransaction>);

}  // namespace leanstore::transaction