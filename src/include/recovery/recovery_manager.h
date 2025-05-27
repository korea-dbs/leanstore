#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "recovery/io_block.h"
#include "recovery/recovery_backend.h"
#include "sync/hybrid_guard.h"

#include "gtest/gtest_prod.h"
#include "tbb/concurrent_unordered_set.h"

#include <atomic>
#include <functional>
#include <unordered_set>
#include <utility>

namespace leanstore::buffer {
class BufferManager;
}

namespace leanstore::sync {
template <class PageClass>
class ExclusiveGuard;
}

namespace leanstore::recovery {

/* Forward declaration for testing */
class TestTreeRecovery;

class RecoveryManager {
 public:
  explicit RecoveryManager(buffer::BufferManager *buffer_pool);
  ~RecoveryManager() = default;

  /* Recovery utilities */
  void RecoveryPreparation();
  auto SignatureOfPrevSession() -> u32;

  /* Key APIs */
  auto MaterializeLogs() -> u64;
  void Analysis();
  void Redo();
  void Undo();

  /* Redo utilities */
  auto HasRecovered(pageid_t pid) -> bool;
  void PrepareRedoEnv();
  template <class BTreeNode>
  void PerPageRedo(sync::ExclusiveGuard<BTreeNode> &page, pageid_t pid);

 private:
  friend class TestTreeRecovery;
  FRIEND_TEST(TestRecovery, SmallChunks);
  FRIEND_TEST(TestRecovery, LargeChunk);
  FRIEND_TEST(TestTreeRecovery, BulkInsertRecovery);
  FRIEND_TEST(TestTreeRecovery, MixWorkloads);
  FRIEND_TEST(TestTreeRecovery, InstantRecovery);

  void AnalyzeLogSegment(const LogIOSegment::Buffer &tmp);
  template <class BTreeNode>
  void RedoLog(sync::ExclusiveGuard<BTreeNode> &page, const DataEntry *log);

  /* Page-based recovery component */
  buffer::BufferManager *buffer_;
  recovery::RecoveryBackend backend_;
  ComparisonLambda cmp_lambda_{ComparisonOperator::MEMCMP, std::memcmp};

  /* Whether a PID has already been recovered, only used for instant recovery */
  std::unique_ptr<std::atomic<bool>[]> has_recovered_;

  /* Analyzed log info */
  std::atomic<pageid_t> max_logged_pid_;
  std::atomic<u64> global_durable_gsn_[MAX_NUMBER_OF_WORKER];
  std::unordered_map<pageid_t, std::vector<LogIOSegment::Buffer>> page_log_[MAX_NUMBER_OF_WORKER];

  /* Loser & winner */
  tbb::concurrent_unordered_set<txnid_t> winners_;
  tbb::concurrent_unordered_set<txnid_t> losers_;
  std::unordered_set<txnid_t> tl_losers_[MAX_NUMBER_OF_WORKER];
  std::unordered_map<txnid_t, std::unordered_map<wid_t, timestamp_t>> tl_winners_[MAX_NUMBER_OF_WORKER];
};

}  // namespace leanstore::recovery