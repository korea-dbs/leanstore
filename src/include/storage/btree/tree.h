#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "leanstore/kv_interface.h"
#include "storage/btree/node.h"
#include "storage/btree/wal.h"
#include "storage/page.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/shared_guard.h"

#include <cstring>
#include <functional>
#include <span>
#include <utility>

namespace leanstore::storage {

class BTree : public KVInterface {
 public:
  BTree(buffer::BufferManager *buffer_pool, recovery::RecoveryManager *recovery, u32 tree_slot);
  ~BTree() override = default;

  /* BTree config*/
  void SetComparisonOperator(ComparisonLambda cmp) override;

  /* All BTree operators */
  // -------------------------------------------------------------------------------------
  /* Public APIs for external use */
  auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> bool override;
  void Insert(std::span<u8> key, std::span<const u8> payload) override;
  auto Remove(std::span<u8> key) -> bool override;
  auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> bool override;
  auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> bool override;
  void ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) override;
  void ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) override;
  auto CountEntries() -> u64 override;
  auto SizeInMB() -> float override;
  auto LookUpBlob(std::span<const u8> blob_key, const ComparisonLambda &cmp, const AccessPayloadFunc &read_cb)
    -> bool override;

  // -------------------------------------------------------------------------------------
  /* APIs for use within LeanStore */
  auto IsNotEmpty() -> bool;
  auto CountPages() -> u64;

 private:
  /* Iterate all pages utilities */
  auto IterateAllNodes(sync::OptimisticGuard<BTreeNode> &node, const std::function<u64(BTreeNode &)> &inner_fn,
                       const std::function<u64(BTreeNode &)> &leaf_fn) -> u64;
  auto IterateUntils(sync::OptimisticGuard<BTreeNode> &node, const std::function<bool(BTreeNode &)> &inner_fn,
                     const std::function<bool(BTreeNode &)> &leaf_fn) -> bool;

  /* Find Leaf Node storing the key */
  auto FindLeafOptimistic(std::span<u8> key) -> sync::OptimisticGuard<BTreeNode>;
  auto FindLeafShared(std::span<u8> key) -> sync::SharedGuard<BTreeNode>;

  /* Split/Merge utilities */
  void TrySplit(sync::ExclusiveGuard<BTreeNode> &&parent, sync::ExclusiveGuard<BTreeNode> &&node);
  void EnsureSpaceForSplit(BTreeNode *to_split, std::span<u8> key);
  void TryMerge(sync::ExclusiveGuard<BTreeNode> &&parent, sync::ExclusiveGuard<BTreeNode> &&left,
                sync::ExclusiveGuard<BTreeNode> &&right, leng_t left_pos);
  void EnsureUnderfullInnersForMerge(BTreeNode *to_merge);

  /* Instant recovery */
  inline void InstantRecovery(pageid_t pid) {
    if (FLAGS_wal_enable_recovery && !recovery_->HasRecovered(pid)) [[unlikely]] {
      sync::ExclusiveGuard<BTreeNode> page(buffer_, pid);
      // While waiting for the page latch, the page may already be recovered by another worker
      if (!recovery_->HasRecovered(pid)) { recovery_->PerPageRedo(page, pid); }
    }
  }

  /* Access record utility for scan */
  template <typename PageGuard>
  inline auto AccessRecord(PageGuard &node, u64 pos, const AccessRecordFunc &fn) -> bool {
    u64 key_len = node->header.prefix_len + node->slots[pos].key_length;
    u8 key[key_len];
    std::memcpy(key, node->GetPrefix(), node->header.prefix_len);
    std::memcpy(key + node->header.prefix_len, node->GetKey(pos), node->slots[pos].key_length);
    return fn({key, key_len}, node->GetPayload(pos));
  }

  /* Core properties */
  buffer::BufferManager *buffer_;
  recovery::RecoveryManager *recovery_;
  leng_t metadata_slotid_;

  /* Comparison properties */
  ComparisonLambda cmp_lambda_{ComparisonOperator::MEMCMP, std::memcmp};
};

}  // namespace leanstore::storage