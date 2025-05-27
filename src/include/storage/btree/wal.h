#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "recovery/log_entry.h"
#include "storage/btree/node.h"

namespace leanstore::storage {

enum class WalType : u8 {
  INSERT        = 0,
  AFTER_IMAGE   = 1,
  DELTA_IMAGE   = 2,
  REMOVE        = 3,
  LOGICAL_SPLIT = 4,
  NEW_PAGE      = 6,
  FRESH_PAGE    = 7,
  NEW_ROOT      = 8,
  INSERT_SEP    = 9,
};

struct WALEntry : recovery::DataEntry {
  WalType wal_type : 4;
};

struct WALNewPage : WALEntry {
  leng_t metadata_size;
  leng_t payload_size;
  u8 payload[];
};

struct WALFreshPage : WALEntry {
  bool is_inner : 1;
  pageid_t right_most_child : 63;  // only used if inner node
};

struct WALNewRoot : WALEntry {
  leng_t slot_id;
  pageid_t new_root_pid;
};

struct WALLogicalSplit : WALEntry {
  pageid_t next_leaf_node;
  SeparatorInfo sep;
};

struct WALAfterImage : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

struct WALDeltaImage : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

struct WALInsert : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

struct WALInsertSep : WALEntry {
  pageid_t left_pid;
  pageid_t right_pid;
  u16 sep_length;
  u8 sep_key[];
};

struct WALRemove : WALEntry {
  leng_t slot_id;
};

// --------------------------------------------------------------------------
// WAL macros to shorten WAL impl

template <typename PageGuard, typename WalType>
void GenerateWAL(PageGuard &node, std::span<u8> key, std::span<const u8> payload) {
  auto &entry        = node.template PrepareWalEntry<WalType>(key.size() + payload.size());
  entry.key_length   = key.size();
  entry.value_length = payload.size();
  std::memcpy(entry.payload, key.data(), key.size());
  std::memcpy(entry.payload + key.size(), payload.data(), payload.size());
  node.SubmitActiveWalEntry();
}

template <typename PageGuard>
void GenerateWALNewRoot(PageGuard &meta, u64 slot_id, pageid_t new_root_pid) {
  auto &log        = meta.template PrepareWalEntry<WALNewRoot>(0);
  log.slot_id      = slot_id;
  log.new_root_pid = new_root_pid;
  meta.SubmitActiveWalEntry();
}

template <typename PageGuard>
void GenerateWALNewPage(PageGuard &page) {
  auto total_log_sz   = PAGE_SIZE - page->FreeSpaceAfterCompaction();
  auto &entry         = page.template PrepareWalEntry<WALNewPage>(total_log_sz);
  entry.payload_size  = page->header.space_used;
  entry.metadata_size = total_log_sz - entry.payload_size;
  std::memcpy(entry.payload, page.Ptr(), entry.metadata_size);
  std::memcpy(entry.payload + entry.metadata_size, reinterpret_cast<u8 *>(page.Ptr()) + PAGE_SIZE - entry.payload_size,
              entry.payload_size);
  page.SubmitActiveWalEntry();
}

template <typename PageGuard>
void GenerateWALFreshPage(PageGuard &page, bool is_leaf, pageid_t right_most_child = 0) {
  auto &entry    = page.template PrepareWalEntry<WALFreshPage>(0);
  entry.is_inner = !is_leaf;
  if (!is_leaf) { entry.right_most_child = right_most_child; }
  page.SubmitActiveWalEntry();
}

}  // namespace leanstore::storage