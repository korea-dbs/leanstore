#pragma once

#include "gflags/gflags.h"
#include "share_headers/config.h"

DECLARE_bool(uring_iopool);
DECLARE_string(exmap_path);
// -------------------------------------------------------------------------------------
DECLARE_uint32(worker_count);
DECLARE_uint32(page_provider_thread);
DECLARE_bool(worker_pin_thread);
DECLARE_uint32(txn_rate);
// -------------------------------------------------------------------------------------
DECLARE_uint64(bm_virtual_gb);
DECLARE_uint64(bm_alias_block_mb);
DECLARE_uint64(bm_evict_batch_size);
DECLARE_bool(bm_enable_fair_eviction);
// -------------------------------------------------------------------------------------
DECLARE_bool(wal_enable);
DECLARE_uint32(wal_variant);
DECLARE_bool(wal_debug);
DECLARE_bool(wal_fsync);
DECLARE_uint64(wal_buffer_size_mb);
DECLARE_uint32(wal_batch_write_kb);
DECLARE_uint32(wal_block_size_mb);
DECLARE_uint32(wal_max_idle_time_us);
DECLARE_uint32(wal_stealing_group_size);
DECLARE_bool(wal_enable_recovery);
DECLARE_bool(wal_instant_recovery);
DECLARE_uint32(wal_recovery_threads);
// -------------------------------------------------------------------------------------
DECLARE_bool(txn_debug);
DECLARE_int32(txn_commit_variant);
DECLARE_uint32(txn_commit_group_size);
DECLARE_uint32(txn_queue_size_mb);
// -----------------------------------------------------------------------------------
DECLARE_bool(blob_enable);
DECLARE_bool(blob_tail_extent);
DECLARE_bool(blob_normal_buffer_pool);
DECLARE_uint64(blob_buffer_pool_gb);