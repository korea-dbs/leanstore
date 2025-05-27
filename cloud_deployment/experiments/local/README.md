# LeanStore on cloud log service

## Bookkeeper

```bash
./benchmark/LeanStore_YCSB -worker_count=1 -worker_pin_thread=true -ycsb_record_count=100000 -ycsb_exec_seconds=5 -ycsb_read_ratio=50 -bm_virtual_gb=128 -bm_physical_gb=32 -db_path=/dev/s0 -txn_debug=true -txn_commit_variant=3 -wal_batch_write_kb=4 -wal_log_backend=2 -wal_stealing_group_size=1 -txn_commit_group_size=1 -wal_enable_async={false, true}
```

*Note*: Only when `-wal_enable_async=false`, we can retrieve average I/O latency per log write