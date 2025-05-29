#!/bin/bash
#set -e  # Exit immediately if a command exits with a non-zero status
set -x

# Compile the project once
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j LeanStore_YCSB
make -j LeanStore_TPCC

sanitize_nvme() {
    echo "sanitize"
    sudo nvme sanitize --sanact=2 "$FILENAME"
    sleep 30s
    sudo nvme sanitize-log "$FILENAME"
    sleep 5s
    sudo blkdiscard  -v "$FILENAME"
    sleep 20s
}

BENCHMARK_NAME=$1

run_benchmark() {
  local PREFIXPREFIX="$1"
  local BENCHMARK="$2"
  local TX_RATE="$3"
  local RUN_FOR_SECONDS="$4"
  local SANITIZE="$5"
  local ZIPF="$6"

	for fname in "${FILENAMES[@]}"; do
	  (
		 FILENAME="$fname"

		 if [ "$SANITIZE" = true ]; then
			 echo "Sanitizing $FILENAME"
			 sanitize_nvme
		 fi

		 # Resolve controller path for SMART monitoring
		 ns_path=$(readlink -f "$fname")
		 ns_dev=$(basename "$ns_path")
		 ctrlr_dev=${ns_dev%n*}
		 ctrlr_path="/dev/$ctrlr_dev"

		 PREFIX="${PREFIXPREFIX}_${BENCHMARK}_${TX_RATE}_${ctrlr_dev}"

		 echo "Let's go $FILENAME"

		 if [ "$BENCHMARK" = "tpcc" ]; then
			 exit -1
		 else
			 echo "Running YCSB"
			 ./benchmark/LeanStore_YCSB -worker_count=96 -worker_pin_thread=true -ycsb_record_count=3000000000 -ycsb_exec_seconds=$RUN_FOR_SECONDS -ycsb_read_ratio=50 -bm_virtual_gb=1000 -bm_physical_gb=64 -db_path="$fname" -txn_debug=true -txn_queue_size_mb=10 -txn_commit_variant=3 -wal_batch_write_kb=4 -txn_rate=$TX_RATE -blob_buffer_pool_gb=1 -ycsb_zipf_theta=$ZIPF | tee vmc_output_${PREFIX}.txt &
		 fi

		  tpcc_pid=$!

		 # Poll SMART data
		 while kill -0 "$tpcc_pid" 2>/dev/null; do
			{
			  echo
			  echo "=== $(date '+%Y-%m-%d %H:%M:%S') === ${fname} === ${ctrlr_path} === ${PREFIX} ==="
			  sudo nvme ocp smart-add-log -o json "$ctrlr_path"
			  sudo nvme smart-log -o json "$ctrlr_path"
			} >> "log_nvme_${PREFIX}.log"
			sleep 10
		 done

		 datafolder="v${PREFIX}"
		 mkdir "v${PREFIX}" 
		 cp lat_inc_wait_w*.csv $datafolder
		 cp "log_nvme_${PREFIX}.log" $datafolder
		 cp "vmc_output_${PREFIX}.txt" $datafolder
		 #python ../smartconvert.py build/log_nvme_t07_tpcc_15000_nvme11.log build/log_nvme_t07_tpcc_15000_nvme11.log.csv

		 wait "$tpcc_pid"
		 echo "[$fname] tpcc completed."
	  )  &
	done
	wait
}

FILENAMES=(/blk/s0)
run_benchmark "${BENCHMARK_NAME}" "ycsb" "1000000"      "23000" true "0.99"
FILENAMES=(/blk/m0)
run_benchmark "${BENCHMARK_NAME}" "ycsb" "1000000"      "23000" true "0.99"

exit 0
