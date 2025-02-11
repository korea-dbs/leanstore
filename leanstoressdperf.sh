#!/bin/bash
#set -e  # Exit immediately if a command exits with a non-zero status
set -x

# Compile the project once
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j tpcc
make -j ycsb

sanitize_nvme() {
    echo "sanitize"
    sudo nvme sanitize --sanact=2 "$FILENAME"
    sleep 2m
    sudo nvme sanitize-log "$FILENAME"
    sleep 10s
    sudo blkdiscard  -v "$FILENAME"
    sleep 1m
}

#FILENAMES=(/blk/m0 /blk/h0 /blk/s0 /blk/wd)
#FILENAMES=(/blk/s0 /blk/m0)
#FILENAMES=(/blk/w0)
FILENAMES=(/blk/m0)
#FILENAMES=(/blk/s0)

BENCHMARK_NAME=$1

	# 6500 leads to 905 GB on SSD, but also an I/O error after 12k seconds
	# 6000 leads to              , but also an I/O error after 18k seconds
	# 5400
	# use 6550 !

run_benchmark() {
  local PREFIXPREFIX="$1"
  local BENCHMARK="$2"
  local TX_RATE="$3"
  local RUN_FOR_SECONDS="$4"
  local SANITIZE="$5"

	for fname in "${FILENAMES[@]}"; do
	  (
		 FILENAME="$fname"

		 if [ "$SANITIZE" = true ]; then
			 sanitize_nvme
		 fi

		 # Resolve controller path for SMART monitoring
		 ns_path=$(readlink -f "$fname")
		 ns_dev=$(basename "$ns_path")
		 ctrlr_dev=${ns_dev%n*}
		 ctrlr_path="/dev/$ctrlr_dev"

		 PREFIX="${PREFIXPREFIX}_${BENCHMARK}_${TX_RATE}_${ctrlr_dev}"

		 if [ "$BENCHMARK" = "tpcc" ]; then
			 ./frontend/tpcc --ssd_path="$fname" --partition_bits=12 --tpcc_warehouse_count=6550 --run_for_seconds=$RUN_FOR_SECONDS --print_tx_console --dram_gib=64 --ssd_gib=960 --free_pct=1 --xmerge=1 --contention_split=1 --worker_threads=64 --pp_threads=8 \
				 --tx_rate=$TX_RATE \
				 --steady_tpcc \
				 --csv_path="./log_${PREFIX}" \
				 --pin_threads \
				 --tag="${PREFIX}" &

		 else 
			 # 600 => 888 GB on SSD
			 # 500 => 883 on SSD / 817 leanstore
			 ./frontend/ycsb --ssd_path="$fname" --partition_bits=12 --target_gib=500 --ycsb_read_ratio=50 --run_for_seconds=$RUN_FOR_SECONDS --print_tx_console --dram_gib=64 --ssd_gib=960 --free_pct=1 --xmerge=1 --contention_split=1 --worker_threads=64 --pp_threads=8 \
				 --zipf_factor=0.99 \
				 --pin_threads \
				 --tx_rate=$TX_RATE \
				 --csv_path="./log_${PREFIX}" \
				 --tag="${PREFIX}" &
		 fi

		  tpcc_pid=$!

		 # Poll SMART data while tpcc is running
		 while kill -0 "$tpcc_pid" 2>/dev/null; do
			{
			  echo
			  echo "=== $(date '+%Y-%m-%d %H:%M:%S') === ${fname} === ${ctrlr_path} === ${PREFIX} ==="
			  sudo nvme ocp smart-add-log -o json "$ctrlr_path"
			  sudo nvme smart-log -o json "$ctrlr_path"
			} >> "log_nvme_${PREFIX}.log"
			sleep 10
		 done

		 wait "$tpcc_pid"
		 echo "[$fname] tpcc completed."
	  )  &
	done
	wait
}

FILENAMES=(/blk/s0)
#                                        rate    time
run_benchmark "${BENCHMARK_NAME}" "tpcc" "0"     "12000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "20000"   "12000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "17500"   "13000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "15000" "14000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "10000" "16000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "05000" "18000" true

FILENAMES=(/blk/m0)
#                                        rate    time
run_benchmark "${BENCHMARK_NAME}" "tpcc" "0"     "12000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "15000" "14000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "12500" "15000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "10000" "16000" true
run_benchmark "${BENCHMARK_NAME}" "tpcc" "05000" "18000" true
