#!/bin/bash
#set -e  # Exit immediately if a command exits with a non-zero status
set -x

# Compile the project once
cmake -DCMAKE_BUILD_TYPE=Release ..
make tpcc

DEVICE='/blk/k3'
# Array of physical GB sizes to test
#physical_sizes=(64 32 16 8 4 2 1)
physical_sizes=(32)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Loop over each physical size
for phys in "${physical_sizes[@]}"; do
    # Create a unique trace file name for this run
    echo "Running benchmark with bm_physical_gb=${phys}"
   
    "${SCRIPT_DIR}/trace-io.sh" > "traceio_output_${phys}.log" &
    TRACE_IO_PID=$!

	 ./frontend/tpcc --ssd_path=${DEVICE} --partition_bits=12 --tpcc_warehouse_count=1000 --run_for_seconds=10000 -print_tx_console --dram_gib=${phys} --ssd_gib=3000 --wal_pwrite=true --free_pct=1 --xmerge=1 --contention_split=1 --worker_threads=32 --pp_threads=8 --tag=tpcc # --io_trace --io_trace_file="iotrace_${phys}.csv"

	 kill $TRACE_IO_PID
    wait $TRACE_IO_PID 2>/dev/null


    echo "Completed run with bm_physical_gb=${phys}"
done

