#include "leanstore/env.h"

namespace leanstore {

std::atomic<bool> start_profiling         = false;  // Whether the profiling thread is running
std::atomic<bool> start_profiling_latency = false;  // Whether the profiling thread is running
std::atomic<u64> total_log_write          = 0;

}  // namespace leanstore
