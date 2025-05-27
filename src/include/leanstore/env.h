#pragma once

#include "common/typedefs.h"

#include <atomic>
#include <memory>

namespace leanstore {

namespace cloud {
class BookkeeperJNI;
}

enum LogServiceBackend : u8 { SSD = 0, SIMPLE_LOG_SERVICE = 1, BOOKKEEPER = 2 };

// Environment vars
extern std::string bk_classpath;
extern std::atomic<bool> start_profiling;
extern std::atomic<bool> start_profiling_latency;
extern std::atomic<u64> total_log_write;

}  // namespace leanstore
