#pragma once

#include "common/typedefs.h"
#include "recovery/log_entry.h"

#include <atomic>
#include <functional>
#include <queue>
#include <unordered_set>
#include <utility>

namespace leanstore::recovery {

struct LogIOSegment {
  u8 *buffer;  // the buffer used to store temp WAL entries before flushing them to the storage

  /**
   * @brief A byte buffer which stores a sorted (based on LSN/GSN) set of multiple log entries
   */
  struct Buffer {
    u8 *buffer         = nullptr;
    u64 size           = 0;
    bool stolen_buffer = false;

    Buffer() = default;
    explicit Buffer(u64 size);
    Buffer(u8 *stolen_buffer, u64 size);
    Buffer(Buffer &&obj) noexcept; /* Required for std::vector<Buffer> */
    ~Buffer();
    void Deallocate();

    void ResizeBuffer(u64 new_size);
    void InsertNewLog(const u8 *log_buffer, u64 log_size);
    auto IsEmpty() -> bool;
    auto Iterate(const std::function<bool(const LogEntry *)> &fn) const -> u64;
  };

  /* A tournament-tree-like data structure that generates the next smallest-GSN log entry from all `Buffer`s*/
  struct LazyGenerator {
    struct Offset {
      Buffer *log;
      u64 offset;

      explicit Offset(Buffer *log, u64 offset = 0);
      auto operator<=>(const Offset &other) const;
    };

    std::priority_queue<Offset> tournament;

    LazyGenerator() = default;
    void ReceiveInput(Buffer *w_segment);
    auto Iterate(const std::function<bool(const DataEntry *)> &fn) -> u64;
  };

  LogIOSegment();
  ~LogIOSegment();

  void Serialize(u64 log_size, const std::function<void(u8 *, u64 &)> &fill_fn,
                 const std::function<void(u8 *, u64)> &write_fn);
  static auto Deserialize(u8 *buffer, u64 end_offset, u32 required_signature,
                          const std::function<void(u8 *, u64)> &analyze_fn) -> u64;
};

}  // namespace leanstore::recovery