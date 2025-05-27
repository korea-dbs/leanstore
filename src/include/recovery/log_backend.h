#pragma once

#include "common/typedefs.h"
#include "common/utils.h"

#include "gtest/gtest_prod.h"

namespace leanstore::recovery {

class LogManager;

struct AutonomousLogBlock {
  u64 start_offset{0};
  u64 end_offset{0};
};

class LogBackend {
 public:
  static constexpr u8 LOGGING_AIO_QD = 8;

  explicit LogBackend(LogManager *log_manager);
  void Connect();

  /* Sync life-cycle */
  void SyncAppend(u8 *buffer, u64 size, const std::function<void()> &async_fn = {});

 private:
  friend class LogManager;

  LogManager *manager_;

  /* Properties for SSD log backend */
  struct io_uring ring_;
  AutonomousLogBlock log_block_;

  /* Helpers for SSD log backend */
  auto LogFlushOffset(u64 chunk_size) -> u64;
};

}  // namespace leanstore::recovery