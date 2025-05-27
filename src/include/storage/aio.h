#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "storage/extent/large_page.h"
#include "storage/page.h"

#include "liburing.h"

namespace leanstore::storage {

class LibaioInterface {
 public:
  // For consumer SSDs, use this flags: IORING_SETUP_SINGLE_ISSUER
  // For enterprise SSDs, use this flags: IORING_SETUP_IOPOLL | IORING_SETUP_SINGLE_ISSUER
  static u32 uring_flags;

  LibaioInterface(int blockfd, Page *virtual_mem);
  ~LibaioInterface() = default;
  auto Ring() -> struct io_uring &;
  void WritePages(std::vector<pageid_t> &pages);
  void ReadLargePages(const LargePageList &large_pages);

 private:
  int blockfd_;
  Page *virtual_mem_;

  /* io_uring properties */
  struct io_uring ring_;
};

}  // namespace leanstore::storage