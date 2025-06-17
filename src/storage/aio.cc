#include "storage/aio.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace leanstore::storage {

u32 LibaioInterface::uring_flags = IORING_SETUP_SINGLE_ISSUER;

LibaioInterface::LibaioInterface(int blockfd, Page *virtual_mem) : blockfd_(blockfd), virtual_mem_(virtual_mem) {
  if (FLAGS_uring_iopool) { uring_flags |= IORING_SETUP_IOPOLL; }
}

auto LibaioInterface::Ring() -> struct io_uring & { return ring_; }

void LibaioInterface::WritePages(std::vector<pageid_t> &pages) {
  u32 submit_cnt = 0;

  for (auto pid : pages) {
    auto sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      UringSubmit(&ring_, submit_cnt);
      sqe = io_uring_get_sqe(&ring_);
      Ensure(sqe != nullptr);
      submit_cnt = 0;
    }
    submit_cnt++;
    io_uring_prep_write(sqe, blockfd_, &virtual_mem_[pid], PAGE_SIZE, PAGE_SIZE * pid);
  }
  UringSubmit(&ring_, submit_cnt);
}

/**
 * @brief Read a list of huge pages
 */
void LibaioInterface::ReadLargePages(const LargePageList &large_pages) {
  u32 submit_cnt = 0;
  for (auto &large_pg : large_pages) {
    auto sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      UringSubmit(&ring_, submit_cnt);
      sqe = io_uring_get_sqe(&ring_);
      Ensure(sqe != nullptr);
      submit_cnt = 0;
    }
    submit_cnt++;
    io_uring_prep_read(sqe, blockfd_, &virtual_mem_[large_pg.start_pid], PAGE_SIZE * large_pg.page_cnt,
                       PAGE_SIZE * large_pg.start_pid);
  }
  UringSubmit(&ring_, submit_cnt);
}

}  // namespace leanstore::storage