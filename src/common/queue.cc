#include "common/queue.h"
#include "common/rand.h"
#include "common/utils.h"
#include "sync/hybrid_guard.h"
#include "transaction/transaction.h"

#include <sys/mman.h>
#include <atomic>
#include <iostream>

namespace leanstore {

// ----------------------------------------------------------------------------------------------

template <typename T>
LockFreeQueue<T>::LockFreeQueue() : buffer_capacity_(FLAGS_txn_queue_size_mb * MB) {
  assert(std::is_trivially_destructible_v<T>);
  buffer_ = reinterpret_cast<u8 *>(AllocHuge(buffer_capacity_));
}

template <typename T>
LockFreeQueue<T>::~LockFreeQueue() {
  munmap(buffer_, buffer_capacity_);
}

/**
 * @brief Erase multiple items at once
 * `no_bytes` starting from `head_.load()` should perfectly store `n_items` queued items
 * i.e., you should call SizeApprox() -> LoopElement() -> Erase():
 * - SizeApprox(): Return the number of queued items at the moment
 * - LoopElement(): Loop through all these elements and return total number of bytes these items consume
 * - Erase(): Remove `n_items` from SizeApprox() and `no_bytes` from LoopElement()
 */
template <typename T>
void LockFreeQueue<T>::Erase(u64 no_bytes) {
  if (no_bytes <= 0) { return; }
  auto r_head = head_.load();

  if (r_head + no_bytes < buffer_capacity_) {
    r_head += no_bytes;
  } else {
    r_head = no_bytes - (buffer_capacity_ - r_head);
  }
  head_.store(r_head);
}

/**
 * @brief Return the byte offset at which the loop stops
 *
 * Users have to provide a `read_cb` to process with each loop item and
 *  return true/false whether the users want to continue the loop or not.
 * Note that, if the `read_cb` return false, that evaluated object will be re-evaluated in next iteration
 * The queue may also stop looping if it reaches the last queued item, i.e., r_head == until_tail
 */
template <typename T>
auto LockFreeQueue<T>::LoopElements(u64 until_tail, const std::function<bool(T &)> &read_cb) -> u64 {
  auto r_head   = head_.load();
  auto old_head = r_head;

  for (auto idx = 0UL; r_head != until_tail; idx++) {
    assert(r_head != until_tail);
    /* Circular back to the beginning of the buffer if deadend meet */
    if (T::InvalidByteBuffer(&buffer_[r_head])) { r_head = 0; }

    /* Read the queued item */
    const auto item = reinterpret_cast<T *>(&buffer_[r_head]);
    if (!read_cb(*item)) { break; }
    r_head += item->MemorySize();
  }

  return (r_head > old_head) ? r_head - old_head : r_head + buffer_capacity_ - old_head;
}

// ----------------------------------------------------------------------------------------------

template <class T>
auto ConcurrentQueue<T>::LoopElement(u64 no_elements, const std::function<bool(T &)> &fn) -> u64 {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  auto idx = 0UL;
  for (; idx < no_elements; idx++) {
    if (!fn(internal_[idx])) { break; }
  }
  return idx;
}

template <class T>
void ConcurrentQueue<T>::Push(T &element) {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  internal_.emplace_back(element);
}

template <class T>
auto ConcurrentQueue<T>::Erase(u64 no_elements) -> bool {
  sync::HybridGuard guard(&latch_, sync::GuardMode::EXCLUSIVE);
  if (internal_.size() < no_elements) { return false; }
  internal_.erase(internal_.begin(), internal_.begin() + no_elements);
  return true;
}

template <class T>
auto ConcurrentQueue<T>::SizeApprox() -> size_t {
  size_t ret = 0;

  while (true) {
    try {
      sync::HybridGuard guard(&latch_, sync::GuardMode::OPTIMISTIC);
      ret = internal_.size();
      break;
    } catch (const sync::RestartException &) {}
  }

  return ret;
}

// ----------------------------------------------------------------------------------------------

template class ConcurrentQueue<transaction::Transaction>;
template class LockFreeQueue<transaction::SerializableTransaction>;

}  // namespace leanstore