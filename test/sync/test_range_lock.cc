#include "leanstore/leanstore.h"
#include "sync/range_lock.h"

#include "gtest/gtest.h"
#include "range_lock/range_lock.h"

namespace leanstore::sync {

static constexpr int NO_THREADS    = 50;
static constexpr int NO_OPERATIONS = 10000;

TEST(TestRangeLock, Simple) {
  LeanStore::worker_thread_id = 0;

  RangeLock lock(1);
  EXPECT_TRUE(lock.TryLockRange(101, 50));
  EXPECT_FALSE(lock.TryLockRange(100, 2));
  EXPECT_TRUE(lock.TryLockRange(100, 1));
  EXPECT_FALSE(lock.TryLockRange(50, 100));
  EXPECT_FALSE(lock.TryLockRange(50, 200));
  EXPECT_FALSE(lock.TryLockRange(150, 50));
  EXPECT_FALSE(lock.TestRange(120, 10));
  lock.UnlockRange(101);
  EXPECT_FALSE(lock.TestRange(100, 1));
  EXPECT_FALSE(lock.TestRange(90, 20));
  EXPECT_TRUE(lock.TestRange(101, 10));
}

TEST(TestConcurrentRangeLock, DISABLED_Simple) {
  LeanStore::worker_thread_id = 0;

  ConcurrentRangeLock<u64, 20> lock;
  EXPECT_TRUE(lock.tryLock(101, 50));
  EXPECT_FALSE(lock.tryLock(100, 2));
  EXPECT_FALSE(lock.tryLock(101, 50));
  EXPECT_TRUE(lock.tryLock(100, 1));
  EXPECT_FALSE(lock.tryLock(50, 100));
  EXPECT_FALSE(lock.tryLock(50, 200));
  EXPECT_FALSE(lock.tryLock(150, 50));
  EXPECT_FALSE(lock.tryLock(120, 10));
  lock.releaseLock(101, 50);
  EXPECT_FALSE(lock.tryLock(100, 1));
  EXPECT_FALSE(lock.tryLock(90, 20));
  EXPECT_TRUE(lock.tryLock(101, 10));
}

TEST(TestRangeLock, Concurrency) {
  RangeLock lock(NO_THREADS);
  std::thread threads[NO_THREADS];

  // NOLINTNEXTLINE
  for (int idx = 0; idx < NO_THREADS; idx++) {
    threads[idx] = std::thread([&, t_id = idx]() {
      LeanStore::worker_thread_id = t_id;

      for (auto i = 0; i < NO_OPERATIONS; i++) {
        EXPECT_TRUE(lock.TryLockRange(t_id * 100 + 1, 100));
        EXPECT_FALSE(lock.TestRange(t_id * 100 + 1, 100));
        lock.UnlockRange(t_id * 100 + 1);
      }
    });
  }

  for (auto &thread : threads) { thread.join(); }
}

TEST(TestConcurrentRangeLock, DISABLED_Concurrency) {
  ConcurrentRangeLock<u64, 20> lock;
  std::thread threads[NO_THREADS];

  // NOLINTNEXTLINE
  for (int idx = 0; idx < NO_THREADS; idx++) {
    threads[idx] = std::thread([&, t_id = idx]() {
      for (auto i = 0; i < NO_OPERATIONS; i++) {
        EXPECT_TRUE(lock.tryLock(t_id * 100 + 1, 100));
        EXPECT_FALSE(lock.tryLock(t_id * 100 + 1, 100));
        lock.releaseLock(t_id * 100 + 1, 100);
      }
    });
  }

  for (auto &thread : threads) { thread.join(); }
}

}  // namespace leanstore::sync