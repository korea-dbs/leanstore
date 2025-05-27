#include "common/worker_pool.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/leanstore.h"

#include <cassert>

namespace leanstore {

WorkerPool::WorkerPool(std::atomic<bool> &keep_running, const std::function<void()> &worker_env_init)
    : is_running_(&keep_running) {
  assert(FLAGS_worker_count < MAX_NUMBER_OF_WORKER);
  for (auto t_i = 0UL; t_i < FLAGS_worker_count; t_i++) {
    workers_.emplace_back([&, t_i]() {
      if (FLAGS_worker_pin_thread) { PinThisThread(t_i); }
      pthread_setname_np(pthread_self(), "worker");
      LeanStore::worker_thread_id = t_i;
      worker_env_init();
      auto &meta = worker_env_[t_i];
      threads_counter_++;
      while (is_running_->load()) {
        std::unique_lock guard(meta.mutex);
        meta.cv.wait(guard, [&]() { return !is_running_->load() || meta.job_set; });
        if (!is_running_->load()) { break; }
        meta.wt_ready = false;
        meta.job();
        meta.wt_ready = true;
        meta.job_done = true;
        meta.job_set  = false;
        meta.cv.notify_one();
      }
      threads_counter_--;
    });
  }
  for (auto &t : workers_) { t.detach(); }
  while (threads_counter_ < FLAGS_worker_count) { AsmYield(); }
}

WorkerPool::~WorkerPool() { Stop(); }

void WorkerPool::Stop() {
  if (is_running_->load()) {
    is_running_->store(false, std::memory_order_release);
    for (auto t_i = 0UL; t_i < FLAGS_worker_count; t_i++) { worker_env_[t_i].cv.notify_one(); }
    JoinAll();
    while (threads_counter_ > 0) { AsmYield(); }
  }
}

// -------------------------------------------------------------------------------------
void WorkerPool::ScheduleAsyncJob(wid_t wid, const std::function<void()> &job) { SetJob(wid, job); }

void WorkerPool::ScheduleSyncJob(wid_t wid, const std::function<void()> &job) {
  SetJob(wid, job);
  JoinWorker(wid, [&](WorkerThreadEnv &meta) { return meta.job_done; });
}

// -------------------------------------------------------------------------------------
void WorkerPool::JoinAll() {
  for (auto t_i = 0UL; t_i < FLAGS_worker_count; t_i++) { JoinWorker(t_i); }
}

void WorkerPool::JoinWorker(wid_t wid) {
  JoinWorker(wid, [&](WorkerThreadEnv &meta) { return meta.wt_ready && !meta.job_set; });
}

void WorkerPool::SetJob(wid_t wid, const std::function<void()> &job) {
  Ensure(wid < FLAGS_worker_count);
  auto &meta = worker_env_[wid];
  std::unique_lock guard(meta.mutex);
  meta.cv.wait(guard, [&]() { return !meta.job_set && meta.wt_ready; });
  meta.job_set  = true;
  meta.job_done = false;
  meta.job      = job;
  guard.unlock();
  meta.cv.notify_all();
}

void WorkerPool::JoinWorker(wid_t wid, std::function<bool(WorkerThreadEnv &)> condition) {
  Ensure(wid < FLAGS_worker_count);
  auto &meta = worker_env_[wid];
  std::unique_lock guard(meta.mutex);
  meta.cv.wait(guard, [&]() { return condition(meta); });
}

}  // namespace leanstore