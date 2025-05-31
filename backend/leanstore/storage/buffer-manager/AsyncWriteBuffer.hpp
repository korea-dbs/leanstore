#pragma once
#include "BufferFrame.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <libaio.h>
#include <functional>
#include <list>
#include <unordered_map>
#include <fstream>
#include <mutex>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
// -------------------------------------------------------------------------------------
class AsyncWriteBuffer
{
  private:
   struct WriteCommand {
      BufferFrame* bf;
      PID pid;
   };
   io_context_t aio_context;
   int fd;
   u64 page_size, batch_max_size;
   u64 pending_requests = 0;
   // -------------------------------------------------------------------------------------
   struct IOTracing {
      struct IOTraceEvent{
         u64 timestamp;
         PID pid;
         DTID dt_id;
      };
      const u64 max_buffer_size = 10*1024;
      std::vector<IOTraceEvent> buffer;
      std::mutex mutex;
      IOTracing();
      void writeIOTrace();
   };
   IOTracing tracing;
   // -------------------------------------------------------------------------------------
  public:
   std::unique_ptr<BufferFrame::Page[]> write_buffer;
   std::unique_ptr<WriteCommand[]> write_buffer_commands;
   std::unique_ptr<struct iocb[]> iocbs;
   std::unique_ptr<struct iocb*[]> iocbs_ptr;
   std::unique_ptr<struct io_event[]> events;
   // -------------------------------------------------------------------------------------
   // Debug
   // -------------------------------------------------------------------------------------
   AsyncWriteBuffer(int fd, u64 page_size, u64 batch_max_size);
   // Caller takes care of sync
   bool full();
   void add(BufferFrame& bf, PID pid);
   u64 submit();
   u64 pollEventsSync();
   void getWrittenBfs(std::function<void(BufferFrame&, u64, PID)> callback, u64 n_events);
};
// -------------------------------------------------------------------------------------
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
