#pragma once
#include <cstdint>
#include "ProfilingTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/utils/Hist.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
using namespace storage;
class BMTable : public ProfilingTable
{
  private:
   BufferManager& bm;
   s64 local_phase_1_ms = 0, local_phase_2_ms = 0, local_phase_3_ms = 0, local_poll_ms = 0, total;
   u64 local_total_free, local_total_cool;
   Hist<int, uint64_t> ioReadHist;
   Hist<int, uint64_t> ioWriteHist;
   Hist<int, uint64_t> txHist;
   Hist<int, uint64_t> txIncWaitHist;
  public:
   BMTable(BufferManager& bm);
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
