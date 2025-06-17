#pragma once

#include "common/delta.h"
#include "common/typedefs.h"

#include <functional>
#include <span>

namespace leanstore {

// All functions should be idempotent + have no side-effect
using AccessPayloadFunc = std::function<void(std::span<const u8>)>;
using ModifyPayloadFunc = std::function<void(std::span<u8>)>;
using AccessRecordFunc  = std::function<bool(std::span<u8>, std::span<u8>)>;

enum class ComparisonOperator : u8 {
  MEMCMP,
  BLOB_HANDLER,
  BLOB_LOOKUP,
};

struct ComparisonLambda {
  ComparisonOperator op;
  std::function<int(const void *, const void *, size_t)> func;
};

class KVInterface {
 public:
  virtual ~KVInterface() = default;

  // Insertion should be append-biases or not
  virtual void SetComparisonOperator(ComparisonLambda cmp) = 0;

  // -------------------------------------------------------------------------------------
  virtual auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> bool                            = 0;
  virtual void Insert(std::span<u8> key, std::span<const u8> payload)                                         = 0;
  virtual auto Remove(std::span<u8> key) -> bool                                                              = 0;
  virtual auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> bool  = 0;
  virtual auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> bool = 0;
  virtual void ScanAscending(std::span<u8> key, const AccessRecordFunc &fn)                                   = 0;
  virtual void ScanDescending(std::span<u8> key, const AccessRecordFunc &fn)                                  = 0;
  virtual auto CountEntries() -> u64                                                                          = 0;
  virtual auto SizeInMB() -> float                                                                            = 0;

  // -------------------------------------------------------------------------------------
  virtual auto LookUpBlob(std::span<const u8> blob_payload, const ComparisonLambda &cmp,
                          const AccessPayloadFunc &read_cb) -> bool = 0;
};

}  // namespace leanstore
