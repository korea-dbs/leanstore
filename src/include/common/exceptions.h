#pragma once

#include "cpptrace/cpptrace.hpp"
#include "spdlog/spdlog.h"

#include <cassert>
#include <exception>

#define GENERIC_EXCEPTION(name)                                                                                    \
  struct name : public std::exception {                                                                            \
    const std::string msg;                                                                                         \
    explicit name() : msg(#name) { spdlog::error("Exception: {}\n", #name); }                                      \
    explicit name(const std::string &msg) : msg(msg) { spdlog::error("Exception: {}({})\n", #name, msg.c_str()); } \
    ~name() = default;                                                                                             \
  };

namespace leanstore::ex {

GENERIC_EXCEPTION(GenericException);
GENERIC_EXCEPTION(EnsureFailed);
GENERIC_EXCEPTION(Unreachable);
GENERIC_EXCEPTION(TODO);

}  // namespace leanstore::ex

namespace leanstore::sync {

class RestartException {
 public:
  RestartException() = default;
};

}  // namespace leanstore::sync

// Support Macros
#define UnreachableCode() \
  throw leanstore::ex::Unreachable(std::string(__FILE__) + ":" + std::string(std::to_string(__LINE__)));
#define AlwaysCheck(e)                                                                               \
  (__builtin_expect(!(e), 0) ? ({                                                                    \
    cpptrace::generate_trace().print();                                                              \
    throw leanstore::ex::EnsureFailed(std::string(__func__) + " in " + std::string(__FILE__) + "@" + \
                                      std::to_string(__LINE__) + " msg: " + std::string(#e));        \
  })                                                                                                 \
                             : (void)0)

#ifdef DEBUG
#define Ensure(e) assert(e);
#else
#define Ensure(e) AlwaysCheck(e)
#endif

#define PosixCheck(expr) \
  ({                     \
    if (!(expr)) {       \
      perror(#expr);     \
      raise(SIGTRAP);    \
    }                    \
  })
