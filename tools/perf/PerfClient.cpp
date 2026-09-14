#include "PerfSuites.hpp"

#include <cstdio>

#include "PerfLink.hpp"

namespace perf {

int RunClient(const PerfOptions& options, PerfLink& link) {
  (void)options;
  (void)link;
  std::printf("transport_perf: 客户端骨架就位(两套 suite 在后续里程碑接入)。\n");
  return 0;
}

}  // namespace perf
