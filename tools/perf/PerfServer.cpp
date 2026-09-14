#include "PerfSuites.hpp"

#include <cstdio>

#include "PerfLink.hpp"

namespace perf {

int RunServer(const PerfOptions& options, PerfLink& link) {
  (void)options;
  (void)link;
  std::printf("transport_perf: 服务端骨架就位(控制信道与回显在后续里程碑接入)。\n");
  return 0;
}

}  // namespace perf
