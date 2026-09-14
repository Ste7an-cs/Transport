#include "PerfSuites.hpp"

#include <cstdio>

#include "PerfLink.hpp"

namespace perf {

int Run(const PerfOptions& options) {
  std::printf("transport_perf: role=%s medium=%s suite=%s\n",
              ToString(options.role), ToString(options.medium),
              ToString(options.suite));
  std::fflush(stdout);

  PerfLink link(options);
  auto started = link.Start();
  if (!started) {
    std::fprintf(stderr, "transport_perf: 链路装配失败:%s\n",
                 started.error().message().c_str());
    return 1;
  }

  const int rc = options.role == Role::kServer ? RunServer(options, link)
                                               : RunClient(options, link);
  link.Stop();
  return rc;
}

}  // namespace perf
