#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct TcpServerConfig {
  std::string bind_addr = "0.0.0.0";
  uint16_t    port = 0;     // 0 = OS 分配(LocalPort() 取回)
  int         backlog = 0;  // <=0 → Qt 默认(setMaxPendingConnections 默认 30)
};

}  // namespace transport
