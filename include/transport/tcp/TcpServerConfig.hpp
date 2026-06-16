#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct TcpServerConfig {
  std::string bind_addr = "0.0.0.0";
  uint16_t    port = 0;     // 0 = OS 分配(LocalPort() 取回)
  int         backlog = 0;  // <=0 → 用 asio 默认 max_listen_connections
};

}  // namespace transport
