#pragma once

// -----------------------------------------------------------------------------
// TcpServerConfig.hpp — TCP 服务端配置
// 监听 bind_addr:port、最大客户端数，以及应用于每个 accepted 连接的可选分帧配置。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <optional>
#include <string>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

struct TcpServerConfig {
  std::string bind_addr   = "0.0.0.0";
  uint16_t    port        = 0;     // 0 = 由 OS 分配临时端口
  int         max_clients = 10;
  std::optional<LengthFieldFramerConfig> framer;  // 应用于每个 accepted 连接的接收侧
};

}  // namespace transport
