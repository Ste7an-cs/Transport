#pragma once

// -----------------------------------------------------------------------------
// TcpClientConfig.hpp — TCP 客户端配置
// 目标 host:port、连接超时、是否自动重连，以及可选的接收侧分帧配置(framer)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <optional>
#include <string>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

struct TcpClientConfig {
  std::string host;
  uint16_t    port               = 0;
  uint32_t    connect_timeout_ms = 5000;
  bool        auto_reconnect     = true;
  std::optional<LengthFieldFramerConfig> framer;  // 不设则接收为透传模式
  bool enable_topic_routing = false;  // 开启 topic→codec 多路复用(in-band envelope)
};

}  // namespace transport
