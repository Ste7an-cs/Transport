#pragma once

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
};

}  // namespace transport
