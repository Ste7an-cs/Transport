#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct TcpClientConfig {
  std::string host = "127.0.0.1";
  uint16_t    port = 0;
  uint32_t    connect_timeout_ms = 5000;
  bool        auto_reconnect = false;
};

}  // namespace transport
