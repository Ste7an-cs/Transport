#pragma once

// -----------------------------------------------------------------------------
// UdpConfig.hpp — UDP 配置（UdpMode + UdpConfig）
// 单播/组播/广播三模式；本地绑定、默认发送目的地、组播组与 TTL。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>

namespace transport {

enum class UdpMode { kUnicast, kMulticast, kBroadcast };

struct UdpConfig {
  UdpMode     mode            = UdpMode::kUnicast;
  std::string local_addr      = "0.0.0.0";
  uint16_t    local_port      = 0;     // 0 = 由 OS 分配临时端口
  std::string remote_addr;             // Send() 默认目的地（单播/广播）
  uint16_t    remote_port     = 0;
  std::string multicast_group;         // 仅 kMulticast：Send() 默认目的地
  uint8_t     ttl             = 1;     // 组播 TTL（hops）
};

}  // namespace transport
