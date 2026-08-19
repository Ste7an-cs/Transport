#pragma once

// -----------------------------------------------------------------------------
// UdpConfig.hpp — UDP 配置（UdpMode + UdpConfig）
// 单播/组播/广播三模式；本地绑定、默认发送目的地、组播组与 TTL。
// -----------------------------------------------------------------------------

#include <chrono>
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

  // 读超时（静默/心跳超时）：读等待超过本时长仍无任何报文，即判链路已坏 —— 泵解绑并
  // 重建 socket。UDP 无连接、无对端断开事件，"链路坏了"没有别的可观测信号，故这是唯一的
  // 主动判据。
  //
  // **同时是 bind 失败后的重试间隔**：两者是同一个"多久算不对劲"的量，不设两个旋钮。
  // 因此本项**不接受 0**（那会让 bind 失败的退避退化为不带间隔的紧转）；非正值一律按默认
  // 5s 处理，见 UdpTransport.cpp 的 EffectiveTimeout()。
  std::chrono::milliseconds silence_timeout{5000};
};

}  // namespace transport
