#pragma once

// -----------------------------------------------------------------------------
// SerialConfig.hpp — 串口配置
// device 路径 + 波特率/数据位/停止位/校验位，以及可选的接收侧分帧配置(framer)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <optional>
#include <string>

#include "transport/framing/LengthFieldFramer.hpp"

namespace transport {

struct SerialConfig {
  std::string device;              // 例如 "/dev/ttyS0"
  uint32_t    baud_rate  = 115200;
  uint8_t     data_bits  = 8;
  uint8_t     stop_bits  = 1;      // 1 或 2
  char        parity     = 'N';    // 'N'（无）/ 'E'（偶）/ 'O'（奇）
  std::optional<LengthFieldFramerConfig> framer;  // 不设则接收为透传模式
};

}  // namespace transport
