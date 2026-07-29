#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct SerialConfig {
  std::string device;
  uint32_t    baud_rate = 115200;
  uint8_t     data_bits = 8;
  uint8_t     stop_bits = 1;       // 1 或 2
  char        parity    = 'N';     // 'N'/'E'/'O'
};

}  // namespace transport
