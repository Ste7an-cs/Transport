#pragma once

// -----------------------------------------------------------------------------
// IUdpTransport.hpp — UDP 扩展接口（ITransport + 运行期指定目的地）
// 在 ITransport 基础上加 SendTo（发往运行期 ip:port，忽略 config 默认 remote）。
// 实现见 UdpImpl。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"

namespace transport {

class IUdpTransport : public ITransport {
 public:
  // 发往运行期指定的目的地；忽略 config 的默认 remote。
  virtual Status SendTo(const std::vector<uint8_t>& data,
                        const std::string& ip, uint16_t port) = 0;
};

}  // namespace transport
