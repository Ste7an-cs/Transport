#pragma once

#include <cstdint>
#include <vector>

#include "transport/Result.hpp"

namespace transport {

// 发送/接收边界由框架自动调用；未设置时原始字节直接透传。
class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发送前调用：将数据编码为字节流（用户在此组装 header + body）
  virtual Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& data) = 0;

  // 接收后调用：将一帧完整字节流解码
  virtual Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& data) = 0;
};

}  // namespace transport
