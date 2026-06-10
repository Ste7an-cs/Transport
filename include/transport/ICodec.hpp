#pragma once

// -----------------------------------------------------------------------------
// ICodec.hpp — 编解码扩展点（接口，用户实现）
// 框架在发送前(Encode)/接收后(Decode)自动调用，用于组装/解析协议帧。
// 未设置时字节原样透传；TransportCore 以 shared_ptr<ICodec> 持有。
// -----------------------------------------------------------------------------

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
