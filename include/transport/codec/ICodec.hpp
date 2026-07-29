#pragma once

// -----------------------------------------------------------------------------
// ICodec.hpp — 线缆格式扩展点(分帧 + 序列化 + 承载交互元数据)
// Encode: 一条 Message → 一段线缆字节(一对一)。
// Decode: 喂入字节切片 → 切出 0..N 条完整 Message。
// 实现可有状态(内部维护滚动缓冲,单线程喂,如 SystemCodec),
// 或无状态且支持并发 Decode(如 DdsCodec,被多个 DDS 监听线程并发喂)。
// 不依赖 Transport。
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/core/Message.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Result.hpp"

namespace transport {

class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发:一条消息 → 一段线缆字节。
  virtual Result<std::vector<uint8_t>> Encode(const Message& msg) = 0;

  // 收:喂入字节切片 → 切出 0..N 条完整消息(半包返回空、粘包返回多条)。
  // 解析错误按类别报 TransportErrc:分帧/长度/最大帧长 → kFrame;
  // codec 语义(坏判别符/字段越界)→ kCodec。
  virtual Result<std::vector<Message>> Decode(const uint8_t* data,
                                                    std::size_t len) = 0;
};

}  // namespace transport
