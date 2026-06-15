#pragma once

// -----------------------------------------------------------------------------
// ICodec.hpp — 线缆格式扩展点(分帧 + 序列化 + 承载交互元数据)
// Encode: 一条 Message → 一段线缆字节(一对一)。
// Decode: 喂入字节切片 → 切出 0..N 条完整 Message(内部维护滚动缓冲,有状态)。
// 不依赖 Transport;由(未来)transport io 线程单线程喂,无需线程安全。
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发:一条消息 → 一段线缆字节。
  virtual Result<std::vector<uint8_t>> Encode(const Message& msg) = 0;

  // 收:喂入字节切片 → 切出 0..N 条完整消息(半包返回空、粘包返回多条)。
  // 解析错误(坏帧头/越界)→ Fail("frame: ..." / "codec: ...")。
  virtual Result<std::vector<Message>> Decode(const uint8_t* data,
                                              std::size_t len) = 0;
};

}  // namespace transport
