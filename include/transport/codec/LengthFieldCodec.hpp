#pragma once

// LengthFieldCodec.hpp — 「固定 header + 长度字段」分帧 codec。
// Encode 透传 payload;Decode 用滚动缓冲按长度字段切帧,每帧作为一条 kOneway Message。
// 由原 LengthFieldFramer + FrameAssembler 合并而来。

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/ICodec.hpp"

namespace transport {

struct LengthFieldCodecConfig {
  // 帧布局:[header(含一个长度字段)][body]。
  std::size_t header_size = 0;             // header 总字节数(必须 >0)
  std::size_t length_offset = 0;           // 长度字段在 header 内的偏移
  std::size_t length_size = 4;             // 长度字段宽度:2 / 4 / 8 字节
  bool big_endian = true;                  // 长度字段字节序
  bool length_includes_header = false;     // 长度值是否含 header(false=仅 body 长)
  std::size_t max_frame_size = 16 * 1024 * 1024;  // 帧上限(超出 → kFrame,防恶意超长)
};

class LengthFieldCodec : public ICodec {
 public:
  // 配置非法 → 构造仍成功,但首次 Decode 返回错误:length_size∉{2,4,8} → kUnsupported;
  // 其余(header_size==0 / 长度字段越出 header / max_frame_size < header_size)→ kInvalidArgument。
  explicit LengthFieldCodec(LengthFieldCodecConfig config);

  coro::Result<std::vector<uint8_t>> Encode(const Message& msg) override;
  coro::Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override;

 private:
  LengthFieldCodecConfig config_;
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
