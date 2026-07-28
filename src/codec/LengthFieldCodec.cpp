#include "transport/codec/LengthFieldCodec.hpp"

#include <utility>

#include "transport/coro/Error.hpp"

// LengthFieldCodec.cpp — 见 LengthFieldCodec.hpp。
// Decode:把字节追加进滚动缓冲,循环按 header 内长度字段切出完整帧。

namespace transport {

namespace {
coro::Status ValidateConfig(const LengthFieldCodecConfig& c) {
  // 非法配置参数 → kConfiguration;不支持的长度字段宽度 → kUnsupported。
  if (c.header_size == 0)
    return coro::make_error_code(coro::TransportErrc::kConfiguration);
  if (c.length_size != 2 && c.length_size != 4 && c.length_size != 8)
    return coro::make_error_code(coro::TransportErrc::kUnsupported);
  if (c.length_offset + c.length_size > c.header_size)
    return coro::make_error_code(coro::TransportErrc::kConfiguration);
  if (c.max_frame_size < c.header_size)
    return coro::make_error_code(coro::TransportErrc::kConfiguration);
  return {};
}
}  // namespace

LengthFieldCodec::LengthFieldCodec(LengthFieldCodecConfig config)
    : config_(config) {}

coro::Result<std::vector<uint8_t>> LengthFieldCodec::Encode(const Message& msg) {
  return msg.payload;  // 透传
}

coro::Result<std::vector<Message>> LengthFieldCodec::Decode(const uint8_t* data,
                                                            std::size_t len) {
  if (auto v = ValidateConfig(config_); !v) return v.error();

  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  std::size_t offset = 0;
  while (buffer_.size() - offset >= config_.header_size) {
    const uint8_t* p = buffer_.data() + offset + config_.length_offset;
    uint64_t value = 0;
    if (config_.big_endian)
      for (std::size_t i = 0; i < config_.length_size; ++i)
        value = (value << 8) | static_cast<uint64_t>(p[i]);
    else
      for (std::size_t i = 0; i < config_.length_size; ++i)
        value |= static_cast<uint64_t>(p[i]) << (8 * i);

    const uint64_t frame_size =
        config_.length_includes_header ? value : config_.header_size + value;
    // 声明帧长小于 header,或超过最大帧长 → 分帧错误,kFrame。
    if (frame_size < config_.header_size)
      return coro::make_error_code(coro::TransportErrc::kFrame);
    if (frame_size > config_.max_frame_size)
      return coro::make_error_code(coro::TransportErrc::kFrame);
    if (buffer_.size() - offset < frame_size) break;

    Message m;
    m.payload.assign(buffer_.begin() + offset,
                     buffer_.begin() + offset + static_cast<std::size_t>(frame_size));
    out.push_back(std::move(m));
    offset += static_cast<std::size_t>(frame_size);
  }
  if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
  return out;
}

}  // namespace transport
