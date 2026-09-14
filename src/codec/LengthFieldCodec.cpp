#include "transport/codec/LengthFieldCodec.hpp"

#include <utility>

#include <QByteArray>

#include "transport/core/Error.hpp"

// LengthFieldCodec.cpp — 见 LengthFieldCodec.hpp。
// Decode:把字节追加进滚动缓冲,循环按 header 内长度字段切出完整帧。

namespace transport {

namespace {
Coro::Result<void> ValidateConfig(const LengthFieldCodecConfig& c) {
  // 非法配置参数 → kConfiguration;不支持的长度字段宽度 → kUnsupported。
  if (c.header_size == 0)
    return make_error_code(TransportErrc::kConfiguration);
  if (c.length_size != 2 && c.length_size != 4 && c.length_size != 8)
    return make_error_code(TransportErrc::kUnsupported);
  if (c.length_offset + c.length_size > c.header_size)
    return make_error_code(TransportErrc::kConfiguration);
  if (c.max_frame_size < c.header_size)
    return make_error_code(TransportErrc::kConfiguration);
  return {};
}
}  // namespace

LengthFieldCodec::LengthFieldCodec(LengthFieldCodecConfig config)
    : config_(config) {}

Coro::Result<std::vector<uint8_t>> LengthFieldCodec::Encode(const Message& msg) {
  // 透传 payload;**完全忽略 msg.frame**(ADR-0020 D7)。
  const auto* p = reinterpret_cast<const uint8_t*>(msg.payload.constData());
  return std::vector<uint8_t>(p, p + msg.payload.size());
}

Coro::Result<std::vector<Message>> LengthFieldCodec::Decode(const uint8_t* data,
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
      return make_error_code(TransportErrc::kFrame);
    if (frame_size > config_.max_frame_size)
      return make_error_code(TransportErrc::kFrame);
    if (buffer_.size() - offset < frame_size) break;

    Message m;
    // 本 codec **不剥帧头**:payload 历来就是整帧(header + body),故 payload 偏移为 0、
    // 长度与 frame 相同——两者内容一致,但 payload 仍是 frame 的视图(ADR-0020 D2)。
    // ★ 顺序:先落 frame,再在【它】上面建视图。
    m.frame = QByteArray(
        reinterpret_cast<const char*>(buffer_.data() + offset),
        static_cast<int>(frame_size));
    m.payload = QByteArray::fromRawData(m.frame.constData(),
                                        static_cast<int>(frame_size));
    out.push_back(std::move(m));
    offset += static_cast<std::size_t>(frame_size);
  }
  if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
  return out;
}

}  // namespace transport
