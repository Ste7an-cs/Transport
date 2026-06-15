#include "transport/codec/LengthFieldCodec.hpp"

#include <utility>

// LengthFieldCodec.cpp — 见 LengthFieldCodec.hpp。
// Decode:把字节追加进滚动缓冲,循环按 header 内长度字段切出完整帧。

namespace transport {

namespace {
Status ValidateConfig(const LengthFieldCodecConfig& c) {
  if (c.header_size == 0) return Status::Fail("config: header_size must be > 0");
  if (c.length_size != 2 && c.length_size != 4 && c.length_size != 8)
    return Status::Fail("config: length_size must be 2, 4, or 8");
  if (c.length_offset + c.length_size > c.header_size)
    return Status::Fail("config: length field exceeds header_size");
  if (c.max_frame_size < c.header_size)
    return Status::Fail("config: max_frame_size smaller than header_size");
  return Status::Success(std::monostate{});
}
}  // namespace

LengthFieldCodec::LengthFieldCodec(LengthFieldCodecConfig config)
    : config_(config) {}

Result<std::vector<uint8_t>> LengthFieldCodec::Encode(const Message& msg) {
  return Result<std::vector<uint8_t>>::Success(msg.payload);  // 透传
}

Result<std::vector<Message>> LengthFieldCodec::Decode(const uint8_t* data,
                                                      std::size_t len) {
  using R = Result<std::vector<Message>>;
  if (auto v = ValidateConfig(config_); !v) return R::Fail(v.error);

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
    if (frame_size < config_.header_size)
      return R::Fail("frame: declared frame size smaller than header");
    if (frame_size > config_.max_frame_size)
      return R::Fail("frame: frame size exceeds max_frame_size");
    if (buffer_.size() - offset < frame_size) break;

    Message m;
    m.payload.assign(buffer_.begin() + offset,
                     buffer_.begin() + offset + static_cast<std::size_t>(frame_size));
    out.push_back(std::move(m));
    offset += static_cast<std::size_t>(frame_size);
  }
  if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
  return R::Success(std::move(out));
}

}  // namespace transport
