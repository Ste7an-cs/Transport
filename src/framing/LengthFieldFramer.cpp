#include "transport/framing/LengthFieldFramer.hpp"

#include <variant>

namespace transport {

Status LengthFieldFramer::ValidateConfig(const LengthFieldFramerConfig& c) {
  if (c.header_size == 0) {
    return Status::Fail("config: header_size must be > 0");
  }
  if (c.length_size != 2 && c.length_size != 4 && c.length_size != 8) {
    return Status::Fail("config: length_size must be 2, 4, or 8");
  }
  if (c.length_offset + c.length_size > c.header_size) {
    return Status::Fail("config: length field exceeds header_size");
  }
  if (c.max_frame_size < c.header_size) {
    return Status::Fail("config: max_frame_size smaller than header_size");
  }
  return Status::Success(std::monostate{});
}

LengthFieldFramer::LengthFieldFramer(LengthFieldFramerConfig config)
    : config_(config) {}

Result<FrameResult> LengthFieldFramer::TryExtract(const uint8_t* buf, size_t len) {
  if (len < config_.header_size) {
    return Result<FrameResult>::Success(FrameResult{0, false});
  }

  uint64_t value = 0;
  const uint8_t* p = buf + config_.length_offset;
  if (config_.big_endian) {
    for (size_t i = 0; i < config_.length_size; ++i) {
      value = (value << 8) | static_cast<uint64_t>(p[i]);
    }
  } else {
    for (size_t i = 0; i < config_.length_size; ++i) {
      value |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
  }

  const uint64_t frame_size =
      config_.length_includes_header ? value : config_.header_size + value;

  if (frame_size < config_.header_size) {
    return Result<FrameResult>::Fail(
        "frame: declared frame size smaller than header");
  }
  if (frame_size > config_.max_frame_size) {
    return Result<FrameResult>::Fail("frame: frame size exceeds max_frame_size");
  }
  if (len < frame_size) {
    return Result<FrameResult>::Success(FrameResult{0, false});
  }
  return Result<FrameResult>::Success(
      FrameResult{static_cast<size_t>(frame_size), true});
}

}  // namespace transport
