#include "transport/codec/SystemCodec.hpp"

#include <utility>

// SystemCodec.cpp — 见 SystemCodec.hpp。

namespace transport {

namespace {

void PutU16(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(v & 0xFF));
}
void PutU32(std::vector<uint8_t>& out, uint32_t v) {
  out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
  out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(v & 0xFF));
}
uint16_t GetU16(const uint8_t* p) {
  return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}
uint32_t GetU32(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

uint8_t KindToByte(MessageKind k) { return static_cast<uint8_t>(k); }

bool ByteToKind(uint8_t b, MessageKind* out) {
  if (b > static_cast<uint8_t>(MessageKind::kNotify)) return false;
  *out = static_cast<MessageKind>(b);
  return true;
}

}  // namespace

Result<std::vector<uint8_t>> SystemCodec::Encode(const Message& msg) {
  std::vector<uint8_t> body;
  body.push_back(KindToByte(msg.kind));
  PutU16(body, static_cast<uint16_t>(msg.correlation_id.size()));
  body.insert(body.end(), msg.correlation_id.begin(), msg.correlation_id.end());
  PutU16(body, static_cast<uint16_t>(msg.topic.size()));
  body.insert(body.end(), msg.topic.begin(), msg.topic.end());
  body.insert(body.end(), msg.payload.begin(), msg.payload.end());

  std::vector<uint8_t> out;
  out.reserve(4 + body.size());
  PutU32(out, static_cast<uint32_t>(body.size()));
  out.insert(out.end(), body.begin(), body.end());
  return Result<std::vector<uint8_t>>::Success(std::move(out));
}

Result<std::vector<Message>> SystemCodec::Decode(const uint8_t* data,
                                                 std::size_t len) {
  using R = Result<std::vector<Message>>;
  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  std::size_t offset = 0;
  while (buffer_.size() - offset >= 4) {
    const std::size_t frame_len = GetU32(buffer_.data() + offset);
    if (frame_len > kMaxFrame) return R::Fail("frame: frame length exceeds max");
    if (buffer_.size() - offset - 4 < frame_len) break;

    const uint8_t* b = buffer_.data() + offset + 4;
    std::size_t pos = 0;
    auto need = [&](std::size_t n) { return pos + n <= frame_len; };

    if (!need(1)) return R::Fail("frame: frame too short for kind");
    MessageKind kind;
    if (!ByteToKind(b[pos], &kind)) return R::Fail("codec: unknown message kind");
    pos += 1;

    if (!need(2)) return R::Fail("frame: corr_len exceeds frame");
    const std::size_t corr_len = GetU16(b + pos); pos += 2;
    if (!need(corr_len)) return R::Fail("frame: corr_id exceeds frame");
    std::string corr(reinterpret_cast<const char*>(b + pos), corr_len);
    pos += corr_len;

    if (!need(2)) return R::Fail("frame: topic_len exceeds frame");
    const std::size_t topic_len = GetU16(b + pos); pos += 2;
    if (!need(topic_len)) return R::Fail("frame: topic exceeds frame");
    std::string topic(reinterpret_cast<const char*>(b + pos), topic_len);
    pos += topic_len;

    Message m;
    m.kind = kind;
    m.correlation_id = std::move(corr);
    m.topic = std::move(topic);
    m.payload.assign(b + pos, b + frame_len);
    out.push_back(std::move(m));

    offset += 4 + frame_len;
  }
  if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
  return R::Success(std::move(out));
}

}  // namespace transport
