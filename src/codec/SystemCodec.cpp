#include "transport/codec/SystemCodec.hpp"

#include <array>
#include <utility>

// SystemCodec.cpp — 见 .hpp。小端;坏帧 resync;CRC 注入。

namespace transport {

namespace {
constexpr std::array<uint8_t, 4> kHeadFlag{0xAA, 0xBB, 0xCC, 0xDD};

void PutU16LE(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back(static_cast<uint8_t>(v & 0xFF));
  out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}
uint16_t GetU16LE(const uint8_t* p) {
  return static_cast<uint16_t>(p[0] | (static_cast<uint16_t>(p[1]) << 8));
}
bool MatchFlag(const uint8_t* p) {
  return p[0] == kHeadFlag[0] && p[1] == kHeadFlag[1] &&
         p[2] == kHeadFlag[2] && p[3] == kHeadFlag[3];
}
}  // namespace

uint16_t DefaultCrc16(const uint8_t* body, std::size_t len) {
  uint16_t crc = 0xFFFF;
  for (std::size_t i = 0; i < len; ++i) {
    crc ^= static_cast<uint16_t>(static_cast<uint16_t>(body[i]) << 8);
    for (int b = 0; b < 8; ++b)
      crc = (crc & 0x8000) ? static_cast<uint16_t>((crc << 1) ^ 0x1021)
                           : static_cast<uint16_t>(crc << 1);
  }
  return crc;
}

SystemCodec::SystemCodec(CrcFn crc) : crc_(std::move(crc)) {}

Result<std::vector<uint8_t>> SystemCodec::Encode(const Message& msg) {
  using R = Result<std::vector<uint8_t>>;
  if (msg.payload.size() + 2 > kMaxBody) return R::Fail("frame: payload too long");

  std::vector<uint8_t> body;
  body.reserve(2 + msg.payload.size());
  PutU16LE(body, msg.message_id);
  body.insert(body.end(), msg.payload.begin(), msg.payload.end());
  const uint16_t crc = crc_(body.data(), body.size());

  std::vector<uint8_t> out;
  out.reserve(kHeaderLen + body.size());
  out.insert(out.end(), kHeadFlag.begin(), kHeadFlag.end());          // head_flag
  out.push_back(static_cast<uint8_t>(msg.frm_type));                  // frm_type
  out.push_back(msg.protocol_id);                                    // protocol_id
  out.push_back(msg.session_id);                                     // session_id
  out.insert(out.end(), {0, 0, 0, 0});                               // reserve
  PutU16LE(out, crc);                                                // crc
  PutU16LE(out, static_cast<uint16_t>(body.size()));                 // frm_len
  out.insert(out.end(), body.begin(), body.end());                   // frm_body
  return R::Success(std::move(out));
}

Result<std::vector<Message>> SystemCodec::Decode(const uint8_t* data, std::size_t len) {
  using R = Result<std::vector<Message>>;
  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  std::size_t off = 0;

  while (true) {
    // 1. 同步:从 off 起找 head_flag。
    std::size_t flag = off;
    bool found = false;
    while (flag + 4 <= buffer_.size()) {
      if (MatchFlag(buffer_.data() + flag)) { found = true; break; }
      ++flag;
    }
    if (!found) { off = flag; break; }   // 保留末尾 ≤3 字节(可能半个同步头)
    off = flag;

    if (buffer_.size() - off < kHeaderLen) break;                 // 头不全,等
    const uint8_t* h = buffer_.data() + off;
    const uint16_t crc_in = GetU16LE(h + 11);
    const uint16_t frm_len = GetU16LE(h + 13);
    if (buffer_.size() - off - kHeaderLen < frm_len) break;       // body 不全,等

    const uint8_t* bd = h + kHeaderLen;
    if (crc_(bd, frm_len) != crc_in) { off += 1; continue; }      // CRC 不符 → resync

    Message m;
    m.frm_type = static_cast<FrameType>(h[4]);
    switch (m.frm_type) {                                         // 未知字节 → kUnknown
      case FrameType::kUnknown: case FrameType::kCommand: case FrameType::kResponse:
      case FrameType::kResult: case FrameType::kState: case FrameType::kHeartbeat: break;
      default: m.frm_type = FrameType::kUnknown;
    }
    m.protocol_id = h[5];
    m.session_id = h[6];
    if (frm_len >= 2) {
      m.message_id = GetU16LE(bd);
      m.payload.assign(bd + 2, bd + frm_len);
    }
    out.push_back(std::move(m));
    off += kHeaderLen + frm_len;
  }

  if (off > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + off);
  return R::Success(std::move(out));
}

}  // namespace transport
