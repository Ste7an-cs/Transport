#pragma once

// DdsCodec.hpp — DDS 无状态 codec(header-only)。DDS 每 sample 即一条完整消息,
// 无需滚动缓冲;携带交互元数据(kind/correlation_id/reply_to)+ payload。
// 线缆:[kind:1][corr_len:2 BE][corr][reply_len:2 BE][reply_to][payload]
// 无成员状态 → 多 topic 并发 Decode 安全。topic/source 由上层按来源 topic 填。

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

class DdsCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    if (msg.correlation_id.size() > 0xFFFF || msg.reply_to.size() > 0xFFFF)
      return Result<std::vector<uint8_t>>::Fail("codec: dds field too long");
    std::vector<uint8_t> out;
    out.reserve(1 + 2 + msg.correlation_id.size() + 2 + msg.reply_to.size() +
                msg.payload.size());
    out.push_back(static_cast<uint8_t>(msg.kind));
    PutLenPrefixed(out, msg.correlation_id);
    PutLenPrefixed(out, msg.reply_to);
    out.insert(out.end(), msg.payload.begin(), msg.payload.end());
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }

  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override {
    std::vector<Message> out;
    if (len == 0) return Result<std::vector<Message>>::Success(std::move(out));
    std::size_t pos = 0;
    Message m;
    m.kind = static_cast<MessageKind>(data[pos]);
    pos += 1;
    if (static_cast<uint8_t>(m.kind) > static_cast<uint8_t>(MessageKind::kNotify))
      return Fail("codec: dds bad kind");
    if (!GetLenPrefixed(data, len, pos, m.correlation_id))
      return Fail("codec: dds truncated corr field");
    if (!GetLenPrefixed(data, len, pos, m.reply_to))
      return Fail("codec: dds truncated reply field");
    m.payload.assign(data + pos, data + len);  // 余下即 payload
    out.push_back(std::move(m));
    return Result<std::vector<Message>>::Success(std::move(out));
  }

 private:
  static Result<std::vector<Message>> Fail(const std::string& e) {
    return Result<std::vector<Message>>::Fail(e);
  }
  static void PutLenPrefixed(std::vector<uint8_t>& out, const std::string& s) {
    const uint16_t n = static_cast<uint16_t>(s.size());
    out.push_back(static_cast<uint8_t>((n >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>(n & 0xFF));
    out.insert(out.end(), s.begin(), s.end());
  }
  static bool GetLenPrefixed(const uint8_t* data, std::size_t len, std::size_t& pos,
                             std::string& out) {
    if (pos + 2 > len) return false;
    const uint16_t n = (static_cast<uint16_t>(data[pos]) << 8) |
                       static_cast<uint16_t>(data[pos + 1]);
    pos += 2;
    if (pos + n > len) return false;
    out.assign(reinterpret_cast<const char*>(data + pos), n);
    pos += n;
    return true;
  }
};

}  // namespace transport
