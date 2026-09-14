#pragma once

// DdsCodec.hpp — DDS 无状态 codec(header-only)。DDS 每 sample 即一条完整消息,
// 无需滚动缓冲;携带交互元数据(kind/correlation_id/reply_to)+ payload。
// 线缆:[kind:1][corr_len:2 BE][corr][reply_len:2 BE][reply_to][payload]
// 无成员状态 → 多 topic 并发 Decode 安全。endpoint 由上层按来源 topic 填。

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <QByteArray>

#include "transport/codec/ICodec.hpp"
#include "transport/core/Message.hpp"

namespace transport {

class DdsCodec : public ICodec {
 public:
  Coro::Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    // 字段超 uint16 长度前缀上限:codec 格式无法承载 → kCodec。
    if (msg.correlation_id.size() > 0xFFFF || msg.reply_to.size() > 0xFFFF)
      return make_error_code(TransportErrc::kCodec);
    // **完全忽略 msg.frame**(ADR-0020 D7):出站样本只由当前各字段生成。
    const std::size_t payload_len = static_cast<std::size_t>(msg.payload.size());
    const auto* payload = reinterpret_cast<const uint8_t*>(msg.payload.constData());
    std::vector<uint8_t> out;
    out.reserve(1 + 2 + msg.correlation_id.size() + 2 + msg.reply_to.size() +
                payload_len);
    out.push_back(static_cast<uint8_t>(msg.kind));
    PutLenPrefixed(out, msg.correlation_id);
    PutLenPrefixed(out, msg.reply_to);
    out.insert(out.end(), payload, payload + payload_len);
    return out;
  }

  Coro::Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override {
    std::vector<Message> out;
    if (len == 0) return out;
    std::size_t pos = 0;
    Message m;
    m.kind = static_cast<MessageKind>(data[pos]);
    pos += 1;
    // 坏判别符 / 字段越界:sample 语义损坏 → kCodec。
    if (static_cast<uint8_t>(m.kind) > static_cast<uint8_t>(MessageKind::kNotify))
      return make_error_code(TransportErrc::kCodec);
    if (!GetLenPrefixed(data, len, pos, m.correlation_id))
      return make_error_code(TransportErrc::kCodec);
    if (!GetLenPrefixed(data, len, pos, m.reply_to))
      return make_error_code(TransportErrc::kCodec);
    // 整帧 = 整个 sample(DDS 每 sample 即一条完整消息),payload 偏移即解析停下的 pos。
    // ★ 顺序:先落 frame,再在【它】上面建视图(ADR-0020 D2)。
    m.frame = QByteArray(reinterpret_cast<const char*>(data), static_cast<int>(len));
    m.payload = QByteArray::fromRawData(m.frame.constData() + pos,
                                        static_cast<int>(len - pos));  // 余下即 payload
    out.push_back(std::move(m));
    return out;
  }

 private:
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
