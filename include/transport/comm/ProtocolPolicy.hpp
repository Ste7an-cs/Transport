#pragma once

// ProtocolPolicy.hpp — 外部协议策略(header-only)。Key=pack(session,message);tag=frm_type。
// session_id 滚动 0–255;message_id = 调用方填的命令码(不动);protocol_id 配置。

#include <charconv>
#include <cstdint>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class ProtocolPolicy : public InteractionPolicy {
 public:
  explicit ProtocolPolicy(uint8_t protocol_id, bool reply_to_source = false)
      : protocol_id_(protocol_id), reply_to_source_(reply_to_source) {}

  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.frm_type); }
  void SetTag(Message& m, FrameTag tag) override { m.frm_type = static_cast<FrameType>(tag); }

  Key NewCorrelation(Message& out) override {
    out.session_id = session_ctr_++;     // 滚动 0–255(uint8 自然回绕)
    out.protocol_id = protocol_id_;       // message_id 由调用方填(命令码),不动
    return Pack(out.session_id, out.message_id);
  }
  Key KeyOf(const Message& in) override { return Pack(in.session_id, in.message_id); }
  void EchoCorrelation(Message& reply, const Message& req) override {
    reply.session_id = req.session_id; reply.message_id = req.message_id; reply.protocol_id = req.protocol_id;
  }
  Endpoint ReplyTo(const Message& req) override {
    if (!reply_to_source_) return Endpoint::Default();
    const std::string& s = req.source;
    const auto pos = s.rfind(':');                       // 按最后一个 ':' 切分(兼容 IPv4 与 ::1)
    if (pos == std::string::npos || pos == 0 || pos + 1 >= s.size()) return Endpoint::Default();
    unsigned long port = 0;
    const char* first = s.c_str() + pos + 1;
    const char* last = s.c_str() + s.size();
    auto res = std::from_chars(first, last, port);
    if (res.ec != std::errc() || res.ptr != last || port == 0 || port > 65535)
      return Endpoint::Default();
    return Endpoint::Net(s.substr(0, pos), static_cast<uint16_t>(port));
  }
  Route RouteUnmatched(const Message& in) override {
    switch (in.frm_type) {
      case FrameType::kCommand: case FrameType::kState: return Route::kInboundRequest;
      case FrameType::kHeartbeat: return Route::kDeliver;
      // 有意:未挂起的 RESPONSE/RESULT 与 kUnknown 均静默丢弃。
      // (旧 ProtocolNode 对 kUnknown 发 OnError("codec: unknown frame type");按 spec 现改为静默 drop。)
      default: return Route::kDrop;  // RESPONSE/RESULT(无挂起)/UNKNOWN
    }
  }

 private:
  static Key Pack(uint8_t s, uint16_t m) {
    Key k(3, '\0');
    k[0] = static_cast<char>(s);
    k[1] = static_cast<char>(m & 0xFF);
    k[2] = static_cast<char>((m >> 8) & 0xFF);
    return k;
  }
  uint8_t protocol_id_;
  uint8_t session_ctr_ = 0;
  bool reply_to_source_ = false;
};

}  // namespace transport
