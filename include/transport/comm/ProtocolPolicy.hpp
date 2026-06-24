#pragma once

// ProtocolPolicy.hpp — 外部协议策略(header-only)。Key=pack(session,message);tag=frm_type。
// session_id 滚动 0–255;message_id = 调用方填的命令码(不动);protocol_id 配置。

#include <cstdint>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class ProtocolPolicy : public InteractionPolicy {
 public:
  explicit ProtocolPolicy(uint8_t protocol_id) : protocol_id_(protocol_id) {}

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
  Endpoint ReplyTo(const Message&) override { return Endpoint::Default(); }
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
};

}  // namespace transport
