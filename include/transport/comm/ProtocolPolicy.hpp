#pragma once

// =============================================================================
// ProtocolPolicy.hpp — 外部协议帧的交互策略(header-only,InteractionPolicy 实现)
//
// 把外部系统协议帧(SystemCodec 那套 AA BB CC DD 帧)的语义翻译成引擎认的 Key/FrameTag:
//   · 判别符 FrameTag = frm_type(COMMAND/RESPONSE/RESULT/STATE/HEARTBEAT/UNKNOWN)。
//   · 匹配键 Key = pack(session_id, message_id)(3 字节串)。
//       - session_id:本实例【滚动 0–255】,每发一个请求 +1(uint8 自然回绕);它让"哪个
//         请求等哪个应答"可区分。⇒ 同一时刻最多 256 个并发挂起请求。
//       - message_id:调用方填的【命令码】(每种命令一个固定值),NewCorrelation 不动它。
//   · protocol_id:用于区分不同外部系统,按节点配置盖到出站帧。
//   · 应答寻址:默认 Default(走配置目的地/同一连接);reply_to_source=true 时(1:多 UDP)
//       从入站 Message.source("ip:port")解析出 Net 地址,把应答回送给来源对端。
// =============================================================================

#include <charconv>
#include <cstdint>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class ProtocolPolicy : public InteractionPolicy {
 public:
  // protocol_id：盖到出站帧的外部系统 id。
  // reply_to_source：true 用于"一个 UDP socket 服务多个对端"——应答/ack 回到入站来源 ip:port;
  //   TCP/串口/1:1 留 false(它们对非 Default Endpoint 会报 io: 错)。
  explicit ProtocolPolicy(uint8_t protocol_id, bool reply_to_source = false)
      : protocol_id_(protocol_id), reply_to_source_(reply_to_source) {}

  // 判别符 = frm_type(引擎只比较相等)。
  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.frm_type); }
  void SetTag(Message& m, FrameTag tag) override { m.frm_type = static_cast<FrameType>(tag); }

  // 出站请求:滚动 session_id、盖 protocol_id;message_id(命令码)保持调用方所填,返回匹配键。
  Key NewCorrelation(Message& out) override {
    out.session_id = session_ctr_++;     // 滚动 0–255(uint8 自然回绕 → 第 257 个请求复用 0)
    out.protocol_id = protocol_id_;      // message_id 由调用方填(命令码),不动
    return Pack(out.session_id, out.message_id);
  }
  // 入站匹配键 = pack(session_id, message_id);服务端 EchoCorrelation 原样回填后即与请求相等。
  Key KeyOf(const Message& in) override { return Pack(in.session_id, in.message_id); }
  void EchoCorrelation(Message& reply, const Message& req) override {
    reply.session_id = req.session_id; reply.message_id = req.message_id; reply.protocol_id = req.protocol_id;
  }
  // 应答目的地:默认 Default;reply_to_source 时把入站来源 "host:port" 解析成 Net。
  Endpoint ReplyTo(const Message& req) override {
    if (!reply_to_source_) return Endpoint::Default();
    const std::string& s = req.source;                   // UDP 下形如 "192.168.1.5:7000" 或 "::1:7000"
    const auto pos = s.rfind(':');                       // 按【最后一个】':' 切分(兼容 IPv4 与 ::1)
    if (pos == std::string::npos || pos == 0 || pos + 1 >= s.size()) return Endpoint::Default();
    unsigned long port = 0;
    const char* first = s.c_str() + pos + 1;
    const char* last = s.c_str() + s.size();
    auto res = std::from_chars(first, last, port);       // 不抛异常的端口解析(全库 no-throw 纪律)
    if (res.ec != std::errc() || res.ptr != last || port == 0 || port > 65535)
      return Endpoint::Default();                        // 解析失败/越界 → 安全回退 Default
    return Endpoint::Net(s.substr(0, pos), static_cast<uint16_t>(port));
  }
  // 无主入站帧去向:COMMAND/STATE→当请求(交 OnCommand);HEARTBEAT→投递(交 OnHeartbeat);余→丢。
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
  // 把 (session_id, message_id) 打包成 3 字节键:[session][message_lo][message_hi]。
  static Key Pack(uint8_t s, uint16_t m) {
    Key k(3, '\0');
    k[0] = static_cast<char>(s);
    k[1] = static_cast<char>(m & 0xFF);
    k[2] = static_cast<char>((m >> 8) & 0xFF);
    return k;
  }
  uint8_t protocol_id_;            // 外部系统 id(盖到出站帧)
  uint8_t session_ctr_ = 0;        // session_id 滚动计数器(0–255)
  bool reply_to_source_ = false;   // 见构造注释
};

}  // namespace transport
