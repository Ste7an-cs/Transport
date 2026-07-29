#pragma once

// -----------------------------------------------------------------------------
// Message.hpp — 一条消息的数据模型(贯穿三层的载体)
//
// 一条 Message 既携带 payload(应用字节),又携带【交互元数据】。元数据分两套,
// 互不干扰、按路径取用:
//   · 通用交互元数据(kind/correlation_id/reply_to):DDS 路径用,由 DdsCodec 上线缆、
//     DdsPolicy 解读。
//   · 外部协议字段(frm_type/protocol_id/session_id/message_id):外部协议路径用,由
//     SystemCodec 上线缆、ProtocolPolicy 解读。
// 引擎只在边界传递 Message,从不解读这些字段——解读全在对应的 policy/codec 里。
// source/topic 由引擎在收到时按来源(ip:port / topic)填。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

// DDS 路径的交互种类(DdsPolicy 把它当判别符 FrameTag;DdsCodec 上线缆首字节)。
enum class MessageKind {
  kOneway,    // 单向(无需应答)
  kRequest,   // 请求(期待应答/反馈)
  kReply,     // 终结应答(请求-应答的应答,或请求-结果反馈的最终结果)
  kFeedback,  // 中间结果反馈(可多次,非终结)
  kNotify,    // 订阅通知(主动推送/发布)
};

// 外部协议帧类型(ProtocolPolicy 把它当判别符;SystemCodec 上线缆 frm_type 字节)。
// ⚠️ 这里是【占位值】:真实对接外部系统时,改成外部协议规定的真实字节值。
enum class FrameType : uint8_t {
  kUnknown   = 0,
  kCommand   = 1,  // 命令(请求)
  kResponse  = 2,  // 即时回应(中间或终结)
  kResult    = 3,  // 最终结果(终结)
  kState     = 4,  // 状态(周期 STATE)
  kHeartbeat = 5,  // 心跳
};

struct Message {
  std::vector<uint8_t> payload;                 // 应用字节(框架不解读其语义)
  std::string topic;                            // 操作/通道名(DDS=topic);收到时缺省取 source
  std::string source;                           // 来源标识(引擎填:UDP="ip:port"、DDS=topic)
  int64_t timestamp = 0;                        // 预留(本库未用)
  // ---- 通用交互元数据(DDS 路径)----
  MessageKind kind = MessageKind::kOneway;      // 交互种类(DdsPolicy 判别符)
  std::string correlation_id;                   // 配对请求↔应答(DdsPolicy 匹配键);非请求为空
  std::string reply_to;                         // 应答回送目的 topic(DDS 多路 req-resp);否则空
  // ---- 外部协议字段(SystemCodec / ProtocolNode 路径)----
  FrameType frm_type    = FrameType::kUnknown;  // 帧类型(ProtocolPolicy 判别符)
  uint8_t   protocol_id = 0;                    // 外部系统 id
  uint8_t   session_id  = 0;                    // 会话 id(滚动 0–255,匹配键一半)
  uint16_t  message_id  = 0;                    // 命令码(匹配键另一半)
};

}  // namespace transport
