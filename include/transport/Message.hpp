#pragma once

// -----------------------------------------------------------------------------
// Message.hpp — 一条消息的数据模型(含交互元数据)
// payload/topic + kind/correlation_id(交互语义,由 ICodec 上线缆,由 System 消费)。
// source/timestamp 由上层 System 填(本轮底层留默认)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

enum class MessageKind {
  kOneway,    // 单向(无需应答)
  kRequest,   // 请求(期待应答/反馈)
  kReply,     // 终结应答(请求-应答的应答,或请求-结果反馈的最终结果)
  kFeedback,  // 中间结果反馈(可多次,非终结)
  kNotify,    // 订阅通知(主动推送)
};

enum class FrameType : uint8_t {   // 占位值,实现前替换为外部真实字节值
  kUnknown   = 0,
  kCommand   = 1,
  kResponse  = 2,
  kResult    = 3,
  kState     = 4,
  kHeartbeat = 5,
};

struct Message {
  std::vector<uint8_t> payload;                 // 应用字节
  std::string topic;                            // 操作/通道名;否则空
  std::string source;                           // 来源标识;本轮底层留空
  int64_t timestamp = 0;                        // 本轮留 0
  MessageKind kind = MessageKind::kOneway;      // 交互种类
  std::string correlation_id;                   // 配对请求↔应答/反馈;非请求为空
  std::string reply_to;                         // 应答回送目的(topic-based 传输);否则空
  FrameType frm_type    = FrameType::kUnknown;  // 协议:帧类型
  uint8_t   protocol_id = 0;                    // 协议:外部系统 id
  uint8_t   session_id  = 0;                    // 协议:会话 id
  uint16_t  message_id  = 0;                    // 协议:帧唯一 id
};

}  // namespace transport
