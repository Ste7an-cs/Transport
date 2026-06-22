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

struct Message {
  std::vector<uint8_t> payload;                 // 应用字节
  std::string topic;                            // 操作/通道名;否则空
  std::string source;                           // 来源标识;本轮底层留空
  int64_t timestamp = 0;                        // 本轮留 0
  MessageKind kind = MessageKind::kOneway;      // 交互种类
  std::string correlation_id;                   // 配对请求↔应答/反馈;非请求为空
  std::string reply_to;                         // 应答回送目的(topic-based 传输);否则空
};

}  // namespace transport
