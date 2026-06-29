#pragma once

// =============================================================================
// DdsPolicy.hpp — DDS 发布-订阅的交互策略(header-only,InteractionPolicy 实现)
//
// 把 DDS 的语义翻译成引擎认的抽象 Key/FrameTag:
//   · 判别符 FrameTag = MessageKind(kRequest/kReply/kFeedback/kOneway/kNotify)。
//   · 匹配键 Key = correlation_id(一个全局唯一字符串)。
//   · 应答寻址:请求出站时把本节点 inbox 写进 reply_to(随 DdsCodec 上线缆);服务端
//     应答时 ReplyTo = Topic(reply_to) → 精确回送发起方 inbox。这就是"多路请求-应答
//     不串台"的关键:多个客户端打同一服务 topic,各自的应答各回各家 inbox。
//
// 相关号唯一性:用 random_device 生成的进程级前缀 + 自增序号,避免不同节点/重启间撞号。
// =============================================================================

#include <cstdint>
#include <random>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class DdsPolicy : public InteractionPolicy {
 public:
  // inbox:本节点的 inbox topic —— 请求把它写进 reply_to,应答据此回到这里。
  explicit DdsPolicy(std::string inbox) : inbox_(std::move(inbox)) {
    std::random_device rd;
    prefix_ = std::to_string(rd()) + "-";   // 进程级随机前缀,保证 correlation_id 全局不撞
  }

  // 判别符 = DDS 交互 kind(引擎只比较相等)。
  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.kind); }
  void SetTag(Message& m, FrameTag tag) override { m.kind = static_cast<MessageKind>(tag); }

  // 出站请求:盖全新 correlation_id 作匹配键,并把自身 inbox 写进 reply_to(让应答能回来)。
  Key NewCorrelation(Message& out) override {
    out.correlation_id = prefix_ + std::to_string(++seq_);
    out.reply_to = inbox_;
    return out.correlation_id;
  }
  // 入站匹配键 = correlation_id(应答经 EchoCorrelation 回填后即与原请求相等)。
  Key KeyOf(const Message& in) override { return in.correlation_id; }
  void EchoCorrelation(Message& reply, const Message& req) override { reply.correlation_id = req.correlation_id; }
  // 应答目的地:发回请求自带的 reply_to topic;reply_to 为空则用默认目的地。
  Endpoint ReplyTo(const Message& req) override {
    return req.reply_to.empty() ? Endpoint::Default() : Endpoint::Topic(req.reply_to);
  }
  // 无主入站帧的去向:请求→交 OnRequest;单向/通知→投递 OnMessage;余(无挂起的应答/反馈)→丢。
  Route RouteUnmatched(const Message& in) override {
    switch (in.kind) {
      case MessageKind::kRequest: return Route::kInboundRequest;
      case MessageKind::kOneway: case MessageKind::kNotify: return Route::kDeliver;
      default: return Route::kDrop;  // kReply/kFeedback 却无挂起 → 迟到/重复,丢弃
    }
  }

 private:
  std::string inbox_;     // 本节点 inbox topic(写进出站请求的 reply_to)
  std::string prefix_;    // correlation_id 的进程级随机前缀
  uint64_t seq_ = 0;      // correlation_id 自增序号
};

}  // namespace transport
