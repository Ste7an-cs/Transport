#pragma once

// DdsPolicy.hpp — DDS 策略(header-only)。Key=correlation_id;tag=kind;reply_to=inbox(请求)/路由(应答)。

#include <cstdint>
#include <random>
#include <string>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/comm/InteractionPolicy.hpp"

namespace transport {

class DdsPolicy : public InteractionPolicy {
 public:
  explicit DdsPolicy(std::string inbox) : inbox_(std::move(inbox)) {
    std::random_device rd;
    prefix_ = std::to_string(rd()) + "-";
  }

  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.kind); }
  void SetTag(Message& m, FrameTag tag) override { m.kind = static_cast<MessageKind>(tag); }

  Key NewCorrelation(Message& out) override {
    out.correlation_id = prefix_ + std::to_string(++seq_);
    out.reply_to = inbox_;
    return out.correlation_id;
  }
  Key KeyOf(const Message& in) override { return in.correlation_id; }
  void EchoCorrelation(Message& reply, const Message& req) override { reply.correlation_id = req.correlation_id; }
  Endpoint ReplyTo(const Message& req) override {
    return req.reply_to.empty() ? Endpoint::Default() : Endpoint::Topic(req.reply_to);
  }
  Route RouteUnmatched(const Message& in) override {
    switch (in.kind) {
      case MessageKind::kRequest: return Route::kInboundRequest;
      case MessageKind::kOneway: case MessageKind::kNotify: return Route::kDeliver;
      default: return Route::kDrop;  // kReply/kFeedback(无挂起)
    }
  }

 private:
  std::string inbox_;
  std::string prefix_;
  uint64_t seq_ = 0;
};

}  // namespace transport
