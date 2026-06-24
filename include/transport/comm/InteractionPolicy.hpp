#pragma once

// InteractionPolicy.hpp — 交互引擎的协议策略缝(声明式)。引擎只问、不解释 FrameTag/Key。

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

using Key = std::string;   // 统一匹配键;policy 打包协议字段成串
using FrameTag = int;      // 抽象判别符;policy 把 frm_type/kind 枚举 cast 成 int

class InteractionPolicy {
 public:
  virtual ~InteractionPolicy() = default;

  virtual FrameTag TagOf(const Message& m) = 0;
  virtual void     SetTag(Message& m, FrameTag tag) = 0;

  virtual Key  NewCorrelation(Message& out) = 0;     // 盖全新相关号进 out,返回挂起 key
  virtual Key  KeyOf(const Message& in) = 0;          // 取入站 key(须与应答相等)
  virtual void EchoCorrelation(Message& reply, const Message& request) = 0;

  virtual Endpoint ReplyTo(const Message& request) = 0;

  enum class Route { kInboundRequest, kDeliver, kDrop };
  virtual Route RouteUnmatched(const Message& in) = 0;
};

// request-await 配置。
struct RequestSpec {
  FrameTag request_tag = 0;
  std::optional<FrameTag> intermediate_tag;
  FrameTag terminal_tag = 0;
  std::optional<FrameTag> auto_ack_tag;
  std::function<void(const Message&)> on_intermediate;
  std::function<void(Result<Message>)> on_terminal;
  uint32_t timeout_ms = 1000;
  uint32_t max_retries = 0;
};

}  // namespace transport
