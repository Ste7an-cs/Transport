#pragma once

// =============================================================================
// InteractionPolicy.hpp — 交互引擎的「协议策略缝」(声明式)
//
// InteractionEngine(交互引擎)把所有交互【机制】写成一份:挂起表、超时、重发、
// 分发、periodic、并发纪律。但不同外部协议在【语义】上各不相同——怎么生成请求的
// 相关号、怎么从一帧里取出匹配键、应答该发回哪里、无主入站帧该当请求还是丢弃……
// 这些【差异】被抽到这个声明式接口里。
//
// 核心约定:引擎只认两个【抽象】类型——
//   · Key       匹配键(把"哪个请求等哪个应答"用一个字符串表达)
//   · FrameTag  判别符(把"这是 COMMAND 还是 RESPONSE"用一个 int 表达)
// 引擎对 Key/FrameTag 只做【相等比较】,【从不解释】它们的含义。所有"把协议字段
// 翻译成 Key/FrameTag"的工作都由 policy 做。于是引擎代码里看不到任何 frm_type、
// MessageKind、protocol_id —— 这是「机制与策略解耦」的护栏:加一种新协议 = 写一个
// 新 InteractionPolicy,引擎一行不改。
//
// 已有两个实现:ProtocolPolicy(外部协议帧)、DdsPolicy(DDS 发布-订阅)。
// =============================================================================

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

// 统一匹配键。policy 把协议的相关字段(如 session_id+message_id,或 correlation_id)
// 打包成一个字符串;引擎用它做 std::map 的键,只比较相等、不解析内容。
using Key = std::string;

// 抽象判别符。policy 把协议的帧类型枚举(frm_type / MessageKind)cast 成 int;
// 引擎只比较两个 FrameTag 是否相等(如"这帧的 tag 是否等于 RequestSpec.terminal_tag"),
// 从不关心 int 背后是哪种帧。
using FrameTag = int;

// 协议策略接口(纯虚)。引擎在收发/分发的每个决策点回调这 7 个方法;实现须无副作用
// 之外的状态(NewCorrelation 例外:它需要一个自增/滚动的相关号计数器)。
class InteractionPolicy {
 public:
  virtual ~InteractionPolicy() = default;

  // ---- 判别符读写:把一帧的"类型"在 Message 与抽象 FrameTag 间互转 ----
  // TagOf:从入站 Message 读出判别符(如 (int)m.frm_type)。引擎据此判断该帧是不是
  //        某挂起请求等待的中间/终结帧。
  virtual FrameTag TagOf(const Message& m) = 0;
  // SetTag:把判别符写回出站 Message(如 m.frm_type = (FrameType)tag)。引擎在 Fire/
  //        发送前调用,给消息盖上正确的帧类型。
  virtual void     SetTag(Message& m, FrameTag tag) = 0;

  // ---- 相关键:让"请求"与"它的应答"能配对 ----
  // NewCorrelation:出站请求发出前调用。给 out 盖上一个【全新】相关号(并可顺带填
  //   应答回送地址等),返回这次请求的挂起 Key。引擎用该 Key 把请求登记进挂起表。
  //   这是唯一允许带内部状态的方法(相关号计数器)。
  virtual Key  NewCorrelation(Message& out) = 0;
  // KeyOf:入站帧到达时调用,取出它的匹配 Key。若该 Key 命中挂起表,即为某请求的应答。
  //   必须保证:服务端按 EchoCorrelation 回填后,应答的 KeyOf == 原请求的 NewCorrelation。
  virtual Key  KeyOf(const Message& in) = 0;
  // EchoCorrelation:服务端构造应答时调用,把 request 的相关字段回填进 reply,使发起方
  //   的 KeyOf(reply) 命中原挂起项。
  virtual void EchoCorrelation(Message& reply, const Message& request) = 0;

  // ReplyTo:服务端应答/客户端自动 ack 时,算出该发往哪里。
  //   DDS:Topic(request.reply_to)(发回发起方 inbox);
  //   外部协议:默认 Default(走同一连接/配置目的地),1:多 UDP 时返回 Net(来源 ip:port)。
  virtual Endpoint ReplyTo(const Message& request) = 0;

  // 无主入站帧的去向:一帧没命中任何挂起请求时,引擎问 policy 该怎么处理。
  enum class Route {
    kInboundRequest,  // 当作对端发来的【请求】→ 交 OnInboundRequest(节点造 Responder)
    kDeliver,         // 当作单向【投递】→ 交 OnInboundDeliver(如 DDS 发布、心跳)
    kDrop             // 丢弃(如外部协议里无主的 RESPONSE/RESULT、未知帧)
  };
  virtual Route RouteUnmatched(const Message& in) = 0;

  // IsTerminal:该 frm_type 是否【终结】一个请求(协程引擎 Request 用;异步引擎不调它)。
  // 默认 RESULT 终结;需要别的终结规则的 policy 可覆写。纯加性,不影响异步引擎行为。
  virtual bool IsTerminal(FrameType t) const { return t == FrameType::kResult; }
};

// request-await(发请求并等待回应)的一次性配置。由节点按交互模式填好,传给
// InteractionEngine::RequestAwait;引擎据此驱动"等中间帧→等终结帧→超时重发"的状态机。
struct RequestSpec {
  // 出站请求帧自身的判别符(如 COMMAND / kRequest)。引擎发送前 SetTag(out, request_tag)。
  FrameTag request_tag = 0;
  // 可选的【中间】帧判别符。收到它 → 调 on_intermediate、停止重发、但【保留】挂起继续等终结。
  // 不设(nullopt)= 没有中间阶段(needresponse / withfeedback 等单回合模式)。
  std::optional<FrameTag> intermediate_tag;
  // 【终结】帧判别符(如 RESPONSE / RESULT / kReply)。收到它 → 调 on_terminal、结束挂起。
  FrameTag terminal_tag = 0;
  // 可选:收到终结帧后,引擎【自动】回送一帧 ack(此 tag),用于 needfeedback 模式。
  std::optional<FrameTag> auto_ack_tag;
  // 中间帧回调(拷贝调用,保留挂起)。
  std::function<void(const Message&)> on_intermediate;
  // 终结回调(移出调用,恰好一次):成功带应答 Message,失败带 timeout:/conn: 错误。
  std::function<void(Result<Message>)> on_terminal;
  // 单次等待超时(毫秒)。超时后:未推进且未达上限 → 重发;否则 → on_terminal(timeout:)。
  uint32_t timeout_ms = 1000;
  // 重发上限。0 = 不重发(超时即失败)。「首帧停重发」:任何中间/终结帧到达即不再重发。
  uint32_t max_retries = 0;
};

}  // namespace transport
