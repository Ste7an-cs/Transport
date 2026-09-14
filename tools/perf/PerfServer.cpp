/**
 * @file PerfServer.cpp
 * @brief 服务端角色——**另一个真节点**(ADR-0018 **D5** / **D4** / **D3**)。
 *
 * ## 只用公开面
 *
 * ADR-0019 之后 `Send` 不再盖 `session_id`,服务端因此与客户端对称:`Subscribe` +
 * 填字段 + `Send` 即可回出合法应答帧,不必绕到 `ICodec::Encode` + `ITransport::AsyncWrite`。
 *
 * **应答必须回带三项,缺一不可**:
 * ```
 * rsp.session_id = req.session_id;   // ADR-0019:否则客户端 Dispatcher 匹配不上
 * rsp.message_id = req.message_id;
 * rsp.endpoint   = req.endpoint;     // ADR-0021:UDP 上否则发往默认对端、请求方收不到
 * ```
 *
 * ## 各模式的应答形态(**D5**)
 *
 * | 模式 | 服务端回什么 |
 * |---|---|
 * | `send` | 一帧 `kResponse`(回显 payload),客户端经旁路订阅收 |
 * | `response` | `kResponse`(同 `session_id` / `message_id`) |
 * | `result` | 先 `kResponse`(受理)后 `kResult`;**框架自动补发的末尾那帧不由对端管** |
 * | `resultdirect` | 只回 `kResult` |
 * | `pubsub` | 往回显 topic `Publish` 一条 |
 * | `reqresp` | `Reply()` 一条终结应答 |
 *
 * ## 接收侧计数(**D3** / **D10**)
 *
 * 框架不提供任何计数,且写侧无背压——**实收与丢失由本工具按 payload 内嵌的自增序号
 * 自行算出**。不给库加任何计数器、钩子或观测接口。
 */

#include "PerfSuites.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "task/fibertask.h"

#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/DdsNode.hpp"
#include "transport/node/ProtocolNode.hpp"

#include "PerfLink.hpp"
#include "PerfWire.hpp"

namespace perf {
namespace {

using Clock = std::chrono::steady_clock;
using transport::DdsNode;
using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::MessageKind;
using transport::ProtocolNode;
using transport::TransportErrc;
using transport::make_error_code;

/// 接收侧计数器——**一行矩阵一套**,由 `kBegin` 清零(**D3**)。
class InboundCounter {
 public:
  /// @brief 按 `kBegin` 给的起始序号重置:小于它的样本一律不计(**D9**,挡掉预热)。
  void Reset(std::uint32_t start_seq) {
    start_seq_ = start_seq;
    received_ = 0;
    max_seq_ = 0;
    seen_ = false;
  }

  /// @brief 记一条入站数据样本;序号取自 payload 前四字节。
  void Record(const QByteArray& payload) {
    std::uint32_t seq = 0;
    if (!SeqOf(payload, &seq) || seq < start_seq_) {
      return;
    }
    const Clock::time_point now = Clock::now();
    if (!seen_) {
      first_ = now;
      max_seq_ = seq;
      seen_ = true;
    }
    last_ = now;
    if (seq > max_seq_) {
      max_seq_ = seq;
    }
    ++received_;
  }

  /// @brief 结算:实收 / 丢失 / 首末样本间隔。
  ///
  /// 丢失 = (最大序号 − 起始序号 + 1) − 实收。重发导致的重复会让它变负,**夹到 0**
  /// ——重复不是丢失,报负数只会让读表的人困惑。
  [[nodiscard]] CtrlReply Settle(CtrlCommand command) const {
    CtrlReply reply;
    reply.command = command;
    reply.received = received_;
    if (seen_) {
      const std::uint64_t span =
          static_cast<std::uint64_t>(max_seq_ - start_seq_) + 1;
      reply.lost = span > received_ ? span - received_ : 0;
      reply.rec_time_us = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(last_ - first_)
              .count());
    }
    return reply;
  }

 private:
  std::uint32_t start_seq_{0};
  std::uint64_t received_{0};
  std::uint32_t max_seq_{0};
  bool seen_{false};
  Clock::time_point first_{};
  Clock::time_point last_{};
};

/// 一行矩阵的当前设定:由 `kBegin` 带来。
struct RunSetup {
  Suite suite = Suite::kLatency;
  Mode mode = Mode::kSend;
  bool echo = true;
};

/// @brief 由 suite 与 mode 推出"这一行要不要回显"。
///
/// 吞吐 suite 的**单向模式**(`send` / `pubsub`)是纯 burst,不回显——回显会把它变成
/// 往返测试。其余情形一律回显:时延 suite 全靠回显,吞吐 suite 的请求型模式本身就等应答。
bool EchoFor(Suite suite, Mode mode) {
  if (suite == Suite::kThroughput &&
      (mode == Mode::kSend || mode == Mode::kPubSub)) {
    return false;
  }
  return true;
}

/// @brief 处理一条控制命令,回出应答负载。
CtrlReply HandleControl(const CtrlRequest& request, RunSetup* setup,
                        InboundCounter* counter, bool* stop) {
  switch (request.command) {
    case CtrlCommand::kHello:
      break;
    case CtrlCommand::kBegin: {
      setup->suite = request.suite == 0 ? Suite::kLatency : Suite::kThroughput;
      setup->mode = static_cast<Mode>(request.mode);
      setup->echo = EchoFor(setup->suite, setup->mode);
      counter->Reset(request.start_seq);
      break;
    }
    case CtrlCommand::kEnd:
      return counter->Settle(request.command);
    case CtrlCommand::kBye:
      *stop = true;
      break;
  }
  CtrlReply reply;
  reply.command = request.command;
  return reply;
}

// ── ProtocolNode 路径(TCP / UDP)─────────────────────────────────────────────

/// @brief 回一帧:三项回带缺一不可(ADR-0019 / ADR-0021)。
Message ReplyFrame(const Message& request, FrameType type,
                   std::uint16_t message_id, const QByteArray& payload) {
  Message reply;
  reply.frm_type = type;
  reply.session_id = request.session_id;  // ADR-0019
  reply.message_id = message_id;
  reply.endpoint = request.endpoint;      // ADR-0021
  reply.payload = payload;
  return reply;
}

int ServeProtocol(const PerfOptions& options, PerfLink& link) {
  ProtocolNode& node = *link.protocol_node();
  auto subscription = node.Subscribe(transport::AnyOfType(FrameType::kCommand));
  if (!subscription) {
    std::fprintf(stderr, "transport_perf: 订阅失败:%s\n",
                 subscription.error().message().c_str());
    return 1;
  }
  auto inbox = std::move(subscription).value();

  RunSetup setup;
  InboundCounter counter;
  bool stop = false;
  std::printf("transport_perf: 服务端就位(%s),等客户端握手……\n",
              ToString(options.medium));
  std::fflush(stdout);

  while (!stop) {
    // **无限期等**:信箱被节点关闭时才返回错误。控制流自带 `kBye`,不需要轮询标志位,
    // 也就不必每条消息付一次定时器开销——那会落在被测的应答路径上。
    auto got = inbox.Wait();
    if (!got) {
      break;
    }
    const Message& request = got.value();

    // ── 控制(**D4**:in-band,用保留的 message_id 与数据流区分)──────────────
    if (request.message_id == kCtrlMessageId) {
      CtrlRequest control;
      if (!DecodeCtrlRequest(request.payload, &control)) {
        continue;  // 坏控制帧:不应答,让客户端的重发再来一次。
      }
      const CtrlReply reply = HandleControl(control, &setup, &counter, &stop);
      (void)node.Send(ReplyFrame(request, FrameType::kResult,
                                 kCtrlResultMessageId, EncodeCtrlReply(reply)));
      continue;
    }

    // ── 数据 ────────────────────────────────────────────────────────────────
    counter.Record(request.payload);
    if (!setup.echo) {
      continue;
    }
    // `request.payload` 是指进 `request.frame` 的视图(ADR-0020);在回显语句里直接用
    // 是安全的——`request` 尚在作用域内。
    switch (setup.mode) {
      case Mode::kSend:
      case Mode::kResponse:
        (void)node.Send(ReplyFrame(request, FrameType::kResponse,
                                   request.message_id, request.payload));
        break;
      case Mode::kResult:
        // 先受理、后结果;**框架自动补发的末尾那帧不由对端管**(ADR-0010 D8)。
        (void)node.Send(ReplyFrame(request, FrameType::kResponse,
                                   request.message_id, request.payload));
        (void)node.Send(ReplyFrame(request, FrameType::kResult,
                                   kDataResultMessageId, request.payload));
        break;
      case Mode::kResultDirect:
        (void)node.Send(ReplyFrame(request, FrameType::kResult,
                                   kDataResultMessageId, request.payload));
        break;
      case Mode::kPubSub:
      case Mode::kReqResp:
        break;  // DDS 专属,走不到这里。
    }
  }
  std::printf("transport_perf: 服务端收工。\n");
  std::fflush(stdout);
  return 0;
}

// ── DdsNode 路径 ─────────────────────────────────────────────────────────────

int ServeDds(const PerfOptions& options, PerfLink& link) {
  (void)options;
  DdsNode& node = *link.dds_node();

  auto ctrl = node.ServeRequests(kCtrlService);
  auto data_service = node.ServeRequests(kDataService);
  auto data_topic = node.Subscribe(std::string(kDataTopic), MessageKind::kNotify);
  if (!ctrl || !data_service || !data_topic) {
    std::fprintf(stderr, "transport_perf: DDS 订阅失败\n");
    return 1;
  }
  DdsNode::Ticket ctrl_inbox = std::move(ctrl).value();
  DdsNode::Ticket service_inbox = std::move(data_service).value();
  DdsNode::Ticket topic_inbox = std::move(data_topic).value();

  RunSetup setup;
  InboundCounter counter;

  // 三路入站各一条消费 fiber(信箱是队列语义,一条 fiber 只能等一个)。数据两路先起,
  // 控制走本 fiber;`kBye` 之后关节点,两条数据 fiber 的信箱随之关闭、自行退出。
  auto service_loop = Coro::makeTask([&] {
    for (;;) {
      auto got = service_inbox.Wait();
      if (!got) {
        return;
      }
      const Message& request = got.value();
      counter.Record(request.payload);
      if (!setup.echo) {
        continue;
      }
      Message result;
      result.payload = request.payload;  // 回显;`request` 尚在作用域内,视图有效。
      (void)node.Reply(request, std::move(result));
    }
  });
  auto topic_loop = Coro::makeTask([&] {
    for (;;) {
      auto got = topic_inbox.Wait();
      if (!got) {
        return;
      }
      const Message& sample = got.value();
      counter.Record(sample.payload);
      if (!setup.echo) {
        continue;
      }
      Message echo;
      echo.payload = sample.payload;
      echo.endpoint = Endpoint::Topic(kEchoTopic);
      (void)node.Publish(std::move(echo));
    }
  });

  std::printf("transport_perf: 服务端就位(dds domain=%d),等客户端握手……\n",
              options.domain);
  std::fflush(stdout);

  bool stop = false;
  while (!stop) {
    auto got = ctrl_inbox.Wait();
    if (!got) {
      break;
    }
    const Message& request = got.value();
    CtrlRequest control;
    if (!DecodeCtrlRequest(request.payload, &control)) {
      continue;
    }
    const CtrlReply reply = HandleControl(control, &setup, &counter, &stop);
    Message result;
    result.payload = EncodeCtrlReply(reply);
    (void)node.Reply(request, std::move(result));
  }

  // 关节点 → 两条数据 fiber 的信箱被关 → 它们的 `Wait` 返错退出 → 可以 join 了。
  link.Stop();
  (void)service_loop.get();
  (void)topic_loop.get();
  std::printf("transport_perf: 服务端收工。\n");
  std::fflush(stdout);
  return 0;
}

}  // namespace

int RunServer(const PerfOptions& options, PerfLink& link) {
  return link.IsDds() ? ServeDds(options, link) : ServeProtocol(options, link);
}

}  // namespace perf
