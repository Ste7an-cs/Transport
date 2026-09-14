/**
 * @file PerfClient.cpp
 * @brief 客户端角色——**发起端,打印最终表格**(ADR-0018 **D1**–**D6**、**D9**)。
 *
 * ## 控制信道(**D4**)
 *
 * 握手、开始 / 结束、结果回传一律 **in-band**,走被测传输本身,用保留的 `message_id`
 * (`ProtocolNode`)/ 保留服务名(`DdsNode`)与数据流区分。**一律走带重发的交互**
 * (`RequestForResultDirect`)而不是 `Send`——UDP 会丢包,控制流不能丢;这等于用框架
 * 自己的重发能力兜住控制可靠性,不手搓一套。
 *
 * **每一次控制往返都落在测量窗口之外**:窗口由 `kBegin`(第二次)之后的第一条数据样本
 * 开始,到最后一条为止,`kEnd` 在其后才发。
 *
 * ## 预热(**D9**)
 *
 * 每一行矩阵开测前先跑一轮预热(缺省 100 条,不计入),把链路建立、DDS 发现窗口
 * (约 240 ms)、首帧路径上的惰性开销全部付掉。预热之后再发一次 `kBegin` 把接收侧的
 * 起始序号抬到预热之后,故预热样本(含迟到的)一条也不进统计。
 *
 * ## 时延口径(**D2**)
 *
 * `延迟 = RTT / 2 − 时钟开销`(回显型)或 `RTT − 时钟开销`(请求型);时钟开销由
 * **1001 次时钟读取**标定后**从每条里扣**,且不减半——一次往返读两次时钟。
 *
 * ## 吞吐口径(**D3**)
 *
 * 两侧分列。发送侧只能报**入队条数**(写侧 fire-and-forget、队列满静默丢最旧且返回
 * 成功);实收与丢失由**接收侧**按 payload 内嵌的自增序号算出,经 `kEnd` 回传。
 *
 * **只报数,不做 pass/fail**(**D6**):本文件没有任何阈值断言。
 */

#include "PerfSuites.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for / yield

#include "task/fibertask.h"

#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/DdsNode.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/node/RetryPolicy.hpp"

#include "PerfLink.hpp"
#include "PerfStats.hpp"
#include "PerfWire.hpp"

namespace perf {
namespace {

using Clock = std::chrono::steady_clock;
using Millis = std::chrono::milliseconds;
using transport::DdsNode;
using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::MessageDispatcher;
using transport::MessageKind;
using transport::ProtocolNode;
using transport::RetryPolicy;
using transport::TransportErrc;
using transport::make_error_code;

/// 在途样本落地的宽限期:测量窗口**关闭之后**才睡,不计入任何统计。
constexpr int kDrainMs = 300;

/// @brief 两个时刻之差,微秒(double)。
[[nodiscard]] double ElapsedUs(Clock::time_point from, Clock::time_point to) {
  return std::chrono::duration<double, std::micro>(to - from).count();
}

/// @brief 造一条数据请求。
///
/// `session_id` 只对 `Send` 有意义(ADR-0019 D1:原样透传);三个 `RequestFor*` 会自己
/// 分配并覆盖它。`endpoint` 是出站目的地(ADR-0021 D1)——UDP 上不可省。
[[nodiscard]] Message DataRequest(std::uint32_t seq, std::size_t bytes,
                                  const Endpoint& peer) {
  Message request;
  request.message_id = kDataMessageId;
  request.payload = MakeDataPayload(seq, bytes);
  request.endpoint = peer;
  request.session_id = static_cast<std::uint8_t>(seq & 0xFF);
  return request;
}

/// @brief 等一条序号匹配的回显帧;序号不匹配的(上一条超时后迟到的)直接丢掉重等。
/// @return 收到匹配帧则 `true`,并把收到时刻写进 `stamp`。
template <typename TicketT>
[[nodiscard]] bool AwaitEcho(TicketT& inbox, std::uint32_t expected,
                             Millis timeout, Clock::time_point* stamp) {
  const Clock::time_point deadline = Clock::now() + timeout;
  for (;;) {
    const auto remaining =
        std::chrono::duration_cast<Millis>(deadline - Clock::now());
    if (remaining <= Millis::zero()) {
      return false;
    }
    auto got = inbox.Wait(remaining);
    if (!got) {
      return false;
    }
    const Clock::time_point now = Clock::now();
    std::uint32_t seq = 0;
    if (SeqOf(got.value().payload, &seq) && seq == expected) {
      *stamp = now;
      return true;
    }
  }
}

/**
 * @brief `result` 模式的**受理时刻探针**——旁路订阅那一路的最近一条记录。
 *
 * `RequestForResult` 只返回最终 `kResult`,**「到受理」的时刻取不到**;旁路订阅
 * (`AnyOfType(kResponse)`)拿得到那一帧,但"拿到"这个动作必须有一条 fiber 在等,
 * 否则时刻只能在交互结束之后才读到、那就成了"到结果"。故另起一条探针 fiber 专门
 * 守着它,收到即盖时刻。
 *
 * **精度下界是一次 fiber 切换**:分发在读循环里同步投递给两个订阅者(本探针先登记、
 * 故先被唤醒),探针 fiber 随后被调度上来才盖时刻。这与 ADR-0018「明确接受的代价」
 * 5 是同一件事——对端立即回结果,该量测的本就是框架开销的下界。
 */
struct AcceptProbe {
  Clock::time_point stamp{};
  std::uint32_t seq{0};
  bool valid{false};
};

/// @brief 客户端:一条链路 + 一套选项 + 一组按模式建起来的订阅。
class Client {
 public:
  Client(const PerfOptions& options, PerfLink& link)
      : options_(options),
        link_(link),
        retry_{Millis{options.timeout_ms}, options.attempts},
        peer_(link.DataPeer()) {}

  int Run();

 private:
  // ── 控制信道(**D4**)───────────────────────────────────────────────────
  [[nodiscard]] Coro::Result<CtrlReply> Control(CtrlCommand command, Mode mode,
                                                std::size_t payload_bytes,
                                                std::uint32_t start_seq,
                                                std::uint32_t expected,
                                                std::uint32_t run_index);
  [[nodiscard]] bool Handshake();
  void Farewell();

  // ── 每模式的订阅(建一次,横跨该模式的全部矩阵行)──────────────────────
  [[nodiscard]] bool OpenMode(Mode mode);
  void CloseMode();

  // ── 单条交互 ────────────────────────────────────────────────────────────
  /// @brief 跑一次完整往返并给出耗时(µs,**未扣时钟开销**)。
  /// @param accept_us 仅 `result` 模式有意义:到**受理**的耗时;其余写 0。
  [[nodiscard]] bool RoundTrip(Mode mode, std::uint32_t seq,
                               std::size_t payload_bytes, double* round_trip_us,
                               double* accept_us);
  /// @brief 单向发一条(吞吐 suite 的 burst 用),返回是否**已入队**。
  [[nodiscard]] bool SendOneWay(Mode mode, std::uint32_t seq,
                                std::size_t payload_bytes);

  // ── 两套 suite ──────────────────────────────────────────────────────────
  [[nodiscard]] int RunLatency();
  [[nodiscard]] int RunThroughput();

  const PerfOptions& options_;
  PerfLink& link_;
  RetryPolicy retry_;
  Endpoint peer_;
  double clock_overhead_us_{0.0};

  Mode current_mode_{Mode::kSend};
  /// `send` 模式的回显订阅;`result` 模式的**旁路**受理订阅(`AnyOfType(kResponse)`)。
  std::unique_ptr<MessageDispatcher::Ticket> response_inbox_;
  /// `pubsub` 模式的回显 topic 订阅。
  std::unique_ptr<DdsNode::Ticket> echo_inbox_;
  /// `result` 模式的受理时刻探针 fiber 及其记录。
  std::shared_ptr<Coro::FiberTask<void>> probe_task_;
  bool probe_stop_{false};
  AcceptProbe probe_;
};

// ── 控制信道 ────────────────────────────────────────────────────────────────

Coro::Result<CtrlReply> Client::Control(CtrlCommand command, Mode mode,
                                        std::size_t payload_bytes,
                                        std::uint32_t start_seq,
                                        std::uint32_t expected,
                                        std::uint32_t run_index) {
  CtrlRequest control;
  control.command = command;
  control.suite = options_.suite == Suite::kLatency ? 0 : 1;
  control.mode = static_cast<std::uint8_t>(mode);
  control.payload_bytes = static_cast<std::uint32_t>(payload_bytes);
  control.expected = expected;
  control.run_index = run_index;
  control.start_seq = start_seq;

  Message request;
  request.payload = EncodeCtrlRequest(control);
  // 控制**一律走带重发的交互**(D4):UDP 会丢包,控制流不能丢。
  Coro::Result<Message> answer = make_error_code(TransportErrc::kInternal);
  if (link_.IsDds()) {
    request.endpoint = Endpoint::Service(kCtrlService);
    answer = link_.dds_node()->RequestForResultDirect(std::move(request), retry_);
  } else {
    request.message_id = kCtrlMessageId;
    request.endpoint = peer_;
    answer = link_.protocol_node()->RequestForResultDirect(std::move(request),
                                                           retry_,
                                                           kCtrlResultMessageId);
  }
  if (!answer) {
    return answer.error();
  }
  CtrlReply reply;
  if (!DecodeCtrlReply(answer.value().payload, &reply)) {
    return make_error_code(TransportErrc::kFrame);
  }
  return reply;
}

bool Client::Handshake() {
  // DDS 的发现窗口(约 240 ms)与 TCP 的首连都在这里付掉;`RetryPolicy` 的重发吸收它。
  (void)link_.WaitLinkUp(10000);
  auto hello = Control(CtrlCommand::kHello, options_.modes.front(),
                       options_.payloads.front(), 0, 0, 0);
  if (!hello) {
    std::fprintf(stderr, "transport_perf: 握手失败:%s\n",
                 hello.error().message().c_str());
    return false;
  }
  return true;
}

void Client::Farewell() {
  (void)Control(CtrlCommand::kBye, options_.modes.front(),
                options_.payloads.front(), 0, 0, 0);
}

// ── 每模式的订阅 ────────────────────────────────────────────────────────────

bool Client::OpenMode(Mode mode) {
  CloseMode();
  current_mode_ = mode;
  if (mode == Mode::kSend || mode == Mode::kResult) {
    // `send`:回显帧是一条普通 `kResponse`,只能靠旁路订阅收。
    // `result`:**「到受理」的时刻取不到**——`RequestForResult` 只返回最终 `kResult`,
    //           故另开一路旁路订阅按帧类型收受理帧(框架支持,见用例
    //           `SideChannelSubscriberAlsoReceivesMatchedResponse`)。
    auto subscription =
        link_.protocol_node()->Subscribe(transport::AnyOfType(FrameType::kResponse));
    if (!subscription) {
      std::fprintf(stderr, "transport_perf: 旁路订阅失败:%s\n",
                   subscription.error().message().c_str());
      return false;
    }
    response_inbox_ = std::make_unique<MessageDispatcher::Ticket>(
        std::move(subscription).value());
  }
  if (mode == Mode::kResult) {
    // 受理时刻只有"有人正等着"才盖得准,故专起一条探针 fiber(见 `AcceptProbe`)。
    probe_stop_ = false;
    probe_task_ = std::make_shared<Coro::FiberTask<void>>(Coro::makeTask([this] {
      while (!probe_stop_) {
        auto got = response_inbox_->Wait(Millis{50});
        if (!got) {
          if (got.error() == make_error_code(TransportErrc::kTimeout)) {
            continue;  // 只是给 `probe_stop_` 一个观察点。
          }
          return;  // 信箱被关(节点收敛)。
        }
        const Clock::time_point now = Clock::now();
        std::uint32_t seq = 0;
        if (!SeqOf(got.value().payload, &seq)) {
          continue;
        }
        probe_.stamp = now;
        probe_.seq = seq;
        probe_.valid = true;
      }
    }));
  }
  if (mode == Mode::kPubSub) {
    auto subscription =
        link_.dds_node()->Subscribe(std::string(kEchoTopic), MessageKind::kNotify);
    if (!subscription) {
      std::fprintf(stderr, "transport_perf: 回显 topic 订阅失败:%s\n",
                   subscription.error().message().c_str());
      return false;
    }
    echo_inbox_ =
        std::make_unique<DdsNode::Ticket>(std::move(subscription).value());
  }
  return true;
}

void Client::CloseMode() {
  probe_stop_ = true;
  if (probe_task_) {
    (void)probe_task_->get();  // 让出式 join:返回即探针 fiber 已退出,可以拆订阅了。
    probe_task_.reset();
  }
  response_inbox_.reset();
  echo_inbox_.reset();
}

// ── 单条交互 ────────────────────────────────────────────────────────────────

bool Client::RoundTrip(Mode mode, std::uint32_t seq, std::size_t payload_bytes,
                       double* round_trip_us, double* accept_us) {
  *accept_us = 0.0;
  const Millis timeout{options_.timeout_ms};

  switch (mode) {
    case Mode::kSend: {
      Message request = DataRequest(seq, payload_bytes, peer_);
      const Clock::time_point start = Clock::now();
      if (!link_.protocol_node()->Send(std::move(request))) {
        return false;
      }
      Clock::time_point end;
      if (!AwaitEcho(*response_inbox_, seq, timeout, &end)) {
        return false;
      }
      *round_trip_us = ElapsedUs(start, end);
      return true;
    }
    case Mode::kResponse: {
      Message request = DataRequest(seq, payload_bytes, peer_);
      const Clock::time_point start = Clock::now();
      auto reply = link_.protocol_node()->RequestForResponse(std::move(request),
                                                             retry_);
      const Clock::time_point end = Clock::now();
      if (!reply) {
        return false;
      }
      *round_trip_us = ElapsedUs(start, end);
      return true;
    }
    case Mode::kResult: {
      Message request = DataRequest(seq, payload_bytes, peer_);
      probe_.valid = false;
      const Clock::time_point start = Clock::now();
      auto result = link_.protocol_node()->RequestForResult(
          std::move(request), retry_, kDataResultMessageId, timeout);
      const Clock::time_point end = Clock::now();
      if (!result) {
        return false;
      }
      *round_trip_us = ElapsedUs(start, end);
      // 探针盖的那一下就是"到受理"。探针 fiber 被唤醒后不一定当场被调度上来,故在此
      // 让出几次给它机会;仍取不到就记 0(该条不进受理那张表)。
      //
      // ⚠ **该量是个上界,可能略大于"到结果"**:对端立即回结果,两帧常在**同一批**
      // 到达,而探针盖时刻又必然晚于主 fiber 消费完结果的那一刻(单线程 fiber 协作,
      // 没有别的观测位置——**D10** 不许为测量给库加钩子)。这正是 ADR-0018「明确接受
      // 的代价」5 所说的:该间隔测的是框架开销的下界,不代表真实业务。
      for (int spin = 0; spin < 8 && !probe_.valid; ++spin) {
        boost::this_fiber::yield();
      }
      if (probe_.valid && probe_.seq == seq) {
        *accept_us = ElapsedUs(start, probe_.stamp);
      }
      return true;
    }
    case Mode::kResultDirect: {
      Message request = DataRequest(seq, payload_bytes, peer_);
      const Clock::time_point start = Clock::now();
      auto result = link_.protocol_node()->RequestForResultDirect(
          std::move(request), retry_, kDataResultMessageId);
      const Clock::time_point end = Clock::now();
      if (!result) {
        return false;
      }
      *round_trip_us = ElapsedUs(start, end);
      return true;
    }
    case Mode::kPubSub: {
      Message sample;
      sample.payload = MakeDataPayload(seq, payload_bytes);
      sample.endpoint = Endpoint::Topic(kDataTopic);
      const Clock::time_point start = Clock::now();
      if (!link_.dds_node()->Publish(std::move(sample))) {
        return false;
      }
      Clock::time_point end;
      if (!AwaitEcho(*echo_inbox_, seq, timeout, &end)) {
        return false;
      }
      *round_trip_us = ElapsedUs(start, end);
      return true;
    }
    case Mode::kReqResp: {
      Message request;
      request.payload = MakeDataPayload(seq, payload_bytes);
      request.endpoint = Endpoint::Service(kDataService);
      const Clock::time_point start = Clock::now();
      auto result =
          link_.dds_node()->RequestForResultDirect(std::move(request), retry_);
      const Clock::time_point end = Clock::now();
      if (!result) {
        return false;
      }
      *round_trip_us = ElapsedUs(start, end);
      return true;
    }
  }
  return false;
}

bool Client::SendOneWay(Mode mode, std::uint32_t seq, std::size_t payload_bytes) {
  bool enqueued = false;
  if (mode == Mode::kPubSub) {
    Message sample;
    sample.payload = MakeDataPayload(seq, payload_bytes);
    sample.endpoint = Endpoint::Topic(kDataTopic);
    enqueued = static_cast<bool>(link_.dds_node()->Publish(std::move(sample)));
  } else {
    enqueued = static_cast<bool>(
        link_.protocol_node()->Send(DataRequest(seq, payload_bytes, peer_)));
  }
  // **让出一次 fiber**:写侧是 fire-and-forget,写泵与本 fiber 同线程。不让出,整个
  // burst 只会堆在写队列里(TCP/UDP 的写队列无上界),测出的是"入队速率"而不是这条
  // 链路送得出去多少。让出之后 `Sent Samples` 仍是**入队条数**(D3),但写泵真的跑了。
  boost::this_fiber::yield();
  return enqueued;
}

// ── 时延 suite(**D2**)──────────────────────────────────────────────────────

int Client::RunLatency() {
  std::uint32_t run_index = 0;
  for (Mode mode : options_.modes) {
    if (!OpenMode(mode)) {
      return 1;
    }
    const bool half = IsHalfRoundTrip(mode);
    char title[256];
    std::snprintf(title, sizeof(title),
                  "--- latency | medium=%s | mode=%s | metric=%s − clock overhead"
                  " | overhead=%.3f us | round-trip times in us ---",
                  ToString(options_.medium), ToString(mode),
                  half ? "RTT/2" : "RTT", clock_overhead_us_);
    PrintLatencyHeader(title);

    std::vector<LatencyStats> accept_rows;  // 仅 `result` 模式:到受理。
    for (std::size_t payload_bytes : options_.payloads) {
      const std::uint32_t warmup = static_cast<std::uint32_t>(options_.warmup);

      // ① 告诉对端本行的模式(它据此决定怎么回显),并清零其计数。
      if (!Control(CtrlCommand::kBegin, mode, payload_bytes, 0, warmup,
                   run_index)) {
        std::fprintf(stderr, "transport_perf: kBegin 失败(预热前)\n");
        return 1;
      }
      // ② 预热(**D9**),不计入。
      double ignored = 0.0;
      double ignored_accept = 0.0;
      for (std::uint32_t i = 0; i < warmup; ++i) {
        (void)RoundTrip(mode, i, payload_bytes, &ignored, &ignored_accept);
      }
      // ③ 把接收侧的起始序号抬到预热之后——**测量窗口自此开始**。
      if (!Control(CtrlCommand::kBegin, mode, payload_bytes, warmup,
                   static_cast<std::uint32_t>(options_.samples), run_index)) {
        std::fprintf(stderr, "transport_perf: kBegin 失败(预热后)\n");
        return 1;
      }

      std::vector<double> times;
      std::vector<double> accepts;
      times.reserve(static_cast<std::size_t>(options_.samples));
      for (int i = 0; i < options_.samples; ++i) {
        double round_trip_us = 0.0;
        double accept_us = 0.0;
        const std::uint32_t seq = warmup + static_cast<std::uint32_t>(i);
        if (!RoundTrip(mode, seq, payload_bytes, &round_trip_us, &accept_us)) {
          continue;  // 超时 / 未受理:该条不计入,**不做任何断言**(D6)。
        }
        // **D2**:先按口径取半,再扣**整份**时钟开销——一次往返读两次时钟,开销本就
        // 是两次读取之和,故不减半(照 `LatencyTestPublisher.cpp:596-597` 的注释)。
        const double latency =
            (half ? round_trip_us / 2.0 : round_trip_us) - clock_overhead_us_;
        if (latency > 0.0) {
          times.push_back(latency);
        }
        if (accept_us > 0.0) {
          const double accept_latency = accept_us - clock_overhead_us_;
          if (accept_latency > 0.0) {
            accepts.push_back(accept_latency);
          }
        }
      }
      LatencyStats stats =
          ComputeLatencyStats(static_cast<std::uint64_t>(payload_bytes), times);
      PrintLatencyRow(stats);
      if (mode == Mode::kResult) {
        accept_rows.push_back(ComputeLatencyStats(
            static_cast<std::uint64_t>(payload_bytes), accepts));
      }
      ++run_index;
    }

    if (mode == Mode::kResult) {
      // **D5**:`RequestForResult` 报**两个量**——上表是"到结果",本表是"到受理"。
      char accept_title[256];
      std::snprintf(accept_title, sizeof(accept_title),
                    "--- latency | medium=%s | mode=result(到受理 / accept) |"
                    " metric=RTT − clock overhead | overhead=%.3f us ---",
                    ToString(options_.medium), clock_overhead_us_);
      PrintLatencyHeader(accept_title);
      for (const LatencyStats& row : accept_rows) {
        PrintLatencyRow(row);
      }
    }
  }
  CloseMode();
  return 0;
}

// ── 吞吐 suite(**D3**)──────────────────────────────────────────────────────

int Client::RunThroughput() {
  std::uint32_t run_index = 0;
  for (Mode mode : options_.modes) {
    if (!OpenMode(mode)) {
      return 1;
    }
    const bool one_way = mode == Mode::kSend || mode == Mode::kPubSub;
    char title[256];
    std::snprintf(title, sizeof(title),
                  "--- throughput | medium=%s | mode=%s | %s | test time=%d ms ---",
                  ToString(options_.medium), ToString(mode),
                  one_way ? "单向 burst(对端不回显)" : "逐条往返(对端回显)",
                  options_.test_time_ms);
    PrintThroughputHeader(title);

    for (std::size_t payload_bytes : options_.payloads) {
      const std::uint32_t warmup = static_cast<std::uint32_t>(options_.warmup);
      if (!Control(CtrlCommand::kBegin, mode, payload_bytes, 0, warmup,
                   run_index)) {
        std::fprintf(stderr, "transport_perf: kBegin 失败(预热前)\n");
        return 1;
      }
      double ignored = 0.0;
      double ignored_accept = 0.0;
      for (std::uint32_t i = 0; i < warmup; ++i) {
        if (one_way) {
          (void)SendOneWay(mode, i, payload_bytes);
        } else {
          (void)RoundTrip(mode, i, payload_bytes, &ignored, &ignored_accept);
        }
      }
      boost::this_fiber::sleep_for(Millis{kDrainMs});  // 预热残余先落地。
      if (!Control(CtrlCommand::kBegin, mode, payload_bytes, warmup, 0,
                   run_index)) {
        std::fprintf(stderr, "transport_perf: kBegin 失败(预热后)\n");
        return 1;
      }

      // ── 测量窗口:burst × recovery,跑满测试时长 ──────────────────────────
      std::uint32_t seq = warmup;
      double sent = 0.0;
      const Clock::time_point start = Clock::now();
      const Clock::time_point deadline = start + Millis{options_.test_time_ms};
      while (Clock::now() < deadline) {
        for (int i = 0; i < options_.demand; ++i) {
          if (one_way) {
            if (SendOneWay(mode, seq, payload_bytes)) {
              sent += 1.0;
            }
          } else if (RoundTrip(mode, seq, payload_bytes, &ignored,
                               &ignored_accept)) {
            sent += 1.0;
          }
          ++seq;
        }
        if (options_.recovery_ms > 0) {
          boost::this_fiber::sleep_for(Millis{options_.recovery_ms});
        }
      }
      const Clock::time_point stop = Clock::now();

      // ── 窗口之外:等在途样本落地,再问对端要接收侧数字(**D4**)────────────
      boost::this_fiber::sleep_for(Millis{kDrainMs});
      auto settled = Control(CtrlCommand::kEnd, mode, payload_bytes, warmup,
                             static_cast<std::uint32_t>(sent), run_index);
      if (!settled) {
        std::fprintf(stderr, "transport_perf: kEnd 失败:%s\n",
                     settled.error().message().c_str());
        return 1;
      }

      ThroughputRow row;
      row.payload_bytes = static_cast<std::uint32_t>(payload_bytes);
      row.demand = static_cast<std::uint32_t>(options_.demand);
      row.recovery_time_ms = static_cast<std::uint32_t>(options_.recovery_ms);
      row.sent_samples = sent;
      row.send_time_us = ElapsedUs(start, stop);
      row.recv_samples = static_cast<double>(settled.value().received);
      row.lost_samples = static_cast<double>(settled.value().lost);
      row.recv_time_us = static_cast<double>(settled.value().rec_time_us);
      PrintThroughputRow(row);
      ++run_index;
    }
  }
  CloseMode();
  return 0;
}

int Client::Run() {
  // **D2**:时钟开销先标定,后面每条都从中扣。
  clock_overhead_us_ = MeasureClockOverheadUs();
  std::printf("Overhead %f us\n", clock_overhead_us_);
  std::fflush(stdout);

  if (!Handshake()) {
    return 1;
  }
  const int rc = options_.suite == Suite::kLatency ? RunLatency() : RunThroughput();
  Farewell();
  return rc;
}

}  // namespace

int RunClient(const PerfOptions& options, PerfLink& link) {
  Client client(options, link);
  return client.Run();
}

}  // namespace perf
