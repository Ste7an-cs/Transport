// ProtocolNode 单元测试(重设计 + ADR-0009 之后的形态)。
//
// 在 fiber 调度器(coro_test_main)内,以假传输 + SystemDatagramCodec 驱动,覆盖五组事实:
//
//   1. 出站盖章:frm_type / protocol_id / session_id 的填写与 session_id 的循环递增;
//   2. 请求-响应关联:由 Dispatcher 按"会话 + 命令码 + 帧类型"三字段匹配,任一不符即不
//      终结该请求;
//   3. 分发去向(ADR-0009 D1/D5):入站只有订阅一条通路——业务帧投给键匹配的订阅者,
//      无人认领的终结帧归因 unmatched-or-late-response,无人认领的业务帧静默丢弃且**不**
//      产生任何 drop 归因;
//   4. 生命周期:节点关闭令在途请求恰好终结一次、并关闭全部订阅信箱(即订阅者的协作取消
//      信号,ADR-0009 D4);传输终结使节点自行关闭;节点不启停传输;
//   5. 分段交互:`Subscribe` 允许一次交互登记多段等待,各段各自设定时限。
//
// 入站业务一律经 `Subscribe` + **调用方自有消费 fiber** 取用(ADR-0009 D2 的样板,本文件
// 收在 `Subscriber` 小件里):串行、异常隔离与 join 全由调用方自己负责(RT_INBOUND_005)。
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "coro_test_util.hpp"
#include "fake_transport.hpp"
#include "task/fibertask.h"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/ProtocolNode.hpp"

using namespace std::chrono_literals;
using testutil::FakeTransport;
using transport::FrameType;
using transport::kAny;
using transport::Message;
using transport::MessageDispatcher;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::SystemDatagramCodec;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

constexpr std::uint8_t kProtocolId = 0x2A;

/// 本文件用例的默认时限。时限不在配置面上、必须逐次显式给出(SRS §3.1.4.4 /
/// ADR-0010 D6),凡用例本身不关心具体取值的调用一律填本值,使各用例时限语义一致。
constexpr auto kCaseTimeout = 200ms;

ProtocolNodeConfig BaseConfig() {
  ProtocolNodeConfig config;
  config.protocol_id = kProtocolId;
  return config;
}

std::unique_ptr<transport::ICodec> MakeCodec() {
  return std::make_unique<SystemDatagramCodec>();
}

Message Command(std::uint16_t message_id, std::vector<std::uint8_t> payload = {1}) {
  Message msg;
  msg.message_id = message_id;
  msg.payload = std::move(payload);
  return msg;
}

/// 造一条回帧(以独立 codec 编码),供假传输投递。
std::vector<std::uint8_t> EncodeFrame(std::uint8_t session_id,
                                      std::uint16_t message_id, FrameType type,
                                      std::vector<std::uint8_t> payload = {9}) {
  Message msg;
  msg.protocol_id = kProtocolId;
  msg.session_id = session_id;
  msg.message_id = message_id;
  msg.frm_type = type;
  msg.payload = std::move(payload);
  SystemDatagramCodec codec;
  auto bytes = codec.Encode(msg);
  EXPECT_TRUE(bytes);
  return std::move(bytes).value();
}

/// 解出假传输收到的第 index 条出站报文。
Message DecodeSent(const FakeTransport& fake, std::size_t index) {
  EXPECT_GT(fake.sent().size(), index);
  const auto& bytes = fake.sent()[index].bytes;
  SystemDatagramCodec codec;
  auto decoded = codec.Decode(bytes.data(), bytes.size());
  EXPECT_TRUE(decoded);
  EXPECT_EQ(decoded.value().size(), 1u);
  return decoded.value().front();
}

bool SawDrop(const transport::CapturingTraceSink& sink, const std::string& reason) {
  for (const auto& record : sink.Records()) {
    if (record.category == "drop" && record.message == reason) {
      return true;
    }
  }
  return false;
}

/// drop 类事件总数(不问归因),用于证明"一条都没有"。
std::size_t DropCount(const transport::CapturingTraceSink& sink) {
  std::size_t count = 0;
  for (const auto& record : sink.Records()) {
    if (record.category == "drop") {
      ++count;
    }
  }
  return count;
}

/**
 * 订阅 + 自有消费 fiber——ADR-0009 D2 的调用方样板,本文件内收一份复用。
 *
 * 它**不是**框架件:节点不再代管入站业务消费,串行(一条 fiber 顺序消费)、逃逸异常隔离
 * (自己 try/catch)与汇合(自己 join 自己的 fiber)全部由调用方自负(RT_INBOUND_005)。
 *
 * 消费 fiber 的退出有两条路径,都是"信箱被关":① 节点 `Close()` → `Dispatcher::CloseAll`
 * 关闭全部信箱(ADR-0009 D4 的协作取消信号);② 本件 `Join()` 自己关自己的信箱——用于节点
 * 尚未关闭就要收尾的用例。二者都令在途 `await` 恰好终结一次。
 */
class Subscriber {
 public:
  Subscriber(ProtocolNode& node, MessageDispatcher::Key key,
             std::function<void(const Message&)> on_message)
      : ticket_(node.Subscribe(std::move(key))), mailbox_(ticket_.mailbox()) {
    task_ = std::make_shared<Coro::FiberTask<void>>(
        Coro::makeTask([this, on_message = std::move(on_message)] {
          for (;;) {
            Coro::Result<Message, std::error_code> msg = Coro::await(mailbox_);
            if (!msg) {
              stop_error_ = msg.error();  // 信箱被关 → 退出
              break;
            }
            try {
              on_message(msg.value());
            } catch (...) {
              ++exceptions_;  // 自行隔离:框架不再兜住逃逸异常
            }
          }
          exited_ = true;
        }));
  }

  ~Subscriber() { Join(); }

  Subscriber(const Subscriber&) = delete;
  Subscriber& operator=(const Subscriber&) = delete;

  /// 关自己的信箱(幂等:节点已关过则不覆盖首个终止原因)+ join 自己的 fiber。
  /// **不依赖 `WaitClosed()`**——它不 join 宿主的 fiber(ADR-0009 D4)。
  void Join() {
    if (!task_) {
      return;
    }
    mailbox_->close(make_error_code(TransportErrc::kClosed));
    (void)task_->get();  // 让出式 join:返回即消费 fiber 已退出。
    task_.reset();
  }

  [[nodiscard]] bool exited() const { return exited_; }
  [[nodiscard]] std::error_code stop_error() const { return stop_error_; }
  [[nodiscard]] std::size_t exceptions() const { return exceptions_; }

 private:
  MessageDispatcher::Ticket ticket_;
  std::shared_ptr<Coro::Awaitable<Message>> mailbox_;
  std::shared_ptr<Coro::FiberTask<void>> task_;
  bool exited_ = false;
  std::error_code stop_error_;
  std::size_t exceptions_ = 0;
};

/// 已 Start 的节点;传输由**测试**(即宿主)启停,节点只借用。
struct Fixture {
  FakeTransport transport;
  std::unique_ptr<ProtocolNode> node;

  explicit Fixture(ProtocolNodeConfig config = BaseConfig()) {
    EXPECT_TRUE(transport.Start());
    node = std::make_unique<ProtocolNode>(transport, MakeCodec(), std::move(config));
    EXPECT_TRUE(node->Start());
  }
  ~Fixture() {
    node.reset();  // 析构内含 Close + WaitClosed
    (void)transport.Close();
  }
};

}  // namespace

// —— 1. 出站盖章 ————————————————————————————————————————————————————

TEST(ProtocolNode, SendStampsCommandFrameAndProtocolId) {
  Fixture fx;
  ASSERT_TRUE(fx.node->Send(Command(0x0102)));

  ASSERT_EQ(fx.transport.sent().size(), 1u);
  const Message sent = DecodeSent(fx.transport, 0);
  EXPECT_EQ(sent.frm_type, FrameType::kCommand);
  EXPECT_EQ(sent.protocol_id, kProtocolId);
  EXPECT_EQ(sent.message_id, 0x0102);
}

// 调用方给出的业务类型优先于默认的 kCommand。
TEST(ProtocolNode, SendKeepsCallerSuppliedFrameType) {
  Fixture fx;
  Message msg = Command(0x0007);
  msg.frm_type = FrameType::kState;
  ASSERT_TRUE(fx.node->Send(std::move(msg)));

  EXPECT_EQ(DecodeSent(fx.transport, 0).frm_type, FrameType::kState);
}

// session_id 是自增计数器:逐次递增,越过 255 回绕到 0。
TEST(ProtocolNode, SessionIdIncrementsAndWrapsAround) {
  Fixture fx;
  for (int i = 0; i < 258; ++i) {
    ASSERT_TRUE(fx.node->Send(Command(0x0001))) << "第 " << i << " 次";
  }
  ASSERT_EQ(fx.transport.sent().size(), 258u);
  EXPECT_EQ(DecodeSent(fx.transport, 0).session_id, 0);
  EXPECT_EQ(DecodeSent(fx.transport, 1).session_id, 1);
  EXPECT_EQ(DecodeSent(fx.transport, 255).session_id, 255);
  EXPECT_EQ(DecodeSent(fx.transport, 256).session_id, 0) << "越过 255 应回绕";
  EXPECT_EQ(DecodeSent(fx.transport, 257).session_id, 1);
}

// —— 2. 请求-响应关联 ————————————————————————————————————————————————

TEST(ProtocolNode, RequestIsTerminatedByMatchingResponse) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    reply = fx.node->RequestForResponse(Command(0x0010), {kCaseTimeout, 1});
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {7, 7})));
  (void)caller.get();

  ASSERT_TRUE(reply) << reply.error().message();
  EXPECT_EQ(reply.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(reply.value().payload, (std::vector<std::uint8_t>{7, 7}));
}

// 无回应 → 单次尝试耗尽。终结原因是 kNotAccepted 而非 kTimeout(ADR-0010 D12:
// 受理阶段没等到即"对端始终没有受理"),这是 `Request` 删除后本用例唯一的可观察差异,
// 时限(80ms)与"一次尝试、不重发"均与原用例一致。
TEST(ProtocolNode, RequestTimesOutWithoutResponse) {
  Fixture fx;
  auto reply = fx.node->RequestForResponse(Command(0x0010), {80ms, 1});
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kNotAccepted));
}

// 三个匹配字段任缺其一都不终结该请求。
TEST(ProtocolNode, ResponseWithMismatchedSessionDoesNotTerminateRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), {120ms, 1}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(static_cast<std::uint8_t>(sent.session_id + 1), sent.message_id,
                  FrameType::kResponse)));
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kNotAccepted))
      << "不匹配的帧不终结该请求,只能由单次尝试耗尽终结";
}

TEST(ProtocolNode, ResponseWithMismatchedFrameTypeDoesNotTerminateRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), {120ms, 1}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  // 同会话、同命令码,但类型是结果而非回应。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResult)));
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kNotAccepted))
      << "不匹配的帧不终结该请求,只能由单次尝试耗尽终结";
}

TEST(ProtocolNode, ResponseWithMismatchedMessageIdDoesNotTerminateRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), {120ms, 1}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, static_cast<std::uint16_t>(sent.message_id + 1),
                  FrameType::kResponse)));
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kNotAccepted))
      << "不匹配的帧不终结该请求,只能由单次尝试耗尽终结";
}

// 并发的两个请求各自拿到自己的响应:session_id 是二者的唯一区分。
// 以回声(响应原样带回请求的 payload)判定归属,不依赖两个 fiber 的启动先后。
TEST(ProtocolNode, ConcurrentRequestsAreCorrelatedIndependently) {
  Fixture fx;
  Coro::Result<Message> first = make_error_code(TransportErrc::kInternal);
  Coro::Result<Message> second = make_error_code(TransportErrc::kInternal);
  auto a = Coro::makeTask([&] {
    first = fx.node->RequestForResponse(Command(0x0010, {0xAA}), {500ms, 1});
  });
  auto b = Coro::makeTask([&] {
    second = fx.node->RequestForResponse(Command(0x0010, {0xBB}), {500ms, 1});
  });

  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return fx.transport.sent().size() >= 2; }, 500));
  const Message sent_first = DecodeSent(fx.transport, 0);
  const Message sent_second = DecodeSent(fx.transport, 1);
  ASSERT_NE(sent_first.session_id, sent_second.session_id);

  // 故意逆序回应:关联只看键,与到达次序无关。
  for (const Message* sent : {&sent_second, &sent_first}) {
    ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(
        sent->session_id, sent->message_id, FrameType::kResponse, sent->payload)));
  }
  (void)a.get();
  (void)b.get();

  ASSERT_TRUE(first) << first.error().message();
  ASSERT_TRUE(second) << second.error().message();
  EXPECT_EQ(first.value().payload, (std::vector<std::uint8_t>{0xAA}));
  EXPECT_EQ(second.value().payload, (std::vector<std::uint8_t>{0xBB}));
}

// —— 3. 分发去向(ADR-0009:入站只有订阅一条通路)——————————————————————————

// 【新增语义 ①】业务帧有订阅者 → 投递到其信箱,消费 fiber 收到(RT_INBOUND_001)。
TEST(ProtocolNode, BusinessFrameIsDeliveredToSubscriber) {
  Fixture fx;
  std::vector<std::uint16_t> seen;
  Subscriber business(*fx.node, transport::AnyOfType(FrameType::kCommand),
                      [&seen](const Message& msg) { seen.push_back(msg.message_id); });

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(3, 0x0055, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return !seen.empty(); }, 500));
  EXPECT_EQ(seen.front(), 0x0055);

  business.Join();  // 宿主自己 join 自己的 fiber(ADR-0009 D4)。
  EXPECT_TRUE(business.exited());
}

// 投递按键走:请求的响应只进该请求的信箱,业务订阅者(订的是命令帧)见不到它。
// 这条替代旧的"命中订阅者的消息不再进入业务队列"——第二条通路已不存在,同一事实现在由
// 键匹配本身保证。
TEST(ProtocolNode, MatchedResponseDoesNotReachBusinessSubscriber) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));

  std::vector<FrameType> seen;
  Subscriber business(*fx.node, transport::AnyOfType(FrameType::kCommand),
                      [&seen](const Message& msg) { seen.push_back(msg.frm_type); });

  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    reply = fx.node->RequestForResponse(Command(0x0010), {kCaseTimeout, 1});
  });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse)));
  (void)caller.get();
  ASSERT_TRUE(reply);

  boost::this_fiber::sleep_for(50ms);
  EXPECT_TRUE(seen.empty()) << "响应帧不应投给订阅命令帧的业务订阅者";
  EXPECT_EQ(DropCount(sink), 0u) << "被请求认领的响应不是丢弃";
  business.Join();
}

// 【新增语义 ③】终结帧无人认领 → 仍归因 kUnmatchedOrLateResponse。
// 该分支属请求-响应侧,与 handler 无关,ADR-0009 明确**保留**。
TEST(ProtocolNode, UnmatchedTerminalFrameIsStillAttributed) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));

  // 订的是命令帧,故终结帧无人认领。
  std::vector<FrameType> seen;
  Subscriber business(*fx.node, transport::AnyOfType(FrameType::kCommand),
                      [&seen](const Message& msg) { seen.push_back(msg.frm_type); });

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(9, 0x0099, FrameType::kResponse)));
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return SawDrop(sink, "unmatched-or-late-response"); }, 500))
      << "未归因 unmatched-or-late-response";
  EXPECT_TRUE(seen.empty()) << "终结帧不应投给业务订阅者";

  // 结果帧同为终结帧,走同一条归因分支。
  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(9, 0x0099, FrameType::kResult)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return DropCount(sink) >= 2u; }, 500));
  EXPECT_EQ(DropCount(sink), 2u) << "两条终结帧各归因一次,不多不少";
  business.Join();
}

// 【新增语义 ②】业务帧无订阅者 → 静默丢弃,drop 类 Trace **一条都没有**(ADR-0009 D5)。
// 这是"完整性归因覆盖面明确变窄"的行为证据:旧形态此处归因 no-handler-configured。
TEST(ProtocolNode, UnclaimedBusinessFrameIsSilentlyDropped) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0033, FrameType::kCommand)));
  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(2, 0x0034, FrameType::kState)));
  // 先确认两帧确已被解出并走完分发(recv 事件到齐),再断言"无归因"——否则可能只是还没到。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] {
        std::size_t recv = 0;
        for (const auto& record : sink.Records()) {
          if (record.category == "recv") {
            ++recv;
          }
        }
        return recv >= 2;
      },
      500));
  boost::this_fiber::sleep_for(20ms);
  EXPECT_EQ(DropCount(sink), 0u) << "无订阅者的业务帧不得产生任何 drop 归因";
}

// 坏帧不影响后续报文(丢弃由 codec 内部 resync 消化,故此处不断言 bad-frame 归因:
// SystemDatagramCodec 的 Decode 对扫不出帧的报文返回空成功,不报错)。
TEST(ProtocolNode, BadFrameIsDroppedAndDoesNotBlockLaterFrames) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));

  std::vector<std::uint16_t> seen;
  Subscriber business(*fx.node, transport::AnyOfType(FrameType::kCommand),
                      [&seen](const Message& msg) { seen.push_back(msg.message_id); });

  ASSERT_TRUE(fx.transport.Deliver({0xDE, 0xAD, 0xBE, 0xEF}));  // 非法帧
  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0044, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return !seen.empty(); }, 500));
  EXPECT_EQ(seen.front(), 0x0044) << "坏帧之后的报文仍应正常分发";
  business.Join();
}

// 同一条业务帧投给全部键匹配的订阅者,各得一份副本(RT_INBOUND_001)。
TEST(ProtocolNode, BusinessFrameIsCopiedToEveryMatchingSubscriber) {
  Fixture fx;
  std::vector<std::uint16_t> wide;
  std::vector<std::uint16_t> narrow;
  Subscriber all(*fx.node, transport::AnyOfType(FrameType::kCommand),
                 [&wide](const Message& msg) { wide.push_back(msg.message_id); });
  Subscriber one(*fx.node, transport::FrameOf(4, 0x0077, FrameType::kCommand),
                 [&narrow](const Message& msg) { narrow.push_back(msg.message_id); });

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(4, 0x0077, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return !wide.empty() && !narrow.empty(); }, 500));
  EXPECT_EQ(wide.front(), 0x0077);
  EXPECT_EQ(narrow.front(), 0x0077);
  one.Join();
  all.Join();
}

// —— 4. 生命周期 ————————————————————————————————————————————————————

// 节点关闭令在途请求恰好终结一次。
TEST(ProtocolNode, CloseTerminatesInFlightRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), {5000ms, 1}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));

  ASSERT_TRUE(fx.node->Close());
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kClosed));
}

TEST(ProtocolNode, RequestAndSendRejectedBeforeStartAndAfterClose) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode node(fake, MakeCodec(), BaseConfig());

  EXPECT_EQ(
      node.RequestForResponse(Command(0x0001), {kCaseTimeout, 1}).error(),
      make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Send(Command(0x0001)).error(),
            make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(node.Start());
  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  EXPECT_EQ(
      node.RequestForResponse(Command(0x0001), {kCaseTimeout, 1}).error(),
      make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Send(Command(0x0001)).error(),
            make_error_code(TransportErrc::kClosed));
  (void)fake.Close();
}

// 节点不启停传输:传输的 Start / Close 只由宿主发起。
TEST(ProtocolNode, NodeNeverStartsOrClosesTheTransport) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ASSERT_EQ(fake.start_count(), 1u);
  {
    ProtocolNode node(fake, MakeCodec(), BaseConfig());
    ASSERT_TRUE(node.Start());
    ASSERT_TRUE(node.Send(Command(0x0001)));
    ASSERT_TRUE(node.Close());
    node.WaitClosed();
  }
  EXPECT_EQ(fake.start_count(), 1u) << "节点不得启动传输";
  EXPECT_EQ(fake.close_count(), 0u) << "节点不得关闭传输";
  EXPECT_TRUE(fake.running()) << "节点关闭后传输仍应可用";
  (void)fake.Close();
}

// 传输终结(源读队列被关)使节点自行关闭——读循环退出时无条件调公开的 Close()。
TEST(ProtocolNode, TransportTerminationClosesNode) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode node(fake, MakeCodec(), BaseConfig());
  ASSERT_TRUE(node.Start());
  ASSERT_TRUE(node.IsRunning());

  (void)fake.Close();  // 关闭源读队列 → 节点读循环退出
  EXPECT_TRUE(testutil::pumpFiberUntil([&] { return !node.IsRunning(); }, 500));
  node.WaitClosed();
}

// 【新增语义 ④】Close() 后订阅者的在途 await 得到终止错误,消费 fiber 自行退出
// (ADR-0009 D4 / RT_INBOUND_004):信箱关闭就是协作取消信号,框架不强杀 fiber。
TEST(ProtocolNode, CloseTerminatesSubscriberAwaitAndConsumerExits) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode node(fake, MakeCodec(), BaseConfig());
  ASSERT_TRUE(node.Start());

  Subscriber business(node, transport::AnyOfType(FrameType::kCommand),
                      [](const Message&) {});
  // 让消费 fiber 先真正挂到 await 上,再关节点——考的正是"在途 await 被终结"。
  boost::this_fiber::sleep_for(20ms);
  EXPECT_FALSE(business.exited()) << "无消息且节点未关时消费 fiber 应挂在 await 上";

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(testutil::pumpFiberUntil([&] { return business.exited(); }, 500))
      << "信箱被 CloseAll 关闭后消费 fiber 应自行退出";
  EXPECT_EQ(business.stop_error(), make_error_code(TransportErrc::kClosed))
      << "在途 await 应得到节点的终止原因";
  business.Join();  // 幂等:已退出,立即返回。
  node.WaitClosed();
  (void)fake.Close();
}

// 关闭后新登记的订阅其信箱已处于关闭态:消费 fiber 起来即退出,不会挂死。
TEST(ProtocolNode, SubscribeAfterCloseYieldsClosedMailbox) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode node(fake, MakeCodec(), BaseConfig());
  ASSERT_TRUE(node.Start());
  ASSERT_TRUE(node.Close());
  node.WaitClosed();

  Subscriber late(node, transport::AnyOfType(FrameType::kCommand),
                  [](const Message&) { ADD_FAILURE() << "关闭后不应再有投递"; });
  late.Join();
  EXPECT_TRUE(late.exited());
  EXPECT_EQ(late.stop_error(), make_error_code(TransportErrc::kClosed));
  (void)fake.Close();
}

// 订阅者可在自己的 fiber 内关闭本节点(ADR-0009 D4):消费 fiber 属宿主、不是节点的内部
// 工作单元,且 Close() 只发信号、不含等待点。
TEST(ProtocolNode, SubscriberCanCloseNodeFromItsOwnFiber) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode node(fake, MakeCodec(), BaseConfig());
  ASSERT_TRUE(node.Start());

  Subscriber business(node, transport::AnyOfType(FrameType::kCommand),
                      [&node](const Message&) { (void)node.Close(); });

  ASSERT_TRUE(fake.Deliver(EncodeFrame(1, 0x0066, FrameType::kCommand)));
  EXPECT_TRUE(testutil::pumpFiberUntil([&] { return !node.IsRunning(); }, 500));
  business.Join();
  node.WaitClosed();
  (void)fake.Close();
}

// 消费代码的逃逸异常由**调用方自己**隔离(ADR-0009 D3 / RT_INBOUND_005):框架不再兜住,
// 宿主的 try/catch 使消费 fiber 只丢掉该条、继续消费后续报文。
TEST(ProtocolNode, SubscriberExceptionIsIsolatedByCaller) {
  Fixture fx;
  std::vector<std::uint16_t> seen;
  Subscriber business(*fx.node, transport::AnyOfType(FrameType::kCommand),
                      [&seen](const Message& msg) {
                        if (msg.message_id == 0x0001) {
                          throw std::runtime_error("boom");
                        }
                        seen.push_back(msg.message_id);
                      });

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0001, FrameType::kCommand)));
  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0002, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return !seen.empty(); }, 500));
  EXPECT_EQ(seen.front(), 0x0002) << "异常应只隔离该事件,不中断消费者";
  EXPECT_EQ(business.exceptions(), 1u);
  business.Join();
}

// "不得永不超时"(SRS §3.1.4.4)的唯一保证:时限已无节点级缺省值,改由交互方法的参数
// 校验直接**拒绝**任何非正时限——比缺省值更硬(缺省值只在调用方省略时兜底)。本用例接手
// 原 `StartRejectsNonPositiveRequestTimeout` 对该要求的覆盖,断言对象由配置改为参数,并
// 补上负值一支(零值一支另见 ④ / ⑬)。
TEST(ProtocolNode, InteractionsRejectNonPositiveTimeout) {
  Fixture fx;

  auto negative_timeout =
      fx.node->RequestForResponse(Command(0x0010), {-1ms, 1});
  ASSERT_FALSE(negative_timeout);
  EXPECT_EQ(negative_timeout.error(),
            make_error_code(TransportErrc::kInvalidArgument));

  auto negative_result_timeout = fx.node->RequestForResult(
      Command(0x0010), {kCaseTimeout, 1}, 0x03F2, -1ms);
  ASSERT_FALSE(negative_result_timeout);
  EXPECT_EQ(negative_result_timeout.error(),
            make_error_code(TransportErrc::kInvalidArgument));

  auto zero_result_timeout = fx.node->RequestForResult(
      Command(0x0010), {kCaseTimeout, 1}, 0x03F2, 0ms);
  ASSERT_FALSE(zero_result_timeout);
  EXPECT_EQ(zero_result_timeout.error(),
            make_error_code(TransportErrc::kInvalidArgument));

  EXPECT_TRUE(fx.transport.sent().empty()) << "时限非正时一帧都不该发出";
}

// —— 5. 分段交互与旁路监听 ————————————————————————————————————————————

// 一次交互登记两段等待:各段各自设定时限,互不干扰。
TEST(ProtocolNode, SubscribeSupportsMultiPhaseInteraction) {
  Fixture fx;
  // 结果帧的命令码与请求不同,故按"任意会话 + 该命令码 + 结果"登记。
  auto result = fx.node->Subscribe({kAny, 0x03F2, FrameType::kResult});

  Coro::Result<Message> ack = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { ack = fx.node->RequestForResponse(Command(0x0010), {500ms, 1}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  // 第一段:回应。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  (void)caller.get();
  ASSERT_TRUE(ack) << ack.error().message();
  EXPECT_EQ(ack.value().payload, (std::vector<std::uint8_t>{1}));

  // 第二段:另一命令码的结果,由此前登记的订阅接住。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, 0x03F2, FrameType::kResult, {2})));
  auto got = result.Wait(500ms);
  ASSERT_TRUE(got) << got.error().message();
  EXPECT_EQ(got.value().payload, (std::vector<std::uint8_t>{2}));
}

// 旁路监听与精确等待同时命中同一条消息,各得一份。
TEST(ProtocolNode, SideChannelSubscriberAlsoReceivesMatchedResponse) {
  Fixture fx;
  auto audit = fx.node->Subscribe(transport::AnyOfType(FrameType::kResponse));

  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), {500ms, 1}); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {5})));
  (void)caller.get();

  ASSERT_TRUE(reply);
  auto observed = audit.Wait(200ms);
  ASSERT_TRUE(observed) << "旁路订阅者应另得一份副本";
  EXPECT_EQ(observed.value().payload, (std::vector<std::uint8_t>{5}));
}

// —— 6. 交互模式:RequestForResponse / RequestForResult(ADR-0010)————————————
//
// 两个方法的状态机全部活在**调用方 fiber 的局部变量**里(D1):节点不新增成员、不持有
// 在途交互表,Dispatcher 也不认识"模式"。以下用例逐条验证 ADR-0010 的可观察后果。

namespace {

/// 受理阶段的重发策略,写法收口以免各用例散落字面量。
transport::RetryPolicy Retry(std::chrono::milliseconds timeout, int attempts) {
  transport::RetryPolicy retry;
  retry.timeout = timeout;
  retry.max_attempts = attempts;
  return retry;
}

}  // namespace

// ① 一次成功:发命令 → 回 kResponse → 返回该帧。
TEST(ProtocolNode, RequestForResponseSucceedsOnFirstAttempt) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), Retry(500ms, 3)); });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  EXPECT_EQ(sent.frm_type, FrameType::kCommand);
  EXPECT_EQ(sent.protocol_id, kProtocolId);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {7, 7})));
  (void)caller.get();

  ASSERT_TRUE(reply) << reply.error().message();
  EXPECT_EQ(reply.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(reply.value().payload, (std::vector<std::uint8_t>{7, 7}));
  EXPECT_EQ(fx.transport.sent().size(), 1u) << "首次即受理,不应有重发";
}

// ② 重发:前 N−1 次不回,最后一次回 → 成功。断言实际发出 N 帧,且每帧 session_id 相同
//    (D3:重发的是字节完全相同的原帧,原订阅横跨全部重发继续有效)。
TEST(ProtocolNode, RequestForResponseRetransmitsSameFrameUntilAccepted) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask(
      [&] { reply = fx.node->RequestForResponse(Command(0x0010), Retry(60ms, 3)); });

  // 前两次尝试不回应,等第三帧发出。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return fx.transport.sent().size() >= 3u; }, 1000));
  const Message first = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(first.session_id, first.message_id, FrameType::kResponse, {3})));
  (void)caller.get();

  ASSERT_TRUE(reply) << reply.error().message();
  EXPECT_EQ(reply.value().payload, (std::vector<std::uint8_t>{3}));
  ASSERT_EQ(fx.transport.sent().size(), 3u) << "总发送次数应恰为 max_attempts";
  for (std::size_t i = 0; i < 3u; ++i) {
    const Message attempt = DecodeSent(fx.transport, i);
    EXPECT_EQ(attempt.session_id, first.session_id) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.message_id, first.message_id) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.frm_type, FrameType::kCommand) << "第 " << i << " 帧";
    EXPECT_EQ(fx.transport.sent()[i].bytes, fx.transport.sent()[0].bytes)
        << "重发的应是**字节完全相同**的原帧,第 " << i << " 帧";
  }
}

// ③ 次数耗尽 → kNotAccepted(**不是** kTimeout):其语义是"对端始终没有受理"(D12)。
TEST(ProtocolNode, RequestForResponseReturnsNotAcceptedWhenAttemptsExhausted) {
  Fixture fx;
  auto reply = fx.node->RequestForResponse(Command(0x0010), Retry(40ms, 2));

  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kNotAccepted));
  EXPECT_NE(reply.error(), make_error_code(TransportErrc::kTimeout))
      << "受理阶段耗尽与'已受理但没出结果'是两类事实";
  EXPECT_EQ(fx.transport.sent().size(), 2u) << "应恰好发出 max_attempts 帧";
}

// ④ 前置判据:策略非法 → kInvalidArgument;未 Start / 已关闭 → kClosed。
TEST(ProtocolNode, InteractionMethodsRejectInvalidPolicyAndClosedNode) {
  Fixture fx;
  auto zero_attempts = fx.node->RequestForResponse(Command(0x0010), Retry(50ms, 0));
  ASSERT_FALSE(zero_attempts);
  EXPECT_EQ(zero_attempts.error(), make_error_code(TransportErrc::kInvalidArgument));

  auto zero_timeout = fx.node->RequestForResponse(Command(0x0010), Retry(0ms, 3));
  ASSERT_FALSE(zero_timeout);
  EXPECT_EQ(zero_timeout.error(), make_error_code(TransportErrc::kInvalidArgument));

  auto bad_result_timeout =
      fx.node->RequestForResult(Command(0x0010), Retry(50ms, 3), 0x03F2, 0ms);
  ASSERT_FALSE(bad_result_timeout);
  EXPECT_EQ(bad_result_timeout.error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_TRUE(fx.transport.sent().empty()) << "判据不通过时一帧都不该发出";

  // 未 Start 的节点。
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode fresh(fake, MakeCodec(), BaseConfig());
  auto before_start = fresh.RequestForResponse(Command(0x0010), Retry(50ms, 3));
  ASSERT_FALSE(before_start);
  EXPECT_EQ(before_start.error(), make_error_code(TransportErrc::kClosed));

  // 已关闭的节点。
  ASSERT_TRUE(fresh.Start());
  ASSERT_TRUE(fresh.Close());
  fresh.WaitClosed();
  auto after_close =
      fresh.RequestForResult(Command(0x0010), Retry(50ms, 3), 0x03F2, 50ms);
  ASSERT_FALSE(after_close);
  EXPECT_EQ(after_close.error(), make_error_code(TransportErrc::kClosed));
  (void)fake.Close();
}

// ⑤ 完整链路:命令 → 受理 → 结果 → **我方回发一帧 kResponse**。
//    该回应帧完全由收到的 kResult 派生(D8 / RT_NODE_002_f):payload 原样回显,
//    session_id / message_id 沿用,**仅**帧类型改为 kResponse。
TEST(ProtocolNode, RequestForResultRepliesWithDerivedResponseFrame) {
  Fixture fx;
  constexpr std::uint16_t kResultId = 0x03F2;
  const std::vector<std::uint8_t> kResultPayload{4, 2, 0};

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome =
        fx.node->RequestForResult(Command(0x0010), Retry(500ms, 3), kResultId, 500ms);
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, kResultId, FrameType::kResult, kResultPayload)));
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResult) << "返回的是结果那一帧";
  EXPECT_EQ(outcome.value().payload, kResultPayload);

  ASSERT_EQ(fx.transport.sent().size(), 2u) << "命令 + 回应结果,各一帧";
  const Message reply = DecodeSent(fx.transport, 1);
  EXPECT_EQ(reply.frm_type, FrameType::kResponse) << "仅此一处相对 kResult 有改动";
  EXPECT_EQ(reply.session_id, sent.session_id) << "session_id 沿用";
  EXPECT_EQ(reply.message_id, kResultId) << "message_id 沿用结果帧的";
  EXPECT_EQ(reply.payload, kResultPayload) << "payload 原样回显";
  EXPECT_EQ(reply.protocol_id, kProtocolId);
}

// ⑥ kResult **先于** kResponse 到达仍能成功——D4 的行为证据:两个订阅一起在发命令之前
//    登记;若改成"收到受理再登记结果订阅",先到的结果帧会因无匹配而被丢弃,本用例必失败。
TEST(ProtocolNode, RequestForResultAcceptsResultArrivingBeforeAck) {
  Fixture fx;
  constexpr std::uint16_t kResultId = 0x03F2;

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome =
        fx.node->RequestForResult(Command(0x0010), Retry(500ms, 3), kResultId, 500ms);
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  // **先**结果、**后**受理。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, kResultId, FrameType::kResult, {8})));
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{8}));
  ASSERT_EQ(fx.transport.sent().size(), 2u);
  EXPECT_EQ(DecodeSent(fx.transport, 1).frm_type, FrameType::kResponse);
}

// ⑦ 受理后等结果超时 → kTimeout(区别于 ③ 的 kNotAccepted),且**不发生重发**(D2:
//    kResult 未达意味着对端正在执行,重发有使其重复执行的风险)。
TEST(ProtocolNode, RequestForResultTimesOutWithoutRetransmittingAfterAck) {
  Fixture fx;
  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome =
        fx.node->RequestForResult(Command(0x0010), Retry(500ms, 3), 0x03F2, 80ms);
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  (void)caller.get();

  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_NE(outcome.error(), make_error_code(TransportErrc::kNotAccepted))
      << "已受理,失败点在第二阶段";
  EXPECT_EQ(fx.transport.sent().size(), 1u)
      << "等结果阶段不得重发,失败时也不回应结果";
}

// ⑧ 受理阶段的**重复 kResponse**(重发引起)在该阶段完成后到达 → 归因
//    kUnmatchedOrLateResponse。这是 D5"阶段一完成后立即注销 ack 订阅"的行为证据:
//    不注销则重复回应继续落入信箱、被后续逻辑误读。
TEST(ProtocolNode, DuplicateAckAfterAcceptPhaseIsAttributedAsUnmatched) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));
  constexpr std::uint16_t kResultId = 0x03F2;

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome =
        fx.node->RequestForResult(Command(0x0010), Retry(500ms, 3), kResultId, 500ms);
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  // 让调用方 fiber 走完受理阶段(含 ack.Reset())再投重复回应。
  boost::this_fiber::sleep_for(30ms);
  ASSERT_EQ(DropCount(sink), 0u) << "首个受理帧被认领,不是丢弃";

  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  EXPECT_TRUE(testutil::pumpFiberUntil(
      [&] { return SawDrop(sink, "unmatched-or-late-response"); }, 500))
      << "阶段完成后的重复受理帧应无匹配、按迟到终结帧归因";

  // 收尾:交付结果,让交互正常终结。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, kResultId, FrameType::kResult, {9})));
  (void)caller.get();
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{9}));
}

// —— 7. 另一种协议的直取结果交互:RequestForResultDirect(ADR-0010 D13 / RT_NODE_002_g)——
//
// 本组与第 6 组**分属两种协议**。两条与 `RequestForResult` 恰好相反的规则各有一条用例作为
// 行为分界证据:⑩ 证明"等结果阶段确实重发"(对比 ⑦ 的"不得重发"),⑪ 证明"失败返
// kTimeout 而非 kNotAccepted"(对比 ③)。此外 ⑨ 证明"收到结果后不回应"(对比 ⑤ 的 D8
// 末步),⑫ 证明"本交互不订阅受理帧,中途到达的 kResponse 无匹配、按迟到终结帧归因丢弃"。

// ⑨ 一次成功:发命令 → 回 kResult → 返回该帧,且**我方未回发任何帧**。
//    与 ⑤ 对照:`RequestForResult` 收到结果后必回一帧 kResponse(D8),本交互没有这一步。
TEST(ProtocolNode, RequestForResultDirectSucceedsAndSendsNoReply) {
  Fixture fx;
  constexpr std::uint16_t kResultId = 0x03F2;
  const std::vector<std::uint8_t> kResultPayload{4, 2, 0};

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome =
        fx.node->RequestForResultDirect(Command(0x0010), Retry(500ms, 3), kResultId);
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);
  EXPECT_EQ(sent.frm_type, FrameType::kCommand);
  EXPECT_EQ(sent.protocol_id, kProtocolId);
  // **不发受理帧**:本交互没有受理阶段,直接投结果。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, kResultId, FrameType::kResult, kResultPayload)));
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResult) << "返回的是结果那一帧";
  EXPECT_EQ(outcome.value().payload, kResultPayload);
  EXPECT_EQ(outcome.value().session_id, sent.session_id);
  EXPECT_EQ(fx.transport.sent().size(), 1u)
      << "只有那一条命令帧:收到 kResult 后**不回应任何帧**(与 RequestForResult 相反)";
}

// ⑩ **等结果阶段确实重发**——本交互与 `RequestForResult` 的行为分界证据之一
//    (D13 / RT_NODE_002_g)。前 N−1 次不回结果,最后一次回 → 成功;断言实际发出 N 帧且
//    逐帧 session_id 相同(D3)。对比 ⑦:`RequestForResult` 在等 kResult 时**不得**重发,
//    那条规则(RT_NODE_002_c)只约束外部系统协议,不是框架的普遍规则。
TEST(ProtocolNode, RequestForResultDirectRetransmitsWhileAwaitingResult) {
  Fixture fx;
  constexpr std::uint16_t kResultId = 0x03F2;

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome =
        fx.node->RequestForResultDirect(Command(0x0010), Retry(60ms, 3), kResultId);
  });

  // 前两次尝试不投结果,等第三帧发出——这正是"在唯一的等待阶段重发"。
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return fx.transport.sent().size() >= 3u; }, 1000));
  const Message first = DecodeSent(fx.transport, 0);
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(first.session_id, kResultId, FrameType::kResult, {3})));
  (void)caller.get();

  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{3}));
  ASSERT_EQ(fx.transport.sent().size(), 3u)
      << "总发送次数应恰为 max_attempts——等结果阶段重发了,且收到结果后不回应";
  for (std::size_t i = 0; i < 3u; ++i) {
    const Message attempt = DecodeSent(fx.transport, i);
    EXPECT_EQ(attempt.session_id, first.session_id)
        << "逐帧同一 session_id,第 " << i << " 帧";
    EXPECT_EQ(attempt.message_id, first.message_id) << "第 " << i << " 帧";
    EXPECT_EQ(attempt.frm_type, FrameType::kCommand) << "第 " << i << " 帧";
    EXPECT_EQ(fx.transport.sent()[i].bytes, fx.transport.sent()[0].bytes)
        << "重发的应是**字节完全相同**的原帧,第 " << i << " 帧";
  }
}

// ⑪ 次数耗尽 → **kTimeout**(**不是** kNotAccepted)——行为分界证据之二(D12):
//    kNotAccepted 的语义是"对端没有受理",而本交互根本不存在受理这一步。
TEST(ProtocolNode, RequestForResultDirectReturnsTimeoutWhenAttemptsExhausted) {
  Fixture fx;
  auto outcome =
      fx.node->RequestForResultDirect(Command(0x0010), Retry(40ms, 2), 0x03F2);

  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_NE(outcome.error(), make_error_code(TransportErrc::kNotAccepted))
      << "本交互没有受理阶段,'未受理'这一事实不存在";
  EXPECT_EQ(fx.transport.sent().size(), 2u)
      << "应恰好发出 max_attempts 帧,且失败时不回应任何帧";
}

// ⑫ 中途到达的 kResponse **不影响本交互**:本交互不订阅受理帧,故该帧无匹配、按
//    kUnmatchedOrLateResponse 归因丢弃,交互继续等结果并正常成功。
TEST(ProtocolNode, RequestForResultDirectIgnoresInterveningResponseFrame) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));
  constexpr std::uint16_t kResultId = 0x03F2;

  Coro::Result<Message> outcome = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] {
    outcome = fx.node->RequestForResultDirect(Command(0x0010), Retry(2000ms, 1),
                                              kResultId);
  });

  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  // 一条"同会话、同命令码"的受理帧——若本交互登记了 ack 订阅,它会被认领。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResponse, {1})));
  EXPECT_TRUE(testutil::pumpFiberUntil(
      [&] { return SawDrop(sink, "unmatched-or-late-response"); }, 500))
      << "本交互不订阅受理帧,该帧应无匹配、按迟到终结帧归因丢弃";
  EXPECT_FALSE(outcome) << "交互不应被受理帧终结,应继续等结果";
  EXPECT_EQ(fx.transport.sent().size(), 1u) << "受理帧不引起任何我方动作";

  // 交互仍在等结果:投结果后正常成功。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, kResultId, FrameType::kResult, {9})));
  (void)caller.get();
  ASSERT_TRUE(outcome) << outcome.error().message();
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{9}));
  EXPECT_EQ(fx.transport.sent().size(), 1u) << "成功后仍不回应任何帧";
}

// ⑬ 前置判据(D6 的 2026-08-26 补记):策略非法 → kInvalidArgument;未 Start / 已关闭 →
//    kClosed。本方法只有一个等待阶段,其时限即 RetryPolicy::timeout,无独立 result_timeout。
TEST(ProtocolNode, RequestForResultDirectRejectsInvalidPolicyAndClosedNode) {
  Fixture fx;
  auto zero_attempts =
      fx.node->RequestForResultDirect(Command(0x0010), Retry(50ms, 0), 0x03F2);
  ASSERT_FALSE(zero_attempts);
  EXPECT_EQ(zero_attempts.error(), make_error_code(TransportErrc::kInvalidArgument));

  auto negative_attempts =
      fx.node->RequestForResultDirect(Command(0x0010), Retry(50ms, -1), 0x03F2);
  ASSERT_FALSE(negative_attempts);
  EXPECT_EQ(negative_attempts.error(),
            make_error_code(TransportErrc::kInvalidArgument));

  auto zero_timeout =
      fx.node->RequestForResultDirect(Command(0x0010), Retry(0ms, 3), 0x03F2);
  ASSERT_FALSE(zero_timeout);
  EXPECT_EQ(zero_timeout.error(), make_error_code(TransportErrc::kInvalidArgument));

  auto negative_timeout =
      fx.node->RequestForResultDirect(Command(0x0010), Retry(-5ms, 3), 0x03F2);
  ASSERT_FALSE(negative_timeout);
  EXPECT_EQ(negative_timeout.error(),
            make_error_code(TransportErrc::kInvalidArgument));
  EXPECT_TRUE(fx.transport.sent().empty()) << "判据不通过时一帧都不该发出";

  // 未 Start 的节点。
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode fresh(fake, MakeCodec(), BaseConfig());
  auto before_start =
      fresh.RequestForResultDirect(Command(0x0010), Retry(50ms, 3), 0x03F2);
  ASSERT_FALSE(before_start);
  EXPECT_EQ(before_start.error(), make_error_code(TransportErrc::kClosed));

  // 已关闭的节点。
  ASSERT_TRUE(fresh.Start());
  ASSERT_TRUE(fresh.Close());
  fresh.WaitClosed();
  auto after_close =
      fresh.RequestForResultDirect(Command(0x0010), Retry(50ms, 3), 0x03F2);
  ASSERT_FALSE(after_close);
  EXPECT_EQ(after_close.error(), make_error_code(TransportErrc::kClosed));
  (void)fake.Close();
}
