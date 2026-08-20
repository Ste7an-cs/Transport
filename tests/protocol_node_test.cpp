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

ProtocolNodeConfig BaseConfig() {
  ProtocolNodeConfig config;
  config.protocol_id = kProtocolId;
  config.default_request_timeout = 200ms;
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
  auto caller = Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010)); });

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

TEST(ProtocolNode, RequestTimesOutWithoutResponse) {
  Fixture fx;
  auto reply = fx.node->Request(Command(0x0010), 80ms);
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kTimeout));
}

// 三个匹配字段任缺其一都不终结该请求。
TEST(ProtocolNode, ResponseWithMismatchedSessionDoesNotTerminateRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller =
      Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010), 120ms); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(static_cast<std::uint8_t>(sent.session_id + 1), sent.message_id,
                  FrameType::kResponse)));
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kTimeout));
}

TEST(ProtocolNode, ResponseWithMismatchedFrameTypeDoesNotTerminateRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller =
      Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010), 120ms); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  // 同会话、同命令码,但类型是结果而非回应。
  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, sent.message_id, FrameType::kResult)));
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kTimeout));
}

TEST(ProtocolNode, ResponseWithMismatchedMessageIdDoesNotTerminateRequest) {
  Fixture fx;
  Coro::Result<Message> reply = make_error_code(TransportErrc::kInternal);
  auto caller =
      Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010), 120ms); });
  ASSERT_TRUE(
      testutil::pumpFiberUntil([&] { return !fx.transport.sent().empty(); }, 500));
  const Message sent = DecodeSent(fx.transport, 0);

  ASSERT_TRUE(fx.transport.Deliver(
      EncodeFrame(sent.session_id, static_cast<std::uint16_t>(sent.message_id + 1),
                  FrameType::kResponse)));
  (void)caller.get();
  ASSERT_FALSE(reply);
  EXPECT_EQ(reply.error(), make_error_code(TransportErrc::kTimeout));
}

// 并发的两个请求各自拿到自己的响应:session_id 是二者的唯一区分。
// 以回声(响应原样带回请求的 payload)判定归属,不依赖两个 fiber 的启动先后。
TEST(ProtocolNode, ConcurrentRequestsAreCorrelatedIndependently) {
  Fixture fx;
  Coro::Result<Message> first = make_error_code(TransportErrc::kInternal);
  Coro::Result<Message> second = make_error_code(TransportErrc::kInternal);
  auto a = Coro::makeTask(
      [&] { first = fx.node->Request(Command(0x0010, {0xAA}), 500ms); });
  auto b = Coro::makeTask(
      [&] { second = fx.node->Request(Command(0x0010, {0xBB}), 500ms); });

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
  auto caller = Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010)); });
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
  auto caller =
      Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010), 5000ms); });
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

  EXPECT_EQ(node.Request(Command(0x0001)).error(),
            make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.Send(Command(0x0001)).error(),
            make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(node.Start());
  ASSERT_TRUE(node.Close());
  node.WaitClosed();
  EXPECT_EQ(node.Request(Command(0x0001)).error(),
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

TEST(ProtocolNode, StartRejectsNonPositiveRequestTimeout) {
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNodeConfig config = BaseConfig();
  config.default_request_timeout = 0ms;
  ProtocolNode node(fake, MakeCodec(), std::move(config));

  auto started = node.Start();
  ASSERT_FALSE(started);
  EXPECT_EQ(started.error(), make_error_code(TransportErrc::kConfiguration));
  EXPECT_FALSE(node.IsRunning()) << "配置非法应停在 Created,允许改配后重试";
  (void)fake.Close();
}

// —— 5. 分段交互与旁路监听 ————————————————————————————————————————————

// 一次交互登记两段等待:各段各自设定时限,互不干扰。
TEST(ProtocolNode, SubscribeSupportsMultiPhaseInteraction) {
  Fixture fx;
  // 结果帧的命令码与请求不同,故按"任意会话 + 该命令码 + 结果"登记。
  auto result = fx.node->Subscribe({kAny, 0x03F2, FrameType::kResult});

  Coro::Result<Message> ack = make_error_code(TransportErrc::kInternal);
  auto caller = Coro::makeTask([&] { ack = fx.node->Request(Command(0x0010), 500ms); });
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
  auto caller =
      Coro::makeTask([&] { reply = fx.node->Request(Command(0x0010), 500ms); });
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
