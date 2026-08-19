// ProtocolNode 单元测试(重设计后的形态)。
//
// 在 fiber 调度器(coro_test_main)内,以假传输 + SystemDatagramCodec 驱动,覆盖五组事实:
//
//   1. 出站盖章:frm_type / protocol_id / session_id 的填写与 session_id 的循环递增;
//   2. 请求-响应关联:由 Dispatcher 按"会话 + 命令码 + 帧类型"三字段匹配,任一不符即不
//      终结该请求;
//   3. 分发去向:命中订阅者的消息不再进入业务队列;未命中的终结帧归因丢弃,其余业务帧
//      交入站处理器;
//   4. 生命周期:节点关闭令在途请求恰好终结一次;传输终结使节点自行关闭;节点不启停传输;
//   5. 分段交互:`Subscribe` 允许一次交互登记多段等待,各段各自设定时限。
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include <boost/fiber/operations.hpp>
#include <gtest/gtest.h>

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
using transport::HandlerContext;
using transport::kAny;
using transport::Message;
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

// —— 3. 分发去向 ————————————————————————————————————————————————————

TEST(ProtocolNode, BusinessFrameIsDeliveredToHandler) {
  std::vector<std::uint16_t> seen;
  ProtocolNodeConfig config = BaseConfig();
  config.handler = [&seen](const Message& msg, HandlerContext&) -> Coro::Result<void> {
    seen.push_back(msg.message_id);
    return Coro::Result<void>{};
  };
  Fixture fx(std::move(config));

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(3, 0x0055, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return !seen.empty(); }, 500));
  EXPECT_EQ(seen.front(), 0x0055);
}

// 命中订阅者的消息不再进入业务队列:处理器只见到无人认领的业务帧。
TEST(ProtocolNode, MatchedResponseDoesNotReachHandler) {
  std::vector<FrameType> seen;
  ProtocolNodeConfig config = BaseConfig();
  config.handler = [&seen](const Message& msg, HandlerContext&) -> Coro::Result<void> {
    seen.push_back(msg.frm_type);
    return Coro::Result<void>{};
  };
  Fixture fx(std::move(config));

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
  EXPECT_TRUE(seen.empty()) << "已被订阅者认领的响应不应再进入业务队列";
}

// 无人认领的终结帧归因丢弃,且不转交处理器。
TEST(ProtocolNode, UnmatchedTerminalFrameIsDroppedNotHandled) {
  transport::CapturingTraceSink sink;
  std::vector<FrameType> seen;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  config.handler = [&seen](const Message& msg, HandlerContext&) -> Coro::Result<void> {
    seen.push_back(msg.frm_type);
    return Coro::Result<void>{};
  };
  Fixture fx(std::move(config));

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(9, 0x0099, FrameType::kResponse)));
  ASSERT_TRUE(testutil::pumpFiberUntil(
      [&] { return SawDrop(sink, "unmatched-or-late-response"); }, 500))
      << "未归因 unmatched-or-late-response";
  EXPECT_TRUE(seen.empty()) << "终结帧不应转交业务处理器";
}

// 未配置处理器时业务帧归因丢弃。
TEST(ProtocolNode, BusinessFrameIsDroppedWhenNoHandlerConfigured) {
  transport::CapturingTraceSink sink;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  Fixture fx(std::move(config));

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0033, FrameType::kCommand)));
  EXPECT_TRUE(testutil::pumpFiberUntil(
      [&] { return SawDrop(sink, "no-handler-configured"); }, 500));
}

// 坏帧由 codec 判定,归因丢弃且不影响后续报文。
TEST(ProtocolNode, BadFrameIsDroppedAndDoesNotBlockLaterFrames) {
  transport::CapturingTraceSink sink;
  std::vector<std::uint16_t> seen;
  ProtocolNodeConfig config = BaseConfig();
  config.trace_sink = &sink;
  config.handler = [&seen](const Message& msg, HandlerContext&) -> Coro::Result<void> {
    seen.push_back(msg.message_id);
    return Coro::Result<void>{};
  };
  Fixture fx(std::move(config));

  ASSERT_TRUE(fx.transport.Deliver({0xDE, 0xAD, 0xBE, 0xEF}));  // 非法帧
  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0044, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return !seen.empty(); }, 500));
  EXPECT_EQ(seen.front(), 0x0044) << "坏帧之后的报文仍应正常分发";
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

// 入站处理器可请求关闭本节点:Close 只发信号、不含等待点,故在处理器 fiber 内调用安全。
TEST(ProtocolNode, HandlerCanRequestClose) {
  ProtocolNodeConfig config = BaseConfig();
  config.handler = [](const Message&, HandlerContext& ctx) -> Coro::Result<void> {
    return ctx.RequestClose();
  };
  FakeTransport fake;
  ASSERT_TRUE(fake.Start());
  ProtocolNode node(fake, MakeCodec(), std::move(config));
  ASSERT_TRUE(node.Start());

  ASSERT_TRUE(fake.Deliver(EncodeFrame(1, 0x0066, FrameType::kCommand)));
  EXPECT_TRUE(testutil::pumpFiberUntil([&] { return !node.IsRunning(); }, 500));
  node.WaitClosed();
  (void)fake.Close();
}

// 处理器抛出的异常被边界兜住:隔离该事件,节点继续处理后续报文。
TEST(ProtocolNode, HandlerExceptionIsIsolated) {
  std::vector<std::uint16_t> seen;
  ProtocolNodeConfig config = BaseConfig();
  config.handler = [&seen](const Message& msg, HandlerContext&) -> Coro::Result<void> {
    if (msg.message_id == 0x0001) {
      throw std::runtime_error("boom");
    }
    seen.push_back(msg.message_id);
    return Coro::Result<void>{};
  };
  Fixture fx(std::move(config));

  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0001, FrameType::kCommand)));
  ASSERT_TRUE(fx.transport.Deliver(EncodeFrame(1, 0x0002, FrameType::kCommand)));
  ASSERT_TRUE(testutil::pumpFiberUntil([&] { return !seen.empty(); }, 500));
  EXPECT_EQ(seen.front(), 0x0002) << "异常应只隔离该事件,不中断消费者";
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
