// -----------------------------------------------------------------------------
// protocol_node_handler_test.cpp — P2-3 入站 handler + 有界队列 + noresponse Send
//
// 验收(RT_HANDLER_001–006 / RT_ERROR_001 / RT_TRANSPORT_008 / ADR-0003 D10):
//   · 多业务帧 → handler 单消费者严格串行(handler await 时不启动下一条)。
//   · handler 经 ctx.Send 回帧 → 对端(Fake sent())收到。
//   · 队列满 → tail-drop + business_queue_overflow 计数,且响应匹配照常。
//   · 未设 handler → 业务帧仍 dropped_no_handler(P1 行为不变)。
//   · handler 抛异常 → 转 kInternal 隔离、node 不关、继续下一条。
//   · node.Send noresponse → 帧上线、不登记 PendingTable(PendingCount/Size 不增)。
//   · Close → 消费者 fiber 干净退出、WaitClosed 完成。
//
// 用 FakeCoroTransport 作传输、真实 SystemCodec 作 codec;测试 fiber 用 pumpFiberUntil
// 驱动(coro_test_main 范式),被测 fiber(Request/Send)用 makeTask 起独立 fiber。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/node/ProtocolNode.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::CapturingTraceSink;
using transport::Datagram;
using transport::DropReason;
using transport::DropReasonName;
using transport::FrameType;
using transport::HandlerContext;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::Result;
using transport::SystemCodec;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 过滤出 category=="drop" 的记录:sink 同时收 P5-3 的丢弃事件与 P5-4 的 send/recv/
// decode/handler/close 等事件(共用同一 trace_sink),按 category 过滤才是"这次丢弃
// 恰好一条 Trace"断言的正确写法,不能假设 sink 总记录数等于丢弃数。
std::vector<CapturingTraceSink::Record> DropRecords(
    const std::vector<CapturingTraceSink::Record>& records) {
  std::vector<CapturingTraceSink::Record> out;
  for (const auto& rec : records) {
    if (rec.category == "drop") {
      out.push_back(rec);
    }
  }
  return out;
}

// 构造一个业务帧 Datagram(非 kResponse/kResult):经 SystemCodec 编成字节喂读循环。
Datagram MakeBusinessDatagram(FrameType frm_type, std::uint8_t session_id,
                              std::uint16_t message_id,
                              std::vector<std::uint8_t> payload = {}) {
  Message msg;
  msg.frm_type = frm_type;
  msg.session_id = session_id;
  msg.message_id = message_id;
  msg.payload = std::move(payload);
  SystemCodec wire;
  auto bytes = wire.Encode(msg);
  EXPECT_TRUE(bytes);
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

// 构造一个响应帧 Datagram(kResponse):用于验证 handler 阻塞时响应匹配照常。
Datagram MakeResponseDatagram(std::uint8_t session_id, std::uint16_t message_id,
                              std::vector<std::uint8_t> payload) {
  Message resp;
  resp.frm_type = FrameType::kResponse;
  resp.session_id = session_id;
  resp.message_id = message_id;
  resp.payload = std::move(payload);
  SystemCodec wire;
  auto bytes = wire.Encode(resp);
  EXPECT_TRUE(bytes);
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

// 一个可被测试逐个放行的门闩序列:handler 每次调用创建一个门并 await 它,测试按序放行,
// 借此断言严格串行(前一条未放行前后一条不出队)。
struct GateBank {
  std::mutex mutex;
  std::deque<std::shared_ptr<Coro::Awaitable<void>>> gates;
  int entered = 0;
  int finished = 0;

  std::shared_ptr<Coro::Awaitable<void>> Enter() {
    auto gate = std::make_shared<Coro::Awaitable<void>>();
    std::lock_guard<std::mutex> lock(mutex);
    ++entered;
    gates.push_back(gate);
    return gate;
  }
  void Leave() {
    std::lock_guard<std::mutex> lock(mutex);
    ++finished;
  }
  int Entered() {
    std::lock_guard<std::mutex> lock(mutex);
    return entered;
  }
  int Finished() {
    std::lock_guard<std::mutex> lock(mutex);
    return finished;
  }
  // 放行最早等待的门(FIFO);无门可放则返回 false。
  bool Release() {
    std::shared_ptr<Coro::Awaitable<void>> gate;
    {
      std::lock_guard<std::mutex> lock(mutex);
      if (gates.empty()) {
        return false;
      }
      gate = gates.front();
      gates.pop_front();
    }
    gate->resolve();
    gate->close();
    return true;
  }
};

}  // namespace

// handler 单消费者严格串行:注入 3 条业务帧,handler 内 await 门闩;放行前只有 1 条进入
// (第 2 条不出队),逐个放行后依次推进(RT_HANDLER_003)。
TEST(ProtocolNodeHandler, HandlersRunStrictlySerial) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  GateBank bank;
  ProtocolNodeConfig config;
  config.handler = [&bank](const Message&, HandlerContext&) -> Coro::Result<void> {
    auto gate = bank.Enter();
    Coro::await(gate);
    bank.Leave();
    return Coro::Result<void>{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0002));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0003));

  // 只有第 1 条进入 handler;第 2/3 条在队列里等,不启动(串行)。
  ASSERT_TRUE(pumpFiberUntil([&] { return bank.Entered() == 1; }));
  // 稳定性:再泵一会儿仍只进入 1 条(await 中不启动下一条)。
  pumpFiberUntil([&] { return false; }, 30);
  EXPECT_EQ(bank.Entered(), 1);
  EXPECT_EQ(bank.Finished(), 0);

  ASSERT_TRUE(bank.Release());  // 放行第 1 条 → 第 2 条得以出队进入。
  ASSERT_TRUE(pumpFiberUntil([&] { return bank.Entered() == 2; }));
  EXPECT_EQ(bank.Finished(), 1);
  pumpFiberUntil([&] { return false; }, 30);
  EXPECT_EQ(bank.Entered(), 2);  // 第 3 条仍未启动。

  ASSERT_TRUE(bank.Release());
  ASSERT_TRUE(pumpFiberUntil([&] { return bank.Entered() == 3; }));
  ASSERT_TRUE(bank.Release());
  ASSERT_TRUE(pumpFiberUntil([&] { return bank.Finished() == 3; }));

  node.Close();
}

// handler 经 ctx.Send 回帧 → 对端(Fake sent())收到,且解码 payload 一致。
TEST(ProtocolNodeHandler, HandlerRepliesViaContextSend) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.handler = [](const Message& msg, HandlerContext& ctx) -> Coro::Result<void> {
    Message reply;
    reply.message_id = 0x00AA;
    reply.payload = msg.payload;  // 回显收到的 payload。
    return ctx.Send(std::move(reply));
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 7, 0x0009, {0x5A, 0x6B}));
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));

  auto sent = fake->sent();
  ASSERT_EQ(sent.size(), 1u);
  SystemCodec wire;
  auto decoded = wire.Decode(sent[0].bytes.data(), sent[0].bytes.size());
  ASSERT_TRUE(decoded);
  ASSERT_EQ(decoded.value().size(), 1u);
  const Message& out = decoded.value().front();
  EXPECT_EQ(out.message_id, 0x00AA);
  EXPECT_EQ(out.frm_type, FrameType::kCommand);  // Send 默认盖命令帧。
  EXPECT_EQ(out.payload, (std::vector<std::uint8_t>{0x5A, 0x6B}));
  // fire-and-forget:未登记 PendingTable。
  EXPECT_EQ(node.PendingCount(), 0u);

  node.Close();
}

// 队列满 → tail-drop + business_queue_overflow 计数;handler 阻塞期间响应匹配照常
// (RT_HANDLER_004)。
TEST(ProtocolNodeHandler, QueueOverflowTailDropsWhileResponsesStillMatch) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  GateBank bank;
  ProtocolNodeConfig config;
  config.business_queue_max_events = 2;  // 小容量,便于溢出。
  config.handler = [&bank](const Message&, HandlerContext&) -> Coro::Result<void> {
    auto gate = bank.Enter();
    Coro::await(gate);
    bank.Leave();
    return Coro::Result<void>{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 起一个在途请求(session_id=0),用于验证响应匹配不被 handler 阻塞。
  Coro::Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));

  // 第 1 条被消费者取走并卡在 handler(队列空);随后灌满 + 溢出。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0011));
  ASSERT_TRUE(pumpFiberUntil([&] { return bank.Entered() == 1; }));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0012));  // 入队(size 1)
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0013));  // 入队(size 2 满)
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 4, 0x0014));  // 满 → tail-drop
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 5, 0x0015));  // 满 → tail-drop

  ASSERT_TRUE(pumpFiberUntil([&] { return node.BusinessQueueOverflowCount() == 2u; }));
  EXPECT_EQ(node.BusinessQueueOverflowCount(), 2u);

  // 响应匹配照常:注入 session=0 的匹配响应 → 在途请求完成(handler 仍阻塞中)。
  fake->Inject(MakeResponseDatagram(0, 0x1002, {0x01}));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome);
  EXPECT_EQ(outcome.value().message_id, 0x1002);
  EXPECT_EQ(bank.Finished(), 0);  // handler 全程未被放行,仍卡着。

  // 放行门闩让消费者收尾,再关闭。
  while (bank.Release()) {
    pumpFiberUntil([&] { return false; }, 5);
  }
  node.Close();
  EXPECT_TRUE(request.get());
}

// P5-3(issue #88):kBusinessQueueOverflow 定义点在 BoundedQueue::Push 内(经
// HandlerLoop 构造业务队列时透传 config.trace_sink)——配置 trace_sink 时,队列满
// tail-drop 应同步产生可辨识的 TraceEvent,且 BusinessQueueOverflowCount()(代理
// BoundedQueue::DroppedCount())不变。沿用上一用例(GateBank 卡住消费者)确定化溢出的
// 拓扑,只是额外挂了 sink 断言。
TEST(ProtocolNodeHandler, QueueOverflowWithSinkEmitsDropTraceForEachDrop) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  GateBank bank;
  CapturingTraceSink sink;
  ProtocolNodeConfig config;
  config.business_queue_max_events = 2;  // 小容量,便于溢出。
  config.trace_sink = &sink;
  config.handler = [&bank](const Message&, HandlerContext&) -> Coro::Result<void> {
    auto gate = bank.Enter();
    Coro::await(gate);
    bank.Leave();
    return Coro::Result<void>{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 第 1 条被消费者取走并卡在 handler(队列空);随后灌满 + 溢出 2 条。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0011));
  ASSERT_TRUE(pumpFiberUntil([&] { return bank.Entered() == 1; }));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0012));  // 入队(size 1)
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0013));  // 入队(size 2 满)
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 4, 0x0014));  // 满 → tail-drop
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 5, 0x0015));  // 满 → tail-drop

  ASSERT_TRUE(pumpFiberUntil([&] { return node.BusinessQueueOverflowCount() == 2u; }));
  EXPECT_EQ(node.BusinessQueueOverflowCount(), 2u);

  const auto records = DropRecords(sink.Records());
  // 每次 tail-drop 恰好一条 Trace(RecordDrop 与 DroppedCount 同一临界区同步 +1)。
  ASSERT_EQ(records.size(), 2u);
  for (const auto& rec : records) {
    EXPECT_EQ(rec.category, "drop");
    EXPECT_EQ(rec.message, DropReasonName(DropReason::kBusinessQueueOverflow));
  }

  while (bank.Release()) {
    pumpFiberUntil([&] { return false; }, 5);
  }
  node.Close();
}

// 未设 handler → 业务帧仍归因 dropped_no_handler(P1 行为不变),不入队。
TEST(ProtocolNodeHandler, NoHandlerKeepsDroppedNoHandlerBehavior) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  fake->Inject(MakeBusinessDatagram(FrameType::kCommand, 2, 0x0002));

  ASSERT_TRUE(pumpFiberUntil([&] { return node.DroppedNoHandlerCount() == 2u; }));
  EXPECT_EQ(node.DroppedNoHandlerCount(), 2u);
  EXPECT_EQ(node.BusinessQueueOverflowCount(), 0u);

  node.Close();
}

// handler 抛异常 → 边界转 kInternal 隔离当前事件、node 不关、继续下一条(RT_HANDLER_006)。
TEST(ProtocolNodeHandler, HandlerExceptionIsolatedAndLoopContinues) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  int calls = 0;
  bool second_seen = false;
  ProtocolNodeConfig config;
  config.handler = [&](const Message&, HandlerContext&) -> Coro::Result<void> {
    ++calls;
    if (calls == 1) {
      throw std::runtime_error("handler boom");  // 逃逸异常。
    }
    second_seen = true;
    return Coro::Result<void>{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0002));

  ASSERT_TRUE(pumpFiberUntil([&] { return second_seen; }));
  EXPECT_EQ(node.HandlerExceptionCount(), 1u);  // 首条异常被兜住计数。
  EXPECT_EQ(calls, 2);                            // 继续跑了第 2 条。

  // node 未自关:仍可正常关闭(WaitClosed 完成)。
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
}

// node.Send noresponse:帧上线(Fake sent()),不登记 PendingTable(PendingCount 不增)。
TEST(ProtocolNodeHandler, NoresponseSendDoesNotRegisterPending) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  Coro::Result<void> result = make_error_code(TransportErrc::kInternal);
  bool done = false;
  auto sender = Coro::makeTask([&] {
    Message msg;
    msg.message_id = 0x0033;
    msg.payload = {0xDE, 0xAD};
    result = node.Send(std::move(msg));
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(result);
  ASSERT_EQ(fake->sent().size(), 1u);
  // 不登记 PendingTable:在途请求数不增。
  EXPECT_EQ(node.PendingCount(), 0u);

  // 解码上线帧核对盖章。
  auto sent = fake->sent();
  SystemCodec wire;
  auto decoded = wire.Decode(sent[0].bytes.data(), sent[0].bytes.size());
  ASSERT_TRUE(decoded);
  ASSERT_EQ(decoded.value().size(), 1u);
  EXPECT_EQ(decoded.value().front().message_id, 0x0033);
  EXPECT_EQ(decoded.value().front().frm_type, FrameType::kCommand);

  node.Close();
  EXPECT_TRUE(sender.get());
}

// Close → 消费者 handler fiber 干净退出、WaitClosed 完成(收敛者 join handler 任务)。
TEST(ProtocolNodeHandler, CloseConvergesConsumerFiber) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  ProtocolNodeConfig config;
  config.handler = [](const Message&, HandlerContext&) -> Coro::Result<void> {
    return Coro::Result<void>{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 消费者 fiber 此刻在空队列 Pop 上挂起;Close 关队列唤醒其 kClosed 退出。
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
}
