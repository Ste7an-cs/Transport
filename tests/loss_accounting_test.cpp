// -----------------------------------------------------------------------------
// loss_accounting_test.cpp — P5-5:loss=0 验证 harness(issue #90,承接 ADR-0003 D13)
//
// 本文件是**测试文件**,不是新运行时组件(D13 纪律)。验收三件事:
//   ① 干净跑场景:容量内、无外部故障的正常负载全程跑完,全部 DropReason 计数器保持 0
//      (3.6.2 loss=0 字面判据)。配置 CapturingTraceSink 时不断言 Records() 为空——干净
//      跑本就会产生 send/recv/decode/match/handler/close 等非丢弃事件(P5-4),只断言
//      "过滤 category=="drop" 后为空"。
//   ② 混合故障场景:同一测试构造的 6 类 DropReason 各自用最小介质触发(业务队列打满/
//      迟到响应/关闭丢弃/无 handler/坏帧走 Fake ProtocolNode;DDS 交接溢出走 DdsTransport)
//      → 逐 reason 核对 + Σ(各 reason getter)精确等于测试自己持有的 ground truth
//      (不是被测系统自证)。原第七项「连接代际隔离丢弃」随 ADR-0004 D3 撤销代际隔离
//      而删除(#112),其最小介质段落一并移除。
//   ③ Trace/counter 一致性 + RT_TRACE_002 零影响:配置共享 CapturingTraceSink 时,按
//      category=="drop" 过滤后逐 reason 核对条数与计数器增量一致;同一场景(用同一批
//      构造函数、sink=nullptr)再跑一遍,计数结果逐项相同。
//
// 场景构造函数按 reason 拆成独立小函数、以可选 ITraceSink* 参数复用于「配 sink」与
// 「不配 sink」两次跑,避免两份平行代码走漂。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <boost/fiber/operations.hpp>

#include "task/fibertask.h"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"
#include "transport/codec/DdsCodec.hpp"
#include "transport/codec/ICodec.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "transport/node/DdsNode.hpp"
#include "transport/node/ProtocolNode.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::CapturingTraceSink;
using transport::Datagram;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsHandlerContext;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsTransport;
using transport::DropReason;
using transport::DropReasonName;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::FrameType;
using transport::HandlerContext;
using transport::ICodec;
using transport::ITraceSink;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::Result;
using transport::SystemCodec;
using transport::TransportErrc;
using transport::make_error_code;
using Clock = OperationOptions::Clock;

namespace {

// 过滤出 category=="drop" 的记录(同款 helper,见 protocol_node_test.cpp /
// dds_node_test.cpp):sink 同时收非丢弃事件(send/recv/decode/match/handler/close/
// connect/generation/reconnect/timeout/cancel),按 category 过滤才是正确的"丢弃恰好
// 对应一条 Trace"断言写法。
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

// 在已过滤的 drop 记录里数出 message==DropReasonName(reason) 的条数。
std::size_t CountReason(const std::vector<CapturingTraceSink::Record>& drop_records,
                        DropReason reason) {
  const auto name = DropReasonName(reason);
  return static_cast<std::size_t>(std::count_if(
      drop_records.begin(), drop_records.end(),
      [&](const auto& rec) { return rec.message == name; }));
}

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = Clock::now() + d;
  return o;
}

Message MakeRequest(std::uint16_t message_id, std::vector<std::uint8_t> payload) {
  Message req;
  req.message_id = message_id;
  req.payload = std::move(payload);
  return req;
}

Datagram MakeResponseDatagram(std::uint8_t session_id, std::uint16_t message_id,
                              std::vector<std::uint8_t> payload,
                              FrameType frm_type = FrameType::kResponse) {
  Message resp;
  resp.frm_type = frm_type;
  resp.session_id = session_id;
  resp.message_id = message_id;
  resp.payload = std::move(payload);
  SystemCodec wire;
  auto bytes = wire.Encode(resp);
  EXPECT_TRUE(static_cast<bool>(bytes));
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

Datagram MakeBusinessDatagram(FrameType frm_type, std::uint8_t session_id,
                              std::uint16_t message_id) {
  Message msg;
  msg.frm_type = frm_type;
  msg.session_id = session_id;
  msg.message_id = message_id;
  SystemCodec wire;
  auto bytes = wire.Encode(msg);
  EXPECT_TRUE(static_cast<bool>(bytes));
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

// codec 双:Encode 委托真实 SystemCodec,Decode 恒返回 kCodec——确定性触发
// DecodeAndDispatch 的坏帧分支(kBadFrame),不依赖 SystemCodec 的坏 CRC 重扫细节。
class AlwaysFailDecodeCodec : public ICodec {
 public:
  Coro::Result<std::vector<std::uint8_t>> Encode(const Message& msg) override {
    return real_.Encode(msg);
  }
  Coro::Result<std::vector<Message>> Decode(const std::uint8_t*, std::size_t) override {
    return make_error_code(TransportErrc::kCodec);
  }

 private:
  SystemCodec real_;
};

}  // namespace

// -----------------------------------------------------------------------------
// ① 干净跑场景:容量内正常负载,全部 DropReason 计数器全程保持 0。
// -----------------------------------------------------------------------------

// ProtocolNode(Fake 传输):5 轮请求-响应 + 3 次 fire-and-forget Send + 3 条业务帧全部
// 被 handler 正常消费,全程(每一步之后)node 侧 5 个 DropReason getter 保持 0
// (第 6 项 kDdsHandoffOverflow 属 DdsTransport,不在 ProtocolNode 上);配置
// CapturingTraceSink 但只断言"过滤 drop 后为空",不断言 Records() 整体为空(干净跑仍会
// 产生 send/recv/decode/match/handler/close 等非丢弃事件,这是预期行为)。
TEST(LossAccounting, CleanRunProtocolNodeAllDropCountersStayZero) {
  CapturingTraceSink sink;
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  config.handler = [](const Message&, HandlerContext&) -> Coro::Result<void> { return Coro::Result<void>{}; };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  auto AssertAllZero = [&] {
    EXPECT_EQ(node.UnmatchedResponseCount(), 0u);
    EXPECT_EQ(node.DroppedNoHandlerCount(), 0u);
    EXPECT_EQ(node.BusinessQueueOverflowCount(), 0u);
    EXPECT_EQ(node.CloseDropCount(), 0u);
    EXPECT_EQ(node.BadFrameCount(), 0u);
  };
  AssertAllZero();

  SystemCodec decoder;
  for (int i = 0; i < 5; ++i) {
    const std::uint16_t msg_id = static_cast<std::uint16_t>(0x0010 + i);
    const std::size_t prior_sent_size = fake->sent().size();
    Coro::Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
    bool done = false;
    auto request = Coro::makeTask([&] {
      outcome = node.Request(MakeRequest(msg_id, {static_cast<std::uint8_t>(i)}));
      done = true;
    });
    ASSERT_TRUE(pumpFiberUntil([&] { return fake->sent().size() > prior_sent_size; }));

    // 从实际写出的帧里解出 node 分配的 session_id,而不是猜测其内部 FIFO 分配序——
    // 只依赖公开可观测的 fake->sent() 契约。
    const auto sent = fake->sent();
    const auto& last_bytes = sent.back().bytes;
    auto decoded = decoder.Decode(last_bytes.data(), last_bytes.size());
    ASSERT_TRUE(static_cast<bool>(decoded));
    ASSERT_FALSE(decoded.value().empty());
    const std::uint8_t session_id = decoded.value().back().session_id;

    fake->Inject(MakeResponseDatagram(session_id, static_cast<std::uint16_t>(msg_id | 0x1000),
                                      {static_cast<std::uint8_t>(i)}));
    ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
    ASSERT_TRUE(outcome) << outcome.error().message();
    AssertAllZero();
    EXPECT_TRUE(request.get());
  }

  for (int i = 0; i < 3; ++i) {
    Coro::Result<void> st = node.Send(MakeRequest(static_cast<std::uint16_t>(0x0080 + i), {}));
    EXPECT_TRUE(static_cast<bool>(st));
    AssertAllZero();
  }

  for (int i = 0; i < 3; ++i) {
    fake->Inject(MakeBusinessDatagram(FrameType::kState, static_cast<std::uint8_t>(0x40 + i),
                                      static_cast<std::uint16_t>(0x0090 + i)));
  }
  ASSERT_TRUE(pumpFiberUntil([&] { return node.LastHandlerDuration().count() > 0; }));
  pumpFiberUntil([] { return false; }, 30);  // 给最后一条业务帧留出消费余量。
  AssertAllZero();

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
  AssertAllZero();  // Close 后仍全 0:无未启动排队业务。

  const auto records = sink.Records();
  EXPECT_TRUE(DropRecords(records).empty());  // 干净跑:过滤后确无 drop 记录。
  EXPECT_FALSE(records.empty());  // 但 Records() 本身非空(send/recv/decode/... 照常产生)。
}

// DdsNode(共享 Bus):server 回显请求-应答 + publisher/subscriber 发布订阅,全程容量内、
// 无坏帧/无迟到 → 全部 DropReason getter(含底层 DdsTransport 的 DdsHandoffOverflowCount)
// 保持 0。四节点共享同一 CapturingTraceSink,同样只断言"过滤 drop 后为空"。
TEST(LossAccounting, CleanRunDdsNodeAllDropCountersStayZero) {
  CapturingTraceSink sink;
  DdsConfig dds_cfg;
  dds_cfg.domain_id = 0;
  auto bus = std::make_shared<FakeDdsProvider::Bus>();

  auto server_transport_owner = std::make_unique<DdsTransport>(
      std::make_unique<FakeDdsProvider>(bus), dds_cfg,
      std::vector<std::string>{"svc", "srv_inbox"});
  DdsTransport* server_transport = server_transport_owner.get();
  DdsNodeConfig server_cfg;
  server_cfg.inbox_topic = "srv_inbox";
  server_cfg.node_id = "srv";
  server_cfg.trace_sink = &sink;
  server_cfg.handler = [](const Message& msg, DdsHandlerContext& ctx) -> Coro::Result<void> {
    Message reply;
    reply.payload = msg.payload;
    return ctx.Reply(msg, std::move(reply));
  };
  DdsNode server(std::move(server_transport_owner), std::make_unique<DdsCodec>(),
                std::move(server_cfg));

  auto client_transport_owner = std::make_unique<DdsTransport>(
      std::make_unique<FakeDdsProvider>(bus), dds_cfg,
      std::vector<std::string>{"cli_inbox"});
  DdsTransport* client_transport = client_transport_owner.get();
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  client_cfg.trace_sink = &sink;
  DdsNode client(std::move(client_transport_owner), std::make_unique<DdsCodec>(),
                std::move(client_cfg));

  auto sub_transport_owner = std::make_unique<DdsTransport>(
      std::make_unique<FakeDdsProvider>(bus), dds_cfg,
      std::vector<std::string>{"news", "sub_inbox"});
  DdsTransport* sub_transport = sub_transport_owner.get();
  int notify_count = 0;
  DdsNodeConfig sub_cfg;
  sub_cfg.inbox_topic = "sub_inbox";
  sub_cfg.node_id = "sub";
  sub_cfg.trace_sink = &sink;
  sub_cfg.handler = [&notify_count](const Message&, DdsHandlerContext&) -> Coro::Result<void> {
    ++notify_count;
    return Coro::Result<void>{};
  };
  DdsNode subscriber(std::move(sub_transport_owner), std::make_unique<DdsCodec>(),
                     std::move(sub_cfg));

  auto pub_transport_owner = std::make_unique<DdsTransport>(
      std::make_unique<FakeDdsProvider>(bus), dds_cfg,
      std::vector<std::string>{"pub_inbox"});
  DdsTransport* pub_transport = pub_transport_owner.get();
  DdsNodeConfig pub_cfg;
  pub_cfg.inbox_topic = "pub_inbox";
  pub_cfg.node_id = "pub";
  pub_cfg.trace_sink = &sink;
  DdsNode publisher(std::move(pub_transport_owner), std::make_unique<DdsCodec>(),
                    std::move(pub_cfg));

  ASSERT_TRUE(static_cast<bool>(server.Start()));
  ASSERT_TRUE(static_cast<bool>(client.Start()));
  ASSERT_TRUE(static_cast<bool>(subscriber.Start()));
  ASSERT_TRUE(static_cast<bool>(publisher.Start()));

  auto AssertAllZero = [&] {
    EXPECT_EQ(client.UnmatchedReplyCount(), 0u);
    EXPECT_EQ(server.UnmatchedReplyCount(), 0u);
    EXPECT_EQ(subscriber.UnmatchedReplyCount(), 0u);
    EXPECT_EQ(publisher.UnmatchedReplyCount(), 0u);
    EXPECT_EQ(client.DroppedNoHandlerCount(), 0u);
    EXPECT_EQ(server.DroppedNoHandlerCount(), 0u);
    EXPECT_EQ(subscriber.DroppedNoHandlerCount(), 0u);
    EXPECT_EQ(publisher.DroppedNoHandlerCount(), 0u);
    EXPECT_EQ(client.BusinessQueueOverflowCount(), 0u);
    EXPECT_EQ(server.BusinessQueueOverflowCount(), 0u);
    EXPECT_EQ(subscriber.BusinessQueueOverflowCount(), 0u);
    EXPECT_EQ(publisher.BusinessQueueOverflowCount(), 0u);
    EXPECT_EQ(client.CloseDropCount(), 0u);
    EXPECT_EQ(server.CloseDropCount(), 0u);
    EXPECT_EQ(subscriber.CloseDropCount(), 0u);
    EXPECT_EQ(publisher.CloseDropCount(), 0u);
    EXPECT_EQ(client.BadFrameCount(), 0u);
    EXPECT_EQ(server.BadFrameCount(), 0u);
    EXPECT_EQ(subscriber.BadFrameCount(), 0u);
    EXPECT_EQ(publisher.BadFrameCount(), 0u);
    EXPECT_EQ(server_transport->DdsHandoffOverflowCount(), 0u);
    EXPECT_EQ(client_transport->DdsHandoffOverflowCount(), 0u);
    EXPECT_EQ(sub_transport->DdsHandoffOverflowCount(), 0u);
    EXPECT_EQ(pub_transport->DdsHandoffOverflowCount(), 0u);
  };
  AssertAllZero();

  // 3 轮请求-应答。
  for (int i = 0; i < 3; ++i) {
    Coro::Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
    bool done = false;
    auto req = Coro::makeTask([&] {
      Message m;
      m.payload = {static_cast<std::uint8_t>(i)};
      outcome = client.Request(std::move(m), Endpoint::Topic("svc"), Deadline(3000ms));
      done = true;
    });
    ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
    ASSERT_TRUE(outcome) << outcome.error().message();
    AssertAllZero();
    EXPECT_TRUE(req.get());
  }

  // 3 次发布通知,订阅方全部正常消费。
  for (int i = 0; i < 3; ++i) {
    const int prior = notify_count;
    Message note;
    note.payload = {static_cast<std::uint8_t>(0x50 + i)};
    bool sent = false;
    auto pub = Coro::makeTask([&] {
      (void)publisher.Publish(std::move(note), Endpoint::Topic("news"));
      sent = true;
    });
    ASSERT_TRUE(pumpFiberUntil([&] { return notify_count > prior; }));
    ASSERT_TRUE(pumpFiberUntil([&] { return sent; }));
    AssertAllZero();
  }

  server.Close();
  client.Close();
  subscriber.Close();
  publisher.Close();
  EXPECT_TRUE(static_cast<bool>(server.WaitClosed(Deadline(2000ms))));
  EXPECT_TRUE(static_cast<bool>(client.WaitClosed(Deadline(2000ms))));
  EXPECT_TRUE(static_cast<bool>(subscriber.WaitClosed(Deadline(2000ms))));
  EXPECT_TRUE(static_cast<bool>(publisher.WaitClosed(Deadline(2000ms))));
  AssertAllZero();

  EXPECT_TRUE(DropRecords(sink.Records()).empty());
}

// -----------------------------------------------------------------------------
// ② 混合故障场景:6 类 DropReason 各自用最小介质触发,测试自己持有 ground truth。
//
// 场景构造函数按 reason 拆成独立小函数,接收可选 ITraceSink*,复用于「配 sink」与
// 「不配 sink」两次跑(③ RT_TRACE_002 零影响),避免两份平行代码走漂。
// -----------------------------------------------------------------------------

namespace {

// nodeA(Fake 传输 + SystemCodec):同一节点生命周期内组合触发三类丢弃——
//   · business_queue_max_events=2,handler 卡在 ctx.cancellation().Wait()(不放行)→
//     灌 5 帧:1 条被消费卡住、2 条入队占满、2 条 tail-drop(kBusinessQueueOverflow=2)。
//   · 无在途 Request 时注入 3 条响应帧 → 全部无匹配(kUnmatchedOrLateResponse=3)。
//   · Close:协作取消唤醒卡住的 handler 退出,finalizer Drain 队列里剩余的 2 条未启动
//     业务(kCloseDrop=2)。
struct NodeACounts {
  std::size_t overflow = 0;
  std::size_t unmatched = 0;
  std::size_t close_drop = 0;
};

NodeACounts RunNodeAScenario(ITraceSink* sink) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_int entered{0};
  ProtocolNodeConfig config;
  config.trace_sink = sink;
  config.business_queue_max_events = 2;
  config.handler = [&entered](const Message&, HandlerContext& ctx) -> Coro::Result<void> {
    entered.fetch_add(1);
    ctx.cancellation().Wait();  // 卡住首条,不放行 —— 后续帧只入队 / 溢出,不处理。
    return Coro::Result<void>{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  EXPECT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0201));  // 被消费、卡住。
  pumpFiberUntil([&] { return entered.load() == 1; });
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0202));  // 入队(1)。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0203));  // 入队(2,满)。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 4, 0x0204));  // 满 → tail-drop。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 5, 0x0205));  // 满 → tail-drop。
  pumpFiberUntil([&] { return node.BusinessQueueOverflowCount() == 2u; });

  // 无在途 Request:注入的响应帧全部无匹配在途键。
  fake->Inject(MakeResponseDatagram(10, 0x1210, {}));
  fake->Inject(MakeResponseDatagram(11, 0x1211, {}));
  fake->Inject(MakeResponseDatagram(12, 0x1212, {}));
  pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 3u; });

  EXPECT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());

  return NodeACounts{node.BusinessQueueOverflowCount(), node.UnmatchedResponseCount(),
                     node.CloseDropCount()};
}

// nodeB(Fake 传输,未设 handler):业务帧一律归因 dropped_no_handler(P1 行为)。
std::size_t RunNodeBScenario(ITraceSink* sink) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.trace_sink = sink;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  EXPECT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0301));
  fake->Inject(MakeBusinessDatagram(FrameType::kCommand, 2, 0x0302));
  pumpFiberUntil([&] { return node.DroppedNoHandlerCount() == 2u; });

  EXPECT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
  return node.DroppedNoHandlerCount();
}

// nodeC(Fake 传输 + 恒 Decode 失败 codec):读循环坏帧分支。
std::size_t RunNodeCScenario(ITraceSink* sink) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.trace_sink = sink;
  ProtocolNode node(std::move(fake_owner), std::make_unique<AlwaysFailDecodeCodec>(),
                    std::move(config));
  EXPECT_TRUE(node.Start());

  Datagram g1;
  g1.bytes = {0xDE, 0xAD};
  fake->Inject(std::move(g1));
  Datagram g2;
  g2.bytes = {0x01};
  fake->Inject(std::move(g2));
  pumpFiberUntil([&] { return node.BadFrameCount() == 2u; });

  EXPECT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
  return node.BadFrameCount();
}

}  // namespace

constexpr std::size_t kOverflowGroundTruth = 2;
constexpr std::size_t kUnmatchedGroundTruth = 3;
constexpr std::size_t kCloseDropGroundTruth = 2;
constexpr std::size_t kNoHandlerGroundTruth = 2;
constexpr std::size_t kBadFrameGroundTruth = 2;

// Σ(业务队列溢出 + 迟到/无匹配响应 + 关闭丢弃 + 无 handler + 坏帧)精确等于测试自己
// 构造的 ground truth(逐 reason 拆解验证);配置共享 CapturingTraceSink,按
// category=="drop" 过滤后逐 reason 核对条数与计数器增量一致。
TEST(LossAccounting, MixedFailureFakeTransportReasonsMatchGroundTruth) {
  CapturingTraceSink sink;
  const NodeACounts a = RunNodeAScenario(&sink);
  const std::size_t no_handler = RunNodeBScenario(&sink);
  const std::size_t bad_frame = RunNodeCScenario(&sink);

  EXPECT_EQ(a.overflow, kOverflowGroundTruth);
  EXPECT_EQ(a.unmatched, kUnmatchedGroundTruth);
  EXPECT_EQ(a.close_drop, kCloseDropGroundTruth);
  EXPECT_EQ(no_handler, kNoHandlerGroundTruth);
  EXPECT_EQ(bad_frame, kBadFrameGroundTruth);

  const std::size_t sigma = a.overflow + a.unmatched + a.close_drop + no_handler + bad_frame;
  constexpr std::size_t kTotalGroundTruth = kOverflowGroundTruth + kUnmatchedGroundTruth +
                                            kCloseDropGroundTruth + kNoHandlerGroundTruth +
                                            kBadFrameGroundTruth;
  EXPECT_EQ(sigma, kTotalGroundTruth);

  const auto drop_records = DropRecords(sink.Records());
  EXPECT_EQ(drop_records.size(), sigma);
  EXPECT_EQ(CountReason(drop_records, DropReason::kBusinessQueueOverflow), a.overflow);
  EXPECT_EQ(CountReason(drop_records, DropReason::kUnmatchedOrLateResponse), a.unmatched);
  EXPECT_EQ(CountReason(drop_records, DropReason::kCloseDrop), a.close_drop);
  EXPECT_EQ(CountReason(drop_records, DropReason::kNoHandlerConfigured), no_handler);
  EXPECT_EQ(CountReason(drop_records, DropReason::kBadFrame), bad_frame);
}

namespace {

// kDdsHandoffOverflow 最小介质:裸 DdsTransport(无需 DdsNode)——交接边界满 tail-drop。
std::size_t RunDdsHandoffScenario(ITraceSink* sink) {
  DdsConfig cfg;
  cfg.domain_id = 0;
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx(bus);
  EXPECT_TRUE(static_cast<bool>(tx.Init(cfg)));

  auto rx = std::make_unique<DdsTransport>(
      std::make_unique<FakeDdsProvider>(bus), cfg,
      std::vector<std::string>{"loss-acct-handoff"}, /*max_samples=*/3,
      DdsTransport::kDefaultMaxBytes, sink);
  EXPECT_TRUE(static_cast<bool>(rx->Start()));

  for (int i = 0; i < 10; ++i) {
    EXPECT_TRUE(static_cast<bool>(
        tx.Publish("loss-acct-handoff", {static_cast<std::uint8_t>(i)})));
  }
  return rx->DdsHandoffOverflowCount();
}

}  // namespace

constexpr std::size_t kDdsHandoffGroundTruth = 7;  // 收下前 3,发 10 条丢后 7。

// kDdsHandoffOverflow 单独核对:最小介质(裸 DdsTransport)触发的丢弃数精确等于
// ground truth,配置 sink 时对应 Trace 条数一致。
TEST(LossAccounting, MixedFailureDdsHandoffOverflowMatchesGroundTruth) {
  CapturingTraceSink sink;
  const std::size_t handoff = RunDdsHandoffScenario(&sink);

  EXPECT_EQ(handoff, kDdsHandoffGroundTruth);

  const auto drop_records = DropRecords(sink.Records());
  EXPECT_EQ(drop_records.size(), handoff);
  EXPECT_EQ(CountReason(drop_records, DropReason::kDdsHandoffOverflow), handoff);
}

constexpr std::size_t kAllReasonsTotalGroundTruth =
    kOverflowGroundTruth + kUnmatchedGroundTruth + kCloseDropGroundTruth +
    kNoHandlerGroundTruth + kBadFrameGroundTruth + kDdsHandoffGroundTruth;

// 主验收:Σ(全部 6 类 DropReason getter)精确等于测试自己构造的总丢弃事件数
// (逐 reason 拆解验证,不只验总数,acceptance criteria 第二条)。每个 reason 用能触发它
// 的最小介质(kDdsHandoffOverflow 用裸 DdsTransport,其余 5 项用 Fake ProtocolNode),
// 共享同一 CapturingTraceSink,按 category=="drop" 过滤后逐 reason 核对 Trace 条数与
// 计数器增量一致(acceptance criteria 第三条)。
//
// 原第七项「连接代际隔离丢弃」及其"真实 TCP 自动重连"最小介质段落随 ADR-0004 D3
// (撤销连接代际隔离)删除(#112):断链时交互层不再清空旧链路排队业务,该归因项的产生
// 机制已不存在。核心断言强度不变——仍是 Σ命名原因 == 总丢弃 且
// drop_records.size() == Σ(证无多记漏记),只是原因项由七减六。
TEST(LossAccounting, MixedFailureAllSixReasonsSigmaMatchesGroundTruth) {
  CapturingTraceSink sink;
  const NodeACounts a = RunNodeAScenario(&sink);
  const std::size_t no_handler = RunNodeBScenario(&sink);
  const std::size_t bad_frame = RunNodeCScenario(&sink);
  const std::size_t handoff = RunDdsHandoffScenario(&sink);

  EXPECT_EQ(a.overflow, kOverflowGroundTruth);
  EXPECT_EQ(a.unmatched, kUnmatchedGroundTruth);
  EXPECT_EQ(a.close_drop, kCloseDropGroundTruth);
  EXPECT_EQ(no_handler, kNoHandlerGroundTruth);
  EXPECT_EQ(bad_frame, kBadFrameGroundTruth);
  EXPECT_EQ(handoff, kDdsHandoffGroundTruth);

  const std::size_t sigma =
      a.overflow + a.unmatched + a.close_drop + no_handler + bad_frame + handoff;
  EXPECT_EQ(sigma, kAllReasonsTotalGroundTruth);

  const auto drop_records = DropRecords(sink.Records());
  EXPECT_EQ(drop_records.size(), sigma);
  EXPECT_EQ(CountReason(drop_records, DropReason::kBusinessQueueOverflow), a.overflow);
  EXPECT_EQ(CountReason(drop_records, DropReason::kUnmatchedOrLateResponse), a.unmatched);
  EXPECT_EQ(CountReason(drop_records, DropReason::kCloseDrop), a.close_drop);
  EXPECT_EQ(CountReason(drop_records, DropReason::kNoHandlerConfigured), no_handler);
  EXPECT_EQ(CountReason(drop_records, DropReason::kBadFrame), bad_frame);
  EXPECT_EQ(CountReason(drop_records, DropReason::kDdsHandoffOverflow), handoff);
}

// -----------------------------------------------------------------------------
// ③ RT_TRACE_002 零影响:同一场景(同一批构造函数)sink=nullptr 再跑一遍,计数结果
// 逐项相同(acceptance criteria 第四条)。
// -----------------------------------------------------------------------------

TEST(LossAccounting, MixedFailureCountsUnaffectedWithoutTraceSink) {
  const NodeACounts a = RunNodeAScenario(nullptr);
  const std::size_t no_handler = RunNodeBScenario(nullptr);
  const std::size_t bad_frame = RunNodeCScenario(nullptr);
  const std::size_t handoff = RunDdsHandoffScenario(nullptr);

  EXPECT_EQ(a.overflow, kOverflowGroundTruth);
  EXPECT_EQ(a.unmatched, kUnmatchedGroundTruth);
  EXPECT_EQ(a.close_drop, kCloseDropGroundTruth);
  EXPECT_EQ(no_handler, kNoHandlerGroundTruth);
  EXPECT_EQ(bad_frame, kBadFrameGroundTruth);
  EXPECT_EQ(handoff, kDdsHandoffGroundTruth);

  const std::size_t sigma =
      a.overflow + a.unmatched + a.close_drop + no_handler + bad_frame + handoff;
  EXPECT_EQ(sigma, kAllReasonsTotalGroundTruth);
}
