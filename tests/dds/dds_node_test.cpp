// -----------------------------------------------------------------------------
// dds_node_test.cpp — P4-5 DdsNode:pub-sub + 多路请求-应答(D10 复用实证)
//
// 验收(issue #73 / RT_NODE_001/003/004/005 / RT_REQUEST / RT_IF_DDS / ADR-0003 D10/D12):
//   · Request → 对端 kReply(correlation_id 一致、发到 reply_to inbox)→ 恰好一次完成、
//     返 Message、关联清理(PendingCount 归零)。
//   · Publish(kNotify)→ 订阅方 handler 收到(kind==kNotify)。
//   · 多路:不同 topic 并发 Request 各自 correlation_id 匹配、互不串。
//   · 迟到 / 无匹配 correlation_id 的 kReply → 归因丢弃(UnmatchedReplyCount)不误配。
//   · Close 收敛:在途 Request kClosed、WaitClosed 完成。
//
// 用 FakeDdsProvider(共享 Bus 模拟多 participant)作底层、真实 DdsCodec 作 codec、
// DdsTransport 作传输。测试 fiber 用 pumpFiberUntil 驱动(coro_test_main 范式);被测
// Request(await)用 makeTask 起独立 fiber。
// -----------------------------------------------------------------------------

#include "transport/node/DdsNode.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/codec/DdsCodec.hpp"
#include "transport/codec/ICodec.hpp"
#include "transport/io/dds/FakeDdsProvider.hpp"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::CapturingTraceSink;
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
using transport::ICodec;
using transport::Message;
using transport::MessageKind;
using transport::OperationOptions;
using transport::Result;
using transport::Status;
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

DdsConfig Cfg() {
  DdsConfig c;
  c.domain_id = 0;
  return c;
}

OperationOptions Deadline(std::chrono::milliseconds d) {
  OperationOptions o;
  o.deadline = OperationOptions::Clock::now() + d;
  return o;
}

// 共享 Bus 的节点工厂;并保留一个独立发布方 provider(tx)用于注入裸帧。
struct Cluster {
  std::shared_ptr<FakeDdsProvider::Bus> bus =
      std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx{bus};
  Cluster() { (void)tx.Init(Cfg()); }

  std::unique_ptr<DdsNode> MakeNode(std::vector<std::string> topics,
                                    DdsNodeConfig config) {
    return MakeNodeWithCodec(std::move(topics), std::make_unique<DdsCodec>(),
                             std::move(config));
  }

  // 同 MakeNode,但允许注入自定义 codec(供 P5-3 kBadFrame 用例:恒 Decode 失败的双)。
  std::unique_ptr<DdsNode> MakeNodeWithCodec(std::vector<std::string> topics,
                                             std::unique_ptr<ICodec> codec,
                                             DdsNodeConfig config) {
    auto provider = std::make_unique<FakeDdsProvider>(bus);
    auto transport = std::make_unique<DdsTransport>(std::move(provider), Cfg(),
                                                    std::move(topics));
    return std::make_unique<DdsNode>(std::move(transport), std::move(codec),
                                     std::move(config));
  }

  // 直接向 topic 注入一条已编码帧(模拟对端 / 裸帧,用于迟到-无匹配路径)。
  void InjectRaw(const std::string& topic, MessageKind kind,
                 const std::string& correlation_id, const std::string& reply_to,
                 std::vector<std::uint8_t> payload) {
    Message m;
    m.kind = kind;
    m.correlation_id = correlation_id;
    m.reply_to = reply_to;
    m.payload = std::move(payload);
    DdsCodec codec;
    auto bytes = codec.Encode(m);
    ASSERT_TRUE(static_cast<bool>(bytes));
    ASSERT_TRUE(static_cast<bool>(tx.Publish(topic, bytes.value())));
  }
};

// 回显 handler:对 kRequest 原样回送 payload 作 kReply。
DdsNodeConfig EchoServerConfig(std::string inbox, std::string node_id) {
  DdsNodeConfig c;
  c.inbox_topic = std::move(inbox);
  c.node_id = std::move(node_id);
  c.handler = [](const Message& msg, DdsHandlerContext& ctx) -> Status {
    Message reply;
    reply.payload = msg.payload;  // 回显。
    return ctx.Reply(msg, std::move(reply));
  };
  return c;
}

// codec 双:Encode 委托真实 DdsCodec(Publish 仍可正常编码上线),Decode 恒返回
// kCodec——供确定性触发 DecodeAndDispatch 的坏帧分支(P5-3 kBadFrame)。
class AlwaysFailDecodeCodec : public ICodec {
 public:
  Result<std::vector<std::uint8_t>> Encode(const Message& msg) override {
    return real_.Encode(msg);
  }
  Result<std::vector<Message>> Decode(const std::uint8_t*, std::size_t) override {
    return make_error_code(TransportErrc::kCodec);
  }

 private:
  DdsCodec real_;
};

}  // namespace

// 验收①:Request → 对端 kReply(correlation 一致、回 reply_to)→ 恰好一次、返 Message、清理。
TEST(DdsNode, RequestGetsMatchingReplyAndCleansUp) {
  Cluster c;
  auto server = c.MakeNode({"svc", "srv_inbox"}, EchoServerConfig("srv_inbox", "srv"));
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  auto client = c.MakeNode({"cli_inbox"}, std::move(client_cfg));
  ASSERT_TRUE(static_cast<bool>(server->Start()));
  ASSERT_TRUE(static_cast<bool>(client->Start()));

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m;
    m.payload = {0x11, 0x22, 0x33};
    outcome = client->Request(std::move(m), Endpoint::Topic("svc"), Deadline(3000ms));
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(outcome)) << outcome.error().message();
  EXPECT_EQ(outcome.value().kind, MessageKind::kReply);
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{0x11, 0x22, 0x33}));
  EXPECT_EQ(outcome.value().correlation_id, "cli:1");  // 确定性、单调。
  EXPECT_EQ(client->PendingCount(), 0u);               // 关联清理。

  client->Close();
  server->Close();
  EXPECT_TRUE(req.get());
}

// 验收②:Publish(kNotify)→ 订阅方 handler 收到(kind==kNotify)。
TEST(DdsNode, PublishNotifyReachesSubscriberHandler) {
  Cluster c;
  Message received;
  bool got = false;
  DdsNodeConfig sub_cfg;
  sub_cfg.inbox_topic = "sub_inbox";
  sub_cfg.node_id = "sub";
  sub_cfg.handler = [&](const Message& msg, DdsHandlerContext&) -> Status {
    received = msg;
    got = true;
    return Status{};
  };
  auto subscriber = c.MakeNode({"news", "sub_inbox"}, std::move(sub_cfg));

  DdsNodeConfig pub_cfg;
  pub_cfg.inbox_topic = "pub_inbox";
  pub_cfg.node_id = "pub";
  auto publisher = c.MakeNode({"pub_inbox"}, std::move(pub_cfg));
  ASSERT_TRUE(static_cast<bool>(subscriber->Start()));
  ASSERT_TRUE(static_cast<bool>(publisher->Start()));

  Message note;
  note.payload = {0xAB, 0xCD};
  bool sent = false;
  auto pub = Coro::makeTask([&] {
    (void)publisher->Publish(std::move(note), Endpoint::Topic("news"));
    sent = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return got; }));
  EXPECT_EQ(received.kind, MessageKind::kNotify);
  EXPECT_EQ(received.payload, (std::vector<std::uint8_t>{0xAB, 0xCD}));
  EXPECT_EQ(received.topic, "news");  // 引擎按来源 topic 填。

  publisher->Close();
  subscriber->Close();
  (void)pub.get();
  (void)sent;
}

// 验收③:不同 topic 并发 Request 各自 correlation_id 匹配、互不串。
TEST(DdsNode, MultiTopicConcurrentRequestsDoNotCrossTalk) {
  Cluster c;
  auto s1 = c.MakeNode({"svc1", "s1_inbox"}, EchoServerConfig("s1_inbox", "s1"));
  auto s2 = c.MakeNode({"svc2", "s2_inbox"}, EchoServerConfig("s2_inbox", "s2"));
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  auto client = c.MakeNode({"cli_inbox"}, std::move(client_cfg));
  ASSERT_TRUE(static_cast<bool>(s1->Start()));
  ASSERT_TRUE(static_cast<bool>(s2->Start()));
  ASSERT_TRUE(static_cast<bool>(client->Start()));

  Result<Message> o1{make_error_code(TransportErrc::kInternal)};
  Result<Message> o2{make_error_code(TransportErrc::kInternal)};
  bool d1 = false, d2 = false;
  auto r1 = Coro::makeTask([&] {
    Message m;
    m.payload = {0xA1};
    o1 = client->Request(std::move(m), Endpoint::Topic("svc1"), Deadline(3000ms));
    d1 = true;
  });
  auto r2 = Coro::makeTask([&] {
    Message m;
    m.payload = {0xB2};
    o2 = client->Request(std::move(m), Endpoint::Topic("svc2"), Deadline(3000ms));
    d2 = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return d1 && d2; }));
  ASSERT_TRUE(static_cast<bool>(o1)) << o1.error().message();
  ASSERT_TRUE(static_cast<bool>(o2)) << o2.error().message();
  // 各自回显各自 payload —— 未串线。
  EXPECT_EQ(o1.value().payload, (std::vector<std::uint8_t>{0xA1}));
  EXPECT_EQ(o2.value().payload, (std::vector<std::uint8_t>{0xB2}));
  EXPECT_NE(o1.value().correlation_id, o2.value().correlation_id);
  EXPECT_EQ(client->PendingCount(), 0u);

  client->Close();
  s1->Close();
  s2->Close();
  EXPECT_TRUE(r1.get());
  EXPECT_TRUE(r2.get());
}

// 验收④:迟到 / 无匹配 correlation_id 的 kReply → 归因丢弃、不误配。
TEST(DdsNode, LateOrUnmatchedReplyIsAttributedAndDropped) {
  Cluster c;
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  auto client = c.MakeNode({"cli_inbox"}, std::move(client_cfg));
  ASSERT_TRUE(static_cast<bool>(client->Start()));

  // 无任何在途 Request:注入一条带陌生 correlation_id 的 kReply → 无匹配、归因丢弃。
  c.InjectRaw("cli_inbox", MessageKind::kReply, "cli:999", "", {0xEE});
  ASSERT_TRUE(pumpFiberUntil([&] { return client->UnmatchedReplyCount() == 1u; }));
  EXPECT_EQ(client->UnmatchedReplyCount(), 1u);
  EXPECT_EQ(client->PendingCount(), 0u);  // 未误建 / 误配任何 entry。

  client->Close();
}

// 验收⑤:Close 收敛——在途 Request 得 kClosed、WaitClosed 完成。
TEST(DdsNode, CloseConvergesInflightRequestWithClosed) {
  Cluster c;
  // 无对端应答:Request 永挂,直到 Close 的 FailAll(kClosed) 收敛。
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  auto client = c.MakeNode({"cli_inbox"}, std::move(client_cfg));
  ASSERT_TRUE(static_cast<bool>(client->Start()));

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m;
    m.payload = {0x01};
    outcome = client->Request(std::move(m), Endpoint::Topic("svc"), Deadline(5000ms));
    done = true;
  });

  // 等请求登记在途(已发送、正等应答)。
  ASSERT_TRUE(pumpFiberUntil([&] { return client->PendingCount() == 1u; }));
  EXPECT_FALSE(done);

  ASSERT_TRUE(static_cast<bool>(client->Close()));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  EXPECT_FALSE(static_cast<bool>(outcome));
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(static_cast<bool>(client->WaitClosed(Deadline(2000ms))));
  EXPECT_EQ(client->PendingCount(), 0u);
  EXPECT_TRUE(req.get());
}

// 关闭 / 未启动后 Request/Publish 一律 kClosed(拒新交互)。
TEST(DdsNode, RequestAndPublishRejectedWhenNotRunning) {
  Cluster c;
  DdsNodeConfig cfg;
  cfg.inbox_topic = "cli_inbox";
  cfg.node_id = "cli";
  auto client = c.MakeNode({"cli_inbox"}, std::move(cfg));

  // 未 Start:kClosed。
  Message m;
  m.payload = {0x01};
  auto r0 = client->Request(std::move(m), Endpoint::Topic("svc"), Deadline(200ms));
  EXPECT_EQ(r0.error(), make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(static_cast<bool>(client->Start()));
  ASSERT_TRUE(static_cast<bool>(client->Close()));

  // 已 Close:kClosed。
  Message m2;
  m2.payload = {0x02};
  auto r1 = client->Request(std::move(m2), Endpoint::Topic("svc"), Deadline(200ms));
  EXPECT_EQ(r1.error(), make_error_code(TransportErrc::kClosed));
  Message m3;
  auto r2 = client->Publish(std::move(m3), Endpoint::Topic("news"));
  EXPECT_EQ(r2.error(), make_error_code(TransportErrc::kClosed));
}

// 非法 config(inbox/node_id 空)→ Start 返 kConfiguration、停 Created 可改配重试。
TEST(DdsNode, InvalidConfigRejectedAtStart) {
  Cluster c;
  DdsNodeConfig bad;  // inbox_topic / node_id 均空。
  auto node = c.MakeNode({"t"}, bad);
  auto r = node->Start();
  EXPECT_EQ(r.error(), make_error_code(TransportErrc::kConfiguration));
}

// -----------------------------------------------------------------------------
// P5-3(issue #88):全线接入丢弃归因——配 CapturingTraceSink 时,各丢弃点计数与可辨识
// 的 TraceEvent(category="drop", message=DropReasonName)同步产生。
// -----------------------------------------------------------------------------

// kUnmatchedOrLateResponse(DdsNode 的 UnmatchedReplyCount):迟到/无匹配 kReply 归因
// 丢弃时,配置 trace_sink → 收到对应事件。
TEST(DdsNode, UnmatchedReplyWithSinkEmitsDropTrace) {
  Cluster c;
  CapturingTraceSink sink;
  DdsNodeConfig client_cfg;
  client_cfg.inbox_topic = "cli_inbox";
  client_cfg.node_id = "cli";
  client_cfg.trace_sink = &sink;
  auto client = c.MakeNode({"cli_inbox"}, std::move(client_cfg));
  ASSERT_TRUE(static_cast<bool>(client->Start()));

  c.InjectRaw("cli_inbox", MessageKind::kReply, "cli:999", "", {0xEE});
  ASSERT_TRUE(pumpFiberUntil([&] { return client->UnmatchedReplyCount() == 1u; }));

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message,
           DropReasonName(DropReason::kUnmatchedOrLateResponse));

  client->Close();
}

// kNoHandlerConfigured(DdsNode 的 DroppedNoHandlerCount):未设 handler 的入站业务消息
// 归因丢弃时,配置 trace_sink → 收到对应事件。
TEST(DdsNode, DroppedNoHandlerWithSinkEmitsDropTrace) {
  Cluster c;
  CapturingTraceSink sink;
  DdsNodeConfig sub_cfg;
  sub_cfg.inbox_topic = "sub_inbox";
  sub_cfg.node_id = "sub";
  sub_cfg.trace_sink = &sink;  // 未设 handler。
  auto subscriber = c.MakeNode({"news", "sub_inbox"}, std::move(sub_cfg));
  ASSERT_TRUE(static_cast<bool>(subscriber->Start()));

  c.InjectRaw("news", MessageKind::kNotify, "", "", {0xAB});
  ASSERT_TRUE(pumpFiberUntil([&] { return subscriber->DroppedNoHandlerCount() == 1u; }));

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message, DropReasonName(DropReason::kNoHandlerConfigured));

  subscriber->Close();
}

// kBadFrame:codec.Decode 失败时,新增 BadFrameCount() 归因 +1,配置 trace_sink → 收到
// 对应事件。
TEST(DdsNode, BadFrameDecodeFailureCountedAndTraced) {
  Cluster c;
  CapturingTraceSink sink;
  DdsNodeConfig cfg;
  cfg.inbox_topic = "cli_inbox";
  cfg.node_id = "cli";
  cfg.trace_sink = &sink;
  auto node = c.MakeNodeWithCodec({"cli_inbox"}, std::make_unique<AlwaysFailDecodeCodec>(),
                                  std::move(cfg));
  ASSERT_TRUE(static_cast<bool>(node->Start()));

  EXPECT_EQ(node->BadFrameCount(), 0u);
  ASSERT_TRUE(static_cast<bool>(c.tx.Publish("cli_inbox", {0xDE, 0xAD})));

  ASSERT_TRUE(pumpFiberUntil([&] { return node->BadFrameCount() == 1u; }));
  EXPECT_EQ(node->BadFrameCount(), 1u);

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message, DropReasonName(DropReason::kBadFrame));

  node->Close();
}

// kBusinessQueueOverflow(DdsNode 经 NodeRuntime 组合的业务队列):满 tail-drop 时,
// 配置 trace_sink → 逐条 RecordDrop,计数与 Trace 条数同步。
TEST(DdsNode, BusinessQueueOverflowWithSinkEmitsDropTrace) {
  Cluster c;
  CapturingTraceSink sink;
  auto gate = std::make_shared<Coro::Awaitable<void>>();
  int entered = 0;
  DdsNodeConfig sub_cfg;
  sub_cfg.inbox_topic = "sub_inbox";
  sub_cfg.node_id = "sub";
  sub_cfg.trace_sink = &sink;
  sub_cfg.business_queue_max_events = 1;
  sub_cfg.handler = [&](const Message&, DdsHandlerContext&) -> Status {
    ++entered;
    Coro::await(gate);  // 卡住首条,让后续帧只入队不启动 → 可控溢出。
    return Status{};
  };
  auto subscriber = c.MakeNode({"news", "sub_inbox"}, std::move(sub_cfg));
  ASSERT_TRUE(static_cast<bool>(subscriber->Start()));

  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x01});  // 被消费、卡住。
  ASSERT_TRUE(pumpFiberUntil([&] { return entered == 1; }));
  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x02});  // 入队(size 1 满)。
  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x03});  // 满 → tail-drop。
  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x04});  // 满 → tail-drop。

  ASSERT_TRUE(
      pumpFiberUntil([&] { return subscriber->BusinessQueueOverflowCount() == 2u; }));
  EXPECT_EQ(subscriber->BusinessQueueOverflowCount(), 2u);

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 2u);
  for (const auto& rec : records) {
    EXPECT_EQ(rec.category, "drop");
    EXPECT_EQ(rec.message, DropReasonName(DropReason::kBusinessQueueOverflow));
  }

  gate->resolve();
  gate->close();
  subscriber->Close();
}

// kCloseDrop(DdsNode 经 NodeRuntime 组合的业务队列 Close 批量归因):未启动的排队业务
// → Close 时逐条 RecordDrop,配置 trace_sink → 收到对应事件。
TEST(DdsNode, CloseDropWithSinkEmitsTraceEvents) {
  Cluster c;
  CapturingTraceSink sink;
  auto gate = std::make_shared<Coro::Awaitable<void>>();
  int entered = 0;
  DdsNodeConfig sub_cfg;
  sub_cfg.inbox_topic = "sub_inbox";
  sub_cfg.node_id = "sub";
  sub_cfg.trace_sink = &sink;
  sub_cfg.handler = [&](const Message&, DdsHandlerContext& ctx) -> Status {
    ++entered;
    ctx.cancellation().Wait();  // 卡住首条,让后续帧只入队不启动。
    return Status{};
  };
  auto subscriber = c.MakeNode({"news", "sub_inbox"}, std::move(sub_cfg));
  ASSERT_TRUE(static_cast<bool>(subscriber->Start()));

  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x01});  // 被消费、卡住。
  ASSERT_TRUE(pumpFiberUntil([&] { return entered == 1; }));
  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x02});  // 入队,未启动。
  c.InjectRaw("news", MessageKind::kNotify, "", "", {0x03});  // 入队,未启动。
  pumpFiberUntil([&] { return false; }, 40);

  ASSERT_TRUE(static_cast<bool>(subscriber->Close()));
  EXPECT_TRUE(static_cast<bool>(subscriber->WaitClosed()));
  EXPECT_EQ(subscriber->CloseDropCount(), 2u);

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 2u);
  for (const auto& rec : records) {
    EXPECT_EQ(rec.category, "drop");
    EXPECT_EQ(rec.message, DropReasonName(DropReason::kCloseDrop));
  }
}

// RT_TRACE_002:未配置 trace_sink(默认 nullptr)时,坏帧计数行为不受影响。
TEST(DdsNode, NoSinkConfiguredCountsUnaffected) {
  Cluster c;
  DdsNodeConfig cfg;
  cfg.inbox_topic = "cli_inbox";
  cfg.node_id = "cli";
  auto node = c.MakeNodeWithCodec({"cli_inbox"}, std::make_unique<AlwaysFailDecodeCodec>(),
                                  std::move(cfg));
  ASSERT_TRUE(static_cast<bool>(node->Start()));  // trace_sink 缺省 nullptr。

  ASSERT_TRUE(static_cast<bool>(c.tx.Publish("cli_inbox", {0xFF})));
  ASSERT_TRUE(pumpFiberUntil([&] { return node->BadFrameCount() == 1u; }));
  EXPECT_EQ(node->BadFrameCount(), 1u);

  node->Close();
}
