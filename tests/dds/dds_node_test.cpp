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

#include "transport/DdsNode.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "task/fibertask.h"
#include "transport/DdsTransport.hpp"
#include "transport/Endpoint.hpp"
#include "transport/Error.hpp"
#include "transport/Message.hpp"
#include "transport/codec/DdsCodec.hpp"
#include "transport/dds/FakeDdsProvider.hpp"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using testutil::pumpFiberUntil;
using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsHandlerContext;
using transport::DdsNode;
using transport::DdsNodeConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::Message;
using transport::MessageKind;
using transport::OperationOptions;
using transport::Result;
using transport::Status;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

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
    auto provider = std::make_unique<FakeDdsProvider>(bus);
    auto transport = std::make_unique<DdsTransport>(std::move(provider), Cfg(),
                                                    std::move(topics));
    return std::make_unique<DdsNode>(std::move(transport),
                                     std::make_unique<DdsCodec>(),
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
