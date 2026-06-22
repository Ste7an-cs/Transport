#include "transport/comm/DdsNode.hpp"

#include "transport/codec/DdsCodec.hpp"
#include "transport/dds/DdsTransport.hpp"
#include "transport/dds/FakeDdsProvider.hpp"
#include "inline_executor.hpp"

#include <functional>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

using transport::DdsCodec;
using transport::DdsConfig;
using transport::DdsNode;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::ICodec;
using transport::IExecutor;
using transport::Message;
using transport::Responder;
using transport::Result;
using testutil::InlineExecutor;

namespace {
Message Msg(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

// 测试子类:记录 OnMessage;OnRequest 委托给可设的 on_req。
class TestNode : public DdsNode {
 public:
  using DdsNode::DdsNode;
  std::vector<Message> messages;
  std::function<void(const Message&, Responder)> on_req;
  void OnMessage(const Message& m) override { messages.push_back(m); }
  void OnRequest(const Message& req, Responder r) override {
    if (on_req) on_req(req, std::move(r));
  }
};

// 一对共享 Bus(DI)的 DdsNode + 各自 InlineExecutor(确定性);保留 exec 裸指针驱动定时器。
struct Net {
  std::shared_ptr<FakeDdsProvider::Bus> bus = std::make_shared<FakeDdsProvider::Bus>();
  std::shared_ptr<TestNode> Make(const std::string& inbox, InlineExecutor** out_exec) {
    DdsConfig cfg; cfg.domain_id = 0;  // provider 注入,name/domain 不参与
    auto tp = std::make_shared<DdsTransport>(cfg, std::make_unique<FakeDdsProvider>(bus));
    auto ex = std::make_unique<InlineExecutor>();
    if (out_exec) *out_exec = ex.get();
    return std::make_shared<TestNode>(tp, inbox, std::unique_ptr<ICodec>(new DdsCodec()),
                                      std::unique_ptr<IExecutor>(std::move(ex)));
  }
};
}  // namespace

TEST(DdsNode, PublishSubscribeRoundtrip) {
  Net net;
  auto a = net.Make("A_in", nullptr);
  auto b = net.Make("B_in", nullptr);
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(a->Subscribe("T")));
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({1, 2, 3}), Endpoint::Topic("T"))));  // 发布
  ASSERT_EQ(a->messages.size(), 1u);                       // InlineExecutor 同步交付
  EXPECT_EQ(a->messages[0].payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(a->messages[0].topic, "T");                    // topic = 来源 topic
  a->Close(); b->Close();
}

TEST(DdsNode, MultiTopicPublishSubscribe) {
  Net net;
  auto a = net.Make("A_in", nullptr);
  auto b = net.Make("B_in", nullptr);
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(a->Subscribe("T1")));
  ASSERT_TRUE(static_cast<bool>(a->Subscribe("T2")));
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({0x11}), Endpoint::Topic("T1"))));
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({0x22}), Endpoint::Topic("T2"))));
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({0x33}), Endpoint::Topic("T3"))));  // 未订阅
  ASSERT_EQ(a->messages.size(), 2u);
  EXPECT_EQ(a->messages[0].topic, "T1");
  EXPECT_EQ(a->messages[1].topic, "T2");
  a->Close(); b->Close();
}

TEST(DdsNode, UnsubscribeStopsDelivery) {
  Net net;
  auto a = net.Make("A_in", nullptr);
  auto b = net.Make("B_in", nullptr);
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(a->Subscribe("T")));
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({1}), Endpoint::Topic("T"))));
  ASSERT_EQ(a->messages.size(), 1u);
  ASSERT_TRUE(static_cast<bool>(a->Unsubscribe("T")));
  ASSERT_TRUE(static_cast<bool>(b->Send(Msg({2}), Endpoint::Topic("T"))));
  EXPECT_EQ(a->messages.size(), 1u);  // 退订后不再收
  a->Close(); b->Close();
}

TEST(DdsNode, RequestReplyOverTopics) {
  Net net;
  auto a = net.Make("A_in", nullptr);
  auto b = net.Make("B_in", nullptr);
  b->on_req = [](const Message& req, Responder r) {
    auto out = req.payload; out.push_back(0xFF);
    (void)r.Reply(Msg(out));  // 经 reply_to(=A_in)回送
  };
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("svc")));
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      a->Request(Msg({5}), [&](Result<Message> r) { got = std::move(r); }, 1000,
                 Endpoint::Topic("svc"))));
  ASSERT_TRUE(static_cast<bool>(got));   // InlineExecutor 全同步:已回
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xFF}));
  a->Close(); b->Close();
}

TEST(DdsNode, RequestAcrossMultipleServiceTopics) {
  Net net;
  auto a = net.Make("A_in", nullptr);
  auto b = net.Make("B_in", nullptr);
  b->on_req = [](const Message& req, Responder r) {
    auto out = req.payload;
    out.push_back(req.topic == "svc1" ? 0x01 : 0x02);  // topic = 投递的请求 topic
    (void)r.Reply(Msg(out));
  };
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("svc1")));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("svc2")));
  Result<Message> r1 = Result<Message>::Fail("none"), r2 = Result<Message>::Fail("none");
  (void)a->Request(Msg({9}), [&](Result<Message> r) { r1 = std::move(r); }, 1000,
                   Endpoint::Topic("svc1"));
  (void)a->Request(Msg({9}), [&](Result<Message> r) { r2 = std::move(r); }, 1000,
                   Endpoint::Topic("svc2"));
  ASSERT_TRUE(static_cast<bool>(r1)); ASSERT_TRUE(static_cast<bool>(r2));
  EXPECT_EQ(r1.value.payload, (std::vector<uint8_t>{9, 0x01}));  // 各自 reply_to 精确回送
  EXPECT_EQ(r2.value.payload, (std::vector<uint8_t>{9, 0x02}));
  a->Close(); b->Close();
}

TEST(DdsNode, FeedbackThenFinalOverTopics) {
  Net net;
  auto a = net.Make("A_in", nullptr);
  auto b = net.Make("B_in", nullptr);
  b->on_req = [](const Message&, Responder r) {
    (void)r.Feedback(Msg({0x01}));
    (void)r.Feedback(Msg({0x02}));
    (void)r.Reply(Msg({0xEE}));
  };
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("svc")));
  std::vector<std::vector<uint8_t>> fbs;
  Result<Message> fin = Result<Message>::Fail("none");
  (void)a->Request(Msg({9}),
                   [&](const Message& m) { fbs.push_back(m.payload); },
                   [&](Result<Message> r) { fin = std::move(r); }, 1000,
                   Endpoint::Topic("svc"));
  ASSERT_EQ(fbs.size(), 2u);
  EXPECT_EQ(fbs[0], (std::vector<uint8_t>{0x01}));
  EXPECT_EQ(fbs[1], (std::vector<uint8_t>{0x02}));
  ASSERT_TRUE(static_cast<bool>(fin));
  EXPECT_EQ(fin.value.payload, (std::vector<uint8_t>{0xEE}));
  a->Close(); b->Close();
}

TEST(DdsNode, RequestTimeoutOverTopics) {
  Net net;
  InlineExecutor* exa = nullptr;
  auto a = net.Make("A_in", &exa);
  auto b = net.Make("B_in", nullptr);  // 无人订阅 nobody → 请求无人应答
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  Result<Message> got = Result<Message>::Fail("none");
  (void)a->Request(Msg({1}), [&](Result<Message> r) { got = std::move(r); }, 50,
                   Endpoint::Topic("nobody"));
  EXPECT_FALSE(static_cast<bool>(got));  // 还没超时
  exa->FireAll();                        // 驱动 a 的超时定时器
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  a->Close(); b->Close();
}

// 执行器可换性 + 真实并发:默认 ThreadExecutor,future 等回。
TEST(DdsNode, WorksWithThreadExecutor) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  DdsConfig cfg; cfg.domain_id = 0;
  auto ta = std::make_shared<DdsTransport>(cfg, std::make_unique<FakeDdsProvider>(bus));
  auto tb = std::make_shared<DdsTransport>(cfg, std::make_unique<FakeDdsProvider>(bus));
  auto a = std::make_shared<TestNode>(ta, "A_in", nullptr, nullptr);  // 默认 DdsCodec+ThreadExecutor
  auto b = std::make_shared<TestNode>(tb, "B_in", nullptr, nullptr);
  b->on_req = [](const Message& req, Responder r) {
    auto out = req.payload; out.push_back(0xFF); (void)r.Reply(Msg(out));
  };
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Subscribe("svc")));
  auto r = a->Request(Msg({3}), 2000, Endpoint::Topic("svc")).get();  // future 等回(真实线程)
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xFF}));
  a->Close(); b->Close();
}
