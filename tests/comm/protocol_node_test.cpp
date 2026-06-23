#include "transport/comm/ProtocolNode.hpp"

#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <functional>
#include <future>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameType;
using transport::ICodec;
using transport::IExecutor;
using transport::Message;
using transport::ProtocolConfig;
using transport::ProtocolNode;
using Responder = transport::ProtocolNode::Responder;
using transport::Result;
using testutil::FakeTransport;
using testutil::InlineExecutor;

namespace {
std::vector<uint8_t> P(std::initializer_list<uint8_t> l) { return std::vector<uint8_t>(l); }

// 测试子类:记录收到的 COMMAND,可设回应行为。
class TestNode : public ProtocolNode {
 public:
  using ProtocolNode::ProtocolNode;
  int commands = 0;
  int heartbeats = 0;
  std::function<void(const Message&, Responder)> on_cmd;
  void OnCommand(const Message& m, Responder r) override {
    ++commands; if (on_cmd) on_cmd(m, std::move(r));
  }
  void OnHeartbeat(const Message&) override { ++heartbeats; }
};

ProtocolConfig Cfg(uint8_t proto = 1, uint32_t timeout = 1000, uint32_t retries = 3) {
  ProtocolConfig c; c.protocol_id = proto; c.response_timeout_ms = timeout;
  c.max_retries = retries; c.heartbeat_interval_ms = 0; return c;
}

// 一对经 FakeTransport 互联 + InlineExecutor(确定性)的 TestNode。保留 exec 裸指针驱动定时器。
struct Pair {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<TestNode> a, b;
  InlineExecutor* exa = nullptr; InlineExecutor* exb = nullptr;
  Pair(ProtocolConfig ca = Cfg(), ProtocolConfig cb = Cfg()) {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    auto eb = std::make_unique<InlineExecutor>(); exb = eb.get();
    a = std::make_shared<TestNode>(ta, nullptr, ca, std::move(ea));
    b = std::make_shared<TestNode>(tb, nullptr, cb, std::move(eb));
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(ProtocolNode, NoResponseSends) {
  Pair p; p.Open();
  ASSERT_TRUE(static_cast<bool>(p.a->SendNoResponse(P({1, 2, 3}))));
  EXPECT_EQ(p.b->commands, 1);  // 对端收到 COMMAND(InlineExecutor 同步)
  p.Close();
}

TEST(ProtocolNode, NeedResponseCompletesOnResponse) {
  Pair p; p.Open();
  p.b->on_cmd = [](const Message& m, Responder r) {
    auto out = m.payload; out.push_back(0xEE); (void)r.Response(out);
  };
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      p.a->Request(P({5}), [&](Result<Message> rr) { got = std::move(rr); })));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xEE}));
  EXPECT_EQ(got.value.frm_type, FrameType::kResponse);
  p.Close();
}

TEST(ProtocolNode, ReceiverRoleHandlesIncomingCommand) {
  Pair p; p.Open();
  Message seen;
  p.b->on_cmd = [&](const Message& m, Responder r) { seen = m; (void)r.Response(P({9})); };
  Result<Message> got = Result<Message>::Fail("none");
  (void)p.a->Request(P({0x42}), [&](Result<Message> rr) { got = std::move(rr); });
  EXPECT_EQ(seen.frm_type, FrameType::kCommand);
  EXPECT_EQ(seen.protocol_id, 1);
  EXPECT_EQ(seen.payload, (std::vector<uint8_t>{0x42}));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{9}));
  p.Close();
}

TEST(ProtocolNode, TimeoutRetransmitsUpToThreeThenFails) {
  Pair p(Cfg(1, 50, 3), Cfg(2, 50, 3));
  p.Open();
  // 对端只计数不回应。
  Result<Message> got = Result<Message>::Success(Message{});
  (void)p.a->Request(P({1}), [&](Result<Message> rr) { got = std::move(rr); });
  EXPECT_EQ(p.b->commands, 1);                 // 初次发送
  p.exa->FireAll();                            // 超时 → 重发1
  EXPECT_EQ(p.b->commands, 2);
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 3);  // 重发2
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 4);  // 重发3(达上限)
  p.exa->FireAll();                            // 再超时 → 失败
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  EXPECT_EQ(p.b->commands, 4);                 // 不再重发
  p.Close();
}

TEST(ProtocolNode, WorksWithThreadExecutor) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<TestNode>(ta, nullptr, Cfg(), nullptr);  // 默认 SystemCodec+ThreadExecutor
  auto b = std::make_shared<TestNode>(tb, nullptr, Cfg(), nullptr);
  b->on_cmd = [](const Message& m, Responder r) {
    auto out = m.payload; out.push_back(0xEE); (void)r.Response(out);
  };
  (void)b->Open(); (void)a->Open();
  std::promise<Result<Message>> prom; auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(
      a->Request(P({3}), [&](Result<Message> r) { prom.set_value(std::move(r)); })));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xEE}));
  a->Close(); b->Close();
}
