#include "transport/comm/InteractionEngine.hpp"

#include "transport/codec/DdsCodec.hpp"
#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <chrono>
#include <future>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

using transport::DdsCodec;
using transport::Endpoint;
using transport::FrameTag;
using transport::ICodec;
using transport::IExecutor;
using transport::InteractionEngine;
using transport::InteractionPolicy;
using transport::Key;
using transport::Message;
using transport::MessageKind;
using transport::RequestSpec;
using transport::Result;
using testutil::FakeTransport;
using testutil::InlineExecutor;

namespace {
// 测试 policy:Key=correlation_id 字符串;tag=(int)kind;DDS 风格但用同管道(ReplyTo=Default)。
class TestPolicy : public InteractionPolicy {
 public:
  FrameTag TagOf(const Message& m) override { return static_cast<int>(m.kind); }
  void SetTag(Message& m, FrameTag t) override { m.kind = static_cast<MessageKind>(t); }
  Key NewCorrelation(Message& out) override { out.correlation_id = "c" + std::to_string(++seq_); return out.correlation_id; }
  Key KeyOf(const Message& in) override { return in.correlation_id; }
  void EchoCorrelation(Message& reply, const Message& req) override { reply.correlation_id = req.correlation_id; }
  Endpoint ReplyTo(const Message&) override { return Endpoint::Default(); }
  Route RouteUnmatched(const Message& in) override {
    if (in.kind == MessageKind::kRequest) return Route::kInboundRequest;
    if (in.kind == MessageKind::kOneway || in.kind == MessageKind::kNotify) return Route::kDeliver;
    return Route::kDrop;
  }
 private:
  uint64_t seq_ = 0;
};

int T(MessageKind k) { return static_cast<int>(k); }
Message Pay(std::vector<uint8_t> p) { Message m; m.payload = std::move(p); return m; }

struct Net {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<InteractionEngine> a, b;
  InlineExecutor* exa = nullptr;
  Net() {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::unique_ptr<IExecutor>(std::move(ea)));
    b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::make_unique<InlineExecutor>());
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(InteractionEngine, FireDelivers) {
  Net n; std::vector<uint8_t> got; bool called = false;
  n.b->OnInboundDeliver([&](const Message& m) { got = m.payload; called = true; });
  n.Open();
  ASSERT_TRUE(static_cast<bool>(n.a->Fire(Pay({1, 2, 3}), T(MessageKind::kOneway))));
  EXPECT_TRUE(called); EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));
  n.Close();
}

TEST(InteractionEngine, RequestAwaitTerminalCompletes) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    auto out = req.payload; out.push_back(0xEE);
    (void)n.b->SendReply(req, T(MessageKind::kReply), out);
  });
  n.Open();
  Result<Message> got = Result<Message>::Fail("none");
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.on_terminal = [&](Result<Message> r) { got = std::move(r); };
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xEE}));
  n.Close();
}

TEST(InteractionEngine, IntermediateThenTerminalKeepsPendingThenCompletes) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    (void)n.b->SendReply(req, T(MessageKind::kFeedback), {0x01});
    (void)n.b->SendReply(req, T(MessageKind::kReply), {0x02});
  });
  n.Open();
  std::vector<std::vector<uint8_t>> inter; Result<Message> fin = Result<Message>::Fail("none");
  RequestSpec s; s.request_tag = T(MessageKind::kRequest);
  s.intermediate_tag = T(MessageKind::kFeedback); s.terminal_tag = T(MessageKind::kReply);
  s.on_intermediate = [&](const Message& m) { inter.push_back(m.payload); };
  s.on_terminal = [&](Result<Message> r) { fin = std::move(r); };
  (void)n.a->RequestAwait(Pay({9}), s);
  ASSERT_EQ(inter.size(), 1u); EXPECT_EQ(inter[0], (std::vector<uint8_t>{0x01}));
  ASSERT_TRUE(static_cast<bool>(fin)); EXPECT_EQ(fin.value.payload, (std::vector<uint8_t>{0x02}));
  n.Close();
}

TEST(InteractionEngine, AutoAckOnTerminal) {
  Net n; int acks = 0;
  n.b->OnInboundRequest([&](const Message& req) { (void)n.b->SendReply(req, T(MessageKind::kReply), {0xAA}); });
  n.b->OnInboundDeliver([&](const Message& m) { if (m.kind == MessageKind::kNotify) ++acks; });  // ack = kNotify
  n.Open();
  Result<Message> fin = Result<Message>::Fail("none");
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.auto_ack_tag = T(MessageKind::kNotify); s.on_terminal = [&](Result<Message> r) { fin = std::move(r); };
  (void)n.a->RequestAwait(Pay({1}), s);
  ASSERT_TRUE(static_cast<bool>(fin)); EXPECT_EQ(acks, 1);  // b 收到发起方自动回的 ack
  n.Close();
}

TEST(InteractionEngine, TimeoutRetransmitsThenFailsAndFirstFrameStops) {
  Net n; int reqs = 0;
  n.b->OnInboundRequest([&](const Message&) { ++reqs; });  // 不回
  n.Open();
  Result<Message> got = Result<Message>::Success(Message{});
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 50; s.max_retries = 3; s.on_terminal = [&](Result<Message> r) { got = std::move(r); };
  (void)n.a->RequestAwait(Pay({1}), s);
  EXPECT_EQ(reqs, 1);
  n.exa->FireAll(); EXPECT_EQ(reqs, 2);
  n.exa->FireAll(); EXPECT_EQ(reqs, 3);
  n.exa->FireAll(); EXPECT_EQ(reqs, 4);
  n.exa->FireAll();                                  // 重发耗尽 → 失败
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  EXPECT_EQ(reqs, 4);
  n.Close();
}

TEST(InteractionEngine, PeriodicSendsUntilStopped) {
  Net n; int delivered = 0;
  n.b->OnInboundDeliver([&](const Message&) { ++delivered; });
  n.Open();
  uint32_t h = n.a->StartPeriodic(Pay({7}), T(MessageKind::kOneway), 100);
  EXPECT_EQ(delivered, 1);
  n.exa->FireAll(); EXPECT_EQ(delivered, 2);
  n.a->StopPeriodic(h);
  n.exa->FireAll(); EXPECT_EQ(delivered, 2);
  EXPECT_EQ(n.a->StartPeriodic(Pay({0}), T(MessageKind::kOneway), 0), 0u);  // 零间隔拒绝
  n.Close();
}

TEST(InteractionEngine, StartPeriodicNullFactoryReturnsZero) {
  Net n; n.Open();
  EXPECT_EQ(n.a->StartPeriodic(std::function<transport::Message()>{}, T(MessageKind::kNotify), 50), 0u);  // 空工厂拒绝,不崩
  n.Close();
}

TEST(InteractionEngine, CloseFinalizesPendingNoDoubleInvoke) {
  Net n; int term = 0;
  n.b->OnInboundRequest([&](const Message& req) { (void)n.b->SendReply(req, T(MessageKind::kFeedback), {0x01}); });  // 只中间,不终结
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest);
  s.intermediate_tag = T(MessageKind::kFeedback); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 100000; s.on_terminal = [&](Result<Message>) { ++term; };
  (void)n.a->RequestAwait(Pay({1}), s);
  n.a->Close();                       // 终结挂起一次
  EXPECT_EQ(term, 1);
  n.b->Close();
}

TEST(InteractionEngine, WorksWithThreadExecutor) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(), std::make_unique<TestPolicy>(), nullptr);
  auto b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(), std::make_unique<TestPolicy>(), nullptr);
  b->OnInboundRequest([b](const Message& req) { auto o = req.payload; o.push_back(0xEE); (void)b->SendReply(req, static_cast<int>(MessageKind::kReply), o); });
  (void)b->Open(); (void)a->Open();
  std::promise<Result<Message>> prom; auto fut = prom.get_future();
  RequestSpec s; s.request_tag = static_cast<int>(MessageKind::kRequest); s.terminal_tag = static_cast<int>(MessageKind::kReply);
  s.timeout_ms = 2000; s.on_terminal = [&](Result<Message> r) { prom.set_value(std::move(r)); };
  (void)a->RequestAwait(Pay({3}), s);
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r)); EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xEE}));
  a->Close(); b->Close();
}

// 从真实 ThreadExecutor worker 线程内调用 Close() 不得自连接/崩溃(守卫在 ThreadExecutor::Stop)。
TEST(InteractionEngine, CloseFromWorkerNoSelfJoinCrash) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(), std::make_unique<TestPolicy>(), nullptr);
  auto b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(), std::make_unique<TestPolicy>(), nullptr);
  std::promise<void> prom; auto fut = prom.get_future();
  // inbound handler 在 worker 线程内自关闭,然后置位 promise。
  a->OnInboundDeliver([a, &prom](const Message&) { a->Close(); prom.set_value(); });
  (void)b->Open(); (void)a->Open();
  ASSERT_TRUE(static_cast<bool>(b->Fire(Pay({1}), T(MessageKind::kOneway))));
  ASSERT_EQ(fut.wait_for(std::chrono::seconds(2)), std::future_status::ready);
  fut.get();
  EXPECT_FALSE(a->IsOpen());
  b->Close();
}

TEST(InteractionEngine, PeriodicFactoryFiresLatestEachTick) {
  Net n; std::vector<std::vector<uint8_t>> got;
  n.b->OnInboundDeliver([&](const Message& m) { got.push_back(m.payload); });
  n.Open();
  int ctr = 0;
  uint32_t h = n.a->StartPeriodic(
      [&ctr] { Message m; m.payload = {static_cast<uint8_t>(ctr++)}; return m; },
      T(MessageKind::kNotify), /*interval_ms=*/50);
  n.exa->FireAll();   // 第 2 拍
  n.exa->FireAll();   // 第 3 拍
  n.a->StopPeriodic(h);
  ASSERT_GE(got.size(), 3u);
  EXPECT_EQ(got[0], (std::vector<uint8_t>{0}));   // 立即一帧 = 最新
  EXPECT_EQ(got[1], (std::vector<uint8_t>{1}));   // 每拍重新 make()
  EXPECT_EQ(got[2], (std::vector<uint8_t>{2}));
  n.Close();
}

TEST(InteractionEngine, UpdatePeriodicChangesSubsequentFrames) {
  Net n; std::vector<std::vector<uint8_t>> got;
  n.b->OnInboundDeliver([&](const Message& m) { got.push_back(m.payload); });
  n.Open();
  uint32_t h = n.a->StartPeriodic(Pay({1}), T(MessageKind::kNotify), /*interval_ms=*/50);
  ASSERT_EQ(got.size(), 1u); EXPECT_EQ(got[0], (std::vector<uint8_t>{1}));  // 固定版立即一帧
  EXPECT_TRUE(n.a->UpdatePeriodic(h, Pay({2})));
  n.exa->FireAll();
  ASSERT_EQ(got.size(), 2u); EXPECT_EQ(got[1], (std::vector<uint8_t>{2}));   // 后续帧用新值
  EXPECT_FALSE(n.a->UpdatePeriodic(9999, Pay({3})));                          // 未知 handle → false
  n.a->StopPeriodic(h); n.Close();
}
