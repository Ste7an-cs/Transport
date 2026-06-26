#include "transport/comm/InteractionEngine.hpp"
#include "transport/ITraceSink.hpp"

#include "transport/codec/DdsCodec.hpp"
#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>

using transport::CapturingTraceSink;
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
using transport::Status;
using transport::TraceLevel;
using testutil::FakeTransport;
using testutil::InlineExecutor;

namespace {
// 与 interaction_engine_test 同形的最小 policy(此文件独立,故重声明)。
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

// 计某 category(+可选 message)在记录里出现的次数。
size_t Count(const CapturingTraceSink& c, const std::string& cat, const std::string& msg = "") {
  size_t n = 0;
  for (const auto& r : c.Records())
    if (r.category == cat && (msg.empty() || r.message == msg)) ++n;
  return n;
}

struct Net {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<InteractionEngine> a, b;
  InlineExecutor* exa = nullptr;
  std::shared_ptr<CapturingTraceSink> cap = std::make_shared<CapturingTraceSink>();
  Net() {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    a = std::make_shared<InteractionEngine>(ta, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::unique_ptr<IExecutor>(std::move(ea)));
    b = std::make_shared<InteractionEngine>(tb, std::make_unique<DdsCodec>(),
        std::make_unique<TestPolicy>(), std::make_unique<InlineExecutor>());
    a->SetTrace(cap);                 // 仅 a 装 sink
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(InteractionTrace, FireEmitsSend) {
  Net n;
  n.b->OnInboundDeliver([](const Message&) {});
  n.Open();
  ASSERT_TRUE(static_cast<bool>(n.a->Fire(Pay({1, 2, 3}), T(MessageKind::kOneway))));
  ASSERT_EQ(Count(*n.cap, "send"), 1u);
  for (const auto& r : n.cap->Records())
    if (r.category == "send") { EXPECT_EQ(r.tag, T(MessageKind::kOneway)); EXPECT_EQ(r.size, 3); }
  n.Close();
}

TEST(InteractionTrace, RequestThenTerminal) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    auto out = req.payload; out.push_back(0xEE);
    (void)n.b->SendReply(req, T(MessageKind::kReply), out);
  });
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  EXPECT_EQ(Count(*n.cap, "request"), 1u);
  EXPECT_EQ(Count(*n.cap, "dispatch", "match-terminal"), 1u);
  n.Close();
}

TEST(InteractionTrace, IntermediateThenTerminal) {
  Net n;
  n.b->OnInboundRequest([&](const Message& req) {
    (void)n.b->SendReply(req, T(MessageKind::kFeedback), {1});  // 中间
    (void)n.b->SendReply(req, T(MessageKind::kReply), {2});     // 终结
  });
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest);
  s.intermediate_tag = T(MessageKind::kFeedback); s.terminal_tag = T(MessageKind::kReply);
  s.on_intermediate = [](const Message&) {}; s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  EXPECT_EQ(Count(*n.cap, "dispatch", "match-intermediate"), 1u);
  EXPECT_EQ(Count(*n.cap, "dispatch", "match-terminal"), 1u);
  n.Close();
}

TEST(InteractionTrace, RetransmitsThenTimeout) {
  Net n;
  n.b->OnInboundRequest([](const Message&) {});  // 不回应 → 超时
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 10; s.max_retries = 2; s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  n.exa->FireAll();  // 第1次超时 → 重发1 + 重排
  n.exa->FireAll();  // 第2次超时 → 重发2 + 重排
  n.exa->FireAll();  // 第3次超时 → 达上限,失败
  EXPECT_EQ(Count(*n.cap, "retransmit"), 2u);
  EXPECT_EQ(Count(*n.cap, "timeout"), 1u);
  n.Close();
}

TEST(InteractionTrace, CloseEmitsFinalizedCount) {
  Net n;
  n.b->OnInboundRequest([](const Message&) {});  // 不回应,留一个挂起
  n.Open();
  RequestSpec s; s.request_tag = T(MessageKind::kRequest); s.terminal_tag = T(MessageKind::kReply);
  s.timeout_ms = 10000; s.on_terminal = [](Result<Message>) {};
  ASSERT_TRUE(static_cast<bool>(n.a->RequestAwait(Pay({5}), s)));
  n.a->Close();
  bool found = false;
  for (const auto& r : n.cap->Records())
    if (r.category == "close") { found = true; EXPECT_EQ(r.size, 1); }
  EXPECT_TRUE(found);
  n.b->Close();
}

TEST(InteractionTrace, NoSinkIsSafe) {
  Net n;
  n.a->SetTrace(nullptr);  // 显式清空
  n.b->OnInboundDeliver([](const Message&) {});
  n.Open();
  EXPECT_TRUE(static_cast<bool>(n.a->Fire(Pay({9}), T(MessageKind::kOneway))));
  EXPECT_TRUE(n.cap->Records().empty());  // sink 已摘,零捕获
  n.Close();
}

// 解码失败:独立引擎 + 必败 codec + InjectBytes。
TEST(InteractionTrace, DecodeFailEmitsWarn) {
  class FailCodec : public ICodec {
   public:
    Result<std::vector<uint8_t>> Encode(const Message&) override {
      return Result<std::vector<uint8_t>>::Success({});
    }
    Result<std::vector<Message>> Decode(const uint8_t*, std::size_t) override {
      return Result<std::vector<Message>>::Fail("frame: bad");
    }
  };
  auto t = std::make_shared<FakeTransport>();
  auto cap = std::make_shared<CapturingTraceSink>();
  auto e = std::make_shared<InteractionEngine>(t, std::make_unique<FailCodec>(),
      std::make_unique<TestPolicy>(), std::make_unique<InlineExecutor>());
  e->SetTrace(cap);
  (void)e->Open();
  t->InjectBytes({0xFF, 0xFF});
  EXPECT_EQ(Count(*cap, "decode", "decode-fail"), 1u);
  e->Close();
}
