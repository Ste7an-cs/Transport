#include "transport/comm/ProtocolNode.hpp"

#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include "transport/Endpoint.hpp"
#include "transport/ITraceSink.hpp"
#include "transport/codec/SystemCodec.hpp"

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

// 录制本端 Send 的出站字节(仍照常转发给对端),用于观测自动 ack。
class RecordingTransport : public FakeTransport {
 public:
  std::vector<std::vector<uint8_t>> sent;
  transport::Status Send(const std::vector<uint8_t>& bytes) override {
    sent.push_back(bytes);
    return FakeTransport::Send(bytes);
  }
};

// 记录最近一次带 Endpoint 的 Send 的目的地(用于验证发送方法透传 to)。
class DestRecorder : public FakeTransport {
 public:
  transport::Endpoint last_to;
  transport::Status Send(const std::vector<uint8_t>& bytes, const transport::Endpoint& to) override {
    last_to = to;
    return FakeTransport::Send(bytes);
  }
};

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
  ASSERT_TRUE(static_cast<bool>(p.a->SendNoResponse(0x10, P({1, 2, 3}))));
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
      p.a->Request(0x10, P({5}), [&](Result<Message> rr) { got = std::move(rr); })));
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
  (void)p.a->Request(0x10, P({0x42}), [&](Result<Message> rr) { got = std::move(rr); });
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
  (void)p.a->Request(0x10, P({1}), [&](Result<Message> rr) { got = std::move(rr); });
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
      a->Request(0x10, P({3}), [&](Result<Message> r) { prom.set_value(std::move(r)); })));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xEE}));
  a->Close(); b->Close();
}

TEST(ProtocolNode, WithFeedbackCompletesOnResult) {
  Pair p; p.Open();
  p.b->on_cmd = [](const Message& m, Responder r) {
    auto out = m.payload; out.push_back(0xCC); (void)r.Result(out);  // 直接发 RESULT
  };
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      p.a->RequestWithResult(0x10, P({6}), [&](Result<Message> rr) { got = std::move(rr); })));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.frm_type, FrameType::kResult);
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{6, 0xCC}));
  p.Close();
}

TEST(ProtocolNode, NeedFeedbackResponseThenResultThenAck) {
  // 发起方 a 用 RecordingTransport 录出站字节,以真实观测收 RESULT 后自动回的 RESPONSE ack。
  auto ta = std::make_shared<RecordingTransport>();
  auto tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<TestNode>(ta, nullptr, Cfg(), std::make_unique<InlineExecutor>());
  auto b = std::make_shared<TestNode>(tb, nullptr, Cfg(), std::make_unique<InlineExecutor>());
  std::vector<uint8_t> resp, res; int order = 0; int resp_at = 0, res_at = 0;
  Message req_seen;
  // 对端:收 COMMAND → 回 RESPONSE,再回 RESULT(发起方收 RESULT 后会自动回 RESPONSE ack)。
  b->on_cmd = [&](const Message& m, Responder r) {
    req_seen = m;
    (void)r.Response(P({0x01}));
    (void)r.Result(m.payload);
  };
  (void)b->Open(); (void)a->Open();
  ta->sent.clear();  // 丢弃 Open 期间无关字节,只看请求后的出站
  ASSERT_TRUE(static_cast<bool>(a->RequestNeedFeedback(
      0x10, P({7}),
      [&](Result<Message> rr) { if (rr) { resp = rr.value.payload; resp_at = ++order; } },
      [&](Result<Message> rr) { if (rr) { res = rr.value.payload; res_at = ++order; } })));
  EXPECT_EQ(resp, (std::vector<uint8_t>{0x01}));   // 中间 RESPONSE
  EXPECT_EQ(res, (std::vector<uint8_t>{7}));         // 最终 RESULT
  EXPECT_LT(resp_at, res_at);                        // 先 RESPONSE 后 RESULT

  // 观测自动 ack:解码 a 的出站字节,最后一帧应为 RESPONSE,且 session/message 回显请求帧。
  transport::SystemCodec codec;
  bool ack_after_result = false;
  for (const auto& bytes : ta->sent) {
    auto decoded = codec.Decode(bytes.data(), bytes.size());
    ASSERT_TRUE(static_cast<bool>(decoded));
    for (const auto& fm : decoded.value) {
      if (fm.frm_type == FrameType::kResponse &&
          fm.session_id == req_seen.session_id && fm.message_id == req_seen.message_id) {
        ack_after_result = true;
      }
    }
  }
  EXPECT_TRUE(ack_after_result);  // 删掉自动 ack 此断言即失败
  // 出站序列只应有 COMMAND(请求)与 RESPONSE(自动 ack)两帧。
  EXPECT_EQ(ta->sent.size(), 2u);
  a->Close(); b->Close();
}

// 回归(FIX 1):needfeedback 收到中间 RESPONSE 后未收 RESULT 即 Close,
// on_response 必须只触发一次(不被 Close 二次触发),on_result 以 conn: 失败一次。
TEST(ProtocolNode, NeedFeedbackCloseAfterResponseNoDoubleInvoke) {
  Pair p; p.Open();
  // 对端只回 RESPONSE,不回 RESULT。
  p.b->on_cmd = [](const Message& m, Responder r) { (void)m; (void)r.Response(P({0x55})); };
  int resp_calls = 0, result_calls = 0;
  std::string result_err;
  ASSERT_TRUE(static_cast<bool>(p.a->RequestNeedFeedback(
      0x10, P({8}),
      [&](Result<Message> rr) { (void)rr; ++resp_calls; },
      [&](Result<Message> rr) { ++result_calls; if (!rr) result_err = rr.error; })));
  EXPECT_EQ(resp_calls, 1);     // 中间 RESPONSE 已触发一次
  EXPECT_EQ(result_calls, 0);   // 尚无 RESULT
  p.Close();
  EXPECT_EQ(resp_calls, 1);     // Close 未二次触发 on_response
  EXPECT_EQ(result_calls, 1);   // on_result 因关闭失败一次
  EXPECT_EQ(result_err.rfind("conn:", 0), 0u);
}

TEST(ProtocolNode, RepeatingSendsPeriodicallyUntilStopped) {
  Pair p; p.Open();
  uint32_t h = p.a->StartRepeating(0x10, P({0xAB}), 100);
  EXPECT_EQ(p.b->commands, 1);   // 起始即发一帧 STATE
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 2);  // 到点再发
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 3);
  p.a->StopRepeating(h);
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 3);  // 停后不再发
  p.Close();
}

TEST(ProtocolNode, RepeatingZeroIntervalRejected) {
  Pair p; p.Open();
  uint32_t h = p.a->StartRepeating(0x10, P({0xAB}), 0);
  EXPECT_EQ(h, 0u);              // 0 间隔被拒,返回无效句柄 0
  EXPECT_EQ(p.b->commands, 0);   // 未发任何 STATE 帧
  p.Close();
}

TEST(ProtocolNode, SetTraceForwardsToEngine) {
  Pair p;
  auto cap = std::make_shared<transport::CapturingTraceSink>();
  p.a->SetTrace(cap);               // 节点透传 → 引擎 SetTrace
  p.Open();
  (void)p.a->SendNoResponse(/*cmd=*/0x10, P({1}));
  bool sawSend = false;
  for (const auto& r : cap->Records()) if (r.category == "send") sawSend = true;
  EXPECT_TRUE(sawSend);              // 引擎在发起端发了 trace,证明透传生效
  p.Close();
}

TEST(ProtocolNode, HeartbeatPeriodic) {
  ProtocolConfig ca = Cfg(); ca.heartbeat_interval_ms = 100;  // a 周期发心跳
  Pair p(ca, Cfg());
  p.Open();
  EXPECT_EQ(p.b->heartbeats, 1);   // StartPeriodic 立即发一帧 HEARTBEAT → b 收
  p.exa->FireAll();                // a 的心跳定时器到点 → 再发 HEARTBEAT → b 收
  EXPECT_GE(p.b->heartbeats, 2);   // b 的 OnHeartbeat 周期性被调
  p.Close();
}

TEST(ProtocolNode, SendForwardsDestinationEndpoint) {
  auto rec = std::make_shared<DestRecorder>();
  auto peer = std::make_shared<FakeTransport>();
  FakeTransport::Link(rec, peer);
  auto node = std::make_shared<TestNode>(rec, nullptr, Cfg(), std::make_unique<InlineExecutor>());
  (void)node->Open();
  (void)node->SendNoResponse(0x10, P({1}), transport::Endpoint::Net("9.9.9.9", 1234));
  EXPECT_EQ(rec->last_to.kind, transport::Endpoint::Kind::kNet);
  EXPECT_EQ(rec->last_to.host, "9.9.9.9");
  EXPECT_EQ(rec->last_to.port, 1234);
  node->Close();
}

TEST(ProtocolNode, RepeatingPullSendsLatestState) {
  Pair p; std::vector<std::vector<uint8_t>> got;
  p.b->on_cmd = [&](const Message& m, Responder) { got.push_back(m.payload); };
  p.Open();
  int ctr = 0;
  uint32_t h = p.a->StartRepeating(/*cmd=*/0x14,
      [&ctr] { return P({static_cast<uint8_t>(ctr++)}); }, /*interval_ms=*/50);
  p.exa->FireAll();   // 第 2 拍
  p.exa->FireAll();   // 第 3 拍
  p.a->StopRepeating(h);
  ASSERT_GE(got.size(), 3u);
  EXPECT_EQ(got[0], P({0}));
  EXPECT_EQ(got[1], P({1}));
  EXPECT_EQ(got[2], P({2}));
  p.Close();
}

TEST(ProtocolNode, UpdateRepeatingChangesState) {
  Pair p; std::vector<std::vector<uint8_t>> got;
  p.b->on_cmd = [&](const Message& m, Responder) { got.push_back(m.payload); };
  p.Open();
  uint32_t h = p.a->StartRepeating(/*cmd=*/0x14, P({1}), /*interval_ms=*/50);
  ASSERT_EQ(got.size(), 1u); EXPECT_EQ(got[0], P({1}));
  EXPECT_TRUE(p.a->UpdateRepeating(h, /*cmd=*/0x14, P({2})));
  p.exa->FireAll();
  ASSERT_EQ(got.size(), 2u); EXPECT_EQ(got[1], P({2}));
  p.a->StopRepeating(h); p.Close();
}
