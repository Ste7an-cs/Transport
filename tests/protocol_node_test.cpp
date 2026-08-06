// -----------------------------------------------------------------------------
// protocol_node_test.cpp — 最小 ProtocolNode 端到端契约单测(RT_NODE_003 / RT_REQUEST)
//
// 用 FakeCoroTransport 作传输、真实 SystemCodec 作 codec,全程走 encode/decode。
// 构造响应帧:另起一个 SystemCodec 实例 Encode 一个响应 Message 得字节,fake.Inject
// (Datagram) 喂给读循环。请求 fiber 用 makeTask 起独立 fiber 在 Request 挂起,主(测试)
// fiber 用 pumpFiberUntil 驱动时钟 / 让出(coro_test_main 范式)。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <vector>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/node/ProtocolNode.hpp"
#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/ICodec.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::CapturingTraceSink;
using transport::CorrelationKeyStrategy;
using transport::Datagram;
using transport::DefaultProtocolKeyStrategy;
using transport::DropReason;
using transport::DropReasonName;
using transport::FrameType;
using transport::ICodec;
using transport::Message;
using transport::OperationOptions;
using transport::ProtocolKey;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::Result;
using transport::SystemCodec;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

// 过滤出 category=="drop" 的记录:sink 同时收 P5-3 的丢弃事件与 P5-4 的 send/recv/
// decode/close 等事件(共用同一 trace_sink),按 category 过滤才是"这次丢弃恰好一条
// Trace"断言的正确写法,不能假设 sink 总记录数等于丢弃数。
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

// 构造一个响应帧 Datagram:用独立 SystemCodec 把响应 Message 编成字节。
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
  EXPECT_TRUE(bytes);
  Datagram dg;
  dg.bytes = bytes ? std::move(bytes).value() : std::vector<std::uint8_t>{};
  return dg;
}

// 构造一个业务帧 Datagram(非 kResponse/kResult)。
Datagram MakeBusinessDatagram(FrameType frm_type, std::uint8_t session_id,
                              std::uint16_t message_id) {
  Message msg;
  msg.frm_type = frm_type;
  msg.session_id = session_id;
  msg.message_id = message_id;
  SystemCodec wire;
  auto bytes = wire.Encode(msg);
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

// codec 双:Encode 委托真实 SystemCodec(Request 仍可正常编码上线),Decode 恒返回
// kCodec——供确定性触发 DecodeAndDispatch 的坏帧分支(P5-3 kBadFrame),不依赖
// SystemCodec 内部 resync 细节(其 Decode 遇坏 CRC 会前移重扫、不对外报错)。
class AlwaysFailDecodeCodec : public ICodec {
 public:
  Result<std::vector<std::uint8_t>> Encode(const Message& msg) override {
    return real_.Encode(msg);
  }
  Result<std::vector<Message>> Decode(const std::uint8_t*, std::size_t) override {
    return make_error_code(TransportErrc::kCodec);
  }

 private:
  SystemCodec real_;
};

}  // namespace

// 端到端 happy path:发 Request → 注入匹配响应 → Request 恰好一次完成、返响应、关联清理。
TEST(ProtocolNode, RequestResolvedByMatchingResponse) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {0x11, 0x22}));
    done = true;
  });

  // 请求 fiber 已进入 Write/Wait:node 盖了 session_id=0(首个滚动值)。等它把帧写出。
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  // 注入匹配响应:session 不变、message_id=请求码|0x1000、frm_type=kResponse。
  fake->Inject(MakeResponseDatagram(0, 0x1002, {0xAB, 0xCD}));

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome);
  EXPECT_EQ(outcome.value().frm_type, FrameType::kResponse);
  EXPECT_EQ(outcome.value().session_id, 0);
  EXPECT_EQ(outcome.value().message_id, 0x1002);
  EXPECT_EQ(outcome.value().payload, (std::vector<std::uint8_t>{0xAB, 0xCD}));
  EXPECT_EQ(node.UnmatchedResponseCount(), 0u);

  node.Close();
  EXPECT_TRUE(request.get());
}

// 迟到 / 乱序:请求完成后再注入同 key 响应 → 丢弃 + UnmatchedResponseCount +1,不二次完成。
TEST(ProtocolNode, LateResponseAfterCompletionIsDroppedAndCounted) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  fake->Inject(MakeResponseDatagram(0, 0x1002, {}));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome);

  // 关联已清理:同 key 的迟到响应无在途匹配 → 丢弃计数 +1,不影响已完成的 Request。
  fake->Inject(MakeResponseDatagram(0, 0x1002, {}));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 1u; }));
  EXPECT_EQ(node.UnmatchedResponseCount(), 1u);

  node.Close();
  EXPECT_TRUE(request.get());
}

// 业务帧:注入 kState / kCommand → DroppedNoHandlerCount +1,不投递、不 crash。
TEST(ProtocolNode, BusinessFrameCountedAsDroppedNoHandler) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0005));
  fake->Inject(MakeBusinessDatagram(FrameType::kCommand, 4, 0x0006));

  ASSERT_TRUE(pumpFiberUntil([&] { return node.DroppedNoHandlerCount() == 2u; }));
  EXPECT_EQ(node.DroppedNoHandlerCount(), 2u);
  EXPECT_EQ(node.UnmatchedResponseCount(), 0u);

  node.Close();
}

// 自定义 key:改 response_marker=0x2000 → 匹配按新规则成立。
TEST(ProtocolNode, CustomKeyStrategyMatchesByNewMarker) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.key_strategy = DefaultProtocolKeyStrategy(0x2000);
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0007, {}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));
  // 响应码按新 marker:请求 0x0007 → 响应 0x2007。旧 0x1000 规则下不会匹配。
  fake->Inject(MakeResponseDatagram(0, 0x2007, {0x01}));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome);
  EXPECT_EQ(outcome.value().message_id, 0x2007);
  EXPECT_EQ(node.UnmatchedResponseCount(), 0u);

  node.Close();
  EXPECT_TRUE(request.get());
}

// 生命周期:在途 Request → Close 恰好一次返 kClosed;WaitClosed 在读循环退出后完成;
// 关闭后再 Request → kClosed。
TEST(ProtocolNode, CloseFailsInflightRequestAndBlocksLaterRequests) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    outcome = node.Request(MakeRequest(0x0002, {}));
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !fake->sent().empty(); }));

  // Close:在途 Request 恰好一次以 kClosed 收敛。
  ASSERT_TRUE(node.Close());
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kClosed));

  // WaitClosed 在读循环退出后完成。
  EXPECT_TRUE(node.WaitClosed());

  // 关闭后再 Request → kClosed。
  auto after = node.Request(MakeRequest(0x0003, {}));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));

  EXPECT_TRUE(request.get());
}

// (可选)Request 超时:小 deadline 无响应 → kTimeout 且关联清理。
TEST(ProtocolNode, RequestTimesOutAndReleasesCorrelation) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 20ms;
    outcome = node.Request(MakeRequest(0x0002, {}), options);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));

  // 关联已清理:此刻注入迟到响应 → 无匹配丢弃计数 +1(证明超时时已 Evict entry)。
  fake->Inject(MakeResponseDatagram(0, 0x1002, {}));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 1u; }));

  node.Close();
  EXPECT_TRUE(request.get());
}

// -----------------------------------------------------------------------------
// 总超时缺省值(SRS §3.1.4.4 / ADR-0004 D3,issue #108)——调用方不给 deadline 时节点
// 套用 config.default_request_timeout;节点不接受"永不超时"的请求。测试把默认值配小
// (50ms / 20ms,而非缺省 30s)以落在单测时间尺度内。
// -----------------------------------------------------------------------------

// ①不给 deadline:无响应时于配置的默认超时后返 kTimeout,且关联被清理(与显式 deadline
// 走同一条终结路径)。若无此缺省,断链后该请求将无任何终结源(交互层已不再终结在途请求)。
TEST(ProtocolNode, RequestWithoutDeadlineTimesOutAtConfiguredDefault) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.default_request_timeout = 50ms;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  OperationOptions::Clock::duration elapsed{};
  auto request = Coro::makeTask([&] {
    const auto started = OperationOptions::Clock::now();
    outcome = node.Request(MakeRequest(0x0002, {}));  // 不给 options → 走默认超时。
    elapsed = OperationOptions::Clock::now() - started;
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_GE(elapsed, 50ms);  // 不早于默认超时终结(缺省值确实被套用而非即时失败)。

  // 关联已清理:迟到响应无匹配丢弃 +1(证明超时时已 Evict entry、session_id 已归还)。
  fake->Inject(MakeResponseDatagram(0, 0x1002, {}));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 1u; }));

  node.Close();
  EXPECT_TRUE(request.get());
}

// ②-a 显式 deadline 比默认值短:以调用方的为准(默认值不得把它拉长)。默认配 2s,显式
// 给 20ms —— 若默认值覆盖了显式值,请求将在 pump 预算内一直挂着而非 kTimeout。
TEST(ProtocolNode, ExplicitShortDeadlineNotOverriddenByDefault) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  ProtocolNodeConfig config;
  config.default_request_timeout = 2s;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  OperationOptions::Clock::duration elapsed{};
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 20ms;
    const auto started = OperationOptions::Clock::now();
    outcome = node.Request(MakeRequest(0x0002, {}), options);
    elapsed = OperationOptions::Clock::now() - started;
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 1000));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kTimeout));
  EXPECT_LT(elapsed, 1s);  // 按 20ms 终结,而非等到 2s 的默认值。

  node.Close();
  EXPECT_TRUE(request.get());
}

// ②-b 显式 deadline 比默认值长:同样以调用方的为准(默认值不得把它截短)。默认配 20ms,
// 显式给 3s —— 越过默认值良久后请求仍在途,随后注入的匹配响应正常完成它。
TEST(ProtocolNode, ExplicitLongDeadlineNotShortenedByDefault) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.default_request_timeout = 20ms;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  bool done = false;
  auto request = Coro::makeTask([&] {
    OperationOptions options;
    options.deadline = OperationOptions::Clock::now() + 3s;
    outcome = node.Request(MakeRequest(0x0002, {}), options);
    done = true;
  });

  // 默认超时(20ms)早已越过,请求仍未终结。
  EXPECT_FALSE(pumpFiberUntil([&] { return done; }, 200));

  fake->Inject(MakeResponseDatagram(0, 0x1002, {0x5A}));
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(outcome);
  EXPECT_EQ(outcome.value().payload, std::vector<std::uint8_t>{0x5A});

  node.Close();
  EXPECT_TRUE(request.get());
}

// -----------------------------------------------------------------------------
// P5-3:全线接入丢弃归因(issue #88)——配 CapturingTraceSink 时,各丢弃点计数与
// 可辨识的 TraceEvent(category="drop", message=DropReasonName)同步产生。
// -----------------------------------------------------------------------------

// kUnmatchedOrLateResponse:迟到响应归因丢弃时,配置 trace_sink → 收到对应事件。
TEST(ProtocolNode, UnmatchedResponseWithSinkEmitsDropTrace) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  CapturingTraceSink sink;
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 无在途请求:注入一条响应帧 → 无匹配、归因丢弃。
  fake->Inject(MakeResponseDatagram(0, 0x1002, {}));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.UnmatchedResponseCount() == 1u; }));
  EXPECT_EQ(node.UnmatchedResponseCount(), 1u);

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message,
           DropReasonName(DropReason::kUnmatchedOrLateResponse));

  node.Close();
}

// kNoHandlerConfigured:未设 handler 的业务帧归因丢弃时,配置 trace_sink → 收到对应事件。
TEST(ProtocolNode, DroppedNoHandlerWithSinkEmitsDropTrace) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  CapturingTraceSink sink;
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.DroppedNoHandlerCount() == 1u; }));

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message, DropReasonName(DropReason::kNoHandlerConfigured));

  node.Close();
}

// kBadFrame:codec.Decode 失败(坏帧/codec 语义错误)时,新增 BadFrameCount() 归因 +1,
// 配置 trace_sink → 收到对应事件。
TEST(ProtocolNode, BadFrameDecodeFailureCountedAndTraced) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  CapturingTraceSink sink;
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  ProtocolNode node(std::move(fake_owner), std::make_unique<AlwaysFailDecodeCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  EXPECT_EQ(node.BadFrameCount(), 0u);
  Datagram garbage;
  garbage.bytes = {0xDE, 0xAD, 0xBE, 0xEF};
  fake->Inject(std::move(garbage));

  ASSERT_TRUE(pumpFiberUntil([&] { return node.BadFrameCount() == 1u; }));
  EXPECT_EQ(node.BadFrameCount(), 1u);

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().category, "drop");
  EXPECT_EQ(records.front().message, DropReasonName(DropReason::kBadFrame));

  // 再来一帧:计数与 Trace 同步递增。
  Datagram garbage2;
  garbage2.bytes = {0x01};
  fake->Inject(std::move(garbage2));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.BadFrameCount() == 2u; }));
  EXPECT_EQ(DropRecords(sink.Records()).size(), 2u);

  node.Close();
}

// RT_TRACE_002:未配置 trace_sink(默认 nullptr)时,坏帧 / 迟到响应的计数行为与配置了
// sink 时完全一致——sink 只是可选旁路,不影响控制流/计数。
TEST(ProtocolNode, NoSinkConfiguredCountsUnaffected) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<AlwaysFailDecodeCodec>());
  ASSERT_TRUE(node.Start());  // config.trace_sink 缺省 nullptr。

  Datagram garbage;
  garbage.bytes = {0xFF};
  fake->Inject(std::move(garbage));
  ASSERT_TRUE(pumpFiberUntil([&] { return node.BadFrameCount() == 1u; }));
  EXPECT_EQ(node.BadFrameCount(), 1u);

  node.Close();
}
