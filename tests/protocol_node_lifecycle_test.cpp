// -----------------------------------------------------------------------------
// protocol_node_lifecycle_test.cpp — P2-4 生命周期硬化(RT_LIFECYCLE_003–007)
//
// 验收(Fake 传输,单 fiber 协作调度器,pumpFiberUntil 驱动):
//   · 并发/幂等 Start:已 Running 再启成功、不重复 spawn 消费者;Closed 再 Start →
//     kInvalidState(RT_LIFECYCLE_003)。
//   · 配置校验失败:队列上界越界 / key_strategy 空 → Start 返 kConfiguration、停 Created
//     (可重试,不被污染)(RT_LIFECYCLE_007)。
//   · Close 协作取消收敛正在运行的 handler(观察 token 返回)、三方汇合后 Closed、多个
//     WaitClosed 全唤醒(RT_LIFECYCLE_004/006)。
//   · handler 内经 ctx.RequestClose() 发起关闭(**只发信号、不等待**,ADR-0006 D8)→
//     受理即返回、返回时尚未 Closed、其后正常收敛,外部 WaitClosed 唤醒(RT_LIFECYCLE_005)。
//     注:handler 内直调**等待型**关闭接口(node.Close()/WaitClosed())在 D8 下静默挂死,
//     属明确接受的违约面(使用契约,无运行时守卫),故无对应用例。
//   · Close 时未启动的排队业务 → CloseDropCount() 归因(不排空处理,ADR-0001 D5)。
//   · **致命错误自终**(RT_LIFECYCLE_008 / ADR-0005 D5):不可重连介质底层致命错误
//     (Read 返 kClosed)而节点仍 Running → 节点自行 Closing→Closed,在途 WaitClosed
//     等待者被唤醒、在途请求恰好一次 kClosed、未启动业务归因 close_drop、其后 Request/
//     Send 返 kClosed(可观察结果与外部发起关闭一致,SRS §3.1.6.3 第 7 条)。
//     反向用例(TCP 客户端断链**不**自终)在 protocol_node_reconnect_test.cpp。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
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
using transport::Status;
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

}  // namespace

// RT_LIFECYCLE_003:已 Running 再 Start 幂等成功,且不重复 spawn 消费者 fiber——注入一帧
// 业务,handler 恰好被调用一次(双消费者会重复消费或抢读循环导致挂起)。
TEST(ProtocolNodeLifecycle, RedundantStartIsIdempotentAndDoesNotRespawn) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_int calls{0};
  ProtocolNodeConfig config;
  config.handler = [&calls](const Message&, HandlerContext&) -> Status {
    calls.fetch_add(1);
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());
  ASSERT_TRUE(node.Start());  // 已 Running 再启 → 成功、不重复 spawn。
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  ASSERT_TRUE(pumpFiberUntil([&] { return calls.load() == 1; }));
  // 稳定:再泵仍恰好 1 次(无第二个消费者重复消费)。
  pumpFiberUntil([&] { return false; }, 30);
  EXPECT_EQ(calls.load(), 1);

  node.Close();
}

// RT_LIFECYCLE_003:Closed 后再 Start → kInvalidState(不得重启)。
TEST(ProtocolNodeLifecycle, StartAfterClosedReturnsInvalidState) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());
  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());

  auto restarted = node.Start();
  ASSERT_FALSE(restarted);
  EXPECT_EQ(restarted.error(), make_error_code(TransportErrc::kInvalidState));
}

// RT_LIFECYCLE_007:队列事件上界越界(0 < kMinEvents)→ Start 返 kConfiguration、停 Created;
// 重试仍走校验(状态未被污染到 Closed/Running),节点可干净关闭。
TEST(ProtocolNodeLifecycle, InvalidQueueBoundStaysCreatedAndRetryable) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.business_queue_max_events = 0;  // < kMinEvents(1)→ 非法。
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));

  auto first = node.Start();
  ASSERT_FALSE(first);
  EXPECT_EQ(first.error(), make_error_code(TransportErrc::kConfiguration));
  // 停在 Created:传输从未启动(仍 kCreated),节点未进 Running。
  EXPECT_EQ(fake->state(), transport::LifecycleState::kCreated);

  // 重试:仍是配置校验(未被污染成 kInvalidState),证实停 Created 可重试。
  auto retry = node.Start();
  ASSERT_FALSE(retry);
  EXPECT_EQ(retry.error(), make_error_code(TransportErrc::kConfiguration));
}

// RT_LIFECYCLE_007:key_strategy 任一支为空 → Start 返 kConfiguration、停 Created。
TEST(ProtocolNodeLifecycle, NullKeyStrategyRejectedAsConfiguration) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  ProtocolNodeConfig config;
  config.key_strategy.request_key = nullptr;  // 破坏配对契约。
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));

  auto started = node.Start();
  ASSERT_FALSE(started);
  EXPECT_EQ(started.error(), make_error_code(TransportErrc::kConfiguration));
}

// RT_LIFECYCLE_007 + SRS §3.1.4.4(issue #108):默认请求超时非正(0 = "永不超时"、负值
// = 已过期)→ Start 返 kConfiguration、停 Created、可改配重试。节点不得接受"永不超时"的
// 请求:断链已不再终结在途请求(ADR-0004 D3),缺省超时是其唯一兜底终结源。
TEST(ProtocolNodeLifecycle, NonPositiveDefaultRequestTimeoutRejectedAsConfiguration) {
  for (auto bad : {OperationOptions::Clock::duration::zero(),
                   OperationOptions::Clock::duration(-1)}) {
    auto fake_owner = std::make_unique<FakeCoroTransport>();
    FakeCoroTransport* fake = fake_owner.get();
    ProtocolNodeConfig config;
    config.default_request_timeout = bad;
    ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                      std::move(config));

    auto started = node.Start();
    ASSERT_FALSE(started);
    EXPECT_EQ(started.error(), make_error_code(TransportErrc::kConfiguration));
    // 停在 Created:传输从未启动,节点未进 Running。
    EXPECT_EQ(fake->state(), transport::LifecycleState::kCreated);
  }
}

// RT_LIFECYCLE_007:修正配置后重试成功——先以非法配置构造(Start 失败停 Created),再以
// 合法配置构造同型节点验证 Start 成功(node 无 pre-start reconfig API,以状态语义体现)。
TEST(ProtocolNodeLifecycle, ValidConfigStartsAfterInvalidRejected) {
  {
    ProtocolNodeConfig bad;
    bad.business_queue_max_bytes = 1;  // < kMinBytes(64KiB)→ 非法。
    ProtocolNode node(std::make_unique<FakeCoroTransport>(),
                      std::make_unique<SystemCodec>(), std::move(bad));
    auto started = node.Start();
    ASSERT_FALSE(started);
    EXPECT_EQ(started.error(), make_error_code(TransportErrc::kConfiguration));
  }
  {
    ProtocolNodeConfig good;  // 全默认 = 合法。
    ProtocolNode node(std::make_unique<FakeCoroTransport>(),
                      std::make_unique<SystemCodec>(), std::move(good));
    EXPECT_TRUE(node.Start());
    node.Close();
  }
}

// RT_LIFECYCLE_004/006:Close 以协作取消收敛正在运行的 handler(handler 观察 token 返回),
// 三方汇合后 Closed;多个 WaitClosed 等待者全被唤醒。
TEST(ProtocolNodeLifecycle, CloseCooperativelyCancelsHandlerAndWakesAllWaiters) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_bool handler_returned{false};
  std::atomic_bool handler_entered{false};
  ProtocolNodeConfig config;
  config.handler = [&](const Message&, HandlerContext& ctx) -> Status {
    handler_entered.store(true);
    ctx.cancellation().Wait();  // 协作取消:阻塞至 Close 触发令牌再返回。
    handler_returned.store(true);
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 一帧业务被消费者取走并卡在协作取消等待点(handler 正在运行)。同步点由"读循环回到
  // 在途读"改为"handler 已进入"(单读守卫随 ADR-0007 D4 删除,ActiveRead 观测面消失)。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  ASSERT_TRUE(pumpFiberUntil([&] { return handler_entered.load(); }));

  // 多个 WaitClosed 等待者。
  std::atomic_int woken{0};
  auto w1 = Coro::makeTask([&] { (void)node.WaitClosed(); woken.fetch_add(1); });
  auto w2 = Coro::makeTask([&] { (void)node.WaitClosed(); woken.fetch_add(1); });
  auto w3 = Coro::makeTask([&] { (void)node.WaitClosed(); woken.fetch_add(1); });

  ASSERT_TRUE(node.Close());  // 触发令牌 → handler 返回 → 三方汇合 → Closed。
  EXPECT_TRUE(handler_returned.load());
  ASSERT_TRUE(pumpFiberUntil([&] { return woken.load() == 3; }));
  EXPECT_EQ(woken.load(), 3);
  EXPECT_TRUE(w1.get());
  EXPECT_TRUE(w2.get());
  EXPECT_TRUE(w3.get());
}

// RT_LIFECYCLE_005 / ADR-0006 D8:handler 内经 ctx.RequestClose() 发起关闭 → **只发信号、
// 不等待**:调用在消费者 fiber 内立即返回(受理:已置 Closing 拒新交互 + 协作取消已触发),
// 此刻节点**尚未** Closed(收敛还要等本 handler 退出);节点随后由读循环收敛到 Closed,外部
// WaitClosed 完成(不挂起)即证无自等待死锁。
TEST(ProtocolNodeLifecycle, HandlerRequestCloseDoesNotSelfDeadlock) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode* node_ptr = nullptr;
  std::atomic_bool accepted{false};
  std::atomic_bool cancelled_on_return{false};
  std::atomic_bool closed_on_return{true};
  std::atomic_bool send_rejected_on_return{false};
  ProtocolNodeConfig config;
  config.handler = [&](const Message&, HandlerContext& ctx) -> Status {
    // 消费者 fiber 内发起关闭:不等待,返回即"已受理"(RequestClose 若等待收敛,本行
    // 就是等自己退出 → 整个用例挂死)。
    accepted.store(static_cast<bool>(ctx.RequestClose()));
    // 受理已生效:汇合信号(协作取消 + 拒新交互)在返回前就已发出。
    cancelled_on_return.store(ctx.cancellation().IsCancellationRequested());
    Message probe;
    probe.frm_type = FrameType::kState;
    auto sent = ctx.Send(std::move(probe));
    send_rejected_on_return.store(
        !sent && sent.error() == make_error_code(TransportErrc::kClosed));
    // 但**尚未关完**:收敛(置 Closed + 记关闭时延)要等本 handler 退出后由读循环做。
    closed_on_return.store(node_ptr->LastCloseLatency() !=
                           OperationOptions::Clock::duration::zero());
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  node_ptr = &node;
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  // 节点应自行收敛到 Closed;外部 WaitClosed 完成(不挂起)证实不自锁。
  EXPECT_TRUE(node.WaitClosed());
  EXPECT_TRUE(accepted.load());                // 受理成功。
  EXPECT_TRUE(cancelled_on_return.load());     // 返回时汇合信号已发出。
  EXPECT_TRUE(send_rejected_on_return.load());  // 返回时已拒新交互(Closing)。
  EXPECT_FALSE(closed_on_return.load());       // 返回时尚未 Closed(只受理,不等待)。
}

// RT_LIFECYCLE_005 / ADR-0006 D8:`RequestClose()` 的返回值只表示"已受理",不表示"已关完"
// ——以**外部** WaitClosed 等待者为观察点:handler 内 RequestClose 返回的那一刻等待者尚未
// 被唤醒(节点未 Closed),其后读循环收敛完成,该外部等待者才正常返回。
TEST(ProtocolNodeLifecycle, HandlerRequestCloseIsAcceptedButNotYetClosed) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_int woken{0};
  std::atomic_int woken_on_request_close_return{-1};
  ProtocolNodeConfig config;
  config.handler = [&](const Message&, HandlerContext& ctx) -> Status {
    auto accepted = ctx.RequestClose();
    woken_on_request_close_return.store(woken.load());  // 快照:此刻外部等待者是否已醒。
    return accepted;
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 外部(非内部工作单元)等待者:唯一被授权等待收敛的一方。
  auto waiter = Coro::makeTask([&] {
    (void)node.WaitClosed();
    woken.fetch_add(1);
  });

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  ASSERT_TRUE(pumpFiberUntil([&] { return woken.load() == 1; }));  // 收敛完成,等待者醒。
  EXPECT_EQ(woken_on_request_close_return.load(), 0);  // 受理返回时:尚未 Closed。
  EXPECT_TRUE(waiter.get());
  EXPECT_TRUE(node.WaitClosed());  // 已 Closed:后续外部等待立即成功。
}

// RT_LIFECYCLE / ADR-0001 D5:Close 时未启动的排队业务 → CloseDropCount() 归因,不排空处理。
// handler 卡在首条(协作取消等待),其余入队;Close 后消费者退出、finalizer Drain 剩余计数。
TEST(ProtocolNodeLifecycle, UnstartedQueuedBusinessCountedAsCloseDrop) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_int entered{0};
  ProtocolNodeConfig config;
  config.handler = [&entered](const Message&, HandlerContext& ctx) -> Status {
    entered.fetch_add(1);
    ctx.cancellation().Wait();  // 卡住首条,让后续帧只入队不启动。
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));  // 被消费、卡住。
  ASSERT_TRUE(pumpFiberUntil([&] { return entered.load() == 1; }));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0002));  // 入队,未启动。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0003));  // 入队,未启动。
  // 让读循环把两帧解码入队(消费者卡住不取)。
  pumpFiberUntil([&] { return false; }, 40);

  ASSERT_TRUE(node.Close());  // 协作取消 → handler 返回退出 → finalizer Drain 剩余 2 帧。
  EXPECT_TRUE(node.WaitClosed());
  EXPECT_EQ(node.CloseDropCount(), 2u);  // 未启动的两帧归因 close_drop,不处理。
  EXPECT_EQ(entered.load(), 1);          // 仅首条曾启动。
}

// P5-3(issue #88):同一场景配置 trace_sink → close_drop 逐条 RecordDrop,DroppedCount
// 与 sink 收到的 TraceEvent 条数一致同步(2 帧未启动 → 2 条 category="drop"/kCloseDrop)。
TEST(ProtocolNodeLifecycle, UnstartedQueuedBusinessCloseDropWithSinkEmitsTraceEvents) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_int entered{0};
  CapturingTraceSink sink;
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  config.handler = [&entered](const Message&, HandlerContext& ctx) -> Status {
    entered.fetch_add(1);
    ctx.cancellation().Wait();  // 卡住首条,让后续帧只入队不启动。
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));  // 被消费、卡住。
  ASSERT_TRUE(pumpFiberUntil([&] { return entered.load() == 1; }));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0002));  // 入队,未启动。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 3, 0x0003));  // 入队,未启动。
  pumpFiberUntil([&] { return false; }, 40);

  ASSERT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
  EXPECT_EQ(node.CloseDropCount(), 2u);

  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 2u);  // 逐条归因:2 帧 close_drop → 2 条 TraceEvent。
  for (const auto& rec : records) {
    EXPECT_EQ(rec.category, "drop");
    EXPECT_EQ(rec.message, DropReasonName(DropReason::kCloseDrop));
  }
}

// RT_LIFECYCLE_008 / ADR-0005 D5:不可重连介质发生底层致命错误(Read 返 kClosed,ADR-0004
// D1 唯一终止语义)而节点仍 Running → 节点**自行**收敛:读循环置 Closing、发与 Close 完全
// 相同的一组汇合信号(含 handler 协作取消)、走同一段收敛尾段。断言其可观察结果与外部发起
// 关闭一致(SRS §3.1.6.3 第 7 条):在途 WaitClosed 等待者被唤醒(不再是僵尸节点)、未启动
// 的排队业务归因 close_drop、其后 Request/Send 一律 kClosed、再 Close 幂等成功。
TEST(ProtocolNodeLifecycle, FatalReadErrorSelfTerminatesAndWakesWaiters) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  std::atomic_int entered{0};
  CapturingTraceSink sink;
  ProtocolNodeConfig config;
  config.trace_sink = &sink;
  config.handler = [&entered](const Message&, HandlerContext& ctx) -> Status {
    entered.fetch_add(1);
    // 卡住首条:自终若不触发 handler 协作取消,收敛将卡在 join handler 上(必挂)。
    ctx.cancellation().Wait();
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));  // 被消费、卡住。
  ASSERT_TRUE(pumpFiberUntil([&] { return entered.load() == 1; }));
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 2, 0x0002));  // 入队,未启动。
  pumpFiberUntil([&] { return false; }, 40);  // 让读循环把该帧解码入队(确定化)。

  // 在途关闭等待者:自终前登记,收敛后须全部被唤醒(僵尸节点的正是这些等待者永不返回)。
  std::atomic_int woken{0};
  auto w1 = Coro::makeTask([&] { (void)node.WaitClosed(); woken.fetch_add(1); });
  auto w2 = Coro::makeTask([&] { (void)node.WaitClosed(); woken.fetch_add(1); });

  // 底层致命错误:此刻节点仍 Running,无人调用 Close。
  fake->InjectError(make_error_code(TransportErrc::kClosed));

  ASSERT_TRUE(pumpFiberUntil([&] { return woken.load() == 2; }));
  EXPECT_EQ(woken.load(), 2);
  EXPECT_TRUE(w1.get());
  EXPECT_TRUE(w2.get());
  EXPECT_EQ(entered.load(), 1);          // 仅首条曾启动(取消后返回)。
  EXPECT_EQ(node.CloseDropCount(), 1u);  // 未启动的一帧归因 close_drop,不处理。
  const auto records = DropRecords(sink.Records());
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(records.front().message, DropReasonName(DropReason::kCloseDrop));

  // 其后新交互一律 kClosed;再 Close / WaitClosed 幂等成功(不挂起)。
  Message req;
  req.message_id = 0x0007;
  auto after = node.Request(std::move(req));
  ASSERT_FALSE(after);
  EXPECT_EQ(after.error(), make_error_code(TransportErrc::kClosed));
  Message evt;
  evt.message_id = 0x0008;
  auto sent = node.Send(std::move(evt));
  ASSERT_FALSE(sent);
  EXPECT_EQ(sent.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_TRUE(node.Close());
  EXPECT_TRUE(node.WaitClosed());
}

// RT_LIFECYCLE_008 + SRS §3.1.6.3 第 3 条:自终路径必须发出**与 Close 相同**的 node 侧收敛
// 信号(PendingTable.FailAll)——在途请求恰好一次以 kClosed 终结,而不是苦等各自的总超时。
// (该信号此前只作为 Close 的入参传入,自终取不到;故改由 node 的 `DoClose()` 钩子承载。)
TEST(ProtocolNodeLifecycle, FatalReadErrorSelfTerminationFailsInFlightRequest) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>());
  ASSERT_TRUE(node.Start());

  // 在途请求:对端(fake)不回响应 → 只能由收敛或总超时(默认 30s)终结。
  transport::Result<Message> outcome{make_error_code(TransportErrc::kInternal)};
  std::atomic_bool done{false};
  auto request = Coro::makeTask([&] {
    Message req;
    req.message_id = 0x0011;
    outcome = node.Request(std::move(req));
    done.store(true);
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return node.PendingCount() == 1u; }));
  EXPECT_FALSE(done.load());

  fake->InjectError(make_error_code(TransportErrc::kClosed));  // 底层致命错误。

  ASSERT_TRUE(pumpFiberUntil([&] { return done.load(); }));
  ASSERT_FALSE(outcome);
  EXPECT_EQ(outcome.error(), make_error_code(TransportErrc::kClosed));
  EXPECT_EQ(node.PendingCount(), 0u);
  EXPECT_TRUE(request.get());
  EXPECT_TRUE(node.WaitClosed());  // 已自行收敛:不挂起。
}
