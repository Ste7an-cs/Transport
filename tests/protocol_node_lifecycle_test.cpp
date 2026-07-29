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
//   · handler 内经 ctx.RequestClose() / 捕获 node& 调 node.Close() 发起关闭 → 不自锁、
//     正常收敛(RT_LIFECYCLE_005)。
//   · Close 时未启动的排队业务 → CloseDropCount() 归因(不排空处理,ADR-0001 D5)。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

#include "await/awaitable.hpp"
#include "task/fibertask.h"
#include "transport/node/ProtocolNode.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::Datagram;
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
  ProtocolNodeConfig config;
  config.handler = [&handler_returned](const Message&, HandlerContext& ctx) -> Status {
    ctx.cancellation().Wait();  // 协作取消:阻塞至 Close 触发令牌再返回。
    handler_returned.store(true);
    return Status{};
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  // 一帧业务被消费者取走并卡在协作取消等待点(handler 正在运行)。
  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  ASSERT_TRUE(pumpFiberUntil([&] { return fake->ActiveRead(); }));

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

// RT_LIFECYCLE_005:handler 内经 ctx.RequestClose() 发起关闭 → 不自锁,节点正常收敛到
// Closed(外部 WaitClosed 完成即证无自等待死锁)。
TEST(ProtocolNodeLifecycle, HandlerRequestCloseDoesNotSelfDeadlock) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNodeConfig config;
  config.handler = [](const Message&, HandlerContext& ctx) -> Status {
    return ctx.RequestClose();  // 消费者 fiber 内发起关闭。
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  // 节点应自行收敛到 Closed;外部 WaitClosed 完成(不挂起)证实不自锁。
  EXPECT_TRUE(node.WaitClosed());
}

// RT_LIFECYCLE_005:handler 捕获 node& 直接调 node.Close() → 重入自锁防护只发起、不自等,
// 节点正常收敛。
TEST(ProtocolNodeLifecycle, HandlerNodeCloseDoesNotSelfDeadlock) {
  auto fake_owner = std::make_unique<FakeCoroTransport>();
  FakeCoroTransport* fake = fake_owner.get();
  ProtocolNode* node_ptr = nullptr;
  ProtocolNodeConfig config;
  config.handler = [&node_ptr](const Message&, HandlerContext&) -> Status {
    return node_ptr->Close();  // 消费者 fiber 内直接 Close(重入)。
  };
  ProtocolNode node(std::move(fake_owner), std::make_unique<SystemCodec>(),
                    std::move(config));
  node_ptr = &node;
  ASSERT_TRUE(node.Start());

  fake->Inject(MakeBusinessDatagram(FrameType::kState, 1, 0x0001));
  EXPECT_TRUE(node.WaitClosed());  // 收敛不挂起 = 不自锁。
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
