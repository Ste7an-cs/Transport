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
#include "transport/ProtocolNode.hpp"
#include "transport/TransportTypes.hpp"
#include "transport/codec/SystemCodec.hpp"
#include "coro_test_util.hpp"
#include "fake_coro_transport.hpp"

using namespace std::chrono_literals;
using testutil::FakeCoroTransport;
using testutil::pumpFiberUntil;
using transport::CorrelationKeyStrategy;
using transport::Datagram;
using transport::DefaultProtocolKeyStrategy;
using transport::FrameType;
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
