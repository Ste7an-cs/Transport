#include <chrono>
#include <memory>
#include <string>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"
#include "transport/coro/InteractionEngine.hpp"
#include "task/fibertask.h"
#include "coro_test_util.hpp"

using namespace std::chrono_literals;
using transport::FrameTag;
using transport::FrameType;
using transport::Message;
using transport::ProtocolPolicy;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::coro::InteractionEngine;
using testutil::FakeTransport;
using testutil::pumpFiberUntil;

namespace {
// 把 fake 已发出的命令帧解回,回显成一条 RESULT(终结)帧字节。
std::vector<uint8_t> MakeResultReply(const std::vector<uint8_t>& cmd_bytes,
                                     std::vector<uint8_t> payload) {
  SystemDatagramCodec codec;
  auto cmd = codec.Decode(cmd_bytes.data(), cmd_bytes.size());
  Message reply = cmd.value[0];               // 回显 session_id/message_id/protocol_id
  reply.frm_type = FrameType::kResult;        // 终结
  reply.payload = std::move(payload);
  return codec.Encode(reply).value;
}
const FrameTag kCmd = static_cast<FrameTag>(FrameType::kCommand);
}  // namespace

TEST(CoroEngineRequest, Roundtrip) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42; m.payload = {1, 2, 3};
    got = eng.Request(m, kCmd, 1000ms);
    done = true;
  });

  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));   // 等命令发出
  tp->inject(MakeResultReply(tp->sent[0], {9, 9}));                 // 注入终结应答
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{9, 9}));
  (void)req;
}

TEST(CoroEngineRequest, TimesOut) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, kCmd, 150ms);   // 不注入应答
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }, 2000));
  EXPECT_FALSE(static_cast<bool>(got));
  EXPECT_NE(got.error.find("timeout:"), std::string::npos);
  (void)req;
}

TEST(CoroEngineRequest, CloseFailsPendingWithConn) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, kCmd, 5000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));
  eng.Close();                                    // 关引擎 → 唤醒挂起
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  EXPECT_FALSE(static_cast<bool>(got));
  EXPECT_NE(got.error.find("conn:"), std::string::npos);
  (void)req;
}

TEST(CoroEngineRequest, IntermediateResponseDropped) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, kCmd, 2000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));

  // 先注入一条中间 RESPONSE(非终结)→ 不应终结请求。
  SystemDatagramCodec codec;
  auto cmd = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  Message resp = cmd.value[0]; resp.frm_type = FrameType::kResponse; resp.payload = {7};
  tp->inject(codec.Encode(resp).value);
  EXPECT_FALSE(pumpFiberUntil([&] { return done; }, 100));   // 仍未完成

  tp->inject(MakeResultReply(tp->sent[0], {8}));             // 再注入终结 RESULT
  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{8}));
  (void)req;
}
