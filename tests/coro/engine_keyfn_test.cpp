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
using transport::Key;
using transport::Message;
using transport::ProtocolPolicy;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::coro::InteractionEngine;
using testutil::FakeTransport;
using testutil::pumpFiberUntil;

// 自定义键:仅按 session_id 配对(应答 message_id 变了也能匹配)。
TEST(CoroEngineKeyFn, SessionOnlyMatchesWhenMessageIdChanges) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(1));
  eng.SetKeyFn([](const Message& m) -> Key { return Key(1, static_cast<char>(m.session_id)); });
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Result<Message> got = Result<Message>::Fail("unset");
  bool done = false;
  auto req = Coro::makeTask([&] {
    Message m; m.message_id = 0x42;
    got = eng.Request(m, static_cast<FrameTag>(FrameType::kCommand), 1000ms);
    done = true;
  });
  ASSERT_TRUE(pumpFiberUntil([&] { return !tp->sent.empty(); }));

  // 造应答:回显 session_id,但 message_id 改成 0x99(响应码),frm_type=RESULT。
  SystemDatagramCodec codec;
  auto cmd = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  Message reply = cmd.value[0];
  reply.message_id = 0x99;                        // 变了
  reply.frm_type = FrameType::kResult;
  reply.payload = {5};
  tp->inject(codec.Encode(reply).value);

  ASSERT_TRUE(pumpFiberUntil([&] { return done; }));   // 默认键会因 message_id 变而超时;自定义键匹配
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5}));
  (void)req;
}
