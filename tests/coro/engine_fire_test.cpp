#include <memory>
#include <variant>
#include <gtest/gtest.h>

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/comm/ProtocolPolicy.hpp"
#include "transport/coro/InteractionEngine.hpp"
#include "coro_test_util.hpp"

using transport::FrameTag;
using transport::FrameType;
using transport::Message;
using transport::ProtocolPolicy;
using transport::SystemDatagramCodec;
using transport::coro::InteractionEngine;
using testutil::FakeTransport;

// Fire 单向发:编码后交给传输,字节可被解回同一条消息。
TEST(CoroEngine, FireEncodesAndSends) {
  auto tp = std::make_shared<FakeTransport>();
  InteractionEngine eng(tp, std::make_unique<SystemDatagramCodec>(),
                        std::make_unique<ProtocolPolicy>(7));
  ASSERT_TRUE(static_cast<bool>(eng.Open()));

  Message m; m.message_id = 0x0055; m.payload = {0xAB};
  auto st = eng.Fire(m, static_cast<FrameTag>(FrameType::kCommand));
  ASSERT_TRUE(static_cast<bool>(st));
  ASSERT_EQ(tp->sent.size(), 1u);

  SystemDatagramCodec codec;
  auto back = codec.Decode(tp->sent[0].data(), tp->sent[0].size());
  ASSERT_TRUE(static_cast<bool>(back));
  ASSERT_EQ(back.value.size(), 1u);
  EXPECT_EQ(back.value[0].frm_type, FrameType::kCommand);   // SetTag 生效
  EXPECT_EQ(back.value[0].message_id, 0x0055);
  EXPECT_EQ(back.value[0].protocol_id, 0);                  // Fire 仅 SetTag,不调 NewCorrelation → protocol_id 保持默认 0
  EXPECT_EQ(back.value[0].payload, (std::vector<uint8_t>{0xAB}));
}
