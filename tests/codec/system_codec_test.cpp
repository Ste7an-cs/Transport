#include "transport/codec/SystemCodec.hpp"

#include <gtest/gtest.h>

using transport::Message;
using transport::MessageKind;
using transport::SystemCodec;

namespace {
Message Make(MessageKind k, std::string corr, std::string topic,
             std::vector<uint8_t> payload) {
  Message m; m.kind = k; m.correlation_id = std::move(corr);
  m.topic = std::move(topic); m.payload = std::move(payload);
  return m;
}
}  // namespace

TEST(SystemCodec, RoundtripAllFields) {
  SystemCodec enc, dec;
  auto m = Make(MessageKind::kRequest, "req-1", "calc", {9, 8, 7});
  auto bytes = enc.Encode(m);
  ASSERT_TRUE(static_cast<bool>(bytes));
  auto msgs = dec.Decode(bytes.value.data(), bytes.value.size());
  ASSERT_TRUE(static_cast<bool>(msgs));
  ASSERT_EQ(msgs.value.size(), 1u);
  EXPECT_EQ(msgs.value[0].kind, MessageKind::kRequest);
  EXPECT_EQ(msgs.value[0].correlation_id, "req-1");
  EXPECT_EQ(msgs.value[0].topic, "calc");
  EXPECT_EQ(msgs.value[0].payload, (std::vector<uint8_t>{9, 8, 7}));
}

TEST(SystemCodec, RoundtripEmptyCorrelationAndTopic) {
  SystemCodec enc, dec;
  auto m = Make(MessageKind::kOneway, "", "", {1});
  auto bytes = enc.Encode(m);
  ASSERT_TRUE(static_cast<bool>(bytes));
  auto msgs = dec.Decode(bytes.value.data(), bytes.value.size());
  ASSERT_TRUE(static_cast<bool>(msgs));
  ASSERT_EQ(msgs.value.size(), 1u);
  EXPECT_EQ(msgs.value[0].kind, MessageKind::kOneway);
  EXPECT_TRUE(msgs.value[0].correlation_id.empty());
  EXPECT_TRUE(msgs.value[0].topic.empty());
  EXPECT_EQ(msgs.value[0].payload, (std::vector<uint8_t>{1}));
}

TEST(SystemCodec, DecodeAcrossPartialFeeds) {
  SystemCodec enc, dec;
  auto bytes = enc.Encode(Make(MessageKind::kReply, "r", "t", {5, 6}));
  ASSERT_TRUE(static_cast<bool>(bytes));
  const auto& b = bytes.value;
  auto r1 = dec.Decode(b.data(), 3);
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value.empty());
  auto r2 = dec.Decode(b.data() + 3, b.size() - 3);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0].correlation_id, "r");
}

TEST(SystemCodec, DecodeGluedFrames) {
  SystemCodec enc, dec;
  auto a = enc.Encode(Make(MessageKind::kFeedback, "c", "t", {1}));
  auto bb = enc.Encode(Make(MessageKind::kReply, "c", "t", {2}));
  ASSERT_TRUE(static_cast<bool>(a)); ASSERT_TRUE(static_cast<bool>(bb));
  std::vector<uint8_t> glued = a.value;
  glued.insert(glued.end(), bb.value.begin(), bb.value.end());
  auto r = dec.Decode(glued.data(), glued.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0].kind, MessageKind::kFeedback);
  EXPECT_EQ(r.value[1].kind, MessageKind::kReply);
}

TEST(SystemCodec, DecodeBadKindByteFails) {
  SystemCodec dec;
  std::vector<uint8_t> bad = {0, 0, 0, 1, 9};
  auto r = dec.Decode(bad.data(), bad.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("codec:", 0), 0u);
}

TEST(SystemCodec, DecodeInnerLengthExceedsFrameFails) {
  SystemCodec dec;
  std::vector<uint8_t> bad = {0, 0, 0, 4, 0, 0xFF, 0xFF, 0x00};
  auto r = dec.Decode(bad.data(), bad.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
