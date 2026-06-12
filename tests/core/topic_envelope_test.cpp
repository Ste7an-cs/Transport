#include "transport/core/TopicEnvelope.hpp"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameStream;
using transport::PackTopic;
using transport::TopicFrame;
using transport::TopicFrameAssembler;
using transport::UnpackTopic;
using Bytes = std::vector<uint8_t>;

TEST(TopicEnvelope, PackUnpackRoundTrip) {
  Bytes body{1, 2, 3, 4};
  auto packed = PackTopic("cmd", body);
  ASSERT_EQ(packed.size(), 2u + 3u + 4u);
  EXPECT_EQ(packed[0], 0u);
  EXPECT_EQ(packed[1], 3u);
  auto r = UnpackTopic(packed.data(), packed.size());
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "cmd");
  EXPECT_EQ(r.value.body, body);
}

TEST(TopicEnvelope, EmptyTopicAllowed) {
  Bytes body{9, 9};
  auto packed = PackTopic("", body);
  EXPECT_EQ(packed[0], 0u);
  EXPECT_EQ(packed[1], 0u);
  auto r = UnpackTopic(packed.data(), packed.size());
  ASSERT_TRUE(r.ok);
  EXPECT_TRUE(r.value.topic.empty());
  EXPECT_EQ(r.value.body, body);
}

TEST(TopicEnvelope, UnpackTooShortFails) {
  Bytes one{0};
  auto r = UnpackTopic(one.data(), one.size());
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}

TEST(TopicEnvelope, UnpackTopicLenExceedsFails) {
  Bytes bad{0, 5, 'a', 'b'};  // 声称 topic 5 字节,实际只有 2
  auto r = UnpackTopic(bad.data(), bad.size());
  EXPECT_FALSE(r.ok);
}

TEST(TopicEnvelope, StreamAssemblerSplitAcrossFeeds) {
  auto f1 = FrameStream("a", Bytes{1, 1});
  auto f2 = FrameStream("bb", Bytes{2, 2, 2});
  Bytes wire;
  wire.insert(wire.end(), f1.begin(), f1.end());
  wire.insert(wire.end(), f2.begin(), f2.end());

  TopicFrameAssembler asm_;
  size_t mid = f1.size() + 1;
  auto r1 = asm_.Feed(wire.data(), mid);
  ASSERT_TRUE(r1.ok);
  ASSERT_EQ(r1.value.size(), 1u);
  EXPECT_EQ(r1.value[0].topic, "a");
  EXPECT_EQ(r1.value[0].body, (Bytes{1, 1}));

  auto r2 = asm_.Feed(wire.data() + mid, wire.size() - mid);
  ASSERT_TRUE(r2.ok);
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0].topic, "bb");
  EXPECT_EQ(r2.value[0].body, (Bytes{2, 2, 2}));
}

TEST(TopicEnvelope, StreamAssemblerMultipleInOneFeed) {
  auto f1 = FrameStream("x", Bytes{7});
  auto f2 = FrameStream("y", Bytes{8});
  Bytes wire(f1);
  wire.insert(wire.end(), f2.begin(), f2.end());
  TopicFrameAssembler asm_;
  auto r = asm_.Feed(wire.data(), wire.size());
  ASSERT_TRUE(r.ok);
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0].topic, "x");
  EXPECT_EQ(r.value[1].topic, "y");
}

TEST(TopicEnvelope, StreamAssemblerOverflowFails) {
  Bytes bad{0xFF, 0xFF, 0xFF, 0xFF};
  TopicFrameAssembler asm_;
  auto r = asm_.Feed(bad.data(), bad.size());
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
