#include "transport/framing/FrameAssembler.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/framing/LengthFieldFramer.hpp"

using transport::FrameAssembler;
using transport::LengthFieldFramer;
using transport::LengthFieldFramerConfig;

namespace {

LengthFieldFramerConfig BeConfig() {
  LengthFieldFramerConfig c;
  c.header_size = 8;
  c.length_offset = 4;
  c.length_size = 4;
  c.big_endian = true;
  c.max_frame_size = 1024;
  return c;
}

std::vector<uint8_t> BuildFrame(uint32_t body_len, uint8_t fill) {
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
  buf[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
  buf[6] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
  buf[7] = static_cast<uint8_t>(body_len & 0xFF);
  buf.insert(buf.end(), body_len, fill);
  return buf;
}

}  // namespace

TEST(FrameAssembler, PassthroughWhenNoFramer) {
  FrameAssembler a(nullptr);
  std::vector<uint8_t> chunk{1, 2, 3, 4};
  auto r = a.Feed(chunk.data(), chunk.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 1u);
  EXPECT_EQ(r.value[0], chunk);
}

TEST(FrameAssembler, AssemblesAcrossPartialReads) {
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  FrameAssembler a(framer);
  auto frame = BuildFrame(5, 0xAB);  // 13 字节

  auto r1 = a.Feed(frame.data(), 6);
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value.empty());

  auto r2 = a.Feed(frame.data() + 6, frame.size() - 6);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0], frame);
}

TEST(FrameAssembler, SplitsTwoFramesInOneFeed) {
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  FrameAssembler a(framer);
  auto f1 = BuildFrame(2, 0x11);
  auto f2 = BuildFrame(3, 0x22);
  std::vector<uint8_t> both = f1;
  both.insert(both.end(), f2.begin(), f2.end());

  auto r = a.Feed(both.data(), both.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0], f1);
  EXPECT_EQ(r.value[1], f2);
}

TEST(FrameAssembler, ByteByByteDelivery) {
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  FrameAssembler a(framer);
  auto frame = BuildFrame(4, 0x33);

  size_t total = 0;
  for (size_t i = 0; i + 1 < frame.size(); ++i) {
    auto r = a.Feed(frame.data() + i, 1);
    ASSERT_TRUE(static_cast<bool>(r));
    total += r.value.size();
  }
  EXPECT_EQ(total, 0u);
  auto last = a.Feed(frame.data() + frame.size() - 1, 1);
  ASSERT_TRUE(static_cast<bool>(last));
  ASSERT_EQ(last.value.size(), 1u);
  EXPECT_EQ(last.value[0], frame);
}

TEST(FrameAssembler, PropagatesFramerError) {
  auto c = BeConfig();
  c.max_frame_size = 4;
  auto framer = std::make_shared<LengthFieldFramer>(c);
  FrameAssembler a(framer);
  auto frame = BuildFrame(100, 0x44);
  auto r = a.Feed(frame.data(), frame.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
