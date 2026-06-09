#include "transport/framing/LengthFieldFramer.hpp"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameResult;
using transport::LengthFieldFramer;
using transport::LengthFieldFramerConfig;

namespace {

// header_size=8, length_offset=4, length_size=4, big-endian, body 长度=length 值
LengthFieldFramerConfig BeConfig() {
  LengthFieldFramerConfig c;
  c.header_size = 8;
  c.length_offset = 4;
  c.length_size = 4;
  c.big_endian = true;
  c.length_includes_header = false;
  c.max_frame_size = 1024;
  return c;
}

// 构造一帧：8 字节 header（offset4..7 存 body_len, BE）+ body_len 字节 body
std::vector<uint8_t> BuildFrame(uint32_t body_len, uint8_t fill = 0xAB) {
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
  buf[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
  buf[6] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
  buf[7] = static_cast<uint8_t>(body_len & 0xFF);
  buf.insert(buf.end(), body_len, fill);
  return buf;
}

}  // namespace

TEST(LengthFieldFramer, NeedMoreWhenLessThanHeader) {
  LengthFieldFramer f(BeConfig());
  std::vector<uint8_t> buf(5, 0x00);
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_FALSE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 0u);
}

TEST(LengthFieldFramer, ExtractsFullFrameBigEndian) {
  LengthFieldFramer f(BeConfig());
  auto buf = BuildFrame(3);  // total = 8 + 3 = 11
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 11u);
}

TEST(LengthFieldFramer, NeedMoreWhenBodyIncomplete) {
  LengthFieldFramer f(BeConfig());
  auto buf = BuildFrame(10);
  buf.resize(8 + 4);  // 只到了一部分 body
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_FALSE(r.value.has_frame);
}

TEST(LengthFieldFramer, LittleEndianLengthField) {
  auto c = BeConfig();
  c.big_endian = false;
  LengthFieldFramer f(c);
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = 0x02;  // LE: body_len = 2
  buf.insert(buf.end(), 2, 0xCD);
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 10u);
}

TEST(LengthFieldFramer, LengthIncludesHeader) {
  auto c = BeConfig();
  c.length_includes_header = true;
  LengthFieldFramer f(c);
  std::vector<uint8_t> buf(8, 0x00);
  buf[7] = 11;  // 总帧长 = 11（含 header）
  buf.insert(buf.end(), 3, 0xEE);
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 11u);
}

TEST(LengthFieldFramer, ErrorWhenExceedsMaxFrameSize) {
  auto c = BeConfig();
  c.max_frame_size = 16;
  LengthFieldFramer f(c);
  auto buf = BuildFrame(100);  // total 108 > 16
  auto r = f.TryExtract(buf.data(), buf.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);  // 以 "frame:" 开头
}

TEST(LengthFieldFramer, ValidateConfigRejectsBadLengthSize) {
  auto c = BeConfig();
  c.length_size = 3;
  EXPECT_FALSE(static_cast<bool>(LengthFieldFramer::ValidateConfig(c)));
}

TEST(LengthFieldFramer, ValidateConfigRejectsLengthBeyondHeader) {
  auto c = BeConfig();
  c.length_offset = 6;
  c.length_size = 4;  // 6 + 4 > 8
  EXPECT_FALSE(static_cast<bool>(LengthFieldFramer::ValidateConfig(c)));
}

TEST(LengthFieldFramer, ValidateConfigAcceptsGoodConfig) {
  EXPECT_TRUE(static_cast<bool>(LengthFieldFramer::ValidateConfig(BeConfig())));
}
