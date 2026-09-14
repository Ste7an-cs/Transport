#include "transport/codec/LengthFieldCodec.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "message_test_util.hpp"
#include "transport/core/Error.hpp"

using testutil::Pay;
using testutil::PayloadIsViewOfFrame;
using testutil::ToVec;
using transport::LengthFieldCodec;
using transport::LengthFieldCodecConfig;
using transport::Message;
using transport::TransportErrc;
using transport::make_error_code;

namespace {
LengthFieldCodecConfig BeCfg() {
  LengthFieldCodecConfig c;
  c.header_size = 8; c.length_offset = 4; c.length_size = 4;
  c.big_endian = true; c.length_includes_header = false;
  return c;
}
std::vector<uint8_t> Frame(uint32_t n, uint8_t fill) {
  std::vector<uint8_t> f(8 + n, fill);
  f[0]=f[1]=f[2]=f[3]=0;
  f[4]=(n>>24)&0xFF; f[5]=(n>>16)&0xFF; f[6]=(n>>8)&0xFF; f[7]=n&0xFF;
  return f;
}
}  // namespace

TEST(LengthFieldCodec, EncodePassesThroughPayload) {
  LengthFieldCodec codec(BeCfg());
  Message m; m.payload = Pay({1, 2, 3});
  auto r = codec.Encode(m);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value(), (std::vector<uint8_t>{1, 2, 3}));
}

TEST(LengthFieldCodec, DecodeSingleFrame) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(3, 0xAB);
  auto r = codec.Decode(frame.data(), frame.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value().size(), 1u);
  EXPECT_EQ(ToVec(r.value()[0].payload), frame);
  EXPECT_EQ(r.value()[0].kind, transport::MessageKind::kOneway);
}

TEST(LengthFieldCodec, DecodeAcrossPartialFeeds) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(5, 0x11);
  auto r1 = codec.Decode(frame.data(), 6);
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value().empty());
  auto r2 = codec.Decode(frame.data() + 6, frame.size() - 6);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value().size(), 1u);
  EXPECT_EQ(ToVec(r2.value()[0].payload), frame);
}

TEST(LengthFieldCodec, DecodeGluedFrames) {
  LengthFieldCodec codec(BeCfg());
  auto a = Frame(2, 0x01), b = Frame(3, 0x02);
  std::vector<uint8_t> glued = a; glued.insert(glued.end(), b.begin(), b.end());
  auto r = codec.Decode(glued.data(), glued.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value().size(), 2u);
  EXPECT_EQ(ToVec(r.value()[0].payload), a);
  EXPECT_EQ(ToVec(r.value()[1].payload), b);
}

TEST(LengthFieldCodec, DecodeOversizeFails) {
  LengthFieldCodecConfig c = BeCfg(); c.max_frame_size = 8;
  LengthFieldCodec codec(c);
  auto frame = Frame(100, 0x44);
  auto r = codec.Decode(frame.data(), frame.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error(), make_error_code(TransportErrc::kFrame));
}

// ADR-0020 **D2**:本 codec **不剥帧头**——payload 历来就是整帧,故 `frame` 与 payload
// 内容相同、偏移为 0;但 payload 仍是 `frame` 的**视图**,不是第二份拷贝。
TEST(LengthFieldCodec, FrameIsTheWholeFrameAndPayloadIsAViewIntoIt) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(3, 0xAB);
  auto r = codec.Decode(frame.data(), frame.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value().size(), 1u);
  const transport::Message& m = r.value()[0];

  EXPECT_EQ(ToVec(m.frame), frame);
  EXPECT_TRUE(PayloadIsViewOfFrame(m));
  EXPECT_EQ(m.payload.constData(), m.frame.constData());
  EXPECT_EQ(m.payload.size(), m.frame.size());
}

// 粘包:两帧各有各的块,互不相干(滚动缓冲被压缩也不影响——frame 是拷出来的)。
TEST(LengthFieldCodec, GluedFramesEachGetTheirOwnBlock) {
  LengthFieldCodec codec(BeCfg());
  auto a = Frame(2, 0x01), b = Frame(3, 0x02);
  std::vector<uint8_t> glued = a; glued.insert(glued.end(), b.begin(), b.end());
  auto r = codec.Decode(glued.data(), glued.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value().size(), 2u);
  EXPECT_EQ(ToVec(r.value()[0].frame), a);
  EXPECT_EQ(ToVec(r.value()[1].frame), b);
  EXPECT_NE(r.value()[0].frame.constData(), r.value()[1].frame.constData());
  EXPECT_TRUE(PayloadIsViewOfFrame(r.value()[0]));
  EXPECT_TRUE(PayloadIsViewOfFrame(r.value()[1]));
}
