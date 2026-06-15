#include "transport/codec/LengthFieldCodec.hpp"

#include <gtest/gtest.h>

using transport::LengthFieldCodec;
using transport::LengthFieldCodecConfig;
using transport::Message;

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
  Message m; m.payload = {1, 2, 3};
  auto r = codec.Encode(m);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(LengthFieldCodec, DecodeSingleFrame) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(3, 0xAB);
  auto r = codec.Decode(frame.data(), frame.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 1u);
  EXPECT_EQ(r.value[0].payload, frame);
  EXPECT_EQ(r.value[0].kind, transport::MessageKind::kOneway);
}

TEST(LengthFieldCodec, DecodeAcrossPartialFeeds) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(5, 0x11);
  auto r1 = codec.Decode(frame.data(), 6);
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value.empty());
  auto r2 = codec.Decode(frame.data() + 6, frame.size() - 6);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0].payload, frame);
}

TEST(LengthFieldCodec, DecodeGluedFrames) {
  LengthFieldCodec codec(BeCfg());
  auto a = Frame(2, 0x01), b = Frame(3, 0x02);
  std::vector<uint8_t> glued = a; glued.insert(glued.end(), b.begin(), b.end());
  auto r = codec.Decode(glued.data(), glued.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0].payload, a);
  EXPECT_EQ(r.value[1].payload, b);
}

TEST(LengthFieldCodec, DecodeOversizeFails) {
  LengthFieldCodecConfig c = BeCfg(); c.max_frame_size = 8;
  LengthFieldCodec codec(c);
  auto frame = Frame(100, 0x44);
  auto r = codec.Decode(frame.data(), frame.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
