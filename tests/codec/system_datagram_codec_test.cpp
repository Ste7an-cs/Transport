#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/codec/SystemCodec.hpp"

#include <cstdint>
#include <vector>
#include <gtest/gtest.h>

using transport::FrameType;
using transport::Message;
using transport::SystemCodec;
using transport::SystemDatagramCodec;

namespace {
// 确定性注入 CRC:body 字节和(便于字节级断言)。
uint16_t SumCrc(const uint8_t* b, std::size_t n) {
  uint16_t s = 0;
  for (std::size_t i = 0; i < n; ++i) s = static_cast<uint16_t>(s + b[i]);
  return s;
}
Message Cmd(uint8_t proto, uint8_t sess, uint16_t mid, std::vector<uint8_t> p) {
  Message m; m.frm_type = FrameType::kCommand; m.protocol_id = proto;
  m.session_id = sess; m.message_id = mid; m.payload = std::move(p); return m;
}
std::vector<Message> Decode(SystemDatagramCodec& c, const std::vector<uint8_t>& b) {
  auto r = c.Decode(b.data(), b.size());
  EXPECT_TRUE(static_cast<bool>(r));
  return r.value;
}
}  // namespace

TEST(SystemDatagramCodec, EncodeMatchesStreaming) {
  SystemCodec stream(SumCrc);
  SystemDatagramCodec dgram(SumCrc);
  Message m = Cmd(1, 7, 0x0102, {0xAA, 0xBB});
  auto a = stream.Encode(m); auto b = dgram.Encode(m);
  ASSERT_TRUE(static_cast<bool>(a)); ASSERT_TRUE(static_cast<bool>(b));
  EXPECT_EQ(a.value, b.value);                 // 编码两者字节一致
}

TEST(SystemDatagramCodec, DecodesSingleWholeFrame) {
  SystemDatagramCodec c(SumCrc);
  auto frame = SystemCodec(SumCrc).Encode(Cmd(2, 9, 0x0033, {1, 2, 3})).value;
  auto out = Decode(c, frame);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].frm_type, FrameType::kCommand);
  EXPECT_EQ(out[0].session_id, 9);
  EXPECT_EQ(out[0].message_id, 0x0033);
  EXPECT_EQ(out[0].payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(SystemDatagramCodec, DecodesMultipleFramesInOneDatagram) {
  SystemCodec enc(SumCrc);
  auto f1 = enc.Encode(Cmd(1, 1, 0x0001, {0xA})).value;
  auto f2 = enc.Encode(Cmd(1, 2, 0x0002, {0xB})).value;
  std::vector<uint8_t> both = f1; both.insert(both.end(), f2.begin(), f2.end());
  SystemDatagramCodec c(SumCrc);
  auto out = Decode(c, both);
  ASSERT_EQ(out.size(), 2u);
  EXPECT_EQ(out[0].message_id, 0x0001);
  EXPECT_EQ(out[1].message_id, 0x0002);
}

TEST(SystemDatagramCodec, StatelessNoCarryOverAcrossDatagrams) {
  SystemDatagramCodec c(SumCrc);
  auto whole = SystemCodec(SumCrc).Encode(Cmd(1, 1, 0x0001, {9, 9, 9})).value;
  std::vector<uint8_t> truncated(whole.begin(), whole.end() - 1);   // 缺最后一字节
  EXPECT_EQ(Decode(c, truncated).size(), 0u);                       // 半截 → 0,且不保留
  auto good = SystemCodec(SumCrc).Encode(Cmd(1, 2, 0x0002, {7})).value;
  auto out = Decode(c, good);                                       // 下一报文不被污染
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].message_id, 0x0002);
}

TEST(SystemDatagramCodec, CrcMismatchDropped) {
  SystemDatagramCodec c(SumCrc);
  auto frame = SystemCodec(SumCrc).Encode(Cmd(1, 1, 0x0001, {5})).value;
  frame[11] ^= 0xFF;                                                // 篡改 crc 字段
  EXPECT_EQ(Decode(c, frame).size(), 0u);
}

TEST(SystemDatagramCodec, EmptyYieldsNone) {
  SystemDatagramCodec c(SumCrc);
  EXPECT_EQ(Decode(c, {}).size(), 0u);
}
