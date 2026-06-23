#include "transport/codec/SystemCodec.hpp"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameType;
using transport::Message;
using transport::SystemCodec;

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
}  // namespace

TEST(SystemCodec, EncodeProducesProtocolFrame) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(0x07, 0x09, 0x0201, {0xAA, 0xBB}));
  ASSERT_TRUE(static_cast<bool>(enc));
  const auto& f = enc.value;
  // 头 15 + body(2 message_id + 2 payload)= 19
  ASSERT_EQ(f.size(), 19u);
  EXPECT_EQ(f[0], 0xAA); EXPECT_EQ(f[1], 0xBB); EXPECT_EQ(f[2], 0xCC); EXPECT_EQ(f[3], 0xDD);
  EXPECT_EQ(f[4], static_cast<uint8_t>(FrameType::kCommand));
  EXPECT_EQ(f[5], 0x07);                          // protocol_id
  EXPECT_EQ(f[6], 0x09);                          // session_id
  EXPECT_EQ(f[7], 0); EXPECT_EQ(f[8], 0); EXPECT_EQ(f[9], 0); EXPECT_EQ(f[10], 0);  // reserve
  // body = [01 02][AA BB];sum = 0x01+0x02+0xAA+0xBB = 0x168
  const uint16_t crc = 0x0168;
  EXPECT_EQ(f[11], static_cast<uint8_t>(crc & 0xFF));        // crc LE
  EXPECT_EQ(f[12], static_cast<uint8_t>((crc >> 8) & 0xFF));
  EXPECT_EQ(f[13], 4); EXPECT_EQ(f[14], 0);                  // frm_len = 4 LE
  EXPECT_EQ(f[15], 0x01); EXPECT_EQ(f[16], 0x02);            // message_id 0x0201 LE
  EXPECT_EQ(f[17], 0xAA); EXPECT_EQ(f[18], 0xBB);            // payload
}

TEST(SystemCodec, EncodeDecodeRoundtrip) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));
  ASSERT_TRUE(static_cast<bool>(enc));
  auto dec = c.Decode(enc.value.data(), enc.value.size());
  ASSERT_TRUE(static_cast<bool>(dec));
  ASSERT_EQ(dec.value.size(), 1u);
  const Message& m = dec.value[0];
  EXPECT_EQ(m.frm_type, FrameType::kCommand);
  EXPECT_EQ(m.protocol_id, 3);
  EXPECT_EQ(m.session_id, 5);
  EXPECT_EQ(m.message_id, 0x1234);
  EXPECT_EQ(m.payload, (std::vector<uint8_t>{1, 2, 3, 4}));
}

TEST(SystemCodec, DecodeSplitAcrossReads) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(1, 2, 7, {9, 9, 9}));
  ASSERT_TRUE(static_cast<bool>(enc));
  const auto& f = enc.value;
  auto d1 = c.Decode(f.data(), 6);                    // 半包
  ASSERT_TRUE(static_cast<bool>(d1)); EXPECT_TRUE(d1.value.empty());
  auto d2 = c.Decode(f.data() + 6, f.size() - 6);     // 补齐
  ASSERT_TRUE(static_cast<bool>(d2)); ASSERT_EQ(d2.value.size(), 1u);
  EXPECT_EQ(d2.value[0].payload, (std::vector<uint8_t>{9, 9, 9}));
}

TEST(SystemCodec, DecodeMultipleFramesOneRead) {
  SystemCodec c(SumCrc);
  auto a = c.Encode(Cmd(1, 1, 1, {0xA})); auto b = c.Encode(Cmd(1, 2, 2, {0xB}));
  std::vector<uint8_t> both = a.value; both.insert(both.end(), b.value.begin(), b.value.end());
  auto dec = c.Decode(both.data(), both.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value.size(), 2u);
  EXPECT_EQ(dec.value[0].message_id, 1); EXPECT_EQ(dec.value[1].message_id, 2);
}

TEST(SystemCodec, ResyncOnBadHeadFlag) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(1, 2, 3, {7, 7}));
  std::vector<uint8_t> junk = {0x00, 0x11, 0xAA, 0xBB, 0x22};  // 含半个假同步头
  junk.insert(junk.end(), enc.value.begin(), enc.value.end());
  auto dec = c.Decode(junk.data(), junk.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value.size(), 1u);
  EXPECT_EQ(dec.value[0].payload, (std::vector<uint8_t>{7, 7}));
}

TEST(SystemCodec, ResyncOnCrcMismatch) {
  SystemCodec c(SumCrc);
  auto bad = c.Encode(Cmd(1, 2, 3, {5, 5})); bad.value[15] ^= 0xFF;  // 破坏 body → CRC 不符
  auto good = c.Encode(Cmd(1, 2, 4, {6, 6}));
  std::vector<uint8_t> s = bad.value; s.insert(s.end(), good.value.begin(), good.value.end());
  auto dec = c.Decode(s.data(), s.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value.size(), 1u);  // 坏帧跳过,好帧解出
  EXPECT_EQ(dec.value[0].message_id, 4);
}

TEST(SystemCodec, EncodeRejectsOversizePayload) {
  SystemCodec c(SumCrc);
  Message m = Cmd(1, 1, 1, std::vector<uint8_t>(65534, 0));  // 65534 + 2 > 65535
  auto enc = c.Encode(m);
  ASSERT_FALSE(static_cast<bool>(enc));
  EXPECT_EQ(enc.error.rfind("frame:", 0), 0u);
}
