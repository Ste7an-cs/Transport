#include "transport/codec/SystemCodec.hpp"

#include <cstdint>
#include <utility>
#include <vector>

#include <QByteArray>
#include <gtest/gtest.h>

#include "message_test_util.hpp"
#include "transport/core/Error.hpp"

using testutil::Pay;
using testutil::PayloadIsViewOfFrame;
using testutil::ToVec;
using transport::FrameType;
using transport::Message;
using transport::SystemCodec;
using transport::TransportErrc;
using transport::make_error_code;

namespace {
// 确定性注入 CRC:body 字节和(便于字节级断言)。
uint16_t SumCrc(const uint8_t* b, std::size_t n) {
  uint16_t s = 0;
  for (std::size_t i = 0; i < n; ++i) s = static_cast<uint16_t>(s + b[i]);
  return s;
}
Message Cmd(uint8_t proto, uint8_t sess, uint16_t mid, std::vector<uint8_t> p) {
  Message m; m.frm_type = FrameType::kCommand; m.protocol_id = proto;
  m.session_id = sess; m.message_id = mid; m.payload = Pay(p); return m;
}
}  // namespace

TEST(SystemCodec, EncodeProducesProtocolFrame) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(0x07, 0x09, 0x0201, {0xAA, 0xBB}));
  ASSERT_TRUE(static_cast<bool>(enc));
  const auto& f = enc.value();
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
  auto dec = c.Decode(enc.value().data(), enc.value().size());
  ASSERT_TRUE(static_cast<bool>(dec));
  ASSERT_EQ(dec.value().size(), 1u);
  const Message& m = dec.value()[0];
  EXPECT_EQ(m.frm_type, FrameType::kCommand);
  EXPECT_EQ(m.protocol_id, 3);
  EXPECT_EQ(m.session_id, 5);
  EXPECT_EQ(m.message_id, 0x1234);
  EXPECT_EQ(ToVec(m.payload), (std::vector<uint8_t>{1, 2, 3, 4}));
}

TEST(SystemCodec, DecodeSplitAcrossReads) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(1, 2, 7, {9, 9, 9}));
  ASSERT_TRUE(static_cast<bool>(enc));
  const auto& f = enc.value();
  auto d1 = c.Decode(f.data(), 6);                    // 半包
  ASSERT_TRUE(static_cast<bool>(d1)); EXPECT_TRUE(d1.value().empty());
  auto d2 = c.Decode(f.data() + 6, f.size() - 6);     // 补齐
  ASSERT_TRUE(static_cast<bool>(d2)); ASSERT_EQ(d2.value().size(), 1u);
  EXPECT_EQ(ToVec(d2.value()[0].payload), (std::vector<uint8_t>{9, 9, 9}));
}

TEST(SystemCodec, DecodeMultipleFramesOneRead) {
  SystemCodec c(SumCrc);
  auto a = c.Encode(Cmd(1, 1, 1, {0xA})); auto b = c.Encode(Cmd(1, 2, 2, {0xB}));
  std::vector<uint8_t> both = a.value(); both.insert(both.end(), b.value().begin(), b.value().end());
  auto dec = c.Decode(both.data(), both.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value().size(), 2u);
  EXPECT_EQ(dec.value()[0].message_id, 1); EXPECT_EQ(dec.value()[1].message_id, 2);
}

TEST(SystemCodec, ResyncOnBadHeadFlag) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(1, 2, 3, {7, 7}));
  std::vector<uint8_t> junk = {0x00, 0x11, 0xAA, 0xBB, 0x22};  // 含半个假同步头
  junk.insert(junk.end(), enc.value().begin(), enc.value().end());
  auto dec = c.Decode(junk.data(), junk.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value().size(), 1u);
  EXPECT_EQ(ToVec(dec.value()[0].payload), (std::vector<uint8_t>{7, 7}));
}

TEST(SystemCodec, ResyncOnCrcMismatch) {
  SystemCodec c(SumCrc);
  auto bad = c.Encode(Cmd(1, 2, 3, {5, 5})); bad.value()[15] ^= 0xFF;  // 破坏 body → CRC 不符
  auto good = c.Encode(Cmd(1, 2, 4, {6, 6}));
  std::vector<uint8_t> s = bad.value(); s.insert(s.end(), good.value().begin(), good.value().end());
  auto dec = c.Decode(s.data(), s.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value().size(), 1u);  // 坏帧跳过,好帧解出
  EXPECT_EQ(dec.value()[0].message_id, 4);
}

TEST(SystemCodec, EncodeRejectsOversizePayload) {
  SystemCodec c(SumCrc);
  Message m = Cmd(1, 1, 1, std::vector<uint8_t>(65534, 0));  // 65534 + 2 > 65535
  auto enc = c.Encode(m);
  ASSERT_FALSE(static_cast<bool>(enc));
  EXPECT_EQ(enc.error(), make_error_code(TransportErrc::kFrame));
}

// ── ADR-0020:frame / payload 视图的四条承重断言 ──────────────────────────────
//
// 本组是这次重构**是否真的成立**的证据。前三条断言的都是**地址**,不是内容——内容相等的
// 拷贝实现也能让"内容"断言变绿,证不了零拷贝。

/// 解码一条帧,返回 `{整帧字节, 解出的 Message}`。
namespace {
std::pair<std::vector<uint8_t>, Message> DecodeOne(const Message& src) {
  SystemCodec codec(SumCrc);
  auto enc = codec.Encode(src);
  EXPECT_TRUE(static_cast<bool>(enc));
  std::vector<uint8_t> wire = enc.value();
  auto dec = codec.Decode(wire.data(), wire.size());
  EXPECT_TRUE(static_cast<bool>(dec));
  EXPECT_EQ(dec.value().size(), 1u);
  return {std::move(wire), dec.value().front()};
}
}  // namespace

// ⭐ ① **零拷贝确实成立**(D2):`payload.constData()` 落在 `frame` 的数据块区间内 ⇒
//    它是**视图**而非拷贝。
TEST(SystemCodec, DecodedPayloadIsAZeroCopyViewIntoTheFrame) {
  auto [wire, msg] = DecodeOne(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));

  ASSERT_FALSE(msg.frame.isEmpty());
  EXPECT_TRUE(PayloadIsViewOfFrame(msg))
      << "payload 必须指进 frame 的数据块——不在区间内即说明它被拷了一份";
  // 偏移恰是"帧头 15 + message_id 2":线缆布局说什么,视图就从哪儿起。
  EXPECT_EQ(msg.payload.constData() - msg.frame.constData(), 17);
  EXPECT_EQ(msg.payload.size(), 4);
}

// ⭐ ④ **frame 就是线缆上那一整帧**(帧头 → payload 末),对本 codec 可逐字节核对。
TEST(SystemCodec, DecodedFrameIsTheWholeWireFrameByteForByte) {
  auto [wire, msg] = DecodeOne(Cmd(0x07, 0x09, 0x0201, {0xAA, 0xBB}));

  EXPECT_EQ(ToVec(msg.frame), wire) << "frame 应与 Encode 出来的整帧逐字节相同";
  EXPECT_EQ(msg.frame.size(), 19);  // 头 15 + body(2 + 2)

  // 帧头里的字节在 payload 里看不到,但在 frame 里能看到——这正是本 ADR 买到的东西。
  EXPECT_EQ(static_cast<uint8_t>(msg.frame[0]), 0xAA);   // head_flag
  EXPECT_EQ(static_cast<uint8_t>(msg.frame[11]), 0x68);  // crc LE 低字节
  EXPECT_EQ(static_cast<uint8_t>(msg.frame[12]), 0x01);
}

// 前导垃圾(resync)之后,frame 仍**恰好**是那一帧:不含被跳过的垃圾字节。
TEST(SystemCodec, FrameExcludesResyncGarbage) {
  SystemCodec c(SumCrc);
  auto enc = SystemCodec(SumCrc).Encode(Cmd(1, 2, 3, {7, 7}));
  ASSERT_TRUE(static_cast<bool>(enc));
  std::vector<uint8_t> junk = {0x00, 0x11, 0xAA, 0xBB, 0x22};
  junk.insert(junk.end(), enc.value().begin(), enc.value().end());

  auto dec = c.Decode(junk.data(), junk.size());
  ASSERT_TRUE(static_cast<bool>(dec));
  ASSERT_EQ(dec.value().size(), 1u);
  EXPECT_EQ(ToVec(dec.value()[0].frame), enc.value());
  EXPECT_TRUE(PayloadIsViewOfFrame(dec.value()[0]));
}

// ⭐ ② **拷贝之后视图仍然有效**(D2 的拷贝安全性论证):`Dispatcher` 给每个订阅者各发一份
//    副本走的正是这条路。`QByteArray` 拷贝只是引用计数加一,**数据块地址不变**。
TEST(SystemCodec, PayloadViewSurvivesMessageCopy) {
  auto [wire, msg] = DecodeOne(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));
  const char* original_block = msg.frame.constData();

  Message copy = msg;  // ← Dispatcher 投递给订阅者时做的就是这一步。

  EXPECT_EQ(copy.frame.constData(), original_block) << "拷贝不该换块";
  EXPECT_TRUE(PayloadIsViewOfFrame(copy));
  EXPECT_EQ(ToVec(copy.payload), (std::vector<uint8_t>{1, 2, 3, 4}));

  // 原件析构之后副本照样可读——块被副本继续引用着。
  msg = Message{};
  EXPECT_EQ(ToVec(copy.payload), (std::vector<uint8_t>{1, 2, 3, 4}));
  EXPECT_TRUE(PayloadIsViewOfFrame(copy));
}

// ⭐ ③ **移动之后同样有效**:`std::vector<Message>` 扩容、入队出队都只是移动,d 指针转手、
//    块地址不变。
TEST(SystemCodec, PayloadViewSurvivesMoveAndVectorReallocation) {
  auto [wire, msg] = DecodeOne(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));
  const char* original_block = msg.frame.constData();

  std::vector<Message> queue;
  queue.reserve(1);
  queue.push_back(std::move(msg));
  // 反复 push 强制扩容,每次扩容把已有元素**移动**到新缓冲。
  for (int i = 0; i < 64; ++i) {
    queue.push_back(DecodeOne(Cmd(1, 1, 1, {0xEE})).second);
  }

  EXPECT_EQ(queue.front().frame.constData(), original_block) << "移动不该换块";
  EXPECT_TRUE(PayloadIsViewOfFrame(queue.front()));
  EXPECT_EQ(ToVec(queue.front().payload), (std::vector<uint8_t>{1, 2, 3, 4}));

  // 出队(再一次移动)之后仍然成立。
  Message popped = std::move(queue.front());
  queue.clear();
  EXPECT_EQ(popped.frame.constData(), original_block);
  EXPECT_EQ(ToVec(popped.payload), (std::vector<uint8_t>{1, 2, 3, 4}));
}

// ⭐ ⑤ **`OwnedPayload()` 确为深拷贝**:内容相同,但地址**不在** frame 区间内(D8)。
TEST(SystemCodec, OwnedPayloadEscapesTheFrame) {
  auto [wire, msg] = DecodeOne(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));
  ASSERT_TRUE(PayloadIsViewOfFrame(msg));

  const QByteArray owned = msg.OwnedPayload();
  const char* begin = msg.frame.constData();
  const char* end = begin + msg.frame.size();
  EXPECT_TRUE(owned.constData() < begin || owned.constData() >= end)
      << "OwnedPayload() 必须落在 frame 之外";
  EXPECT_EQ(ToVec(owned), ToVec(msg.payload));

  // 让整条 Message 消失——`owned` 是能活得比它久的那一个,这就是它唯一的用途。
  msg = Message{};
  EXPECT_EQ(ToVec(owned), (std::vector<uint8_t>{1, 2, 3, 4}));
}

// **`Encode` 完全忽略 `frame`**(D7):把一条收到的 Message 改几个字段再发出,出站帧由
// **当前字段**生成,不是把 `frame` 原样吐回去。
TEST(SystemCodec, EncodeIgnoresTheFrameAndRebuildsFromFields) {
  auto [wire, msg] = DecodeOne(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));
  ASSERT_FALSE(msg.frame.isEmpty());

  msg.message_id = 0x4321;  // 改一个字段:若"frame 非空就原样重发",这一改就不生效。
  SystemCodec codec(SumCrc);
  auto again = codec.Encode(msg);
  ASSERT_TRUE(static_cast<bool>(again));

  EXPECT_NE(again.value(), wire);
  EXPECT_EQ(again.value()[15], 0x21);  // message_id LE —— 新值生效了。
  EXPECT_EQ(again.value()[16], 0x43);
}
