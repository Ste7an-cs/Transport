#include "transport/codec/DatagramCodec.hpp"

#include <vector>

#include <gtest/gtest.h>

#include "message_test_util.hpp"

using testutil::Pay;
using testutil::PayloadIsViewOfFrame;
using testutil::ToVec;
using transport::DatagramCodec;
using transport::Message;

TEST(DatagramCodec, EncodePassthrough) {
  DatagramCodec c;
  Message m; m.payload = Pay({7, 8, 9});
  auto r = c.Encode(m);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value(), (std::vector<uint8_t>{7, 8, 9}));
}

TEST(DatagramCodec, DecodeWholeChunkAsOneMessage) {
  DatagramCodec c;
  std::vector<uint8_t> dg = {1, 2, 3, 4};
  auto r = c.Decode(dg.data(), dg.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value().size(), 1u);
  EXPECT_EQ(ToVec(r.value()[0].payload), dg);
  EXPECT_EQ(r.value()[0].kind, transport::MessageKind::kOneway);
}

TEST(DatagramCodec, DecodeEmptyYieldsNoMessage) {
  DatagramCodec c;
  auto r = c.Decode(nullptr, 0);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value().empty());
}

// ADR-0020 **D2**:本 codec 无帧头,故 `frame` 是整段报文、payload 是它**全长**的视图
// ——内容相同,但仍是视图(地址落在 frame 的块里),不是第二份拷贝。
TEST(DatagramCodec, FrameIsTheWholeDatagramAndPayloadIsAViewIntoIt) {
  DatagramCodec c;
  std::vector<uint8_t> dg = {0x11, 0x22, 0x33, 0x44};
  auto r = c.Decode(dg.data(), dg.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value().size(), 1u);
  const Message& m = r.value()[0];

  EXPECT_EQ(ToVec(m.frame), dg);
  EXPECT_TRUE(PayloadIsViewOfFrame(m));
  EXPECT_EQ(m.payload.constData(), m.frame.constData());  // 偏移 0、全长。
  EXPECT_EQ(m.payload.size(), m.frame.size());
}
