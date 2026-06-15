#include "transport/codec/DatagramCodec.hpp"

#include <gtest/gtest.h>

using transport::DatagramCodec;
using transport::Message;

TEST(DatagramCodec, EncodePassthrough) {
  DatagramCodec c;
  Message m; m.payload = {7, 8, 9};
  auto r = c.Encode(m);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value, (std::vector<uint8_t>{7, 8, 9}));
}

TEST(DatagramCodec, DecodeWholeChunkAsOneMessage) {
  DatagramCodec c;
  std::vector<uint8_t> dg = {1, 2, 3, 4};
  auto r = c.Decode(dg.data(), dg.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 1u);
  EXPECT_EQ(r.value[0].payload, dg);
  EXPECT_EQ(r.value[0].kind, transport::MessageKind::kOneway);
}

TEST(DatagramCodec, DecodeEmptyYieldsNoMessage) {
  DatagramCodec c;
  auto r = c.Decode(nullptr, 0);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.empty());
}
