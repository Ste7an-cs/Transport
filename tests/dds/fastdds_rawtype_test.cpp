#include "../../src/dds/FastDdsRawType.hpp"

#include <cstdint>
#include <cstring>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/RawMessage.hpp"

using transport::FastDdsRawType;
using transport::RawMessage;
using SerializedPayload = eprosima::fastrtps::rtps::SerializedPayload_t;

TEST(FastDdsRawType, GoldenBytes) {
  FastDdsRawType type;
  RawMessage msg;
  msg.request_id = "ab";
  msg.reply_topic = "cd";
  msg.payload = {0xDE, 0xAD};

  SerializedPayload payload(64);
  ASSERT_TRUE(type.serialize(&msg, &payload));
  // [u16 LE id_len][id][u16 LE reply_len][reply][payload...]
  const uint8_t expected[] = {0x02, 0x00, 'a', 'b',
                              0x02, 0x00, 'c', 'd', 0xDE, 0xAD};
  ASSERT_EQ(payload.length, sizeof(expected));
  EXPECT_EQ(0, memcmp(payload.data, expected, sizeof(expected)));
}

TEST(FastDdsRawType, RoundtripAndEmptyFields) {
  FastDdsRawType type;
  RawMessage in;
  in.payload = {1, 2, 3};  // pub-sub 形态：id/reply 为空（各 2 字节 0 前缀）

  SerializedPayload payload(64);
  ASSERT_TRUE(type.serialize(&in, &payload));
  EXPECT_EQ(payload.length, 4u + 3u);

  RawMessage out;
  ASSERT_TRUE(type.deserialize(&payload, &out));
  EXPECT_TRUE(out.request_id.empty());
  EXPECT_TRUE(out.reply_topic.empty());
  EXPECT_EQ(out.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(FastDdsRawType, SizeProviderMatchesSerializedLength) {
  FastDdsRawType type;
  RawMessage msg;
  msg.request_id = "x";
  msg.payload = std::vector<uint8_t>(100, 0xAA);
  auto size_fn = type.getSerializedSizeProvider(&msg);
  SerializedPayload payload(256);
  ASSERT_TRUE(type.serialize(&msg, &payload));
  EXPECT_EQ(size_fn(), payload.length);
}

TEST(FastDdsRawType, DeserializeRejectsTruncated) {
  FastDdsRawType type;
  SerializedPayload payload(8);
  payload.data[0] = 0x05; payload.data[1] = 0x00;  // 声称 id_len=5
  payload.length = 3;                              // 实际只有 3 字节
  RawMessage out;
  EXPECT_FALSE(type.deserialize(&payload, &out));
}
