#include "transport/ICodec.hpp"
#include "transport/IFramer.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace {

// 一个最简的恒等 codec，验证 ICodec 可被实现
class IdentityCodec : public transport::ICodec {
 public:
  transport::Result<std::vector<uint8_t>> Encode(
      const std::vector<uint8_t>& data) override {
    return transport::Result<std::vector<uint8_t>>::Success(data);
  }
  transport::Result<std::vector<uint8_t>> Decode(
      const std::vector<uint8_t>& data) override {
    return transport::Result<std::vector<uint8_t>>::Success(data);
  }
};

}  // namespace

TEST(Interfaces, MessageFieldsDefault) {
  transport::Message m;
  EXPECT_TRUE(m.payload.empty());
  EXPECT_TRUE(m.topic.empty());
  EXPECT_TRUE(m.source.empty());
  EXPECT_EQ(m.timestamp, 0);
}

TEST(Interfaces, CodecCanBeImplementedAndCalled) {
  std::unique_ptr<transport::ICodec> codec = std::make_unique<IdentityCodec>();
  std::vector<uint8_t> in{1, 2, 3};
  auto enc = codec->Encode(in);
  ASSERT_TRUE(static_cast<bool>(enc));
  EXPECT_EQ(enc.value, in);
  auto dec = codec->Decode(enc.value);
  ASSERT_TRUE(static_cast<bool>(dec));
  EXPECT_EQ(dec.value, in);
}

TEST(Interfaces, FrameResultDefaults) {
  transport::FrameResult fr;
  EXPECT_EQ(fr.consumed, 0u);
  EXPECT_FALSE(fr.has_frame);
}
