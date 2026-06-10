#include "transport/core/TransportCore.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Result;
using transport::TransportCore;

namespace {

// 在每个字节上 +1（Encode）/ -1（Decode）的可逆 codec
class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto out = d;
    for (auto& b : out) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto out = d;
    for (auto& b : out) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
};

// 始终失败的 Decode
class FailDecodeCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    return Result<std::vector<uint8_t>>::Success(d);
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>&) override {
    return Result<std::vector<uint8_t>>::Fail("codec: bad frame");
  }
};

}  // namespace

TEST(TransportCore, PassthroughDeliversRawBytes) {
  TransportCore core;
  core.DeliverFrame({10, 20, 30}, "1.2.3.4:5", "topicA");
  auto r = core.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_EQ(r.value.source, "1.2.3.4:5");
  EXPECT_EQ(r.value.topic, "topicA");
  EXPECT_GT(r.value.timestamp, 0);
}

TEST(TransportCore, EncodeForSendIsIdentityWithoutCodec) {
  TransportCore core;
  auto enc = core.EncodeForSend({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(enc));
  EXPECT_EQ(enc.value, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportCore, CodecAppliedOnSend) {
  TransportCore core;
  core.SetCodec(std::make_shared<ShiftCodec>());
  auto enc = core.EncodeForSend({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(enc));
  EXPECT_EQ(enc.value, (std::vector<uint8_t>{2, 3, 4}));
}

TEST(TransportCore, CodecAppliedOnReceive) {
  TransportCore core;
  core.SetCodec(std::make_shared<ShiftCodec>());
  core.DeliverFrame({2, 3, 4}, "src", "");
  auto r = core.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportCore, DecodeFailureDeliversFail) {
  TransportCore core;
  core.SetCodec(std::make_shared<FailDecodeCodec>());
  core.DeliverFrame({9, 9}, "src", "");
  auto r = core.Receive(100);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("codec:", 0), 0u);
}

TEST(TransportCore, DisconnectCallbackInvoked) {
  TransportCore core;
  std::string reason;
  core.OnDisconnect([&](const std::string& r) { reason = r; });
  core.NotifyDisconnect("conn: peer closed");
  EXPECT_EQ(reason, "conn: peer closed");
}
