#include "transport/core/TransportBase.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Message;
using transport::Result;
using transport::Status;
using transport::TransportBase;

namespace {

// 暴露 protected 辅助方法的可测试子类
class FakeTransport : public TransportBase {
 public:
  Status Open() override { return Status::Success(std::monostate{}); }
  void Close() override { CloseQueue(); }
  bool IsOpen() const override { return true; }
  Status Send(const std::vector<uint8_t>& data) override {
    auto enc = EncodeForSend(data);
    if (!enc) return Status::Fail(enc.error);
    last_sent = enc.value;
    return Status::Success(std::monostate{});
  }

  // 把 protected 辅助暴露给测试
  void TestDeliver(std::vector<uint8_t> frame, const std::string& source,
                   const std::string& topic) {
    DeliverFrame(std::move(frame), source, topic);
  }
  void TestNotifyDisconnect(const std::string& reason) {
    NotifyDisconnect(reason);
  }

  std::vector<uint8_t> last_sent;
};

// 在每个字节上 +1（Encode）/ -1（Decode）的可逆 codec
class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(
      const std::vector<uint8_t>& data) override {
    std::vector<uint8_t> out = data;
    for (auto& b : out) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
  Result<std::vector<uint8_t>> Decode(
      const std::vector<uint8_t>& data) override {
    std::vector<uint8_t> out = data;
    for (auto& b : out) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
};

// 始终失败的 Decode，用于验证 codec 错误投递
class FailDecodeCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(
      const std::vector<uint8_t>& data) override {
    return Result<std::vector<uint8_t>>::Success(data);
  }
  Result<std::vector<uint8_t>> Decode(
      const std::vector<uint8_t>&) override {
    return Result<std::vector<uint8_t>>::Fail("codec: bad frame");
  }
};

}  // namespace

TEST(TransportBase, PassthroughDeliversRawBytes) {
  FakeTransport t;
  t.TestDeliver({10, 20, 30}, "1.2.3.4:5", "topicA");
  auto r = t.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_EQ(r.value.source, "1.2.3.4:5");
  EXPECT_EQ(r.value.topic, "topicA");
  EXPECT_GT(r.value.timestamp, 0);
}

TEST(TransportBase, EncodeForSendIsIdentityWithoutCodec) {
  FakeTransport t;
  auto st = t.Send({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(st));
  EXPECT_EQ(t.last_sent, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportBase, CodecAppliedOnSend) {
  FakeTransport t;
  t.SetCodec(std::make_shared<ShiftCodec>());
  auto st = t.Send({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(st));
  EXPECT_EQ(t.last_sent, (std::vector<uint8_t>{2, 3, 4}));
}

TEST(TransportBase, CodecAppliedOnReceive) {
  FakeTransport t;
  t.SetCodec(std::make_shared<ShiftCodec>());
  t.TestDeliver({2, 3, 4}, "src", "");
  auto r = t.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportBase, DecodeFailureDeliversFail) {
  FakeTransport t;
  t.SetCodec(std::make_shared<FailDecodeCodec>());
  t.TestDeliver({9, 9}, "src", "");
  auto r = t.Receive(100);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("codec:", 0), 0u);
}

TEST(TransportBase, DisconnectCallbackInvoked) {
  FakeTransport t;
  std::string reason;
  t.OnDisconnect([&](const std::string& r) { reason = r; });
  t.TestNotifyDisconnect("conn: peer closed");
  EXPECT_EQ(reason, "conn: peer closed");
}
