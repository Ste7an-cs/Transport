#include "transport/ITransport.hpp"

#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::ICodec;
using transport::ITransport;
using transport::Message;
using transport::Result;
using transport::Status;
using Bytes = std::vector<uint8_t>;

namespace {
// 最小 ITransport:只记录最后一次 Send(payload) 的入参,其余接收侧空实现。
// 不覆写 Send(Message)/SetCodec(topic,codec) → 走 ITransport 基类默认。
class RecordingTransport : public ITransport {
 public:
  // 保留基类 Send(Message)/SetCodec(topic,codec) 重载,避免被同名 override 隐藏
  // —— 与各具体传输一致的 using 约定。
  using ITransport::Send;
  using ITransport::SetCodec;

  Status Open() override { return Status::Success({}); }
  void Close() override {}
  bool IsOpen() const override { return true; }
  Status Send(const std::vector<uint8_t>& data) override {
    last_payload = data;
    ++send_calls;
    return Status::Success({});
  }
  Result<Message> Receive(uint32_t) override {
    return Result<Message>::Fail("io: not supported");
  }
  void OnReceive(ReceiveCallback) override {}
  std::future<Result<Message>> AsyncReceive() override {
    std::promise<Result<Message>> p;
    p.set_value(Result<Message>::Fail("io: not supported"));
    return p.get_future();
  }
  void OnDisconnect(DisconnectCallback) override {}
  void SetCodec(std::shared_ptr<ICodec>) override {}

  Bytes last_payload;
  int send_calls = 0;
};
}  // namespace

TEST(SendMessageBaseDefault, EmptyTopicDegradesToSendPayload) {
  RecordingTransport t;
  Message m;
  m.payload = Bytes{1, 2, 3};  // topic 空
  auto st = t.Send(m);
  EXPECT_TRUE(st.ok);
  EXPECT_EQ(t.send_calls, 1);
  EXPECT_EQ(t.last_payload, (Bytes{1, 2, 3}));
}

TEST(SendMessageBaseDefault, NonEmptyTopicNotSupported) {
  RecordingTransport t;
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = t.Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "io: topic routing not supported");
  EXPECT_EQ(t.send_calls, 0);
}

TEST(SendMessageBaseDefault, SetCodecTopicIsNoopOnBase) {
  RecordingTransport t;
  // 基类 no-op:不抛、不崩,纯粹忽略。
  t.SetCodec("x", nullptr);
  SUCCEED();
}
