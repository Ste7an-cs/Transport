#include "transport/dds/DdsImpl.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"
#include "transport/ICodec.hpp"
#include "transport/Message.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FakeDdsProvider;
using transport::ICodec;
using transport::Message;
using transport::Result;
using Bytes = std::vector<uint8_t>;

namespace {
DdsConfig PubSubCfg(std::vector<std::string> topics) {
  DdsConfig c;
  c.mode = DdsMode::kPubSub;
  c.topics = std::move(topics);
  return c;
}
std::shared_ptr<DdsImpl> Make(std::shared_ptr<FakeDdsProvider::Bus> bus,
                              DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FakeDdsProvider>(bus));
}
class TagCodec : public ICodec {
 public:
  explicit TagCodec(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o{tag_};
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};
}  // namespace

TEST(DdsTopicRouting, SendMessageEncodesByTopic) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"b"}));
  auto rx = Make(bus, PubSubCfg({"b"}));
  ASSERT_TRUE(tx->Open().ok);
  ASSERT_TRUE(rx->Open().ok);
  // tx 编码、rx 解码,两端都注册 "b"→CodecB。
  tx->SetCodec("b", std::make_shared<TagCodec>(0xBB));
  rx->SetCodec("b", std::make_shared<TagCodec>(0xBB));
  ASSERT_TRUE(rx->Subscribe("b").ok);

  Message m;
  m.payload = Bytes{4, 5};
  m.topic = "b";
  ASSERT_TRUE(tx->Send(m).ok);  // 发往原生 topic "b",按 "b" 选 codec 编码

  auto r = rx->Receive(1000);
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (Bytes{4, 5}));  // CodecB 解码还原,无前缀残留
}
