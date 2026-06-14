#include "transport/dds/DdsImpl.hpp"

#include <cstdint>
#include <map>
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

// 两个 topic 各注册不同 tag 的 codec:只有“按 topic 选 codec”时,
// 两条消息才能各自用对应 tag 解码成功。若错按 topic 选错 codec,
// Decode 会因 tag 不匹配返回 Fail("codec: bad tag"),Receive 即 !ok。
TEST(DdsTopicRouting, TwoTopicsSelectDistinctCodecs) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"a", "b"}));
  auto rx = Make(bus, PubSubCfg({"a", "b"}));
  ASSERT_TRUE(tx->Open().ok);
  ASSERT_TRUE(rx->Open().ok);
  // 两端都按 topic 注册不同 tag 的 codec:"a"→0xAA,"b"→0xBB。
  tx->SetCodec("a", std::make_shared<TagCodec>(0xAA));
  tx->SetCodec("b", std::make_shared<TagCodec>(0xBB));
  rx->SetCodec("a", std::make_shared<TagCodec>(0xAA));
  rx->SetCodec("b", std::make_shared<TagCodec>(0xBB));
  ASSERT_TRUE(rx->Subscribe("a").ok);
  ASSERT_TRUE(rx->Subscribe("b").ok);

  Message ma;
  ma.payload = Bytes{1, 1};
  ma.topic = "a";
  ASSERT_TRUE(tx->Send(ma).ok);  // 按 "a" 选 CodecA(0xAA) 编码
  Message mb;
  mb.payload = Bytes{2, 2, 2};
  mb.topic = "b";
  ASSERT_TRUE(tx->Send(mb).ok);  // 按 "b" 选 CodecB(0xBB) 编码

  // 顺序无关:收两条,按 topic 收集到 map 后断言各自 payload。
  // 任一条若被错按 topic 选错 codec,Decode 会失败导致 Receive !ok。
  std::map<std::string, Bytes> got;
  for (int i = 0; i < 2; ++i) {
    auto r = rx->Receive(1000);
    ASSERT_TRUE(r.ok);  // tag 不匹配会使 Decode 失败,这里即 !ok
    got[r.value.topic] = r.value.payload;
  }
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got["a"], (Bytes{1, 1}));      // CodecA 正确解码 "a"
  EXPECT_EQ(got["b"], (Bytes{2, 2, 2}));   // CodecB 正确解码 "b"
}
