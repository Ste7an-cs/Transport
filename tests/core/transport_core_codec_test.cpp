#include "transport/core/TransportCore.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Message;
using transport::Result;
using transport::TransportCore;
using Bytes = std::vector<uint8_t>;

namespace {
// 在 body 前加一个 tag 字节;Decode 校验并剥除。用于证明「按 topic 选对了 codec」。
class TagCodec : public ICodec {
 public:
  explicit TagCodec(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o;
    o.reserve(d.size() + 1);
    o.push_back(tag_);
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

TEST(TransportCoreCodec, EncodeForSendSelectsByTopic) {
  TransportCore core;
  core.SetCodec("a", std::make_shared<TagCodec>(0xAA));
  core.SetCodec("b", std::make_shared<TagCodec>(0xBB));
  auto ea = core.EncodeForSend(Bytes{1}, "a");
  ASSERT_TRUE(ea.ok);
  EXPECT_EQ(ea.value, (Bytes{0xAA, 1}));
  auto eb = core.EncodeForSend(Bytes{1}, "b");
  ASSERT_TRUE(eb.ok);
  EXPECT_EQ(eb.value, (Bytes{0xBB, 1}));
}

TEST(TransportCoreCodec, UnregisteredTopicFallsBackToDefault) {
  TransportCore core;
  core.SetCodec(std::make_shared<TagCodec>(0xDD));  // 默认
  auto e = core.EncodeForSend(Bytes{5}, "unknown");
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value, (Bytes{0xDD, 5}));
}

TEST(TransportCoreCodec, NoCodecPassthrough) {
  TransportCore core;
  auto e = core.EncodeForSend(Bytes{5, 6}, "x");
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value, (Bytes{5, 6}));  // 无 codec → 透传
}

TEST(TransportCoreCodec, DeliverFrameDecodesByTopic) {
  TransportCore core;
  core.SetCodec("a", std::make_shared<TagCodec>(0xAA));
  core.SetCodec("b", std::make_shared<TagCodec>(0xBB));
  core.DeliverFrame(Bytes{0xBB, 7, 8}, "src", "b");
  auto m = core.Receive(100);
  ASSERT_TRUE(m.ok);
  EXPECT_EQ(m.value.topic, "b");
  EXPECT_EQ(m.value.payload, (Bytes{7, 8}));  // 用 CodecB 解码剥除 tag
}

TEST(TransportCoreCodec, LegacySingleCodecUnchanged) {
  TransportCore core;
  core.SetCodec(std::make_shared<TagCodec>(0xCC));
  auto e = core.EncodeForSend(Bytes{1, 2});  // 旧无 topic 重载
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value, (Bytes{0xCC, 1, 2}));
}
