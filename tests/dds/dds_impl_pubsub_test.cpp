#include "transport/dds/DdsImpl.hpp"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"
#include "transport/Endpoint.hpp"
#include "transport/ICodec.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FakeDdsProvider;
using transport::ICodec;
using transport::Result;

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

class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto o = d; for (auto& b : o) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto o = d; for (auto& b : o) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
};

}  // namespace

TEST(DdsPubSub, SendGoesToDefaultTopic) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"t0"}));
  auto rx = Make(bus, PubSubCfg({"t0"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("t0")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2})));  // 默认 topic = topics[0]
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2}));
  EXPECT_EQ(r.value.topic, "t0");
  EXPECT_EQ(r.value.source, "t0");
}

TEST(DdsPubSub, SendToSpecificTopicAndMultiTopicRouting) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"a"}));
  auto rx = Make(bus, PubSubCfg({"a"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("a")));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("b")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1}, transport::Endpoint::Topic("a"))));
  ASSERT_TRUE(static_cast<bool>(tx->Send({2}, transport::Endpoint::Topic("b"))));
  auto r1 = rx->Receive(1000);
  auto r2 = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r1));
  ASSERT_TRUE(static_cast<bool>(r2));
  EXPECT_EQ(r1.value.topic, "a");
  EXPECT_EQ(r1.value.payload, (std::vector<uint8_t>{1}));
  EXPECT_EQ(r2.value.topic, "b");
  EXPECT_EQ(r2.value.payload, (std::vector<uint8_t>{2}));
}

TEST(DdsPubSub, UnsubscribeStopsDelivery) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"t"}));
  auto rx = Make(bus, PubSubCfg({"t"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("t")));
  ASSERT_TRUE(static_cast<bool>(rx->Unsubscribe("t")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({9})));
  auto r = rx->Receive(50);
  EXPECT_FALSE(static_cast<bool>(r));  // timeout:
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(DdsPubSub, CodecAppliedBothDirections) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"t"}));
  auto rx = Make(bus, PubSubCfg({"t"}));
  tx->SetCodec(std::make_shared<ShiftCodec>());
  rx->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("t")));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));  // Encode +1 → 总线上 {2,3,4}
  auto r = rx->Receive(1000);             // Decode -1 → {1,2,3}
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(DdsPubSub, ModeConstraintRejectsReqRespMethods) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto t = Make(bus, PubSubCfg({"t"}));
  ASSERT_TRUE(static_cast<bool>(t->Open()));
  auto st = t->SendRequest({1}, "t", [](Result<transport::Message>) {}, 1000);
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  auto st2 = t->OnRequest("t", [](const transport::Message&,
                                  transport::IDdsTransport::ReplyFn) {});
  EXPECT_FALSE(static_cast<bool>(st2));
  EXPECT_EQ(st2.error.rfind("config:", 0), 0u);
}

TEST(DdsPubSub, OpenValidations) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  // 空 topics
  auto a = Make(bus, PubSubCfg({}));
  auto st = a->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  // history_depth = 0
  auto cfg = PubSubCfg({"t"});
  cfg.qos.history_depth = 0;
  auto b = Make(bus, cfg);
  auto st2 = b->Open();
  EXPECT_FALSE(static_cast<bool>(st2));
  // 未注册 provider（不注入）
  DdsConfig c3 = PubSubCfg({"t"});
  c3.provider = "NoSuch";
  auto d = std::make_shared<DdsImpl>(c3);
  auto st3 = d->Open();
  EXPECT_FALSE(static_cast<bool>(st3));
  EXPECT_EQ(st3.error.rfind("config:", 0), 0u);
}
