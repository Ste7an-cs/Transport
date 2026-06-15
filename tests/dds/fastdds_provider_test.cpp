#include "../../src/dds/FastDdsProvider.hpp"

#include <future>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/DdsImpl.hpp"
#include "transport/dds/DdsProviderRegistry.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FastDdsProvider;
using transport::IDdsTransport;
using transport::Message;
using transport::Result;

namespace {

constexpr int kTestDomain = 42;  // 避免与环境中其它 DDS 互扰

DdsConfig Cfg(DdsMode mode, std::vector<std::string> topics) {
  DdsConfig c;
  c.mode = mode;
  c.topics = std::move(topics);
  c.domain_id = kTestDomain;
  // TransientLocal：晚匹配的 reader 仍可取到 depth 内历史样本，
  // 吸收 DDS 发现期（~百 ms 级），避免 sleep-flaky。
  c.qos.durability = transport::DdsQos::Durability::kTransientLocal;
  return c;
}

std::shared_ptr<DdsImpl> MakeReal(DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FastDdsProvider>());
}

}  // namespace

TEST(FastDdsIntegration, PubSubRoundtrip) {
  auto rx = MakeReal(Cfg(DdsMode::kPubSub, {"itopic"}));
  auto opened = rx->Open();
  if (!opened) GTEST_SKIP() << "FastDDS participant unavailable: " << opened.error;
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("itopic")));

  auto tx = MakeReal(Cfg(DdsMode::kPubSub, {"itopic"}));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));

  auto r = rx->Receive(3000);  // 容纳发现期
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(r.value.topic, "itopic");
  tx->Close();
  rx->Close();
}

TEST(FastDdsIntegration, ReqRespRoundtrip) {
  auto server = MakeReal(Cfg(DdsMode::kReqResp, {"icalc"}));
  auto opened = server->Open();
  if (!opened) GTEST_SKIP() << "FastDDS participant unavailable";
  ASSERT_TRUE(static_cast<bool>(server->OnRequest(
      "icalc", [](const Message& req, IDdsTransport::ReplyFn reply) {
        auto out = req.payload;
        for (auto& b : out) b = static_cast<uint8_t>(b + 1);
        (void)reply(out);
      })));

  auto client = MakeReal(Cfg(DdsMode::kReqResp, {"icalc"}));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  std::promise<Result<Message>> prom;
  auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(client->SendRequest(
      {10, 20}, "icalc",
      [&](Result<Message> r) { prom.set_value(std::move(r)); },
      /*timeout_ms=*/5000)));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{11, 21}));
  client->Close();
  server->Close();
}

TEST(FastDdsIntegration, RegistryProvidesFastDds) {
  transport::RegisterFastDdsProvider();  // 显式注册（静态库防裁剪）
  auto p = transport::DdsProviderRegistry::Create("FastDDS");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->ProviderName(), "FastDDS");
}
