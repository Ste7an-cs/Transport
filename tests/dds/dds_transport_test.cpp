#include "transport/dds/DdsTransport.hpp"
#include "transport/dds/FakeDdsProvider.hpp"

#include <vector>

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::FakeDdsProvider;
using transport::Result;

namespace {
struct Pair {
  std::shared_ptr<FakeDdsProvider::Bus> bus = std::make_shared<FakeDdsProvider::Bus>();
  std::shared_ptr<DdsTransport> Make(DdsConfig cfg) {
    return std::make_shared<DdsTransport>(cfg, std::make_unique<FakeDdsProvider>(bus));
  }
};
DdsConfig Cfg(std::string def = "") { DdsConfig c; c.domain_id = 0; c.default_topic = std::move(def); return c; }
}  // namespace

TEST(DdsTransport, PublishSubscribeDelivery) {
  Pair p;
  auto tx = p.Make(Cfg()), rx = p.Make(Cfg());
  std::vector<uint8_t> got; std::string from;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f) {
    if (r) { got = r.value; from = f; }
  });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("cmd")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({4, 5, 6}, Endpoint::Topic("cmd"))));
  EXPECT_EQ(got, (std::vector<uint8_t>{4, 5, 6}));
  EXPECT_EQ(from, "cmd");
  tx->Close(); rx->Close();
}

TEST(DdsTransport, DefaultTopic) {
  Pair p;
  auto tx = p.Make(Cfg("telemetry")), rx = p.Make(Cfg("telemetry"));
  std::vector<uint8_t> got;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&) { if (r) got = r.value; });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("telemetry")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({9})));  // 无 endpoint → default_topic
  EXPECT_EQ(got, (std::vector<uint8_t>{9}));
  tx->Close(); rx->Close();
}

TEST(DdsTransport, NetEndpointRejected) {
  Pair p;
  auto tx = p.Make(Cfg("d"));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  auto st = tx->Send({1}, Endpoint::Net("127.0.0.1", 9000));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  tx->Close();
}

TEST(DdsTransport, NoDefaultTopicFails) {
  Pair p;
  auto tx = p.Make(Cfg(""));  // 空 default_topic
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  auto st = tx->Send({1});
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  tx->Close();
}

TEST(DdsTransport, MultiTopicRouting) {
  Pair p;
  auto tx = p.Make(Cfg()), rx = p.Make(Cfg());
  std::vector<std::pair<std::string, std::vector<uint8_t>>> seen;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f) {
    if (r) seen.emplace_back(f, r.value);
  });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("a")));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("b")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({1}, Endpoint::Topic("a"))));
  ASSERT_TRUE(static_cast<bool>(tx->Send({2}, Endpoint::Topic("b"))));
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0].first, "a"); EXPECT_EQ(seen[0].second, (std::vector<uint8_t>{1}));
  EXPECT_EQ(seen[1].first, "b"); EXPECT_EQ(seen[1].second, (std::vector<uint8_t>{2}));
  tx->Close(); rx->Close();
}

TEST(DdsTransport, UnknownProviderFailsOpen) {
  DdsConfig cfg; cfg.provider = "no-such";
  auto t = std::make_shared<DdsTransport>(cfg);  // 不注入 → 走 registry
  auto st = t->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(DdsTransport, DefaultFakeProviderViaRegistry) {
  // 不注入 provider → Open 经 registry 建 "fake",接入按 domain 的静态总线。
  // 用独立 domain 避免与别的测试串扰。
  DdsConfig cfg; cfg.provider = "fake"; cfg.domain_id = 4242; cfg.default_topic = "x";
  auto tx = std::make_shared<DdsTransport>(cfg);
  auto rx = std::make_shared<DdsTransport>(cfg);
  std::vector<uint8_t> got;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&) { if (r) got = r.value; });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("x")));
  ASSERT_TRUE(static_cast<bool>(tx->Send({7})));
  EXPECT_EQ(got, (std::vector<uint8_t>{7}));
  tx->Close(); rx->Close();
}
