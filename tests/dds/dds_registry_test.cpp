#include "transport/dds/DdsProviderRegistry.hpp"

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"

using transport::DdsProviderRegistry;
using transport::FakeDdsProvider;
using transport::IDdsProvider;

TEST(DdsRegistry, UnknownNameReturnsNull) {
  EXPECT_EQ(DdsProviderRegistry::Create("NoSuchProvider"), nullptr);
}

TEST(DdsRegistry, RegisterAndCreate) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  DdsProviderRegistry::RegisterProvider(
      "FakeForRegistryTest", [bus] { return std::make_unique<FakeDdsProvider>(bus); });
  auto p = DdsProviderRegistry::Create("FakeForRegistryTest");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->ProviderName(), "Fake");
}

TEST(DdsRegistry, FakeBusRoundtrip) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider a(bus), b(bus);

  std::vector<uint8_t> got;
  ASSERT_TRUE(static_cast<bool>(
      b.Subscribe("t", [&](transport::Result<transport::Message> m) {
        if (m) got = m.value.payload;
      })));
  ASSERT_TRUE(static_cast<bool>(a.Publish("t", {1, 2, 3})));
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));

  // req-resp 原语
  std::string seen_id, seen_reply_topic;
  std::vector<uint8_t> seen_req;
  ASSERT_TRUE(static_cast<bool>(b.ServeRequests(
      "calc_Request", [&](const std::vector<uint8_t>& p, const std::string& id,
                          const std::string& rt) {
        seen_req = p; seen_id = id; seen_reply_topic = rt;
      })));
  std::string reply_id;
  std::vector<uint8_t> reply_payload;
  ASSERT_TRUE(static_cast<bool>(a.SubscribeReplies(
      "calc_Reply", [&](const std::string& id, const std::vector<uint8_t>& p) {
        reply_id = id; reply_payload = p;
      })));
  ASSERT_TRUE(static_cast<bool>(
      a.SendRequest("calc_Request", "id-1", "calc_Reply", {9})));
  EXPECT_EQ(seen_req, (std::vector<uint8_t>{9}));
  EXPECT_EQ(seen_id, "id-1");
  EXPECT_EQ(seen_reply_topic, "calc_Reply");
  ASSERT_TRUE(static_cast<bool>(b.Reply("calc_Reply", "id-1", {8})));
  EXPECT_EQ(reply_id, "id-1");
  EXPECT_EQ(reply_payload, (std::vector<uint8_t>{8}));

  // Unsubscribe 生效
  got.clear();
  ASSERT_TRUE(static_cast<bool>(b.Unsubscribe("t")));
  ASSERT_TRUE(static_cast<bool>(a.Publish("t", {7})));
  EXPECT_TRUE(got.empty());
}
