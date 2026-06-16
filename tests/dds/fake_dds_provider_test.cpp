#include "transport/dds/FakeDdsProvider.hpp"

#include <vector>

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::FakeDdsProvider;

namespace {
DdsConfig Cfg(int domain) { DdsConfig c; c.domain_id = domain; return c; }
}  // namespace

TEST(FakeDdsProvider, SharedBusDeliversAcrossProviders) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx(bus), rx(bus);
  ASSERT_TRUE(static_cast<bool>(tx.Init(Cfg(0))));
  ASSERT_TRUE(static_cast<bool>(rx.Init(Cfg(0))));

  std::vector<uint8_t> got;
  ASSERT_TRUE(static_cast<bool>(
      rx.Subscribe("t", [&](const std::vector<uint8_t>& b) { got = b; })));
  ASSERT_TRUE(static_cast<bool>(tx.Publish("t", {1, 2, 3})));
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(FakeDdsProvider, UnsubscribeStopsDelivery) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx(bus), rx(bus);
  (void)tx.Init(Cfg(0)); (void)rx.Init(Cfg(0));
  int count = 0;
  (void)rx.Subscribe("t", [&](const std::vector<uint8_t>&) { ++count; });
  (void)tx.Publish("t", {1});
  EXPECT_EQ(count, 1);
  (void)rx.Unsubscribe("t");
  (void)tx.Publish("t", {2});
  EXPECT_EQ(count, 1);  // 不再投递
}

TEST(FakeDdsProvider, StaticBusIsolatesByDomain) {
  // 默认构造(无注入 Bus)→ Init 接入按 domain 的静态总线。
  FakeDdsProvider tx, rx_same, rx_other;
  (void)tx.Init(Cfg(7)); (void)rx_same.Init(Cfg(7)); (void)rx_other.Init(Cfg(8));
  int same = 0, other = 0;
  (void)rx_same.Subscribe("t", [&](const std::vector<uint8_t>&) { ++same; });
  (void)rx_other.Subscribe("t", [&](const std::vector<uint8_t>&) { ++other; });
  (void)tx.Publish("t", {1});
  EXPECT_EQ(same, 1);
  EXPECT_EQ(other, 0);  // 不同 domain 不可见
}
