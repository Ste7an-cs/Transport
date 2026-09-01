#include "transport/io/dds/FakeDdsProvider.hpp"

#include <memory>
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
  // 静态 domain 总线是进程级共享状态,且 provider 析构不自动退订:不复位则上一轮
  // 的订阅会残留到下一轮(--gtest_repeat 下确定性失败)。用 RAII 保证即便断言中途
  // 失败也退订复位。
  struct ShutdownAll {
    FakeDdsProvider* ps[3];
    ~ShutdownAll() { for (auto* p : ps) p->Shutdown(); }
  } cleanup{{&tx, &rx_same, &rx_other}};

  (void)tx.Init(Cfg(7)); (void)rx_same.Init(Cfg(7)); (void)rx_other.Init(Cfg(8));
  int same = 0, other = 0;
  (void)rx_same.Subscribe("t", [&](const std::vector<uint8_t>&) { ++same; });
  (void)rx_other.Subscribe("t", [&](const std::vector<uint8_t>&) { ++other; });
  (void)tx.Publish("t", {1});
  EXPECT_EQ(same, 1);
  EXPECT_EQ(other, 0);  // 不同 domain 不可见
}

// ADR-0013 D13 新增的 MatchedCount():Fake 拿"同一条总线上别人的订阅数"当对端数。
TEST(FakeDdsProvider, MatchedCountZeroWithoutBusOrPeers) {
  FakeDdsProvider lonely;
  // 未 Init:没接上总线,谈不上匹配。
  EXPECT_EQ(lonely.MatchedCount().matched, 0);
  EXPECT_EQ(lonely.MatchedCount().alive, 0);

  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider solo(bus);
  ASSERT_TRUE(static_cast<bool>(solo.Init(Cfg(0))));
  EXPECT_EQ(solo.MatchedCount().matched, 0);  // 总线上只有自己

  (void)solo.Subscribe("t", [](const std::vector<uint8_t>&) {});
  EXPECT_EQ(solo.MatchedCount().matched, 0);  // 自己的订阅不算对端
}

TEST(FakeDdsProvider, MatchedCountCountsPeerSubscriptions) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  FakeDdsProvider tx(bus), rx1(bus), rx2(bus);
  ASSERT_TRUE(static_cast<bool>(tx.Init(Cfg(0))));
  ASSERT_TRUE(static_cast<bool>(rx1.Init(Cfg(0))));
  ASSERT_TRUE(static_cast<bool>(rx2.Init(Cfg(0))));

  (void)rx1.Subscribe("t", [](const std::vector<uint8_t>&) {});
  EXPECT_EQ(tx.MatchedCount().matched, 1);
  EXPECT_EQ(tx.MatchedCount().alive, 1);  // Fake 无独立判活:匹配即存活

  (void)rx2.Subscribe("t", [](const std::vector<uint8_t>&) {});
  EXPECT_EQ(tx.MatchedCount().matched, 2);
  // rx1 眼里只有 rx2 那一个对端(自己的不算)。
  EXPECT_EQ(rx1.MatchedCount().matched, 1);

  rx2.Shutdown();
  EXPECT_EQ(tx.MatchedCount().matched, 1);
  (void)rx1.Unsubscribe("t");
  EXPECT_EQ(tx.MatchedCount().matched, 0);
}
