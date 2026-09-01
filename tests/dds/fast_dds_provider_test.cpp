// fast_dds_provider_test.cpp — 真实 provider(Fast DDS 3.x)的用例。
// 只在 TRANSPORT_HAS_FASTDDS 时编入(CMakeLists);Fast DDS 缺席时整份不参与构建。

#include "io/dds/FastDdsProvider.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <fastdds/dds/core/ReturnCode.hpp>
#include <gtest/gtest.h>

using namespace std::chrono_literals;
using transport::DdsConfig;
using transport::FastDdsProvider;
using Clock = std::chrono::steady_clock;

namespace {

int MsSince(Clock::time_point t) {
  return static_cast<int>(
      std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t).count());
}

DdsConfig Cfg(int domain) {
  DdsConfig c;
  c.domain_id = domain;
  c.qos.history_depth = 10;
  c.qos.max_blocking_time = 200ms;
  c.qos.liveliness_lease = 1000ms;
  return c;
}

/// 等到 provider 至少匹配上一个对端(发现窗口约 240ms,ADR-0013 D9);超时返 false。
bool WaitMatched(const FastDdsProvider& p, std::chrono::milliseconds budget = 5s) {
  auto t0 = Clock::now();
  while (MsSince(t0) < budget.count()) {
    if (p.MatchedCount().matched > 0) return true;
    std::this_thread::sleep_for(10ms);
  }
  return false;
}

/// Init 失败也要能收拾干净:RAII 保证 --gtest_repeat 下不留 participant。
struct Pair {
  FastDdsProvider tx, rx;
  ~Pair() { tx.Shutdown(); rx.Shutdown(); }
};

}  // namespace

// 3.x 的 TopicDataType 重写(引用 + DataRepresentationId_t + calculate_serialized_size)
// 是否真的把字节原样送到了对端。
TEST(FastDdsProvider, PublishSubscribeRoundTrip) {
  Pair p;
  ASSERT_TRUE(static_cast<bool>(p.tx.Init(Cfg(88))));
  ASSERT_TRUE(static_cast<bool>(p.rx.Init(Cfg(88))));

  std::atomic<int> hits{0};
  std::vector<uint8_t> got;
  ASSERT_TRUE(static_cast<bool>(
      p.rx.Subscribe("round-trip", [&](const std::vector<uint8_t>& b) {
        got = b;
        ++hits;
      })));

  // 头一次 Publish 顺带把 writer 建出来,之后才谈得上"匹配"。
  ASSERT_TRUE(static_cast<bool>(p.tx.Publish("round-trip", {0})));
  ASSERT_TRUE(WaitMatched(p.tx));

  const std::vector<uint8_t> payload{0x00, 0x01, 0x7f, 0x80, 0xff, 0x42};
  ASSERT_TRUE(static_cast<bool>(p.tx.Publish("round-trip", payload)));

  auto t0 = Clock::now();
  while (got != payload && MsSince(t0) < 5000) std::this_thread::sleep_for(10ms);
  EXPECT_EQ(got, payload);
  EXPECT_GT(hits.load(), 0);
}

// ⚠ 回归闸:3.x 的 write() 返回 ReturnCode_t(int32_t,RETCODE_OK == 0),旧代码写的是
// `if (!writer->write(&copy)) return kIo;` —— **成功恰好返 0**,取反为真,于是一次成功的
// 发布会被报成 kIo(且零告警照常编译)。本例钉死"发布成功 → Result 为真"。
TEST(FastDdsProvider, SuccessfulPublishReportsSuccess) {
  EXPECT_EQ(eprosima::fastdds::dds::RETCODE_OK, 0)
      << "RETCODE_OK 不再是 0 的话,Publish 里的返回值判定要重看";

  Pair p;
  ASSERT_TRUE(static_cast<bool>(p.tx.Init(Cfg(88))));
  // 无对端也一样:RELIABLE 下没有 reader 就无人可等,write 直接成功。
  EXPECT_TRUE(static_cast<bool>(p.tx.Publish("no-peer", {1, 2, 3})));
}

TEST(FastDdsProvider, MatchedCountZeroBeforeInitAndAfterShutdown) {
  FastDdsProvider p;
  EXPECT_EQ(p.MatchedCount().matched, 0);
  EXPECT_EQ(p.MatchedCount().alive, 0);

  ASSERT_TRUE(static_cast<bool>(p.Init(Cfg(89))));
  p.Shutdown();
  EXPECT_EQ(p.MatchedCount().matched, 0);
  EXPECT_EQ(p.MatchedCount().alive, 0);
}

// D9 的判活素材:两侧都要能报出 matched > 0 且 alive > 0。
TEST(FastDdsProvider, MatchedCountSeesPeerOnBothSides) {
  Pair p;
  ASSERT_TRUE(static_cast<bool>(p.tx.Init(Cfg(89))));
  ASSERT_TRUE(static_cast<bool>(p.rx.Init(Cfg(89))));
  ASSERT_TRUE(static_cast<bool>(
      p.rx.Subscribe("matched", [](const std::vector<uint8_t>&) {})));
  ASSERT_TRUE(static_cast<bool>(p.tx.Publish("matched", {1})));

  ASSERT_TRUE(WaitMatched(p.tx));
  ASSERT_TRUE(WaitMatched(p.rx));

  const auto mtx = p.tx.MatchedCount();
  const auto mrx = p.rx.MatchedCount();
  EXPECT_GT(mtx.matched, 0);
  EXPECT_GT(mtx.alive, 0);   // writer 侧:匹配上的 reader 一律计为存活
  EXPECT_GT(mrx.matched, 0);
  // reader 侧的 alive 来自 LivelinessChangedStatus,须等一次 liveliness 断言到达。
  auto t0 = Clock::now();
  while (p.rx.MatchedCount().alive == 0 && MsSince(t0) < 5000)
    std::this_thread::sleep_for(10ms);
  EXPECT_GT(p.rx.MatchedCount().alive, 0);
}

TEST(FastDdsProvider, PublishAfterShutdownIsInvalidState) {
  FastDdsProvider p;
  ASSERT_TRUE(static_cast<bool>(p.Init(Cfg(89))));
  ASSERT_TRUE(static_cast<bool>(p.Publish("late", {1})));
  p.Shutdown();

  auto r = p.Publish("late", {1});
  ASSERT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error(), make_error_code(transport::TransportErrc::kInvalidState));
}

// 🔬 ADR-0013「明确接受的代价」7 要求的前置补测:**`Shutdown()` 打不断在途的阻塞
// `Publish`**——它只能等对方跑完。本例把 `Publish` 按住一段可控的时长,在其间调
// `Shutdown()`,核对 ①`Publish` 照跑满、没有被截断,②`Shutdown()` 迟至 `Publish`
// 收工之后才返回。#203 的关闭路径据此设计:`Close()` 落在一次阻塞写上时,
// `WaitClosed()` 就得等那一次写自己结束。
//
// 按住 `Publish` 的手法:Fast DDS 默认开 **intraprocess 交付**,同进程内订阅方的
// `on_data_available` 是在**发布线程上同步**跑的(本文件实测),故 sink 里睡多久,
// `Publish` 就卡多久。**不假定第一发就一定走这条路**——发布线程循环发,直到主线程
// 观察到"有一次 Publish 已经卡了 kBlockedThresholdMs 还没回来",那才是真的在途阻塞。
TEST(FastDdsProvider, ShutdownWaitsOutInFlightPublishAndDoesNotInterruptIt) {
  constexpr int kSinkStallMs = 600;
  constexpr int kBlockedThresholdMs = 250;

  Pair p;
  ASSERT_TRUE(static_cast<bool>(p.tx.Init(Cfg(90))));
  ASSERT_TRUE(static_cast<bool>(p.rx.Init(Cfg(90))));

  std::atomic<bool> stalling{false};
  std::atomic<int> sink_hits{0};
  ASSERT_TRUE(static_cast<bool>(
      p.rx.Subscribe("stall", [&](const std::vector<uint8_t>&) {
        ++sink_hits;
        if (!stalling.load()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(kSinkStallMs));
      })));

  // 预热:先确认这条 topic 的交付确实通了(不停滞),再开始按住。
  ASSERT_TRUE(static_cast<bool>(p.tx.Publish("stall", {0})));
  ASSERT_TRUE(WaitMatched(p.tx));
  auto t_warm = Clock::now();
  while (sink_hits.load() == 0 && MsSince(t_warm) < 5000) {
    (void)p.tx.Publish("stall", {0});
    std::this_thread::sleep_for(20ms);
  }
  ASSERT_GT(sink_hits.load(), 0) << "样本压根没送到 sink,后面测不出东西";

  // publish_start_ns 非 0 = 此刻正卡在一次 Publish 里(值为进入时刻)。
  std::atomic<int64_t> publish_start_ns{0};
  std::atomic<int> blocked_publish_ms{-1};
  std::atomic<bool> blocked_publish_ok{false};
  std::atomic<int64_t> blocked_publish_end_ns{0};
  std::atomic<bool> stop{false};
  stalling = true;

  const auto Now = [] {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch())
        .count();
  };

  std::thread writer([&] {
    while (!stop.load()) {
      const int64_t begin = Now();
      publish_start_ns = begin;
      auto r = p.tx.Publish("stall", {1, 2, 3});
      const int64_t end = Now();
      publish_start_ns = 0;
      const int ms = static_cast<int>((end - begin) / 1000000);
      if (ms >= kBlockedThresholdMs) {
        blocked_publish_ms = ms;
        blocked_publish_ok = static_cast<bool>(r);
        blocked_publish_end_ns = end;
      }
      if (!r) break;  // Shutdown 已把闸拉下 → kInvalidState,收工
      std::this_thread::sleep_for(5ms);
    }
  });

  // 等到确有一次 Publish 卡够了阈值。
  bool caught = false;
  auto t_wait = Clock::now();
  while (MsSince(t_wait) < 10000) {
    const int64_t begin = publish_start_ns.load();
    if (begin != 0 && (Now() - begin) / 1000000 >= kBlockedThresholdMs) {
      caught = true;
      break;
    }
    std::this_thread::sleep_for(5ms);
  }
  if (!caught) stop = true;

  const auto t_shutdown = Clock::now();
  p.tx.Shutdown();
  const int shutdown_ms = MsSince(t_shutdown);
  const int64_t shutdown_end_ns = Now();
  stop = true;
  writer.join();

  ASSERT_TRUE(caught) << "没能把 Publish 按住,本例测不到东西";

  // ① 那一次 Publish 跑满了整段停滞——**没有被 Shutdown 截断**。
  EXPECT_GE(blocked_publish_ms.load(), kSinkStallMs - 150);
  EXPECT_TRUE(blocked_publish_ok.load()) << "被 Shutdown 撞上的写不该失败";
  // ② Shutdown 是**等**出来的:它至少等到了那次 Publish 剩下的一段,
  //    且**迟于**它返回。这就是"打不断,只能等"。
  EXPECT_GE(shutdown_ms, kSinkStallMs - kBlockedThresholdMs - 150);
  EXPECT_GT(blocked_publish_end_ns.load(), 0);
  EXPECT_LE(blocked_publish_end_ns.load(), shutdown_end_ns);
}
