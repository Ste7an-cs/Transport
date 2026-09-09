// Dispatcher 的跨线程并发用例(ADR-0016):索引由 State 内的 boost::fibers::mutex 保护。
//
// 加锁前,这些用例会稳定地在 `Dispatch` 遍历 `by_mask` 与工作线程 `Unsubscribe` 擦除条目
// 之间踩到失效迭代器 / 撕裂的哈希桶。此处不断言"一定崩",只断言加锁后的三条不变量:
//   1. 并发登记与注销后,在册数与索引形态回到确切值(无丢失、无残留、无重复 id);
//   2. 边投递边增删不崩、不丢锁(投递次数与在册数各自自洽);
//   3. `CloseAll` 与并发 `Subscribe` 竞争时,终态恒为"索引已清空",不留悬挂条目。
//
// 线程模型:工作线程是**非 fiber 线程**——boost.fiber 会为其惰性建主 fiber,取不到锁时
// 阻塞该线程,正是期望语义(与 ChannelHub 跨线程 push 的既有用法同源,见 ADR-0001 未决项)。
// 断言一律回到测试 fiber 上做,线程内只累加原子量。
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::yield / sleep_for
#include <gtest/gtest.h>

#include "task/fibertask.h"
#include "transport/core/Dispatcher.hpp"
#include "transport/core/Error.hpp"

using namespace std::chrono_literals;
using transport::Dispatcher;
using transport::kAny;
using transport::TransportErrc;
using transport::make_error_code;

namespace {

struct Ev {
  std::uint8_t level = 0;
  std::uint16_t code = 0;
  std::string tag;
};

using EvDispatcher = Dispatcher<Ev, std::uint8_t, std::uint16_t>;

EvDispatcher MakeDispatcher() {
  return EvDispatcher(
      [](const Ev& e) { return std::make_tuple(e.level, e.code); });
}

constexpr int kThreads = 4;
constexpr int kRounds = 200;

}  // namespace

// —— 并发登记 / 注销 ————————————————————————————————————————————————

// 多线程同时 Subscribe:每次登记都必须落进索引,且各得独立的 id(next_id 的自增在锁内)。
// 全部凭据析构后索引须回到空——注销路径同样加锁,不会漏删或删错条目。
TEST(DispatcherConcurrency, ConcurrentSubscribeThenReleaseLeavesIndexEmpty) {
  auto d = MakeDispatcher();
  {
    std::vector<std::vector<EvDispatcher::Ticket>> per_thread(kThreads);
    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
      workers.emplace_back([&, t] {
        per_thread[t].reserve(kRounds);
        for (int i = 0; i < kRounds; ++i) {
          // 同一 mask(两字段全约束),键值逐线程错开,压的是同一批哈希桶。
          per_thread[t].push_back(d.Subscribe(
              {static_cast<std::uint8_t>(t), static_cast<std::uint16_t>(i)}));
        }
      });
    }
    for (auto& w : workers) w.join();

    EXPECT_EQ(d.Size(), static_cast<std::size_t>(kThreads * kRounds));
    EXPECT_EQ(d.ProbeCount(), 1u) << "全是同一种 mask";
  }
  EXPECT_EQ(d.Size(), 0u);
  EXPECT_EQ(d.ProbeCount(), 0u) << "空桶与空 mask 未被清理";
}

// 登记与注销同时进行:一半线程只登记后立刻 Reset,一半线程长持。终态须精确等于长持的份数。
TEST(DispatcherConcurrency, InterleavedSubscribeAndUnsubscribeIsExact) {
  auto d = MakeDispatcher();
  std::vector<EvDispatcher::Ticket> kept;
  std::mutex kept_mutex;  // 只护测试自己的 vector，与被测件无关

  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      for (int i = 0; i < kRounds; ++i) {
        auto ticket = d.Subscribe(
            {static_cast<std::uint8_t>(t), static_cast<std::uint16_t>(i % 7)});
        if (i % 2 == 0) {
          ticket.Reset();  // 立即注销：与其他线程的登记正面对撞
        } else {
          std::lock_guard<std::mutex> lock(kept_mutex);
          kept.push_back(std::move(ticket));
        }
      }
    });
  }
  for (auto& w : workers) w.join();

  EXPECT_EQ(d.Size(), static_cast<std::size_t>(kThreads * kRounds / 2));
  kept.clear();
  EXPECT_EQ(d.Size(), 0u);
}

// —— 边投递边增删 ————————————————————————————————————————————————

// 本用例是这次加锁的**直接动因**:测试 fiber 持续 Dispatch(遍历 by_mask),工作线程同时
// Subscribe / Reset(插入与擦除同一批桶)。加锁前必踩失效迭代器。
//
// 投递份数不作精确断言——快照与投递之间可能有凭据注销,这是文件头写明的窗口;这里断言的是
// "不崩、终态精确、且确实有消息送达过"。
TEST(DispatcherConcurrency, DispatchWhileSubscribersComeAndGo) {
  auto d = MakeDispatcher();
  std::atomic<bool> stop{false};
  std::atomic<std::uint64_t> churn{0};

  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      for (int i = 0; i < kRounds && !stop.load(); ++i) {
        auto ticket = d.Subscribe({static_cast<std::uint8_t>(t % 2), kAny});
        auto wide = d.Subscribe({kAny, kAny});  // 引入第二种 mask，压 by_mask 的增删
        churn.fetch_add(1);
        ticket.Reset();
        // wide 由析构注销：走的是 ~Ticket 而非显式 Reset，两条路径都要覆盖
      }
    });
  }

  std::uint64_t delivered = 0;
  for (int i = 0; i < 4000; ++i) {
    delivered += d.Dispatch(Ev{static_cast<std::uint8_t>(i % 2),
                               static_cast<std::uint16_t>(i), "churn"});
    if (churn.load() >= static_cast<std::uint64_t>(kThreads * kRounds)) break;
    boost::this_fiber::yield();
  }
  stop.store(true);
  for (auto& w : workers) w.join();

  EXPECT_GT(delivered, 0u) << "整轮竟无一条送达，说明投递路径被并发改动打断";
  EXPECT_EQ(d.Size(), 0u) << "所有凭据均已注销，索引应为空";
  EXPECT_EQ(d.ProbeCount(), 0u);
}

// —— 与 CloseAll 竞争 ——————————————————————————————————————————————

// CloseAll 与并发 Subscribe 对撞:每一张凭据要么进了索引(随即被 CloseAll 关闭并摘除),
// 要么因 closed 已置而拿到一张已关闭的空凭据。两条路径都不得留下悬挂条目。
TEST(DispatcherConcurrency, CloseAllRacingWithSubscribeLeavesNoEntries) {
  auto d = MakeDispatcher();
  std::vector<std::vector<EvDispatcher::Ticket>> per_thread(kThreads);

  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&, t] {
      per_thread[t].reserve(kRounds);
      for (int i = 0; i < kRounds; ++i) {
        per_thread[t].push_back(d.Subscribe({static_cast<std::uint8_t>(t), 10}));
      }
    });
  }

  boost::this_fiber::sleep_for(1ms);  // 让工作线程先跑起来，确保是真对撞
  d.CloseAll(make_error_code(TransportErrc::kClosed));
  for (auto& w : workers) w.join();

  // CloseAll 之后再登记的凭据不进索引;之前登记的已被 CloseAll 清空。
  EXPECT_EQ(d.Size(), 0u);
  EXPECT_EQ(d.ProbeCount(), 0u);
  EXPECT_EQ(d.Dispatch(Ev{0, 10, "after-close"}), 0u);

  // 抽查:凡是拿到信箱的凭据,Wait 都应立即得到终止原因,不得挂住。
  for (auto& tickets : per_thread) {
    ASSERT_FALSE(tickets.empty());
    auto got = tickets.front().Wait(200ms);
    ASSERT_FALSE(got);
    EXPECT_EQ(got.error(), make_error_code(TransportErrc::kClosed));
  }
}

// 凭据在**非 fiber 线程**上析构:注销要取 fiber 互斥量,该线程会被 boost.fiber 惰性建主
// fiber 后阻塞等待,而非崩溃。这是宿主把凭据交给普通工作线程持有时的实际形态。
TEST(DispatcherConcurrency, TicketDestroyedOnPlainThreadUnsubscribes) {
  auto d = MakeDispatcher();
  auto ticket = d.Subscribe({7, 10});
  EXPECT_EQ(d.Size(), 1u);

  std::thread([moved = std::move(ticket)]() mutable {
    // moved 在此线程栈上析构 → Unsubscribe → 取锁
  }).join();

  EXPECT_EQ(d.Size(), 0u);
  EXPECT_EQ(d.Dispatch(Ev{7, 10, "gone"}), 0u);
}
