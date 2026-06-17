#include "transport/comm/ThreadExecutor.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using transport::ThreadExecutor;
using namespace std::chrono_literals;

TEST(ThreadExecutor, PostRunsTasksSeriallyInOrder) {
  ThreadExecutor ex(64);
  ex.Start();
  std::vector<int> seen;
  std::mutex m;
  for (int i = 0; i < 100; ++i)
    ex.Post([&, i] { std::lock_guard<std::mutex> lk(m); seen.push_back(i); });
  // 等排空:投递一个置位任务并轮询
  std::atomic<bool> done{false};
  ex.Post([&] { done = true; });
  for (int i = 0; i < 100 && !done; ++i) std::this_thread::sleep_for(5ms);
  ex.Stop();
  ASSERT_EQ(seen.size(), 100u);
  for (int i = 0; i < 100; ++i) EXPECT_EQ(seen[i], i);  // 串行有序
}

TEST(ThreadExecutor, ScheduleAtFires) {
  ThreadExecutor ex(64);
  ex.Start();
  std::atomic<bool> fired{false};
  ex.ScheduleAt(std::chrono::steady_clock::now() + 30ms, [&] { fired = true; });
  for (int i = 0; i < 100 && !fired; ++i) std::this_thread::sleep_for(5ms);
  EXPECT_TRUE(fired.load());
  ex.Stop();
}

TEST(ThreadExecutor, CancelPreventsFire) {
  ThreadExecutor ex(64);
  ex.Start();
  std::atomic<bool> fired{false};
  auto id = ex.ScheduleAt(std::chrono::steady_clock::now() + 50ms, [&] { fired = true; });
  ex.Cancel(id);
  std::this_thread::sleep_for(120ms);
  EXPECT_FALSE(fired.load());
  ex.Stop();
}

TEST(ThreadExecutor, PostBlocksWhenFull) {
  ThreadExecutor ex(/*capacity=*/1);
  ex.Start();
  std::promise<void> gate;             // 阻住 worker 的第一个任务
  auto gate_fut = gate.get_future();
  ex.Post([&] { gate_fut.wait(); });   // worker 卡在这
  ex.Post([] {});                      // 占满容量(队列 size=1)
  std::atomic<bool> third_posted{false};
  std::thread t([&] { ex.Post([] {}); third_posted = true; });  // 应阻塞
  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(third_posted.load());   // 满 → 第三个 Post 仍阻塞
  gate.set_value();                    // 放行 → 排空 → 第三个 Post 解除
  t.join();
  EXPECT_TRUE(third_posted.load());
  ex.Stop();
}
