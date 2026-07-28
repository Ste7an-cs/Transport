#include <gtest/gtest.h>
#include "await/awaitable.hpp"  // Coro::Awaitable

// 证明 fiber 运行时 + channel + 链接均通:在 fiber 内 resolve 再 await 取回。
TEST(CoroHarness, AwaitableRoundtrip) {
  Coro::Awaitable<int> aw;
  ASSERT_TRUE(aw.resolve(42));       // 生产者投递
  auto r = aw.await();               // 同 fiber 消费(队列已有值,立即返回)
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value(), 42);
}
