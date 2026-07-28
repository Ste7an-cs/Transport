#include <QCoreApplication>
#include <gtest/gtest.h>

#include "task/fiberapplication.h"   // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"          // Coro::makeTask

// 协程测试须在 fiber 调度器内跑:主线程装本地调度器,断言体在 makeTask 的 fiber 中执行
//(这样 Request/await_for 有 fiber 上下文),跑完 quit() 让 exec() 返回。
int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  int rc = 0;
  Coro::installFiberApplication();
  auto task = Coro::makeTask([&] {
    rc = RUN_ALL_TESTS();
    Coro::quit();
  });
  Coro::exec();
  (void)task;
  return rc;
}
