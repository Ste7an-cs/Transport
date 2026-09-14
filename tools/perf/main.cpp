/**
 * @file main.cpp
 * @brief `transport_perf` 的入口——命令行解析 + fiber 引导(ADR-0018 **D1** / **D7**)。
 *
 * **独立可执行,不进 `transport_tests`**(**D7**):性能测试慢且抖,混进单元测试会污染
 * 全绿口径、也会拖慢每次构建。
 *
 * **只报数,不做 pass/fail**(**D6**):打出表格即完成使命,没有任何阈值断言。退出码只
 * 反映"跑没跑通",不反映"数字好不好看"。
 *
 * 一切等待语义必须在 fiber 内,故与 `tests/coro_test_main.cpp` 同形:
 * `QCoreApplication` + `installFiberApplication` + `makeTask` + `exec`。
 */

#include <cstdio>

#include <QCoreApplication>

#include "task/fiberapplication.h"  // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"         // Coro::makeTask

#include "PerfOptions.hpp"
#include "PerfSuites.hpp"

int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);

  perf::PerfOptions options;
  const perf::ParseOutcome outcome = perf::ParseOptions(argc, argv, &options);
  if (!outcome.ok) {
    std::fprintf(stderr, "transport_perf: %s\n", outcome.error.c_str());
    std::fprintf(stderr, "用 --help 看用法。\n");
    return 2;
  }
  if (outcome.help) {
    perf::PrintUsage();
    return 0;
  }

  int rc = 0;
  Coro::installFiberApplication();
  auto task = Coro::makeTask([&] {
    rc = perf::Run(options);
    Coro::quit();
  });
  Coro::exec();
  (void)task;
  return rc;
}
