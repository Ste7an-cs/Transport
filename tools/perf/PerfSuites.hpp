#pragma once

/**
 * @file PerfSuites.hpp
 * @brief 两套 suite 与两个角色的入口(ADR-0018 **D1**)。
 *
 * 全部入口都**已在 fiber 内**——一切等待语义都要 fiber 上下文,引导在 `main.cpp`。
 */

#include "PerfOptions.hpp"

namespace perf {

class PerfLink;

/// @brief 顶层:装配链路 → 按角色分派。返回进程退出码(0 = 正常)。
[[nodiscard]] int Run(const PerfOptions& options);

/// @brief 服务端角色:订阅 → 按控制消息切换回显模式 → 逐条应答 / 计数,直到收到 `kBye`。
[[nodiscard]] int RunServer(const PerfOptions& options, PerfLink& link);

/// @brief 客户端角色:逐行矩阵跑两套 suite 之一,**打印最终表格**。
[[nodiscard]] int RunClient(const PerfOptions& options, PerfLink& link);

}  // namespace perf
