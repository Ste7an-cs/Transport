#pragma once

/// @file TraceCategories.hpp
/// @brief Trace 类别常量的单一权威(ADR-0003 D13 Q5 九类;P6 前置清理 #98 集中收口)。
///
/// 全部 `RecordEvent`/`RecordDrop` 发射点必须引用本头常量,不得散落字符串字面量——
/// 手误拼错类别会静默产生"新类别",loss=0 harness 与任何按类别过滤的消费方都筛不到,
/// 编译器无从发现。测试侧断言可继续用字面量(作为对线缆值的独立验证)。
///
/// 类别语义(仅在状态跃迁/结果确定边界触发,不逐字节、不逐 fiber 调度事件):
/// - `drop`:丢弃归因(`RecordDrop` 专用;message = DropReasonName)。
/// - `lifecycle`:节点/传输生命周期跃迁(Created→Running→Closing→Closed;
///   原名 `close`,P6 前置清理 #98 改名——"running" 挂在 "close" 下是命名误导)。
/// - `connect`:连接状态机跃迁(ConnectionStateName)。
/// - `generation`:连接代际推进。
/// - `send` / `recv` / `decode`:出站写完成 / 解出消息 / 一次 Decode 成功。
/// - `match` / `timeout` / `cancel`:请求终结三分类(第四类 FailAll 不发 Trace)。
/// - `handler`:业务处理器单次调用起止。
/// - `reconnect`:自动重连尝试及其结果。

#include <string_view>

namespace transport {

inline constexpr std::string_view kTraceCategoryDrop = "drop";
inline constexpr std::string_view kTraceCategoryLifecycle = "lifecycle";
inline constexpr std::string_view kTraceCategoryConnect = "connect";
inline constexpr std::string_view kTraceCategoryGeneration = "generation";
inline constexpr std::string_view kTraceCategorySend = "send";
inline constexpr std::string_view kTraceCategoryRecv = "recv";
inline constexpr std::string_view kTraceCategoryDecode = "decode";
inline constexpr std::string_view kTraceCategoryMatch = "match";
inline constexpr std::string_view kTraceCategoryTimeout = "timeout";
inline constexpr std::string_view kTraceCategoryCancel = "cancel";
inline constexpr std::string_view kTraceCategoryHandler = "handler";
inline constexpr std::string_view kTraceCategoryReconnect = "reconnect";

}  // namespace transport
