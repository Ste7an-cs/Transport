#pragma once

/// @file Observability.hpp
/// @brief 协议/介质无关的共享观测原语:丢弃归因(pull+push)与非丢弃事件(仅 push)。
///
/// 复用既有 `ITraceSink`(零分配视图 `TraceEvent`)。RT_TRACE_002:未配置 sink
/// (`sink == nullptr`)时不得改变控制流/字节流/错误结果——两个原语在此路径上仅做一次
/// 判空,无其他开销。见 ADR-0003 D13(Q2/Q3:小型可复用原语,非全局单例、非通用指标聚合
/// 抽象,守 D10「不造上帝对象」)。
///
/// 纪律(D13,详见 ADR-0003):本头协议/介质无关——不知道任何具体协议消息类型或字段、
/// 不知道任何具体介质(TCP/UDP/DDS)。调用方(各 node/transport)在其归属的定义时刻
/// 传入协议/介质相关的 `key`/`endpoint` 等*已是纯文本*的观测值,本头本身不解释、不校验
/// 这些字符串的协议含义。

#include <cstddef>
#include <string_view>

#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/TraceCategories.hpp"

namespace transport {

/// @brief 丢弃归因原语:一次调用完成计数(pull)+ 可选 Trace(push)。
///
/// 供各组件在其"恰好一个归属定义时刻"(ADR-0003 D13 Q3 可审计表)调用:`counter`
/// (调用方既有的具名 `std::size_t` 计数成员,不改既有公开 getter API)原地恰好 +1;
/// 若 `sink` 非空,额外发送一条 `category = "drop"`、`message = DropReasonName(reason)`
/// 的 `TraceEvent`。RT_TRACE_002:`sink == nullptr` 时只有一次判空,不构造
/// `TraceEvent`、不做任何 Trace 相关工作。
/// 重入警示(#98):调用点常与 `counter` 的保护锁同临界区 → `sink->OnTrace` 可能在库内
/// 锁内被调——sink 实现须遵 `ITraceSink` 重入契约(快速返回、不回调本库 API)。
/// @param reason 丢弃归因(见 `DropReason`)。
/// @param counter 调用方持有的具名计数成员,调用后恰好 +1。
/// @param sink 可选 Trace 出口,可为 `nullptr`。
/// @param key 可选相关键(如 session/correlation 键的纯文本表示),写入 `TraceEvent.key`。
/// @param endpoint 可选 endpoint/from/route 名,写入 `TraceEvent.endpoint`。
/// @param level Trace 级别,默认 `kWarn`(丢弃是需要关注的信号)。
inline void RecordDrop(DropReason reason, std::size_t& counter, ITraceSink* sink,
                        std::string_view key = {}, std::string_view endpoint = {},
                        TraceLevel level = TraceLevel::kWarn) {
  ++counter;
  if (sink == nullptr) return;
  TraceEvent ev{};
  ev.level = level;
  ev.category = kTraceCategoryDrop;
  ev.message = DropReasonName(reason);
  ev.key = key;
  ev.endpoint = endpoint;
  sink->OnTrace(ev);
}

/// @brief 非丢弃类观测事件原语:仅 push,不关联任何计数器。
///
/// 供连接/代际/收发/解码/关联/超时·取消/处理器/重连/关闭等状态跃迁/结果确定边界调用
/// (ADR-0003 D13 Q5 九类 Trace 类别),不逐字节、不逐 fiber 调度事件触发。
/// RT_TRACE_002:`sink == nullptr` 时只有一次判空,是空操作,不构造 `TraceEvent`。
/// @param category Trace 类别静态字面量(如 "connect"/"send"/"recv"/"decode"/...)。
/// @param sink 可选 Trace 出口,可为 `nullptr`。
/// @param message 可选短子原因,写入 `TraceEvent.message`。
/// @param key 可选相关键,写入 `TraceEvent.key`。
/// @param endpoint 可选 endpoint/from/route 名,写入 `TraceEvent.endpoint`。
/// @param error 可选错误串,写入 `TraceEvent.error`。
/// @param size 可选字节数/计数(按 category 解释),默认 `kNoNum`(无该数值)。
/// @param attempt 可选重试第几次,默认 -1(无)。
/// @param tag 可选 FrameTag,默认 `kNoTag`(无判别符)。
/// @param level Trace 级别,默认 `kInfo`。
inline void RecordEvent(std::string_view category, ITraceSink* sink,
                         std::string_view message = {}, std::string_view key = {},
                         std::string_view endpoint = {}, std::string_view error = {},
                         long size = kNoNum, int attempt = -1, int tag = kNoTag,
                         TraceLevel level = TraceLevel::kInfo) {
  if (sink == nullptr) return;
  TraceEvent ev{};
  ev.level = level;
  ev.category = category;
  ev.message = message;
  ev.key = key;
  ev.endpoint = endpoint;
  ev.error = error;
  ev.size = size;
  ev.attempt = attempt;
  ev.tag = tag;
  sink->OnTrace(ev);
}

}  // namespace transport
