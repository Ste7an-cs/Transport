#pragma once

/// @file DropReason.hpp
/// @brief 协议/介质无关的丢弃归因枚举。
///
/// 见 ADR-0003 D13(Q2)、CONTEXT.md「丢弃归因」、
/// docs/需求规格说明书-协程原生.md RT_DATA_BUFFER("每一次框架本地丢弃或重复抑制应归因到
/// 恰好一个命名且计数的原因")。每个 DropReason 恰好归属一个组件的一个定义时刻
/// (可审计表见 ADR-0003 D13 Q3),供 `core/Observability.hpp` 的 `RecordDrop` 在该
/// 时刻调用完成计数(pull)+ 可选 Trace(push)。
///
/// 纪律(D13,详见 ADR-0003):本头协议/介质无关——不知道任何具体协议消息类型或字段、
/// 不知道任何具体介质(TCP/UDP/DDS)。

#include <string_view>

namespace transport {

/// @brief 框架本地丢弃/重复抑制的命名归因原因(七项,穷尽 CONTEXT.md「丢弃归因」)。
///
/// 每项恰好一个归属组件 + 一个定义时刻(ADR-0003 D13 Q3 可审计表):
/// - `kBusinessQueueOverflow`   -> `BoundedQueue::Push` 满
/// - `kDdsHandoffOverflow`      -> `DdsTransport` listener 交接满
/// - `kBadFrame`                -> 各 node 读循环 `codec.Decode` 失败
/// - `kUnmatchedOrLateResponse` -> 各 node `Dispatch` 的 `PendingTable::Resolve` 返回 false
/// - `kCloseDrop`               -> `NodeRuntime` 关闭 drain
/// - `kGenerationIsolationDrop` -> `ProtocolNode` reactor 断连收敛
/// - `kNoHandlerConfigured`     -> 各 node `Dispatch` 未设 handler
///
/// `HandlerExceptionCount`(处理器执行失败)**不进本口径**——RT_HANDLER_006 是隔离当前
/// 事件语义,不是帧/响应未投递。
enum class DropReason {
  kBusinessQueueOverflow,
  kDdsHandoffOverflow,
  kBadFrame,
  kUnmatchedOrLateResponse,
  kCloseDrop,
  kGenerationIsolationDrop,
  kNoHandlerConfigured,
};

/// @brief 返回 `DropReason` 的稳定短名,供 Trace `message`/日志复用。
///
/// 静态字面量 `string_view`,零分配;供 `RecordDrop` 写入 `TraceEvent.message`。
/// @param reason 待命名的丢弃原因。
/// @return 该原因的稳定短名(kebab-case);未识别的枚举值返回 "unknown"。
[[nodiscard]] constexpr std::string_view DropReasonName(DropReason reason) noexcept {
  switch (reason) {
    case DropReason::kBusinessQueueOverflow:
      return "business-queue-overflow";
    case DropReason::kDdsHandoffOverflow:
      return "dds-handoff-overflow";
    case DropReason::kBadFrame:
      return "bad-frame";
    case DropReason::kUnmatchedOrLateResponse:
      return "unmatched-or-late-response";
    case DropReason::kCloseDrop:
      return "close-drop";
    case DropReason::kGenerationIsolationDrop:
      return "generation-isolation-drop";
    case DropReason::kNoHandlerConfigured:
      return "no-handler-configured";
  }
  return "unknown";
}

}  // namespace transport
