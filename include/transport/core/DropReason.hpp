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

/// @brief 框架本地丢弃/重复抑制的命名归因原因(四项,穷尽 CONTEXT.md「丢弃归因」)。
///
/// 每项恰好一个归属组件 + 一个定义时刻(ADR-0003 D13 Q3 可审计表):
/// - `kBusinessQueueOverflow`   -> `BoundedQueue::Push` 满
/// - `kBadFrame`                -> 各 node 读循环 `codec.Decode` 失败
/// - `kUnmatchedOrLateResponse` -> 各 node `Dispatch` 的 `PendingTable::Resolve` 返回 false
/// - `kCloseDrop`               -> `NodeBase` 收敛 drain
///
/// 原「连接代际隔离丢弃」(`ProtocolNode` reactor 断连收敛)随 ADR-0004 D3
/// **撤销连接代际隔离**而移除:交互层不再于断链时清空旧链路排队业务,该归因项的产生
/// 机制已不存在。
///
/// 原 `kNoHandlerConfigured`(各 node `Dispatch` 未设 handler)随 ADR-0009 **废止内建
/// handler 通道**而移除:入站业务改由订阅承载,"未设 handler"这一产生时刻不复存在。
///
/// 原 `kDdsHandoffOverflow`(`DdsTransport` listener 交接满)随 ADR-0013 **D11** 移除:
/// DDS 的接收队列与 UDP/TCP/串口的接收队列同性质(同为「有界 + 静默丢最旧」,SDD
/// DD-15),三介质都不为它单设归因项,DDS 亦不例外;其唯一产生点——那条独立的跨线程
/// 交接队列——已随 ADR-0008 **D8** 一并消失。
///
/// `HandlerExceptionCount`(处理器执行失败)**不进本口径**——RT_HANDLER_006 是隔离当前
/// 事件语义,不是帧/响应未投递。
enum class DropReason {
  kBusinessQueueOverflow,
  kBadFrame,
  kUnmatchedOrLateResponse,
  kCloseDrop,
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
    case DropReason::kBadFrame:
      return "bad-frame";
    case DropReason::kUnmatchedOrLateResponse:
      return "unmatched-or-late-response";
    case DropReason::kCloseDrop:
      return "close-drop";
  }
  return "unknown";
}

}  // namespace transport
