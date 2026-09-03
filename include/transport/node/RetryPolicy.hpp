#pragma once

/**
 * @file RetryPolicy.hpp
 * @brief 一次交互中**重发策略**的公共值类型(ADR-0010 D6 / RT_NODE_002_e)。
 *
 * 独立成头,因为 `ProtocolNode` 的三个交互方法与 `DdsNode::RequestForResultDirect`
 * (ADR-0013 **D7** 与 **D8**)用的是同一个类型;`ProtocolNode.hpp` include 本头。
 *
 * **逐次调用传入而不进节点配置**:同一节点上不同命令的耐受度不同,不宜由节点级配置一刀切;
 * 两阶段模型里两个等待阶段更是数量级不同的量。
 */

#include <chrono>

namespace transport {

/// @brief 重发策略:单次等待时限 + 总发送次数。
///
/// 具体在**哪个阶段**重发由使用它的交互方法决定:`ProtocolNode::RequestForResponse` /
/// `RequestForResult` 用于**受理阶段**;`ProtocolNode::RequestForResultDirect` 与
/// `DdsNode::RequestForResultDirect` 用于**唯一的等结果阶段**(ADR-0010 D13 /
/// ADR-0013 D7)——后者的签名因此**没有**独立的 `result_timeout`。
struct RetryPolicy {
  /// 单次尝试的等待时长;须为正值,否则调用返 kInvalidArgument。
  std::chrono::milliseconds timeout{};
  /// 总发送次数(**含首发**);须 ≥ 1,否则调用返 kInvalidArgument。
  int max_attempts = 1;
};

}  // namespace transport
