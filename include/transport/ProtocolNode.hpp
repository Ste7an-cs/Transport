#pragma once

/**
 * @file ProtocolNode.hpp
 * @brief 最小外部协议交互节点 ProtocolNode(RT_NODE_003 / RT_REQUEST / ADR-0003 D8/D9)。
 *
 * ProtocolNode **组合**(不继承、不共享引擎)`ITransport`(纯字节管道)+ `ICodec`
 * (线缆格式)+ `PendingTable`(挂起-应答薄基座),内联一条读-分发循环,交付一次
 * needresponse 的请求-响应。协议特有语义——键派生、frm_type 盖章、session_id 滚动
 * 分配、终结帧判别(kResponse/kResult)、未匹配路由——**全部内联在本类**;PendingTable
 * 保持协议无关(ADR-0003 D9 红线)。只经 CorrelationKeyStrategy 开放 KeyOf 注入,
 * IsTerminal / RouteUnmatched 内联锁死。
 *
 * 交互状态(session_id 分配器、生命周期、观测计数器)由一把 std::mutex 守(D8);单
 * fiber 调度器、无 affinity(D8/Q9)。并发硬化 / 协作取消 / 256 上限留 P2。
 */

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/PendingTable.hpp"
#include "transport/Result.hpp"
#include "transport/SharedCompletion.hpp"
#include "transport/TransportTypes.hpp"

namespace transport {

/// 关联键类型:请求↔响应配对的机器键(P1 为 (session_id<<16)|命令码 的 uint32)。
using ProtocolKey = std::uint32_t;

/// 占位外部响应标记位(TBD-003):请求命令码或此位得响应命令码。可经 config 注入替换。
inline constexpr std::uint16_t kResponseMarker = 0x1000;

/**
 * @brief 关联键派生策略:把一条 Message 映射成请求键 / 响应键(仅此二者可注入)。
 *
 * 请求键与响应键对同一次交互必须相等——这是配对成立的定义。DefaultProtocolKeyStrategy
 * 给出占位实现(见其文档);对接真实外部协议时替换为其配对规则。
 */
struct CorrelationKeyStrategy {
  std::function<ProtocolKey(const Message&)> request_key;
  std::function<ProtocolKey(const Message&)> response_key;
};

/**
 * @brief 缺省占位关联键策略。
 *
 * 请求键 = (session_id<<16) | 命令码原样;响应键 = (session_id<<16) |
 * (命令码 & ~response_marker) 归一化回请求命令码。于是请求 message_id=0x0002 的响应
 * message_id=0x1002 两键相等而配对成立。
 *
 * @param response_marker 响应命令码相对请求命令码所置的标记位(占位默认 kResponseMarker)。
 */
CorrelationKeyStrategy DefaultProtocolKeyStrategy(
    std::uint16_t response_marker = kResponseMarker);

/// ProtocolNode 配置:关联键策略 + 默认外部协议 id。
struct ProtocolNodeConfig {
  CorrelationKeyStrategy key_strategy = DefaultProtocolKeyStrategy();
  std::uint8_t protocol_id = 0;
};

/**
 * @brief 最小外部协议交互节点:组合 transport + codec + PendingTable,交付请求-响应。
 *
 * 生命周期:Created→Running(Start)→Closing→Closed(Close)。不可拷贝、不可移动
 * (读-分发循环 fiber 捕获 this)。
 */
class ProtocolNode {
 public:
  ProtocolNode(std::unique_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
               ProtocolNodeConfig config = {});
  ~ProtocolNode();

  ProtocolNode(const ProtocolNode&) = delete;
  ProtocolNode& operator=(const ProtocolNode&) = delete;

  /// @brief 启动底层 transport 并 spawn 读-分发循环 fiber。Created→Running。
  Status Start();

  /**
   * @brief 幂等关闭:Closing → RequestClose + PendingTable.FailAll(kClosed) + 等读循环
   *        退出 → Closed。关闭后 Request 一律 kClosed。
   */
  Status Close();

  /// @brief 等待节点收敛到 Closed(复用 SharedCompletion<void>,支持 deadline/取消)。
  [[nodiscard]] Status WaitClosed(OperationOptions options = {});

  /**
   * @brief 交付一次 needresponse 请求-响应。
   *
   * 调用方给 payload + message_id;node 盖 frm_type=kCommand、默认 protocol_id、滚动分配
   * session_id;request_key → PendingTable.Register(重复键 kInvalidState 透传)→ Encode →
   * transport.Write → Handle::Wait 等唯一响应。总超时经 options.deadline(调用方从接受请求
   * 起算)。关闭后返 kClosed。
   *
   * @param req     请求 Message(payload + message_id 由调用方填)。
   * @param options 截止时间与取消令牌。
   * @return 匹配响应 Message,或机器可判别错误(kClosed / kInvalidState / kTimeout / …)。
   */
  [[nodiscard]] Result<Message> Request(Message req, OperationOptions options = {});

  /// @brief 观测:响应帧无匹配在途请求(迟到 / 乱序 / 无匹配)而被丢弃的累计次数。
  [[nodiscard]] std::size_t UnmatchedResponseCount() const;

  /// @brief 观测:业务帧因 P1 无 handler / 无队列而被丢弃的累计次数。
  [[nodiscard]] std::size_t DroppedNoHandlerCount() const;

 private:
  /// 读-分发循环体(在 spawn 的 fiber 中跑):Read → Decode → 逐条分发。
  void RunReadLoop();
  /// 单条 Message 的分发:响应帧 → Resolve;业务帧 → 丢弃归因。
  void Dispatch(Message msg);

  std::unique_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  ProtocolNodeConfig config_;
  PendingTable<ProtocolKey, Message> pending_;
  SharedCompletion<void> loop_done_;  ///< 读循环退出通知(Close 等待点)。
  SharedCompletion<void> closed_;     ///< 节点 Closed 通知(WaitClosed 等待点)。

  mutable std::mutex mutex_;  ///< 守交互状态(D8)。
  LifecycleState lifecycle_{LifecycleState::kCreated};
  std::uint8_t next_session_{0};  ///< 滚动 session_id 分配器(uint8 单调滚动)。
  std::size_t unmatched_response_count_{0};
  std::size_t dropped_no_handler_count_{0};
};

}  // namespace transport
