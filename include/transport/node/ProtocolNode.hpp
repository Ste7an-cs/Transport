#pragma once

/**
 * @file ProtocolNode.hpp
 * @brief 最小外部协议交互节点 ProtocolNode(RT_NODE_003 / RT_REQUEST / ADR-0003 D8/D9)。
 *
 * ProtocolNode **组合**(不继承、不共享引擎)`ITransport`(纯字节管道)+ `ICodec`
 * (线缆格式)+ `PendingTable`(挂起-应答薄基座),内联一条读-分发循环,交付一次
 * needresponse 的请求-响应。协议特有语义——键派生、frm_type 盖章、session_id 空闲集
 * LRU 分配、终结帧判别(kResponse/kResult)、未匹配路由——**全部内联在本类**;
 * PendingTable 保持协议无关(ADR-0003 D9/D10 红线)。只经 CorrelationKeyStrategy 开放
 * KeyOf 注入,IsTerminal / RouteUnmatched 内联锁死。
 *
 * session_id 容量:线缆 uint8 硬顶 256 个并发在途。分配器维护 0..255 空闲集,Request
 * 取最久释放者(FIFO 复用 = 最大退休窗口,RT_REQUEST_005),256 全在途 → 发送前返
 * kResourceExhausted(RT_REQUEST_006);请求终结后释放回空闲集。session_id 分配器 + 256
 * 这个值是协议特有、内联本类(D10);PendingTable 的纯计数上限才是协议无关。
 *
 * 交互状态(session_id 空闲集、生命周期、观测计数器)由一把 std::mutex 守(D8);单
 * fiber 调度器、无 affinity(D8/Q9)。
 */

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "transport/node/BoundedQueue.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/io/IConnectionObservable.hpp"
#include "transport/codec/ICodec.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/NodeRuntime.hpp"
#include "transport/node/PendingTable.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/SharedCompletion.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

class ProtocolNode;

/**
 * @brief 入站业务处理器的能力面(RT_HANDLER_001):handler 经它与节点交互,而非裸捕获
 *        node&。只露三项能力,协议内部状态不外泄。
 *
 * 由 ProtocolNode 在消费者 fiber 内构造并按引用传入 handler;handler 不得持有其地址越出
 * 单次调用(生命周期系于该次分发)。
 */
class HandlerContext {
 public:
  /// @brief 从本节点 fire-and-forget 发一帧(noresponse);委托到 ProtocolNode::Send。
  [[nodiscard]] Status Send(Message msg);

  /**
   * @brief 请求关闭本节点(非阻塞):发起完整收敛拆卸但不自等待(RT_LIFECYCLE_005)。
   *
   * 委托 ProtocolNode::Close;因当前即 handler 消费者 fiber,Close 内的重入自锁防护会
   * 只发起拆卸(置 Closing + RequestClose 传输 + Close 业务队列 + 触发 handler 取消 +
   * FailAll + 起 finalizer),跳过对 handler_done_/closed_ 的自等待(等自己 = 自锁),
   * 立即返回。节点由独立 finalizer fiber 在读循环与 handler 均退出后收敛到 Closed。
   */
  Status RequestClose();

  /// @brief 节点所属执行域的协作取消令牌(Close 时被触发);handler 可据它提前收手。
  [[nodiscard]] const CancellationToken& cancellation() const {
    return cancellation_;
  }

 private:
  friend class ProtocolNode;
  HandlerContext(ProtocolNode* node, CancellationToken cancellation)
      : node_(node), cancellation_(std::move(cancellation)) {}

  ProtocolNode* node_;
  CancellationToken cancellation_;
};

/// 入站业务处理器(组合注入,RT_HANDLER_001):对一条业务帧返回结构化结果(仅记录,
/// 框架不据此自动应答,避 TBD-001);预期失败用 Status 表达,不抛异常(RT_HANDLER_005)。
using InboundHandler = std::function<Status(const Message&, HandlerContext&)>;

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

/// ProtocolNode 配置:关联键策略 + 默认外部协议 id + 可选入站业务处理器 + 业务队列上界。
struct ProtocolNodeConfig {
  CorrelationKeyStrategy key_strategy = DefaultProtocolKeyStrategy();
  std::uint8_t protocol_id = 0;
  /// 入站业务处理器(RT_HANDLER_001);为空 = P1 行为(业务帧归因 dropped_no_handler)。
  InboundHandler handler;
  /// 业务队列事件数上界(仅 handler 设时用;越界由 BoundedQueue 钳制)。
  std::size_t business_queue_max_events = BoundedQueue<Message>::kDefaultMaxEvents;
  /// 业务队列字节数上界(仅 handler 设时用;越界由 BoundedQueue 钳制)。
  std::size_t business_queue_max_bytes = BoundedQueue<Message>::kDefaultMaxBytes;
  /// 可选 Trace 出口(P5-4,RT_TRACE_001/002):非空则在 send/recv/decode/match/timeout/
  /// cancel/handler/close 等边界点上报事件;为空时 `RecordEvent` 仅一次判空,不产生任何
  /// 其它开销(RT_TRACE_002)。
  ITraceSink* trace_sink = nullptr;
};

/**
 * @brief 最小外部协议交互节点:组合 transport + codec + PendingTable,交付请求-响应。
 *
 * 生命周期:Created→Running(Start)→Closing→Closed(Close)。不可拷贝、不可移动
 * (读-分发循环 fiber 捕获 this)。
 */
class ProtocolNode {
 public:
  using Clock = OperationOptions::Clock;

  ProtocolNode(std::unique_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
               ProtocolNodeConfig config = {});
  ~ProtocolNode();

  ProtocolNode(const ProtocolNode&) = delete;
  ProtocolNode& operator=(const ProtocolNode&) = delete;

  /**
   * @brief 并发安全幂等启动(RT_LIFECYCLE_003 / RT_LIFECYCLE_007)。
   *
   * 首个 Start 先校验 config(队列上界落 [1,65536] / [64KiB,256MiB]、key_strategy 非空)
   * → 失败返 kConfiguration、停在 Created、允许改配重试;通过则置 starting、做实事(transport
   * Start + spawn 读循环/handler fiber)、Complete start_done_。初始化期间并发进来的 Start
   * await 同一 start_done_ 结果、不重复 spawn。已 Running 再启 → 成功;Closing/Closed →
   * kInvalidState。
   */
  Status Start();

  /**
   * @brief 并发安全幂等关闭(RT_LIFECYCLE_004/005/006)。
   *
   * 首个关闭者拆卸:Running→Closing(立即拒新 Request/Send)→ 三方汇合(transport.RequestClose
   * + 业务队列 Close + 触发 handler 取消)+ PendingTable.FailAll(kClosed),再起独立 finalizer
   * fiber 等读循环退出 +(设 handler 时)消费者 fiber 退出 → Drain 未启动业务归因 close_drop →
   * 置 Closed → closed_.Complete。后续关闭者不重复拆资源、共享 closed_(多等待者);已 Closed
   * 再关直接成功。当前若就是 handler 消费者 fiber(重入)→ 只发起拆卸、跳过对 closed_ 的自
   * 等待(避自锁),节点由 finalizer 收敛。关闭后 Request/Send 一律 kClosed。
   */
  Status Close();

  /// @brief 等待节点收敛到 Closed(复用 SharedCompletion<void>,支持 deadline/取消)。
  [[nodiscard]] Status WaitClosed(OperationOptions options = {});

  /**
   * @brief 交付一次 needresponse 请求-响应。
   *
   * 调用方给 payload + message_id;node 盖 frm_type=kCommand、默认 protocol_id、从空闲集
   * LRU 分配 session_id;request_key → PendingTable.Register(重复键 kInvalidState 透传)→
   * Encode → transport.Write → Handle::Wait 等唯一响应,终结后释放 session_id 回空闲集。
   * 256 个 session_id 全在途时发送前返 kResourceExhausted(不登记、不发送)。总超时经
   * options.deadline(调用方从接受请求起算)。关闭后返 kClosed。
   *
   * @param req     请求 Message(payload + message_id 由调用方填)。
   * @param options 截止时间与取消令牌。
   * @return 匹配响应 Message,或机器可判别错误(kClosed / kResourceExhausted /
   *         kInvalidState / kTimeout / …)。
   */
  [[nodiscard]] Result<Message> Request(Message req, OperationOptions options = {});

  /**
   * @brief noresponse fire-and-forget 出站:盖章 + 编码 + 写出,不期待应答。
   *
   * node 盖 frm_type(调用方给的业务类型优先,否则默认 kCommand)、默认 protocol_id、从
   * 空闲集分配一个 session_id 盖帧后**立即释放**(不登记 PendingTable、不占 256 在途预算)。
   * 编码后经 transport.Write 上线,遵 RT_TRANSPORT_008 背压。关闭后返 kClosed;256 个
   * session_id 全在途(无空闲)时返 kResourceExhausted(边界策略:与 Request 一致地拒绝,
   * 不与在途请求争用 session_id 空间)。
   *
   * @param msg 出站 Message(payload + 可选 message_id / frm_type 由调用方填)。
   * @return 写出结果或机器可判别错误(kClosed / kResourceExhausted / 编码 / 传输错误)。
   */
  [[nodiscard]] Status Send(Message msg);

  /// @brief 观测:响应帧无匹配在途请求(迟到 / 乱序 / 无匹配)而被丢弃的累计次数。
  [[nodiscard]] std::size_t UnmatchedResponseCount() const;

  /// @brief 观测:业务帧因无 handler / 无队列而被丢弃的累计次数(P1 行为)。
  [[nodiscard]] std::size_t DroppedNoHandlerCount() const;

  /// @brief 观测:业务队列满而 tail-drop 的累计次数(命名归因 business_queue_overflow)。
  [[nodiscard]] std::size_t BusinessQueueOverflowCount() const;

  /// @brief 观测:handler 逃逸异常被边界兜住、转 kInternal 隔离的累计次数(RT_HANDLER_006)。
  [[nodiscard]] std::size_t HandlerExceptionCount() const;

  /// @brief 观测:当前在途(已登记未终结)请求数(≤256);背压 / 关联清理判据。
  [[nodiscard]] std::size_t PendingCount() const;

  /// @brief 观测:Close 时业务队列内未启动、被 Drain 丢弃归因的业务事件累计数(close_drop)。
  [[nodiscard]] std::size_t CloseDropCount() const;

  /// @brief 观测:Close 时 handler 协作取消超 ~500ms 观测阈值(TBD-007)记 kInternal 的累计
  ///        次数;超时不强杀 fiber、仍等其实际退出(RT_LIFECYCLE_006)。
  [[nodiscard]] std::size_t HandlerCancelOverrunCount() const;

  /// @brief 观测:连接断连时,旧连接代际里尚未启动处理的排队业务事件被 Drain 丢弃、归因
  ///        `连接代际隔离丢弃` 的累计数(RT_TCP_RECONNECT 3.1.7.4;仅自动重连传输上有
  ///        reactor 时非零)。区别于 Close 的 close_drop(终态)。
  [[nodiscard]] std::size_t GenerationIsolationDropCount() const;

  /// @brief 观测:最近一次请求从 Register 到终结的时延(P5-4,RT_DATA_BUFFER)。尚无
  ///        已终结请求时为 0。
  [[nodiscard]] Clock::duration LastRequestLatency() const;

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。
  [[nodiscard]] Clock::duration LastHandlerDuration() const;

  /// @brief 观测:最近一次 Close 发起到 Closed 完成的时延(P5-4,RT_DATA_BUFFER)。尚未
  ///        关闭完成时为 0。
  [[nodiscard]] Clock::duration LastCloseLatency() const;

 private:
  friend class HandlerContext;

  /// @brief 校验 config(RT_LIFECYCLE_007);非法返 kConfiguration(停 Created 可重试)。
  [[nodiscard]] Status ValidateConfig() const;

  /// @brief 读循环分发回调体(协议特有):Decode 一帧 → 逐条 Dispatch。runtime 骨架 Read
  ///        成功后调本回调(runtime 不 decode、不分类,守 RT_NODE_003)。
  void DecodeAndDispatch(Datagram datagram);
  /// 单条 Message 的分发(协议特有分类):响应帧 → Resolve;业务帧 → 入队 / 丢弃归因。
  void Dispatch(Message msg);
  /// @brief reactor fiber 体(仅传输为 IConnectionObservable 时 spawn,ADR-0003 D11 Q1③/Q3④):
  ///        WaitStateChange 循环订阅连接状态跃迁;每次代际结束(离开 kConnected)执行代际隔离——
  ///        FailAll(kConnection, 不 latch) 令在途请求恰好一次收敛 + Drain 未启动旧代际业务归因
  ///        `连接代际隔离丢弃`;正在运行的 handler 让其跑完(不强杀);node 保持 Running。Close 时
  ///        经 reactor_cancellation_ 取消,WaitStateChange 返 kCancelled → 干净退出、纳入关闭汇合。
  void RunReactorLoop(IConnectionObservable* observable);

  /**
   * @brief 从空闲集分配一个 session_id(最久释放者优先 = FIFO / 最大退休窗口)。
   *
   * 协议特有语义(uint8=256 空间),内联本类(D10)。可复用:P2-3 noresponse Send 亦
   * 盖 session_id(盖帧后立即释放、不占在途预算)。自持锁。
   *
   * @return 一个空闲 session_id;256 个全在途时返 std::nullopt(调用方据此拒绝发送)。
   */
  [[nodiscard]] std::optional<std::uint8_t> AllocateSession();

  /// @brief 归还一个 session_id 回空闲集尾(push_back → 最大化其复用前的退休窗口)。自持锁。
  void ReleaseSession(std::uint8_t session_id);

  std::unique_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  ProtocolNodeConfig config_;
  PendingTable<ProtocolKey, Message> pending_;
  /// 协议无关运行时机制(组合并驱动,ADR-0003 D10/D12):生命周期状态机 + 三方汇合 +
  /// handler 消费者 fiber + 业务队列 + 读循环骨架。node 内联协议特有语义驱动它。
  NodeRuntime<Message> runtime_;
  /// reactor fiber 协作取消源:Close 时 Cancel,令 WaitStateChange 返 kCancelled 退出(D11)。
  CancellationSource reactor_cancellation_;
  /// reactor fiber 退出通知(finalizer 追加汇合点;仅传输为 IConnectionObservable 时用)。
  SharedCompletion<void> reactor_done_;

  mutable std::mutex mutex_;  ///< 守协议特有交互状态(session 空闲集、协议计数,D8)。
  /// session_id 空闲集(构造时填 0..255);pop_front 分配、push_back 释放 = FIFO 复用。
  std::deque<std::uint8_t> free_sessions_;
  std::size_t unmatched_response_count_{0};
  std::size_t dropped_no_handler_count_{0};
  std::size_t generation_isolation_drop_count_{0};
};

}  // namespace transport
