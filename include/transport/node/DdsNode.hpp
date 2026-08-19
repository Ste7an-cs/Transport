#pragma once

/**
 * @file DdsNode.hpp
 * @brief DDS 交互节点 DdsNode(pub-sub + 多路请求-应答;RT_NODE_003 / RT_REQUEST /
 *        RT_IF_DDS / ADR-0003 D10/D12 Q2)。
 *
 * DdsNode **继承 `NodeBase`**(生命周期模板方法:基类管幂等与收敛,本类实现
 * `DoStart`/`DoClose` 等钩子;ADR-0006 D1)并**组合**(不共享交互引擎)
 * `DdsTransport`(P4-4 纯字节管道,经 ITransport)+ `DdsCodec`(线缆格式,经 ICodec)+
 * `PendingTable<std::string, Message>`(挂起-应答薄基座)+ 可选的 `HandlerLoop`
 * (handler 消费者小件,ADR-0006 D4),**自持**一条读-分发循环(ADR-0006 D5),交付 DDS
 * 语义的多路请求-应答与单向发布订阅。
 *
 * **D10 可复用性实证**:关联键此处是 **correlation_id 字符串**——`PendingTable<Key,T>`
 * 一行不改,只把 Key 实例化为 std::string(P1 曾实例化为 uint32);`BoundedQueue` /
 * `NodeBase` / `HandlerLoop` 零改动复用。DDS 特有语义——correlation_id 生成、`kReply`
 * 终结判别、topic 寻址、reply_to=inbox——**全部内联本类**;基座保持协议无关
 * (RT_DESIGN_008 红线)。
 *
 * **无连接**(D3′):无连接状态机 / 重连;底层 provider 致命 → 传输 Read 返 kClosed
 * (ADR-0004 D1 单一终止语义)→ 读循环退出 → Closing→Closed。判活(Liveliness/
 * Deadline QoS 或心跳超时,RT_NODE_006)归协议层,P4 不强做、留占位。
 *
 * 交互状态(correlation_id 计数器、生命周期、观测计数器)由一把 std::mutex 守(D8);单
 * fiber 调度器、无 affinity(D8/Q9)。
 */

#include "detail/result.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include "transport/node/BoundedQueue.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/core/Endpoint.hpp"
#include "transport/node/HandlerLoop.hpp"
#include "transport/codec/ICodec.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/NodeBase.hpp"
#include "transport/node/PendingTable.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

class DdsNode;

/**
 * @brief 入站业务处理器的能力面(RT_HANDLER_001):handler 经它与节点交互,而非裸捕获
 *        node&。只露请求-应答所需能力,协议内部状态不外泄。
 *
 * 由 DdsNode 在消费者 fiber 内构造并按引用传入 handler;handler 不得持有其地址越出单次
 * 调用(生命周期系于该次分发)。
 */
class DdsHandlerContext {
 public:
  /**
   * @brief 对一条入站 `kRequest` 回送终结应答(pub-sub 上的请求-应答闭环)。
   *
   * 内联 DDS 应答寻址:盖 `kind=kReply`、`correlation_id=request.correlation_id`,发往
   * `Endpoint::Topic(request.reply_to)`(请求方 inbox)。request 非 `kRequest` 或其
   * reply_to 为空时返 kInvalidArgument(无从回送)。
   */
  [[nodiscard]] Coro::Result<void> Reply(const Message& request, Message reply);

  /// @brief 从本节点单向发布一条 `kNotify`(fire-and-forget);委托到 DdsNode::Publish。
  [[nodiscard]] Coro::Result<void> Publish(Message msg, Endpoint topic);

  /**
   * @brief 请求关闭本节点:**只发起、不等待**(RT_LIFECYCLE_005 / ADR-0006 D8)。
   *
   * 走框架的发信号路径(`NodeBase::SignalClose`):置 Closing
   * (立即拒新交互)+ 发出全部汇合信号,随即返回;**不**调会等待的 `Close()`。节点由读循环
   * 在汇合完成后收敛到 Closed(ADR-0005 D1)。命名与 `ITransport::RequestClose()`(发信号)
   * / `WaitClosed()`(等待)的既有约定一致。
   *
   * @return 仅表示**已受理**,**不表示已关完**。处理器内不得等待本节点关闭完成(等自己
   *         退出,静默挂死;框架不设运行时守卫,ADR-0006 D8)。
   */
  Coro::Result<void> RequestClose();

  /// @brief 节点所属执行域的协作取消令牌(Close 时被触发);handler 可据它提前收手。
  [[nodiscard]] const CancellationToken& cancellation() const {
    return cancellation_;
  }

 private:
  friend class DdsNode;
  DdsHandlerContext(DdsNode* node, CancellationToken cancellation)
      : node_(node), cancellation_(std::move(cancellation)) {}

  DdsNode* node_;
  CancellationToken cancellation_;
};

/// 入站业务处理器(组合注入,RT_HANDLER_001):对一条入站业务消息(kRequest/kNotify/…)
/// 返回结构化结果(仅记录,框架不据此自动应答,避 TBD-001);预期失败用 Coro::Result<void> 表达,不抛
/// 异常(RT_HANDLER_005)。回送应答由 handler 经 DdsHandlerContext::Reply 显式发起。
using DdsInboundHandler = std::function<Coro::Result<void>(const Message&, DdsHandlerContext&)>;

/// DdsNode 配置:inbox topic(reply_to)+ 节点标识(correlation_id 前缀)+ 可选入站
/// 业务处理器 + 业务队列上界。
struct DdsNodeConfig {
  /// 本节点 inbox topic:Request 盖入 reply_to,对端 kReply 回送至此。须非空且已在底层
  /// DdsTransport 的订阅 topic 集内(否则收不到应答)。
  std::string inbox_topic;
  /// 节点标识:确定性 correlation_id 的前缀(`node_id:序号`),须非空且集群内唯一以免
  /// 跨节点键碰撞。不用随机数(确定性可测,RT_REQUEST)。
  std::string node_id;
  /// 入站业务处理器(RT_HANDLER_001);为空 = 业务消息归因 dropped_no_handler。
  DdsInboundHandler handler;
  /// 业务队列事件数上界(仅 handler 设时用;越界由 BoundedQueue 钳制)。
  std::size_t business_queue_max_events = BoundedQueue<Message>::kDefaultMaxEvents;
  /// 业务队列字节数上界(仅 handler 设时用;越界由 BoundedQueue 钳制)。
  std::size_t business_queue_max_bytes = BoundedQueue<Message>::kDefaultMaxBytes;
  /// 可选 Trace 出口(P5-3/P5-4,ADR-0003 D13);非拥有,可为 nullptr。传给 NodeBase
  /// (生命周期跃迁 + close_drop 归因)、HandlerLoop 业务队列与本类各丢弃归因点
  /// (kBadFrame/kUnmatchedOrLateResponse/kNoHandlerConfigured),
  /// 并在 send/recv/decode/match/timeout/cancel/handler/close 等边界点上报事件。
  /// RT_TRACE_002:为空时不改变任何控制流/字节流/错误结果/计数,`RecordEvent`/`RecordDrop`
  /// 仅一次判空。
  ITraceSink* trace_sink = nullptr;
};

/**
 * @brief DDS 交互节点:继承 NodeBase(生命周期)+ 组合 transport + codec + PendingTable +
 *        HandlerLoop,交付 DDS pub-sub 与多路请求-应答。
 *
 * **生命周期全部由 `NodeBase` 承载**(ADR-0006 D1/D6):`Start()` / `Close()` /
 * `WaitClosed()` / `IsRunning()` / `CloseDropCount()` / `LastCloseLatency()` 一律**继承自
 * 基类**,本类只实现 DDS 特有的钩子(`ValidateConfig` / `DoStart` / `DoClose` /
 * `JoinHandler` / `DrainUnstartedBusiness`)。Created→Running(Start)→Closing→Closed(Close)。
 * - `Close()` 只发汇合信号 + 等收敛结果;收敛由读循环兼任(ADR-0005 D1)。关闭后
 *   Request/Publish 一律 kClosed。**须由节点外部调用**(RT_LIFECYCLE_005 使用契约,
 *   ADR-0006 D8):它**会等**收敛完成,在 handler 内调用等于等自己退出 → 静默挂死;框架不设
 *   运行时重入守卫。处理器请求关闭走只发信号的 `DdsHandlerContext::RequestClose()`。
 * - **致命错误自终(ADR-0005 D5 / RT_LIFECYCLE_008)**:DDS 非重连(断开即致命,Read 返
 *   kClosed),此时读循环退出而节点仍 Running → 由读循环**自行**走同一条关闭路径,宿主
 *   无需干预;可观察结果与外部发起关闭一致。
 *
 * 不可拷贝、不可移动(读-分发循环 fiber 捕获 this)。
 */
class DdsNode : public NodeBase {
 public:
  using Clock = OperationOptions::Clock;

  DdsNode(std::unique_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
          DdsNodeConfig config);
  /// @brief 析构即关闭:在**本类**析构体内调 `Close()`——彼时动态类型仍是 DdsNode,虚钩子
  ///        可用、成员尚存(基类析构不得收敛,见 `NodeBase::~NodeBase`)。
  ~DdsNode() override;

  DdsNode(const DdsNode&) = delete;
  DdsNode& operator=(const DdsNode&) = delete;

  /**
   * @brief 交付一次请求-应答(多路并发在途,各自 correlation_id 互不串)。
   *
   * node 盖 `kind=kRequest`、生成确定性 correlation_id(`node_id:序号`)、`reply_to=inbox`;
   * PendingTable.Register(correlation_id) → Encode → transport.Write(destination=@p target)
   * → Handle::Wait 等 inbox 上匹配 correlation_id 的唯一 `kReply`。总超时经 options.deadline。
   * 关闭后返 kClosed。
   *
   * @param req     请求 Message(payload 由调用方填;kind/correlation_id/reply_to 由 node 盖)。
   * @param target  目标 topic(须为 `Endpoint::Topic`,否则传输返 kInvalidArgument)。
   * @param options 截止时间与取消令牌。
   * @return 匹配的 kReply Message,或机器可判别错误(kClosed / kTimeout / kCancelled / …)。
   */
  [[nodiscard]] Coro::Result<Message> Request(Message req, Endpoint target,
                                        OperationOptions options = {});

  /**
   * @brief 单向发布一条 `kNotify`(fire-and-forget,pub-sub):不登记 PendingTable、不期待应答。
   *
   * node 盖 `kind=kNotify`、Encode、transport.Write(destination=@p topic)。关闭后返 kClosed;
   * 非 topic 目的地由传输返 kInvalidArgument。
   *
   * @param msg   出站 Message(payload 由调用方填)。
   * @param topic 目标 topic(须为 `Endpoint::Topic`)。
   */
  [[nodiscard]] Coro::Result<void> Publish(Message msg, Endpoint topic);

  /// @brief 观测:`kReply` 无匹配在途 Request(迟到 / 无匹配 correlation_id)而归因丢弃的累计数。
  [[nodiscard]] std::size_t UnmatchedReplyCount() const;

  /// @brief 观测:入站业务消息因无 handler 而被丢弃的累计次数。
  [[nodiscard]] std::size_t DroppedNoHandlerCount() const;

  /// @brief 观测:业务队列满而 tail-drop 的累计次数(命名归因 business_queue_overflow)。
  [[nodiscard]] std::size_t BusinessQueueOverflowCount() const;

  /// @brief 观测:handler 逃逸异常被边界兜住、转 kInternal 隔离的累计次数(RT_HANDLER_006)。
  [[nodiscard]] std::size_t HandlerExceptionCount() const;

  /// @brief 观测:当前在途(已登记未终结)Request 数;关联清理判据。
  [[nodiscard]] std::size_t PendingCount() const;

  /// @brief 观测:读循环 `codec_->Decode` 失败(坏 sample / codec 语义错误)而丢弃的累计
  ///        次数(P5-3,ADR-0003 D13;命名归因 kBadFrame)。
  [[nodiscard]] std::size_t BadFrameCount() const;

  /// @brief 观测:最近一次 Request 从 Register 到终结的时延(P5-4,RT_DATA_BUFFER)。尚无
  ///        已终结请求时为 0。
  [[nodiscard]] Clock::duration LastRequestLatency() const;

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。
  [[nodiscard]] Clock::duration LastHandlerDuration() const;

 protected:
  // —— NodeBase 生命周期钩子(DDS 特有实事,ADR-0006 D1)——————————————————————

  /// @brief 校验 config(RT_LIFECYCLE_007):inbox_topic / node_id 非空、队列上界落法定
  ///        区间。非法返 kConfiguration(停 Created、不 latch start_done_、可改配重试)。
  Coro::Result<void> ValidateConfig() const override;

  /// @brief 首个 Start 的实事:transport.Start(Init + 订阅 topic 集)→ `MarkRunning()` →
  ///        spawn 读-分发循环 +(设了 handler 时)handler 消费者。**无连接**:不 spawn
  ///        reactor(D3′)。三介质(含 DDS)共用同一段无分支读循环(ADR-0004 D1/D2)。
  Coro::Result<void> DoStart() override;

  /// @brief 关闭汇合信号(首个关闭者独占执行一次,`Close` 与致命错误自终共用):按序
  ///        transport.RequestClose → 业务队列 Close + handler 协作取消 →
  ///        PendingTable.FailAll(kClosed)(令在途 Request 恰好一次 kClosed 收敛)。
  ///        无 reactor(无连接),故无额外取消源。
  Coro::Result<void> DoClose() override;

  /// @brief 收敛:让出式 join handler 消费者 fiber(未设 handler 时立即返回)。
  void JoinHandler() override;

  /// @brief 收敛:Drain 业务队列内未启动的排队业务,返回条数(归因 close_drop 在基类)。
  std::size_t DrainUnstartedBusiness() override;

 private:
  friend class DdsHandlerContext;

  /**
   * @brief spawn 读-分发循环 fiber(ADR-0006 D5:骨架归本类)。
   *
   * **三介质同一段读循环、无介质分支、无能力探测**(ADR-0004 D1 / RT_TRANSPORT_008):
   *
   * ```
   * Read() → 成功    → DecodeAndDispatch(解码 + DDS 特有分类/寻址)
   *        → kClosed → 退出读循环
   *        → 其它     → 瞬时错误,继续
   * ```
   *
   * `kClosed` 是**唯一**的传输终结信号(我方关闭,或 DDS provider 底层致命错误——DDS 无
   * 连接、断开即致命);其余失败一律视为可继续的瞬时错误。
   *
   * **退出后本 fiber 兼任收敛者**(ADR-0005 D1):读循环恒是第一个退出的内部工作单元,故
   * 它天然是收敛的正确位置。收敛本身在基类内(ADR-0006 D6),本方法只在循环出口调基类的
   * `ConvergeAfterReadLoop()`;**不得**改调公开的 `Close()`(那会等自己退出)。
   */
  void SpawnReadLoop();

  /// @brief 读循环体内的 DDS 特有处理:Decode 一 sample → 填 source/topic → 逐条 Dispatch
  ///        (读循环骨架本身不 decode、不分类,守 RT_NODE_003)。
  void DecodeAndDispatch(Datagram datagram);
  /// @brief 单条 Message 的分发(DDS 特有分类,内联):`kReply` → correlation_id 键 Resolve;
  ///        其它(kRequest/kNotify/…)业务 → 入队 handler / 无 handler 归因丢弃。
  void Dispatch(Message msg);

  /// @brief 盖 kind、Encode、transport.Write 到 @p dest 的内部收口(Request/Publish/Reply 共用)。
  [[nodiscard]] Coro::Result<void> WriteFramed(Message msg, MessageKind kind, Endpoint dest);

  /// @brief 生成确定性 correlation_id(`node_id:序号`,单调递增)。自持锁。
  [[nodiscard]] std::string NextCorrelationId();

  std::unique_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  DdsNodeConfig config_;
  /// 关联表:correlation_id 字符串键(D10 实证——PendingTable 一行不改,仅换 Key 类型)。
  PendingTable<std::string, Message> pending_;
  /// 入站业务处理器消费者小件(**可选**件,ADR-0006 D4):业务队列 + 消费者 fiber 句柄 +
  /// 协作取消 + 异常隔离 + 时长计量。未设 `config_.handler`(即未 `Spawn`)时它只是个没人
  /// 消费的空队列。自守其锁,不进基类(基类只装每个节点都有的东西)。
  HandlerLoop<Message> handler_loop_;

  mutable std::mutex mutex_;  ///< 守 DDS 特有交互状态(correlation 计数、观测计数,D8)。
  std::uint64_t correlation_counter_{0};  ///< 确定性 correlation_id 单调序号。
  std::size_t unmatched_reply_count_{0};
  std::size_t dropped_no_handler_count_{0};
  std::size_t bad_frame_count_{0};
};

}  // namespace transport
