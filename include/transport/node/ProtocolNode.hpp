#pragma once

/**
 * @file ProtocolNode.hpp
 * @brief 最小外部协议交互节点 ProtocolNode(RT_NODE_003 / RT_REQUEST / ADR-0003 D8/D9)。
 *
 * ProtocolNode **继承 `NodeBase`**(生命周期模板方法:基类管幂等与收敛,本类实现
 * `DoStart`/`DoClose` 等钩子;ADR-0006 D1)并**组合**(不共享交互引擎)`ITransport`
 * (纯字节管道)+ `ICodec`(线缆格式)+ `PendingTable`(挂起-应答薄基座)+ 可选的
 * `HandlerLoop`(handler 消费者小件,ADR-0006 D4),**自持**一条读-分发循环
 * (ADR-0006 D5:读循环是 node 自己的 `Read → decode → dispatch`,不属任何共享机制),
 * 交付一次 needresponse 的请求-响应。协议特有语义——键派生、frm_type
 * 盖章、session_id 空闲集
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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>

#include "transport/node/BoundedQueue.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/node/HandlerLoop.hpp"
#include "transport/codec/ICodec.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/Message.hpp"
#include "transport/node/NodeBase.hpp"
#include "transport/node/PendingTable.hpp"
#include "transport/core/Result.hpp"
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
   * @brief 请求关闭本节点:**只发起、不等待**(RT_LIFECYCLE_005 / ADR-0006 D8)。
   *
   * 走框架的发信号路径(`NodeBase::SignalClose`):置
   * Closing(立即拒新交互)+ 发出全部汇合信号(RequestClose 传输 + Close 业务队列 + 触发
   * handler 取消 + FailAll),随即返回;**不**调会等待的 `Close()`。节点由**读循环**在其
   * 自身退出且 handler 也退出后收敛到 Closed(ADR-0005 D1)。
   * 命名与 `ITransport::RequestClose()`(发信号)/ `WaitClosed()`(等待)的既有约定一致。
   *
   * @return 仅表示**已受理**(关闭已发起或此前已发起/已完成),**不表示已关完**。处理器
   *         内**不得**等待本节点关闭完成(等待收敛 = 等自己退出,静默挂死;框架不设运行时
   *         守卫,ADR-0006 D8)——需要确认关闭完成只能由节点**外部** `WaitClosed()`,或经
   *         可观测状态旁路观察。
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

/// ProtocolNode 配置:关联键策略 + 默认外部协议 id + 默认请求超时 + 可选入站业务处理器
/// + 业务队列上界。
struct ProtocolNodeConfig {
  CorrelationKeyStrategy key_strategy = DefaultProtocolKeyStrategy();
  std::uint8_t protocol_id = 0;
  /**
   * 默认请求总超时(SRS §3.1.4.4 总超时缺省值,ADR-0004 D3):调用方 `Request` 未显式给出
   * `options.deadline` 时,节点以 `now + default_request_timeout` 补齐——**节点不得接受
   * "永不超时"的请求**。
   *
   * 存在理由:链路断开**不再终结在途请求**(RT_TCP_RECONNECT_002 改写——代际隔离已撤销,
   * 重连对交互层完全透明),故断链后一个无 deadline 的请求将失去全部终结源、一直挂到节点
   * 关闭,与 RT_REQUEST_003"每个请求恰好终结一次"冲突。总超时缺省值即该场景下的兜底终结源。
   *
   * 须为正值,否则 `Start` 返 kConfiguration 并停在 Created(RT_LIFECYCLE_007)。
   */
  OperationOptions::Clock::duration default_request_timeout = std::chrono::seconds(30);
  /// 入站业务处理器(RT_HANDLER_001);为空 = P1 行为(业务帧归因 dropped_no_handler)。
  InboundHandler handler;
  /// 业务队列事件数上界(仅 handler 设时用;越界由 BoundedQueue 钳制)。
  std::size_t business_queue_max_events = BoundedQueue<Message>::kDefaultMaxEvents;
  /// 业务队列字节数上界(仅 handler 设时用;越界由 BoundedQueue 钳制)。
  std::size_t business_queue_max_bytes = BoundedQueue<Message>::kDefaultMaxBytes;
  /// 可选 Trace 出口(P5-3/P5-4,ADR-0003 D13);非拥有,可为 nullptr。传给 NodeBase
  /// (生命周期跃迁 + close_drop 归因)、HandlerLoop 业务队列与本类各丢弃归因点
  /// (kBadFrame / kUnmatchedOrLateResponse /
  /// kNoHandlerConfigured),并在 send/recv/decode/match/timeout/cancel/handler/close
  /// 等边界点上报事件。RT_TRACE_002:为空时不改变任何控制流/字节流/错误结果/计数,
  /// `RecordEvent`/`RecordDrop` 仅一次判空。
  ITraceSink* trace_sink = nullptr;
};

/**
 * @brief 最小外部协议交互节点:继承 NodeBase(生命周期)+ 组合 transport + codec +
 *        PendingTable,交付请求-响应。
 *
 * **生命周期全部由 `NodeBase` 承载**(ADR-0006 D1/D6):`Start()` / `Close()` /
 * `WaitClosed()` / `IsRunning()` / `CloseDropCount()` / `LastCloseLatency()` 一律**继承自
 * 基类**,本类只实现协议特有的钩子(`ValidateConfig` / `DoStart` / `DoClose` /
 * `JoinHandler` / `DrainUnstartedBusiness`)。Created→Running(Start)→Closing→Closed(Close)。
 * - `Close()` 只发汇合信号 + 等收敛结果;收敛由读循环兼任(ADR-0005 D1)。关闭后
 *   Request/Send 一律 kClosed。**须由节点外部调用**(RT_LIFECYCLE_005 使用契约,
 *   ADR-0006 D8):它**会等**收敛完成,在 handler 内调用等于等自己退出 → 静默挂死;框架不设
 *   运行时重入守卫。处理器请求关闭走只发信号的 `HandlerContext::RequestClose()`。
 * - **致命错误自终(ADR-0005 D5 / RT_LIFECYCLE_008)**:不具重连能力的传输(UDP / 串口 /
 *   TCP 服务端已接受连接)发生底层致命错误时,读循环退出而节点仍 Running,此时由读循环
 *   **自行**走同一条关闭路径(置 Closing + 发同一组汇合信号 + 收敛),宿主无需干预;其可
 *   观察结果与外部发起关闭一致(SRS §3.1.6.3 第 7 条)。TCP 客户端无限重连,不自终。
 *
 * 不可拷贝、不可移动(读-分发循环 fiber 捕获 this)。
 */
class ProtocolNode : public NodeBase {
 public:
  using Clock = OperationOptions::Clock;

  ProtocolNode(std::unique_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
               ProtocolNodeConfig config = {});
  /// @brief 析构即关闭:在**本类**析构体内调 `Close()`——彼时动态类型仍是 ProtocolNode,
  ///        虚钩子可用、成员尚存(基类析构不得收敛,见 `NodeBase::~NodeBase`)。
  ~ProtocolNode() override;

  ProtocolNode(const ProtocolNode&) = delete;
  ProtocolNode& operator=(const ProtocolNode&) = delete;

  /**
   * @brief 交付一次 needresponse 请求-响应。
   *
   * 调用方给 payload + message_id;node 盖 frm_type=kCommand、默认 protocol_id、从空闲集
   * LRU 分配 session_id;request_key → PendingTable.Register(重复键 kInvalidState 透传)→
   * Encode → transport.Write → Handle::Wait 等唯一响应,终结后释放 session_id 回空闲集。
   * 256 个 session_id 全在途时发送前返 kResourceExhausted(不登记、不发送)。总超时经
   * options.deadline(从节点接受请求起算);**调用方未给 deadline 时,节点在接受请求处补
   * `now + config.default_request_timeout`**(SRS §3.1.4.4 总超时缺省值)——链路断开不再终结
   * 在途请求(ADR-0004 D3),无此缺省则该请求会一直挂到节点关闭。调用方**显式给出的
   * deadline 一律照用,不被缺省覆盖**。关闭后返 kClosed。
   *
   * @param req     请求 Message(payload + message_id 由调用方填)。
   * @param options 截止时间(缺省时套用节点默认请求超时)与取消令牌。
   * @return 匹配响应 Message,或机器可判别错误(kClosed / kResourceExhausted /
   *         kInvalidState / kTimeout / …)。
   */
  [[nodiscard]] Result<Message> Request(Message req, OperationOptions options = {});

  /**
   * @brief noresponse fire-and-forget 出站:盖章 + 编码 + 写出,不期待应答。
   *
   * node 盖 frm_type(调用方给的业务类型优先,否则默认 kCommand)、默认 protocol_id、
   * session_id 取**空闲集尾部(最新释放者)只读盖帧**(#98:不出队、不登记 PendingTable、
   * 不占 256 在途预算,也不扰动 Request 的 FIFO 退休窗口——RT_REQUEST_005 的迟到误配
   * 防护不被高频 Send 削弱)。编码后经 transport.Write 上线,遵 RT_TRANSPORT_008 背压。
   * 关闭后返 kClosed;256 个 session_id 全在途(空闲集空)时返 kResourceExhausted
   * (边界策略:与 Request 一致地拒绝——此时任何可盖的 id 都正被某在途请求占用,盖上
   * 即有误配面)。
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

  /// @brief 观测:读循环单次 `codec_->Decode` 调用返回错误(坏帧 / codec 语义错误,该次
  ///        收到的整段字节判为不可解析而整体丢弃)的累计次数(P5-3,ADR-0003 D13;命名
  ///        归因 kBadFrame)。
  [[nodiscard]] std::size_t BadFrameCount() const;

  /// @brief 观测:最近一次请求从 Register 到终结的时延(P5-4,RT_DATA_BUFFER)。尚无
  ///        已终结请求时为 0。
  [[nodiscard]] Clock::duration LastRequestLatency() const;

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。
  [[nodiscard]] Clock::duration LastHandlerDuration() const;

 protected:
  // —— NodeBase 生命周期钩子(协议特有实事,ADR-0006 D1)————————————————————

  /// @brief 校验 config(RT_LIFECYCLE_007):队列上界落 [1,65536] / [64KiB,256MiB]、
  ///        key_strategy 两支非空、default_request_timeout 为正。非法返 kConfiguration
  ///        (停 Created、不 latch start_done_、允许改配重试)。
  Status ValidateConfig() const override;

  /// @brief 首个 Start 的实事:transport.Start → `MarkRunning()` → spawn 读-分发循环 +
  ///        (设了 handler 时)handler 消费者。**无能力探测、无按介质分支的第二条启动
  ///        路径**(ADR-0004 D2)。传输启动失败即原样返回,基类退回 Created 允许重试。
  Status DoStart() override;

  /// @brief 关闭汇合信号(首个关闭者独占执行一次,`Close` 与致命错误自终共用):按序
  ///        transport.RequestClose → 业务队列 Close + handler 协作取消 →
  ///        PendingTable.FailAll(kClosed)(令在途请求恰好一次收敛)。
  ///        断链**不是**收敛信号(ADR-0004 D3:在途请求只由总超时/取消/关闭终结)。
  Status DoClose() override;

  /// @brief 收敛:让出式 join handler 消费者 fiber(未设 handler 时立即返回)。
  void JoinHandler() override;

  /// @brief 收敛:Drain 业务队列内未启动的排队业务,返回条数(归因 close_drop 在基类)。
  std::size_t DrainUnstartedBusiness() override;

 private:
  friend class HandlerContext;

  /**
   * @brief spawn 读-分发循环 fiber(ADR-0006 D5:骨架归本类)。
   *
   * **三介质同一段读循环、无介质分支、无能力探测**(ADR-0004 D1 / RT_TRANSPORT_008):
   *
   * ```
   * Read() → 成功    → DecodeAndDispatch(解码 + 协议特有分类/寻址)
   *        → kClosed → 退出读循环
   *        → 其它     → 瞬时错误,继续
   * ```
   *
   * `kClosed` 是**唯一**的传输终结信号(我方关闭,或不具重连能力的传输发生底层致命错误);
   * 其余失败一律视为可继续的瞬时错误。具备自动重连的传输在内部透明处理链路中断——`Read`
   * 在重连期间挂起、重连后于新链路继续交付,读循环**看不到任何链路中断事件**,故此处不再
   * 有 `kConnection` 分支。
   *
   * **退出后本 fiber 兼任收敛者**(ADR-0005 D1):两条内部工作单元中读循环恒是第一个退出
   * 的,故它天然是收敛的正确位置——无需独立 finalizer fiber,也无人再等"读循环已退出"这
   * 一事件。收敛本身在基类内(ADR-0006 D6),本方法只在循环出口调基类的
   * `ConvergeAfterReadLoop()`;**不得**改调公开的 `Close()`(那会等自己退出)。
   */
  void SpawnReadLoop();

  /// @brief 读循环体内的协议特有处理:Decode 一帧 → 逐条 Dispatch(读循环骨架本身不
  ///        decode、不分类,守 RT_NODE_003)。
  void DecodeAndDispatch(Datagram datagram);
  /// 单条 Message 的分发(协议特有分类):响应帧 → Resolve;业务帧 → 入队 / 丢弃归因。
  void Dispatch(Message msg);

  /**
   * @brief 从空闲集分配一个 session_id(最久释放者优先 = FIFO / 最大退休窗口)。
   *
   * 协议特有语义(uint8=256 空间),内联本类(D10)。仅 Request 独占分配;noresponse
   * Send 改走 PeekIdleSession 只读盖帧(#98,不出队)。自持锁。
   *
   * @return 一个空闲 session_id;256 个全在途时返 std::nullopt(调用方据此拒绝发送)。
   */
  [[nodiscard]] std::optional<std::uint8_t> AllocateSession();

  /// @brief 归还一个 session_id 回空闲集尾(push_back → 最大化其复用前的退休窗口)。自持锁。
  void ReleaseSession(std::uint8_t session_id);

  /// @brief 只读取空闲集尾部(最新释放者)供 Send 盖帧(#98):不出队、不占在途预算、
  ///        不扰动 FIFO 序;取尾不取头——头部即将被下一个 Request 独占,尾部距离被复用
  ///        最远,误配面最小。空闲集空返 std::nullopt。自持锁。
  [[nodiscard]] std::optional<std::uint8_t> PeekIdleSession() const;

  /**
   * @brief RAII session 租约(#98):构造接管一个已分配的 session_id,析构自动归还空闲集。
   *
   * Request 全部返回路径(登记冲突 / 编码失败 / 写失败 / 正常终结)统一经析构归还,
   * 消除手工 ReleaseSession 纪律——漏一处即慢性泄漏,终致假 kResourceExhausted。
   */
  class SessionLease {
   public:
    SessionLease(ProtocolNode* node, std::uint8_t id) : node_(node), id_(id) {}
    ~SessionLease() { node_->ReleaseSession(id_); }
    SessionLease(const SessionLease&) = delete;
    SessionLease& operator=(const SessionLease&) = delete;

    [[nodiscard]] std::uint8_t id() const { return id_; }

   private:
    ProtocolNode* node_;
    std::uint8_t id_;
  };

  std::unique_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  ProtocolNodeConfig config_;
  PendingTable<ProtocolKey, Message> pending_;
  /// 入站业务处理器消费者小件(**可选**件,ADR-0006 D4):业务队列 + 消费者 fiber 句柄 +
  /// 协作取消 + 异常隔离 + 时长计量。未设 `config_.handler`(即未 `Spawn`)时它只是个没人
  /// 消费的空队列。自守其锁,不进基类(基类只装每个节点都有的东西)。
  HandlerLoop<Message> handler_loop_;

  mutable std::mutex mutex_;  ///< 守协议特有交互状态(session 空闲集、协议计数,D8)。
  /// session_id 空闲集(构造时填 0..255);pop_front 分配、push_back 释放 = FIFO 复用。
  std::deque<std::uint8_t> free_sessions_;
  std::size_t unmatched_response_count_{0};
  std::size_t dropped_no_handler_count_{0};
  std::size_t bad_frame_count_{0};
};

}  // namespace transport
