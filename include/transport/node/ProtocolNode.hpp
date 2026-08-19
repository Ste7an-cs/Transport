#pragma once

/**
 * @file ProtocolNode.hpp
 * @brief 最小外部协议交互节点 ProtocolNode(RT_NODE_003 / RT_REQUEST / ADR-0003 D8/D9)。
 *
 * ProtocolNode 继承 `NodeBase`(生命周期由基类承载幂等与汇合,本类实现 `DoStart` /
 * `DoClose` / `DoJoin` 三个钩子),组合 `ICodec`(线缆格式)、`Dispatcher`(请求与响应的
 * 关联)与可选的 `HandlerLoop`(入站业务处理器),并自持一条读-分发循环
 * (`await → decode → dispatch`),交付一次 needresponse 的请求-响应。
 *
 * **请求与响应的关联由 `Dispatcher` 承担**:发出请求前先按"同会话、同命令码、帧类型为
 * 回应"登记一个订阅,读循环收到消息后交由 `Dispatcher` 按键投递。字段级的部分匹配、
 * 多订阅者同时命中均由其实现,本类不再维护在途请求表,也不再需要把字段压成单一关联键。
 *
 * **不管 transport 的生命周期**:传输由**宿主**创建、`Start()`、`Close()`、`WaitClosed()`,
 * 本节点只按引用借用它:读侧取它的读队列句柄,写侧调它的 `Write()`。链路的绑定、静默超时、重连、退避**全部是传输内部的事**,
 * 节点既不发起也不观测。
 *
 * 因此读侧走 `AsyncRead()->shared()`,**本节点拿自己的一路订阅**:关闭时只 `close` 自己
 * 这一路即可让读循环退出,源队列与其它订阅者不受影响(AsyncTask `shared()` 的语义:
 * 订阅句柄的 `close()` 只终止自己这一路)。这是"节点关闭不等于传输关闭"的实现载体,
 * 也让多个节点能共用一条传输。
 *
 * 写侧调 `transport.AsyncWrite(bytes)`——**fire-and-forget**,入队即完成调用方责任。目的地恒传
 * `Endpoint::Default()`,由传输解析成它自己配置的默认对端:本类传输无关,不知道也不该知道
 * 对端是 ip:port 还是 topic。实际写出的失败不回传,只落传输的 `LastError()`。
 *
 * 协议特有语义——键提取、`frm_type` 盖章、session_id 分配、终结帧判别
 * (kResponse/kResult)、未匹配路由一律内联在本类。
 *
 * session_id 由一个 `std::uint8_t` 计数器**循环递增**给出:每次取用后自增,越过 255 自然
 * 回绕到 0。它只用于区分近期的并发交互,不构成并发上限,故取用不会失败。
 *
 * @warning 在途交互数超过 256 时 session_id 会重复。重复的键意味着两个订阅登记在同一桶
 *          中,一条响应将同时投递给二者。若协议存在此量级的并发,应在键中引入更宽的
 *          区分字段。
 *
 * **无观测接口**:各类丢弃与时延一律只经 `config.trace_sink` 上报(`RecordEvent`),不再
 * 有计数器成员与其 getter。要统计就在 sink 里统计——一个事实一条出口。
 *
 * 与传输、`Dispatcher` 一致,本类面向**单线程 fiber 协作**模型:`Request` 运行于调用方
 * fiber,读-分发循环运行于自持 fiber,二者同线程且仅在挂起点交错,而 session 取用与投递
 * 路径内均无挂起点,故普通成员不加锁。
 */

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "await/awaitable.hpp"
#include "task/fibertask.h"  // Coro::FiberTask —— 读-分发循环的结构化并发句柄。
#include "detail/result.hpp"

#include "transport/codec/ICodec.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/core/Dispatcher.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/node/HandlerLoop.hpp"
#include "transport/node/NodeBase.hpp"

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
  [[nodiscard]] Coro::Result<void> Send(Message msg);

  /// @brief 请求关闭本节点:直接转调节点公开的 `Close()`。
  ///
  /// `NodeBase::Close()` **只发汇合信号、不等待收敛**,不含任何等待点,故在 handler
  /// 自己的 fiber 内调用是安全的——不再需要"内部工作单元只能走某个专用入口"的使用契约
  /// (旧形态的 `SignalClose()` 已随之删除)。
  ///
  /// @return 仅表示**已受理**,不表示已关完。确认收敛完成须由节点**外部** `WaitClosed()`。
  [[nodiscard]] Coro::Result<void> RequestClose();

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
/// 框架不据此自动应答,避 TBD-001);预期失败用 Coro::Result<void> 表达,不抛异常(RT_HANDLER_005)。
using InboundHandler = std::function<Coro::Result<void>(const Message&, HandlerContext&)>;

/// 参与请求-响应关联的字段:会话标识、命令码、帧类型。三者的部分匹配由 `Dispatcher`
/// 实现,本类只需在 `KeyOf` 中给出各字段的具体值。
using MessageDispatcher =
    Dispatcher<Message, std::uint8_t, std::uint16_t, FrameType>;

/// @brief 该请求的回应:同会话、同命令码、帧类型为 `kResponse`。
[[nodiscard]] MessageDispatcher::Key ResponseTo(const Message& request);

/// @brief 指定会话、命令码与帧类型的订阅键。
///
/// 用于一次交互需分段等待多条报文的情形(例如先等回应、再等另一命令码的结果),各段
/// 各自登记、各自设定时限。
[[nodiscard]] MessageDispatcher::Key FrameOf(std::uint8_t session_id,
                                             std::uint16_t message_id,
                                             FrameType type);

/// @brief 任意会话、任意命令码的某类帧,用于旁路监听。
[[nodiscard]] MessageDispatcher::Key AnyOfType(FrameType type);

/// ProtocolNode 配置:默认外部协议 id + 默认请求超时 + 可选入站业务处理器 + 业务队列
/// 容量 + 可选 Trace 出口。
struct ProtocolNodeConfig {
  std::uint8_t protocol_id = 0;
  /**
   * 默认请求总超时(SRS §3.1.4.4):`Request` 未显式给出时限时以本值补齐。节点不接受
   * "永不超时"的请求——链路断开不终结在途请求(重连对交互层透明),写出又是
   * fire-and-forget 而不回传失败,故时限是在途请求唯一的兜底终结源;缺失该终结源的请求
   * 将挂至节点关闭,与 RT_REQUEST_003"每个请求恰好终结一次"冲突。
   *
   * 须为正值,否则 `Start` 返 kConfiguration 并停在 Created(RT_LIFECYCLE_007)。
   */
  std::chrono::milliseconds default_request_timeout{30000};
  /// 入站业务处理器(RT_HANDLER_001);为空 = 业务帧归因 dropped_no_handler 后丢弃。
  InboundHandler handler;
  /// 业务队列容量(事件数;仅 handler 设时用)。0 = 无上限。
  /// **满时静默丢弃队首最旧的事件**——AsyncTask 队列的语义,无计数、无归因(见 #152)。
  std::uint32_t business_queue_capacity = HandlerLoop<Message>::kDefaultCapacity;
  /// 可选 Trace 出口(ADR-0003 D13);非拥有,可为 nullptr。**观测的唯一出口**——本类不再
  /// 有任何计数器与 getter。本类在各丢弃点
  /// (kBadFrame / kUnmatchedOrLateResponse / kNoHandlerConfigured)与 send/recv/decode
  /// 边界上报事件。RT_TRACE_002:为空时不改变任何控制流/字节流/错误结果,仅一次判空。
  ITraceSink* trace_sink = nullptr;
};

/**
 * @brief 最小外部协议交互节点:继承 NodeBase(生命周期)+ 组合 codec + 在途请求表,
 *        借用宿主的 transport,交付请求-响应。
 *
 * **生命周期**:`Start()` / `Close()` / `WaitClosed()` / `IsRunning()` 一律继承自 `NodeBase`,
 * 本类只实现三个钩子。`Close()` 只发信号、任何 fiber 都可调;`WaitClosed()` join 读循环与
 * handler 消费者,返回即可安全析构。
 *
 * **它关的是自己,不是传输**:`DoClose()` 只 close 本节点的读订阅、业务队列与在途请求表,
 * 不触碰 transport。传输的关闭由宿主自己做。
 *
 * **读循环退出即自关**:读循环无论因我方 `Close`(订阅被关)还是因传输终结(源队列被关)
 * 退出,出口处一律调公开的 `Close()`——前者是幂等空操作,后者即"传输没了,节点也该关"。
 * 两条路径合并为一条,不需要判"退出时是否仍 Running"。
 *
 * 不可拷贝、不可移动(读-分发循环 fiber 捕获 this)。
 */
class ProtocolNode : public NodeBase {
 public:
  /// @brief 构造。
  /// @param transport **借用**的传输:宿主负责其 `Start()` / `Close()` / `WaitClosed()`,
  ///                  且须保证其寿命长于本节点。
  /// @param codec     线缆格式(本节点独占)。
  /// @param config    见 ProtocolNodeConfig。
  ProtocolNode(ITransport& transport, std::unique_ptr<ICodec> codec,
               ProtocolNodeConfig config = {});
  /// @brief 析构:`Close()` + `WaitClosed()`——须在**本类**析构体内做,彼时动态类型仍是
  ///        ProtocolNode、虚钩子可用、成员尚存(基类析构不得收敛,见 `NodeBase::~NodeBase`)。
  ~ProtocolNode() override;

  ProtocolNode(const ProtocolNode&) = delete;
  ProtocolNode& operator=(const ProtocolNode&) = delete;

  /**
   * @brief 交付一次 needresponse 请求-响应。
   *
   * 调用方给出 payload 与 message_id;本节点盖 `frm_type=kCommand`、默认 protocol_id,
   * 并取用下一个 session_id,随后**先登记回应订阅、再编码发出**,最后在订阅凭据上等待
   * 唯一响应。请求终结后订阅随凭据析构自动注销。
   *
   * 写出为 fire-and-forget:交给传输即返回,不等待实际发出、也无从得知是否发出成功,
   * 因此 `timeout` 是本调用唯一的兜底终结源,不接受"永不超时"。节点关闭后返 kClosed。
   *
   * @param req     请求 Message(payload + message_id 由调用方填)。
   * @param timeout 本次请求的总超时,自进入本函数起算,涵盖取用 session、登记订阅、编码、
   *                入队与等待响应的全部时间。零值表示套用 `config.default_request_timeout`。
   * @return 匹配到的响应 Message,或机器可判别错误(kClosed / kTimeout / 编码错误等)。
   */
  [[nodiscard]] Coro::Result<Message> Request(Message req,
                                        std::chrono::milliseconds timeout = {});

  /**
   * @brief noresponse fire-and-forget 出站:盖章 + 编码 + 交给传输,不期待应答。
   *
   * 本节点盖 frm_type(调用方给出的业务类型优先,否则取 kCommand)、默认 protocol_id,
   * 并取用下一个 session_id。不登记任何订阅——本调用不期待应答。
   *
   * @param msg 出站 Message(payload + 可选 message_id / frm_type 由调用方填)。
   * @return 已入队,或机器可判别错误(kClosed / 编码错误)。
   *         **返回成功不表示已发出**——实际写出与其失败归因都在传输的写泵里。
   */
  [[nodiscard]] Coro::Result<void> Send(Message msg);

  /**
   * @brief 登记一个订阅,用于分段交互与旁路监听。
   *
   * 一次交互需要等待多条报文时(例如先等回应、再等另一命令码的结果),各段各自登记、
   * 各自设定时限。**登记须先于请求发出**,否则先到的报文因无订阅而被丢弃。
   *
   * @param key 订阅键;不参与匹配的字段填 `kAny`。见 `ResponseTo` / `FrameOf` /
   *            `AnyOfType` 三个具名工厂。
   * @return 订阅凭据,析构时自动注销。
   */
  [[nodiscard]] MessageDispatcher::Ticket Subscribe(MessageDispatcher::Key key);

 protected:
  // —— NodeBase 生命周期钩子 ————————————————————————————————————————————

  /// @brief 启动的协议特有实事:校验 config → 取读订阅 → spawn 读-分发循环 +
  ///        (设了 handler 时)handler 消费者。**不启动 transport**(那是宿主的事)。
  ///        配置非法返 kConfiguration,基类退回 Created 允许改配重试(此时未 spawn)。
  Coro::Result<void> DoStart() override;

  /// @brief 关闭汇合信号(首个关闭者独占执行一次,只发信号、不等待):close 本节点的读
  ///        订阅(读循环据此退出)→ 业务队列 Close + handler 协作取消 →
  ///        `Dispatcher::CloseAll`(令在途请求恰好终结一次)。不关闭 transport。
  Coro::Result<void> DoClose() override;

  /// @brief join 本节点 spawn 的全部 fiber:读-分发循环 + handler 消费者。
  void DoJoin() override;

 private:
  friend class HandlerContext;

  /// @brief 校验 config(RT_LIFECYCLE_007):`default_request_timeout` 须为正值。非法返
  ///        kConfiguration,节点停在 Created,允许宿主改配后重试。由 `DoStart()` 开头调用。
  [[nodiscard]] Coro::Result<void> ValidateConfig() const;

  /**
   * @brief spawn 读-分发循环 fiber。
   *
   * ```
   * await(rx_) → 成功 → DecodeAndDispatch(解码 + 协议特有分类/寻址)
   *            → 错误 → 退出循环 → Close()
   * ```
   *
   * 等待器给出错误只有两种成因:我方 `Close` 关了本节点的订阅,或传输终结关了源队列。
   * 二者都该让节点关闭,故出口处无条件调公开的 `Close()`——前者幂等空操作,后者即自终。
   * 可继续的瞬时错误由传输内部的泵就地消化,不出现在本句柄上。
   */
  void SpawnReadLoop();

  /// @brief 读循环体内的协议特有处理:Decode 一帧 → 逐条 Dispatch。
  void DecodeAndDispatch(Datagram datagram);
  /// 单条 Message 的分发(协议特有分类):响应帧 → Resolve;业务帧 → 入队 / 丢弃归因。
  void Dispatch(Message msg);
  /// @brief 编码 + 交给传输(Request / Send 共用的出站尾段)。
  [[nodiscard]] Coro::Result<void> EncodeAndWrite(const Message& msg);

  /// @brief 取用下一个 session_id:自增计数器,`std::uint8_t` 自然回绕即 0..255 循环。
  ///        取用不会失败,故不返回错误。
  [[nodiscard]] std::uint8_t NextSession();

  ITransport& transport_;  ///< **借用**:宿主拥有并启停,寿命须长于本节点。
  std::unique_ptr<ICodec> codec_;
  ProtocolNodeConfig config_;
  /// 入站业务处理器消费者小件(**可选**件):业务队列 + 消费者 fiber 句柄 + 协作取消 +
  /// 异常隔离。未设 `config_.handler`(即未 `Spawn`)时它只是个没人消费的空队列。
  HandlerLoop<Message> handler_loop_;

  /// 本节点在传输 `read_queue` 上的独立订阅(`shared()`):关闭它只终止本节点这一路,
  /// 源队列与其它订阅者不受影响。`DoStart` 建立,`DoClose` 关闭。
  std::shared_ptr<Coro::Awaitable<Datagram>> rx_;
  /// 读-分发循环的结构化并发句柄;`DoJoin()` 让出式 join 之。
  std::shared_ptr<Coro::FiberTask<void>> read_task_;

  /// 请求与响应的关联:发出请求前登记订阅,读循环收到消息后按键投递。
  MessageDispatcher dispatcher_;

  /// 下一个待取用的 session_id;每次取用后自增,越过 255 自然回绕。
  std::uint8_t next_session_{0};
};

}  // namespace transport
