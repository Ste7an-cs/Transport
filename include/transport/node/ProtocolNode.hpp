#pragma once

/**
 * @file ProtocolNode.hpp
 * @brief 最小外部协议交互节点 ProtocolNode(RT_NODE_003 / RT_REQUEST / ADR-0003 D8/D9)。
 *
 * ProtocolNode 继承 `NodeBase`(生命周期由基类承载幂等与汇合,本类实现 `DoStart` /
 * `DoClose` / `DoJoin` 三个钩子),组合 `ICodec`(线缆格式)与 `Dispatcher`(按键分发),
 * 并自持一条读-分发循环(`await → decode → dispatch`),交付一次 needresponse 的
 * 请求-响应。
 *
 * **入站只有一条通路:订阅**(ADR-0009 D1 / RT_INBOUND_001)。读循环解出的每条消息一律交
 * `Dispatcher` 按键投递给全部匹配的订阅者,各得一份副本;节点**不再内置**"入站业务处理器"
 * 通道,也不再持有第二条业务队列与第二条消费者 fiber。请求-响应的关联与入站业务的取用
 * 因此是同一套机制的两种用法:前者由 `Request` 内部按"同会话、同命令码、帧类型为回应"
 * 临时登记,后者由宿主经公开的 `Subscribe(Key)` 长期登记,在**自己的 fiber** 上消费自己的
 * 信箱。
 *
 * **串行、异常隔离与背压是调用方契约**(ADR-0009 D3 / RT_INBOUND_005):一条 fiber 顺序消费
 * 即得串行,需要并发就自己起多条;消费代码的逃逸异常须自行 `try/catch`;队列容量即订阅
 * 信箱的容量。框架三者一概不保证。
 *
 * **无订阅者的业务帧静默丢弃、不归因**(ADR-0009 D5):订阅模型下"没人订阅"是宿主的正常
 * 选择而非异常。终结帧(kResponse / kResult)无人认领仍归因 `kUnmatchedOrLateResponse`——
 * 那属请求-响应侧的迟到/乱序,是真正的异常。
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
#include <cstdint>
#include <memory>

#include "await/awaitable.hpp"
#include "task/fibertask.h"  // Coro::FiberTask —— 读-分发循环的结构化并发句柄。
#include "detail/result.hpp"

#include "transport/codec/ICodec.hpp"
#include "transport/core/Dispatcher.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/node/NodeBase.hpp"

namespace transport {

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

/**
 * @brief 一次交互中**受理阶段**的重发策略(ADR-0010 D6 / RT_NODE_002_e)。
 *
 * 逐次调用传入而**不进节点配置**:两阶段的等待是数量级不同的量(第一阶段等"对端受理",
 * 第二阶段等"对端执行完"),且同一节点上不同命令的耐受度不同,不宜由节点级配置一刀切。
 */
struct RetryPolicy {
  /// 单次尝试的等待时长;须为正值,否则调用返 kInvalidArgument。
  std::chrono::milliseconds timeout{};
  /// 总发送次数(**含首发**);须 ≥ 1,否则调用返 kInvalidArgument。
  int max_attempts = 1;
};

/// ProtocolNode 配置:默认外部协议 id + 默认请求超时 + 可选 Trace 出口。
///
/// 入站业务不在配置面上——它由宿主经 `Subscribe(Key)` 自行登记(ADR-0009 D1),故本结构
/// 既无处理器字段,也无业务队列容量字段(容量即订阅信箱的容量,ADR-0009 D3)。
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
  /// 可选 Trace 出口(ADR-0003 D13);非拥有,可为 nullptr。**观测的唯一出口**——本类不再
  /// 有任何计数器与 getter。本类在两个丢弃点(kBadFrame / kUnmatchedOrLateResponse)与
  /// send/recv/decode 边界上报事件。无订阅者的业务帧**不**在此列(ADR-0009 D5)。
  /// RT_TRACE_002:为空时不改变任何控制流/字节流/错误结果,仅一次判空。
  ITraceSink* trace_sink = nullptr;
};

/**
 * @brief 最小外部协议交互节点:继承 NodeBase(生命周期)+ 组合 codec + 在途请求表,
 *        借用宿主的 transport,交付请求-响应。
 *
 * **生命周期**:`Start()` / `Close()` / `WaitClosed()` / `IsRunning()` 一律继承自 `NodeBase`,
 * 本类只实现三个钩子。`Close()` 只发信号、任何 fiber 都可调(含宿主的订阅消费 fiber);
 * `WaitClosed()` join 本节点**唯一**的内部工作单元——读-分发循环,返回即可安全析构。
 *
 * @warning `WaitClosed()` 返回**不**代表宿主的订阅消费 fiber 已退出(ADR-0009 D4 /
 *          RT_LIFECYCLE_006):那些 fiber 属宿主而非本节点,本节点无从 join。关闭只保证它们
 *          的在途 `await` 恰好终结一次(信箱被 `CloseAll` 关闭),此后它们至多再跑完手上
 *          那一条即退出。需要严格汇合的宿主须自己 join 自己的 fiber。
 *
 * **它关的是自己,不是传输**:`DoClose()` 只 close 本节点的读订阅与全部订阅信箱,不触碰
 * transport。传输的关闭由宿主自己做。
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
   * @brief 交付一次 `needresponse` 交互:命令 → 等受理回应,超时重发(ADR-0010 D2 ②)。
   *
   * ```
   * → kCommand
   * ⏱ 等 kResponse ──超时──▶ 重发 ──次数耗尽──▶ kNotAccepted
   * ← kResponse                                ⇒ 成功(返回该帧)
   * ```
   *
   * 与 `Request` 的差别(ADR-0010 D10 已记:二者语义相近而不相同,合并推迟):`Request` 是
   * **一次总超时、不重发**,本方法是**每次尝试各一份超时 + 至多 `max_attempts` 次发送**,
   * 且耗尽时返回的是 `kNotAccepted`(对端**始终没有受理**)而非 `kTimeout`。
   *
   * 重发的是**字节完全相同**的原帧,`session_id` 不变(D3),故原订阅横跨全部重发继续有效,
   * **最先到达**的那一帧即终结本次交互,框架不区分它对应第几次尝试。由此要求对端能容忍
   * 重复命令(幂等,或自行按 session_id 去重)——协议层假设,框架不校验。
   *
   * @param req   请求 Message(payload + message_id 由调用方填);本节点盖 kCommand /
   *              protocol_id / session_id。
   * @param retry 受理阶段的重发策略,见 RetryPolicy。
   * @return 受理回应帧;或 kNotAccepted(次数耗尽)、kInvalidArgument(策略非法)、
   *         kClosed(未启动 / 已关闭)、编码错误。
   */
  [[nodiscard]] Coro::Result<Message> RequestForResponse(Message req,
                                                         RetryPolicy retry);

  /**
   * @brief 交付一次 `withfeedback` / `needfeedback` 交互(**同一模型**,ADR-0010 D1 修正)。
   *
   * ```
   * → kCommand
   * ⏱ 等 kResponse ──超时──▶ 重发 ──次数耗尽──▶ kNotAccepted
   * ← kResponse(受理)
   * ⏱ 等 kResult   ──超时──▶ kTimeout(**不重发**)
   * ← kResult
   * → kResponse(回应结果 = 该 kResult 帧原样改帧类型)  ⇒ 成功(返回 kResult 那一帧)
   * ```
   *
   * **两个订阅在发出命令之前一起登记**(D4):`kResult` 可能先于 `kResponse` 到达(对端足够
   * 快),若等收到受理再登记结果订阅,该帧将因无匹配而被丢弃。
   *
   * **第二阶段不重发**(D2 / RT_NODE_002_c):`kResult` 未达意味着对端**正在执行**,重发命令
   * 有使其重复执行的风险;故该阶段超时直接以 `kTimeout` 终结。
   *
   * **末尾的回应结果是本模型固有的最后一步、不是可选项**(D8 / RT_NODE_002_f):该帧完全由
   * 收到的 `kResult` 派生——payload 原样回显、session_id 与 message_id 沿用,**仅**把帧类型
   * 改为 kResponse,CRC 由 `ICodec::Encode` 重算。它不走 `Send()`(那会强制盖新 session_id
   * 与 kCommand)。该帧交给传输失败则整次交互返错(只在节点关闭时可能发生)。
   *
   * @param req               请求 Message;盖章同 `RequestForResponse`。
   * @param retry             **受理阶段**的重发策略。
   * @param result_message_id 结果帧的命令码。它与请求帧**不同**,其对应关系是协议知识,
   *                          框架不猜、不做映射规则(D7),由调用方给出。
   * @param result_timeout    等待结果帧的时限;须为正值。
   * @return 收到的 `kResult` 帧;或 kNotAccepted(受理阶段次数耗尽)、kTimeout(已受理但
   *         结果超时)、kInvalidArgument、kClosed、编码错误。
   */
  [[nodiscard]] Coro::Result<Message> RequestForResult(
      Message req, RetryPolicy retry, std::uint16_t result_message_id,
      std::chrono::milliseconds result_timeout);

  /**
   * @brief 交付一次**另一种协议**的"直取结果"交互(ADR-0010 D13 / RT_NODE_002_g)。
   *
   * ```
   * → kCommand
   * ⏱ 等 kResult ──超时──▶ 重发 ──次数耗尽──▶ kTimeout
   * ← kResult                                ⇒ 成功(返回该帧,**不回应**)
   * ```
   *
   * **它不是外部系统协议的第五种交互**:`Send` / `RequestForResponse` / `RequestForResult`
   * 属外部系统协议,本方法属另一种协议,二者并存于同一节点(D13)。`ProtocolNode` 对线缆
   * 格式不透明(编解码经 `ICodec` 注入),协议差异只体现在"用哪些帧类型、走哪种交互",
   * 不构成新的节点类型,故不另起节点。
   * **调用方须自行确保所用方法与对端协议匹配,框架不校验**——它对协议语义不透明(D13,
   * 已记为明确接受的代价)。
   *
   * 与 `RequestForResult` **恰好相反的三条**,勿混:
   * 1. **本交互在"等结果"阶段就重发**。`RequestForResult` 的"等 `kResult` 时不得重发"
   *    (RT_NODE_002_c)**只约束外部系统协议**,不是框架的普遍规则(该条 2026-08-26 已明确
   *    适用面)。本交互没有受理阶段,唯一的等待就是等结果——不重发则命令帧一旦丢包即
   *    彻底失败、无任何补救(RT_NODE_002_g)。
   * 2. **重发耗尽返 `kTimeout` 而非 `kNotAccepted`**(D12):后者的语义是"对端**没有受理**",
   *    而本交互**根本不存在受理这一步**。
   * 3. **收到 `kResult` 后不回应任何帧**(对比 D8:那是 `RequestForResult` 模型固有的最后
   *    一步)。
   *
   * 因此本方法**不复用** `AwaitAccept()`:那个骨架等的是 `kResponse`、耗尽返 `kNotAccepted`,
   * 两处语义都不对。它自带一个独立的重发循环,且**只登记一个订阅**(无受理帧可订)。
   *
   * 重发的是**字节完全相同**的原帧、`session_id` 不变(D3),以最先到达的那一帧为准;由此
   * **要求对端能容忍重复命令**(幂等,或自行按 session_id 去重)——协议层假设,框架不校验。
   * 因本交互在唯一的等待阶段重发,该要求的适用面比 `RequestForResult` 更广。
   *
   * @param req               请求 Message(payload + message_id 由调用方填);本节点盖
   *                          kCommand / protocol_id / session_id。
   * @param retry             重发策略。本交互**只有一个等待阶段**,其时限即
   *                          `RetryPolicy::timeout`,故签名中**没有**独立的 result_timeout。
   * @param result_message_id 结果帧的命令码。它与请求帧**不同**,其对应关系是协议知识,
   *                          框架不猜、不做映射规则(D7),由调用方给出。
   * @return 收到的 `kResult` 帧;或 kTimeout(重发次数耗尽,**非** kNotAccepted)、
   *         kInvalidArgument(策略非法)、kClosed(未启动 / 已关闭)、编码错误。
   */
  [[nodiscard]] Coro::Result<Message> RequestForResultDirect(
      Message req, RetryPolicy retry, std::uint16_t result_message_id);

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
   * @brief 登记一个订阅——**取用入站消息的唯一入口**(RT_INBOUND_001 / ADR-0009 D1)。
   *
   * 三类用法共用本入口:① 取用入站业务帧(按 `FrameType` 等字段订阅自己关心的帧);
   * ② 分段交互(一次交互需等待多条报文时,各段各自登记、各自设定时限);③ 旁路监听。
   * **登记须先于对应报文到达**,否则先到的报文因无订阅而被丢弃(业务帧静默丢弃,
   * ADR-0009 D5)。
   *
   * 消费在**调用方自己的 fiber** 内进行,节点不代管:
   * ```cpp
   * auto ticket = node.Subscribe(AnyOfType(FrameType::kCommand));
   * auto task = Coro::makeTask([&] {
   *   for (;;) {
   *     auto m = Coro::await(ticket.mailbox());
   *     if (!m) break;                 // 信箱被节点关闭 → 退出
   *     try {
   *       Handle(m.value());           // 业务处理,可自由挂起
   *     } catch (...) {
   *       // 自行隔离:框架不再兜住逃逸异常
   *     }
   *   }
   * });
   * ...
   * (void)task.get();                  // 宿主自己 join,勿依赖 WaitClosed
   * ```
   * 串行、异常隔离与信箱容量之外的背压一律是**调用方契约**(RT_INBOUND_005)。
   *
   * @param key 订阅键;不参与匹配的字段填 `kAny`。见 `ResponseTo` / `FrameOf` /
   *            `AnyOfType` 三个具名工厂。
   * @return 订阅凭据,析构时自动注销。
   */
  [[nodiscard]] MessageDispatcher::Ticket Subscribe(MessageDispatcher::Key key);

 protected:
  // —— NodeBase 生命周期钩子 ————————————————————————————————————————————

  /// @brief 启动的协议特有实事:校验 config → 取读订阅 → spawn 读-分发循环。
  ///        **不启动 transport**(那是宿主的事)。
  ///        配置非法返 kConfiguration,基类退回 Created 允许改配重试(此时未 spawn)。
  Coro::Result<void> DoStart() override;

  /// @brief 关闭汇合信号(首个关闭者独占执行一次,只发信号、不等待):close 本节点的读
  ///        订阅(读循环据此退出)→ `Dispatcher::CloseAll`(关闭全部订阅信箱:令在途请求
  ///        恰好终结一次,同时即订阅者的协作取消信号,ADR-0009 D4)。不关闭 transport。
  Coro::Result<void> DoClose() override;

  /// @brief join 本节点 spawn 的全部 fiber——只有读-分发循环一条。订阅者的消费 fiber 属
  ///        宿主,不在此列(ADR-0009 D4)。
  void DoJoin() override;

 private:
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
  /// 单条 Message 的分发:交 `Dispatcher` 按键投递(**唯一投递路径**);无人认领时终结帧
  /// 归因丢弃、业务帧静默丢弃(ADR-0009 D1/D5)。
  ///
  /// 取 const 引用:投递只读源消息(命中的订阅者各拷一份副本),本函数不再有"把消息移交
  /// 第二条队列"的分支,故不需要按值取走所有权。
  void Dispatch(const Message& msg);
  /// @brief 编码 + 交给传输(Request / Send / 两个交互方法共用的出站尾段)。
  ///        **不盖任何章**——这正是 D8 的回应结果帧走本函数而非 `Send()` 的原因。
  [[nodiscard]] Coro::Result<void> EncodeAndWrite(const Message& msg);

  /// @brief 受理阶段(等 kResponse,超时重发),`RequestForResponse` 与 `RequestForResult`
  ///        共用的私有骨架。
  ///
  /// **仅限外部系统协议**:`RequestForResultDirect` 不走本函数——它等的是 `kResult`、耗尽
  /// 返 `kTimeout`,与本函数的两处语义都不同(ADR-0010 D13/D12)。
  ///
  /// **前置**:`ack_ticket` 须**已由调用者登记**——登记必须先于第一次发出(D4),且
  /// `RequestForResult` 还须与结果订阅一起登记,故不能挪进本函数。
  ///
  /// @return 首个到达的受理帧(D3);次数耗尽返 kNotAccepted(D12);编码失败与
  ///         kClosed 等终止原因直接透出、**不重试**。
  [[nodiscard]] Coro::Result<Message> AwaitAccept(
      const Message& req, const RetryPolicy& retry,
      MessageDispatcher::Ticket& ack_ticket);

  /// @brief 三个交互方法共用的前置判据:节点在运行 + 重发策略合法。
  [[nodiscard]] Coro::Result<void> ValidateInteraction(
      const RetryPolicy& retry) const;

  /// @brief 取用下一个 session_id:自增计数器,`std::uint8_t` 自然回绕即 0..255 循环。
  ///        取用不会失败,故不返回错误。
  [[nodiscard]] std::uint8_t NextSession();

  ITransport& transport_;  ///< **借用**:宿主拥有并启停,寿命须长于本节点。
  std::unique_ptr<ICodec> codec_;
  ProtocolNodeConfig config_;

  /// 本节点在传输 `read_queue` 上的独立订阅(`shared()`):关闭它只终止本节点这一路,
  /// 源队列与其它订阅者不受影响。`DoStart` 建立,`DoClose` 关闭。
  std::shared_ptr<Coro::Awaitable<Datagram>> rx_;
  /// 读-分发循环的结构化并发句柄;`DoJoin()` 让出式 join 之。
  std::shared_ptr<Coro::FiberTask<void>> read_task_;

  /// 入站的**唯一**投递路径:`Request` 与宿主的 `Subscribe` 都在此登记,读循环收到消息后
  /// 按键投给全部匹配的订阅者。
  MessageDispatcher dispatcher_;

  /// 下一个待取用的 session_id;每次取用后自增,越过 255 自然回绕。
  std::uint8_t next_session_{0};
};

}  // namespace transport
