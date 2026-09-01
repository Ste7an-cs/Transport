#pragma once

/**
 * @file DdsNode.hpp
 * @brief DDS 交互节点 DdsNode——**注册接口 + 两种交互模式**(ADR-0013 D5/D6/D7/D8/D16)。
 *
 * `DdsNode` 继承 `NodeBase`(生命周期由基类承载幂等与汇合,本类只实现 `DoStart` /
 * `DoClose` / `DoJoin` 三个钩子),组合 `ICodec`(线缆格式)与 `DdsDispatcher`(按键分发),
 * 借用宿主的 `DdsTransport`,自持一条读-分发循环,交付两种交互模式:
 *
 * | 模式 | 客户端/发布侧 | 服务端/订阅侧 |
 * |---|---|---|
 * | 发布-订阅 | `Publish(topic, msg)` | `Subscribe(topic, kNotify)` |
 * | 请求-响应(**单阶段** `Direct`,**D7**) | `RequestForResultDirect(topic, req, retry)` | `Subscribe(topic, kRequest)` + `Reply(request, result)` |
 *
 * **公开面只有这四个交互方法**(**D8**):服务端**没有** `Accept()`(本模型无受理阶段);
 * `MessageKind::kFeedback` **在本设计中不使用**;**不提供旁路监听**——机制上
 * `Subscribe(kAny, kAny)` 可达,但不作为受支持的用法。
 *
 * ## topic 由注册接口给出,不进配置(**D16**)
 *
 * ```cpp
 * DdsNode node(transport, std::make_unique<DdsCodec>());
 * (void)node.RegisterPublishers({"telemetry"});                        // 发布者
 * (void)node.RegisterServices({{"cfg.get", "cfg.get.reply"}});         // 服务端
 * (void)node.Start();                                                  // 端点在此一次性建出
 * ```
 *
 * **四个注册方法一律只在 `Created` 相位受理**,`Running` / `Closing` / `Closed` 返
 * `kInvalidState`。端点集合"**启动即定型、运行期恒定**"——本设计**不引入运行期动态端点**,
 * 故回应、发布、订阅路径上都不会突然冒出一个约 240ms 的发现窗口(**D9**)。
 *
 * ## 角色由"注册了什么"表达,不设 role 枚举(**D16**)
 *
 * | 注册方法 | 该节点就是 | `DoStart()` 建的端点 |
 * |---|---|---|
 * | `RegisterPublishers` | 发布者 | 每个 topic 的 **Writer** |
 * | `RegisterSubscribers` | 订阅者 | 每个 topic 的 **Reader** |
 * | `RegisterClients` | 请求-响应**客户端** | 键 → **Writer**(发请求)　值 → **Reader**(收应答) |
 * | `RegisterServices` | 请求-响应**服务端** | 键 → **Reader**(收请求)　值 → **Writer**(发应答) |
 *
 * 四者可任意并存(一个节点常常兼任)。请求-响应两侧**传一模一样的实参**,各自按角色建
 * 各自那一侧,不会填错方向、也不必协调。
 *
 * ## 关联键 `correlation_id` 是两段式(**D6**)
 *
 * ```
 * correlation_id = "<uuid>#<request_seq>"
 *                     ↑          ↑
 *       节点构造时生成一次    uint32,从 0 开始自增,每请求一个
 * ```
 *
 * uuid 半段保证**跨节点**不撞——这是"每服务一个应答 topic、该服务全体客户端共用"得以
 * 成立的**全部**根据;`request_seq` 保证**节点内**不撞。`uint32` 回绕(约 42.9 亿次请求后)
 * **明确接受、不加防回绕逻辑**:届时重复的是本节点很久以前用过的值,那条订阅早已注销。
 *
 * @warning **一处承重的区别**:`Subscribe` 交出去的订阅其 `corr` 位**恒为 `kAny`**,而
 *          `RequestForResultDirect` **内部**登记的那一条**用具体值**。共用应答 topic 之所以
 *          能区分客户端,**全靠内部登记的 corr 是具体值**;若内部也用 `kAny`,客户端会匹配
 *          上该 topic 上**所有人**的应答。
 *
 * ## 一处必须先理解再用的限制(**D16**)
 *
 * **`Subscribe(kAny, kind)` 建不了任何 `DataReader`。** DDS 的 reader 是**按 topic** 建的,
 * 而 `kAny` 只是**分发键**上的通配符。故"订阅所有 topic"的实际语义是「**已注册为 reader
 * 的 topic 的全部**」——即 `Subscribers` ∪ `Services` 的键 ∪ `Clients` 的值,**不是**
 * "本 domain 上的全部"。未注册的 topic,其消息**根本不会到达本进程**。
 *
 * ## 不管 transport 的生命周期
 *
 * 传输由**宿主**创建、`Start()`、`Close()`、`WaitClosed()`,本节点只按引用借用它
 * (与 `ProtocolNode` 同,ADR-0009)。`DoStart()` 只在其上逐项 `DeclareWriter` /
 * `DeclareReader`(**D15**:这是**唯一**建端点的地方),故**调用 `Start()` 之前宿主必须先
 * 把传输启起来**,否则声明一律返 `kInvalidState`。
 *
 * **借用而非拥有,还有一条硬理由**:`DdsTransport::WaitClosed()` join 的是一条**专属 OS
 * 线程**且**最坏等待无上界**(在途 `Publish` 打不断)。若由本节点在 `DoJoin()` 里调它,
 * 那是**阻塞整条 fiber 线程**,与 `NodeBase::WaitClosed()`「让出式 join」的纪律正相反。
 *
 * @warning `close()` 是**整流传播**的(AsyncTask `417790c` 起):`DoClose()` 关闭本节点这一路
 *          读订阅时,源读队列与同一条传输上的其它订阅者**一并终结**。与 `ProtocolNode`
 *          同形、同为有意为之——节点关闭即读侧终结,宿主随后关传输。
 *
 * ## 明确接受、框架不管的两件事
 *
 * - **重发要求对端能容忍重复请求**(幂等,或自行按 `correlation_id` 去重)。协议层假设,
 *   **框架不校验**(**D7**)。
 * - **共用应答 topic 带来读入放大**:同一服务的每个客户端都会收到该服务的**全部**应答,
 *   多余样本一路进读队列、解码后才在 `Dispatcher` 处落空(「明确接受的代价」8)。由此
 *   `kUnmatchedOrLateResponse` 这条丢弃归因在客户端侧**本来就会很吵**,不是异常。
 *
 * 与传输、`Dispatcher` 一致,本类面向**单线程 fiber 协作**模型:交互方法运行于调用方
 * fiber,读-分发循环运行于自持 fiber,二者同线程且仅在挂起点交错,故普通成员不加锁。
 */

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "await/awaitable.hpp"
#include "task/fibertask.h"  // Coro::FiberTask —— 读-分发循环的结构化并发句柄。
#include "detail/result.hpp"

#include "transport/codec/ICodec.hpp"
#include "transport/core/Dispatcher.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Message.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/node/NodeBase.hpp"
#include "transport/node/RetryPolicy.hpp"

namespace transport {

/// 参与 DDS 分发的字段:**topic + correlation_id + kind**(ADR-0013 **D6**)。
///
/// 三者分别是 **DDS 的寻址维度**、**其关联符**、**消息类别**;部分匹配由 `Dispatcher`
/// 实现,本类只需在键提取函数里给出各字段的具体值。**这不是照搬 `ProtocolNode`**——
/// 它选 `(session_id, message_id, frm_type)` 是**它的协议**决定的(DD-4:协议无关基座可复用)。
using DdsDispatcher = Dispatcher<Message, std::string /*topic*/,
                                          std::string /*correlation_id*/,
                                          MessageKind /*kind*/>;

/// DdsNode 配置——**只剩两项**(ADR-0013 **D16**)。
///
/// 全部 topic 字段已移出配置、改由四个注册方法给出;历史遗留的 `inbox_topic` / `node_id` /
/// `handler` / `business_queue_max_*` 一并删除(入站业务由订阅承载,ADR-0009 D1)。
struct DdsNodeConfig {
  /// 节点 uuid:**非空则用它,为空才 `QUuid::createUuid()`**(**D6**)。
  ///
  /// `QUuid` 是随机的,而 `correlation_id` 的前缀确定与否直接决定测试能不能断言具体值;
  /// 故留这一个注入口:**测试填固定值,生产留空**。
  std::string uuid_override;
  /// 可选 Trace 出口(ADR-0003 D13);非拥有,可为 `nullptr`。**观测的唯一出口**——本类没有
  /// 任何计数器与 getter。发射点:两个丢弃归因(`kBadFrame` / `kUnmatchedOrLateResponse`)
  /// 与 send / recv / decode 三个边界。RT_TRACE_002:为空时仅一次判空,不改变任何控制流。
  ITraceSink* trace_sink = nullptr;
};

/**
 * @brief DDS 交互节点:注册接口给出 topic,四个方法交付发布-订阅与单阶段请求-响应。
 *
 * 见文件头。不可拷贝、不可移动(读-分发循环 fiber 捕获 this)。
 */
class DdsNode : public NodeBase {
 public:
  /// 订阅凭据(析构自动注销);`Subscribe` 交出的就是它。
  using Ticket = DdsDispatcher::Ticket;
  /// `Subscribe` 的 topic 键:具体 topic 名,或 `kAny`(该字段不参与匹配)。
  using TopicKey = std::optional<std::string>;
  /// `Subscribe` 的 kind 键:具体 `MessageKind`,或 `kAny`。
  using KindKey = std::optional<MessageKind>;

  /// @brief 构造。
  /// @param transport **借用**的 DDS 传输:宿主负责其 `Start()` / `Close()` /
  ///                  `WaitClosed()`,且须保证其寿命长于本节点。**本节点 `Start()` 之前
  ///                  它必须已 Running**——端点声明只能落在已 `Init` 的 provider 上。
  /// @param codec     线缆格式(本节点独占),DDS 路径即 `DdsCodec`。
  /// @param config    见 `DdsNodeConfig`。
  DdsNode(DdsTransport& transport, std::unique_ptr<ICodec> codec,
          DdsNodeConfig config = {});
  /// @brief 析构:`Close()` + `WaitClosed()`——须在**本类**析构体内做,彼时动态类型仍是
  ///        DdsNode、虚钩子可用、成员尚存(基类析构不得收敛,见 `NodeBase::~NodeBase`)。
  ~DdsNode() override;

  DdsNode(const DdsNode&) = delete;
  DdsNode& operator=(const DdsNode&) = delete;

  // ── 注册接口(D16)——【须在 Start() 之前调用】 ──────────────────────────

  /**
   * @brief 注册**发布者** topic:每个 topic 在 `DoStart()` 建一个 **Writer**。
   *
   * **批量**——一次给一组,不必一个 topic 调一次;**可多次调用累加**(便于按模块分别注册);
   * **重复项幂等去重**,不报错。
   *
   * **整批生效或整批不生效**:一批里只要有一项非法,**整批回滚、一项都不落**,返对应错误。
   * 半生效的注册会让调用方难以判断该重试哪些。
   *
   * @param topics 待注册的 topic 集合。
   * @return 成功;不在 `Created` 相位返 `kInvalidState`;任一 topic 为空串返
   *         `kInvalidArgument`(此时整批未落)。
   */
  [[nodiscard]] Coro::Result<void> RegisterPublishers(
      std::vector<std::string> topics);

  /// @brief 注册**订阅者** topic:每个 topic 在 `DoStart()` 建一个 **Reader**。
  ///        批量 / 累加 / 幂等去重 / 整批生效,返回值同 `RegisterPublishers`。
  [[nodiscard]] Coro::Result<void> RegisterSubscribers(
      std::vector<std::string> topics);

  /**
   * @brief 注册**请求-响应客户端**:`请求 topic → 应答 topic`,**一服务一条**。
   *
   * 键在 `DoStart()` 建 **Writer**(发请求)、值建 **Reader**(收应答)。与服务端
   * `RegisterServices` **传一模一样的实参**,各自按角色建各自那一侧(**D16**)。
   *
   * **应答 topic 是每服务一个、该服务的全体客户端共用的**(**D6**),故一个客户端同时调
   * 多个服务时各服务的应答落在各自 topic 上、互不相扰。用 `std::map` 而非
   * `vector<pair>`:天然去重,且**从类型上排除"同一请求 topic 配了两个不同应答 topic"**。
   *
   * @param topics 请求 topic → 应答 topic。
   * @return 成功;不在 `Created` 相位返 `kInvalidState`;下列任一非法返 `kInvalidArgument`
   *         (整批不落):
   *         - 键或值为空串;
   *         - 某条的**键与值相同**(请求与应答同 topic 必然自收自答);
   *         - 某个键**已注册为 `Services` 的键**——**自己请求自己**,且 `corr` 由自己生成、
   *           `Dispatcher` **会真的匹配上**,形成调用方毫无察觉的自问自答。**这是唯一要拦的
   *           方向冲突**;其余"同一 topic 上既有 writer 又有 reader"的组合只造成自收白干、
   *           不会误配,且可能是有意的回环自测,**不拦**;
   *         - 某个键**此前已注册为 `Clients` 的键但应答 topic 不同**——与上面那条 map 的
   *           类型保证同源:一个请求 topic 只能有一个应答 topic,跨批次亦然。
   */
  [[nodiscard]] Coro::Result<void> RegisterClients(
      std::map<std::string, std::string> topics);

  /// @brief 注册**请求-响应服务端**:`请求 topic → 应答 topic`,键建 **Reader**(收请求)、
  ///        值建 **Writer**(发应答)。校验与返回值与 `RegisterClients` 逐条对称
  ///        (方向冲突查的是 `Clients` 的键)。
  [[nodiscard]] Coro::Result<void> RegisterServices(
      std::map<std::string, std::string> topics);

  // ── 公开面:两种交互模式(D8)────────────────────────────────────────────

  /**
   * @brief 登记一个订阅——**取用入站消息的唯一入口**(ADR-0009 D1 / **D6**)。
   *
   * 两个键均可传 `kAny`。**交出去的订阅其 `corr` 位恒为 `kAny`**(见文件头的承重区别):
   * `correlation_id` 不进公开接口——请求-响应侧它由框架在 `RequestForResultDirect` 内生成、
   * 服务端事先不可能知道客户端会生成什么值,发布-订阅侧的"应用自定义子通道"能力已裁决
   * 为不需要。暴露一个只能填一个值的参数是陷阱,不是灵活性。
   *
   * | 用途 | 这样调 | 实际键 |
   * |---|---|---|
   * | 订阅某 topic 的通知 | `Subscribe("t", MessageKind::kNotify)` | `{"t", kAny, kNotify}` |
   * | 收某 topic 的请求 | `Subscribe("t", MessageKind::kRequest)` | `{"t", kAny, kRequest}` |
   * | 收全部(已注册 reader 的)topic 的通知 | `Subscribe(kAny, MessageKind::kNotify)` | `{kAny, kAny, kNotify}` |
   *
   * 消费在**调用方自己的 fiber** 内进行,节点不代管;串行、异常隔离与信箱容量之外的背压
   * 一律是调用方契约(RT_INBOUND_005)。**多 topic 用多次 `Subscribe`,每 topic 一条消费
   * fiber**——各自独立信箱,一路慢不拖累另一路。
   *
   * **返 `Coro::Result<Ticket>` 而不是裸 `Ticket`**(**D8**):"topic 未注册为对应角色"要返
   * `kConfiguration`,而 `Ticket` **装不下错误码**——返裸 `Ticket` 时"忘了注册"只能交出一个
   * 空凭据,其 `Wait` 返的是 `kInvalidState`,**错误码不对,且推迟到第一次 `Wait` 才暴露**。
   * **相位同理**:关闭之后订阅要返 `kClosed`,也要在**返回处**返,而不是交出一张信箱已经
   * 关闭的凭据、把 `kClosed` 推给第一次 `Wait`。
   *
   * **相位规则**:`Created` / `Running` 放行,`Closing` / `Closed` 返 `kClosed`。
   * 与另外三个交互方法(一律 `!IsRunning()` 即 `kClosed`)**有意不同**——`Created` 在本方法
   * 上是**合法且被推荐的**订阅时机:`DataReader` 建于 `Start()`,故 `Start()` 之前一条消息
   * 也到不了本进程,「注册 → 订阅 → `Start()`」是唯一在结构上不漏收启动初期消息的次序。
   * 反过来若只许 `Running` 订阅,零丢失就得靠"`Start()` 与 `Subscribe` 之间不让出"这条隐式
   * 调度约定,中间一有挂起点消息就被静默丢弃。
   *
   * @param topic 具体 topic,或 `kAny`。
   * @param kind  具体 `MessageKind`,或 `kAny`。
   * @return 订阅凭据(析构时自动注销);节点处于 `Closing` / `Closed` 返 `kClosed`
   *         (**先于**下面的注册校验,故已关闭的节点上订阅未注册的 topic 报 `kClosed`,
   *         不报 `kConfiguration`);topic 未注册为对应角色返 `kConfiguration`:
   *         `kNotify` 须已注册为 `Subscribers`,`kRequest` 须是 `Services` 的键,其余
   *         `kind` 须至少在读侧集合内(`Subscribers` ∪ `Services` 的键 ∪ `Clients` 的值)
   *         ——不在读侧的 topic 其消息根本不会到达本进程,订阅它必然是**静默无效**。
   *         **topic 传 `kAny` 时跳过该校验**:`kAny` 不对应任何一个具体 topic,拿它去查
   *         注册表必然落空;它的作用域本就已由注册天然限定(见文件头的限制一节)。
   */
  [[nodiscard]] Coro::Result<Ticket> Subscribe(TopicKey topic, KindKey kind);

  /**
   * @brief 单向发布一条 `kNotify`(fire-and-forget,发布-订阅):不登记任何订阅、不期待应答。
   *
   * 本节点盖 `kind = kNotify` 并**清空** `correlation_id` / `reply_to`——**D6** 之后
   * `correlation_id` 只有框架生成的关联符一个来源,发布路径上它没有第二种用法。
   *
   * @param topic 目标 topic,**须已注册为 `Publishers`**。
   * @param msg   出站 Message(`payload` 由调用方填)。
   * @return 已入队;`kClosed`(未启动 / 关闭中 / 已关闭)、`kConfiguration`(topic 未注册为
   *         发布者)、编码错误。**返回成功不表示已发出**——写出与其失败归因都在传输的
   *         专属写线程里,只落 `LastError()`。
   */
  [[nodiscard]] Coro::Result<void> Publish(const std::string& topic, Message msg);

  /**
   * @brief 交付一次请求-响应(**单阶段**,等结果时重发,不回应;**D7**)。
   *
   * ```
   * → kRequest
   * ⏱ 等 kReply ──超时──▶ 重发 ──次数耗尽──▶ kTimeout
   * ← kReply                                ⇒ 成功(返回该帧,【不回应】)
   * ```
   *
   * 步骤:按 `topic` 查已注册的 `Clients` 表取应答 topic(**查不到即 `kConfiguration`,
   * 不猜、不回落**)→ 盖 `kind` / `corr` / `reply_to` → **先登记订阅再发出** → 编码**一次**、
   * 重发复用同一份字节 → 首个到达即成功。
   *
   * **签名里没有 `result_timeout`**:本交互只有**一个**等待阶段,其时限即 `retry.timeout`。
   * **耗尽返 `kTimeout` 而不是 `kNotAccepted`**——后者的语义是"对端没有受理",而本模型
   * **根本不存在受理这一步**。
   *
   * **为什么 `RELIABLE` 的 DDS 上还要重发**:丢的不是网络,是**队列**——读队列有界 1024、
   * 满时静默丢最旧,且共用应答 topic 把这一段的压力放大了 `N` 倍(**D11** / 代价 8)。
   * 重发正是对这一段的补救。代价是**要求对端能容忍重复请求**,框架不校验。
   *
   * @param topic 请求 topic,**须已注册为 `Clients` 的键**。
   * @param req   请求 Message(`payload` 由调用方填;`kind` / `correlation_id` /
   *              `reply_to` 由本节点盖)。
   * @param retry 重发策略,见 `RetryPolicy`。
   * @return 收到的 `kReply`;或 `kTimeout`(重发次数耗尽)、`kInvalidArgument`(策略非法)、
   *         `kConfiguration`(topic 未注册为客户端)、`kClosed`、编码错误。
   */
  [[nodiscard]] Coro::Result<Message> RequestForResultDirect(
      const std::string& topic, Message req, RetryPolicy retry);

  /**
   * @brief 服务端回一条终结应答 `kReply`——请求-响应服务端**唯一**的方法(无受理阶段)。
   *
   * **应答 topic 从自己注册的 `Services` 表查**(`services_[request.topic]`),**不取信于
   * 线缆、不建端点**(**D15**):运行期不再有任何建端点的路径,该 topic 的 writer 早在
   * `DoStart()` 就建好了,故服务的**第一次应答也不会丢**。
   *
   * 线缆上的 `reply_to` 降为**一致性交叉校验**:非空且与查出的应答 topic 不等即返
   * `kInvalidArgument`。**保留它是有价值的,不是冗余**——两侧注册实参写歪时(客户端在
   * `cfg.get.reply` 上等、服务端注册成 `cfg.reply` 往外发),不带它这种偏差**完全不可见**,
   * 客户端只会一路超时、看起来像对端没响应;带上它,服务端**当场就能报出**"你等的地方和
   * 我发的地方不一样"。
   *
   * @param request 收到的请求(其 `topic` 与 `correlation_id` 是本方法的全部输入)。
   * @param result  应答 Message(`payload` 由调用方填;`kind` / `correlation_id` 由本节点盖)。
   * @return 已入队;`kClosed`、`kConfiguration`(本节点根本不服务 `request.topic`)、
   *         `kInvalidArgument`(`reply_to` 交叉校验不过)、编码错误。
   */
  [[nodiscard]] Coro::Result<void> Reply(const Message& request, Message result);

  /// @brief 本节点的 uuid——`correlation_id` 的前缀半段(**D6**),供诊断与测试断言。
  [[nodiscard]] const std::string& uuid() const { return uuid_; }

 protected:
  // ── NodeBase 生命周期钩子 ──────────────────────────────────────────────

  /// @brief 启动的 DDS 特有实事:**四组注册全空即 `kConfiguration`**(**D12**:一个什么都
  ///        不收不发的节点必是漏了注册)→ 按四组注册逐项在传输上建**对应方向**的端点
  ///        (**D15**:**唯一**建端点的地方)→ 取读订阅 → spawn 读-分发循环。
  ///        **不启动 transport**(那是宿主的事)。任一步失败即返错,基类退回 `Created`
  ///        且**注册表原样保留**——补上漏的那几项再 `Start()` 一次即可(**D16**)。
  Coro::Result<void> DoStart() override;

  /// @brief 关闭汇合信号(只发信号、不等待):close 本节点的读订阅(读循环据此退出)→
  ///        `Dispatcher::CloseAll`(关闭全部订阅信箱:令在途请求恰好终结一次,同时即
  ///        订阅者的协作取消信号)。**不关闭 transport**。
  Coro::Result<void> DoClose() override;

  /// @brief join 本节点 spawn 的全部 fiber——只有读-分发循环一条。订阅者的消费 fiber
  ///        属宿主,不在此列(ADR-0009 D4)。
  void DoJoin() override;

 private:
  /// @brief spawn 读-分发循环 fiber:`await(rx_) → 成功 → DecodeAndDispatch;错误 → Close()`。
  void SpawnReadLoop();
  /// @brief 读循环体内的 DDS 特有处理:Decode 一条样本 → 按来源 topic 填 `topic`/`source`
  ///        → 逐条 Dispatch。**topic 不上线缆**(**D5**),入站只能由 `Datagram.peer` 带出。
  void DecodeAndDispatch(const Datagram& datagram);
  /// @brief 单条 Message 的分发:交 `Dispatcher` 按键投递(**唯一投递路径**);无人认领时
  ///        `kReply` 归因 `kUnmatchedOrLateResponse`、其余静默丢弃(ADR-0009 D5)。
  void Dispatch(const Message& msg);
  /// @brief 编码 + 交给传输的出站尾段(`Publish` / `Reply` 共用)。**不盖任何章**——盖章
  ///        各自做完再进来,故本函数对交互模式不透明。
  [[nodiscard]] Coro::Result<void> EncodeAndWrite(const Message& msg,
                                                  const std::string& topic);
  /// @brief 把**已编码**的字节交给传输(fire-and-forget)。
  ///        `RequestForResultDirect` 直接用它:那条路径**编码一次、重发复用同一份字节**
  ///        (ADR-0010 D3:重发的是字节完全相同的原帧),不能每次重新 Encode。
  [[nodiscard]] Coro::Result<void> WriteEncoded(std::vector<std::uint8_t> bytes,
                                                const std::string& topic);
  /// @brief 取用下一个 `correlation_id`:`"<uuid>#<request_seq>"`,`request_seq` 自增。
  ///        **`uint32` 回绕明确接受**,不加防回绕逻辑(**D6**)。
  [[nodiscard]] std::string NextCorrelationId();
  /// @brief 该 topic 是否在**读侧**集合内(`Subscribers` ∪ `Services` 的键 ∪ `Clients` 的
  ///        值)——即"它的消息有没有可能到达本进程"。
  [[nodiscard]] bool IsReaderSideTopic(const std::string& topic) const;

  DdsTransport& transport_;  ///< **借用**:宿主拥有并启停,寿命须长于本节点。
  std::unique_ptr<ICodec> codec_;
  DdsNodeConfig config_;

  /// 本节点 uuid,**构造时生成一次、此后不变**(**D6**):`config_.uuid_override` 非空则用
  /// 它,为空才 `QUuid::createUuid().toString(QUuid::WithoutBraces)`。
  std::string uuid_;
  /// `correlation_id` 的自增半段。**不叫 `session_id`**:那是**外部协议**的匹配键
  /// (`Message::session_id`,DDS 路径留缺省 `0`),同名是确定的阅读陷阱(**D6**)。
  std::uint32_t request_seq_{0};

  // —— 四组注册表(**D16**)。`Start()` 之前填,此后只读;`Start()` 失败**不清空**。——
  std::set<std::string> publishers_;             ///< topic → Writer。
  std::set<std::string> subscribers_;            ///< topic → Reader。
  std::map<std::string, std::string> clients_;   ///< 请求 topic → 应答 topic(键 W、值 R)。
  std::map<std::string, std::string> services_;  ///< 请求 topic → 应答 topic(键 R、值 W)。

  /// 本节点在传输 `read_queue` 上的订阅(`shared()`);`DoClose` 关闭之——**关闭是整流
  /// 传播的**,连源队列与其它订阅者一并终结(见文件头 warning)。
  std::shared_ptr<Coro::Awaitable<Datagram>> rx_;
  /// 读-分发循环的结构化并发句柄;`DoJoin()` 让出式 join 之。
  std::shared_ptr<Coro::FiberTask<void>> read_task_;

  /// 入站的**唯一**投递路径:`RequestForResultDirect` 与宿主的 `Subscribe` 都在此登记。
  DdsDispatcher dispatcher_;
};

}  // namespace transport
