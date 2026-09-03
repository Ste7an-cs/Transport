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
 * | 请求-响应(**单阶段** `Direct`,**D7**) | `RequestForResultDirect(服务名, req, retry)` | `ServeRequests(服务名)` + `Reply(request, result)` |
 *
 * **公开面只有这五个交互方法**(**D8**):服务端**没有** `Accept()`(本模型无受理阶段);
 * `MessageKind::kFeedback` **在本设计中不使用**;**不提供旁路监听**——机制上
 * `Subscribe(kAny, kAny)` 可达,但不作为受支持的用法。
 *
 * ## 请求-响应的两个 topic 由【服务名派生】(**D6**)
 *
 * ```
 * 请求 topic  =  cfg.<服务名>.request        应答 topic  =  cfg.<服务名>.response
 * ```
 *
 * `cfg.` 是**固定字面前缀,不可配**。注册与调用**一律只说服务名**,派生规则不外泄到调用方:
 *
 * ```cpp
 * (void)client.RegisterClients ({"get"});   // cfg.get.request 发请求、cfg.get.response 收应答
 * (void)server.RegisterServices({"get"});   // cfg.get.request 收请求、cfg.get.response 发应答
 * ```
 *
 * 两侧算的是同一个派生函数(`DdsNode.cpp` 的 `DeriveServiceTopics`)。**服务名的唯一约束
 * 是非空**,不限制字符集。
 *
 * @warning 派生出的 `cfg.*.request` / `cfg.*.response` 与 `RegisterPublishers` /
 *          `RegisterSubscribers` 收的普通 topic **处在同一平面**:
 *          `RegisterSubscribers({"cfg.get.request"})` 与 `RegisterServices({"get"})`
 *          指的是**同一条 topic**。**框架不拦**。
 *
 * ## topic 由注册接口给出,不进配置(**D16**)
 *
 * ```cpp
 * DdsNode node(transport, std::make_unique<DdsCodec>());
 * (void)node.RegisterPublishers({"telemetry"});   // 发布者:topic
 * (void)node.RegisterServices({"get"});           // 服务端:服务名
 * (void)node.Start();                             // 端点在此一次性建出
 * ```
 *
 * **四个注册方法一律只在 `Created` 相位受理**,`Running` / `Closing` / `Closed` 返
 * `kInvalidState`。端点集合"**启动即定型、运行期恒定**",无运行期动态端点(**D9**)。
 *
 * `Subscribe` 与之**互不重叠**:它**只在 `Running` 受理**,`Created` 期订阅是禁用法、返
 * `kClosed`(与另外三个交互方法同一个判据,见该方法的注释)。全流程即
 * 「注册 → `Start()` → 订阅」。
 *
 * ## 角色由"注册了什么"表达,不设 role 枚举(**D16**)
 *
 * | 注册方法 | 收什么 | 该节点就是 | `DoStart()` 建的端点 |
 * |---|---|---|---|
 * | `RegisterPublishers` | **topic** | 发布者 | 每个 topic 的 **Writer** |
 * | `RegisterSubscribers` | **topic** | 订阅者 | 每个 topic 的 **Reader** |
 * | `RegisterClients` | **服务名** | 请求-响应**客户端** | `cfg.<名>.request` → **Writer**(发请求)　`cfg.<名>.response` → **Reader**(收应答) |
 * | `RegisterServices` | **服务名** | 请求-响应**服务端** | `cfg.<名>.request` → **Reader**(收请求)　`cfg.<名>.response` → **Writer**(发应答) |
 *
 * 四者可任意并存(一个节点常常兼任)。请求-响应两侧**传一模一样的服务名**,各自按角色建
 * 各自那一侧,不会填错方向、也不必协调。
 *
 * ## 参数含义按模式分家(**D8**)
 *
 * | 方法 | 第一参 |
 * |---|---|
 * | `Publish` / `Subscribe` | **topic**(永远) |
 * | `RequestForResultDirect` / `ServeRequests` | **服务名**(永远) |
 *
 * **`Subscribe` 保持通用**——它的第一参**永远是 topic**,`ServeRequests(名)` 是它在服务名
 * 一侧的封装(内部即 `Subscribe(cfg.<名>.request, kRequest)`)。
 *
 * ## 关联键 `correlation_id` 是两段式(**D6**)
 *
 * ```
 * correlation_id = "<uuid>#<request_seq>"
 *                     ↑          ↑
 *       节点构造时生成一次    uint32,从 0 开始自增,每请求一个
 * ```
 *
 * uuid 半段保证**跨节点**不撞,`request_seq` 保证**节点内**不撞。`uint32` 回绕(约 42.9 亿
 * 次请求后)**明确接受、不加防回绕逻辑**。
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
 * 的 topic 的全部**」——即 `Subscribers` ∪ 各已注册服务的 `cfg.<名>.request` ∪ 各已注册
 * 客户端的 `cfg.<名>.response`,**不是**"本 domain 上的全部"。未注册的 topic,其消息
 * **根本不会到达本进程**。
 *
 * ## 不管 transport 的生命周期
 *
 * 传输由**宿主**创建、`Start()`、`Close()`、`WaitClosed()`,本节点只按引用借用它
 * (与 `ProtocolNode` 同,ADR-0009)。`DoStart()` 只在其上逐项 `DeclareWriter` /
 * `DeclareReader`(**D15**:这是**唯一**建端点的地方),故**调用 `Start()` 之前宿主必须先
 * 把传输启起来**,否则声明一律返 `kInvalidState`。
 *
 * @warning `close()` 是**整流传播**的(AsyncTask `417790c` 起):`DoClose()` 关闭本节点这一路
 *          读订阅时,源读队列与同一条传输上的其它订阅者**一并终结**。节点关闭即读侧终结,
 *          宿主随后关传输。
 *
 * ## 框架不管的两件事
 *
 * - **重发要求对端能容忍重复请求**(幂等,或自行按 `correlation_id` 去重)。协议层假设,
 *   **框架不校验**(**D7**)。
 * - **共用应答 topic 带来读入放大**:同一服务的每个客户端都会收到该服务的**全部**应答,
 *   多余样本一路进读队列、解码后才在 `Dispatcher` 处落空——框架静默丢弃、不作记录
 *   (ADR-0014 D1)。
 *
 * 与传输、`Dispatcher` 一致,本类面向**单线程 fiber 协作**模型:交互方法运行于调用方
 * fiber,读-分发循环运行于自持 fiber,二者同线程且仅在挂起点交错,故普通成员不加锁。
 */

#include <cstdint>
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
#include "transport/core/Message.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/dds/DdsTransport.hpp"
#include "transport/node/NodeBase.hpp"
#include "transport/node/RetryPolicy.hpp"

namespace transport {

/// 参与 DDS 分发的字段:**topic + correlation_id + kind**(ADR-0013 **D6**)。
///
/// 三者分别是 **DDS 的寻址维度**、**其关联符**、**消息类别**;部分匹配由 `Dispatcher`
/// 实现,本类只需在键提取函数里给出各字段的具体值。
using DdsDispatcher = Dispatcher<Message, std::string /*topic*/,
                                          std::string /*correlation_id*/,
                                          MessageKind /*kind*/>;

/// DdsNode 配置——topic 一律由四个注册方法给出,不进配置(ADR-0013 **D16**)。
struct DdsNodeConfig {
  /// 节点 uuid:**非空则用它,为空才 `QUuid::createUuid()`**(**D6**)。
  ///
  /// **测试填固定值,生产留空**——`QUuid` 是随机的,前缀确定才能断言具体 `correlation_id`。
  std::string uuid_override;
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
   * @brief 注册**请求-响应客户端**——**只收服务名**,两个 topic 由框架派生(**D6**)。
   *
   * `cfg.<名>.request` 在 `DoStart()` 建 **Writer**(发请求)、`cfg.<名>.response` 建
   * **Reader**(收应答)。与服务端 `RegisterServices` **传一模一样的服务名**,各自按角色建
   * 各自那一侧(**D16**)。
   *
   * **应答 topic 是每服务一个、该服务的全体客户端共用的**(**D6**);不同服务的应答落在
   * 各自派生出的 topic 上,互不相扰。
   *
   * 批量 / 累加 / 幂等去重 / 整批生效,同 `RegisterPublishers`。
   *
   * @param service_names 待注册的服务名集合。
   * @return 成功;不在 `Created` 相位返 `kInvalidState`;下列任一非法返 `kInvalidArgument`
   *         (整批不落):
   *         - **服务名为空串**;
   *         - 某个服务名**已注册为 `Services`**——**自己请求自己**,且 `corr` 由自己生成、
   *           `Dispatcher` **会真的匹配上**,形成调用方毫无察觉的自问自答。
   *
   * @note **服务名除"非空"外不限制字符**。真正非法的 topic 名会由 provider 在 `Start()` 的
   *       `Declare*` 处显式报错,不会静默走坏。
   */
  [[nodiscard]] Coro::Result<void> RegisterClients(
      std::vector<std::string> service_names);

  /// @brief 注册**请求-响应服务端**——**只收服务名**:`cfg.<名>.request` 建 **Reader**
  ///        (收请求)、`cfg.<名>.response` 建 **Writer**(发应答)。校验与返回值与
  ///        `RegisterClients` 逐条对称(方向冲突查的是 `Clients`)。
  [[nodiscard]] Coro::Result<void> RegisterServices(
      std::vector<std::string> service_names);

  // ── 公开面:两种交互模式(D8)────────────────────────────────────────────

  /**
   * @brief 登记一个订阅——**取用入站消息的唯一入口**(ADR-0009 D1 / **D6**)。
   *
   * 两个键均可传 `kAny`。**交出去的订阅其 `corr` 位恒为 `kAny`**(见文件头的承重区别):
   * `correlation_id` 不进公开接口。
   *
   * **第一参永远是 topic,不因 `kind` 而异**(**D8**)。收某个服务的请求请调
   * `ServeRequests(服务名)`——它是本方法在服务名一侧的封装,内部即
   * `Subscribe(cfg.<名>.request, kRequest)`。
   *
   * | 用途 | 这样调 | 实际键 |
   * |---|---|---|
   * | 订阅某 topic 的通知 | `Subscribe("t", MessageKind::kNotify)` | `{"t", kAny, kNotify}` |
   * | 收某服务的请求 | `ServeRequests("get")` | `{"cfg.get.request", kAny, kRequest}` |
   * | 收全部(已注册 reader 的)topic 的通知 | `Subscribe(kAny, MessageKind::kNotify)` | `{kAny, kAny, kNotify}` |
   *
   * 消费在**调用方自己的 fiber** 内进行,节点不代管;串行、异常隔离与信箱容量之外的背压
   * 一律是调用方契约(RT_INBOUND_005)。**多 topic 用多次 `Subscribe`,每 topic 一条消费
   * fiber**——各自独立信箱,一路慢不拖累另一路。
   *
   * ## 相位规则:**只在 `Running` 放行**
   *
   * | 相位 | 返回 |
   * |---|---|
   * | `Created`(尚未 `Start()`) | `kClosed` |
   * | `Running` | 放行 |
   * | `Closing` / `Closed` | `kClosed` |
   *
   * **必须在 `Start()` 之后订阅**——`Created` 期订阅是**禁用法**,不是“早一点也行”。注册面
   * 与订阅面**互不重叠**:注册只在 `Created`、订阅只在 `Running`。推荐写法是紧挨着的两句:
   *
   * ```cpp
   * (void)node.Start();
   * auto ticket = node.Subscribe("telemetry", MessageKind::kNotify);
   * ```
   *
   * `DataReader` 建于 `DoStart()`,而 DDS 发现约需 **~240ms**,故 `Start()` 之后紧接着
   * 订阅不会漏收启动初期的消息。
   *
   * @param topic 具体 topic,或 `kAny`。
   * @param kind  具体 `MessageKind`,或 `kAny`。
   * @return 订阅凭据(析构时自动注销);`kClosed`(未启动 / 已关闭)——**先于**下面的注册
   *         校验,故未启动或已关闭的节点上订阅未注册的 topic 报 `kClosed`,不报
   *         `kConfiguration`;topic 未注册为对应角色返 `kConfiguration`:
   *         `kNotify` 须已注册为 `Subscribers`,`kRequest` 须是某个已注册服务的
   *         `cfg.<名>.request`,其余 `kind` 须至少在读侧集合内(`Subscribers` ∪ 各服务的
   *         `cfg.<名>.request` ∪ 各客户端的 `cfg.<名>.response`)——不在读侧的 topic 其消息
   *         根本不会到达本进程,订阅它必然是**静默无效**。
   *         **topic 传 `kAny` 时跳过该校验**:`kAny` 不对应任何一个具体 topic,拿它去查
   *         注册表必然落空;它的作用域本就已由注册天然限定(见文件头的限制一节)。
   */
  [[nodiscard]] Coro::Result<Ticket> Subscribe(TopicKey topic, KindKey kind);

  /**
   * @brief 单向发布一条 `kNotify`(fire-and-forget,发布-订阅):不登记任何订阅、不期待应答。
   *
   * 本节点盖 `kind = kNotify` 并**清空** `correlation_id` / `reply_to`(**D6**)。
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
   * 步骤:查 `service_name` 是否已注册为 `Clients`(**查不到即 `kConfiguration`,不猜、
   * 不回落**)→ 派生出 `cfg.<名>.request` / `cfg.<名>.response` → 盖 `kind` / `corr` /
   * `reply_to` → **先登记订阅再发出** → 编码**一次**、重发复用同一份字节 → 首个到达即成功。
   *
   * **第一参是【服务名】,不是 topic**(**D8**):派生规则不外泄到调用方。
   *
   * **签名里没有 `result_timeout`**:本交互只有**一个**等待阶段,其时限即 `retry.timeout`;
   * 耗尽返 `kTimeout`(本模型没有受理阶段,故不用 `kNotAccepted`)。
   *
   * **`RELIABLE` 的 DDS 上仍要重发**:丢的不是网络,是**队列**——读队列有界 1024、满时
   * 静默丢最旧(**D11**)。代价是**要求对端能容忍重复请求**,框架不校验。
   *
   * @param service_name 服务名,**须已注册为 `Clients`**。
   * @param req   请求 Message(`payload` 由调用方填;`kind` / `correlation_id` /
   *              `reply_to` / `topic` 由本节点盖)。
   * @param retry 重发策略,见 `RetryPolicy`。
   * @return 收到的 `kReply`(其 `topic` 是 `cfg.<名>.response`);或 `kTimeout`(重发次数
   *         耗尽)、`kInvalidArgument`(策略非法)、`kConfiguration`(服务名未注册为客户端)、
   *         `kClosed`、编码错误。
   */
  [[nodiscard]] Coro::Result<Message> RequestForResultDirect(
      const std::string& service_name, Message req, RetryPolicy retry);

  /**
   * @brief 服务端收请求——`Subscribe(cfg.<名>.request, kRequest)` 的**服务名封装**(**D8**)。
   *
   * **不是另一套机制**:它派生出请求 topic 之后原样交给 `Subscribe`,相位与注册两道校验
   * 也都落在那里。
   *
   * 消费方式与 `Subscribe` 完全相同:凭据交给调用方自己的 fiber 顺序消费,收到 `kRequest`
   * 后调 `Reply(request, result)` 回一条终结应答。
   *
   * @param service_name 服务名,**须已注册为 `Services`**。
   * @return 订阅凭据;`kClosed`(未启动 / 关闭中 / 已关闭,**先于**注册校验)、
   *         `kConfiguration`(服务名未注册为服务端——空串亦然,它永远注册不上)。
   */
  [[nodiscard]] Coro::Result<Ticket> ServeRequests(
      const std::string& service_name);

  /**
   * @brief 服务端回一条终结应答 `kReply`——请求-响应服务端**唯一**的方法(无受理阶段)。
   *
   * 它由 `request.topic`(即派生出的 `cfg.<名>.request`)**反查自己注册的服务**,再派生出
   * 该服务的 `cfg.<名>.response`(走的是**同一个派生函数**,不另写解析器)。**不取信于
   * 线缆、不建端点**(**D15**):该应答 topic 的 writer 早在 `DoStart()` 就建好了,故服务
   * 的**第一次应答也不会丢**。
   *
   * 线缆上的 `reply_to` 降为**一致性交叉校验**:非空且与派生出的应答 topic 不等即返
   * `kInvalidArgument`——对**版本不一致的对端**,它是唯一能当场发现偏差的手段。
   *
   * @param request 收到的请求(其 `topic` 与 `correlation_id` 是本方法的全部输入)。
   * @param result  应答 Message(`payload` 由调用方填;`kind` / `correlation_id` / `topic`
   *                由本节点盖)。
   * @return 已入队;`kClosed`、`kConfiguration`(`request.topic` 不是本节点任何一个已注册
   *         服务的请求 topic)、`kInvalidArgument`(`reply_to` 交叉校验不过)、编码错误。
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
  ///        一律静默丢弃,`kReply` 与业务消息无别(ADR-0009 D5、ADR-0014 D1)。
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
  /// @brief 该 topic 是否在**读侧**集合内(`Subscribers` ∪ 各服务的 `cfg.<名>.request` ∪
  ///        各客户端的 `cfg.<名>.response`)——即"它的消息有没有可能到达本进程"。
  [[nodiscard]] bool IsReaderSideTopic(const std::string& topic) const;

  DdsTransport& transport_;  ///< **借用**:宿主拥有并启停,寿命须长于本节点。
  std::unique_ptr<ICodec> codec_;
  DdsNodeConfig config_;

  /// 本节点 uuid,**构造时生成一次、此后不变**(**D6**):`config_.uuid_override` 非空则用
  /// 它,为空才 `QUuid::createUuid().toString(QUuid::WithoutBraces)`。
  std::string uuid_;
  /// `correlation_id` 的自增半段(**D6**)。**与 `Message::session_id` 无关**——后者是
  /// **外部协议**的匹配键,DDS 路径留缺省 `0`。
  std::uint32_t request_seq_{0};

  // —— 四组注册表(**D16**)。`Start()` 之前填,此后只读;`Start()` 失败**不清空**。
  //    请求-响应两组存的是**服务名**,两个 topic 一律由 `DeriveServiceTopics` 现算
  //    (**D6**)——不缓存派生结果,免得同一事实存两份、将来改派生规则时漏改一处。——
  std::set<std::string> publishers_;   ///< topic → Writer。
  std::set<std::string> subscribers_;  ///< topic → Reader。
  std::set<std::string> clients_;      ///< 服务名:request → W(发请求)、response → R(收应答)。
  std::set<std::string> services_;     ///< 服务名:request → R(收请求)、response → W(发应答)。

  /// 本节点在传输 `read_queue` 上的订阅(`shared()`);`DoClose` 关闭之——**关闭是整流
  /// 传播的**,连源队列与其它订阅者一并终结(见文件头 warning)。
  std::shared_ptr<Coro::Awaitable<Datagram>> rx_;
  /// 读-分发循环的结构化并发句柄;`DoJoin()` 让出式 join 之。
  std::shared_ptr<Coro::FiberTask<void>> read_task_;

  /// 入站的**唯一**投递路径:`RequestForResultDirect` 与宿主的 `Subscribe` 都在此登记。
  DdsDispatcher dispatcher_;
};

}  // namespace transport
