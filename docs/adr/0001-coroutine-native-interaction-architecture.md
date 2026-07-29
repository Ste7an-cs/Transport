# ADR-0001：协程原生交互架构

**状态：** Accepted（草案 / 目标架构，尚未实现）
**日期：** 2026-07-17
**关联：** `docs/需求规格说明书-协程原生.md`（本架构的 SRS）；as-built 存档于 git tag `v0.3.0`（见 `CHANGELOG.md`）。

## 背景（Context）

as-built v0.3.0 的交互层是**异步栈**:`IExecutor`/`ThreadExecutor`(单 worker 线程池)+ 共享 `InteractionEngine` + 声明式 `InteractionPolicy` + QtNetwork **回调式** `ITransport`(`OnBytes`);另有一个 Phase 2 的 `coro::InteractionEngine`(复用回调 `ITransport` 经桥,仍是"引擎"形态)。

目标是把交互层**全栈协程原生化**:以 AsyncTask(boost.fiber)协程运行时取代执行器;请求-应答写成线性异步过程。本 ADR 记录多轮需求 grilling 形成的关键架构决策和事实修正。

## 决策（Decision）

**总纲:** 保留 `ITransport` 抽象(改协程 await 式)+ AsyncTask 运行时取代执行器 + 各 node 自实现交互模式(仅共享一个纯挂起原语)。逐条:

- **D1（去引擎到什么程度）：** 保留**薄基座、非引擎**。抽出通用机制、协议语义留各 node。**否决**"彻底各写各的"(会把最易出并发 Critical 的机制复制 N 份——CHANGELOG 记录当年 `CommNode`/`ProtocolNode` 各写一套、各被抓一个并发 Critical),也**否决**"保留极简引擎"(回到 Phase 2 形态)。

- **D2（基座边界）：** 基座 = 纯 **`PendingTable<Key, Message>`** 挂起-应答原语(`register→Awaitable` / `resolve` / `failAll` + `conn:`vs`timeout:` + 取消纪律)。**读-分发循环、`decode`、算 key、判终结、5 种发送模式、心跳——全部内联在各 node**。同一 node 的交互状态必须串行访问；是否用 affinity、mailbox、strand 或锁来满足该要求属于 SDD。若让基座跑读循环则必须回调 policy(KeyOf/IsTerminal/RouteUnmatched)→ 塌回 engine+policy,故读循环留 node。

- **D3（节点-连接拓扑）：** **node 恒绑一个 `ITransport`**。"多对端"下沉到寻址(UDP `Endpoint::Net`/`reply_to_source`、DDS topic/`reply_to`);"多连接"下沉到 **TCP 服务端 accept 循环 + 每连接派生一个 node**。否决"一个 node 持连接表"。

- **D4（DDS provider 交接边界，2026-07-21 修订）：** SRS 不设名为 `DdsBridge` 的独立架构组件，只规定 provider 可从任意线程交付时，样本必须经**有界、线程安全且不阻塞 listener**的边界进入节点所属执行域，并保持同 topic 的框架接受顺序。端到端可靠性必须区分 DDS QoS 与框架本地队列：样本一旦在 listener 后被本地丢弃，DDS Reliable 不能自动恢复。具体容量和溢出策略尚为 TBD，旧稿“满则丢最旧且容量等于 history_depth”不再是已接受需求。

- **D5（Close/取消纪律，2026-07-21 修订）：** 节点生命周期为 `Created → Running → Closing → Closed`。`Close` 幂等且立即拒绝新操作、终结未决请求、中止 I/O，不默认排空；“完全关闭等待”在所有内部任务和处理器退出后完成并支持多等待者。入站业务处理器采用协作取消，框架不得强杀 fiber；处理器自身发起关闭不得自等待死锁。取消或超时发生在帧已经开始写入后时，不截断健康连接上的当前帧；部分写自身失败则关闭物理连接。

- **D6（用户面）：** **组合 + 注册 handler**，**不继承**。SRS 将其表述为外部集成约束：应用创建和配置节点即可使用，不需继承框架类型或重写虚函数；具体 `OnCommand`/`Request` 名称、函数签名、handler 类型和生命周期归入 SDD/API 设计。否决"继承虚钩子"式。

- **D7（协程技术基线）：** **AsyncTask 是目标架构的强制技术约束，而非当前推荐实现。** SRS 仅规定必须采用 AsyncTask、不得以独立线程执行器承担业务调度；`installFiberApplication`、`makeTask`、affinity、`Awaitable`、Qt 事件推进及 CMake 接入等具体机制归入 SDD。改变该选择会影响传输、节点、调度和构建，须重新评审架构。

- **D8（传输抽象的可见性）：** 目标架构仍可在设计中使用 `ITransport` 统一 TCP/UDP/串口/DDS，但它**只供库内部 node 与传输实现协作，不属于用户公共 API**。SRS 只规定多介质收发、协程等待、来源标识、寻址和错误行为；`ITransport` 名称、C++ 签名、对象持有方式及 coro socket 适配结构归入 SDD。否决把内部装配缝暴露为需求级公共扩展点。

- **D9（编解码扩展边界）：** 编解码能力是**用户公共扩展点**，用户应能提供自定义线缆格式并装配到通信节点。SRS 规定可扩展性、逻辑输入输出、流式/报文式边界及错误行为；`ICodec` 类定义、C++ 方法签名、滚动缓冲和 CRC 适配结构归入 SDD/API 设计。该定位与仅供内部使用的 `ITransport` 不同。

- **D10（挂起机制的文档边界）：** `PendingTable` 是 SDD/ADR 层面的内部设计，不是 SRS 需求或领域术语。SRS 只规定并发请求可关联、重复键不得覆盖、冲突请求不得发送、请求恰好终结一次、终结后释放资源，以及关闭时收敛全部未决请求；`PendingTable` 名称、模板参数、map/channel、`register`/`resolve`/`failAll`、erase 和锁策略均归入 SDD。本 ADR 保留选择共享薄基座而非复制机制或恢复引擎的理由。

- **D11（线缆协议的文档边界）：** 不另立接口控制文档（ICD）。外部协议帧的精确字段、偏移、宽度、字节序、同步头、长度、CRC、枚举编码和最大帧长继续归入 SRS 3.2 外部接口需求，因为它们是通信对端可观察且必须互操作的契约，而非软件内部设计。编解码器如何解析、缓存和重同步仍归入 SDD。

- **D12（错误契约的文档边界）：** SRS 规定配置、帧、编解码、连接、超时、I/O、繁忙/关联冲突等稳定错误类别、触发条件和异常行为，并要求调用方可机器判别；不把 `config:`/`conn:` 等字符串前缀固定为新架构需求。错误枚举、错误码、字符串兼容层、`Result<T>`/`Status` 表示和诊断文本格式归入 SDD/API 设计。

- **D13（并发语义）：** AsyncTask 按其本身的 **M:N** 模型使用，不把目标架构限制为单全局调度线程。不同 node 允许并行推进；同一 node 的消息分发、关联状态更新和业务 handler 保持串行语义。SRS 规定该可观察顺序与无数据竞争要求；`Shared`/`Sticky`/`FixedId` affinity、Qt 对象线程归属及串行化实现归入 SDD。

- **D14（节点执行域与 handler）：** 节点首次成功启动时自动选择、或由宿主显式指定稳定的所属执行域，运行期间不迁移。节点公共异步操作可由任意 AsyncTask fiber 发起并转交所属域。同一节点的“入站业务处理器”严格串行，即处理器 await 时也不启动下一条；请求响应匹配和关闭控制独立推进。底层 Qt/DDS callback 不直接执行业务处理器。

- **D15（请求终结与迟到响应）：** 请求总超时从节点接受请求时起算，覆盖发送排队、写入和等待响应。取消、超时、关闭、断连和成功响应竞争时仅允许恰好一次完成。迟到、重复、无匹配和旧连接代际响应直接丢弃并观测，不进入业务处理。不得自动重放已发送请求。

- **D16（TCP 客户端重连）：** 自动重连是 TCP 客户端固定能力，不以动态 `auto_reconnect` 开关控制。断连立即以 `Connection` 终结在途请求；重连期间新业务发送立即失败而不在节点缓存。物理连接具有独立于节点生命周期的状态和连接代际；旧代际事件不得污染新代际。退避基线为 1 s、×2、上限 30 s、±20% jitter，稳定 60 s 后重置。

- **D17（运行时重配置）：** 配置变更与自动重连是两个需求。宿主显式提交带单调版本的完整 TCP 连接管理快照；先完整校验再原子应用。端点变化切换连接代际并立即尝试新端点，旧代际在途请求终结；仅策略参数变化不取消已经开始的连接尝试或退避，下一动作使用新参数。配置应用成功不等于物理连接成功。

- **D18（错误模型）：** 需求层使用稳定、机器可判别的错误类别，而不是 `config:`/`conn:` 等文本前缀。预期失败通过非异常结果返回；意外越过业务处理器边界的异常转换为 `Internal` 并隔离当前事件。确切枚举、`error_code` 和结果类型留给 API/SDD。

- **D19（有界入站处理）：** 每节点业务队列同时受事件数和字节数限制，任一达到即满；默认 1024 事件/16 MiB。满时拒绝最新事件并报告 `ResourceExhausted`，不得阻塞 I/O listener、关闭节点或阻断请求响应匹配。控制、响应匹配、业务处理、非关键观测依次具有语义优先级，但正常容量内已入队业务不得无限期饥饿。

- **D20（编解码恢复）：** 坏帧不进入业务处理。同批输入中的坏帧单独丢弃并报告，坏帧前后合法帧继续交付；不设置固定坏帧率关闭阈值。流式缓存必须有界，无法恢复同步或底层致命 I/O 才关闭物理连接。具体 resync 算法属于 SDD。

- **D21（SRS 量化基线）：** 性能验收同时包含内存 Fake 框架基线和真实介质集成；目标参考机为 FT-2000/4 四核 2.2 GHz、8 GiB，实际部署峰值 20 节点×100 Hz，并以 30 节点×100 Hz 作暂定容量余量。时延、CPU、内存、稳定性和关闭时延的暂定值记录在 SRS，待软件环境固化后复核。

**事实修正（F1，2026-07-21 再修订）：** AsyncTask `67b71a7` 已提供专门的 `CoroUdpSocket::receiveDatagram()`，返回保留 payload、发送方/目标地址和端口 metadata 的 `QNetworkDatagram` 流，并具备确定性关闭语义。故目标 `UdpTransport::Read()` 应复用该 awaitable，不再自建通用 Qt signal→fiber 桥；TCP/串口分别复用 `corosocket`/`coroiodevice`。DDS provider listener 的非 Qt 线程交接仍按 D4 单独设计。

## 影响（Consequences）

- **正面:** 危险的挂起-应答纪律只写一处(`PendingTable`),避免重蹈当年双并发 Critical;协议语义清晰归各 node;内部 `ITransport` 抽象保留分层与可测性；M:N 运行时允许 node 间并行，而 node 内串行语义控制状态复杂度；连接代际使重连和热更新的迟到事件可隔离。
- **代价/风险:** AsyncTask socket awaitable 的超时不会取消底层操作，Transport 必须显式管理来源生命周期；DDS 本地交接仍需关闭容量/溢出策略 TBD；服务端"每连接一 node"要管好 per-connection node 生命周期；严格串行业务处理意味着慢 handler 会形成队列压力，必须依靠有界队列与观测暴露。
- **可逆性:** D1/D2(基座边界)与 D6(用户面)相对可逆;D3(拓扑)、D4(桥策略)、D5(Close 纪律)是行为契约,较难改。
- **SRS 落点:** `RT_CORO_RUNTIME`、`RT_TRANSPORT`、`RT_CODEC`、`RT_REQUEST`、`RT_HANDLER`、`RT_LIFECYCLE`、`RT_TCP_RECONNECT`、`RT_TCP_RECONFIG`、`RT_NODE`、`RT_ERROR_TRACE`、§3.3 和 §3.5。

## 尚未解决

- 迁移分期(每期可测边界)——留待 writing-plans。
- ~~桥"跨线程唤醒 fiber"的具体机制(裸 boost fiber channel 从外线程 push 是否安全 / 锁队列 + 往调度线程 post)。~~ **已由 P4-4(#71)关闭 = 安全**:AsyncTask `Awaitable` 底层 `FiberChannel` 用 `boost::fibers::mutex/condition_variable`,注明"跨线程安全";DDS provider listener 线程直接 `BoundedQueue::Push`(复用 P2 件)唤醒调度器线程上的消费 fiber,1000 轮真跨线程压测无崩溃/无丢唤醒。**无需 Qt QObject 桥、无需改 BoundedQueue**(见 ADR-0003 D12)。
- `PendingTable` 是否需支持一键多待(同 key 并发)——目前 `(session_id,message_id)` 键空间约束下不需要。
- ~~DDS provider 本地交接容量与溢出策略。~~ **已由 ADR-0002 D4 关闭**（有界 + tail-drop，默认 1024 样本/16 MiB 可配）。
- ~~多线程/多 fiber 并发发送的跨调用方顺序。~~ **已由 ADR-0002 D1 关闭**（= 节点执行域到达顺序）。
- 五种外部协议交互状态机、正式帧类型值和 CRC 契约。
