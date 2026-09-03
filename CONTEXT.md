# CONTEXT — 通信中间件框架 术语表（ubiquitous language）

本项目的规范术语。issue 标题、重构提案、假设、测试名等一律用此处定义的词,不要漂移到同义词。
标注 **[as-built]** = 当前实现(v0.3.0);**[target]** = 协程原生目标架构(见 `docs/adr/0001-*`、`docs/需求规格说明书-协程原生.md`)。无标注 = 两者通用。

## 三层

- **Transport（传输）** —— 纯字节收发能力,不知 `Message`、线缆格式或交互模式。
  - **[as-built]** 回调式:`OnBytes(bytes, from)` 交付、`Send()` 立即返回;底层 QtNetwork。
  - **[target]** 面向 node 提供介质无关的协程式收发；内部以 `ITransport` 组织实现，但 `ITransport` **不是用户公共 API**，其名称和方法属于设计说明。
- **编解码器（Codec）** —— 用户可扩展的公共线缆格式能力，在逻辑消息与线缆字节之间转换；流式格式可跨接收片段拼帧，报文式格式保持报文边界。公共抽象在设计中命名为 `ICodec`，具体 C++ 签名属于 API/设计说明。
- **node（交互节点）** —— 交互编程主入口:`ProtocolNode`(外部协议)、`DdsNode`(DDS)。
  - **[as-built]** = 共享 `InteractionEngine` + 声明式 `InteractionPolicy` 的薄壳;用户**继承**重写钩子。
  - **[target]** = 应用以**组合**方式配置和调用节点，不需继承框架节点类型；具体 handler 和请求 API 属于设计说明。

## 数据

- **逻辑消息** —— 编解码边界上的消息语义：`payload` + 当前协议所需的类别、关联、来源和目的信息；不要求 TCP/UDP/串口/DDS 共用一个包含所有字段的 C++ 结构。
- **Endpoint** —— 中立寻址值类型:`Default` / `Net(ip,port)` / `Topic(name)`。
- **Datagram** —— **[target]** 传输层数据面的**唯一**载荷类型 `{bytes, peer}`,读写共用:`AsyncRead()` 交出的队列里 `peer` 是**发送方**,`AsyncWrite()` 送入的 `peer` 是**目的地**——方向由使用它的接口决定,不需要两个结构相同的类型来编码(原 `SendUnit` 已于 ADR-0008 D8 合并进本类型)。UDP/DDS 一次一完整报文(`peer` 可变);TCP/串口一次一任意字节切片(`peer` 恒为对端)。
- **错误类别** —— 用户可机器判别的稳定失败语义：`InvalidArgument`、`InvalidState`、`Configuration`、`Connection`、`Closed`、`Timeout`、`Cancelled`、`Io`、`Frame`、`Codec`、`ResourceExhausted`、`Unsupported`、`Internal`；可附带诊断文本。具体错误码、枚举和结果载体属于 API/设计说明，调用方不得依靠解析文本前缀分类。
- **匹配键（Key）** —— 请求↔应答配对键。**[target]** 不再是压成单值的机器键,而是 `Dispatcher` 的**订阅模式**:逐字段给出约束或 `kAny`(外部协议 = `(session_id, message_id, frm_type)`;DDS = `correlation_id`)。见「按键分配」。
- **判别符** —— 帧类型:外部协议 `FrameType`(COMMAND/RESPONSE/RESULT/STATE/HEARTBEAT);DDS `MessageKind`。

## 机制

- **InteractionEngine / InteractionPolicy** —— **[as-built]** 共享交互引擎(挂起表/超时/重发/分发)+ 声明式协议策略(TagOf/KeyOf/NewCorrelation/EchoCorrelation/ReplyTo/RouteUnmatched)。**[target] 均去除**——语义内联进各 node。
- **IExecutor / ThreadExecutor** —— **[as-built]** 执行器缝 + 单 worker 线程池(决定业务回调在哪跑 + 定时)。**[target] 去除**——并发交给协程运行时。
- **协程运行时** —— **[target]** 框架统一的 M:N 协作式并发与时序环境；**AsyncTask 是需求级强制技术约束，不是可替换的推荐实现**。不同 node 可并行，同一 node 的消息分发和业务处理保持串行语义；具体 affinity、线程分配、调度和构建方式属于设计说明。
- **节点所属执行域** —— **[target]** 节点首次成功启动后绑定的稳定 AsyncTask 执行上下文。不同 node 可位于不同运行线程；同一 node 的可变状态在该域内串行访问，节点运行期间不迁移。
- **入站业务处理器** —— **[target]** 应用以组合方式注册、在 AsyncTask fiber 中处理普通入站业务消息的逻辑。它不是 Qt/DDS 底层回调；同一 node 同时只运行一个，其他 node 可并行。
- **请求等待 fiber** —— **[target]** 发起请求并等待唯一响应、超时、取消或连接错误的调用方协程。
- **连接状态观察器** —— **[target]** 观察 TCP 物理连接状态变化的应用能力，不承载入站业务处理。
- **连接代际** —— **[target]** 每次成功建立 TCP 物理连接后形成的隔离标识；旧代际的迟到数据和事件不得影响新代际。
- **配置版本** —— **[target]** 宿主提交的单调 TCP 连接管理配置快照版本；与连接代际是两个独立概念。
- **coro socket** —— **[target]** AsyncTask 封装的协程 I/O:`corosocket`(QAbstractSocket 流式读写)、`coroiodevice`(QIODevice/串口)、`corotcpserver`(accept)、`coroudpsocket`(保持边界和地址 metadata 的 UDP datagram)。目标 Transport 复用这些 awaitable；DDS provider 的非 Qt listener 仍需单独的线程交接边界。
- **DDS provider 交接边界** —— **[target]** DDS provider listener 样本安全进入节点所属执行域的行为边界；必须有界、非阻塞 listener、同 topic 保持框架接受顺序。它不要求存在名为 `DdsBridge` 的组件；具体投递机制、容量和溢出策略属于设计说明及尚未关闭的需求项。本地已经丢弃的样本不能由 DDS Reliable 自动恢复。
- **provider** —— DDS 底层库的抽象适配(`IDdsProvider`):Fast DDS / 进程内 `FakeDdsProvider`。
- **发送完成语义** —— **[target]** **已于 ADR-0008 D4 改为彻底的 fire-and-forget**:`AsyncWrite()` 只判"生命周期是否允许写、是否真的入队",返回成功仅表示已受理,**不表示已发出**;目的地能否解析、socket 是否写成一律不回传,只落 `LastError()`。链路不可用时数据留在内部队列等待恢复,不拒绝、不丢弃。「帧字节进入操作系统发送缓冲才算完成 + 协程背压」早在 2026-08-06 需求重审(ADR-0004)即已删除,框架自那时起就无背压;ADR-0008 D4 取消的是**剩下的同步作答口子**(目的地非法、报文超长等参数错误),故写侧现已无任何同步错误面。
- **发送排序** —— **[target]** = **节点执行域到达顺序**:单 fiber 程序序必被保持;跨 fiber 并发发送取得某个一致全序,但不可由调用方墙钟时序预测。非"跨调用方全局 FIFO"。见 RT_TRANSPORT_007。
- **读-分发循环** —— **[target]** node 内联的一条长寿 fiber(非独立引擎):`await(自己的读订阅) → ICodec.Decode() → Dispatcher 按键投递 → 无人认领的终结帧归因丢弃、其余业务帧入队交处理器串行消费`。是"无共享引擎、语义内联各 node"(RT_DESIGN_003 / ADR-0001 D2)的落地形态。读订阅取自 `AsyncRead()->shared()`,节点关闭时只断自己这一路,不波及传输与其它订阅者(ADR-0008 D5)。
- **NodeBase** —— 交互节点的生命周期基类(非模板),四个公开方法 + 三个钩子:`Start()` / `Close()`(**只发信号**) / `WaitClosed()`(join,无时限) / `IsRunning()`,协议特有实事由子类经 `DoStart()` / `DoClose()` / `DoJoin()` 提供。`ProtocolNode`/`DdsNode` **继承**它。
  `Close()` 不含等待点,故**任何 fiber 都可调用**(含节点自己的读循环与业务处理器)——旧形态的 `SignalClose()` / `ConvergeAfterReadLoop()` / `MarkRunning()` 与"内部工作单元不得调 Close"的使用契约一并作废(ADR-0008 D2)。取代原 `NodeRuntime`(ADR-0006 D1/D5 已拆除删除)。
- **HandlerLoop** —— 入站业务处理的**可选**小件:单消费者 fiber + 一条 `Coro::Awaitable` 业务队列 + 协作取消令牌 + 逃逸异常隔离。由 node **直接持有**(未设处理器的节点没有它),故**不进** `NodeBase`。队列的容量语义即 `FiberChannel` 的语义——**满时静默丢弃队首最旧的事件**,无计数、无归因,字节上界不再存在(ADR-0008 D8;见 #152)。
- ~~**丢弃归因**~~ —— **本条撤销**（**ADR-0014**，2026-09-03）。框架**不再提供任何可观测性**：`ITraceSink` / `Observability` / `TraceCategories` / `DropReason` 一并删除，`Cancellation` 同轮删除。
  **框架内部的丢弃自此完全静默**——队列满丢最旧、坏帧、迟到/无匹配响应三条路径均无归因、无出口，**排障只能靠宿主自己**在 codec 或订阅侧加日志。
  **撤销依据**：ADR-0003 D13 定的双面观测早已塌缩（pull 面随 ADR-0008 D10 删尽、队列丢弃随 D8 换 `FiberChannel` 后既无计数也无归因、#152 裁决"不加归因"、#212 把归因项六减二）；**要么留一个完整可信的观测面，要么删干净，留半条最坏**。

- **按键分配（Dispatcher）** —— **[target]** 协议无关的入站消息路由器 `Dispatcher<T, Fields...>`(`transport/core/Dispatcher.hpp`),取代原 `PendingTable` 与 `CorrelationKeyStrategy`。调用方只提供**键提取函数**(给出一条消息各匹配字段的具体值),订阅时不参与匹配的字段填 `kAny`。一条消息投给**全部**键匹配的订阅者、各得一份,故支持多消费者与旁路监听;单条消息成本 O(在用 mask 种数 + 收件人数),与订阅者总数无关。见 ADR-0008 D6。
- **kAny（通配）** —— **[target]** 订阅模式中标注"该字段不参与匹配"的具名标记。以 `std::optional` 的持值状态表达,**不占用字段值域**——故 `session_id` 这类 0..255 全用满的字段同样可以通配。`kAny` 与"该字段须等于 0"是两种不同的约束。
- **订阅凭据（Ticket）** —— **[target]** `Dispatcher::Subscribe()` 的返回值:持有一个信箱并在析构时注销该订阅。信箱为队列语义,同一凭据可多次 `Wait()`——一次交互需分段等待多条报文时,各段各自登记、各自设定时限。

## 边界与非目标

- 框架**不解释 payload 业务语义**、**不抛异常**、**不是消息代理/路由守护进程**。
- `ITransport` 是库内部设计缝；编解码器则是用户可实现和注入的公共扩展点。
- `frm_type` 真实字节值与 CRC 算法为**外部常量**,经枚举占位 + `CrcFn` 注入预留,接入前替换、两端一致。
