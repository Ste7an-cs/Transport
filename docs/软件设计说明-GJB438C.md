# 软件设计说明（SDD）

**文档标识：** transport-SDD-438C　**版本：** v2.1　**日期：** 2026-08-06
**编制依据：** GJB 438C《军用软件开发文档通用要求》——软件设计说明模板（对齐 AsyncTask SDD 惯例：CSC 部件 / CSU 单元 / JK 接口 / MS 执行方案 / DD 设计决策 + 双向追溯）
**对应软件基线：** master（里程碑 P0–P5 已交付，版本 `v0.4.5`+）
**需求基线：** `docs/需求规格说明书-协程原生.md`（SRS，标识前缀 `RT_`）　**决策依据：** `docs/adr/`（ADR-0001/0002/0003/0004/0005/**0006**）

> 说明：本文件按 GJB 438C 软件设计说明模板组织，描述 `transport` 通信中间件 CSCI 依据当前实现（as-built）的体系结构与详细设计。ID 体系与追溯方式对齐参考范本 `third_party/AsyncTask/doc/软件设计说明.md`。图形以 Mermaid 源（`docs/diagrams/*.mmd`）渲染的 SVG 给出，含数据流图、类图、时序图、状态图。本文件与路线图文档 `docs/设计说明书-协程原生.md`（分期路线图权威）互补：后者按期推进，本文件是按当前实现回填的结构化设计说明。

---

## 1. 文档标识

- 文档名称：transport 软件设计说明（SDD）
- 对应软件：transport C++17 协程原生通信中间件库（命名空间 `transport::`）
- 对应需求：`docs/需求规格说明书-协程原生.md`（SRS），本文档与其构成追溯关系（见第 6 章）
- 版本：见代码仓库提交记录与 `include/transport/core/version.hpp`

## 2. 引用文档

本设计说明为自包含文档：实现所需的结构、算法、图示均在本文内给出。

| 编号 | 文档 |
|---|---|
| REF-1 | `docs/需求规格说明书-协程原生.md` 软件需求规格说明（SRS）——本设计的需求依据 |
| REF-2 | `docs/adr/0001..0006` 架构决策记录（协程原生总纲 / 发送-丢弃-生命周期 / 目标架构与路线图 D1–D13 / 传输语义统一与链路可用性 / 收敛并入读循环与固定间隔重连 / **NodeBase 模板方法与共享完成量轻量化**） |
| REF-3 | `docs/设计说明书-协程原生.md` 目标架构 + 分期路线图（路线图权威） |
| REF-4 | `third_party/AsyncTask/doc/{需求规格说明,软件设计说明,使用说明}.md` AsyncTask 运行时文档 |
| REF-5 | `CODING_STANDARDS.md` 编码规范；`CONTEXT.md` 术语权威 |
| REF-6 | ISO/IEC 14882:2017（C++17）；Boost.Fiber（≥1.89.0）；Qt 5（≥5.12）；Fast DDS 2.13.x（可选） |

## 3. CSCI 级设计决策

> ### 变更说明（ADR-0008，2026-08-19）
>
> 本章及第 4、5 章的下列内容已被 **ADR-0008「接口重设计」** 推翻或改写，阅读时以 ADR 为准：
>
> | 本文位置 | 变更 |
> |---|---|
> | DD-3 / DD-4 的"协议无关基座"清单 | `PendingTable`、`BoundedQueue`、`SharedCompletion` **已删除** |
> | DD-8（挂起-应答仲裁） | 由 `Dispatcher`（按键分配）取代，见 §5.2 |
> | DD-13（收敛并入读循环） | **推翻**：`Close()` 改为只发信号，收敛由 `WaitClosed()` 的调用方经 `DoJoin()` 自上而下 join |
> | §4.3.5（传输内部接口） | `ITransport` 收窄至七个方法，签名全改 |
> | §5.2 / §5.3 | `CSU_PENDINGTABLE` / `CSU_BOUNDEDQUEUE` 已删除，代之以 `CSU_DISPATCHER` |
> | §5.4（NodeBase） | 五个反向回调删除，公开面收为 4 方法 + 3 钩子 |
> | 全文的 `Result`/`Status`/`OperationOptions`/`SendUnit` | 分别改为 `Coro::Result<T>` / `Coro::Result<void>` / `std::chrono::milliseconds` / 合并进 `Datagram` |
>
> 当前编译面收窄为 **UDP + ProtocolNode**；TCP / 串口 / DDS 及其节点按同一形态后续跟进，
> 其相关小节暂保留原文（描述的是重设计之前的形态）。

从设计视角说明各构件如何满足需求及其设计原则；每条决策标注所满足的 SRS 需求，可回溯 ADR（REF-2 决策编号 Dn）。

- **DD-1 三层解耦 + 内部缝（满足 RT_IN_INTERFACE_001/002、RT_DESIGN_006）：** 传输（纯字节管道）/ 编解码（线缆格式）/ 交互节点三层，以可测替换的**缝**衔接。`ITransport` 是内部缝（非用户 API），`ICodec` 是公共扩展点，编程主入口是交互层 node。原则：各层职责单一、可独立替换与单测。
- **DD-2 AsyncTask 强制运行时（满足 RT_CORO_RUNTIME_001..005、RT_DESIGN_002）：** 不设独立 `IExecutor`/`ThreadExecutor` 业务调度体系；M:N 协作式；同一节点交互状态串行访问，以一把 `std::mutex` 守临界区实现，运行时 await 只出现在 fiber 挂起点，唤醒/回调在锁外调用。**依赖性**：节点所属执行域首次启动后稳定、不迁移（RT_CORO_RUNTIME_003）。
- **DD-3 无共享交互引擎（满足 RT_DESIGN_003、RT_NODE_003）：** 不设独立 `InteractionEngine`/`InteractionPolicy` 层；协议特有语义（键派生、终结判别、寻址、帧盖章）内联各 node。公共只抽出**协议无关机制**：`PendingTable`、`BoundedQueue`、`HandlerLoop`、`SharedCompletion`，以及承载生命周期的基类 `NodeBase`。原则：语义归属清晰、不造上帝对象。
  **变更（ADR-0008 D6/D8）**：清单已改为 **`Dispatcher`（按键分配）、`HandlerLoop`、`NodeBase`** 三件。**再变更（ADR-0009 D1，2026-08-20；#163 收尾，2026-08-25）**：`ProtocolNode` 停止使用 `HandlerLoop`，清单收为 **`Dispatcher` + `NodeBase`** 两件；**`HandlerLoop` 已整体删除**——其唯一名义使用者 `DdsNode` 实为重设计之前的代码、不参与编译。`PendingTable` 由 `Dispatcher` 取代；`BoundedQueue` 与 `SharedCompletion` 删除——前者是 `FiberChannel` 的手工重造，后者是 `Awaitable::close()` 广播的手工重造。
  **变更（ADR-0006 D1）**：原 `NodeRuntime<Event>` **已删除**（#139 迁空生命周期职责，#140 下放读循环与 handler 驱动后删除文件）——它把生命周期、handler 队列、读循环骨架与观测计数四件事装进一个 528 行模板，恰是本决策所反对的上帝对象。生命周期上移基类 `NodeBase`（模板方法：基类管幂等与收敛，子类实现 `DoStart`/`DoClose`），handler 队列下沉为可选小件 `HandlerLoop`，读循环骨架与观测计数回归各 node。
- **DD-4 协议无关基座可复用（满足 RT_DESIGN_008、D10）：** `PendingTable<Key,T>`/`BoundedQueue<T>`/`HandlerLoop<Message>`（**三者均已删除**：前二者随 ADR-0008 D8，后者随 ADR-0009 于 #163）曾对协议类型不透明，经模板参数/注入回调解耦。**变更（ADR-0006 D1）**：`NodeRuntime<Event>` 的模板参数从未被变化过（两处实例化均为 `NodeRuntime<Message>`），泛型代价（头文件实现、`byte_size_of` 回调注入）全部白付，故随其删除一并取消；`NodeBase` 为非模板。DdsNode 复用它们：仅把 Key 从 `uint32` 换为 `std::string`，基座一行不改（实证）。
- **DD-5 结果承载错误、不抛异常（满足 RT_ERROR_001/002/003、RT_DESIGN_005）：** 预期失败用 `Coro::Result<T>` / `Coro::Result<void>`（`[[nodiscard]]`）+ 机器可判别 `TransportErrc` 类别，不靠解析字符串前缀。唯一授权的 `catch` 在 handler 消费者边界（隔离第三方逃逸异常）。
- **DD-6 发送排序（满足 RT_TRANSPORT_007/004；ADR-0004 D5）：** 同 fiber 程序序、跨 fiber 串行为一致全序，单帧字节不与另一帧交错。**发送完成语义与协程背压已撤销**（原"帧进内核才报成功"，ADR-0002 D2 → 被 ADR-0004 D5 撤销）：发送成功此后仅表示该帧已交付下层发送通路。并发写串行化**保留**——它是 RT_TRANSPORT_004 的实现手段，与完成语义是两件事。**代价**：发送侧不再有框架级内存上界（见 §7.2 已知缺口）。
- **DD-7 两个独立生命周期轴 + 链路可用性上移（满足 RT_LIFECYCLE_001/002、RT_TRANSPORT_009；ADR-0004 D2）：** 节点生命周期 `Created→Running→Closing→Closed`（全介质通用）与 TCP 物理连接状态（仅 TCP 客户端，Running 内子状态）正交。**连接管理**（状态机/重连间隔/重连策略/代际推进）不下沉纯字节管道；但**当前链路可用性**作为与 `LastSendTime`/`LastError` 同类的 I/O 事实**上移至 `ITransport` 基类**，所有介质同形作答——交互层因此不再按介质探测可选能力接口（`IConnectionObservable` 取消）。此为 ADR-0002 D3′ 的边界重划。
- **DD-11 读取终止语义单一化 + 重连完全透明（满足 RT_TRANSPORT_008；ADR-0004 D1）：** `Read` 失败中仅 `kClosed` = 传输终结（我方关闭，或不可重连传输的底层致命错误），其余为可继续的瞬时错误。不可重连介质（UDP/串口/已接受的 TCP 连接）致命错误统一返 `kClosed`；**可重连传输在内部透明处理链路中断**，不向交互层暴露断链事件。`kConnection` 此后仅存于写路径（RT_TCP_RECONNECT_003）。**交互层因此对三介质使用同一段读循环，且无链路中断分支**。
- **DD-12 撤销连接代际隔离（满足 RT_TCP_RECONNECT_002 改写；ADR-0004 D3/D4）：** 断链时交互层不再批量终结在途请求、不再清空旧链路排队业务，"代际"概念自交互层消失。在途请求由各自总超时（**缺省值 30 秒，强制项**）、取消或关闭终结。RT_REQUEST_004"旧代际响应不得完成新请求"由物理事实保证（旧 socket 已关，字节不跨链路投递；在途关联标识未释放，新请求取不到同键）。**不引入编解码器重置**（ADR-0004 D4 撤销）：断链残尾与新链路首字节可能拼成错帧，由编解码器既有校验与重同步处置（报坏帧后恢复）。
- **DD-13 收敛并入读循环（满足 RT_LIFECYCLE_004/006；ADR-0005 D1）：**
  > **已推翻（ADR-0008 D2/D3）**：`Close()` 改为**只发信号**，收敛移出内部 fiber——由 `WaitClosed()` 的调用方经 `DoJoin()` 自上而下 join 全部内部 fiber。"读循环兼任收敛者"、`close_signalled_` 握手、`SharedCompletion` 多等待者通知一并取消。下段描述的是重设计之前的形态。
 关闭收敛**不另起 fiber**。**现状（ADR-0008 D2 起）**：`Close()` **只发信号**（关本节点读订阅 + `Dispatcher.CloseAll`）即返回、不含等待点；汇合由 `WaitClosed()` 在**调用方 fiber** 上 join 内部工作单元完成，故不构成自等待。**内部工作单元自 ADR-0009 D1 起只剩读-分发循环一条**——订阅者的消费 fiber 属宿主，节点无从 join（见 SRS RT_LIFECYCLE_006 的覆盖面收窄）。下述"读循环恒是第一个退出的"——无论我方 `Close` 使 `Read` 返 `kClosed`，还是不可重连介质的底层致命错误使其返 `kClosed`（DD-11 之后二者同码），故它天然是收敛的正确位置。独立 finalizer fiber 与其汇合点 `loop_done_` 随之取消；**结构约束**：读循环收敛走内部路径，不得调用公开的 `Close()`（那会等待自身退出）。多等待者通知仍用 `SharedCompletion`（**ADR-0006 D3 后为"存结果 + `close()` 广播"的轻量实现**：支持每等待者独立 deadline，不再支持每等待者取消——ADR-0005 D3 关于"必须为每等待者分配独立 `Awaitable`"的理由已被修正，那只在需要 per-waiter 取消时成立）。
- **DD-8 恰好一次终结的挂起-应答仲裁（满足 RT_REQUEST_001..005）：** 请求↔响应关联复用 `PendingTable`，每个在途 entry 一个等待者、以裸 `Coro::Awaitable<T>` 为信箱，**表锁 `find+erase` 作唯一仲裁点**（Resolve/超时/取消/FailAll 谁先摘除谁胜）。原则：恰好一次不靠状态枚举而靠单点抢占，竞态面小。
  **变更（ADR-0008 D6）**：改由 `Dispatcher` 承担。仲裁点从"表锁 find+erase"变为"**订阅凭据的生存期**"：凭据析构即注销，故一个订阅在其生存期内可接收多条消息（分段交互所需），而请求的"恰好一次终结"由 `Request` 内 `Wait` 一次后凭据即析构来保证。**投递语义同时从"独占"改为"全部键匹配者各得一份"**——多消费者与旁路监听由此成立，唯一性改由协议在键的设计上保证。
- **DD-9 底层回调不碰节点状态（满足 RT_INBOUND_002、RT_NODE_004、RT_IN_INTERFACE_003）：** Qt I/O 回调在 socket QThread、DDS 样本在 provider listener 线程，均须安全转交节点执行域；DDS 经跨线程有界交接边界（`BoundedQueue<Sample>`）转交，跨线程唤醒靠 boost.fiber channel 跨线程安全。
- **DD-14 交互模式与接口一一对应，节点不持交互状态（满足 RT_NODE_002_a..g；ADR-0010）：** 四种交互行为各由一个独立方法承载，**模式不作参数、不入节点状态**。每个方法运行于**调用方自己的 fiber**，其状态机的全部状态——当前阶段、已发送次数、原始命令帧——都是该方法的局部变量，随调用方栈存在。
  由此：节点**无"在途交互表"**、无新增成员，`Dispatcher` **不需要认识模式**（它只认键）。一个看似要新机制的需求（多段交互 + 重试）因此**新增机制为零**——完全由 `Dispatcher` 的既有性质（投递**不终结**订阅、键可部分匹配）加调用方控制流实现。
  **两条相反的重发规则并存**：`RequestForResult`（外部系统协议）**等结果时不重发**（对端正在执行，重发有重复执行风险）；`RequestForResultDirect`（另一种协议）**等结果时必须重发**（它无受理阶段，不重发则丢包即彻底失败）。前者是**该协议的约束，非框架普遍规则**（ADR-0010 D2/D13）。
  详见 §5.5「交互模式」与 §4.2.12（图 4-14）。

- **DD-10 可插拔观测 + 完整性归因（满足 RT_TRACE_001/002、RT_DATA_BUFFER、D13）：** 每个丢弃点经唯一 `RecordDrop` 归因到**五项** `DropReason` 之一（原七项：`kGenerationIsolationDrop` 随 ADR-0004 D3 移除，`kNoHandlerConfigured` 随 ADR-0009 于 #163 移除） + 命名计数；可选 `ITraceSink` 结构化 Trace（push）与命名计数（pull）双面；未配 sink 时零控制流影响。"无静默丢失"结构性可断言（Σ命名 = 总丢弃）。

## 4. CSCI 体系结构设计

### 4.1 CSCI 部件

本 CSCI 为纯软件库（随宿主工程编译），不需额外硬件资源。由四个软件部件（CSC）自底向上构成，上层依赖下层；AsyncTask 为强制运行时，Qt 与 Fast DDS 按宿主配置可选。部件依赖见图 4-1。

**图 4-1 CSC 部件依赖图（`sdd-csc-layers`）**

![CSC 部件依赖图](diagrams/sdd-csc-layers.svg)

**图例说明**：实线箭头读作"A 使用/组合 B"，虚线为可选依赖。`CSC_NODE` 组合 `CSC_CODEC`+`CSC_IO`、依赖 `CSC_CORE`；`CSC_CODEC`/`CSC_IO` 各依赖 `CSC_CORE`；全体运行于 `AsyncTask`；`CSC_IO` 的 Qt socket/串口依赖在宿主未启用 Qt 时不编译。整体单向依赖、上层不被下层反向引用。总体类关系见附图 `arch-class.svg`（`ITransport`/`ICodec`/node/core 的组合 ▷ 与实现 △）。

| 部件 | 目录 | 主要内容 | 响应需求 |
|---|---|---|---|
| **CSC_CORE** | `core/` | `TransportErrc`、`Message`、`Endpoint`、`Cancellation`、**`Dispatcher`**、`ITraceSink`、`Observability`、`DropReason`、`TraceCategories`（**ADR-0008**：`Result`/`Status` 改用 `Coro::Result`，`SharedCompletion` 已删除） | RT_ERROR、RT_DATA_MESSAGE、RT_TRACE、RT_DESIGN_005 |
| **CSC_IO** | `io/`（`tcp/`·`udp/`·`serial/`·`dds/`） | `ITransport`（含链路可用性）、`Tcp/Udp/Serial/DdsTransport`、`TcpClientTransport`、`TcpServer`、`IDdsProvider` | RT_TRANSPORT、RT_TCP_RECONNECT/RECONFIG、RT_IF_*、RT_IN_INTERFACE_002/003 |
| **CSC_CODEC** | `codec/` | `ICodec`、`SystemCodec`、`DatagramCodec`、`DdsCodec`、`LengthFieldCodec` | RT_CODEC、RT_IF_SYSFRAME |
| **CSC_NODE** | `node/` | `NodeBase`、`ProtocolNode`、`Dispatcher`；`DdsNode`、`DdsHandlerContext`（未编译的历史代码，待复活重写）（**已删除**：`PendingTable`/`BoundedQueue` 随 ADR-0008 D8，`HandlerLoop` 随 ADR-0009 #163） | RT_NODE、RT_REQUEST、RT_INBOUND、RT_LIFECYCLE、RT_DESIGN_003/008 |

#### 4.1.1 核心原语部件（CSC_CORE）

- **用途**：与协议/介质无关的值类型与基础并发原语，为上层提供结果承载、协作取消、多等待者完成、统一寻址与观测原语。响应 RT_ERROR_*（结果/错误）、RT_DATA_MESSAGE、RT_TRACE_*、RT_DESIGN_005。
- **主要内容**：`Coro::Result<T,error_code>`（无返回值者用 `Coro::Result<void>`；~~`Status` 别名已删除~~）；`TransportErrc` **十四类**机器可判别错误；`Message`（payload + 两套元数据）；`Endpoint`（kDefault/kNet/kTopic 统一寻址）；`Cancellation`（Source/Token/Registration 协作取消）；`SharedCompletion<T>`（多等待者一次性完成，原子首胜）；`ITraceSink`+`RecordDrop`/`RecordEvent`+`DropReason`+`TraceCategories`（观测）。
- **关系与结构**：被 CSC_IO / CSC_CODEC / CSC_NODE 依赖；自身仅依赖 AsyncTask（`Coro::Result`/`Awaitable`）。结构简单，无独立类图（同 AsyncTask CSC_DETAIL 惯例），详见 §5.1。

#### 4.1.2 传输层部件（CSC_IO）

- **用途**：介质无关的纯字节管道，向节点提供协程 await 式拉模型（`Read` 一次一片）与统一 I/O 观测面（含**链路可用性**）；连接管理（TCP 客户端自动重连）与服务端 accept。响应 RT_TRANSPORT_*、RT_TCP_RECONNECT/RECONFIG、RT_IF_*。
- **主要内容**：接口 `ITransport`（内部缝，**唯一**——`IConnectionObservable` 已随 ADR-0004 D2 取消）；实现 `TcpTransport`（已连接）、`TcpClientTransport`（连接管理）、`UdpTransport`、`SerialTransport`、`DdsTransport`、`TcpServer`；provider `IDdsProvider`（Fake/FastDDS）。
- **类图**：见图 4-2。
- **关系与结构**：依赖 CSC_CORE；被 CSC_NODE 组合（node 持 `unique_ptr<ITransport>`）。

**图 4-2 CSC_IO 类图（`sdd-csc-io`）**

![CSC_IO 类图](diagrams/sdd-csc-io.svg)

**图例说明**：各介质传输实现**唯一**的 `ITransport`（七个方法，含链路可用性；`IConnectionObservable` 已取消，交互层无 `dynamic_cast` 探测）。**读写刻意不对称**：`AsyncRead()` 交出读队列的等待器句柄，超时/取消/扇出由调用方自理；`AsyncWrite(Datagram)` 入队即返，写出结果只落 `LastError()`。`UdpTransport` 已按 ADR-0008 落地（socket 管理泵 + 读写双队列，`silence_timeout` 兼作读超时与 bind 重试间隔）；`TcpClientTransport` / `SerialTransport` / `DdsTransport` **按同一形态后续跟进，当前排除于编译面**，其内部结构仍为重设计之前的形态。
> 注：本图 SVG 尚未随 ADR-0004 重渲染（渲染工具在当前环境不可用），`.mmd` 源以本节文字为准。

#### 4.1.3 编解码层部件（CSC_CODEC）

- **用途**：逻辑 `Message` ↔ 线缆字节的分帧、序列化、校验、重同步；应用可提供并装配的公共扩展点。响应 RT_CODEC_*、RT_IF_SYSFRAME。
- **主要内容**：接口 `ICodec`（`Encode` 一对一 / `Decode` 0..N）；实现 `SystemCodec`（外部协议流式，占位帧常量）、`DatagramCodec`（报文式保边界）、`DdsCodec`（DDS 元数据，无状态并发）、`LengthFieldCodec`（长度字段分帧基元）。
- **关系与结构**：依赖 CSC_CORE（`Message`/`Result`）；被 CSC_NODE 组合（node 持 `unique_ptr<ICodec>`）。结构简单，无独立类图，详见 §5.5。

#### 4.1.4 交互层部件（CSC_NODE）

- **用途**：在传输 + 编解码之上组合请求关联、入站分发、超时、连接状态与协议交互；薄壳组合、不共享引擎。响应 RT_NODE_*、RT_REQUEST_*、RT_INBOUND_*、RT_LIFECYCLE_*、RT_DESIGN_003/008。
- **主要内容**：交互节点 `ProtocolNode`（外部协议请求-响应）、`DdsNode`（DDS pub-sub + 多路请求-应答）；生命周期基类 `NodeBase`（幂等 Start + 只发信号的 Close + join 式 WaitClosed）；协议无关机制 `Dispatcher<T,Fields...>`（按键分配、部分匹配、多订阅者各得一份副本）；`HandlerContext`（handler 能力面）。**变更（ADR-0008）**：`PendingTable` / `BoundedQueue` 已删除，请求关联移交 `Dispatcher`（位于 CSC_CORE，见 §5.2）。
- **类图**：见图 4-3。
- **关系与结构**：依赖 CSC_CORE，组合 CSC_IO + CSC_CODEC。

**图 4-3 CSC_NODE 类图（`sdd-csc-node`）**

![CSC_NODE 类图](diagrams/sdd-csc-node.svg)

**图例说明**：`ProtocolNode`/`DdsNode` **继承 `NodeBase`**（基类管幂等与汇合，子类实现 `DoStart`/`DoClose`/`DoJoin`），并组合 `Dispatcher`（~~`HandlerLoop` 已随 ADR-0009 于 #163 删除~~）（不共享交互引擎）。注：RT_IF_API「不要求应用继承节点类型」约束的是**应用**，`NodeBase` 是库内实现基类，宿主仍按组合方式使用节点。**`ProtocolNode` 对 `ITransport` 是虚线依赖而非组合**——按引用借用，宿主负责传输的启停（ADR-0008 D5）。订阅信箱直接用 `Coro::Awaitable`（原 `BoundedQueue` 与 `HandlerLoop` 均已删除）；node 在消费者 fiber 内构造 `HandlerContext` 传入 handler。

### 4.2 执行方案

说明软件单元间的动态关系、控制流/数据流、状态转换与动态生命周期。各图以 SVG 给出（`docs/diagrams/`，Mermaid 源随图保存）。

#### 4.2.1 数据流上下文图（MS_DFD_CONTEXT）

transport 作为单一 CSCI，外接四个实体：宿主应用、通信介质对端、AsyncTask 运行时、可选 DDS Provider。

**图 4-4（`dfd-context`）**

![数据流上下文图](diagrams/dfd-context.svg)

**图例说明**：① **宿主应用**——装配 codec、**订阅入站业务帧并在自有 fiber 上消费**（ADR-0009）、发起 Request/Send/Publish，取回 Result/Message/连接状态/观测；② **通信介质对端**——收发字节/样本、断连事件；③ **AsyncTask 运行时**——框架请求它创建/恢复协程、超时、跨线程唤醒；④ **DDS Provider**（可选）——按 topic 收发、listener 回调样本。要点：框架不产生业务数据，只在四方之间搬运字节与关联/分发。

#### 4.2.2 顶层数据流（MS_DFD_TOPLEVEL）

分解为 5 个加工（P1–P5）与 3 个数据存储（D1–D3）。

**图 4-5（`dfd-toplevel`）**

![顶层数据流图](diagrams/dfd-toplevel.svg)

**图例说明**：一条请求-响应/业务帧的完整走向——**P1 出站**盖章+Encode+AsyncWrite，先在 D1 登记订阅、取用 D3 的 session_id；**P2 入站读循环** await 读订阅 + Decode + Dispatch，D1 按键投给全部匹配的订阅者、各得一份；命中即唤醒 P1 的 `Ticket.Wait` 交回结果；**P4 业务处理**从 D2 取出交单消费者 handler，handler 可经 P1 回送；**P5 生命周期**驱动 D1 的 `CloseAll` 与 D2 的 Close。

**数据存储说明：**

| 存储 | 实现 | 读者 ← 写者 | 一致性保护 |
|---|---|---|---|
| D1 订阅索引 | `Dispatcher`：`map<Mask, map<Values, vector<Entry{Awaitable<T> mailbox}>>>` | P1 等待者 ← P2 Dispatch / P5 CloseAll | 无锁（单线程 fiber 协作，ADR-0008 D9）；凭据生存期即仲裁 |
| D2 业务队列 | `Coro::Awaitable<Message>` + `setCapacity` | P4 消费者 await ← P2 push | `FiberChannel` 自守；满则**静默丢最旧**（无归因，#152） |
| D3 交互状态 | session 空闲集 `deque<uint8>`（**连接代际已移出交互层**，ADR-0004 D3） | node 各方法 | 一把 `std::mutex`（node 私有） |

#### 4.2.3 请求-响应节点数据流（MS_NODE_DATAFLOW）

**图 4-6（`dataflow`）**

![请求-响应节点数据流图](diagrams/dataflow.svg)

**图例说明**：出站（调用方 fiber）与入站（读-分发循环 fiber）两条独立流，经 `Dispatcher` 的 Dispatch→Wait 唤醒闭环。**投递份数为 0** 时才分流：终结帧归因 `unmatched-or-late-response`，其余业务帧**静默丢弃、不归因**（无订阅者是常态，ADR-0009 D5）；坏帧由 codec 判定。命中的订阅者各得一份副本，业务帧的消费由**宿主自有 fiber** 承担（ADR-0009 D2，节点不再内置 handler 通道）。写出的一切结果（目的地非法 / 报文超长 / socket 写失败）**不回传**，只落 `LastError()`。

> **变更（ADR-0009 D1/D2，#163）**：原文"业务帧交 `HandlerLoop`、无 handler 归因丢弃"已作废——`HandlerLoop` 与 `DropReason::kNoHandlerConfigured` 均已删除。图源 `dataflow.mmd` 早已改画为宿主消费 fiber，是本图例文字漏改。

#### 4.2.4 请求-响应时序（MS_REQ_RESP）

**图 4-7（`seq-request-response`）**

![请求-响应时序图](diagrams/seq-request-response.svg)

**图例说明**：`RequestForResponse` 的 happy path（**首次尝试即命中**）。本图的用途是把**关联与唤醒闭环**画细——订阅登记、编码、写出、`Ticket` 生存期、读-分发 fiber 的投递路径；四种交互模式的**完整状态机**见 §4.2.12 图 4-14，两图互补而不重复。

三处关键：① **先登记订阅、再发出请求**——反之则回应可能先于订阅到达而被丢弃；② 写出是 **fire-and-forget**，不等实际发出也无从得知是否发出成功，故 `timeout` 是本次尝试**唯一**的兜底终结源；③ `timeout` 是**单次尝试**的时限、**不做扣减**，超时即重发字节完全相同的原帧（`session_id` 不变，ADR-0010 D3），至多 `max_attempts` 次，耗尽返 `kNotAccepted`。请求终结后订阅随 `Ticket` 析构自动注销；session_id 由 `uint8` 计数器自增给出，取用不会失败。

> **变更（ADR-0010 D10，2026-08-26）**：本图原以 `Request(Message, milliseconds)` 作画，该方法**已删除**（#171）；改以其等价替代 `RequestForResponse(req, {timeout, 1})` 重绘。原图例"`timeout` 是本调用唯一的兜底终结源"隐含**总超时**语义（旧 `Request` 的 `Wait(timeout - 已耗时)`），现已改为按次计时。

#### 4.2.5 关闭收敛时序（MS_CLOSE）

**图 4-8（`seq-close`）**

![关闭收敛时序图](diagrams/seq-close.svg)

**图例说明（ADR-0008 D2/D3/D5）**：`Close()` **只发信号**（关本节点的读订阅 + `Dispatcher.CloseAll`——后者关闭全部订阅信箱，**即订阅者的协作取消信号**，ADR-0009 D4）即返回，**不含任何等待点**，故读-分发循环与业务处理器均可直接调用它。收敛移出内部 fiber：由 `WaitClosed()` 的调用方经 `DoJoin()` **自上而下** join 读循环与 handler 消费者，返回即全部内部 fiber 已不再触碰节点。读循环退出时无条件调 `Close()`——我方 Close 所致时是幂等空操作、传输终结所致时即自终，两条路径合并为一条。**传输的生命周期由宿主自理**，节点从不启停它。

#### 4.2.6 链路断开处置时序（MS_LINK_DOWN，原 MS_GEN_ISOLATION）

**图 4-9（`seq-link-down`）**

![链路断开处置时序图](diagrams/seq-link-down.svg)

**图例说明（ADR-0004 D1/D3/D4 后的新流程；图已按新流程重绘，源文件随 ID 由 `seq-generation-isolation` 更名 `seq-link-down`，#112）**：链路断开对交互层**完全透明**——`TcpClientTransport` 的 connect-loop 转入重连，`Read` 因对外通道无数据而自然挂起，重连成功后新链路数据到达即被唤醒。node 读循环**无任何断链分支**，node 保持 Running。

**不再发生的动作**（原代际隔离）：不批量终结在途请求、不清空旧链路排队业务、无 reactor 协程、无状态下降沿甄别、**不向交互层发任何断链信号**。在途请求由各自总超时（缺省 30 秒）/取消/关闭终结（ADR-0004 D3）；旧链路排队业务不再被 Drain，故**无「连接代际隔离丢弃」归因**（丢弃归因七项减六项，见 DD-10）。

#### 4.2.7 节点生命周期状态（MS_NODE_LIFECYCLE）

**图 4-10（`state-node-lifecycle`）**

![节点生命周期状态图](diagrams/state-node-lifecycle.svg)

**图例说明**：`Created→Running→Closing→Closed`。`Running` 由基类在 `DoStart()` 返回后置位（不再由子类中途回调）；`Closing→Closed` 由 `WaitClosed()` 的 `DoJoin()` 驱动。**并发 Start 不共享结果**——另一次正在初始化时返 `kInvalidState`（旧形态的一次性 latch 会让重试拿到陈旧失败，#150）。**不再需要重入守卫或使用契约**：`Close()` 拆成只发信号之后，内部工作单元直调它是安全的（ADR-0008 D2）。

#### 4.2.8 TCP 连接状态机（MS_CONNECTION）

**图 4-11（`state-connection`）**

![TCP 客户端连接状态机](diagrams/state-connection.svg)

**图例说明**：`Disconnected/Connecting/Connected/Reconnecting` + 连接代际递增；**固定重连间隔 1s**（指数退避已撤销，见 ADR-0005）；端点热更新掐断当前尝试立即重试。**本状态机完全内于 `TcpClientTransport`**——代际用于其自身内部记账与诊断（`Generation()`），交互层不感知（ADR-0004 D2/D3/D7）；对外只经**链路可用性**与 `Read` 的二义终止呈现。

#### 4.2.9 订阅凭据生存期状态（MS_TICKET）

**图 4-12（`state-dispatch-ticket`）**

![订阅凭据生存期状态图](diagrams/state-dispatch-ticket.svg)

**图例说明**：一票一键，信箱为 `Coro::Awaitable<T>`（队列语义，故同一凭据可多次 `Wait`——分段交互所需）。**注销靠凭据的生存期而非状态枚举**：`~Ticket`/`Reset()` 即从索引摘除，空桶与空 mask 一并清理，使 `Dispatch` 不再探测已无订阅的 mask。`CloseAll` 后再 `Subscribe` 返回的凭据其信箱已关闭，`Wait` 立即得到该终止原因。凭据内部以**弱引用**持有索引，故允许在 `Dispatcher` 析构之后再析构。

#### 4.2.10 传输层 socket 管理泵与读写双队列（MS_TRANSPORT_PUMP）

**图 4-13（`seq-transport-pump`）**

![传输层 socket 管理泵与读写双队列](diagrams/seq-transport-pump.svg)

**图例说明（ADR-0007 + ADR-0008，UDP 先行）**：`Start()` 起**管理泵**与**写泵**两条 fiber 后即返回——首次 bind/connect 未成**不算启动失败**。管理泵为双层循环：**外层**按配置绑定/连接，失败则 `await_for(close_signal, timeout)` 退避后重试（**无限重试**，唯一退出条件是我方 `Close`，故 UDP **不自终**）；**内层**反复 `await_for(stream, timeout)` 并把数据投入 `read_queue`。**读数据与超时判断同在管理泵这一条 fiber 内**，且两处 `timeout` 是同一个量（`UdpConfig::silence_timeout`，默认 5s）——有链路时它是"多久没数据算坏"，没链路时它是"多久试一次"。静默超时 / 流终止 / 我方 `Close` 三者在内层**不作区分**，一律 break 回外层重建，区别只落在 `LastError()` 的归因上；每轮末尾无条件解绑，下轮从确定状态重建。
数据面与 socket 生命周期由此**彻底解耦**:重建不波及正在等待的读者。`Read()` **只交出 `read_queue` 句柄**，deadline/取消与是否 `shared()` 扇出全由调用方决定（传输层不设单读守卫）；`Write()` **只入队即返**（fire-and-forget，失败只进 `LastError()`/Trace），链路不可用时数据留在 `write_queue` 等待恢复，恢复后按序全部发出。终止表现为 **`read_queue` 被 `close()` 并携带终止原因**。
**待定（TBD-009）**：两条队列的容量上限与超限处置；硬约束是"丢弃必归因、字节流不得丢弃"。

#### 4.2.11 对象/线程/协程的动态创建与删除（MS_DYNAMIC_LIFECYCLE）

- **fiber（一个 node 内）**：`Start` 时 spawn 读-分发循环 fiber，**共一条**（ADR-0009 D1：内置 handler 消费者 fiber 已废止；订阅者的消费 fiber 属**宿主**，不计入节点内部工作单元）；`Close` **不再 spawn 任何 fiber**，且不含等待点——汇合由 `WaitClosed()` 在调用方 fiber 上 `FiberTask::get()` join 读循环完成（ADR-0008 D2）。**reactor fiber 已随 ADR-0004 D2/D3 取消，finalizer fiber 已随 ADR-0005 D1 取消。** 均由 AsyncTask `makeTask` 创建、返回即终止。
- **传输连接代际（内于传输层）**：`TcpClientTransport` 的 connect-loop fiber 每次成功物理连接创建一个内层 `TcpTransport`（`Generation()`+1），断链销毁旧内层、隔固定间隔（1s）后建新代际；断链**不向交互层发任何信号**（完全透明，DD-11）。交互层不参与。
- **DDS 交接**：`DdsTransport` `Start` 对每 topic `Subscribe`，listener 回调在外线程构造 `Sample` 非阻塞 `Push`；`RequestClose` 先 `Unsubscribe` 停投递 → 交接边界 `Close` 唤醒在途 `Read` → provider `Shutdown`；迟到回调只捕获交接边界共享句柄、不触碰已销毁对象。
- **TcpServer 子 node**：每接受一条连接经 NodeFactory 派生 `ProtocolNode` + supervisor fiber；对端断开 → supervisor 驱动该 node `Closing→Closed` 并注销（连接生命=节点生命）。

#### 4.2.12 交互模式时序（MS_INTERACTION_MODES）

**图 4-14（`seq-interaction-modes`）**

![交互模式时序图](diagrams/seq-interaction-modes.svg)

**图例说明（ADR-0010 / RT_NODE_002_a..g）**：四个方法分属**两种协议**——`Send`/`RequestForResponse`/`RequestForResult` 属外部系统协议（其中 `withfeedback` 与 `needfeedback` 为**同一模型**，合用 `RequestForResult`），`RequestForResultDirect` 属另一种协议。**状态机全部跑在调用方 fiber 上**——阶段、已发送次数与原始命令帧都是该方法的局部变量，故节点无"在途交互表"、`Dispatcher` 不认识模式（**D1**）。图中标出四个易错点：② ③ ④ 的订阅**必须在发命令之前登记**（**D4**，`kResult` 可能先于 `kResponse` 到达）；阶段一完成后**立即注销受理凭据**（**D5**，否则重发引出的重复受理帧继续入信箱）；第二阶段超时**不重发**（**D2**，`kResult` 未达意味着对端正在执行）；`RequestForResult` **末尾的回应帧完全由收到的 `kResult` 派生**（**D8**：仅改帧类型，payload/sid/mid 原样，CRC 由编码重算），且**不走 `Send()`**（它会强制盖新 `session_id`）——注意 `RequestForResultDirect` **不回应任何帧**，二者相反。两阶段的失败以不同错误码区分：阶段一耗尽为 `kNotAccepted`、阶段二超时为 `kTimeout`（**D12**）。接收侧不建模（**D9**），`repeating` 本轮不定义（**D11**）。

### 4.3 接口设计

#### 4.3.1 接口标识和接口图

```
[宿主应用]
   │ 节点/配置/订阅/请求/可观测        (JK_NODE_API 编程接口, CSC_NODE)
   │ 装配 codec                        (JK_CODEC 编解码扩展点, CSC_CODEC)
   ▼
[transport]
   │ ITransport 内部缝                  (JK_TRANSPORT 传输内部接口, CSC_IO)
   │ IDdsProvider                      (JK_PROVIDER DDS 抽象, CSC_IO)
   │ ITraceSink                        (JK_TRACE 观测出口, CSC_CORE)
   ▼
[Qt / Fast DDS / AsyncTask]
```

外部（面向使用方）接口 JK_NODE_API、JK_CODEC、JK_TRACE；内部缝 JK_TRANSPORT、JK_PROVIDER（框架自用，不对使用方开放为主 API）。**JK_OBSERVABLE 已随 ADR-0004 D2 取消**——连接观察能力并入 JK_TRANSPORT 的链路可用性，不再是独立多态缝。

#### 4.3.2 编程接口（JK_NODE_API）
- 优先级：高（核心对外 API）。
- 接口类型：C++ 类/成员函数（`node/ProtocolNode.hpp`、`node/DdsNode.hpp`、`io/tcp/TcpServer.hpp`）。
- 数据元素：`Message`、`std::chrono::milliseconds`（时限）、`RetryPolicy`（ADR-0010）、各 `*Config`、`MessageDispatcher::Key` / `Ticket`、`Coro::Result<Message>` / `Coro::Result<void>`。
  **变更（2026-08-26 核对）**：原文所列 `OperationOptions`（随 ADR-0006 D3 取消令牌一并退化为时限）、`InboundHandler`（随 ADR-0009 废止 handler 通道而删除）、`Status`（别名已删）**均已不存在**。
- 通信方式：同步函数调用；`RequestFor*`/`Send`/`Publish`/`WaitClosed` 为协程内让出式（不阻塞线程）。
- 协议特征：`ProtocolNode(transport,codec,config)` → `Start/Close/WaitClosed`、`Send(Message)`、`RequestForResponse(Message,RetryPolicy)`、`RequestForResult(Message,RetryPolicy,result_mid,result_timeout)`、`RequestForResultDirect(Message,RetryPolicy,result_mid)`、`Subscribe(Key)`；`DdsNode` → `Request(Message,target,options)`、`Publish(Message,topic)`；`TcpServer(config,factory)` 每连接派生 node。
  **变更（ADR-0010，2026-08-26）**：`ProtocolNode::Request(Message,options)` **已删除**（D10，#171）——时限与重试改由 `RetryPolicy` **逐次传参**（D6），节点配置面上不再有任何时限缺省值（#173 删除 `ProtocolNodeConfig::default_request_timeout`）。所列 `DdsNode::Request` 是**另一个方法**（三参、DDS 侧），不受此变更影响。

#### 4.3.3 编解码扩展点（JK_CODEC）
- 优先级：高（公共扩展点）。
- 接口类型：C++ 抽象类 `ICodec`（`codec/ICodec.hpp`）。
- 数据元素：`Message` ↔ `vector<uint8_t>`。
- 通信方式：同步函数调用，收发边界处装配到 node。
- 协议特征：`Encode(Message)→Result<bytes>`（一对一）；`Decode(data,len)→Result<Message[]>`（半包空、粘包多条）；分帧错误 `kFrame`、语义错误 `kCodec`。

#### 4.3.4 观测出口（JK_TRACE）
- 优先级：中（可选，未配置零影响）。
- 接口类型：C++ 抽象类 `ITraceSink`（`core/ITraceSink.hpp`）。
- 数据元素：`TraceEvent`（零分配视图：level/category/message/key/endpoint/error/size 等）。
- 通信方式：push，`OnTrace` 在库内部**可能持锁调用**——实现须快速返回、不阻塞、不回调本库任何 API（重入契约）。
- 协议特征：`TraceCategories.hpp` 定义 **12 个** category 常量（connect / generation / send / recv / decode / match / timeout / cancel / handler / reconnect / lifecycle / drop）。
  **实况标注（2026-08-26 核对）**：当前编译面内**仅 4 个有发射点**——`send` / `recv` / `decode`（`ProtocolNode`）与 `drop`（`RecordDrop` 专用）。`connect` / `generation` / `reconnect` 只由未参与编译的 `TcpClientTransport` 发射；`match` / `timeout` / `cancel` / `lifecycle` / `handler` **无任何发射点**（前四者自 ADR-0006/0008 重设计起即无来源，`handler` 更随 ADR-0009 废止 handler 通道而永久失去来源）。本条描述目标覆盖面，不表示已实现；是否重新接线或删除待评审。
  **实况标注（2026-08-20）**：`match`、`timeout·cancel`、`handler` 三类当前**定义了但无任何发射点**（ADR-0006/0008 重设计遗留漂移，非 ADR-0009 造成）；`handler` 一类随本轮 handler 通道废止而更无来源。是否重新接线或删除待评审。

#### 4.3.5 传输内部接口（JK_TRANSPORT）
- 优先级：高（框架正确性核心，内部缝）。
- 接口类型：C++ 抽象类 `ITransport`（`io/ITransport.hpp`）。
- 数据元素：`Datagram{bytes, peer}`（读写共用；原 `SendUnit` 已随 ADR-0008 D8 合并进本类型，字段 `source` 更名 `peer`）。
- 通信方式：**队列式**（ADR-0007 D1）。传输内部持两条 `Coro::Awaitable` 队列——`read_queue`（传输作**生产者**，把 I/O 收到的数据投入）与 `write_queue`（传输作**消费者**，取出待写 I/O 的数据）。socket 的创建、重建与关闭由传输内部的管理泵负责，对两端**完全透明**。
- 协议特征（**ADR-0008 D1**：七个方法，分三组）：

  | 组 | 方法 | 语义 |
  |---|---|---|
  | 任务 | `Start()` / `Close()` / `WaitClosed()` | 起泵后即返回 / **只发信号，不等待收敛** / join 全部内部工作单元 |
  | 数据 | `AsyncRead()` / `AsyncWrite(Datagram)` | 交出读队列的等待器句柄 / 送入一份数据 |
  | 观测 | `LastError()` / `CurrentLinkState()` | 每种介质都答得出的最小公分母 I/O 事实 |

- **读写刻意不对称**：读是"数据什么时候来"，只能交出等待器，由调用方自行决定超时、取消与是否扇出；写是"把这份数据发到那里去"，调用方给完即返回。因此**写队列是纯内部的**，调用方不感知其存在，也不需要知道它装的是什么。
- **`AsyncRead()` 交出等待器句柄**：签名为 `std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead()`，**返回 `read_queue` 的句柄而非一份数据**，不接受任何超时参数——超时与取消由调用方自行在句柄上 `await_for` / 接令牌。
  - **是否共享由调用方决定**：需要多消费者扇出时调用方自行 `shared()`；不共享则多个消费者天然抢占——socket 的读取本就是抢占式的。传输层因此**不设单读守卫**（RT_TRANSPORT_004 的该约束已删除）。
  - 未 `Start()` 时交出一个**已以 `kInvalidState` 关闭**的句柄——句柄式接口没有返回错误码的位置，故把该错误作为队列的终止原因交出。
- **`AsyncWrite()` 彻底 fire-and-forget（ADR-0008 D4）**：只判两件事——生命周期是否允许写、是否真的入队；返回成功仅表示"已受理并入队"，**不表示已发出**。目的地能否解析、报文是否超长、socket 是否写成，一律**不回传**，只落 `LastError()`。链路不可用时数据**留在队列中等待恢复**，不拒绝、不丢弃；恢复后按序全部发出（**接受对端可能收到过期数据**）。
  - `Datagram::peer` 在写侧读作**目的地**；`Endpoint::Default()` 表示"发往本传输配置的默认对端"，由实现自行解析，故传输无关的调用方恒可传它。
- **`Close()` / `WaitClosed()` 分工（ADR-0008 D2/D3）**：`Close()` 只发信号即返回、幂等，**不含任何等待点**；`WaitClosed()` join 全部内部工作单元，**不设时限也不返回结果**——`Awaitable::close()` 只保证唤醒等待者，而"可安全释放"要求 fiber 已跑完，只有 `Coro::FiberTask::get()` 给得了，它没有超时，故二者二选一。
- **读取终止语义（DD-11，表达经 ADR-0007 D4 改写）**：传输终结表现为 **`read_queue` 被 `close()` 并携带终止原因**，调用方在等待器上得到该终止错误后应停止读取；其余读取失败为可继续的瞬时错误。**"仅我方 `Close` 才终止"的语义不变**——具备重连能力的传输在内部透明重建，不向调用方暴露链路中断。
- **链路可用性（DD-7）**：`CurrentLinkState()` 返回 `LinkState`，为所有介质同形的当前 I/O 事实；连接管理策略不经本接口暴露。
- **删除的观测面（ADR-0008 D1）**：`LastSendTime()` / `LastReceiveTime()` 已删除——二者无人消费，且与 `LastError()`/`CurrentLinkState()` 不同，它们不是"此刻的 I/O 事实"而是历史记录。
- **待定（TBD-009）**：两条队列的容量上限与超限处置未定。硬约束：任何丢弃**必须归因**；**字节流介质不得丢弃**（丢流中段即帧错乱，正解为有界且满时阻塞生产者）。当前 AsyncTask 默认「有界 1024 + 静默丢最旧」为活跃隐患（#152）。

#### 4.3.6 DDS provider 抽象（JK_PROVIDER）
- 优先级：中。
- 接口类型：C++ 抽象类 `IDdsProvider`（`io/dds/IDdsProvider.hpp`）。
- 数据元素：topic 名、不透明字节、订阅回调。
- 通信方式：`Subscribe` 回调在 provider listener 线程触发（跨线程）。
- 协议特征：`Init/Shutdown/Subscribe/Unsubscribe/Publish`；实现 `FakeDdsProvider`（进程内总线）、`FastDdsProvider`（可选）。

## 5. CSCI 详细设计

各软件单元（CSU）对应 §4.1 部件，细度以"可据此直接编码"为准；结构性类图见 §4.1，本章聚焦逻辑与算法。每个 CSU 给出：单元设计决策 / 设计约束 / 软件逻辑 / 执行时序·数据流。

### 5.1 核心原语详细设计（CSU_CORE）

**单元设计决策**：直接复用 `Coro::Result<T,error_code>`（避免两套 expected 类型；~~原 `Result`/`Status` 两个别名已删除~~）；错误用 `TransportErrc` 机器可判别类别，不解析字符串（DD-5）；`SharedCompletion` 提供多等待者一次性完成（原子首胜 Complete），供多方共享收敛通知。

**设计约束**：不抛异常表达预期失败；并发数据受锁/原子保护；观测原语未配 sink 时仅一次判空（RT_TRACE_002）。

**软件逻辑**：
- **错误承载 / TransportErrc**：统一用 `Coro::Result<T,error_code>`，无返回值者 `Coro::Result<void>`（~~`Result`/`Status` 别名已删除~~）；`TransportErrc` **十四类**（kInvalidArgument/kInvalidState/kConfiguration/kConnection/kClosed/kTimeout/kCancelled/kIo/kFrame/kCodec/kResourceExhausted/kUnsupported/kInternal/**kNotAccepted**）经 `make_error_code` 归入 `transport_error_category()`。
- **Message**：`payload` + 两套元数据——通用交互（kind/correlation_id/reply_to，DDS 路径）与外部协议（frm_type/protocol_id/session_id/message_id，SystemCodec 路径）；`source`/`topic` 由 node 收到时按来源填。
- **Endpoint**：`kDefault`（config 默认目的地）/`kNet(ip,port)`/`kTopic(name)`，经 `SendUnit.destination`/`Datagram.source` 统一寻址。
- **Cancellation**：`CancellationSource.token()` 派发 `CancellationToken`；`Register(cb)` 返回 `CancellationRegistration`（RAII，`Reset` 解注册）；`Cancel` 触发全部回调。
- **~~SharedCompletion<T>~~**：**已删除（ADR-0008 D8）**——它是 `Awaitable::close()` 广播的手工重造。多等待者通知改用 `Awaitable::close()`（唤醒），"等到 fiber 真正跑完"改用 `Coro::FiberTask::get()`（汇合）；二者语义不同，混用正是旧形态把 `WaitClosed` 做成"带 deadline 的 close 等待"却又要求可安全析构的根源（见 §5.4）。
- **Dispatcher<T, Fields...>**：按键分配的消息路由器，详见 §5.2。
  **正确性依据（关键）**：`Coro::Awaitable` 底层 `FiberChannel` 的 `closed_` 是**持久 latch**（`std::atomic_bool`，`close()` 幂等置一次、不复位，随后 `notify_all()`；`pop`/`pop_wait_for` 的谓词均含 `closed_.load()`）。故"查已存结果落空 → 尚未 `await`"这段窗口内发生的 `Complete` **不会丢唤醒**——随后的 `await` 在已关闭通道上立即返回。若 `close()` 只是无 latch 的 `notify_all`，此处即为丢唤醒窗口。`await_for` 超时只 `return timeout`、不触碰 `closed_`，故超时不殃及其他等待者。
  **变更（ADR-0006 D3）**：原实现为每等待者分配独立 `Awaitable` 并维护 `map<id, weak_ptr>`（124 行），其唯一必要性是**每等待者独立取消**——deadline 不需要它（`Awaitable::await_for` 超时返回 `timed_out` 而不关闭 channel），而 `Awaitable::close()` 本就是广播。全仓生产代码无一处向 `WaitClosed` 传取消令牌，故取消能力连同 waiter map 一并移除（约 30 行取代 124 行）。
- **观测**：`RecordDrop(reason, counter, sink)`（计数 pull + 可选 Trace push，持锁调用）；`RecordEvent(category, sink, ...)`（仅 push）；`DropReason` **五项**（原七项：`kGenerationIsolationDrop` 随 ADR-0004 D3 撤销代际隔离而移除；`kNoHandlerConfigured` 随 ADR-0009 废止 handler 通道而移除，#163）；`TraceCategories` **12 个**常量（编译面内仅 4 个有发射点，见 §4.3.4）单一权威。

**执行时序/数据流**：见 §4.2.2（D1/D2/D3 一致性保护）；SharedCompletion 参与 §4.2.5 关闭汇合。

### 5.2 按键分配详细设计（CSU_DISPATCHER）

> 本节取代原 **CSU_PENDINGTABLE**（`node/PendingTable.hpp`，已删除）。ADR-0008 D6。

**单元设计决策**：请求↔响应关联由协议无关的 `Dispatcher<T, Fields...>`（`core/Dispatcher.hpp`）承担。调用方只提供**键提取函数**（给出一条消息各匹配字段的具体值），订阅时不参与匹配的字段填 `kAny`——**部分匹配由本单元实现**，调用方不写通配逻辑、不用哨兵值、不枚举字段组合。

**设计约束**：
- `kAny` 以 `std::optional` 的持值状态表达，**不占用字段值域**，故 `session_id` 这类 0..255 全用满的字段同样可以通配；`kAny` 与"该字段须等于 0"是两种不同的约束。
- 一条消息投给**全部**键匹配的订阅者，各得一份副本（`T` 须可拷贝）；单个订阅只关联一个键，故同一订阅者不会因一条消息收到多份。**不提供"独占投递"模式**——唯一性应由协议在键的设计上保证。
- 单条消息成本 **O(在用 mask 种数 + 收件人数)**，与订阅者总数无关。
- 单线程 fiber 协作，**不加锁**（ADR-0008 D9）：`Subscribe` / `Dispatch` / `~Ticket` 内均无挂起点；`Dispatch` 中的 `resolve` 仅入队并标记等待者就绪、不引发 fiber 切换，故索引在遍历期间保持稳定。键提取函数内不得回调本件。

**软件逻辑**：见 `core/Dispatcher.hpp`。
- 索引为两层：`unordered_map<Mask, unordered_map<Values, vector<Entry>>>`。`Mask` 是"哪些字段被约束"的位图，`Values` 是按该 mask 投影后的键值（未约束的字段置同一占位值）。**外层只保留存在订阅的 mask**，故探测次数随订阅情况自动收敛，既不枚举 2ⁿ 种字段组合，也不逐个求值谓词。
- `Subscribe(Key)`：推导 mask 与投影键值 → 建信箱 → 入索引 → 返回 `Ticket`。已 `CloseAll` 时返回一张信箱已关闭的凭据。
- `Dispatch(const T&)`：跑键提取函数得完整键 → 遍历在用 mask、逐个投影查表 → 命中桶内每个订阅者各 `resolve` 一份 → 返回投递份数。**返回 0 表示无匹配订阅，此时入参未被修改**，调用方可自行处置（转交处理器、归因丢弃）。
- `CloseAll(error)`：关闭全部信箱并置终止标记，令在途 `Wait` 恰好终结一次。
- `Ticket`：持一个信箱 + 析构注销，仅可移动；内部以**弱引用**持有索引，故允许在 `Dispatcher` 析构之后再析构。`Wait(timeout)` 让出式等待，信箱为队列语义——**同一凭据可多次等待**，一次交互需分段等待多条报文时，各段各自登记、各自设定时限。

**执行时序/状态**：见 §4.2.4（请求-响应时序）、§4.2.9（订阅凭据生存期状态）。

### 5.3 入站业务队列（原 CSU_BOUNDEDQUEUE，已删除）

> **已删除（ADR-0008 D8）**：`node/BoundedQueue.hpp` 是 AsyncTask `FiberChannel` 的手工重造（双端队列 + 等待者队列 + 互斥量 + 容量 + 丢弃），故取消，业务队列直接用 `Coro::Awaitable<Event>` + `setCapacity`。

**由此产生的语义变化（一并接受，见 #152）**：
- 满时的行为从 **tail-drop**（拒绝正到达的元素并归因计数）变为 **静默丢弃队首最旧的值**——对业务帧而言是"丢老留新"，与原语义相反；
- **字节数上界消失**，只剩事件数；
- 丢弃**既无计数也无归因**——AsyncTask 不提供丢弃计数，容量是唯一的调节手段。§3.6 的 loss=0 等式因此不再可直接验证。

`DdsTransport` 的跨线程有界交接（原 `BoundedQueue<Sample>`）按同一形态后续跟进。

### 5.4 节点基类详细设计（CSU_NODEBASE）

> 本节已按 **ADR-0008 D2/D3** 重写。原形态（模板方法 + 收敛并入读循环 + 五个反向回调）见 ADR-0005 D1/D2 与 ADR-0006 D1/D6。

**单元设计决策**：本单元只装**每个节点都有的**生命周期机制——状态机 `Created→Running→Closing→Closed`、幂等 `Start`、关闭仲裁与汇合。基类**非模板**、不持 `ITransport*`、对协议类型无感知；**不 include 任何协议或消息类型**。

**关键决策：`Close()` 只发信号，`WaitClosed()` 单独汇合。** 旧形态的 `Close()` 结尾无条件等待收敛，内部工作单元调用它即等于等自己退出、静默挂死，为此先后尝试过运行时重入守卫（ADR-0005 D6）与使用契约（ADR-0006 D8）。拆开之后：

- `Close()` **不含任何等待点**，因此**任何 fiber 都可调用**，包括节点自己的读-分发循环与入站业务处理器；
- 收敛不再由内部 fiber 自行完成，而由 `WaitClosed()` 的调用方经 `DoJoin()` **自上而下** join。

由此删除五个"子类在钩子中途回调基类"的反向动作——`MarkRunning()`、`SignalClose()`、`ConvergeAfterReadLoop()`、`JoinHandler()`、`DrainUnstartedBusiness()`；基类不再反向驱动子类，也不再需要 `close_signalled_` 握手。

**公开面（4）**：`Start()` / `Close()` / `WaitClosed()` / `IsRunning()`。返回 `Coro::Result<void>` 而非 `bool`——`bool` 会把"已 `Running`（成功）"与"已 `Closing`/`Closed`（RT_LIFECYCLE_003 要求 `InvalidState`）"压成同值，且 RT_LIFECYCLE_007 要求校验失败可据错误改配置重试（ADR-0006 D2 保留）。观测接口 `CloseDropCount()` / `LastCloseLatency()` **已删除**（ADR-0008 D10）。

**子类钩子（3）**：`DoStart()`（配置校验 + 传输就绪 + spawn 各 fiber）、`DoClose()`（发出全部汇合信号，**只发信号、不得等待任何 fiber**）、`DoJoin()`（join 本节点 spawn 的全部 fiber）。原 `ValidateConfig()` 并入 `DoStart()` 开头——它此前独立存在的唯一理由是绕开 `start_done_` 那个一次性 latch，latch 删除后两者行为一致。**配置校验是钩子的可选职责而非必备动作**：子类若无可校验的配置项，`DoStart()` 直接做就绪与 spawn 即可（`ProtocolNode` 自 #173 起即属此列，见 §5.6）。

**成员（4）**：`mutex_` / `lifecycle_` / `starting_` / `joined_`。原 `SharedCompletion` 三例（`start_done_` / `close_signalled_` / `closed_`）、`close_drop_count_`、`close_requested_at_`、`last_close_latency_` 全部删除。

**设计约束**：**持 `mutex_` 期间不调任何钩子、不出现挂起点**——三个钩子一律锁外调用，故基类不需要向子类下达"实现不得取锁、不得挂起"这类反向约束（旧形态的 `DrainUnstartedBusiness()` 正是持锁调用的虚函数）。

**软件逻辑**：见 `node/NodeBase.hpp` / `node/NodeBase.cpp`（92 行）。
- **`Start()`**：临界区内判生命周期并置 `starting_` → **锁外**调 `DoStart()` → 成功则由**基类**置 `Running`。**不承诺并发调用共享同一次初始化结果**：另一次 `Start()` 正在初始化时返 `kInvalidState`。旧形态用一次性 `SharedCompletion` 共享首次结果，而它 latch 后不再更新，第二轮重试的调用方会拿到上一轮的陈旧失败（#150）；启动是宿主调一次的动作，不值得为此保留一个坏 latch。
- **`Close()`**：首个关闭者 `Running→Closing` → **锁外**调 `DoClose()` 发出全部汇合信号即返回。从未 `Start()` 过则直接落 `Closed`（无 fiber 可汇合，不调 `DoClose()`）。幂等，仲裁点为 `lifecycle_` 单点。
- **`WaitClosed()`**：`joined_` 闩保证幂等（`FiberTask::get()` 是一次性的，第二个等待者会立刻拿到假的"已收敛"）→ **锁外**调 `DoJoin()` 让出式 join → 置 `Closed`。返回即意味着全部内部 fiber 已不再运行、不再触碰节点成员——这是"可安全析构"的充分条件。
- **致命错误自终（原 ADR-0005 D5）**：不再是独立分支。读-分发循环退出时**无条件调公开的 `Close()`**——我方 `Close` 所致时是幂等空操作，传输终结所致时即自终。两条路径由此合并为一条，不需要"退出时是否仍 `Running`"的判据。
- **析构**：基类析构时子类已析构完毕、虚派发已退回基类（纯虚 ⇒ UB），故每个具体 node 在**自身**析构函数体内调 `Close()` 后再 `WaitClosed()`。

**执行时序/状态**：见 §4.2.5（关闭收敛）、§4.2.7（生命周期状态），两图已按本节重绘。

### 5.5 交互节点详细设计（CSU_PROTOCOLNODE / CSU_DDSNODE）

> **变更（ADR-0008 D5/D6/D7）**：`ProtocolNode` 已重写，下文中与之冲突处以本注为准。
> - **不再拥有、也不启停传输**：改为按引用借用，宿主负责传输的 `Start` / `Close` / `WaitClosed`；读侧走 `AsyncRead()->shared()` 取独立订阅，`DoClose()` 只关闭该订阅，源队列与其它订阅者不受影响。由此一条传输可被多个节点共用。
> - **请求关联改由 `Dispatcher`**：`PendingTable`、`CorrelationKeyStrategy`、`ProtocolKey`、`kResponseMarker` 及"响应命令码归一化"全部删除。键提取收为一行 `make_tuple(session_id, message_id, frm_type)`——帧类型成为键的独立字段后，请求与回应可直接区分。新增公开的 `Subscribe(Key)` 供分段交互与旁路监听。
> - **`session_id` 简化为自增计数器**：`std::uint8_t` 每次取用后自增、越过 255 自然回绕。0..255 空闲集、FIFO 退休窗口、RAII 租约与 `kResourceExhausted` 边界一并删除（推翻 RT_REQUEST_005/006）。
> - **八个观测接口全部删除**，丢弃只经 `ITraceSink` 上报。
> - 公开面收为 5 个：构造 / 析构 / `Request(Message, milliseconds)` / `Send(Message)` / `Subscribe(Key)`，另继承基类的四个生命周期方法。
> - **再变更（ADR-0010 D10，2026-08-26）**：`Request(Message, milliseconds)` **已删除**——与 `RequestForResponse` 语义相近而不相同，后者以 `max_attempts = 1` 完全覆盖其行为。`Send` 保留。
> - **当前公开面（截至 2026-08-26）**：构造 / 析构 / `Send(Message)` / `RequestForResponse` / `RequestForResult` / `RequestForResultDirect` / `Subscribe(Key)`，另继承基类的四个生命周期方法。
> - **新增（ADR-0010，2026-08-26）**：三个交互模式方法 `RequestForResponse` / `RequestForResult` / `RequestForResultDirect`（见下"交互模式"）。`Send` 保留；`Request` 已删除（**D10**）。
> - **配置面（#173，2026-08-26）**：`ProtocolNodeConfig` 现仅余 `protocol_id` 与 `trace_sink` —— `default_request_timeout` **已删除**。它是为已删除的 `Request`（单一总超时）而设，四种交互各阶段的时限是数量级不同的量，一个节点级缺省值套不上去（ADR-0010 **D6**：逐次传参）。由此 `ProtocolNode::ValidateConfig()` 失去唯一校验项、连同 `DoStart()` 中的调用一并删除（留一个恒成功的私有函数属死代码）。
>   **"不得永不超时"的保护未随之丢失**，改由 `ValidateInteraction()` 的**参数校验**承担——它拒绝任何非正的 `RetryPolicy::timeout` / `result_timeout`（返 `kInvalidArgument`），比缺省值更硬：缺省值只在调用方省略时兜底，参数校验则**拒绝**。SRS §3.1.4.4 对应条文已同步作废。

**单元设计决策（DD-3/DD-4）**：继承 `NodeBase`（生命周期），组合 `ICodec`+`Dispatcher`（**ADR-0009**：不再组合 `HandlerLoop`）、**按引用借用** `ITransport`，协议特有语义全内联本类；DdsNode 复用同套基座仅换键字段（D10 实证）。

**设计约束**：`Send` / `RequestFor*` 三个交互方法仅 Running 放行（否则 kClosed；`Request` 已随 ADR-0010 D10 删除）；session_id 空间协议特有（uint8=256）内联 ProtocolNode；correlation_id 确定性（node_id:序号）内联 DdsNode。

**软件逻辑（CSU_PROTOCOLNODE）**：见 `node/ProtocolNode.cpp`。
- **读-分发循环**（ADR-0006 D5 起为 node 的实现细节）：私有 `SpawnReadLoop()` 起一条长寿 fiber，`await(rx_)`（`AsyncRead()->shared()` 取得的本节点读订阅）→ 错误分类（仅 `kClosed` 退出、其余瞬时错误继续）→ 本类 `DecodeAndDispatch()``；退出后调基类 `ConvergeAfterReadLoop()` 兼任收敛者。两个 node 各持一份逐字相同的 13 行——D5 明确接受该重复（"不构成需要共享的机制"）；第三个 node 出现前不宜再抽共享件。
- **键派生**（ADR-0008 D6 后）：无独立策略件——`Dispatcher` 的键提取函数在 `ProtocolNode` 构造期以一行 lambda 给出：`make_tuple(session_id, message_id, frm_type)`。~~`CorrelationKeyStrategy`、`ProtocolKey`、`kResponseMarker` 与"响应键清标记位归一化"~~ 均已删除。
- **session_id**（ADR-0008 D7 后）：`std::uint8_t next_session_` 自增计数器，`NextSession()` 取用后自增、越过 255 自然回绕。~~空闲集 `deque<uint8>`、`pop_front` 取最久释放者、FIFO 退休窗口、`SessionLease` RAII 归还、256 全在途返 `kResourceExhausted`~~ 均已删除；**在途超过 256 时标识重复**，两个订阅落入同一桶、一条响应同时投给二者（SRS RT_REQUEST_MOT_2 已记该边界）。
- **Dispatch**（ADR-0009 D1/D5）：投递给全部键匹配的订阅者，各得一份副本。`kResponse`/`kResult`=响应帧未命中时仍归因 `kUnmatchedOrLateResponse`；**业务帧无人认领则静默丢弃、不归因**（订阅模型下无订阅者是常态而非异常，见 SRS §3.1.5.4）。
- **交互模式（ADR-0010，RT_NODE_002_a..g）**：四个方法，其中三个属**外部系统协议**（`Send` / `RequestForResponse` / `RequestForResult`——后者对应协议里的 `withfeedback` 与 `needfeedback`，二者经核实为**同一个通信模型**），一个属**另一种协议**（`RequestForResultDirect`）。**模式不作参数、不入节点状态**——状态机的阶段、已发送次数与原始命令帧全是该方法的局部变量，活在**调用方 fiber 的栈**上，故节点无"在途交互表"、`Dispatcher` 不认识模式。各方法的公共骨架：
  1. 取 `session_id` → 盖章；
  2. **发命令之前**同时登记两个订阅 `{sid, mid, kResponse}` 与（③④）`{sid, result_mid, kResult}`——`kResult` 可能先于 `kResponse` 到达，等收到受理再登记会丢帧（**D4**）；
  3. 第一阶段：发帧 → 等 `kResponse`，超时则**重发字节完全相同的原帧**（`session_id` 不变，**D3**），至多 `max_attempts` 次；耗尽返 **`kNotAccepted`**（**D12**）；
  4. 收到首个 `kResponse` 后**立即 `Reset()` 该凭据**（**D5**）——否则重发引出的重复受理帧会继续落入信箱；注销后它们成为无匹配终结帧，按 `kUnmatchedOrLateResponse` 归因；
  5. ③④ 第二阶段：等 `kResult`，超时返 `kTimeout`，**不重发**（**D2/D5**：`kResult` 未达意味着对端正在执行）；
  6. `RequestForResult` 收到 `kResult` 后回一帧回应（**该模型固有的最后一步**），该帧**完全由收到的 `kResult` 派生**：payload 原样回显、`session_id`/`message_id` 沿用不变、**仅**把 `frm_type` 改为 `kResponse`，CRC 由 `ICodec::Encode` 重算（`ProtocolNode` 不碰）。**不接受任何调用方参数**，故 ④ 与 ③ **签名相同**。该帧**不得走 `Send()`**（它会强制盖新 `session_id` 与 `kCommand`），走不盖章的私有 `EncodeAndWrite()`（**D8**）。

  7. **`RequestForResultDirect`（另一种协议）**：无受理阶段——登记 `{sid, result_mid, kResult}` 一个订阅，发命令后**直接等结果**，超时即**重发**（与 3 同法），耗尽返 `kTimeout`；收到即成功，**不回应**。其"等结果可重发"与 `RequestForResult` 的"等结果不重发"并存——后者是**外部系统协议**的约束，非框架普遍规则（**D13**）。

  **实现注记**：重发的帧字节完全相同，可编码一次重复写出，不必每次 `Encode`。
  **接收侧不建模**（**D9**）：节点收到 `kCommand` 后如何应答由宿主 `Subscribe` 自理，框架不提供对应辅助。
- **链路断开处置（DD-11/DD-12，取代原 reactor）**：**交互层不参与**——重连由传输内部透明完成，读循环无断链分支。不批量终结在途请求、不清空排队业务、**无 reactor 协程、无能力探测**——三介质同一段读循环（仅区分 `kClosed` 与其余）。
- **处理器能力面（RT_LIFECYCLE_005 / ADR-0006 D8；ADR-0009 D1 后仅存 DDS 侧）**：`HandlerContext` 已随 `ProtocolNode` 的 handler 通道一并移除；`DdsHandlerContext` **保留** `RequestClose()`，但其语义为**只发起、不等待**——内部调框架的发信号路径 `SignalCloseIfFirstCloser()` 而非会等待的 `Close()`，受理即返回,收敛由读-分发循环完成。命名与 `ITransport::RequestClose()`（发信号）/ `WaitClosed()`（等待）的既有约定一致。**返回值仅表示"已受理"，不表示"已关完"**；处理器若需确认关闭完成，只能经可观测状态,不得在处理器内等待。

**软件逻辑（CSU_DDSNODE）**：见 `node/DdsNode.cpp`。correlation_id 生成、`kReply` 终结判别、topic 寻址、`reply_to=inbox`；`Request(Message,target)` 盖 kRequest + Register(correlation_id) + WriteFramed；`Publish` 盖 kNotify fire-and-forget；`DdsHandlerContext::Reply` 对入站 kRequest 回送 kReply。无连接（D3′），无 reactor/重连。

**执行时序/数据流**：见 §4.2.3/§4.2.4/§4.2.6。

### 5.6 传输层详细设计（CSU_IO）

**单元设计决策（ADR-0007 D1，UDP 先行）**：各介质实现**唯一**的 `ITransport` 契约（含链路可用性，DD-7），并统一为「**socket 管理泵 + 读写双队列**」形态——外层循环负责按配置创建/重建 socket 与失败重试，内层循环把 I/O 数据投入 `read_queue`；写侧由消费者从 `write_queue` 取出发出。socket 的生命周期与数据面由此**彻底解耦**：重建不波及正在等待的读者。**本轮仅 `UdpTransport` 落地该形态**，`TcpClientTransport` 已是其前身（#109 的连接泵 + 对外通道），`TcpTransport`/`SerialTransport` 待跟进（队列策略差异见 TBD-009）。
连接管理（TCP 客户端）与纯管道分离并**维持两层**（ADR-0004 D8：合并只会复制收发语义）；TCP 客户端内部改为**连接泵 + 对外通道**（ADR-0004 D6）；DDS 跨线程有界交接闭合 ADR-0001 未决项。

**设计约束**：并发写串行化保留（RT_TRANSPORT_004；其"单读"约束已随 ADR-0007 D4 删除，`AsyncRead()` 交出等待器句柄、是否共享由调用方 `shared()` 决定）、**发送完成语义与背压已撤销**（DD-6）；UDP/DDS 单次一报文/样本，过大发送前失败；**读取终止语义**（DD-11）：不可重连介质致命错误返 `kClosed`，可重连介质链路中断**对调用方透明**（`Read` 挂起至新链路就绪，不返回任何断链错误）；socket/串口在节点执行域 fiber 内创建（亲和纪律）。

**软件逻辑**：见 `src/io/*`。

> **实况标注（2026-08-26 核对）**：下表中**只有 `UdpTransport` 已按 ADR-0007/0008 的新形态实现并参与编译**。`TcpTransport` / `TcpClientTransport` / `SerialTransport` / `DdsTransport` / `TcpServer` 五行描述的是**重设计之前的形态**，其源文件当前**不在 `CMakeLists.txt` 的库源清单内**（编译面收窄至 UDP + ProtocolNode），行内提及的 `Read()`、`BoundedQueue` 等已是历史 API。这些单元的复活与改写属"编译面恢复"。

| 单元 | 关键逻辑 |
|---|---|
| TcpTransport | 接管已连接 socket；复用 corosocket `readAll` 流；写路径**并发写按到达序串行化**（不再等字节进内核）；对端断开 → `Read` 返 `kClosed`（不可重连，DD-11） |
| TcpClientTransport | connect-loop fiber 持 socket 跑状态机 `Connecting/Connected/Reconnecting`，建连后以流式读取器持续取数投入**对外通道**；`Read` 从该通道取；断链**不发信号**，connect-loop 转入重连、`Read` 自然挂起至新链路数据到达（完全透明，DD-11）。`Write` **直操当前 socket**（重连期立即返 `kConnection`，不缓存——RT_TCP_RECONNECT_003），不经通道。`abort()+deleteLater()` 管超时；**固定重连间隔 1s**（ADR-0005）；`ApplyConfig`/`Generation()`/`AttemptCount()` 等降级为**具体方法**（ADR-0004 D7）。**已知缺口**：对外通道无界（见 §7.2） |
| UdpTransport | **socket 管理泵 + 读写双队列**（ADR-0007，样板实现）。外层循环：按配置 bind → 失败**按 `silence_timeout` 所定间隔（默认 5 s）重试、无限重试**，唯一退出条件是我方 `Close`（**不自终**，RT_LIFECYCLE_008 的介质清单已去掉 UDP）。内层循环：`await` 报文流（带**静默超时**，可配、`0` 禁用、默认禁用）→ 投入 `read_queue`；流终止或静默超时 → 退出内层回外层重建。`Read()` 交出 `read_queue` 句柄;`Write()` 投入 `write_queue` 即返（fire-and-forget，链路不可用时排队等待恢复，恢复后按序全部发出）。寻址 kDefault→config 默认 / kNet→ip:port |
| SerialTransport | coroiodevice 字节流；设备断开/致命 → `Read` 返 `kClosed`（不重连，TBD-005） |
| DdsTransport | 组合 IDdsProvider + `BoundedQueue<Sample>` 跨线程交接；listener 线程非阻塞 Push（满归因 kDdsHandoffOverflow）；`Read` 出队 fiber |
| TcpServer | corotcpserver accept 循环 fiber；每连接经 NodeFactory 派生 ProtocolNode + supervisor fiber |

**执行时序/状态**：见 §4.2.8（连接状态机）、§4.2.10（传输层泵与双队列）、§4.2.11（动态生命周期）。

### 5.7 编解码层详细设计（CSU_CODEC）

**单元设计决策**：`ICodec` 可有状态（`SystemCodec` 单线程喂滚动缓冲）或无状态并发（`DdsCodec` 多 listener 线程并发喂）；坏帧诊断 + 重同步。

**设计约束**：`Encode` 一对一；`Decode` 半包返回空、粘包返回多条；分帧错误 `kFrame`、语义错误 `kCodec`。**不设重置操作**（ADR-0004 D4 撤销）——透明重连后残尾与新链路首字节拼成的错帧，由既有校验与重同步处置（报坏帧、计入坏帧计数后恢复）。

**软件逻辑**：`SystemCodec`（外部协议流式，占位帧常量 TBD-003，跨切片拼帧 + 重同步）；`DatagramCodec`（报文式保边界）；`DdsCodec`（kind/correlation_id/reply_to 元数据，无状态）；`LengthFieldCodec`（长度字段分帧基元）。

**执行时序/数据流**：见 §4.2.3（Decode 在读循环、Encode 在出站）。

## 6. 需求可追踪性

与 REF-1（SRS）构成追溯关系。§6.1 为本文档每个设计单元到 SRS 需求的对应；§6.2 为每个 SRS 需求到设计单元的对应。

### 6.1 设计单元 → 需求

| 设计单元 | 类型 | 对应 SRS 需求 | 章节 |
|---|---|---|---|
| DD-1..DD-14 | 设计决策 | 见各决策标注（DD-11/12 由 ADR-0004 引入，DD-13 由 ADR-0005 引入且已被 ADR-0008 推翻，DD-14 由 ADR-0010 引入） | §3 |
| CSC_CORE | 部件 | RT_ERROR、RT_DATA_MESSAGE、RT_TRACE、RT_DESIGN_005 | §4.1.1、§5.1 |
| CSC_IO | 部件 | RT_TRANSPORT、RT_TCP_RECONNECT/RECONFIG、RT_IF_*、RT_IN_INTERFACE_002/003 | §4.1.2、§5.6 |
| CSC_CODEC | 部件 | RT_CODEC、RT_IF_SYSFRAME | §4.1.3、§5.7 |
| CSC_NODE | 部件 | RT_NODE、RT_REQUEST、RT_INBOUND、RT_LIFECYCLE、RT_DESIGN_003/008 | §4.1.4、§5.2–5.5 |
| MS_DFD_CONTEXT / MS_DFD_TOPLEVEL | 执行方案 | RT_IN_INTERFACE_001、RT_TRANSPORT、RT_REQUEST | §4.2.1、§4.2.2 |
| MS_NODE_DATAFLOW / MS_REQ_RESP | 执行方案 | RT_REQUEST、RT_NODE_003 | §4.2.3、§4.2.4 |
| MS_CLOSE / MS_NODE_LIFECYCLE | 执行方案 | RT_LIFECYCLE_001/003–007 | §4.2.5、§4.2.7 |
| MS_TRANSPORT_PUMP | 执行方案 | RT_TRANSPORT_008/010、RT_IF_UDP、RT_LIFECYCLE_008 | §4.2.10 |
| MS_LINK_DOWN / MS_CONNECTION | 执行方案 | RT_TCP_RECONNECT、RT_TRANSPORT_008、RT_LIFECYCLE_002 | §4.2.6、§4.2.8 |
| MS_INTERACTION_MODES | 执行方案 | RT_NODE_002_a..g | §4.2.12 |
| MS_TICKET | 执行方案 | RT_REQUEST_003/004 | §4.2.9 |
| MS_DYNAMIC_LIFECYCLE | 执行方案 | RT_CORO_RUNTIME、RT_NODE_004、RT_DESIGN_004 | §4.2.11 |
| JK_NODE_API | 接口 | RT_IF_API | §4.3.2 |
| JK_CODEC | 接口 | RT_CODEC、RT_IF_SYSFRAME、RT_DESIGN_006 | §4.3.3 |
| JK_TRACE | 接口 | RT_TRACE_001/002 | §4.3.4 |
| JK_TRANSPORT | 接口 | RT_IN_INTERFACE_002、RT_TRANSPORT_008/009/010 | §4.3.5 |
| JK_PROVIDER | 接口 | RT_IN_INTERFACE_003、RT_IF_DDS | §4.3.6 |
| CSU_CORE | 详细设计 | RT_ERROR、RT_DATA_MESSAGE、RT_TRACE、RT_CORO_RUNTIME_005 | §5.1 |
| CSU_DISPATCHER | 详细设计 | RT_REQUEST_001..004、RT_IN_INTERFACE_004、RT_DESIGN_008 | §5.2 |
| ~~CSU_BOUNDEDQUEUE~~ | 已删除（ADR-0008 D8） | RT_DATA_BUFFER 的上界与归因**不再满足**；原 RT_HANDLER_004 已由 RT_INBOUND_003 承接（改由结构保证，不依赖队列上界） | §5.3 |
| CSU_NODEBASE | 详细设计 | RT_LIFECYCLE、RT_NODE_003、RT_DESIGN_008 | §5.4 |
| CSU_PROTOCOLNODE | 详细设计 | RT_REQUEST、RT_NODE_002_a..g（DD-14）、RT_NODE_003、RT_INBOUND、RT_TCP_RECONNECT | §5.5 |
| CSU_DDSNODE | 详细设计 | RT_NODE_004/005/007、RT_IF_DDS、RT_REQUEST | §5.5 |
| CSU_IO | 详细设计 | RT_TRANSPORT、RT_TCP_RECONNECT/RECONFIG、RT_NODE_006、RT_IF_* | §5.6 |
| CSU_CODEC | 详细设计 | RT_CODEC、RT_IF_SYSFRAME | §5.7 |

### 6.2 需求 → 设计单元

| SRS 需求 | 对应设计单元 |
|---|---|
| RT_CORO_RUNTIME_001..005 | DD-2 / CSC_CORE、CSC_NODE / CSU_NODEBASE / MS_DYNAMIC_LIFECYCLE |
| RT_TRANSPORT_001..009 | DD-6、DD-11 / CSC_IO / CSU_IO / JK_TRANSPORT / MS_DFD_TOPLEVEL |
| RT_CODEC_001..006 | CSC_CODEC / CSU_CODEC / JK_CODEC |
| RT_REQUEST_001..006 | DD-8 / CSC_NODE / CSU_DISPATCHER、CSU_PROTOCOLNODE / MS_REQ_RESP、MS_TICKET（**RT_REQUEST_005/006 已由 ADR-0008 D7 推翻**） |
| ~~RT_HANDLER_001..006~~ | **整组废止（ADR-0009）**，由 RT_INBOUND 取代；废止/承接对照见 SRS §3.1.5 引言 |
| RT_INBOUND_001..005 | CSC_NODE / CSU_PROTOCOLNODE、`Dispatcher` / MS_NODE_DATAFLOW（RT_INBOUND_003"不阻断解复用"由**结构**保证：投递非阻塞、消费在宿主 fiber）。**RT_INBOUND_004 的信箱容量与丢弃语义见 TBD-009** |
| RT_LIFECYCLE_001..007 | DD-7、DD-13 / CSU_NODEBASE / JK_TRANSPORT / MS_CLOSE、MS_NODE_LIFECYCLE、MS_CONNECTION |
| RT_NODE_001..007 | DD-3 / CSC_NODE / CSU_PROTOCOLNODE、CSU_DDSNODE / MS_NODE_DATAFLOW |
| RT_NODE_002_a..g（四种交互模式，ADR-0010；`repeating` 已废止，无遗留 TBD） | **DD-14** / CSU_PROTOCOLNODE §5.5「交互模式」/ **MS_INTERACTION_MODES**（§4.2 图 `seq-interaction-modes`）。`repeating` 仍为 TBD，无设计落点 |
| RT_TCP_RECONNECT_001..005 | DD-11、DD-12 / CSU_IO、CSU_PROTOCOLNODE / JK_TRANSPORT / MS_LINK_DOWN、MS_CONNECTION |
| RT_TCP_RECONFIG_001..006 | CSU_IO（TcpClientTransport::ApplyConfig） |
| RT_ERROR_001..003 | DD-5 / CSC_CORE / CSU_CORE |
| RT_TRACE_001/002 | DD-10 / CSC_CORE / CSU_CORE / JK_TRACE |
| RT_DATA_MESSAGE/STATE/CONFIG/BUFFER | CSC_CORE、CSC_NODE / CSU_CORE、CSU_DISPATCHER（**RT_DATA_BUFFER 的队列上界与观测计数已随 ADR-0008 D8/D10 回退**） |
| RT_IN_INTERFACE_001..005 | DD-1 / JK_TRANSPORT、JK_CODEC、JK_PROVIDER、JK_NODE_API |
| RT_IF_API/SYSFRAME/TCP/UDP/SERIAL/DDS | JK_NODE_API、JK_CODEC、JK_TRANSPORT / CSU_IO、CSU_CODEC |
| RT_DESIGN_001..008 | §3 CSCI 级设计决策 DD-1..DD-14 / 全体单元 |
| RT_PERFORMANCE/TESTABILITY/SECURITY | 留 P6（不在本设计说明范围） |

---

## 7. 注解

### 7.1 术语缩略语

| 术语 | 含义 |
|---|---|
| CSCI / CSC / CSU | 计算机软件配置项 / 软件部件 / 软件单元 |
| JK / MS / DD | 接口（本文 ID）/ 执行方案-图（本文 ID）/ 设计决策（本文 ID） |
| 缝（seam） | 可测替换点（接口边界） |
| 节点所属执行域 | node 首次启动绑定的稳定 fiber 调度域 |
| 连接代际 | 每次成功物理连接递增的单调计数；**内于 `TcpClientTransport`**，仅作其内部记账与诊断，交互层不感知（ADR-0004 D3） |
| 链路可用性 | 所有介质同形的当前 I/O 事实（链路此刻是否可用），经 `ITransport` 基类暴露；不含连接管理策略（ADR-0004 D2） |
| ~~四方仲裁~~ | **已作废（2026-08-26 核对）**：原指"Resolve/超时/取消/FailAll 经**表锁 find+erase** 抢占终结权"。`PendingTable` 及其表锁仲裁已随 ADR-0008 D6 删除（改由 `Dispatcher` 按键投递）；请求取消能力亦已撤销（ADR-0008 D8 + 2026-08-26 裁决）。现行终结方为**三方**：响应 / 超时 / 关闭·断连 |
| loss=0 harness | 断言"无静默丢失"的测试：Σ命名计数 = 总丢弃。**覆盖面已收窄**（SRS §3.4.4）：无订阅者的业务帧不归因（ADR-0009 D5）、队列满时丢最旧且无计数（TBD-009 / #152）均不在其内；且该 harness 当前**不参与编译** |

### 7.2 未决项与技术债（详见 REF-3 §6、REF-2）

~~五模式精确状态机（TBD-001）~~ **已关闭**：交互行为已由 **ADR-0010** 全部定义（`repeating` 于 2026-08-26 废止，无遗留），实现见 #167 / #168；帧常量/CRC 真值（TBD-003）；性能基线固化（TBD-004，P6）；串口自动重连（TBD-005）。测试技术债：CGNAT 连接超时 flake、`FakeDdsProvider` 静态总线 repeat artifact。延后重构：node getter 聚合 `NodeStats`（等 P6 指标定型）。

### 7.3 图形来源与再生成

全部图形以 Mermaid 源保存于 `docs/diagrams/*.mmd`，渲染为同名 `.svg`。再生成：

```bash
cd docs/diagrams
for f in *.mmd; do
  npx -y @mermaid-js/mermaid-cli -i "$f" -o "${f%.mmd}.svg" \
    -c mermaid-config.json -p puppeteer-config.json
done
```

| 图号 | 类型 | ID | 源文件 |
|---|---|---|---|
| 图 4-1 | 部件依赖 | — | `sdd-csc-layers.mmd` |
| 图 4-2 | 类图 | CSC_IO | `sdd-csc-io.mmd` |
| 图 4-3 | 类图 | CSC_NODE | `sdd-csc-node.mmd` |
| 图 4-4 | 数据流 | MS_DFD_CONTEXT | `dfd-context.mmd` |
| 图 4-5 | 数据流 | MS_DFD_TOPLEVEL | `dfd-toplevel.mmd` |
| 图 4-6 | 数据流 | MS_NODE_DATAFLOW | `dataflow.mmd` |
| 图 4-7 | 时序 | MS_REQ_RESP | `seq-request-response.mmd` |
| 图 4-8 | 时序 | MS_CLOSE | `seq-close.mmd` |
| 图 4-9 | 时序 | MS_LINK_DOWN | `seq-link-down.mmd`（原名 `seq-generation-isolation.mmd`，#112 改名并按新流程重绘） |
| 图 4-10 | 状态 | MS_NODE_LIFECYCLE | `state-node-lifecycle.mmd` |
| 图 4-11 | 状态 | MS_CONNECTION | `state-connection.mmd` |
| 图 4-12 | 状态 | MS_TICKET | `state-dispatch-ticket.mmd` |
| 图 4-13 | 时序 | MS_TRANSPORT_PUMP | `seq-transport-pump.mmd`（ADR-0007 引入） |
| 图 4-14 | 时序 | MS_INTERACTION_MODES | `seq-interaction-modes.mmd`（ADR-0010 引入；四种交互模式的状态机与失败码） |
| 附图 | 类图 | 总体 | `arch-class.mmd` |

---

*本文件基线：master（v0.4.5+）。与 SRS 冲突时以 SRS 优先；路线图以 REF-3 为准。ID 体系对齐 REF-4 AsyncTask SDD。*
