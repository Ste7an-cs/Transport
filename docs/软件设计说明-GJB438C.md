# 软件设计说明（SDD）

**文档标识：** transport-SDD-438C　**版本：** v2.1　**日期：** 2026-08-06
**编制依据：** GJB 438C《军用软件开发文档通用要求》——软件设计说明模板（对齐 AsyncTask SDD 惯例：CSC 部件 / CSU 单元 / JK 接口 / MS 执行方案 / DD 设计决策 + 双向追溯）
**对应软件基线：** master（里程碑 P0–P5 已交付，版本 `v0.4.5`+）
**需求基线：** `docs/需求规格说明书-协程原生.md`（SRS，标识前缀 `RT_`）　**决策依据：** `docs/adr/`（ADR-0001/0002/0003/**0004/0005**）

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
| REF-2 | `docs/adr/0001..0005` 架构决策记录（协程原生总纲 / 发送-丢弃-生命周期 / 目标架构与路线图 D1–D13 / 传输语义统一与链路可用性 / **收敛并入读循环与固定间隔重连**） |
| REF-3 | `docs/设计说明书-协程原生.md` 目标架构 + 分期路线图（路线图权威） |
| REF-4 | `third_party/AsyncTask/doc/{需求规格说明,软件设计说明,使用说明}.md` AsyncTask 运行时文档 |
| REF-5 | `CODING_STANDARDS.md` 编码规范；`CONTEXT.md` 术语权威 |
| REF-6 | ISO/IEC 14882:2017（C++17）；Boost.Fiber（≥1.89.0）；Qt 5（≥5.12）；Fast DDS 2.13.x（可选） |

## 3. CSCI 级设计决策

从设计视角说明各构件如何满足需求及其设计原则；每条决策标注所满足的 SRS 需求，可回溯 ADR（REF-2 决策编号 Dn）。

- **DD-1 三层解耦 + 内部缝（满足 RT_IN_INTERFACE_001/002、RT_DESIGN_006）：** 传输（纯字节管道）/ 编解码（线缆格式）/ 交互节点三层，以可测替换的**缝**衔接。`ITransport` 是内部缝（非用户 API），`ICodec` 是公共扩展点，编程主入口是交互层 node。原则：各层职责单一、可独立替换与单测。
- **DD-2 AsyncTask 强制运行时（满足 RT_CORO_RUNTIME_001..005、RT_DESIGN_002）：** 不设独立 `IExecutor`/`ThreadExecutor` 业务调度体系；M:N 协作式；同一节点交互状态串行访问，以一把 `std::mutex` 守临界区实现，运行时 await 只出现在 fiber 挂起点，唤醒/回调在锁外调用。**依赖性**：节点所属执行域首次启动后稳定、不迁移（RT_CORO_RUNTIME_003）。
- **DD-3 无共享交互引擎（满足 RT_DESIGN_003、RT_NODE_003）：** 不设独立 `InteractionEngine`/`InteractionPolicy` 层；协议特有语义（键派生、终结判别、寻址、帧盖章）内联各 node。公共只抽出**协议无关机制**：`PendingTable`、`BoundedQueue`、`NodeRuntime`、`SharedCompletion`。原则：语义归属清晰、不造上帝对象。
- **DD-4 协议无关基座可复用（满足 RT_DESIGN_008、D10）：** `PendingTable<Key,T>`/`BoundedQueue<T>`/`NodeRuntime<Event>` 对协议类型不透明，经模板参数/注入回调解耦。DdsNode 复用它们：仅把 Key 从 `uint32` 换为 `std::string`，基座一行不改（实证）。
- **DD-5 结果承载错误、不抛异常（满足 RT_ERROR_001/002/003、RT_DESIGN_005）：** 预期失败用 `Result<T>`/`Status`（`[[nodiscard]]`）+ 机器可判别 `TransportErrc` 类别，不靠解析字符串前缀。唯一授权的 `catch` 在 handler 消费者边界（隔离第三方逃逸异常）。
- **DD-6 发送排序（满足 RT_TRANSPORT_007/004；ADR-0004 D5）：** 同 fiber 程序序、跨 fiber 串行为一致全序，单帧字节不与另一帧交错。**发送完成语义与协程背压已撤销**（原"帧进内核才报成功"，ADR-0002 D2 → 被 ADR-0004 D5 撤销）：发送成功此后仅表示该帧已交付下层发送通路。并发写串行化**保留**——它是 RT_TRANSPORT_004 的实现手段，与完成语义是两件事。**代价**：发送侧不再有框架级内存上界（见 §7.2 已知缺口）。
- **DD-7 两个独立生命周期轴 + 链路可用性上移（满足 RT_LIFECYCLE_001/002、RT_TRANSPORT_009；ADR-0004 D2）：** 节点生命周期 `Created→Running→Closing→Closed`（全介质通用）与 TCP 物理连接状态（仅 TCP 客户端，Running 内子状态）正交。**连接管理**（状态机/重连间隔/重连策略/代际推进）不下沉纯字节管道；但**当前链路可用性**作为与 `LastSendTime`/`LastError` 同类的 I/O 事实**上移至 `ITransport` 基类**，所有介质同形作答——交互层因此不再按介质探测可选能力接口（`IConnectionObservable` 取消）。此为 ADR-0002 D3′ 的边界重划。
- **DD-11 读取终止语义单一化 + 重连完全透明（满足 RT_TRANSPORT_008；ADR-0004 D1）：** `Read` 失败中仅 `kClosed` = 传输终结（我方关闭，或不可重连传输的底层致命错误），其余为可继续的瞬时错误。不可重连介质（UDP/串口/已接受的 TCP 连接）致命错误统一返 `kClosed`；**可重连传输在内部透明处理链路中断**，不向交互层暴露断链事件。`kConnection` 此后仅存于写路径（RT_TCP_RECONNECT_003）。**交互层因此对三介质使用同一段读循环，且无链路中断分支**。
- **DD-12 撤销连接代际隔离（满足 RT_TCP_RECONNECT_002 改写；ADR-0004 D3/D4）：** 断链时交互层不再批量终结在途请求、不再清空旧链路排队业务，"代际"概念自交互层消失。在途请求由各自总超时（**缺省值 30 秒，强制项**）、取消或关闭终结。RT_REQUEST_004"旧代际响应不得完成新请求"由物理事实保证（旧 socket 已关，字节不跨链路投递；在途关联标识未释放，新请求取不到同键）。**不引入编解码器重置**（ADR-0004 D4 撤销）：断链残尾与新链路首字节可能拼成错帧，由编解码器既有校验与重同步处置（报坏帧后恢复）。
- **DD-8 恰好一次终结的挂起-应答仲裁（满足 RT_REQUEST_001..005）：** 请求↔响应关联复用 `PendingTable`，每个在途 entry 一个等待者、以裸 `Coro::Awaitable<T>` 为信箱，**表锁 `find+erase` 作唯一仲裁点**（Resolve/超时/取消/FailAll 谁先摘除谁胜）。原则：恰好一次不靠状态枚举而靠单点抢占，竞态面小。
- **DD-9 底层回调不碰节点状态（满足 RT_HANDLER_002、RT_NODE_004、RT_IN_INTERFACE_003）：** Qt I/O 回调在 socket QThread、DDS 样本在 provider listener 线程，均须安全转交节点执行域；DDS 经跨线程有界交接边界（`BoundedQueue<Sample>`）转交，跨线程唤醒靠 boost.fiber channel 跨线程安全。
- **DD-10 可插拔观测 + 完整性归因（满足 RT_TRACE_001/002、RT_DATA_BUFFER、D13）：** 每个丢弃点经唯一 `RecordDrop` 归因到**六项** `DropReason` 之一（原七项，`kGenerationIsolationDrop` 随 ADR-0004 D3 移除） + 命名计数；可选 `ITraceSink` 结构化 Trace（push）与命名计数（pull）双面；未配 sink 时零控制流影响。"无静默丢失"结构性可断言（Σ命名 = 总丢弃）。

## 4. CSCI 体系结构设计

### 4.1 CSCI 部件

本 CSCI 为纯软件库（随宿主工程编译），不需额外硬件资源。由四个软件部件（CSC）自底向上构成，上层依赖下层；AsyncTask 为强制运行时，Qt 与 Fast DDS 按宿主配置可选。部件依赖见图 4-1。

**图 4-1 CSC 部件依赖图（`sdd-csc-layers`）**

![CSC 部件依赖图](diagrams/sdd-csc-layers.svg)

**图例说明**：实线箭头读作"A 使用/组合 B"，虚线为可选依赖。`CSC_NODE` 组合 `CSC_CODEC`+`CSC_IO`、依赖 `CSC_CORE`；`CSC_CODEC`/`CSC_IO` 各依赖 `CSC_CORE`；全体运行于 `AsyncTask`；`CSC_IO` 的 Qt socket/串口依赖在宿主未启用 Qt 时不编译。整体单向依赖、上层不被下层反向引用。总体类关系见附图 `arch-class.svg`（`ITransport`/`ICodec`/node/core 的组合 ▷ 与实现 △）。

| 部件 | 目录 | 主要内容 | 响应需求 |
|---|---|---|---|
| **CSC_CORE** | `core/` | `Result`/`Status`、`TransportErrc`、`Message`、`Endpoint`、`Cancellation`、`SharedCompletion`、`ITraceSink`、`Observability`、`DropReason`、`TraceCategories` | RT_ERROR、RT_DATA_MESSAGE、RT_TRACE、RT_DESIGN_005 |
| **CSC_IO** | `io/`（`tcp/`·`udp/`·`serial/`·`dds/`） | `ITransport`（含链路可用性）、`Tcp/Udp/Serial/DdsTransport`、`TcpClientTransport`、`TcpServer`、`IDdsProvider` | RT_TRANSPORT、RT_TCP_RECONNECT/RECONFIG、RT_IF_*、RT_IN_INTERFACE_002/003 |
| **CSC_CODEC** | `codec/` | `ICodec`、`SystemCodec`、`DatagramCodec`、`DdsCodec`、`LengthFieldCodec` | RT_CODEC、RT_IF_SYSFRAME |
| **CSC_NODE** | `node/` | `ProtocolNode`、`DdsNode`、`NodeRuntime`、`PendingTable`、`BoundedQueue`、`HandlerContext` | RT_NODE、RT_REQUEST、RT_HANDLER、RT_LIFECYCLE、RT_DESIGN_003/008 |

#### 4.1.1 核心原语部件（CSC_CORE）

- **用途**：与协议/介质无关的值类型与基础并发原语，为上层提供结果承载、协作取消、多等待者完成、统一寻址与观测原语。响应 RT_ERROR_*（结果/错误）、RT_DATA_MESSAGE、RT_TRACE_*、RT_DESIGN_005。
- **主要内容**：`Result<T>=Coro::Result<T,error_code>` / `Status`；`TransportErrc` 十三类机器可判别错误；`Message`（payload + 两套元数据）；`Endpoint`（kDefault/kNet/kTopic 统一寻址）；`Cancellation`（Source/Token/Registration 协作取消）；`SharedCompletion<T>`（多等待者一次性完成，原子首胜）；`ITraceSink`+`RecordDrop`/`RecordEvent`+`DropReason`+`TraceCategories`（观测）。
- **关系与结构**：被 CSC_IO / CSC_CODEC / CSC_NODE 依赖；自身仅依赖 AsyncTask（`Coro::Result`/`Awaitable`）。结构简单，无独立类图（同 AsyncTask CSC_DETAIL 惯例），详见 §5.1。

#### 4.1.2 传输层部件（CSC_IO）

- **用途**：介质无关的纯字节管道，向节点提供协程 await 式拉模型（`Read` 一次一片）与统一 I/O 观测面（含**链路可用性**）；连接管理（TCP 客户端自动重连）与服务端 accept。响应 RT_TRANSPORT_*、RT_TCP_RECONNECT/RECONFIG、RT_IF_*。
- **主要内容**：接口 `ITransport`（内部缝，**唯一**——`IConnectionObservable` 已随 ADR-0004 D2 取消）；实现 `TcpTransport`（已连接）、`TcpClientTransport`（连接管理）、`UdpTransport`、`SerialTransport`、`DdsTransport`、`TcpServer`；provider `IDdsProvider`（Fake/FastDDS）。
- **类图**：见图 4-2。
- **关系与结构**：依赖 CSC_CORE；被 CSC_NODE 组合（node 持 `unique_ptr<ITransport>`）。

**图 4-2 CSC_IO 类图（`sdd-csc-io`）**

![CSC_IO 类图](diagrams/sdd-csc-io.svg)

**图例说明**：各介质传输实现**唯一**的 `ITransport`（含链路可用性；`IConnectionObservable` 已取消，交互层无 `dynamic_cast` 探测）；`TcpClientTransport` 内部以连接泵 + 对外通道承载读路径、`Write` 直操当前 socket，并组合内层 `TcpTransport` 复用收发语义；`DdsTransport` 组合 `IDdsProvider`，listener 线程非阻塞 `Push` 进 `BoundedQueue<Sample>`、`Read` 侧 fiber 出队（跨线程唤醒）；`TcpServer` 每接受一条连接接管一个内层 `TcpTransport` 派生 node。
> 注：本图 SVG 尚未随 ADR-0004 重渲染（渲染工具在当前环境不可用），`.mmd` 源以本节文字为准。

#### 4.1.3 编解码层部件（CSC_CODEC）

- **用途**：逻辑 `Message` ↔ 线缆字节的分帧、序列化、校验、重同步；应用可提供并装配的公共扩展点。响应 RT_CODEC_*、RT_IF_SYSFRAME。
- **主要内容**：接口 `ICodec`（`Encode` 一对一 / `Decode` 0..N）；实现 `SystemCodec`（外部协议流式，占位帧常量）、`DatagramCodec`（报文式保边界）、`DdsCodec`（DDS 元数据，无状态并发）、`LengthFieldCodec`（长度字段分帧基元）。
- **关系与结构**：依赖 CSC_CORE（`Message`/`Result`）；被 CSC_NODE 组合（node 持 `unique_ptr<ICodec>`）。结构简单，无独立类图，详见 §5.5。

#### 4.1.4 交互层部件（CSC_NODE）

- **用途**：在传输 + 编解码之上组合请求关联、入站分发、超时、连接状态与协议交互；薄壳组合、不共享引擎。响应 RT_NODE_*、RT_REQUEST_*、RT_HANDLER_*、RT_LIFECYCLE_*、RT_DESIGN_003/008。
- **主要内容**：交互节点 `ProtocolNode`（外部协议请求-响应）、`DdsNode`（DDS pub-sub + 多路请求-应答）；协议无关机制 `NodeRuntime<Event>`（生命周期三方汇合 + handler 消费者 + 读循环骨架）、`PendingTable<Key,T>`（挂起-应答）、`BoundedQueue<T>`（有界业务队列）；`HandlerContext`（handler 能力面）。
- **类图**：见图 4-3。
- **关系与结构**：依赖 CSC_CORE，组合 CSC_IO + CSC_CODEC。

**图 4-3 CSC_NODE 类图（`sdd-csc-node`）**

![CSC_NODE 类图](diagrams/sdd-csc-node.svg)

**图例说明**：`ProtocolNode`/`DdsNode` 组合 `NodeRuntime`+`PendingTable`（不继承、不共享引擎）；`NodeRuntime` 拥有 `BoundedQueue`；node 在消费者 fiber 内构造 `HandlerContext` 传入 handler。`PendingTable` 的 entry 为裸 `Coro::Awaitable<T>` 信箱，表锁 `find+erase` 作唯一仲裁点。

### 4.2 执行方案

说明软件单元间的动态关系、控制流/数据流、状态转换与动态生命周期。各图以 SVG 给出（`docs/diagrams/`，Mermaid 源随图保存）。

#### 4.2.1 数据流上下文图（MS_DFD_CONTEXT）

transport 作为单一 CSCI，外接四个实体：宿主应用、通信介质对端、AsyncTask 运行时、可选 DDS Provider。

**图 4-4（`dfd-context`）**

![数据流上下文图](diagrams/dfd-context.svg)

**图例说明**：① **宿主应用**——装配 codec / 注册 handler、发起 Request/Send/Publish，取回 Result/Message/连接状态/观测；② **通信介质对端**——收发字节/样本、断连事件；③ **AsyncTask 运行时**——框架请求它创建/恢复协程、超时、跨线程唤醒；④ **DDS Provider**（可选）——按 topic 收发、listener 回调样本。要点：框架不产生业务数据，只在四方之间搬运字节与关联/分发。

#### 4.2.2 顶层数据流（MS_DFD_TOPLEVEL）

分解为 5 个加工（P1–P5）与 3 个数据存储（D1–D3）。

**图 4-5（`dfd-toplevel`）**

![顶层数据流图](diagrams/dfd-toplevel.svg)

**图例说明**：一条请求-响应/业务帧的完整走向——**P1 出站**盖章+Encode+Write，登记 D1、分配 D3 的 session_id；**P2 入站读循环** Read+Decode+Dispatch 分类，响应帧→D1 的 Resolve、业务帧→D2 的 Push；D1 的 Resolve 唤醒 P1 的 `Handle.Wait` 交回 Result；**P4 业务处理**从 D2 `Pop` 交单消费者 handler，handler 可经 P1 回送；**P5 生命周期/连接**驱动 D1 的 FailAll（Close latch / 断连不 latch）与 D2 的 Close/Drain。

**数据存储说明：**

| 存储 | 实现 | 读者 ← 写者 | 一致性保护 |
|---|---|---|---|
| D1 在途请求表 | `PendingTable`：`map<Key, Entry{Awaitable<T> mailbox, ...}>` | P1 等待者 ← P2 Resolve / P5 FailAll | 一把 `std::mutex`；`find+erase` 单点仲裁 |
| D2 业务队列 | `BoundedQueue`：`deque<Item>` + 双上界 | P4 消费者 Pop ← P2 Push | 一把 `std::mutex`；满 tail-drop |
| D3 交互状态 | session 空闲集 `deque<uint8>`（**连接代际已移出交互层**，ADR-0004 D3） | node 各方法 | 一把 `std::mutex`（node 私有） |

#### 4.2.3 请求-响应节点数据流（MS_NODE_DATAFLOW）

**图 4-6（`dataflow`）**

![请求-响应节点数据流图](diagrams/dataflow.svg)

**图例说明**：出站（调用方 fiber）与入站（读循环 fiber）两条独立流，经 PendingTable 的 Resolve→Wait 唤醒闭环；坏帧/无匹配响应/队列满/无 handler 各自命名归因丢弃。

#### 4.2.4 请求-响应时序（MS_REQ_RESP）

**图 4-7（`seq-request-response`）**

![请求-响应时序图](diagrams/seq-request-response.svg)

**图例说明**：`Request` happy path。核心是**四方仲裁的恰好一次终结**——Resolve/超时/取消/FailAll 经表锁 `find+erase` 抢占，胜方对信箱 push/close；session_id 由 `SessionLease`（RAII）接管、返回路径自动归还。

#### 4.2.5 关闭三方汇合时序（MS_CLOSE）

**图 4-8（`seq-close`）**

![关闭三方汇合时序图](diagrams/seq-close.svg)

**图例说明**：首个关闭者发三方汇合信号（transport.RequestClose + 业务队列 Close + handler 取消 + PendingTable.FailAll）并 spawn 独立 finalizer fiber 驱动收敛；天然规避 handler 内 `RequestClose` 的自等待自锁。

#### 4.2.6 链路断开处置时序（MS_LINK_DOWN，原 MS_GEN_ISOLATION）

**图 4-9（`seq-generation-isolation`）**

![链路断开处置时序图](diagrams/seq-generation-isolation.svg)

**图例说明（ADR-0004 D1/D3/D4 后的新流程）**：链路断开对交互层**完全透明**——`TcpClientTransport` 的 connect-loop 转入重连，`Read` 因对外通道无数据而自然挂起，重连成功后新链路数据到达即被唤醒。node 读循环**无任何断链分支**，node 保持 Running。

**不再发生的动作**（原代际隔离）：不批量终结在途请求、不清空旧链路排队业务、无 reactor 协程、无状态下降沿甄别、**不向交互层发任何断链信号**。在途请求由各自总超时（缺省 30 秒）/取消/关闭终结（ADR-0004 D3）。

#### 4.2.7 节点生命周期状态（MS_NODE_LIFECYCLE）

**图 4-10（`state-node-lifecycle`）**

![节点生命周期状态图](diagrams/state-node-lifecycle.svg)

**图例说明**：`Created→Running→Closing→Closed`。并发幂等 Start 共享 `start_done_` 不重复 spawn；多等待者共享 `closed_`；重入自锁防护比对 handler fiber id。

#### 4.2.8 TCP 连接状态机（MS_CONNECTION）

**图 4-11（`state-connection`）**

![TCP 客户端连接状态机](diagrams/state-connection.svg)

**图例说明**：`Disconnected/Connecting/Connected/Reconnecting` + 连接代际递增；**固定重连间隔 1s**（指数退避已撤销，见 ADR-0005）；端点热更新掐断当前尝试立即重试。**本状态机完全内于 `TcpClientTransport`**——代际用于其自身内部记账与诊断（`Generation()`），交互层不感知（ADR-0004 D2/D3/D7）；对外只经**链路可用性**与 `Read` 的二义终止呈现。

#### 4.2.9 PendingTable entry 抢占仲裁状态（MS_PENDING）

**图 4-12（`state-pending-entry`）**

![PendingTable entry 抢占仲裁状态图](diagrams/state-pending-entry.svg)

**图例说明**：entry 单等待者，信箱为裸 `Coro::Awaitable<T>`；唯一仲裁点 = 表锁 `find+erase` 抢占终结权；本地超时抢输时在信箱 `await` 一次 drain 出对方结果（"关闭后先取尽值再报错"语义裁决竞态）。

#### 4.2.10 对象/线程/协程的动态创建与删除（MS_DYNAMIC_LIFECYCLE）

- **fiber（一个 node 内）**：`Start` 时 spawn 读-分发循环 fiber、handler 消费者 fiber（设 handler 时）；`Close` 时临时 spawn finalizer fiber 驱动收敛，收敛后退出。**reactor fiber 已随 ADR-0004 D2/D3 取消。** 均由 AsyncTask `makeTask` 创建、返回即终止。
- **传输连接代际（内于传输层）**：`TcpClientTransport` 的 connect-loop fiber 每次成功物理连接创建一个内层 `TcpTransport`（`Generation()`+1），断链销毁旧内层、隔固定间隔（1s）后建新代际；断链**不向交互层发任何信号**（完全透明，DD-11）。交互层不参与。
- **DDS 交接**：`DdsTransport` `Start` 对每 topic `Subscribe`，listener 回调在外线程构造 `Sample` 非阻塞 `Push`；`RequestClose` 先 `Unsubscribe` 停投递 → 交接边界 `Close` 唤醒在途 `Read` → provider `Shutdown`；迟到回调只捕获交接边界共享句柄、不触碰已销毁对象。
- **TcpServer 子 node**：每接受一条连接经 NodeFactory 派生 `ProtocolNode` + supervisor fiber；对端断开 → supervisor 驱动该 node `Closing→Closed` 并注销（连接生命=节点生命）。

### 4.3 接口设计

#### 4.3.1 接口标识和接口图

```
[宿主应用]
   │ 节点/配置/handler/请求/可观测      (JK_NODE_API 编程接口, CSC_NODE)
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
- 数据元素：`Message`、`OperationOptions`（deadline/取消）、各 `*Config`、`InboundHandler`、`Result<Message>`/`Status`。
- 通信方式：同步函数调用；`Request`/`Send`/`Publish`/`WaitClosed` 为协程内让出式（不阻塞线程）。
- 协议特征：`ProtocolNode(transport,codec,config)` → `Start/Close/WaitClosed`、`Request(Message,options)→Result<Message>`、`Send(Message)`；`DdsNode` → `Request(Message,target,options)`、`Publish(Message,topic)`；`TcpServer(config,factory)` 每连接派生 node。

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
- 协议特征：九类 category（connect/generation/send/recv/decode/match/timeout·cancel/handler/reconnect/lifecycle）+ `drop`（`RecordDrop` 专用，message=DropReasonName）。

#### 4.3.5 传输内部接口（JK_TRANSPORT）
- 优先级：高（框架正确性核心，内部缝）。
- 接口类型：C++ 抽象类 `ITransport`（`io/ITransport.hpp`）。
- 数据元素：`SendUnit{bytes,destination}`、`Datagram{bytes,source}`、`OperationOptions`。
- 通信方式：协程 await 式拉模型；`Read` 一次一片；`Write` 成功表示帧已交付下层发送通路（**非**"已进内核"、**非**"对端已收"，ADR-0004 D5）。
- 协议特征：`Start/Read(options)/Write(SendUnit)/RequestClose/WaitClosed` + 强制 I/O 观测面 `LastSendTime/LastReceiveTime/LastError/CurrentLinkState`；同实例同一时刻至多一个有效读。
- **读取终止语义（DD-11）**：`Read` 失败中仅 `kClosed`=传输终结（应停止读），其余为可继续的瞬时错误；可重连传输透明跨越链路中断，不暴露断链事件。**所有介质同形**，交互层无需知道哪种介质会重连。
- **链路可用性（DD-7）**：`CurrentLinkState()` 返回 `LinkState`，为所有介质同形的当前 I/O 事实；连接管理策略不经本接口暴露。曾经的可选观察面接口 `IConnectionObservable` 已取消，其头文件 `io/IConnectionObservable.hpp` 已删除（#111）；`ConnectionState` 枚举随之迁至 `io/tcp/TcpClientTransport.hpp`（命名空间不变，仍为 `transport::ConnectionState`）。

#### 4.3.6 DDS provider 抽象（JK_PROVIDER）
- 优先级：中。
- 接口类型：C++ 抽象类 `IDdsProvider`（`io/dds/IDdsProvider.hpp`）。
- 数据元素：topic 名、不透明字节、订阅回调。
- 通信方式：`Subscribe` 回调在 provider listener 线程触发（跨线程）。
- 协议特征：`Init/Shutdown/Subscribe/Unsubscribe/Publish`；实现 `FakeDdsProvider`（进程内总线）、`FastDdsProvider`（可选）。

## 5. CSCI 详细设计

各软件单元（CSU）对应 §4.1 部件，细度以"可据此直接编码"为准；结构性类图见 §4.1，本章聚焦逻辑与算法。每个 CSU 给出：单元设计决策 / 设计约束 / 软件逻辑 / 执行时序·数据流。

### 5.1 核心原语详细设计（CSU_CORE）

**单元设计决策**：`Result`/`Status` 直接复用 `Coro::Result<T,error_code>`（避免两套 expected 类型）；错误用 `TransportErrc` 机器可判别类别，不解析字符串（DD-5）；`SharedCompletion` 提供多等待者一次性完成（原子首胜 Complete），供多方共享收敛通知。

**设计约束**：不抛异常表达预期失败；并发数据受锁/原子保护；观测原语未配 sink 时仅一次判空（RT_TRACE_002）。

**软件逻辑**：
- **Result/Status/TransportErrc**：`Result<T>=Coro::Result<T,error_code>`，`Status=Result<void>`；`TransportErrc` 十三类（kInvalidArgument/kInvalidState/kConfiguration/kConnection/kClosed/kTimeout/kCancelled/kIo/kFrame/kCodec/kResourceExhausted/kUnsupported/kInternal）经 `make_error_code` 归入 `transport_error_category()`。
- **Message**：`payload` + 两套元数据——通用交互（kind/correlation_id/reply_to，DDS 路径）与外部协议（frm_type/protocol_id/session_id/message_id，SystemCodec 路径）；`source`/`topic` 由 node 收到时按来源填。
- **Endpoint**：`kDefault`（config 默认目的地）/`kNet(ip,port)`/`kTopic(name)`，经 `SendUnit.destination`/`Datagram.source` 统一寻址。
- **Cancellation**：`CancellationSource.token()` 派发 `CancellationToken`；`Register(cb)` 返回 `CancellationRegistration`（RAII，`Reset` 解注册）；`Cancel` 触发全部回调。
- **SharedCompletion<T>**：共享 `State{mutex; StoredResult completion; map<id, weak_ptr<Awaitable>>}`；`Complete` 锁内首胜置结果并摘全部 waiter、锁外 `resolve+close`；`Wait` 已完成即返，否则登记 waiter `await`/`await_for`，超时置 kTimeout、取消置 kCancelled。**仅供多等待者 void 事件**（NodeRuntime/各传输 closed/TcpServer），PendingTable 不再用它。
- **观测**：`RecordDrop(reason, counter, sink)`（计数 pull + 可选 Trace push，持锁调用）；`RecordEvent(category, sink, ...)`（仅 push）；`DropReason` **六项**（原七项，`kGenerationIsolationDrop` 随 ADR-0004 D3 撤销代际隔离而移除）；`TraceCategories` 九类常量单一权威。

**执行时序/数据流**：见 §4.2.2（D1/D2/D3 一致性保护）；SharedCompletion 参与 §4.2.5 关闭汇合。

### 5.2 挂起-应答详细设计（CSU_PENDINGTABLE）

**单元设计决策（DD-8）**：entry 单等待者 → 信箱退化为裸 `Coro::Awaitable<T>`（底层一条 FiberChannel），**唯一仲裁点 = 表锁 `find+erase` 抢占终结权**；去掉此前 `SharedCompletion<T>` 的多等待者层（未使用），对齐 AsyncTask《使用说明》§6.3 一次性等待范式。

**设计约束**：恰好一次终结（RT_REQUEST_003）；`Resolve` 锁外投递（唤醒不在表临界区，D8）；`FailAll(latch=false)` 断连语义不 latch、表继续可用（RT_TCP_RECONNECT_002）；`T` 须可默认构造且可拷贝。

**软件逻辑**：见 `node/PendingTable.hpp`。`Entry{shared_ptr<Awaitable<T>> mailbox; time_point registered_at}`；`Shared{mutex; map<Key,shared_ptr<Entry>>; closed; sink; last_request_latency}`。
- `Register(key)`：锁内查 closed/max_pending/重复键，建 entry（信箱自动创建）入 map，返回 Handle。
- `Resolve(key,value)`：锁内 `find+erase` 抢占（唯一仲裁）→ 锁外 `mailbox->resolve(value)`（push）+ `close()`；未找到返 false（迟到/重复/无匹配 → 归因 kUnmatchedOrLateResponse）。
- `FailAll(error,latch)`：锁内摘除全部（可选 latch closed）→ 锁外逐个 `mailbox->close(error)`。
- `Handle::Wait(options)`：`await`/`await_for` 信箱——收到值即 Resolve 抢先；本地超时经 `ClaimTerminal` 抢占，抢到即 kTimeout，抢输则 `await` 一次 drain 出对方结果；close_error 分支采 cancel/FailAll 的 error。终结点记时延 + 按结果分类 Trace（match/timeout/cancel）。取消经 `Register([]{ ClaimTerminal 后 close(kCancelled) })`。

**执行时序/状态**：见 §4.2.4（请求-响应时序）、§4.2.9（entry 抢占仲裁状态）。

### 5.3 有界业务队列详细设计（CSU_BOUNDEDQUEUE）

**单元设计决策**：协议无关（对 T 不透明，字节计量注入 `byte_size_of`）；双上界 tail-drop（丢正到达元素，已入队不受影响）；`drop_reason`/`sink` 构造注入。

**设计约束**：`Push` 不阻塞；`RecordDrop` 在 tail-drop 分支持锁调用（与原地 `++dropped` 同临界区）；供 DdsTransport（`kDdsHandoffOverflow`）与 NodeRuntime（`kBusinessQueueOverflow`）复用。

**软件逻辑**：见 `node/BoundedQueue.hpp`。`Push`（满则 `RecordDrop`+`kResourceExhausted`，否则入队唤醒一个消费者）；`Pop`（空则消费者协作 await；Close→kClosed、deadline→kTimeout、取消→kCancelled）；`Close`（latch，唤醒在途消费者）；`Drain`（取尽剩余供 close_drop 归因）。上界 `max_events`[1,65536]/`max_bytes`[64KiB,256MiB]，越界钳制。

**执行时序/数据流**：见 §4.2.2（D2）、§4.2.5（Close 时 Drain 归因 close_drop）。

### 5.4 节点运行时详细设计（CSU_NODERUNTIME）

**单元设计决策（DD-3）**：把多节点共享的协议无关机制收成可组合薄件——①生命周期状态机 + 并发幂等 Start + 三方汇合 + 重入自锁防护；②handler 消费者 fiber + `BoundedQueue` 集成 + 异常隔离 + close_drop 归因；③读-分发循环骨架 `SpawnReadLoop(fn)`。对 Event 不透明。

**设计约束**：同步纪律（D8）——生命周期状态/计数/`has_handler_` 均入锁，唤醒/回调锁外；重入自锁防护比对 `boost::this_fiber::get_id()`；handler 逃逸异常为运行时唯一授权 catch（转 kInternal 隔离，不自关）。

**软件逻辑**：见 `node/NodeRuntime.hpp`。
- **Start(validate, bring_up)**：首个 Start 校验 config → 置 starting → 调 node `bring_up`（transport.Start + MarkRunning + SpawnReadLoop/HandlerLoop + reactor）；并发 Start await 同一 `start_done_`。
- **Close(signal_node_convergence)**：首个关闭者 Running→Closing → 三方汇合信号（transport.RequestClose + 业务队列 Close + handler 取消 + node 侧收敛回调）→ spawn finalizer fiber：等 `loop_done_` → 依序等追加 join（reactor）→ 等 `handler_done_`（超 500ms 记 kInternal 不强杀）→ `ConvergeToClosed()`（Drain close_drop + 置 Closed + 记时延）→ `closed_.Complete`。重入（当前即 handler fiber）只发起不自等。
- **SpawnReadLoop(fn)**：`Read → 错误分类（kClosed/kConnection 退出，其它继续）→ 调 node 的 decode+dispatch`。
- **SpawnHandlerLoop(consume)**：`Pop → consume 到完成（含 await）→ 下一条`（严格串行）；逃逸异常记 handler_exception。

**执行时序/状态**：见 §4.2.5（三方汇合）、§4.2.7（生命周期状态）。

### 5.5 交互节点详细设计（CSU_PROTOCOLNODE / CSU_DDSNODE）

**单元设计决策（DD-3/DD-4）**：组合 `ITransport`+`ICodec`+`PendingTable`+`NodeRuntime`，协议特有语义全内联本类；DdsNode 复用同套基座仅换 Key 类型（D10 实证）。

**设计约束**：`Request/Send/Publish` 仅 Running 放行（否则 kClosed）；session_id 空间协议特有（uint8=256）内联 ProtocolNode；correlation_id 确定性（node_id:序号）内联 DdsNode。

**软件逻辑（CSU_PROTOCOLNODE）**：见 `node/ProtocolNode.cpp`。
- **键派生**：`CorrelationKeyStrategy`（可注入）；默认请求键 `(session_id<<16)|message_id`，响应键清 `0x1000` 标记位归一化。
- **session_id**：空闲集 `deque<uint8>`（0..255），Request `pop_front` 取最久释放者（FIFO 退休窗口 RT_REQUEST_005），`SessionLease` RAII 归还；256 全在途 `kResourceExhausted`。Send 只读空闲集尾部盖帧、不出队（不扰 FIFO、不占预算）。
- **Dispatch**：`kResponse`/`kResult`=响应帧→`Resolve`（未命中归因 kUnmatchedOrLateResponse）；否则业务帧入队/丢弃（无 handler 归因 kNoHandlerConfigured）。
- **链路断开处置（DD-11/DD-12，取代原 reactor）**：**交互层不参与**——重连由传输内部透明完成，读循环无断链分支。不批量终结在途请求、不清空排队业务、**无 reactor 协程、无能力探测**——三介质同一段读循环（仅区分 `kClosed` 与其余）。

**软件逻辑（CSU_DDSNODE）**：见 `node/DdsNode.cpp`。correlation_id 生成、`kReply` 终结判别、topic 寻址、`reply_to=inbox`；`Request(Message,target)` 盖 kRequest + Register(correlation_id) + WriteFramed；`Publish` 盖 kNotify fire-and-forget；`DdsHandlerContext::Reply` 对入站 kRequest 回送 kReply。无连接（D3′），无 reactor/重连。

**执行时序/数据流**：见 §4.2.3/§4.2.4/§4.2.6。

### 5.6 传输层详细设计（CSU_IO）

**单元设计决策**：各介质实现**唯一**的 `ITransport` 契约（含链路可用性，DD-7）；连接管理（TCP 客户端）与纯管道分离并**维持两层**（ADR-0004 D8：合并只会复制收发语义）；TCP 客户端内部改为**连接泵 + 对外通道**（ADR-0004 D6）；DDS 跨线程有界交接闭合 ADR-0001 未决项。

**设计约束**：并发写串行化保留（RT_TRANSPORT_004）、**发送完成语义与背压已撤销**（DD-6）；UDP/DDS 单次一报文/样本，过大发送前失败；**读取终止语义**（DD-11）：不可重连介质致命错误返 `kClosed`，可重连介质链路中断**对调用方透明**（`Read` 挂起至新链路就绪，不返回任何断链错误）；socket/串口在节点执行域 fiber 内创建（亲和纪律）。

**软件逻辑**：见 `src/io/*`。
| 单元 | 关键逻辑 |
|---|---|
| TcpTransport | 接管已连接 socket；复用 corosocket `readAll` 流；写路径**并发写按到达序串行化**（不再等字节进内核）；对端断开 → `Read` 返 `kClosed`（不可重连，DD-11） |
| TcpClientTransport | connect-loop fiber 持 socket 跑状态机 `Connecting/Connected/Reconnecting`，建连后以流式读取器持续取数投入**对外通道**；`Read` 从该通道取；断链**不发信号**，connect-loop 转入重连、`Read` 自然挂起至新链路数据到达（完全透明，DD-11）。`Write` **直操当前 socket**（重连期立即返 `kConnection`，不缓存——RT_TCP_RECONNECT_003），不经通道。`abort()+deleteLater()` 管超时；**固定重连间隔 1s**（ADR-0005）；`ApplyConfig`/`Generation()`/`AttemptCount()` 等降级为**具体方法**（ADR-0004 D7）。**已知缺口**：对外通道无界（见 §7.2） |
| UdpTransport | coroudpsocket 报文收发；`Write` 寻址 kDefault→config 默认 / kNet→ip:port；`writeDatagram` 同步原子发送；底层致命 → `Read` 返 `kClosed` |
| SerialTransport | coroiodevice 字节流；设备断开/致命 → `Read` 返 `kClosed`（不重连，TBD-005） |
| DdsTransport | 组合 IDdsProvider + `BoundedQueue<Sample>` 跨线程交接；listener 线程非阻塞 Push（满归因 kDdsHandoffOverflow）；`Read` 出队 fiber |
| TcpServer | corotcpserver accept 循环 fiber；每连接经 NodeFactory 派生 ProtocolNode + supervisor fiber |

**执行时序/状态**：见 §4.2.8（连接状态机）、§4.2.10（动态生命周期）。

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
| DD-1..DD-12 | 设计决策 | 见各决策标注（DD-11/12 由 ADR-0004 引入） | §3 |
| CSC_CORE | 部件 | RT_ERROR、RT_DATA_MESSAGE、RT_TRACE、RT_DESIGN_005 | §4.1.1、§5.1 |
| CSC_IO | 部件 | RT_TRANSPORT、RT_TCP_RECONNECT/RECONFIG、RT_IF_*、RT_IN_INTERFACE_002/003 | §4.1.2、§5.6 |
| CSC_CODEC | 部件 | RT_CODEC、RT_IF_SYSFRAME | §4.1.3、§5.7 |
| CSC_NODE | 部件 | RT_NODE、RT_REQUEST、RT_HANDLER、RT_LIFECYCLE、RT_DESIGN_003/008 | §4.1.4、§5.2–5.5 |
| MS_DFD_CONTEXT / MS_DFD_TOPLEVEL | 执行方案 | RT_IN_INTERFACE_001、RT_TRANSPORT、RT_REQUEST | §4.2.1、§4.2.2 |
| MS_NODE_DATAFLOW / MS_REQ_RESP | 执行方案 | RT_REQUEST、RT_NODE_003 | §4.2.3、§4.2.4 |
| MS_CLOSE / MS_NODE_LIFECYCLE | 执行方案 | RT_LIFECYCLE_001/003–007 | §4.2.5、§4.2.7 |
| MS_LINK_DOWN / MS_CONNECTION | 执行方案 | RT_TCP_RECONNECT、RT_TRANSPORT_008、RT_LIFECYCLE_002 | §4.2.6、§4.2.8 |
| MS_PENDING | 执行方案 | RT_REQUEST_003/004 | §4.2.9 |
| MS_DYNAMIC_LIFECYCLE | 执行方案 | RT_CORO_RUNTIME、RT_NODE_004、RT_DESIGN_004 | §4.2.10 |
| JK_NODE_API | 接口 | RT_IF_API | §4.3.2 |
| JK_CODEC | 接口 | RT_CODEC、RT_IF_SYSFRAME、RT_DESIGN_006 | §4.3.3 |
| JK_TRACE | 接口 | RT_TRACE_001/002 | §4.3.4 |
| JK_TRANSPORT | 接口 | RT_IN_INTERFACE_002、RT_TRANSPORT_008/009 | §4.3.5 |
| JK_PROVIDER | 接口 | RT_IN_INTERFACE_003、RT_IF_DDS | §4.3.6 |
| CSU_CORE | 详细设计 | RT_ERROR、RT_DATA_MESSAGE、RT_TRACE、RT_CORO_RUNTIME_005 | §5.1 |
| CSU_PENDINGTABLE | 详细设计 | RT_REQUEST_001..005、RT_IN_INTERFACE_004、RT_DESIGN_008 | §5.2 |
| CSU_BOUNDEDQUEUE | 详细设计 | RT_HANDLER_004、RT_DATA_BUFFER、RT_DESIGN_008 | §5.3 |
| CSU_NODERUNTIME | 详细设计 | RT_LIFECYCLE、RT_HANDLER、RT_NODE_003、RT_DESIGN_008 | §5.4 |
| CSU_PROTOCOLNODE | 详细设计 | RT_REQUEST、RT_NODE_003、RT_HANDLER、RT_TCP_RECONNECT | §5.5 |
| CSU_DDSNODE | 详细设计 | RT_NODE_004/005/007、RT_IF_DDS、RT_REQUEST | §5.5 |
| CSU_IO | 详细设计 | RT_TRANSPORT、RT_TCP_RECONNECT/RECONFIG、RT_NODE_006、RT_IF_* | §5.6 |
| CSU_CODEC | 详细设计 | RT_CODEC、RT_IF_SYSFRAME | §5.7 |

### 6.2 需求 → 设计单元

| SRS 需求 | 对应设计单元 |
|---|---|
| RT_CORO_RUNTIME_001..005 | DD-2 / CSC_CORE、CSC_NODE / CSU_NODERUNTIME / MS_DYNAMIC_LIFECYCLE |
| RT_TRANSPORT_001..009 | DD-6、DD-11 / CSC_IO / CSU_IO / JK_TRANSPORT / MS_DFD_TOPLEVEL |
| RT_CODEC_001..006 | CSC_CODEC / CSU_CODEC / JK_CODEC |
| RT_REQUEST_001..006 | DD-8 / CSC_NODE / CSU_PENDINGTABLE、CSU_PROTOCOLNODE / MS_REQ_RESP、MS_PENDING |
| RT_HANDLER_001..006 | CSC_NODE / CSU_NODERUNTIME、CSU_BOUNDEDQUEUE / MS_NODE_DATAFLOW |
| RT_LIFECYCLE_001..007 | DD-7 / CSU_NODERUNTIME / JK_TRANSPORT / MS_CLOSE、MS_NODE_LIFECYCLE、MS_CONNECTION |
| RT_NODE_001..007 | DD-3 / CSC_NODE / CSU_PROTOCOLNODE、CSU_DDSNODE / MS_NODE_DATAFLOW |
| RT_TCP_RECONNECT_001..005 | DD-11、DD-12 / CSU_IO、CSU_PROTOCOLNODE / JK_TRANSPORT / MS_LINK_DOWN、MS_CONNECTION |
| RT_TCP_RECONFIG_001..006 | CSU_IO（TcpClientTransport::ApplyConfig） |
| RT_ERROR_001..003 | DD-5 / CSC_CORE / CSU_CORE |
| RT_TRACE_001/002 | DD-10 / CSC_CORE / CSU_CORE / JK_TRACE |
| RT_DATA_MESSAGE/STATE/CONFIG/BUFFER | CSC_CORE、CSC_NODE / CSU_CORE、CSU_BOUNDEDQUEUE、CSU_PENDINGTABLE |
| RT_IN_INTERFACE_001..005 | DD-1 / JK_TRANSPORT、JK_CODEC、JK_PROVIDER、JK_NODE_API |
| RT_IF_API/SYSFRAME/TCP/UDP/SERIAL/DDS | JK_NODE_API、JK_CODEC、JK_TRANSPORT / CSU_IO、CSU_CODEC |
| RT_DESIGN_001..008 | §3 CSCI 级设计决策 DD-1..DD-12 / 全体单元 |
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
| 四方仲裁 | Resolve/超时/取消/FailAll 经表锁 find+erase 抢占终结权 |
| loss=0 harness | 断言"无静默丢失"的测试：Σ命名计数 = 总丢弃 |

### 7.2 未决项与技术债（详见 REF-3 §6、REF-2）

五模式精确状态机与 `kFeedback`（TBD-001）；帧常量/CRC 真值（TBD-003）；性能基线固化（TBD-004，P6）；串口自动重连（TBD-005）。测试技术债：CGNAT 连接超时 flake、`FakeDdsProvider` 静态总线 repeat artifact。延后重构：node getter 聚合 `NodeStats`（等 P6 指标定型）。

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
| 图 4-9 | 时序 | MS_LINK_DOWN | `seq-generation-isolation.mmd` |
| 图 4-10 | 状态 | MS_NODE_LIFECYCLE | `state-node-lifecycle.mmd` |
| 图 4-11 | 状态 | MS_CONNECTION | `state-connection.mmd` |
| 图 4-12 | 状态 | MS_PENDING | `state-pending-entry.mmd` |
| 附图 | 类图 | 总体 | `arch-class.mmd` |

---

*本文件基线：master（v0.4.5+）。与 SRS 冲突时以 SRS 优先；路线图以 REF-3 为准。ID 体系对齐 REF-4 AsyncTask SDD。*
