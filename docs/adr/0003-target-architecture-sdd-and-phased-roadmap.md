# ADR-0003：目标架构 SDD 与分期路线图（清洁重建）

**状态：** Accepted（草案 / 目标架构，实现随路线图推进）
**日期：** 2026-07-27
**关联：** `docs/设计说明书-协程原生.md`（本 ADR 确立的 SDD + 路线图）；`docs/需求规格说明书-协程原生.md`（SRS v3，需求基线）；ADR-0001（协程原生架构总纲）、ADR-0002（发送完成/丢弃归因/生命周期细化）；as-built 存档于 tag `v0.3.0`。

## 背景（Context）

此前按"每次针对一个点展开 spec 再实现"推进（#19 发送语义已完成），缺**整体设计**与**分期规划**：组件如何分解、缝在哪、node 如何组合没有单一权威源，也没有"先做什么后做什么"的依赖顺序。本 ADR 记录一轮 grilling 形成的顶层决策，据此产出目标架构 SDD + 路线图，供 `/to-tickets` 逐期拆解、逐步实现 SRS 全部需求。

## 决策（Decision）

- **D1（文档形态）：** 新建单文档 `docs/设计说明书-协程原生.md` 兼任**目标架构 SDD + 分期路线图**，作为设计单一权威源（SRS 4.2 把设计留给 SDD，而协程原生 SDD 此前不存在）。否决 epic issue（设计细节不宜塞 issue body）与纯排期文档（缺整体设计）。

- **D2（清洁重建，不写迁移）：** 本 SDD 描述**目标**，不含从 as-built 的迁移关系。as-built 旧架构（`comm/` 引擎/执行器/策略、第二期 `coro/` 引擎、回调式传输、旧文档/示例）**从 master 删除**，已存档于 tag `v0.3.0`。理由：迁移包袱会污染目标设计的表达；旧代码有 tag 留档即可恢复。

- **D3（纵向薄切片优先）：** 路线图按**纵向薄切片**推进，而非横向逐层。首切片打通 TCP 上最小 request-response（TcpTransport→codec→内联 PendingTable→最小 ProtocolNode），**最早证实"无共享引擎、语义内联各 node"这一最大架构赌注**（RT_DESIGN_003 / ADR-0001 D1-D2）。否决横向（所有传输→所有 codec→请求→节点，端到端最晚、组合风险后置）。

- **D4（最大化复用）：** 抢救**与交互架构无关的纯逻辑**（codec 线缆逻辑：帧布局/流式扫描/重同步/`CrcFn`/报文；DDS provider 适配：`IDdsProvider`/Fake/FastDDS/`FastDdsRawType`/registry），并**改造沿用** `Message`/`ICodec`/`ITraceSink`（对齐目标与 coro，不重设计）；`Endpoint` 原样保留。只删被 RT_DESIGN_002/003 否决的架构外壳。**例外**：as-built 顶层 `Result.hpp` 用 `config:`/`conn:` 字符串前缀分类错误，被 RT_ERROR_002 禁止，改用 coro 的 `TransportErrc` 机器可判别模型。

- **D5（命名空间提升）：** as-built 删除后 `coro` 子层失去对照物，P0 将目标提升到 `transport::` 顶层（`transport::ProtocolNode`/`TcpTransport`/`ITransport`/`TransportErrc`），codec→`transport::codec`、provider→`transport::dds`；`include/transport/coro/*`→`include/transport/*`。CMake 由 `transport`+`transport_coro` 合并为单一 `transport`。

- **D6（分期路线图）：** P0 清理+骨架 → P1 TCP 最小请求-响应（证实架构）→ P2 节点加厚（处理器/有界队列/生命周期）→ P3 连接管理（TcpClientTransport，#20）→ P4 其余介质（UDP/串口/DDS + provider 交接）→ P5 观测+完整性归因（D6 命名丢弃计数器 + loss=0 验证）→ P6 性能+硬化+两机验收。P3 在 P4 之前（先把 TCP 加固到生产级再铺介质）。

- **D7（每期验收门槛）：** 功能期（P1–P4）= Fake 确定性契约 + **单机真实介质回环** + 每条原子 RT_* 追溯到测试；两机集成/性能/24h 稳定性集中在 P6；命名丢弃计数器各期就地埋、loss=0 harness 在 P5。沿用 #19 的"Fake 契约 + 真实回环双实现"范式。

- **D13（P5 观测 + 完整性归因:双面观测 + 结构性丢弃归因 + I/O 观测面统一）：**
  - **观测模型(Q1)**:双面——**push**(复用既有 `ITraceSink`,接进各层发结构化事件)+ **pull**(既有具名计数 getter 快照)。完整性("Σ命名原因 = 总丢弃"、"无静默丢失")靠**结构性纪律**保证,不靠事后核对。
  - **`DropReason` taxonomy(Q2)**:七项枚举——`kBusinessQueueOverflow`/`kDdsHandoffOverflow`/`kBadFrame`(**新增埋点**,P1 起坏帧路径零计数)/`kUnmatchedOrLateResponse`(迟到/重复/无匹配合并,对齐 CONTEXT.md"丢弃归因"原文用"/"并列而非三个独立项)/`kCloseDrop`/`kGenerationIsolationDrop`/`kNoHandlerConfigured`(**新增术语**,CONTEXT.md 原六项之外,补业务帧无处理器场景)。`HandlerExceptionCount`(处理器执行失败)**不进丢弃口径**——RT_HANDLER_006 是隔离当前事件语义,不是帧/响应未投递。
  - **chokepoint 落地(Q3)**:不建全局单例函数(会强行缝死 `io/`/`node/` 分层、违反 D10/D12"不造上帝对象"纪律)。落成**纪律**:每个 `DropReason` 恰好一个归属组件 + 一个定义时刻(可审计表:`kBusinessQueueOverflow`→`BoundedQueue.Push`满;`kDdsHandoffOverflow`→`DdsTransport` listener 交接满;`kBadFrame`→各 node 读循环 `codec.Decode` 失败;`kUnmatchedOrLateResponse`→各 node `Dispatch` 的 `PendingTable.Resolve`→false;`kCloseDrop`→`NodeRuntime` 关闭 drain;`kGenerationIsolationDrop`→`ProtocolNode` reactor 断连收敛;`kNoHandlerConfigured`→各 node `Dispatch` 未设 handler)。小型可复用原语 `RecordDrop(reason, counter, sink)`(`core/Observability.hpp`)在定义时刻一次调用完成计数(pull)+ 可选 `OnTrace`(push),既有具名 getter 保留、内部改接该原语,API 不破坏。姊妹原语 `RecordEvent(category, ..., sink)` 服务非丢弃类 Trace(Q5),仅 push 不计数。loss=0 harness **直接求和既有具名 getter**,不为此建通用指标聚合抽象(过度泛化陷阱,同 D10 教训)。
  - **I/O 观测面统一(Q4,遗留项 B)**:`LastSendTime`/`LastReceiveTime`/`LastError` 提升为 base `ITransport` **强制虚方法**——ADR-0002 D3 原文"I/O 事实…所有介质如实报,RT_NODE_006"是强制要求,不同于 `IConnectionObservable`(D3′ 已定案 TCP 客户端专属、可选接口)。四个介质(TCP/TcpClient/UDP/Serial)已天然长同一 duck-typed 形状,只需收编进 base 接口;`DdsTransport` 是真实缺口(此前零覆盖,仅有丢弃计数)需新写;测试替身 `FakeCoroTransport` 同步补齐。`SendWaiterDepth` **不进** base 接口——只有字节流+背压语义的介质(TCP/TcpClient/Serial)有意义,UDP/DDS 是原子单操作、无背压概念(D12 已定案),留各类附加方法。
  - **Trace 类别 + 补齐指标(Q5)**:九类(`connect`/`generation`/`send`/`recv`/`decode`/`match`/`timeout`·`cancel`/`handler`/`reconnect`/`lifecycle`;生命周期类别原名 `close`,P6 前置清理 #98 改名——"running" 挂在 "close" 下是命名误导;常量集中 `core/TraceCategories.hpp`),对齐 RT_TRACE_001 覆盖面;仅在**状态跃迁/结果确定边界**触发,不逐字节、不逐 fiber 调度事件。顺带补齐 RT_DATA_BUFFER 此前零覆盖的四项 pull 指标:请求时延(`PendingTable::Handle::Wait` 终结时记)、处理器时长(`NodeRuntime` handler 调用起止差)、重连次数(`TcpClientTransport` 已有 `AttemptCount()` 只是未 Trace)、关闭时延(`NodeRuntime`/`TcpClientTransport` Close 发起到 Closed 完成)。
  - **loss=0 harness(Q6)**:**harness = 新测试文件,非新运行时组件**——多数验证已被 P1–P4 各票的既有测试顺手覆盖,P5 只需(a)重命名后既有测试仍过、(b)补 `kBadFrame`/`kNoHandlerConfigured` 对应测试、(c)新增 `tests/loss_accounting_test.cpp` 做两类此前没人做过的验证:**干净跑**(容量内无外部故障 → 全部 `DropReason` 计数器全程为 0)+ **混合故障**(同一运行同时触发多种丢弃 → Σ(各 reason 计数)= 测试自造的总丢弃事件数,ground truth 由测试代码持有而非被测系统自证)。每个 reason 用**能触发它的最小介质**验证(局部性质,不需全介质同跑)。不建生产聚合查询 API(宿主要总览自己求和既有 getter)。`CapturingTraceSink` 验证 push/pull 一致 + 未配置 sink 零影响(RT_TRACE_002)。
  - **切片**:P5-1(`DropReason`+原语,prefactor)∥ P5-2(I/O 观测面统一,独立)→ P5-3(接入丢弃归因)∥ P5-4(接入 Trace+补齐指标)→ P5-5(loss=0 harness,依赖 P5-3+P5-4)。

- **D12（P4 其余介质:DDS 交接边界 + NodeRuntime 收口 + 统一寻址）：**
  - **DDS provider 交接边界(Q1)**:provider `Subscribe` 回调在 listener 线程(非 fiber)触发;交接边界**复用 `BoundedQueue<Sample>`**(P2 协议无关件)——listener 线程**非阻塞 `Push`**,满 **tail-drop 丢最新** + 命名计数 `dds_handoff_overflow`(RT_NODE_007/ADR-0002 D4,默认 1024 样本/16 MiB 可配),FIFO 保同 topic 接受顺序(RT_NODE_005);`DdsTransport::Read` = Pop 交接队列(空则 await),`Write` = `provider.Publish(destination.topic, bytes)`,`Datagram.source = kTopic`。**跨线程唤醒**靠 AsyncTask `Awaitable` 的 boost.fiber channel 跨线程安全性(`awaitable.hpp` 注"跨线程安全,由 FiberChannel 保证")——**闭合 ADR-0001 未决项"裸 boost fiber channel 从外线程 push 是否安全"= 安全**;`BoundedQueue::Push` 从非 fiber 线程调用需实现期确证,**不成立则查根因修复**(而非退回 Qt QObject 桥,除非根因不可修)。`Unsubscribe` 先停投递、交接队列 `Close`、迟到样本丢弃防碰已销毁对象;本地已丢样本不得宣称 DDS Reliable 自动恢复。
  - **统一寻址(Q1 #3)**:`ITransport::Write(SendUnit)` 每条消息经 `SendUnit.destination`(`Endpoint`)寻址,Read 经 `Datagram.source` 对称——**同一接口,不同介质取 Endpoint 不同 kind**:TCP 连接忽略、UDP `kNet(ip,port)` 发往不同地址、DDS `kTopic(name)` 发到不同 topic。DdsTransport 订阅一组 topic,交接元素 `Sample{bytes, topic}` 带来源 topic。**无需新增寻址参数**。
  - **NodeRuntime 收口(Q2,D10 兑现)**:DdsNode 是第二个 node,现按 D10"P4 需求具体后收口共享面"——抽出 ProtocolNode 与 DdsNode 共享的**协议无关机制**(生命周期状态机+三方汇合、handler 消费者 fiber+有界队列集成、读-分发循环骨架)成可组合 `NodeRuntime`,两 node **组合并驱动**(node 调 `runtime.enqueue`/`transition`,非 runtime 回调 policy,守 RT_NODE_003);**只抽干净可无关化部分,不造 NodeRuntime 上帝对象**,漏协议特有语义进 helper 即退回内联。ProtocolNode 重构为组合 NodeRuntime、行为不变(P1–P3 绿)。
  - **DdsNode 交互模型(Q2)**:**无连接**(无连接状态机/reactor/重连,底层致命→`Closing→Closed` D3′,判活归协议层 Liveliness/Deadline QoS 或心跳超时 RT_NODE_006);关联复用 `PendingTable<std::string, Message>`(correlation_id 字符串键,**D10 复用实证,基座一行不改**),`reply_to` = node 的 inbox topic,终结判别 `kReply` 内联 DdsNode;订阅 topic 集 = `DdsConfig` 静态(业务 topic + inbox),动态 Subscribe 留后续;P4 模式 = pub-sub(`kNotify`)+ 多路请求-应答(`kRequest`/`kReply`),`kFeedback` 延后。
  - **UDP/串口(Q3/Q4)**:`UdpTransport`(coroudpsocket receiveDatagram/writeDatagram,报文+地址,from 可变)/`SerialTransport`(coroiodevice 字节流切片,SerialConfig);均**非重连**(D3′,只暴露 I/O 事实无连接状态);串口断开**过渡默认致命**(TBD-005,不自动重连)。
  - **codec(Q5)**:`DatagramCodec`/`SystemDatagramCodec`/`DdsCodec` P0 已改造到位且有测试(报文式每次 Decode 一完整边界、不跨报文缓冲,RT_CODEC_003);P4 只装配,transport↔codec 由应用装配。
  - **TCP 服务端 accept(Q6)**:`TcpServer` 包 `corotcpserver.nextConnection`,每接受连接派生 `ProtocolNode`(内层 `TcpTransport` 已连接,**非重连** D3′,连接生命=节点生命 RT_DESIGN_004),server owns 子 node 列表 + 清理;监听/接受失败/连接关闭可观测(RT_IF_TCP)。
  - **切片**:P4-1 NodeRuntime(prefactor)→(P4-2 UDP ∥ P4-3 串口 ∥ P4-4 DdsTransport ∥ P4-6 accept)→ P4-5 DdsNode(需 P4-1+P4-4)。

- **D11（P3 连接管理行为契约 —— 承接 seed #20 分层,细化重连期语义）：** seed #20 已定分层(`TcpClientTransport` 外层 owns socket + 组合 P1 内层一代际一实例;base `ITransport` 纯字节管道;健康三层),本轮 grill 细化未决行为:
  - **重连期三条路径分治(Q1)**:`outer.Read()` **透明跨重连**(断连/重连期阻塞等下一代际连上再返新代际字节,node 读循环永不因 TCP 客户端断连而退出,只 Close 时返 `Closed`);`outer.Write()` 非 Connected 态**立即返 `Connection`**不缓存(RT_TCP_RECONNECT_003);**in-flight 请求终结不靠 Read/Write,靠连接状态观察器**——node 订阅状态,遇 `Disconnected` 调 `PendingTable.FailAll(kConnection)`(RT_TCP_RECONNECT_002)。node "观察连接状态但不管理 churn"。
  - **观察面(Q2)**:可选接口 `IConnectionObservable`(`State`/`WaitForState`/`WaitStateChange` + 代际/配置版本/最近失败/尝试次数/下次尝试时间诊断 getter),`TcpClientTransport` 实现 `ITransport`+`IConnectionObservable`,**不进 base ITransport**(D3′);node 检测该接口 → spawn reactor fiber(第 4 条 node fiber);**拉模型非回调**(契合 fiber + RT_HANDLER_002)。
  - **代际隔离(Q3)**:连接代际(单调 uint64,每次成功物理连接 +1,TcpClientTransport 持有)**不进 PendingTable**——靠"FailAll 清空 ⇒ 在途恒属当前代际"**不变式**满足 RT_DATA_STATE,不把连接概念泄漏进协议无关基座(守 RT_DESIGN_008)。迟到隔离 = 新 socket 物理隔离 + 管理层以代际身份过滤 stray 旧 socket 回调 + FailAll。**断连 = `Running` 内代际重置**(FailAll + Drain 旧代际未启动业务计 `连接代际隔离丢弃` + 让运行中 handler 跑完 + node 不终),区别于 Close 的 `Closing→Closed` 终态。
  - **连接尝试(Q4)**:`Coro::coro(sock).connectToHost` + `await_for(可配 connect_timeout,默认 5s 范围 100ms–60s)`;`await_for` 超时不取消底层 → 显式 `sock->abort()+deleteLater`(corosocket 摩擦 1)。退避 1s×2 上限 30s ±20% jitter,稳定 60s 重置,无限重试;内部不变量破坏→node 安全关闭。**socket 在 connect-loop fiber(节点执行域线程)内创建**(亲和纪律);corosocket 已把 socket action marshal 到其所属线程(`QMetaObject::invokeMethod` QueuedConnection,同线程走快路径),但跨线程 fiber 唤醒未免费(同 D4),P3 单调度器全同线程,多线程亲和留 M:N(RT_IN_INTERFACE_005)。
  - **运行时重配置(Q5)**:`ApplyConfig(TcpClientConfig, version)` 宿主直接对 TcpClientTransport 提交(不经 node);单调版本(过期/乱序拒绝,同版同容 no-op)+ 先校验后原子应用(非法→旧配置旧连接不变);端点(host/port)变化→旧在途以 `Connection` 终结 + 新代际立即尝试(不等退避),仅策略参数变化→当前尝试/退避用旧、下次用新;配置版本与连接代际两轴独立(RT_DATA_STATE);热更新范围仅 host/port/超时/退避(RT_TCP_RECONFIG_002)。
  - **观测面重构(Q6)**:I/O 事实跨介质统一面**推 P5**(P3 只有 TCP,提前泛化违 YAGNI);P3 `TcpClientTransport` 委托 `LastSendTime`/`LastReceiveTime`/`LastError`/`SendWaiterDepth` 给当前代际内层 + 加客户端专属代际/尝试诊断。
  - **切片**:P3-1 TcpClientTransport 核心 → (P3-2 node 集成断连 ∥ P3-3 运行时重配置)→ P3-4 真实 TCP 断连-重连回环 + 代际隔离端到端。

- **D10（P2 结构纪律:协议无关机制可复用,协议特有语义内联 —— RT_DESIGN_008）：** P2 新增的 **有界业务队列 + 生命周期状态机(三方 fiber 汇合)+ handler 消费者 fiber runner + 在途容量计数上限** 是**协议无关机制**,`DdsNode`(P4)须能原样复用;故按 RT_DESIGN_008 建成**可组合的独立件**(`BoundedQueue<T>` 至少像 `PendingTable` 一样独立;生命周期三方汇合尽量抽 helper),由 node **组合并驱动**(node 调 `queue.push`/`lifecycle.transition`),**不是**回调 `KeyOf`/`IsTerminal`/`RouteUnmatched` 的共享引擎(守 RT_NODE_003:机制复用、语义内联,与 PendingTable 定位一致)。**协议特有语义全内联 `ProtocolNode`**:`session_id` 分配器(uint8=256 空间,LRU 空闲分配,退休窗口)、`frm_type` 盖章、key 派生、Dispatch 分类(响应/业务)、noresponse 语义、以及"256=session_id 空间"这个**值**。**不强造 `NodeCore` 上帝对象**——只抽干净可无关化的部分,避免为尚不存在的 DDS 需求过度泛化(泛化过头会把协议特有漏进共享件),P4 `DdsNode` 需求具体后再收口共享面。noresponse 分配 session_id 盖帧但不登记 PendingTable、不占 256 在途预算(纯线缆标签)。256 在途上限受线缆 `session_id` 宽度硬顶;绝对防迟到误配随 P3 连接代际 / TBD-003 加宽 nonce 到位(P2 落 LRU 退休窗口 + 总超时的实用保证)。

- **D9（自定义关联键 = node 级可注入,只开放 `KeyOf`,不塌回 InteractionPolicy）：** 外部协议请求与响应的 `message_id` 不相等（占位规则:响应码 = 请求码 | `kResponseMarker`,占位 `0x1000`;`session_id` 对端原样回带），故匹配键需把响应码**归一化回请求码**:`ProtocolKey = (session_id<<16) | (cmd & ~kResponseMarker)`。用户可自定义该键派生,但必须落成**安全形态**,守两条判据:(1) `PendingTable<ProtocolKey,Message>` 永远只见**不透明键、从不回调用户代码**（D2 薄基座保持）;(2) 自定义是**单个 node 的构造参数**（`ProtocolNodeConfig` 持 `CorrelationKeyStrategy{request_key, response_key}`,缺省 = `DefaultProtocolKeyStrategy(0x1000)`），**非框架级共享 policy 注册表**,不驱动多 node、不形成公共架构层（RT_NODE_003 保持——node 本就各自实现协议语义）。分两档:常量级（改 `response_marker`）与函数级（整对换 `request_key`/`response_key`）。`Key` 类型 P1 固定 `uint32`（`session_id:8+命令码:16`=24 位够用;命令码有效域 `0x000–0xFFF`,高位留 marker）;DDS 的 `correlation_id` 字符串键由 P4 `DdsNode` 各自实例化 `PendingTable`。**红线纪律:P1 只开放 `KeyOf` 一个点,`IsTerminal`(哪种 frm_type 算响应)与 `RouteUnmatched`(未匹配帧处置)保持内联固定;三者一旦都可注入 = 重建 InteractionPolicy(RT_NODE_003 禁止),放开须专门评审 + ADR。**

- **D8（P1 节点交互状态串行化 = 锁,不是 strand/affinity）：** 同一 node 的交互状态（PendingTable 的 map、`session_id` 分配器、生命周期状态、丢弃计数器）被**读-分发循环 fiber、请求发起 fiber(可几百在途)、关闭控制**三类 fiber 结构性并发访问——这不是偶发而是架构必然（读循环独立跑却与请求 fiber 共享挂起表）。不串行会得到:发号丢更新→关联键相撞→响应交错/永等（违反 RT_REQUEST_001/002）；`std::map` 并发 find/insert/erase→容器撕裂或 entry use-after-free；`Register` 抢在 `FailAll` 扫表之后落表→幽灵在途请求永不终结→`WaitClosed` 永挂（违反 RT_LIFECYCLE_006）；计数丢更新→破坏 `丢弃归因` 可审计判据。D2/ADR-0001-D2 允许"锁 / strand / affinity"三选一满足 CONTEXT.md 的 `节点所属执行域` 串行要求；**P1 选 `std::mutex`**（复用 `SharedCompletion` 已验证的锁模型，跨线程 M:N 下也正确），因为 strand/affinity 依赖的**执行域转交管道尚未落地**（RT_IN_INTERFACE_005，见「尚未解决」）。注:P1 单线程协作调度器上,`Register`/`Resolve`/erase 均**不 `await`**、本已天然串行,锁的价值在为将来 M:N 跨线程兜底。运行时(AsyncTask `await`)**只出现在 `PendingTable::Handle::Wait` 的挂起点**,表结构操作是普通同步临界区。

## 影响（Consequences）

- **正面：** 有了单一权威的整体设计 + 有序路线图，`/to-tickets` 可逐期拆票、票带清晰验收；纵向薄切片让最大架构风险（无引擎内联 node）在 P1 即被证实；最大化复用避免重写 CRC/扫描、FastDDS CDR-bypass 等难写对的纯逻辑；命名提升让公共 API 干净。
- **代价/风险：** P0 是一次较大的删除 + 重命名 + 移植，须以 tag `v0.3.0` 为恢复基线并保持每步可构建；改造沿用的 `Message`/`ICodec`/`ITraceSink` 与目标 coro 契合度需在 P1 落地验证；五模式状态机/帧常量/性能基线受外部依赖（TBD-001/003/004）就地占位推进。
- **可逆性：** D3（切法）、D6（顺序）相对可逆（可调期序）；D2（删 as-built）、D5（命名提升）落地后回退成本高（但有 tag 留档）。
- **落点：** `docs/设计说明书-协程原生.md`（§2 设计、§3 模块布局与清理清单、§4 路线图 P0–P6）。

## 尚未解决

- 目标 `Message`/`ICodec`/`ITraceSink` 改造后精确签名（P0/P1 定）。
- socket QThread ↔ 节点执行域绑定机制（RT_IN_INTERFACE_005）；P1 以 `std::mutex` 串行化交互状态（D8）绕过,strand/affinity 限域待执行域转交管道落地后再评估。
- DDS 交接投递机制/容量与 QoS 一致性（P4）。
- `PendingTable` 同 key 并发多待（当前键空间不需要）。
