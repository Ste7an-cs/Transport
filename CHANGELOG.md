# 变更日志（Changelog）

本项目的所有重要变更记录于此。格式借鉴 [Keep a Changelog](https://keepachangelog.com/)，版本遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

权威参考：[需求规格说明书（SRS）](docs/需求规格说明书-协程原生.md) · [软件设计说明（SDD, GJB438C）](docs/软件设计说明-GJB438C.md) · [架构决策记录（ADR）](docs/adr/)。as-built（0.3.0）需求/设计文档存档于 git tag `v0.3.0`。

---

## [Unreleased]

### 重设计:传输/节点接口收窄、请求关联改为按键分配、手搓同步件一律换用 AsyncTask 原语(ADR-0008)

> **破坏性变更。** 依 ADR-0008,`ITransport` / `NodeBase` / `ProtocolNode` 的接口面一次性重画,六个与 AsyncTask 重复的手搓件删除。当前编译面收窄为 **UDP + ProtocolNode**;TCP / 串口 / DDS 及其节点按同一形态后续跟进。全量 **112 tests** 通过(`--gtest_repeat=6` 无抖动)。

**接口变更（破坏性）**

- **变更** `ITransport` 收窄至七个方法:`Start()` / `Close()` / `WaitClosed()` + `AsyncRead()` / `AsyncWrite(Datagram)` + `LastError()` / `CurrentLinkState()`。
  - `Read()` → `AsyncRead()`(签名不变,仍交出读队列句柄);`Write(SendUnit)` → `AsyncWrite(Datagram)`;`RequestClose()` → `Close()`;`WaitClosed(OperationOptions)` → `WaitClosed()`(无参、无返回值)。
  - **删除** `LastSendTime()` / `LastReceiveTime()`——无人消费,且不是"此刻的 I/O 事实"而是历史记录。
- **变更** `NodeBase` 的 `Close()` 改为**只发信号**,等待收敛移入 `WaitClosed()`。`Close()` 因此不含任何等待点、**任何 fiber 都可调用**——旧形态为规避自等待而设的 `SignalClose()` / `ConvergeAfterReadLoop()` / `MarkRunning()` / `JoinHandler()` / `DrainUnstartedBusiness()` 五个反向回调,连同"内部工作单元不得调 `Close()`"的使用契约一并**删除**。公开面 6→4、钩子 5→3(`DoStart`/`DoClose`/`DoJoin`)、成员 10→4。
- **变更** `ProtocolNode` **不再拥有、也不启停传输**:改为按引用借用,宿主负责传输的 `Start`/`Close`/`WaitClosed`;读侧走 `AsyncRead()->shared()` 取独立订阅,节点关闭只断自己这一路。由此一条传输可被多个节点共用。公开面 11→5。

**新增**

- **新增** `include/transport/core/Dispatcher.hpp` —— 协议无关的按键分配器 `Dispatcher<T, Fields...>`。调用方只提供键提取函数,订阅时不参与匹配的字段填 `kAny`;**部分匹配由本件实现**,无需哨兵值或字段组合枚举。一条消息投给全部键匹配的订阅者、各得一份(支持多消费者与旁路监听);内部按 mask 分层索引,单条消息成本 O(在用 mask 种数 + 收件人数),与订阅者总数无关。
- **新增** `ProtocolNode::Subscribe(Key)` —— 供一次交互分段等待多条报文(各段各自设定时限)与旁路监听。
- **新增** `UdpConfig::silence_timeout`(默认 5s)—— 读超时(心跳超时),**同时**是 bind 失败后的重试间隔。

**删除**

- **删除** `core/SharedCompletion.hpp` → `Awaitable::close()` 广播 + `FiberTask::get()` 汇合。
- **删除** `node/BoundedQueue.hpp` → `Coro::Awaitable` + `setCapacity`(`FiberChannel` 本就是有界 FIFO)。
- **删除** `node/PendingTable.hpp` → `Dispatcher`。
- **删除** `OperationOptions` → 超时直接用 `std::chrono::milliseconds`。
- **删除** `transport::Result<T>` / `transport::Status` → `Coro::Result<T>` / `Coro::Result<void>`(前者本就只是别名,`core/Result.hpp` 一并删除)。
- **删除** `SendUnit` → 与 `Datagram` 结构相同,合并;字段 `Datagram::source` 更名 **`peer`**(读侧为发送方、写侧为目的地,方向由接口决定)。
- **删除** `CorrelationKeyStrategy` / `DefaultProtocolKeyStrategy` / `ProtocolKey` / `kResponseMarker` —— 键提取收为一行 `make_tuple(session_id, message_id, frm_type)`。
- **删除** 全部观测计数接口:`NodeBase::CloseDropCount()` / `LastCloseLatency()`、`ProtocolNode` 的八个 getter、`HandlerLoop` 的三个计数。观测只剩 `ITraceSink` 一条出口。

**能力回退（明确接受，不留隐性欠账）**

- **写侧无任何同步错误**:除生命周期外,目的地非法、报文超长、socket 写失败一律只落 `LastError()`。SRS §3.1.3"超出允许大小应在发送前失败"作废。(背压能力此前已随 ADR-0004 的需求重审丧失,本轮不变。)
- **业务队列失去字节上界与 tail-drop**:改为静默丢**最旧**且无计数、无归因,与原 tail-drop 语义相反。§3.6 的 loss=0 等式不再可直接验证(#152)。
- **`Request` 失去取消令牌**:在途请求只能等超时。
- **在途交互超过 256 时 `session_id` 重复**:`session_id` 简化为 `uint8` 自增计数器(推翻 RT_REQUEST_005/006),重复的键会让一条响应同时投给两个订阅。
- **UDP 临时端口在重建后换号**:`local_port = 0` 且静默超时默认 5s 时,空闲链路会周期性重建并更换源端口。

**测试**

- **新增** `tests/dispatcher_test.cpp`(19)、`tests/protocol_node_test.cpp`(23)、`tests/fake_transport.hpp`(实现新 `ITransport` 的假传输);UDP 用例 18 → 22。
- **暂排除** 八个仍在旧接口上的用例(`protocol_node_handler` / `protocol_node_lifecycle` / `protocol_node_capacity` / `protocol_node_udp` / `transport_contract` / `fake_coro_transport` / `send_semantics_fake` / `handler_loop`),清单记在 `CMakeLists.txt` 注释中。

**文档**

- **新增** `docs/adr/0008-interface-redesign-and-key-based-dispatch.md`;ADR-0004/0005/0006/0007 各加变更注记。
- **更新** `CONTEXT.md`(术语:`Datagram`/匹配键/发送完成语义/读-分发循环/`NodeBase`/`HandlerLoop`/丢弃归因,新增按键分配、`kAny`、订阅凭据)、`README.md`(传输契约与状态说明)、`docs/软件设计说明-GJB438C.md`(§3 变更说明、§4.1/4.3.5、§5.2 改 `CSU_DISPATCHER`、§5.3 标注删除、§5.4 重写、§5.5 变更注)、`docs/需求规格说明书-协程原生.md`(RT_REQUEST_005/006、RT_LIFECYCLE_005、`WaitClosed` 接口登记、统计面四处变更登记)。

### 重构:读循环与 handler 计数下放各 node,删除 NodeRuntime(ADR-0006 D5,#140)

> 依 ADR-0006 **D5**,ADR-0006 重构链收尾。**公开 API 面与可观察行为不变**;`NodeBase` **零改动**,`loss_accounting_test.cpp` **零改动**、两条等式(`Σ命名原因 == 总丢弃`、`drop_records.size() == Σ`)原样通过。

- **删除** `include/transport/node/NodeRuntime.hpp`。全库代码/测试/CMake 无残留。至此该件的四项职责各归其位:生命周期 → `NodeBase`(#139)、handler 队列 → `HandlerLoop`(#138)、读循环与 handler 计数 → 各 node(本次)。
- **变更** 读-分发循环回归各 node 的私有 `SpawnReadLoop()`,循环体逐字不变(仅 `kClosed` 退出、其余瞬时错误继续、出口调基类 `ConvergeAfterReadLoop()`)。过渡期的两个 `std::function` 参数(`decode_and_dispatch` / `on_loop_exit`)随跨件接缝一并消失——直接调本类 `DecodeAndDispatch()` 与基类 `ConvergeAfterReadLoop()`,少两次间接与两次堆分配,行为等价。
- **变更** `HandlerLoop` 由各 node 直接持有;三个 handler 观测计数(`BusinessQueueOverflowCount` / `HandlerExceptionCount` / `LastHandlerDuration`)改为一行转发。`CloseDropCount()` / `LastCloseLatency()` 仍继承自 `NodeBase`(#139 已上移)。**公开签名与语义一律未动。**
- **新增** `tests/handler_loop_test.cpp`(11 例),补上与兄弟小件 `BoundedQueue` / `PendingTable` 对齐的独立覆盖(#138 当时以"生命周期尚未稳定"为由未做):串行消费 + FIFO、逃逸异常隔离不自关、队列满 tail-drop 归因(含 sink)、`Join()` 在 consume 卡于 await 时确实挡住、未 Spawn 时 `Join()` 立即返回、`CancelAndClose` 幂等、`DrainForClose` 条数、时长与起止 Trace、协议无关。
- **明确接受的重复**(ADR-0006 D5):两个 node 的 `SpawnReadLoop()` 13 行、两个叶子钩子与三个计数转发现在逐字相同。第三个 node 出现前不宜再抽共享件;届时可考虑一个**不含任何协议知识**的 `ReadLoop(ITransport&, fn)` 自由函数,但那是新决策、须走 ADR。
- **验证** 全量 303 tests `--gtest_repeat=5` 全绿(292 基线 + 11 新增),**零挂起**;`HandlerLoop` 单测 ×20 全绿。`ProtocolNodeReconnect` 的时间预算断言在基线与本分支各 ×40 均为 2 次抖动(约 5%),为既有问题、非本次引入。

### 重构:NodeBase 模板方法取代 NodeRuntime 的生命周期职责(ADR-0006 D1/D2/D6,#139)

> 依 ADR-0006 **D1/D2/D6**。生命周期由新基类 `NodeBase`(非模板,实现进 `.cpp`)以**模板方法**承载:基类管幂等保护与关闭仲裁,协议特有的启动/关闭实事下沉为虚钩子。`ProtocolNode` / `DdsNode` 改为**继承** `NodeBase`(推翻 SDD DD-3/DD-4/§4.1.4 原"不继承"的结构结论)。**公开 API 面与可观察行为不变。**

- **新增** `include/transport/node/NodeBase.hpp` + `src/node/NodeBase.cpp`。公开面 `Start()`/`Close()`/`WaitClosed()`/`IsRunning()`,均返 `Status` 而非 `bool`(D2:`bool` 会把"已 `Running`(成功)"与"已 `Closing`/`Closed`(RT_LIFECYCLE_003 要求 `InvalidState`)"压成同值,且 RT_LIFECYCLE_007 要求宿主据错误改配置重试)。
- **钩子** `ValidateConfig()`(默认成功)、`DoStart()`/`DoClose()`(纯虚)、`JoinHandler()`(默认空)、`DrainUnstartedBusiness()`(默认 0)。`ValidateConfig()` **先于 `starting_` 求值**,故配置校验失败**不 latch `start_done_`**——否则宿主改正配置后重试时并发进来的 `Start` 会共享到陈旧的 `kConfiguration`。后两个钩子使收敛能够到 node 持有的可选 `HandlerLoop`,而不必让基类持有协议类型。
- **删除** `NodeRuntime::SetNodeConvergenceSignal()` —— node 侧收敛信号改由虚钩子 `DoClose()` 承载。**硬约束**:基类在 `DoClose()` **返回之后**才 `close_signalled_.Complete()`,读循环挡在 `Wait` 上直到全部汇合信号发完才 join handler(否则队列未 `Close`、handler 未取消,join 必然挂死)。
- **变更** `NodeRuntime` 收缩为**过渡件**(473 → 185 行):生命周期职责全部迁空,不再持有任何生命周期状态与 `std::mutex`;仅余读循环骨架(新增 `on_loop_exit` 出口参数)与 `HandlerLoop` 的持有驱动,待 **#140** 下放各 node 后删除文件。`CloseDropCount()`/`LastCloseLatency()` 随收敛段落上移基类,公开签名与调用点不变。
- **验证** 全量 292 tests `--gtest_repeat=5` 全绿、生命周期用例 `--gtest_repeat=20` 全绿,**零挂起**。(该段代码正是上一轮生命周期重构挂死并被整体回滚之处,见 ADR-0005 D9。)

### ⚠ 语义变更:RequestClose 只发信号,撤销重入守卫(ADR-0006 D8,#144)

> 依 ADR-0006 **D8**(**撤销 ADR-0005 D6**)。不设任何运行时重入守卫——不比对 fiber id、不登记内部工作单元身份、不设"半执行"分支;`Close()` 结尾无条件 `closed_.Wait()`。取而代之的是**结构性保证**:内部工作单元不再有任何会等待的入口。

- **⚠ 行为变更**(**破坏性**) 调用方若捕获节点引用、在入站业务处理器内**直接调用**等待型的 `Close()` / `WaitClosed()`,此前得到 `kInvalidState`(半执行分支),现在将**静默挂死**。ADR-0005 D6 当初正是以此为由否决该方案,D8 推翻该权衡并记录了理由:守卫的唯一价值是给一个**已确认不会发生**的违约提供诊断,代价却是在生命周期这段最难的代码里常驻两处 fiber id 登记/注销与一个跨锁判据。该约束现为**使用契约**(SRS RT_LIFECYCLE_005),已写入 API 注释。
- **变更** `HandlerContext::RequestClose()` / `DdsHandlerContext::RequestClose()` **签名不变、语义收窄为"只发起、不等待"**:内部改调框架的发信号路径(node 私有 `SignalClose()` → `NodeRuntime::SignalClose()`),**受理即返回**,收敛由读-分发循环完成。返回值仅表示"已受理",**不表示"已关完"**。命名与 `ITransport::RequestClose()`(发信号)/ `WaitClosed()`(等待)的既有约定一致。原登记的"应移除这两个入口"(SRS §3.2.2)**已撤销**——二者保留。
- **删除** `NodeRuntime::Close()` 的 `in_handler_fiber` 半执行分支与 `WaitClosed()` 的同类分支;`HandlerLoop::IsCurrentFiber()` 及其 fiber id 成员随之删除(无使用者)。`Close()` 现即 `SignalClose() + closed_.Wait()`。
- **测试变动**(随能力变更,**非断言放松**) 删除 `ProtocolNodeLifecycle.HandlerNodeCloseDoesNotSelfDeadlock`(该场景在新语义下必挂死,属已接受的违约面,测试文件头已注明为何无用例);加强 `HandlerRequestCloseDoesNotSelfDeadlock`(断言受理即返回、协作取消已触发、`LastCloseLatency()` 仍为 0、其后正常收敛);新增 `HandlerRequestCloseIsAcceptedButNotYetClosed`(以外部 `WaitClosed()` 等待者为观察点,证明受理 ≠ 关完)。
- **验证** 全量 292 tests `--gtest_repeat=5` 全绿,无挂起。

### ⚠ 能力移除:SharedCompletion 轻量化为广播完成量(ADR-0006 D3,#137)

> 依 ADR-0006 **D3**。`SharedCompletion<T>` 由「waiter map + 每等待者独立 `Awaitable`」改为「**存结果 + 一条共享 `Awaitable` + `close()` 广播**」,类体 92 → 48 行(整文件 124 → 60 行)。全仓 12+ 处实例(7 个传输的 `closed`、`TcpServer` 3 处、node 侧 3 处)**调用代码零改动**。

- **⚠ 能力移除**(**破坏性**) `WaitClosed(OperationOptions)` 不再支持 **`cancellation` 取消令牌**;传入的令牌被**静默忽略**。`deadline` 支持不变。依据:全仓生产代码无任何一处向 `WaitClosed` 传取消令牌;`Awaitable::await_for` 超时返回 `timed_out` 而**不关闭 channel**,故 deadline 本就不需要为每等待者分配独立通道。SRS §3.2.2 接口变更登记已记。
- **测试变动**(随能力移除,**非断言放松**) 删除 `CoroSharedCompletion.CancellationOnlyEndsTheCancelledWaiter` 与 `PreCancelledWaitReturnsCancelled`(后者在新实现下会永久挂起);`CoroFakeTransport.WaiterTimeoutAndCancellationAreLocal` 拆分,保留超时局部性部分并改名 `WaiterTimeoutIsLocal`。**新增** `TimedOutWaitLeavesCompletionUsableForLaterWaiters`,覆盖共享通道特有的新风险(超时若误关通道,其后**新进场**的等待者将等不到广播)。`CompleteBeforeCancelReturnsCompletedValue` 等保留,现作为"令牌被静默忽略"的护栏。
- **正确性依据** 底层 `FiberChannel::closed_` 是**持久 latch**(`close()` 幂等置一次、不复位;`pop`/`pop_wait_for` 谓词均含 `closed_.load()`),故"查结果落空 → 尚未 await"窗口内发生的 `Complete` 不会丢唤醒。此依据 ADR-0006 初稿未点明,已回填 SDD §5.1。
- **验证** 全量 292 tests `--gtest_repeat=3` 三轮一致(291 通过 / 1 既有失败 #123),无挂起;`CoroSharedCompletion.*:CoroFakeTransport.*` ×20 轮全绿。

### 功能:致命错误自终,落地 RT_LIFECYCLE_008(ADR-0005 D5,#120)

> 依 ADR-0005 **D5** / RT_LIFECYCLE_008。此前节点因底层致命错误而收发终止后会停留在 `Running`,读-分发循环退出后常驻在收敛入口等待外部关闭,`WaitClosed` 的等待者永不被唤醒(僵尸节点)。本次消除该状态。

- **变更** 读-分发循环退出时若节点仍 `Running`(即非我方 `Close` 所致),由其**自行**置 `Closing` 并发出与 `Close` **完全相同**的一组汇合信号(`transport.RequestClose` + 业务队列 `Close` + handler 协作取消 + node 侧收敛信号),再走**同一段**收敛代码。正常关闭与自终由此合并为一条路径,区别仅在"谁先置的 `Closing`"。
- **变更** 发起段抽为共用的内部 `SignalCloseIfFirstCloser()`;仲裁点仍是 `lifecycle_` 单点,故并发的 `Close` 与自终之间**恒只有一个发起者**,`close_signalled_` 亦只被 Complete 一次。
- **变更** node 侧协议特有收敛信号(`PendingTable.FailAll` 等)由 `NodeRuntime::Close` 的入参改为**构造期登记**(`SetNodeConvergenceSignal`)——自终路径无从取得入参,却必须发出完全相同的一组信号。`NodeRuntime::Close` 签名相应变为无参(内部接缝,不属公开 API)。
- **不按介质分支**:判据只是"读循环退出时是否仍 `Running`"。TCP 客户端无限重连、`Read` 只在我方 `Close` 后返 `kClosed`,彼时 lifecycle 已非 `Running`,**天然不自终**;TCP 服务端已接受连接仍由 `TcpServer` supervisor 驱动收敛。
- **新增测试** 自终唤醒 `WaitClosed` 等待者、自终终结在途请求、TCP 客户端断连不自终。
- **验证** 全量 293 tests `--gtest_repeat=3` 三轮一致(292 通过 / 1 既有失败 #123),**无挂起**;生命周期与重连五套件 52 tests `--gtest_repeat=20` 全绿无挂起。

### 重构:handler 汇合改用结构化并发 join,移除处理器取消超时观测(ADR-0005 D2/D8,#119)

> 依 ADR-0005 **D2**(handler 汇合改 `FiberTask::get()`)与 **D8**(删除 500 ms 处理器取消观测)。**关闭路径的行为保证不变**:仍触发 handler 协作取消、仍等待处理器实际退出、仍不强制销毁 fiber;变的是汇合机制与诊断埋点。

- **变更** `SpawnHandlerLoop` 保留 `Coro::FiberTask<void>` 句柄;收敛者以 `get()`(让出式 join)等待 handler 消费者退出,取代手写的 `SharedCompletion<void> handler_done_`。**删除** `handler_done_` 与 `has_handler_`(句柄为空即无 handler)。附带收益:`FiberTask::get()` 的返回由运行时保证(内部 `catch(...)` 归一为 `interrupted`、包装器在正常与异常两条路径上均 `set_value`),故 `closed_.Complete` 在所有路径上必达——手写完成量在 handler 走异常路径时会永不 Complete。
- **移除** 500 ms 处理器取消超时观测:`kHandlerCancelObservation`、超时记 `kInternal` 的观测分支、累计计数字段。**理由**:该计数自引入起从未被任何测试或示例消费(全仓零命中),且 SRS §3.1.6 本就将"是否单设该计数"列为 P6 待定项;若日后确需"谁拖慢了关闭"的线索,正确落点是在 handler 循环内对**每次回调**计时(可定位到具体回调),而非在收敛处观测总时长。
- **💥 破坏性** 移除公开访问器 `ProtocolNode::HandlerCancelOverrunCount()`(RT_IF_API,SRS §3.2.2 接口变更登记已同步)。
- **保持不变** 协作取消机制、SRS §3.1.5.4 对处理器的 500 ms 返回期望(TBD-007 设计目标,亦是 §3.6 关闭时延目标的适用前提)、`closed_`/`start_done_`/`close_signalled_` 仍为 `SharedCompletion`(D3)。
- **验证** 全量 290 tests `--gtest_repeat=5` 与 master 基线逐轮一致(289 通过 / 1 既有失败 #123),**无挂起**;四个生命周期用例文件 `--gtest_repeat=20` 全绿无挂起。

### 重构:关闭收敛并入读循环,删除 finalizer fiber(ADR-0005 D1,#118)
> 依 ADR-0005 **D1** / RT_LIFECYCLE_006。ADR-0004 落地后交互层内部工作单元由三条降为两条(读-分发循环 + handler 消费者),其中**读循环恒是第一个退出的**(我方 `Close` 与不可重连介质的底层致命错误在 ADR-0004 D1 之后同返 `kClosed`),故它天然是收敛的正确位置。公开 API 与可观察行为不变。
- **变更** `NodeRuntime::Close` 退化为"发汇合信号 + 等待收敛结果":置 `Closing` → `transport.RequestClose` + 业务队列 `Close` + handler 协作取消 + node 侧收敛回调 → 放行读循环收敛 → 等 `closed_`。**不再 spawn 独立 finalizer fiber**;`Close` 时节点内不再新增任何 fiber。
- **变更** 读-分发循环退出后**兼任收敛者**(`ConvergeAfterReadLoop`):等关闭汇合信号 → 等 handler 退出(以 `FiberTask::get()` join,仍等实退出、不强杀)→ Drain 未启动业务归因 `close_drop` → 置 `Closed` + 记关闭时延 → `closed_.Complete` 唤醒全部等待者。**结构约束**:收敛走内部路径,读循环不得调用公开的 `Close()`(那会等待自身退出)。
- **删除** `loop_done_`(无人再等"读循环已退出"——它自己就是收敛者);新增内部 `close_signalled_` 承接"首个 `Close` 已发出全部汇合信号",使读循环因致命错误先行退出时挂在收敛入口等待外部关闭(致命错误自终属 RT_LIFECYCLE_008 / ADR-0005 D5,另票)。
- **保持不变** `closed_` / `start_done_` 仍为 `SharedCompletion`(ADR-0005 **D3**:每等待者独立 deadline/取消,不可换共享 `Awaitable`——**该理由已由 ADR-0006 D3 修正并取代**,见本文件 #137 条目);`LastCloseLatency` / `close_drop` 归因 / 多等待者 `WaitClosed` 行为均不变。
- **验证** 全量 291 tests `--gtest_repeat=5` 与 master 基线逐轮一致(287 通过 / 3 skip(#112)/ 1 既有失败 #123),**无挂起**;四个生命周期用例文件 `--gtest_repeat=20` 全绿无挂起。
- **文档** SDD 新增 **DD-13**、§4.2.5/§4.2.10/§5.4 按新收敛路径重写;SRS §4.4 更新 RT_LIFECYCLE_005/008 的现状描述;时序图 `seq-close` 与状态图 `state-node-lifecycle` 重画并重渲染,`arch-class`/`sdd-csc-node`/`dfd-toplevel` 同步。

### 破坏性变更:删除 `IConnectionObservable` 接口(ADR-0004 D2/D7 收尾,#111)
> ADR-0004 **D2**(链路可用性并入 `ITransport`)与 **D7**(连接诊断项降级为具体方法)落地后,该可选观察面接口已无实现者、无调用方,予以删除。交互层不再按介质探测能力接口。
- **移除**(**破坏性**)公开头文件 `include/transport/io/IConnectionObservable.hpp` 及接口 `transport::IConnectionObservable`。下游若直接包含该头文件,改为包含 `transport/io/tcp/TcpClientTransport.hpp`。
- **变更** `ConnectionState` 枚举的定义位置随之迁至 `include/transport/io/tcp/TcpClientTransport.hpp`(其唯一使用者)。**命名空间与枚举项不变**(仍为 `transport::ConnectionState::{kDisconnected,kConnecting,kConnected,kReconnecting}`),`TcpClientTransport::State/WaitForState/WaitStateChange` 签名不变;库内所有使用者已包含该头文件,零改动。
- **替代能力**:所有介质同形的链路可用性经 `ITransport::CurrentLinkState()` 获取;连接代际/尝试次数/最近失败/下次尝试时刻等诊断为 `TcpClientTransport` 的具体方法,不构成多态缝。
- **文档** 同步更新 SDD(GJB438C §4.3.5)、设计说明书 §3 目录分组与 §6 P3 回填记录、README P3 条目;类图 `arch-class`/`sdd-csc-io`/`sdd-csc-layers` 去掉该接口并补 `CurrentLinkState`,图 4-9(`seq-generation-isolation`)改绘为 ADR-0004 D1/D3 后的"断链对交互层完全透明"流程(SVG 一并重渲染)。

### ⚠ 破坏性变更:丢弃归因七项减为六项 —— 删 `DropReason::kGenerationIsolationDrop`(#112)
> 依据 ADR-0004 **D3(撤销连接代际隔离)**:断链时交互层不再批量终结在途请求、不再清空旧链路排队业务,该归因项的**产生机制已不存在**(其 `ProtocolNode` 侧计数器与访问器已随 #110 删除,计数恒为 0)。本次删除归因项本身并改写 loss=0 harness。
- **破坏性** 公开枚举 `transport::DropReason` 移除枚举项 `kGenerationIsolationDrop`;`DropReasonName` 不再产生稳定短名 `"generation-isolation-drop"`。下游若对该枚举做穷尽 `switch` 或依赖该短名字符串,需同步调整。**六项**为:业务队列溢出 / DDS 交接溢出 / 坏帧 / 迟到·重复·无匹配响应 / 关闭丢弃 / 无处理器丢弃。
- **变更** loss=0 harness(`tests/loss_accounting_test.cpp`)按六项改写:删除专为该项而设的"真实 TCP 自动重连"最小介质场景及其 ground truth 常量与配套夹具;`MixedFailureAllSevenReasonsSigmaMatchesGroundTruth` 改名 `MixedFailureAllSixReasonsSigmaMatchesGroundTruth`。**完整性断言强度不变**——`Σ命名原因 == 总丢弃` 与 `drop_records.size() == Σ`(证无多记漏记)两条等式原样保留,只是求和项由七减六。
- **修复** 解除 #109 遗留的 3 处 `GTEST_SKIP`(该文件已无 skip;全量 skip 数 0)。全量 `--gtest_repeat=3` 三轮一致:290 tests / 289 passed / 0 skipped / 1 failed(唯一失败为既有缺陷 #123,与本次无关)。
- **文档** SDD(GJB438C)图 4-9 按 ADR-0004 新流程**重绘**,源文件随 ID `MS_LINK_DOWN` 由 `seq-generation-isolation` 更名 **`seq-link-down`**;`dfd-toplevel`/`state-connection` 两图清除代际隔离残留并重渲染;ADR-0003 D13 Q2/Q3 加"已由 ADR-0004 D3 撤销"指向(不改写历史决策);CONTEXT.md「丢弃归因」术语、SDD 路线图 §4 P5/§6 回填、README P5 条目同步。

### 文档:SRS/SDD 对齐 AsyncTask 范本(GJB 438C ID 体系 + 双向追溯 + 动因型需求)
> 参考 `third_party/AsyncTask/doc/{需求规格说明,软件设计说明}.md` 的成熟 GJB 438C 惯例,优化 transport 的需求规格说明与设计说明。仅文档,不涉代码。
- **SDD**(`docs/软件设计说明-GJB438C.md` v2.0)重写为完整 ID 体系:DD-n 设计决策(各映射 RT_*);§4.1 CSC 部件(CSC_CORE/IO/CODEC/NODE)+ 部件依赖图 + 部件类图;§4.2 执行方案 MS_* 图(**新增** DFD 上下文图 + 顶层数据流图 + 数据存储表 D1/D2/D3,复用时序/状态/数据流图并赋 MS_* ID);§4.3 JK 接口(五要素:优先级/接口类型/数据元素/通信方式/协议特征);§5 CSU 详细设计(逐单元);§6 **双向追溯**(§6.1 设计单元→需求 + §6.2 需求→设计单元)。
- **SRS**(`docs/需求规格说明书-协程原生.md`):§3.2.1 接口 ASCII 图改 Mermaid→SVG;§4.5 **新增「需求标识说明」**(RT_* 前缀体系);为 6 个核心能力(RT_CORO_RUNTIME/RT_TRANSPORT/RT_REQUEST/RT_HANDLER/RT_LIFECYCLE/RT_NODE)补 **动因型需求 RT_*_MOT_n**(固化设计取舍理由,供 DD-n 对应)。
- **图**:新增 6 张 Mermaid→SVG(dfd-context、dfd-toplevel、sdd-csc-layers、sdd-csc-node、sdd-csc-io、srs-interface);state-pending-entry 已随上条同步。

### 简化:PendingTable entry 改用裸 Coro::Awaitable(去掉 SharedCompletion 层)
> 拉取 AsyncTask 最新《使用说明》后重评:PendingTable 每个在途 entry 只有一个等待者(当前键空间不需同 key 并发多待,ADR-0001),此前背的 `SharedCompletion<T>`(多等待者 waiter-map + 第二把 mutex + 广播)是死重。公开 API 与可观察行为不变。
- **简化** `Entry` 信箱 `SharedCompletion<T>` → 裸 `std::shared_ptr<Coro::Awaitable<T>>`;**唯一仲裁点改为表锁内 `find+erase` 抢占终结权**(四方 Resolve/超时/取消/FailAll 谁先摘除谁胜),胜方在锁外对信箱 `push`+`close`;`Handle::Wait` 用 `await`/`await_for` + 抢输时一次 drain(靠 `Awaitable`"关闭后先取尽值再报错"语义裁决竞态)。对齐 AsyncTask《使用说明》§6.3 一次性等待范式。
- **保留** `SharedCompletion` 仅供多等待者 void 事件(NodeRuntime 生命周期/reactor、各传输 `closed`、TcpServer accept)。`Register/Resolve/FailAll/Handle::Wait` 签名与语义不变,`ProtocolNode`/`DdsNode`/测试零改动。
- **验证** 全量 270 tests 全绿(除已知 CGNAT flake);关联/容量(256 在途+乱序+迟到不误配+FIFO)/取消/重连/生命周期 20 遍重复压测无竞态。文档:GJB438C §5.1/§5.2 + 状态图重渲染 + 路线图 SDD §6 更新。

### 清理:P6 前置清理 —— 请求-响应链路评审整改(#98)
> 来源:ProtocolNode 请求-响应实现评审(2026-08-04)。全部为内部工程质量整改,公开 API 不变;唯一线缆可见变化是 Trace 生命周期类别改名与 noresponse `Send` 盖章取值。
- **变更** Trace 类别常量集中收口 `include/transport/core/TraceCategories.hpp`(单一权威,发射点不再散落字符串字面量);生命周期类别 **`"close"` 改名 `"lifecycle"`**("running" 挂在 "close" 下是命名误导;消费方按类别过滤需同步)。同步 SDD §6 / ADR-0003 D13 九类清单。
- **变更** `ProtocolNode::Send`(noresponse)session_id 改为**只读空闲集尾部盖帧**(不出队):不再搅动 Request 的 FIFO 退休窗口(RT_REQUEST_005 迟到误配防护不被高频 Send 削弱);256 全在途仍拒绝(既定边界策略不变)。新增回归测试 `NoresponseSendDoesNotDisturbSessionFifoOrBudget`。
- **变更** `ITraceSink` 契约硬化:`OnTrace` 可能在库内锁临界区内被调——写明必须快速返回、不得阻塞、**不得回调本库任何 API**(否则同锁重入死锁)。
- **修复** `NodeRuntime` `has_handler_` 读写挪进锁内(消除对 bring-up 无挂起点时序的隐式依赖);`ProtocolNode::Request` 的 session 归还改 RAII `SessionLease`(任一提前返回不再依赖手工 `ReleaseSession` 纪律)。
- **清理** `NodeRuntime::Close` 两分支重复收敛段提取 `ConvergeToClosed()`;删 `PendingTable::Handle::finalized_` 死成员;去重头文件 include;`max_pending=256` 双重限流补"仅防自定义键策略绕过 session 预算"注释。

## [0.4.5] - 2026-08-04

> **P5 观测 + 完整性归因里程碑**——可插拔结构化 Trace(push)+ 命名计数(pull)双面观测,7 类 `DropReason` 经唯一 `RecordDrop` 归因点使"无静默丢失"成为结构性可断言(`Σ命名=总丢弃`),I/O 事实统一为 base `ITransport` 强制接口。全量测试 220→270 全绿;详见 SDD §4 P5 / §7 追溯矩阵、#86–#90。（本版一并含 #81 的 UDP 无缝切换验证与 Default 寻址修复。）

### 里程碑:P5 观测 + 完整性归因 —— 结构化 Trace + 丢弃归因 + loss=0(协程原生)— 2026-08(路线图 P5)
> 按 SDD §4 P5,把各期就地埋的散落计数器收口成"完整性可断言"的观测面。遵 ADR-0003 **D13**:双面观测、结构性丢弃归因纪律、I/O 观测面统一。
- **新增** `DropReason` 七项枚举(业务队列溢出/DDS交接溢出/**坏帧**新增/迟到·重复·无匹配响应合并/关闭丢弃/连接代际隔离丢弃/**无处理器**新增)+ 协议无关观测原语 `RecordDrop`(计数 pull + 可选 Trace push 一次调用)/ `RecordEvent`(`include/transport/core/{DropReason,Observability}.hpp`,#86)。
- **变更** I/O 事实观测面统一(遗留项 B):`LastSendTime`/`LastReceiveTime`/`LastError` 提升为 base `ITransport` **强制虚方法**(RT_NODE_006 所有介质如实报;`SendWaiterDepth` 非普适仍留各类);补 `DdsTransport` 缺口 + `FakeCoroTransport` 测试替身(#87)。合并前审查发现并修正 DDS `LastError` 误把 `kTimeout`/`kClosed`/`kCancelled` 正常结果当故障的语义偏差。
- **新增** 7 类丢弃全线接入 `RecordDrop` 归因(每 reason 一个归属组件+定义时刻),含此前零覆盖的 `kBadFrame`(坏帧)埋点 + `BadFrameCount()`;既有具名 getter API 稳定不变(#88)。
- **新增** 9 类 Trace category(connect/generation/send/recv/decode/match/timeout·cancel/handler/reconnect/close)+ 补齐 4 项零覆盖指标:请求时延/处理器时长/重连次数/关闭时延(#89)。
- **验证** `tests/loss_accounting_test.cpp` loss=0 harness(非新运行时组件):干净跑全部 `DropReason` 计数器保持 0 + 混合故障 7 类各用最小介质触发、`Σ命名=总丢弃`(`drop_records.size()==Σ` 证无多记漏记)+ push/pull 一致 + 未配 sink 零影响(RT_TRACE_002,#90)。全量 **220→270 全绿**。
- **文档** ADR-0003 增 **D13**(P5 观测+完整性归因);SDD §4 P5 标记交付、§6 回填 P5 签名、§7 追溯 RT_TRACE_001/002·RT_DATA_BUFFER·RT_DESIGN_006 升 ✅;README 更新至 P0–P5。

### 验证 + 修复:ProtocolNode 无缝跑在 UdpTransport 上(#81)
- **验证** `ITransport` 缝可无缝切换:同一 `ProtocolNode` 只换注入的 transport/codec(`TcpTransport`+`SystemCodec` → `UdpTransport`+`SystemDatagramCodec`),Request/读-分发循环/关联匹配零改动,真实 UDP 回环跑通请求-响应 + 乱序/迟到丢弃观测(断言与 TCP 回环用例一致);新增 `tests/protocol_node_udp_test.cpp`。
- **修复** `UdpTransport::Write` 寻址:原将 `Endpoint::Default()` 当非法,与 `Endpoint::kDefault`(用 config 默认目的地)+ `UdpConfig.remote_addr` 设计本意冲突,导致恒发 Default 的传输无关调用方在 UDP 上 `kInvalidArgument`;修成 **Default → 解析为 config 默认目的地**(未配 remote 仍 `kInvalidArgument`,向后兼容)。SDD §7 追溯 RT_IF_UDP/RT_IN_INTERFACE_001/RT_DESIGN_006 补 node 端到端。

## [0.4.4] - 2026-07-29

> **P4 其余介质里程碑:UDP/串口/DDS 传输 + DdsNode + TCP 服务端 accept**——在 0.4.3 之上补齐三种介质与第二个交互节点,**证实 D10 协议无关机制可复用**(DdsNode 复用 PendingTable 仅一行 Key 改动、BoundedQueue/NodeRuntime 零改动),并**闭合 ADR-0001 跨线程唤醒 fiber 未决项**。全量测试 180→220 全绿;详见 SDD §4 P4 / §7 追溯矩阵、#68–#73。

### 里程碑:P4 其余介质 —— UDP/串口/DDS + DdsNode + TCP accept(协程原生)— 2026-07(路线图 P4)
> 按 SDD §4 P4 补齐介质与第二节点。遵 ADR-0003 **D12**:DDS 交接边界复用 BoundedQueue、NodeRuntime 收口(D10 兑现)、统一寻址靠 Endpoint。
- **新增** `NodeRuntime<Event>` —— 从 ProtocolNode 抽出的**协议无关**节点机制(生命周期三方汇合 + 并发幂等 Start + handler 单消费者 + BoundedQueue 集成 + 读-分发循环骨架 `SpawnReadLoop` + `AddFinalizerJoin`),ProtocolNode/DdsNode **组合并驱动**(机制复用语义内联,非 policy 引擎,守 RT_NODE_003);ProtocolNode 重构为组合它、行为不变(D10 收口,#68)。
- **新增** `DdsTransport` + provider **跨线程交接边界** —— 复用 `BoundedQueue<Sample{bytes,topic}>`,listener 线程非阻塞 Push tail-drop + `dds_handoff_overflow` 计数;`Read`=Pop(source=kTopic)/`Write`=`provider.Publish`;多 topic 订阅、同 topic 保序;**跨线程唤醒经 AsyncTask FiberChannel 确证安全(1000 轮压测),闭合 ADR-0001 未决项**(#71)。
- **新增** `DdsNode` —— 组合 `NodeRuntime`+`DdsTransport`+`DdsCodec`+`PendingTable<std::string,Message>`;pub-sub(`kNotify`)+ 多路请求-应答(`kRequest`/`kReply` 经 correlation_id + reply_to inbox);`Request(Message, Endpoint target, options)` / `Publish(Message, Endpoint topic)`;无连接(D3′)。**D10 复用实证:PendingTable 一行 Key 改动、BoundedQueue/NodeRuntime 零改动**(#73)。
- **新增** `UdpTransport`(coroudpsocket,报文+地址,`Datagram.source=kNet`,发往不同地址,非重连,#69)、`SerialTransport`(coroiodevice + QSerialPort,字节流切片,openpty PTY 真实测,断开致命 TBD-005,#70)。
- **新增** `TcpServer` accept 层 —— corotcpserver `nextConnection` 每连接派生 ProtocolNode(内层 TcpTransport,非重连 D3′,连接生命=节点生命,RT_DESIGN_004);per-connection supervisor fiber 管子 node 生命周期(#72)。
- **统一寻址**:`SendUnit.destination`/`Datagram.source`(`Endpoint` kNet/kTopic)贯通 UDP 多地址、DDS 多 topic —— 同一 `ITransport::Write` 接口。
- **验证** 各介质单机真实回环:UDP loopback、串口 PTY(openpty)、进程内 FakeDdsProvider 跨线程交接、真实 TCP accept。全量 **180→220 全绿**。
- **文档** ADR-0003 增 **D12**(P4 决策);ADR-0001 关闭"跨线程唤醒 fiber"未决项;SDD §4 P4 标记交付、§6 回填 P4 签名、§7 追溯矩阵 DDS/UDP/串口/accept 升 ✅;README 更新至 P0–P4。

## [0.4.3] - 2026-07-29

> **P3 连接管理里程碑:TCP 客户端自动重连 + 连接代际 + 运行时重配置**——在 0.4.2 交互节点基座上,`TcpClientTransport` 为 TCP 客户端补齐跨重连的稳定收发:自动退避重连、连接代际隔离迟到事件、运行时重配置端点切换,node 观察连接状态但不管理 churn。全量测试 152→180 全绿;详见 SDD §4 P3 / §7 追溯矩阵、#57–#60。

### 里程碑:P3 连接管理 —— TcpClientTransport 自动重连 + 连接代际 + 运行时重配置(协程原生)— 2026-07(路线图 P3)
> 按 SDD §4 P3 交付 TCP 客户端连接管理。遵 ADR-0003 **D11** 行为契约:node 观察连接状态但不管理 churn;连接概念不下沉 base ITransport(D3′),连接代际不进 PendingTable(守 RT_DESIGN_008)。全量测试 152→180 全绿。
- **新增** `TcpClientTransport`(外层 owns socket,实现 `ITransport` + `IConnectionObservable`):状态机(`Disconnected/Connecting/Connected/Reconnecting`)+ corosocket `connectToHost`+`await_for(可配超时)`+超时 `abort()`(摩擦 1)+ 退避重连(1s×2 上限 30s ±20% jitter、稳定 60s 重置、无限重试)+ 连接代际(单调,每次成功物理连接 +1),组合内层 `TcpTransport`(一代际一实例);非 Connected 态 `Write→kConnection`(RT_TCP_RECONNECT_003,#57)。
- **新增** `IConnectionObservable` 可选接口(`State`/`WaitForState`/`WaitStateChange` + 代际/配置版本/最近失败/尝试次数/下次尝试诊断);拉模型多 fiber 条件等待(RT_TCP_RECONNECT_005,不进 base ITransport,#57)。
- **新增** 节点集成断连:`ProtocolNode` 检测 `IConnectionObservable` → spawn reactor fiber,断连时 `PendingTable.FailAll(kConnection)`(在途请求恰好一次 `Connection`,RT_TCP_RECONNECT_002)+ Drain 旧代际未启动业务计 `GenerationIsolationDropCount`(RT_TCP_RECONNECT_004)+ node 保持 Running;`TcpClientTransport::Read` **透明跨重连**(断连期阻塞等下一代际,读循环永不因断连退出),`PendingTable::FailAll` 加协议无关 `latch_closed` 开关(Close=true / 断连=false,#58)。
- **新增** 运行时重配置 `TcpClientTransport::ApplyConfig(TcpClientConfig, version)`:单调版本(过期/乱序拒绝、同版同容 no-op)+ 先校验后原子应用(非法→旧配置旧连接不变)+ 端点变化(旧在途 `Connection` + 新代际立即尝试不等退避)vs 仅策略变化(不打断当前尝试/退避、下次用新);配置版本与连接代际两轴独立(RT_TCP_RECONFIG 全,#59)。
- **验证** 真实单机 TCP 断连-重连回环、代际隔离(旧代际迟到事件不污染新代际)、重连退避时序、重配置端点切换、Close 端到端收敛(#60);全量 **152→180 全绿**,e2e/reconnect/reconfig 用例连跑无 flake。
- **文档** ADR-0003 增 **D11**(P3 连接管理行为契约);SDD §4 P3 标记交付、§6 回填 P3 精确签名、**新增 §7 需求追溯矩阵(双向)**;README「当前状态」更新至 P0–P3 已交付;seed #20 关闭。

## [0.4.2] - 2026-07-28

> **P2 节点加厚里程碑:入站处理器 + 有界队列 + 生命周期硬化**——在 0.4.1 请求-响应基座上,`ProtocolNode` 加厚为可处理入站业务、支持背压与并发在途的完整交互节点。全量测试 119→152 全绿;详见 SDD §4 P2、#47–#50。

### 里程碑:P2 节点加厚 —— 入站处理器 + 有界队列 + 生命周期(协程原生)— 2026-07(路线图 P2)
> 按 SDD §4 P2 把 `ProtocolNode` 从纯客户端加厚为双向交互节点。遵 ADR-0003 D10 结构纪律:协议无关机制建成可复用件、协议特有语义内联。
- **新增** `BoundedQueue<T>` —— **协议无关**有界业务队列薄件(双上界 事件+字节,可配 1–65536 / 64KiB–256MiB;tail-drop 拒最新 + 命名丢弃计数;注入 `byte_size_of` 保持 `T` 不透明;fiber 唤醒消费)。供 P4 `DdsNode` 复用(RT_HANDLER 3.1.5.4 / ADR-0002 D5 / ADR-0003 D10,#47)。
- **新增** 入站业务处理器 —— 组合注册 `InboundHandler = Status(const Message&, HandlerContext&)`(不继承);**单消费者 fiber 严格串行**(出队跑完再下一条,含 await);`HandlerContext` 露 `Send`/`RequestClose`/`cancellation()`;handler 逃逸异常经边界 `try/catch(...)` → `kInternal` 隔离当前事件、不自关 node(RT_HANDLER 全 / RT_ERROR_001,#49)。
- **新增** `ProtocolNode::Send(Message)` —— `noresponse` fire-and-forget(分配 session_id 盖帧但不登记 PendingTable、不占 256 在途预算;遵 RT_TRANSPORT_008 背压,#49)。
- **新增** 256 并发在途上限 + `session_id` **空闲集 LRU 退休窗口**(=线缆 uint8 硬顶,满→`kResourceExhausted`;绝对防误配随 P3 代际到位);`PendingTable` 加协议无关可选 `max_pending`(RT_REQUEST_005/006,#48)。
- **新增** 生命周期硬化到 RT_LIFECYCLE 全:并发幂等 Start(共享结果不重复 spawn)、幂等 Close、多等待者 WaitClosed、**三方汇合**(读循环+handler+FailAll,独立 finalizer fiber)、配置校验停 Created、协作取消 + `close_drop` 归因、**重入自锁防护**(比对 `boost::this_fiber::get_id()`,handler 内发起 Close 不自锁,#50)。
- **验证** Fake 上 256 并发在途 + 乱序恰好一次 + 队列溢出 tail-drop + 关闭协作取消收敛;**真实 TCP 回环冒烟**(handler 收业务帧 + `ctx.Send` 回帧 + 优雅 Close 端到端收敛)。全量 **119→152 全绿**。
- **文档** ADR-0003 增 **D10**(P2 结构纪律:协议无关机制可复用、协议特有内联,守 RT_NODE_003/RT_DESIGN_008);SDD §4 P2 标记交付、§6 回填 P2 精确签名。

## [0.4.1] - 2026-07-28

> **P1 首纵切片里程碑:TCP 上最小请求-响应**——在 0.4.0 骨架上交付协议无关 `PendingTable` 薄基座 + 最小 `transport::ProtocolNode`(needresponse 请求-响应 + 读-分发循环)+ `TcpTransport` 读侧契约,真实单机 TCP 回环端到端验证,**证实"无共享引擎、语义内联各 node"这一最大架构赌注**(RT_DESIGN_003)。全量测试 95→119 全绿;详见 SDD §4 P1、#34–#37。

### 里程碑:P1 首纵切片 —— TCP 上最小请求-响应（协程原生）— 2026-07(路线图 P1)
> 按 SDD `docs/设计说明书-协程原生.md` §4 P1 打通 TCP 上最小 request-response,**证实"无共享引擎、语义内联各 node"这一最大架构赌注**(RT_DESIGN_003)。纵向薄切片:读侧契约 → PendingTable 薄基座 → 最小 ProtocolNode → 真实 TCP 回环端到端。全量测试 **95→119 全绿**。
- **新增** `PendingTable<Key,T>` 挂起-应答**协议无关薄基座**(唯一登记 / 恰好一次完成 / `FailAll`+closed latch / 取消纪律;四方仲裁在 `Handle::Wait`,每 entry 复用 `SharedCompletion` 原子首胜;RT_REQUEST/RT_IN_INTERFACE_004,#35)。
- **新增** 最小 `transport::ProtocolNode`:组合 `ITransport`+`ICodec`+`PendingTable`,内联读-分发循环,交付 needresponse `Request(Message)→Result<Message>` + 基础生命周期(`Start`/`Close`/`WaitClosed`);协议特有语义(键派生 / `frm_type` 盖章 / `session_id` 滚动 / 终结判别 / 未匹配路由)全部内联,PendingTable 保持协议无关(RT_NODE_003 / ADR-0003 D9,#36)。
- **新增** `CorrelationKeyStrategy` + `DefaultProtocolKeyStrategy(response_marker)`:**node 级可注入 KeyOf**(只开放 KeyOf,`IsTerminal`/`RouteUnmatched` 内联锁死,不塌回 InteractionPolicy);`ProtocolKey=(session_id<<16)|(cmd & ~0x1000)` 归一化(占位 marker 可注入,ADR-0003 D9)。
- **补完** `TcpTransport` 读侧契约(遗留项 A):`Read` 返回 `Datagram.source` 填对端 `Endpoint::Net(ip,port)`;新增读侧契约测试(source=对端、断连→`Connection`、`RequestClose`→`Closed`、超时→`Timeout`、单读者→`InvalidState`)(#34)。
- **验证** 真实单机 TCP 回环端到端(真 `ProtocolNode` 请求方 + 裸 `SystemCodec` echo 对端):一次 needresponse 恰好一次完成 + 关联清理 + 乱序/迟到响应丢弃观测 + source=对端 + Close 端到端收敛(#37)。
- **文档** ADR-0003 增 **D8**(P1 节点交互状态串行化=锁,非 strand/affinity)、**D9**(自定义 key 红线 + `ProtocolKey` 归一化算法);SRS 增 **RT_DESIGN_008**(协议可扩展性:新协议复用协议无关基座 + 内联/节点级注入协议语义,不塌回共享 policy);SDD §4 P1 标记交付、§6 回填 P1 精确签名与执行域绑定落点。

## [0.4.0] - 2026-07-27

> **协程原生目标架构的清洁重建里程碑**(见 `docs/adr/0001-*`、`docs/adr/0002-*`、`docs/adr/0003-*`、`docs/需求规格说明书-协程原生.md`)。0.3.0 as-built → 0.4.0 目标架构重建起点:**功能上是骨架** —— 目标传输发送/读语义(`transport::TcpTransport`)+ 抢救的 codec/DDS provider + 统一 `TransportErrc`;节点、其余介质、连接管理、观测等按 SDD 路线图 P1–P6 逐期恢复。as-built(0.3.0 异步栈 + 第二期 `coro::InteractionEngine`)已从 master 删除,完整实现存档于 git tag `v0.3.0`。

### 清理:P0 清洁重建 —— 目标骨架落位（协程原生）— 2026-07(路线图 P0)
> 按 SDD 路线图 `docs/设计说明书-协程原生.md` §4 P0 执行清洁重建：删除 as-built 交互引擎 / 回调式传输 / 旧文档,保留并抢救目标件,提升命名空间,合并单一 CMake 目标。as-built 完整实现存档于 tag `v0.3.0`。
- **⚠️ 移除** as-built 交互层(`comm/` 引擎/执行器/策略、第二期 `coro::InteractionEngine`/`ProtocolNode`)、回调式 `ITransport` 及其五介质外壳、as-built SRS/SDD/架构文档与旧示例;均存档于 tag `v0.3.0`。
- **变更** 统一错误模型为机器可判别的 **`TransportErrc`** 错误类别(`InvalidArgument`/`InvalidState`/`Connection`/`Closed`/`Timeout`/`Cancelled`/`Io`/`Frame`/`Codec`/`ResourceExhausted`/`Unsupported`/`Internal` 等),取代 as-built 的字符串前缀分类(RT_ERROR_002/003)。
- **变更** 命名空间由 `transport::coro::*` 提升到 `transport::` 顶层(`transport::TcpTransport`/`ITransport`/`Result`/`TransportErrc`;codec→`transport::codec`、DDS provider→`transport::dds`);头/源/测试从 `*/coro/*` 移到顶层。
- **抢救沿用** 线缆 codec 逻辑(`SystemCodec`/`LengthFieldCodec` 等帧布局/流式扫描/重同步/`CrcFn`)与 DDS provider 适配(`IDdsProvider`/Fake/可选 FastDDS),移植到目标 `ICodec`/`Message`。
- **构建** CMake 由 `transport`+`transport_coro` 双库合并为**单一 `transport` 库**(直接链接强制依赖 `asynctask` + Qt + 可选 fastrtps);测试目标 `transport_tests`+`transport_coro_tests` 合并为单一可执行文件(在 AsyncTask fiber 调度器内跑全部 `tests/*`)。**移除 `TRANSPORT_BUILD_CORO` 选项**(AsyncTask 现为强制运行时 —— RT_DESIGN_002);保留子模块未初始化的 `FATAL_ERROR` 守卫改为无条件。
- **文档** SRS 引用表 DOC-2/DOC-3 改指 tag `v0.3.0`(as-built 存档);`README.md` 整篇按当前目标架构现实重写;`CHANGELOG.md` 权威参考指针改指协程原生 SRS/SDD。
- **验证** 单目标干净重建零告警;全量测试 95/95 通过(含 #19 发送语义、codec、DDS provider 契约),ctest 1/1。

### 文档:协程原生 SRS v2→v3 —— grilling 收敛 + ADR-0002 — 2026-07-22
> 经 `/grill-with-docs` 逐条盘问,把 SRS v2 的多处可验收性缺口收敛为可验证需求,关闭 TBD-002 与 TBD-006;决策记入新建 ADR-0002 并回指 SRS 需求号。
- **新增** `RT_TRANSPORT_007` 定义并发发送排序 = **节点执行域到达顺序**(单 fiber 程序序保持;跨 fiber 取得某个一致全序,但不保证可由调用方墙钟时序预测),**关闭 TBD-002**;`RT_TRANSPORT_008` 定义**发送完成 = 帧字节全部离开框架用户态发送缓冲(进内核)+ 协程背压**,否决 fire-and-forget(经 corosocket 代码核对:用现有 `write()`+`waitForBytesWritten()` 循环可实现,不改 AsyncTask、不暴露 corosocket 给用户)。
- **新增** `RT_NODE_006`(无连接介质判活归协议/交互层,transport 只暴露最近收发时间戳/计数/单操作错误等 I/O 事实)、`RT_NODE_007`(DDS provider 交接有界队列 + 满时丢最新 tail-drop,默认 1024 样本/16 MiB 可配),**关闭 TBD-006 的 DDS 部分**;`RT_LIFECYCLE_002` 补「连接状态是 TCP 客户端在 `Running` 内的子状态,非对等第二生命周期;非重连节点致命错误→`Closing→Closed`,无 `Reconnecting`」。
- **变更** 业务队列字节上限可配范围定为 **64 KiB~256 MiB**(默认 16 MiB)+ 聚合内存(Σ 业务队列+DDS 交接)归宿主保证落在 RSS 预算,**关闭 TBD-006 剩余部分**;3.6.2 把「框架导致丢失/重复 = 0」改为**容量内可验证判据 + 逐因归因**(每次本地丢弃归因到恰好一个命名计数原因,Σ 命名原因计数 = 总丢弃,无静默丢失;迟到/重复/坏帧属正确过滤而非丢失),RT_DATA_BUFFER/RT_TESTABILITY 同步。
- **新增** `docs/adr/0002-send-completion-drop-attribution-and-lifecycle-refinements.md`(决策 D1–D6 + corosocket 可行性核对 F1);`docs/adr/0001-*` 的「尚未解决」标注 DDS 交接容量/溢出、并发发送排序两项已由 ADR-0002 关闭;`CONTEXT.md` 增术语 **发送完成语义 / 发送排序 / 丢弃归因**。
- **说明** 本节为**目标需求**收敛,不含实现;SRS 版本 v2→v3。

### 地基:协程传输契约 + 协作取消 + 结构化错误 + 共享完成原语 — 2026-07-21 ~ 07-22(PR #17)
> 协程原生传输层的地基件,尚未接入节点。AsyncTask 以 `third_party/AsyncTask` **git 子模块**纳入并补充 socket awaitable(新增 `CoroUdpSocket::receiveDatagram()` 保留 datagram 边界与地址 metadata 等)。
- **新增** 协程传输契约(`ITransport` 协程 await 式定义 + 契约消歧)、**结构化传输错误**、**协作取消**(含取消竞态加固)、**共享完成原语 `SharedCompletion`**(支持 `void`、值拷贝前释放完成锁)。
- **新增** 确定性 **Fake 传输**测试替身 + fake 读超时仲裁(用 AsyncTask `await_for`)。
- **构建/杂项** AsyncTask 转 git 子模块并更新 socket awaitables;忽略本地 worktree 与 subagent 进度账本。
- **设计** `docs/` 增协程传输地基的 plan/design 文档。

### 文档:协程原生架构 SRS(GJB 精简模板)+ agent-skills 配置 — 2026-07-17 ~ 07-21(PR #15/#16)
- **新增** 目标架构 SRS `docs/需求规格说明书-协程原生.md`(GJB 精简模板,标识前缀 `RT`);ADR-0001(协程原生交互架构,多轮 grilling 决策 D1–D21 + 事实修正)。
- **新增** agent-skills 配置:issue tracker(GitHub / `gh`)与 domain docs(single-context)约定,落 `docs/agents/`、`CLAUDE.md`/`AGENTS.md` 的 `## Agent skills`。
- **变更** SRS v1→v2「完善协程原生需求规格」;`CONTEXT.md` 术语表标注 [as-built]/[target]。

## [0.3.0] - 2026-07-09

> **⚠️ 破坏性次版本 —— 传输层从 standalone Asio 迁至 QtNetwork(去 asio、需 Qt5);并新增协程原生交互引擎(第二期)。** 与 0.2.x **不构建兼容**:整仓构建从此需 Qt5,所有传输不再自持 io 线程 → 用它们的宿主须运行 Qt 事件循环。

### ⚠️ 特性:UDP/TCP/串口传输迁移至 QtNetwork(去 asio)— 2026-07-09(PR #12)
> `ITransport` 接口与回调契约**不变**;仅把 5 个具体传输的底层实现从 standalone Asio 换成 QtNetwork,**整仓移除 asio**。为第二期协程原生引擎(跑在 Qt 事件循环线程)铺路。
- **变更** `UdpTransport`→`QUdpSocket`、`TcpConnection`→`QTcpSocket`、`TcpServerTransport`→`QTcpServer`、`TcpClientTransport`→`QTcpSocket`+`QTimer`(连接超时/指数退避重连)、`SerialTransport`→`QSerialPort`。用 functor `connect`(lambda 捕获 `this`),传输类**不加 `Q_OBJECT`**(无需 moc)。
- **⚠️ 线程模型变更** 传输**不再自持 io 线程/strand**;活在**宿主 Qt 事件循环线程**上,`OnBytes`/`OnConnect`/`OnDisconnect` 经 Qt 信号在该线程串行触发。宿主须运行 Qt 事件循环(测试用 `QCoreApplication`+事件泵)。报文/流式语义照旧(UDP 一 datagram 一回调;TCP/串口一次 read 切片一回调),断连语义照旧(主动 `Close` 不报 `OnDisconnect`,真断报一次)。
- **⚠️ 移除** vendored `third_party/asio`;CMake 删 `asio_standalone`(保留 `Threads::Threads`,`ThreadExecutor` 仍用)。
- **构建** `find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)`;`transport` 库链接 `Qt5::Core/Network/SerialPort`。前置依赖:`libqt5serialport5-dev`(串口编译前提)+ `socat`(串口回环测试)。测试改自定义 `QCoreApplication` main + 事件泵助手 `qtutil::pumpUntil`。
- **DDS 不动** 异步 `InteractionEngine`/`IExecutor`/`ThreadExecutor`/`DdsNode`/`DdsTransport`/`DdsCodec` 全保留(DDS 不走 QtNetwork)。
- **验证** 全新构建零告警,118/118(TCP 用例合并后总数微调);串口 socat pty 回环实跑通过。终审 1 Important(客户端 `connect_timer_` 每次重连累积 timeout slot → 退避被乘性推进)+ 3 Minor(再 Open 安全 / UDP 错误经 `OnBytes(Fail)` 上报契约 / 广播冒烟)已修。

### 特性:协程原生交互引擎(`coro::InteractionEngine` + `coro::ProtocolNode`)— 2026-07-09(PR #13)
> 交互层协程原生化第二期(核心)。把请求-应答从异步状态机变成线性的 `send(); r = await_for(timeout);`。基于 **AsyncTask**(boost.fiber,不加修改),复用 QtNetwork `ITransport`。**纯加性**,异步栈/`ITransport`/第一期传输不动。
- **新增** `transport::coro::InteractionEngine`(`include/transport/coro/`,通用机制,协议差异仍外包给 `InteractionPolicy`):`Request(out, tag, timeout, to)→Result<Message>`(`NewCorrelation`→按 `key_fn_(out)` 登记 `Coro::Awaitable`→`Send`→`await_for`,仅挂起**当前 fiber**,消除挂起表/ScheduleAt/Cancel/OnTimeout/IExecutor)、`Fire`(单向)、demux(`OnBytes`→`Decode`→`KeyOf`→命中且终结则唤醒 fiber)、`OnInboundDeliver`、**可插拔关联键 `SetKeyFn`**(默认逐字 `(session_id, message_id)`)。`conn:` vs `timeout:` 由 `channel->is_closed()` 区分。
- **新增** `transport::coro::ProtocolNode`:薄壳,`Start`/`Stop`/`Request(cmd, payload, timeout)`。
- **新增** `InteractionPolicy::IsTerminal(FrameType)`(非纯虚,默认 `== kResult`;异步引擎从不调用它 → 纯加性)。
- **线程模型** 宿主 `Coro::installFiberApplication()`+`exec()`,fiber 调度器**同时推进 fiber 与 Qt 事件**;传输 `OnBytes`、demux、等待 fiber 全在这条线程,协作式无锁。`Request` **须在 fiber(固定于传输 I/O 线程)内调用**。
- **构建** 新增 `option(TRANSPORT_BUILD_CORO)`(默认 ON):`asynctask` 静态库(AsyncTask 编译单元 + `ASYNC_HAS_QTCORE` + AUTOMOC)+ 已编译 boost `fiber/context/thread/chrono`;新 `transport_coro` 库 + `transport_coro_tests`。缓存变量 `ASYNCTASK_DIR`/`BOOST_LOCAL_ROOT`(当前为机器绝对路径,可 `-D` 覆盖;后续改可移植发现)。
- **验证** 119/119 零告警;含真 UDP 回环冒烟(协程节点经两个真 `UdpTransport` 线性 `Request` 拿应答 ~2ms,验证 fiber↔Qt↔UDP 端到端)。终审 Ready-to-merge,2 Important(独立/复用引擎场景潜在隐患,当前无在用调用触达:`Close` 摘传输回调防 UAF、`Request` fiber 亲和注记)+ 1 Minor 已修。
- **待办(本期外)** 服务端 responder/`SendReply`、周期 `StartPeriodic`、心跳、ack;DDS 搬到协程引擎(listener→fiber 线程桥);构建路径可移植化。

## [0.2.1] - 2026-06-29

### 特性:周期发送取最新状态(消息工厂 + 推送更新)— 2026-06-29(PR #10)
> 周期发送此前把 payload 冻结在启动时刻;现支持每帧**发送前取最新状态**。纯加性,固定/心跳路径逐字不变。
- **变更** 引擎 `Periodic` 载体由固定 `Message` 改为 `std::function<Message()>` 工厂;`FirePeriodic` 每拍**锁外**调 `make()` 取最新。`StartPeriodic` 加工厂重载;固定版包装、行为不变;新增 `UpdatePeriodic(handle, Message)`(推送换值)。空工厂 → 返回 0(不崩)。
- **新增** `ProtocolNode::StartRepeating(cmd, state_fn, …)`(拉)+ `UpdateRepeating(handle, cmd, payload)`(推)。
- **新增** `DdsNode` 周期发布(此前无):`StartPublishing`(固定/`sample_fn` 工厂)+ `UpdatePublishing`/`StopPublishing`(tag `kNotify`)。
- **契约** `state_fn`/`sample_fn`/`make` 在 executor 线程、每拍发送前调用,须线程安全、非阻塞、不抛;`Update*` 下一拍生效、会把 pull 周期转为固定。
- **示例** `protocol_node_demo` 周期段改用 `state_fn` 演示每拍拉最新温度。
- **验证** 119/119,零告警;`-Wall -Wextra` 清洁。

- **示例** `examples/dds_node_demo.cpp`:`DdsNode` 完整 demo —— 进程内 DDS 总线上发布-订阅扇出 + 多路请求-应答(reply_to 各回各家 inbox)+ 反馈/终结。

## [0.2.0] - 2026-06-29

> **0.2.0 开发线 —— 三层架构(Transport 纯管道 / ICodec 线缆格式 / Comm 交互层)。** 自 2026-06-15 的两层解耦起,逐 PR 自底向上补齐:TCP 服务端 → DDS 底层 → CommNode 交互层 → DdsNode → 外部协议栈 ProtocolNode → InteractionEngine 抽象 → ITraceSink 可观测 → 外部协议 UDP 1:多。`v0.1.0` 标签保留完整旧实现(`TransportCore`/三模式接收/`TransportFactory`/`RawMessage` 等)备查;0.2.0 与 0.1.0 **不 API 兼容**。

### 特性:外部协议跑 UDP(`SystemDatagramCodec` + 1:多寻址)— 2026-06-26(PR #9)
> 外部协议(`SystemCodec` 帧)此前只配 TCP/串口(有状态流式切帧)。让它正确跑 UDP 1:多——UDP 是报文边界语义,有状态滚动缓冲在多对端会串台、半截报文污染下一报文。**纯加性,默认值使 TCP/串口/流式路径逐字不变。**
- **新增** `SystemDatagramCodec`(header-only,无状态报文版外部帧):每 `Decode` 只解一个 datagram、残留丢弃、不跨报文(多对端不串台);与流式 `SystemCodec` **共用帧核** `EncodeSystemFrame`/`ScanSystemFrames`(抽出、不复制),编码字节一致。`SystemCodec` 重构为调用共享核,行为不变。
- **新增** `ProtocolConfig.reply_to_source`(默认 false):置 true 时 `ProtocolPolicy.ReplyTo` 经 `std::from_chars` 解析入站 `Message.source`(按最后一个 `:` 切,兼容 IPv4 与 `::1`)→ `Endpoint::Net`;**一个开关覆盖服务端 `Responder` 回应 + 客户端 `needfeedback` 自动 ack**(都回到入站来源)。
- **变更** `ProtocolNode` 5 个发送方法加可选尾参 `const Endpoint& to = Default()`(转发引擎原语):客户端经一个 UDP socket 向多设备分别发,重发回同一 `Pending.to`。默认 `Default` → 旧调用点/TCP/串口零改动。
- **示例** `examples/protocol_udp_demo.cpp`:1 控制器对 2 设备的 UDP 1:多 req-resp。
- **验证** 干净构建零告警,111/111(新增 `system_datagram_codec_test` 6 + `protocol_policy_test` 4 + 发送目的地透传 1);`system_codec_test` 不变即证明共享核重构行为保持。

### 特性:可插拔结构化 Trace(`ITraceSink`)— 2026-06-26(PR #8)
> 此前全库零日志,唯一可观测点是引擎的 `on_error_(string)`。给交互层加一套**观测-only、近零开销**的 trace,让分发/超时/重发/auto-ack/periodic/收发/关闭全程可见。
- **新增** `ITraceSink`(层中立 `include/transport/ITraceSink.hpp`,header-only):`TraceLevel{kTrace,kDebug,kInfo,kWarn,kError}` + 零分配 `TraceEvent`(`string_view`+`int` 视图)+ 两个内置 sink `OstreamTraceSink`(→ ostream,默认 stderr)/`CapturingTraceSink`(深拷贝,测试用)。
- **新增** `InteractionEngine::SetTrace`(Open 前注入)+ 全咽喉点埋点:`open`/`close`/`conn`/`send`/`request`/`reply`/`recv`/`decode`(成功=条数 / `decode-fail`)/`dispatch`(match-terminal·match-intermediate·auto-ack)/`unmatched`/`retransmit`/`timeout`/`periodic`(start·fire·stop)/`error`。
- **新增** `DdsNode`/`ProtocolNode` 透传 `SetTrace`。
- **观测-only**:无 sink 时单分支、字节流逐字不变;所有 trace 在 `mu_` 外发(不在锁内回调用户 sink → 杜绝慢 sink 阻塞/重入死锁);sink 须线程安全。
- **预留**:`ITraceSink` 层中立 + 保留 category(`resync`/`frame-drop`/`io`/`reconnect`),后续 codec/transport 加可选 sink 参数即接,接口零改。
- **验证** 干净构建零告警,100/100(新增 trace_sink + interaction_trace + 节点透传);评审抓出并修掉「StopPeriodic 锁内回调 sink」与「缺 spec §5 decode 成功事件」。

### 重构:交互机制抽象(`InteractionEngine` + `InteractionPolicy`)— 2026-06-24(PR #7)
> `CommNode` 与 `ProtocolNode` 曾各自重写同一套交互机制(且各被评审抓出过一个并发 Critical)。三个真实交互节点 = 足够样本,合并机制为一份、最难写对的并发生命周期只写一处。**纯内部重构,DDS/协议行为保持。**
- **新增** `InteractionEngine`(通用机制一份):3 原语 `Fire`(单向)/ `RequestAwait`(发+等回应,带超时/重发/自动 ack)/ `StartPeriodic`(定时发);挂起表、超时、重发、periodic 定时器、io→Decode→Post→单 worker Dispatch、并发纪律(weak_ptr / `Encode`+`Send` 锁外 / 中间帧拷贝调用·终结帧移出调用防双触发 / `Close` 先终结挂起再停定时器再停执行器再关传输 / 恰好一次)全部集中此处。
- **新增** `InteractionPolicy`(声明式协议策略,7 法 `TagOf`/`SetTag`/`NewCorrelation`/`KeyOf`/`EchoCorrelation`/`ReplyTo`/`RouteUnmatched`):匹配键 `Key`=`std::string`、判别符 `FrameTag`=`int`,**引擎只做相等比较、从不解释**(零 per-protocol 泄漏)。`RequestSpec` 配置 request-await;**「首帧停重发」一条规则**(首个配上的中间/终结帧停重发)覆盖各模式,免协议专属 flag。实现 `ProtocolPolicy`(`Key`=(session,message)、`session_id` 滚动、`message_id`=命令码)、`DdsPolicy`(`Key`=correlation_id、`reply_to`=inbox 路由)。
- **变更** `DdsNode` 重做成 `InteractionEngine`+`DdsPolicy` 薄壳(**不再继承 `CommNode`**);`dds_node_test` 仅去 CommNode 依赖、断言不变。
- **变更** `ProtocolNode` 重做成 `InteractionEngine`+`ProtocolPolicy` 薄壳;发送方法**新增首参 `uint16_t cmd`(=`message_id` 命令码)**(纠正旧实现 `message_id` 自增的语义);心跳改为首拍即发。
- **⚠️ 移除** 通用 corr+kind 的 `CommNode`(其机制即引擎,不被实用);`comm_node_test` 转为引擎级 `interaction_engine_test`(覆盖原通用交互面 + worker 内 Close 自连接守卫)。
- **验证** 干净构建零告警,86/86 测试两次连跑稳定;引擎/策略代码零 `frm_type`/`kind`/节点名(缝切对的护栏)。

### 特性:外部协议栈(`SystemCodec` 协议帧 + `ProtocolNode`)— 2026-06-23(PR #6)
> 面向某外部系统的具体通信协议。`CommNode`/`DdsNode`/`DdsCodec` 不动(DDS 不走此协议)。
- **变更** `SystemCodec` 由通用线缆格式**改造为外部协议帧 codec**:`[head_flag:4=AA BB CC DD][frm_type:1][protocol_id:1][session_id:1][reserve:4=0][crc:2 LE][frm_len:2 LE][frm_body: message_id:2 LE | payload]`,小端、有状态流式解码 + 坏帧 **resync**(同步头/CRC 不符 → 前移 1 字节重扫)。**CRC 经 `CrcFn` 注入**(默认占位 `DefaultCrc16`);`frm_type` 枚举占位值 —— 真实外部字节值/算法实现前替换。
- **新增** `Message` 协议字段 `frm_type`/`protocol_id`/`session_id`/`message_id`(加性,通用字段不变)。
- **新增** `ProtocolNode`(独立节点,复用 `ITransport`+`ICodec`+`IExecutor`):5 种发送交互模式 `noresponse` / `needresponse`(等 RESPONSE)/ `withfeedback`(等 RESULT)/ `needfeedback`(RESPONSE→RESULT→自动回 RESPONSE ack)/ `repeating`(定时发 STATE,可停)+ 周期心跳 + 收发双角色(`OnCommand` + `Responder.Response()/Result()`);匹配键 **(session_id, message_id)**;超时**重发 ≤3** 后失败。
- **变更** `SystemCodec` 改为协议专用后,`comm_node_test`/`combination_smoke_test` 改用 `DdsCodec`(仍覆盖 CommNode 通用 req-resp)。
- **验证** 干净构建零告警,86/86 测试两次连跑稳定(含确定性 `InlineExecutor` 与真实 `ThreadExecutor` 双跑)。

### 特性:`DdsNode`(DDS pub-sub + 多路 req-resp)— 2026-06-17(PR #5)
- **新增** `DdsNode : CommNode`,复用交互引擎,加性补 DDS 订阅能力 + 基于 `reply_to` topic 的精确应答路由:多 topic 发布(即 `Send(Endpoint::Topic)`)/ 订阅(`Subscribe`/`Unsubscribe`)/ 多路请求-应答(请求带自身 inbox topic 入 `reply_to`,服务端 `Responder.Reply` 据此精确回送,`correlation_id` 配对)。
- **新增** `DdsCodec`(无状态、带交互元数据 `kind`/`correlation_id`/`reply_to` 的 DDS 线缆格式;DDS 每 sample 即一条完整消息,无滚动缓冲 → 多 topic 并发解码安全);`Message.reply_to` 字段。
- **变更** `CommNode` 加性泛化(`Request` 收可选目的 `Endpoint`、出站填 `reply_to`、`Responder` 按 `reply_to` 回送、入站 `topic` 缺省取来源);p2p 行为不变。

### 特性:`CommNode` 交互层(`IExecutor` + 交互模式基类)— 2026-06-17(PR #4)
- **新增** `IExecutor` 执行器缝(`Post`/`ScheduleAt`/`Cancel`/`Start`/`Stop`)+ 内置 `ThreadExecutor`(1 worker 线程 + 有界队列背压 + 最小堆定时器);为未来自研协程 `CoroExecutor` 预留可换线程模型。
- **新增** `CommNode`(用户继承的交互模式基类,复用任一 `ITransport`):单向 `Send`、请求-应答 `Request`(回调 / `std::future`)、请求-结果反馈、服务端 `OnRequest`+`Responder`;io 线程内联 `Decode` → `executor.Post` → 单 worker 串行 `Dispatch`(按 `kind`),背压在队列;请求超时 / 断连终结挂起 / 恰好一次。
- **新增** `Message` 交互元数据补 `reply_to`;错误前缀新增 `timeout:`(请求超时)。

### 特性:DDS 底层(`IDdsTransport` + `DdsTransport` + provider 抽象)— 2026-06-16(PR #3)
- **新增** `IDdsTransport : ITransport`(加 `Subscribe`/`Unsubscribe`);`DdsTransport` 持有 `IDdsProvider`:`Send(bytes, Endpoint::Topic)` → publish,`Subscribe(topic)` → 样本到达直接在 provider listener 线程调 `OnBytes(bytes, from=topic)`(同 topic 有序、跨 topic 并发)。
- **新增** provider 抽象 `IDdsProvider` + `DdsProviderRegistry`(按名)+ `FakeDdsProvider`(进程内内存 topic 总线,DI 共享 `Bus`,DDS 业务零 FastDDS 依赖即可全测)+ 可选 `FastDdsProvider`(Fast DDS 2.13,手写 `FastDdsRawType` 绕过 IDL/CDR)。
- **变更** `DdsConfig` 精简为 `{domain_id, default_topic, provider, qos}`;`DdsTransport` 是**纯字节管道**(req-resp/关联/超时移交 `DdsNode`,不再在传输层)。

### 特性:TCP 服务端 `TcpServerTransport` — 2026-06-16(PR #2)
- **新增** `TcpServerTransport`(独立接受器,非 `ITransport`):监听 `bind_addr:port`,每 accept 造一个 `TcpConnection`(实现 `ITransport`,client/accepted 共用),经 `OnAccept(shared_ptr<ITransport>)` 交付给用户在每连接上独立收发;`OnError` 通知接受错误;`LocalPort()` 取回 OS 分配端口。
- **设计** 服务端不再继承 `ITransport`/不广播(广播由用户在各连接 transport 上自行实现);与旧 `ITcpServer` 的「服务端即 transport」模型解耦。

### ⚠️ 重大重构:底层分层(Transport + ICodec 两层解耦)— 2026-06-15
> 面向 0.2.0 的破坏性重构第一阶段(PR #1)。把臃肿的富 `ITransport` 拆成两个**解耦的底层层**;上层 `System` 交互模式基类、TCP 服务端、DDS 留作后续阶段(设计见 `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`)。`v0.1.0` 标签保留完整旧实现备查。

- **新增** `ITransport` 纯字节管道:仅 `Open/Close/IsOpen` + `Send(bytes)` / `Send(bytes, Endpoint)` + `OnBytes` / `OnConnect` / `OnDisconnect`,不再涉及 Message/codec/分帧/topic。实现 `UdpTransport`、`TcpClientTransport`(合并旧 client+connection 为单类)、`SerialTransport`。
- **新增** `ICodec` 线缆格式(分帧 + 序列化 + 交互元数据,有状态):`Encode(Message) → bytes` / `Decode(bytes) → 0..N Message`。实现 `SystemCodec`(默认线缆格式,承载 `kind`+`correlation_id`+`topic`+`payload`)、`LengthFieldCodec`(吸收旧 `LengthFieldFramer`+`FrameAssembler`)、`DatagramCodec`(报文直通,header-only)。
- **新增** `Message` 交互元数据:`MessageKind{kOneway,kRequest,kReply,kFeedback,kNotify}` + `correlation_id`,为后续 `System` 的请求-应答/请求-结果反馈/订阅预置。
- **⚠️ 破坏性移除** 旧 `TransportCore`、`IFramer`/`LengthFieldFramer`/`FrameAssembler`、`TopicEnvelope`/`StreamSend`、`ReceiveQueue` 与三模式接收(`Receive`/`OnReceive`/`AsyncReceive`)、topic 路由、`TcpConnectionImpl`/`TcpServerImpl`/`ITcpServer`、整个 DDS(`DdsImpl`/`IDdsProvider`/`FastDdsProvider` 等)、`TransportFactory`。`ICodec` 由无状态 `bytes↔bytes` 改为有状态 `Message↔bytes`。
- **说明** 本次移除使上一条「健壮性优化」中的 `DdsImpl::Send` 空 `topics` 防护、`TransportFactory::ParseSubFramer` 随相关文件一并删除(对当前代码不再适用);`Result<T>`/`Status` 的 `[[nodiscard]]` 保留。
- **解耦** Transport 不依赖 `Message`/`ICodec`,`ICodec` 不依赖任何 transport(唯一交叉点是组合冒烟测试)。
- **验证** 干净构建零告警,27/27 测试通过(TDD,11 commits;含 UDP/TCP 回环、串口 openpty、两层组合冒烟)。

### 健壮性与可维护性优化 — 2026-06-15
- **变更** `Result<T>`/`Status` 标 `[[nodiscard]]`：框架不抛异常、靠返回值传错，忽略 `Open`/`Send` 等返回值改为**编译期告警**；同步清理由此暴露的全部静默丢错（生产代码有意忽略处显式 `(void)` 标注，测试中期望成功的调用升级为 `ASSERT_TRUE` 断言）。
- **修复** `DdsImpl::Send(data)` 增加空 `topics` 前置校验（返回 `config: no topic configured`），堵住先于 `Open` 调用导致 `topics[0]` 越界的未定义行为。
- **重构** `TransportFactory` 中 `tcp_client`/`tcp_server`/`serial` 三处重复的 framer 子解析合并为 `ParseSubFramer` 辅助函数。
- **验证** 干净构建零告警，150/150 测试通过。

## [0.1.0] - 2026-06-14

**首个发布版本**，聚合下列全部里程碑。`transport::LibraryVersion()` 返回 `"0.1.0"`，对应 git 标签 `v0.1.0`。本节内各里程碑的 **⚠️ 破坏性** 均为 0.1.0 发布**之前**的开发期内部变更（彼时尚无已发布版本可破坏）；自 0.1.0 起，破坏性变更将触发主/次版本号递增。

里程碑按倒序排列（最新在前）。

### 文档整理 — 2026-06-14
- **新增** 需求规格说明书（SRS）`docs/需求规格说明书.md` 与 设计说明书（SDD）`docs/设计说明书.md`：基于当前实现，把分散的逐特性 spec 汇总为两份对外权威参考（需求 vs 设计）；新增 `CHANGELOG.md`。
- **新增** README「关键约束」小节，8 条核心约束反向链接 SRS/SDD。
- **变更** as-built 架构文档升级为总结版：§3 运行期协作扩为覆盖**四种传输的收发**（发送统一 / 流式接收 / 报文接收 / DDS+req-resp）；类图同步至最新（`Send(Message)`、删 `IDdsTransport::Send(data,topic)`、`TransportCore` 真实成员名、补 `StreamSend`）。

### 重构：流式发送决策去重 — 2026-06-14
- **变更** 抽出 header-only 自由函数 `BuildStreamFrame(core, routing, payload, topic)`（`core/StreamSend.hpp`），统一 TCP 与串口完全相同的「routing 分支 + `TopicFitsEnvelope` 守卫 + 按 topic 编码 + `FrameStream`」决策；各传输只保留自身 open 检查 + 入队。行为保持，149 测试全绿。

### 特性：Topic 路由编解码多路复用 — 2026-06-12 ~ 06-14
一条连接上承载多种帧格式，按 `topic` 选 codec 编解码；发送侧与接收侧 `Message` 对称。
- **新增** `ITransport::Send(const Message&, Endpoint=Default())`（基类默认实现）与 `SetCodec(topic, codec)`（基类 no-op，各传输覆写）。
- **新增** `TransportCore` 升级为 `topic→codec` 注册表 + 默认 codec（`EncodeForSend(data, topic)`、`DeliverFrame` 按 topic 选 codec）。
- **新增** header-only `TopicEnvelope`：in-band 信封 `PackTopic`/`UnpackTopic`（报文 `[topic_len:2][topic][body]`）、`FrameStream`/`TopicFrameAssembler`（流式 `[frame_len:4][topic_len:2][topic][body]`）、`TopicFitsEnvelope`。
- **新增** TCP/UDP/串口经各自 config 的 `enable_topic_routing`（默认**关**）opt-in；DDS 用原生 topic（无开关、无信封、线格不变）。
- **行为** 路由关闭时与旧帧格式**逐字节一致**；关闭时 `Send(Message{topic 非空})` 返回 `config: topic routing not enabled`；topic 上限 65535 字节，超限返回 `frame: topic too long`。

### 特性：Endpoint 统一寻址发送 — 2026-06-12
- **新增** 中立寻址值类型 `Endpoint`（`Default`/`Net(ip,port)`/`Topic(name)`）与 `ITransport::Send(data, Endpoint)`（基类默认实现，UDP 覆写 `kNet`、DDS 覆写 `kTopic`）。基类句柄即可寻址发送，无需向下转型。
- **⚠️ 破坏性** 删除 `IUdpTransport`（`UdpImpl` 直接实现 `ITransport`，`Factory::Create(UdpConfig)` 返回 `shared_ptr<ITransport>`）。
- **⚠️ 破坏性** 删除 `IDdsTransport::Send(data, topic)`（改用 `Send(data, Endpoint::Topic(...))`）。

### 构建：离线自包含 — 2026-06-12
- **变更** 第三方依赖（asio 1.30.2 / nlohmann json 3.11.3 / googletest 1.14）vendored 进 `third_party/`，构建**不再联网拉取**（移除 FetchContent）。
- **变更** Fast DDS 2.13+ 确立为**唯一可选外部依赖**，`find_package` 自动探测；未装跳过 `FastDdsProvider` 及真实互通测试。

### 里程碑：TransportFactory（主 spec 全部规划范围完成）— 2026-06-12
- **新增** `TransportFactory`：5 个类型化 `Create(config)`（返回各传输最具体接口）+ `CreateFromFile(path) → Result<vector<...>>`（JSON 配置数组）。
- **新增** JSON **严格校验**：未知 type/字段、类型不符、枚举非法、必填缺失 → `config:` 错误（含条目序号+字段），任一条目失败整体失败。
- **修复** `Create(DdsConfig)` 显式注册 FastDDS provider，根治静态库下匿名注册器被链接器裁剪问题。

### 里程碑：DDS 传输 — 2026-06-11
- **新增** `DdsImpl`（实现 `IDdsTransport`，组合 `TransportCore`）：pub-sub 多 topic 懒加载路由 + req-resp（`request_id` 关联、per-request 超时、take-then-invoke 恰好一次）。
- **新增** 内置承载 `RawMessage` + 自定义 `FastDdsRawType`（手写 wire layout，绕过 IDL/Fast DDS-Gen/CDR）。
- **新增** `IDdsProvider` 抽象 + `DdsProviderRegistry` + `FastDdsProvider`（Fast DDS 2.13）；`FakeDdsProvider`（进程内 topic 总线）使 DDS 业务逻辑零 FastDDS 依赖即可全测。
- **变更** `DdsConfig`：`qos_profile` 字符串 → `DdsQos` 结构体（借鉴 Apollo Cyber RT）；移除 `type_name`（承载统一为不透明字节流）。

### 里程碑：串口传输 — 2026-06-11
- **新增** `SerialImpl`（`asio::serial_port`，组合 `TransportCore`+`FrameAssembler`）：波特率/数据位/停止位/校验配置；流式分帧同 TCP；pty 回环测试。

### 里程碑：UDP 传输 + 组合化重构 — 2026-06-10
- **新增** `UdpImpl`（单类支持单播/组播/广播，组合 `TransportCore`，无分帧）；组播 join_group + TTL。
- **⚠️ 破坏性（架构）** 接收基座 `TransportBase`（继承式）→ `TransportCore`（组合式组件）：会收数据的传输改为**持有**它并转发接收侧方法，从根上消除「接收基座与扩展接口同源 `ITransport`」的菱形。

### 里程碑：TCP 传输 — 2026-06-10
- **新增** `TcpConnectionImpl`（已连接 socket 收发，client/accepted 共用）、`TcpClientImpl`（connect + 连接超时 + 指数退避自动重连）、`TcpServerImpl`（acceptor + 每客户端独立 `ITransport` + 广播 + `ITcpServer`）；standalone Asio 集成 + 真实回环集成测试。
- **变更** 具体实现类统一加 `*Impl` 后缀（`TcpConnection→TcpConnectionImpl` 等），区分接口。
- **修复** 锁内拷贝 `disconnect_cb_` 消除数据竞争。

### 里程碑：Foundation 基座 — 2026-06-09 ~ 06-10
- **新增** 核心类型 `Result<T>`/`Status`（不抛异常 + 前缀分类错误）、`Message`、`Endpoint` 雏形。
- **新增** 扩展点 `ICodec`（用户编解码）、`IFramer` ← `LengthFieldFramer`（长度字段分帧）、`FrameAssembler`（滚动缓冲切帧）、`ReceiveQueue`（FIFO，三模式互斥交付）。
- **新增** 统一抽象 `ITransport`（生命周期 / 发送 / 三模式接收 / 编解码挂载）；CMake 骨架 + GoogleTest + 版本冒烟测试。
- **设计基线** 抽象层（`include/`，零第三方依赖）+ 实现层（`src/`，第三方库封在 `*Impl`/provider 内）；`I*` 接口 + `*Impl` 实现；分帧仅流式接收侧；不抛异常；每实例单 io 线程 + strand 串行化。

---

[Unreleased]: https://github.com/Ste7an-cs/Transport/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.4.0
[0.3.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.3.0
[0.2.1]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.2.1
[0.2.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.2.0
[0.1.0]: https://github.com/Ste7an-cs/Transport/releases/tag/v0.1.0
