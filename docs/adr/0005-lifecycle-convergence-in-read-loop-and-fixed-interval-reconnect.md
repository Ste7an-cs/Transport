# ADR-0005：收敛并入读循环、固定间隔重连与重入守卫

> **注（2026-08-08）**：本 ADR 的 **D6（重入守卫）已由 ADR-0006 D8 撤销**——改为使用契约 + 不提供等待型入口，不设运行时守卫。其余决策不受影响。

> **注（2026-08-19）**：本 ADR 的 **D1（收敛并入读循环）与 D2（三方汇合的完成量协议）已由 ADR-0008 D2/D3 推翻**——收敛改由 `WaitClosed()` 的调用方经 `DoJoin()` 自上而下 join；**D5（致命错误自终）**在 ADR-0007 D2 缩小适用介质后，又因 `Close()` 改为只发信号而退化为「读循环退出时无条件调公开的 `Close()`」，不再是独立分支。

**状态：** Accepted
**日期：** 2026-08-07
**关联：** ADR-0004（传输语义统一——本 ADR 建立在其 D1/D2/D3 之上）；SRS `docs/需求规格说明书-协程原生.md`（落点：RT_TCP_RECONNECT §3.1.7.3/§3.1.7.4、RT_TCP_RECONFIG_002、RT_LIFECYCLE_005/008/009）；ADR-0003 D11（连接管理分层——本 ADR 改其退避部分）。

## 背景（Context）

ADR-0004 落地后（#106–#112），交互层的内部工作单元由三条降为**两条**：读-分发循环 + handler 消费者（reactor 随 `IConnectionObservable` 取消而消失）。这一变化让此前几处只为特例存在的机制失去依托：

- `NodeRuntime::AddFinalizerJoin` / `finalizer_joins_` —— 全库唯一使用者是 `ProtocolNode` 为 reactor 登记的汇合点，reactor 消失后**归零**；
- 独立 finalizer fiber —— 其存在理由是"handler 内发起关闭不得自等待"，而项目已确认 **handler 不会调用 `Close`**；
- 连接退避的四套参数（初值/倍率/上限/抖动）—— 与"简化 TCP 客户端"的目标相悖。

同时存在一件历史包袱：一轮生命周期重构（一次性改了 5 件事：`FiberTask` join / 内联 `Close` / 重入检测扩至 3 条 fiber / 自终 / 删 500ms 观测）在全量测试中挂死并被整体回滚，**该代码以 `git checkout --` 丢弃、从未提交，已不可恢复**——因此"诊断旧账"等价于重做一遍。

## 决策（Decision）

- **D1（收敛并入读循环，删除 finalizer fiber）：** 读-分发循环退出后**兼任收敛者**：等 handler 退出 → Drain 未启动业务归因 close_drop → 置 `Closed` → 唤醒全部关闭等待者。`Close()` 退化为"发汇合信号 + 等待收敛结果"。
  依据：两条内部工作单元中**读循环恒是第一个退出的**（无论我方 `Close` 使 `Read` 返 `kClosed`，还是致命错误使其返 `kClosed`——ADR-0004 D1 之后二者同码），故它天然是收敛的正确位置。
  删除：**finalizer fiber**、`finalizer_joins_`、`AddFinalizerJoin`、以及 `loop_done_`（无人再等读循环——它自己就是收敛者）。
  **结构约束**：读循环**不得调用公开的 `Close()`**（那会等待自身退出）；其收敛走内部路径。

- **D2（`handler_done_` 改用结构化并发 join）：** `SpawnHandlerLoop` 保留 `FiberTask` 句柄，读循环以 `FiberTask::get()`（让出式 join）等待 handler 退出，取代手写的 `SharedCompletion<void>`。与 AsyncTask《使用说明》推荐用法及 `TcpClientTransport::loop_task` 的既有先例一致。

- **D3（`closed_` / `start_done_` 保留 `SharedCompletion`）：** 二者**不可**替换为共享 `Awaitable`。
  理由：`WaitClosed(options)` 支持**每个调用方各自的 deadline 与取消令牌**；若共用一条 `Awaitable`，任一等待者取消时只能 `close()` 该共享 channel，**会错误唤醒并终结其余所有等待者**。`SharedCompletion` 为每个等待者分配独立 `Awaitable`、只关闭自己那条——这正是它存在的理由，由 RT_LIFECYCLE_004/006（多等待者 + 各自 deadline/取消）逼出。
  附带结论：**`AutoDisconnect` 不适用于收敛**——它是 Qt 信号连接的生命周期管理器（`disconnectAll()` 同步瞬时、`addCleanup` 同步回调），**无 await/join 语义**，而收敛需要异步等待 handler 真正退出。

- **D4（固定间隔重连，撤销指数退避）：** 重连间隔改为**固定 1 秒**；取消倍率、上限、抖动、稳定重置四套参数。
  **保留非零间隔是必要的**：对端主机在而端口未监听时内核立即回 RST，`connect` 微秒级失败，无间隔重连将退化为紧循环（烧 CPU 且向对端刷 SYN）；而"服务进程未起来"恰是最常见的重连场景。
  抖动原用于避免大量客户端同时重连打垮服务端；若后续部署规模需要，可作为独立能力重新评审。
  落点：SRS §3.1.7.2/§3.1.7.3/§3.1.7.4、RT_TCP_RECONFIG_002（热更新范围由"退避参数"改为"重连间隔"）、`TcpClientConfig`（四字段作废）。**改 ADR-0003 D11 中的退避部分。**

- **D5（致命错误自终，落地 RT_LIFECYCLE_008）：** 读循环退出时若节点仍 `Running`（即非我方 `Close` 所致），自行置 `Closing` + 发汇合信号，再走**同一段**收敛代码。正常关闭与自终由此**合并为一条路径**，区别仅在"谁先置的 `Closing`"。
  注意：**TCP 客户端永不自终**——它无限重连，`Read` 只在我方 `Close` 时返 `kClosed`（ADR-0004 D1）。自终只对 UDP / 串口 / 已接受的 TCP 连接生效。

- **D6（重入守卫，RT_LIFECYCLE_005 的最小版本）—— 已由 ADR-0006 D8 撤销（2026-08-08），下述内容仅存档：** 删除"handler 内 Close 只发起不自等"的半执行分支；改为**内部工作单元（读循环 / handler）调用 `Close`/`WaitClosed` 一律返 `kInvalidState`**。同时移除 `HandlerContext::RequestClose` / `DdsHandlerContext::RequestClose`（RT_IF_API 破坏性变更，SRS 已登记）。
  依据：项目已确认 handler 不调 `Close`，故该守卫从"支撑真实场景"退化为**防呆**——但仍必要：调用方能捕获 `node*` 硬调，全删则违约后果由"半执行但能收敛"变为**静默挂死**（`Close` 等收敛 → 收敛等 handler 退出 → handler 卡在 `Close` 里）。且 D1 之后读循环成了收敛者，它若调 `Close` 同样自等——同一守卫一并挡住。

- **D7（析构语义 RT_LIFECYCLE_009 不做额外工作）：** 当前 `~ProtocolNode() { Close(); }` 且 `Close` 对外部调用者等待至收敛完成，**"析构等价于 Close + WaitClosed"主干已满足**。需求中"不得在节点自身的内部工作单元中析构该节点"在 C++ 中无廉价强制手段（真那么做即 use-after-free），属**使用契约**，写入 API 注释即可。

- **D8（删除 500 ms 处理器取消观测）：** 需求层已将其连同关闭时延指标降级为设计目标（TBD-007）；实现层一并**删除**——`kHandlerCancelObservation`、超时记 `Internal` 的观测分支、累计计数及公开访问器 `ProtocolNode::HandlerCancelOverrunCount()` 全部移除。
  理由：**该计数自引入起从未被任何代码消费**——`HandlerCancelOverrunCount` / `handler_cancel_overrun` 在 `tests/` 与 `examples/` 全仓零命中，无任何断言或示例读取它；SRS §3.1.6 本就把"是否就『处理器取消超时』单设可观测计数"列为 **P6 待定项**，故删除不违反任何既定要求。
  **更正（2026-08-08）**：本决策初稿给出的理由是"D2 之后 `FiberTask::get()` 无带 deadline 的重载，保留观测必须在收敛路径上并行维持第二套等待机制"。**该理由不成立**——`Coro::FiberTask` 内部即 `boost::fibers::future`，其本身具备 `wait_for`/`wait_until`；只是 `FiberTask` 未对外暴露，补一个转发方法即可，观测退化为两行。删除的正当理由是上述"无人消费"，而非结构代价。
  若 P6 性能硬化期确需"谁拖慢了关闭"的信号，正确落点是在 handler 循环内对**每次回调**计时（能定位到具体回调），而非在收敛处观测总时长（只能得知"慢"却不知慢在哪），届时作为独立能力评审。
  连带：移除 `HandlerCancelOverrunCount()` 属 RT_IF_API 破坏性变更，须在 `CHANGELOG.md` 标注；SRS §3.1.5.4 中"超时记录 `Internal`"一句同步删除（协作取消的期望与"完全关闭仍等待处理器实际退出"保留）。

- **D9（不追旧账，每票只改一件事）：** 不重写并诊断上次挂死的实现（代码已不可恢复，见背景）。缓解手段是**拆票纪律**：上次一次性改 5 件事，本轮每张票只改一件，任一步挂住即回滚该步而非整批。**明确记录：我们带着"上次为何挂死"这一未解之谜前进。**

## 影响（Consequences）

- **正面：** 少一条临时 fiber（finalizer）与三个同步原语（`loop_done_`、`handler_done_`、`kHandlerCancelObservation` 观测）；`AddFinalizerJoin`/`finalizer_joins_` 整体删除；**正常关闭与致命自终合并为一条收敛路径**；收敛处只剩一种等待机制（`FiberTask::get()`）；`TcpClientConfig` 由五个连接参数缩为两个（端点 + 连接超时 + 重连间隔常量）；违约调用由静默挂死变为明确错误码（**该项已随 D6 被 ADR-0006 D8 撤销**）。
- **负面（明确接受）：** ① 固定间隔失去"重连风暴错峰"能力（抖动取消），大规模部署需重新评审；② `RT_TCP_RECONFIG` 可热更新的内容显著缩小（仅端点/连接超时/重连间隔）；③ 失去"handler 不配合协作取消"的唯一现成信号，关闭偏慢时无内建线索（D8）；④ 移除 `HandlerCancelOverrunCount()` 为破坏性 API 变更；⑤ 带着上次挂死的未解之谜前进（D9）。
- **对 ADR-0003：** 其 **D11** 中的退避策略部分被本 ADR **D4** 取代；分层结论不变。
- **对 ADR-0004：** 本 ADR 建立在其 D1/D2/D3 之上，不修改其任何决策。

## 备选方案（Alternatives considered）

- **保留独立 finalizer fiber**：否决——其存在理由（handler 内关闭不自等）已随"handler 不调 Close"消失；读循环兼任是更少动件的等价方案（D1）。
- **用 `AutoDisconnect` 或裸 `Awaitable` 替换全部 `SharedCompletion`**：否决——前者无 join 语义；后者会使一个等待者的取消殃及全部等待者（D3）。
- **无延迟重连**：否决——对端端口未监听时退化为紧循环（D4）。
- **保留指数退避**：否决——与本轮简化目标相悖，四套参数换一个常量（D4）。
- **全删重入机器、契约纯靠文档**：当时否决（理由：违约后果由错误码变为静默挂死，排查成本极高，D6）——**该否决已被 ADR-0006 D8 推翻，此方案现为采纳方案**。
- **给 `Coro::FiberTask` 补 `wait_for` 转发、以两行保留 500 ms 观测**：技术上完全可行（AsyncTask 为自有库，`boost::fibers::future` 已具备该能力），但否决——理由不是代价，而是**该计数从未被任何代码消费**，保留它等于维护一个无人读取的埋点（D8）。
- **先重写上次的实现以诊断挂死根因**：否决——代码已不可恢复，重写等于重做一遍明知有雷的东西，收益仅为知识（D9）。
