# ADR-0006：以 NodeBase 模板方法取代 NodeRuntime，并将共享完成量轻量化

**状态：** Accepted
**日期：** 2026-08-08
**关联：** ADR-0005（生命周期收敛——本 ADR 保留其 D1/D5/D6 的**行为结论**，只改承载结构）；SDD `docs/软件设计说明-GJB438C.md` **DD-3 / DD-4 / §4.1.4**（"组合 `NodeRuntime`，不继承"——本 ADR **推翻**其中的结构结论）；SRS RT_LIFECYCLE_003/004/006/007。

## 背景（Context）

`NodeRuntime<Event>` 是 528 行头文件（277 行实代码、17 个加锁点），装了四件事：①生命周期状态机 + 并发幂等 Start/Close + 收敛；②handler 消费者 fiber + `BoundedQueue` 集成；③读-分发循环骨架；④五个观测计数。两个使用者 `ProtocolNode` / `DdsNode` 调用的是**完全相同的 15 个方法、连次数都一样**。

复审中查实了三处与既有设计说辞不符的事实：

1. **模板参数从未被变化过** —— 两处实例化都是 `NodeRuntime<Message>`。泛型的代价（头文件实现、`byte_size_of` 回调注入）全部白付。
2. **"读循环骨架"只有 13 行**（占 277 行实代码的 5%），其余 95% 是生命周期、handler 队列与归因，与"运行时"这一命名给人的印象不符。
3. **`SharedCompletion` 的 waiter map 只为"每等待者独立取消"存在** —— deadline 不需要它（`Awaitable::await_for` 超时返回 `timed_out` 而**不关闭 channel**，天然不殃及其他等待者）；而 `Awaitable::close()` 的语义本就是"唤醒并收敛**所有**等待者"，即广播。**全仓生产代码中没有任何一处给 `WaitClosed` 传取消令牌**；唯一覆盖该能力的两个用例（`shared_completion_test` 的取消用例、`CoroFakeTransport.WaiterTimeoutAndCancellationAreLocal`）是为测该原语本身而写的，不来自任何消费者需求。

## 决策（Decision）

- **D1（删除 `NodeRuntime`，改由 `NodeBase` 基类承载生命周期）：** `ProtocolNode` / `DdsNode` 改为继承 `NodeBase`（非模板）。基类提供 `Start()` / `Close()` / `WaitClosed()` / `IsRunning()`，**幂等保护与关闭仲裁在基类内实现**；协议特有的启动/关闭实事下沉为纯虚钩子 `DoStart()` / `DoClose()`（模板方法模式）。
  **推翻** SDD DD-3/DD-4/§4.1.4 中"`ProtocolNode`/`DdsNode` 组合 `NodeRuntime`，**不继承**"的结构结论。理由：该结论原本防的是"共享引擎导致节点间耦合"，而两个节点对运行时的使用面**逐字相同**，不存在需要分化的部分；此时组合只是把继承关系写成成员变量，多一层转发而不多一分解耦。继承使幂等与收敛这段唯一的高风险代码仍只有一份，同时去掉转发层与模板。

- **D2（公开接口返 `Status`，不返 `bool`）：** `Start()` / `Close()` 保持返回 `Status`。
  理由：`bool` 会把「已 `Running`（成功）」与「已 `Closing`/`Closed`（RT_LIFECYCLE_003 要求返 `InvalidState`）」压成同一个 `false`，二者对调用方含义相反；且 RT_LIFECYCLE_007 要求校验失败后宿主能改配置重试，宿主需要知道错在哪。"本次调用是否真正执行了转换"这一信息仅在基类内部作为仲裁结果使用，不外露。

- **D3（`SharedCompletion` 轻量化为广播完成量）：** 以"存结果 + 一条共享 `Awaitable` + `close()` 广播"取代 waiter map，约 30 行取代 124 行。
  `Complete(result)`：持锁存结果 → 锁外 `close()` 广播唤醒全部等待者。
  `Wait(options)`：先持锁查已存结果（迟到者直接返回）→ 否则 `await_for(deadline)` 或 `await` → 醒来读存好的结果。
  **代价（明确接受）**：失去"每等待者独立取消"。这是**能力移除**，不是断言放松——`shared_completion_test` 的取消用例与 `CoroFakeTransport.WaiterTimeoutAndCancellationAreLocal` 随之删除，须在票中与 `CHANGELOG` 中如实标注为能力移除。deadline 能力不受影响。
  影响面：全仓 12+ 处实例（7 个传输的 `closed`、`TcpServer` 的 3 处、node 侧 3 处）统一受益。

- **D4（handler 队列拆为 `HandlerLoop<Message>` 小件）：** 拥有 `BoundedQueue<Message>` + `FiberTask` 句柄 + 协作取消令牌 + 逃逸异常隔离 + 时长计量。对外仅 `Spawn(consume)` / `Enqueue(e)` / `Join()` / `CancelAndClose()` 与几个计数。由 node 持有，不进基类——**它是可选的**（未设 handler 的节点没有），而基类应只装每个节点都有的东西。

- **D5（读循环骨架与观测计数并入各 node）：** 13 行的读循环回到它本来的归属（node 自己的 `Read → decode → dispatch`）；五个观测计数成为各 node 的成员。二者都不构成需要共享的机制。

- **D6（收敛留在基类）：** 收敛（读循环退出 → join handler → Drain 未启动业务归因 `close_drop` → 置 `Closed` → 广播唤醒）留在 `NodeBase`。
  理由：这是本轮改造中唯一**不可下放**的部分。`Awaitable` 是通知原语——`close()` 只保证等待者被唤醒，不保证被唤醒的 fiber 已经跑完并不再触碰 `this`；安全释放要的是后者，只能靠 join。ADR-0005 的 D1/D5/D6 行为结论（读循环兼任收敛者、致命错误自终、内部工作单元调 `Close`/`WaitClosed` 返 `kInvalidState`）**原样保留**，只是承载类改名换姓。

- **D7（分票纪律沿用 ADR-0005 D9）：** 本次触及的正是上一轮生命周期重构挂死并被整体回滚的那段代码（根因至今未知）。故仍按"每票只改一件事"拆分，且每票以 `--gtest_repeat` 作门禁（#128 已修复夹具，该手段可用）。任一步挂起即回滚该步而非整批。

## 影响（Consequences）

- **正面：** 删除 528 行模板头文件与其转发层；去掉从未变化的模板参数与 `byte_size_of` 回调注入；`SharedCompletion` 由 124 行降为约 30 行且全仓 12+ 处受益；`NodeBase` 只装每个节点都有的东西，可选件（handler 队列）与协议特有件（读循环、计数）各归其位。
- **负面（明确接受）：** ① 结构由组合改继承，推翻 DD-3/DD-4/§4.1.4 的已记录结论，须同步文档否则后人会照旧文档判其违规；② `WaitClosed` 失去取消能力（无人使用，但属公开 API 能力移除，破坏性）；③ 改动落在上一轮挂死过的同一段代码上，风险由 D7 的分票纪律与 repeat 门禁承接。
- **对 ADR-0005：** 其 D1/D5/D6 的**行为结论全部保留**，本 ADR 只改承载结构；D2（`FiberTask::get()` join）、D3（多等待者语义）、D8（删观测）不变——其中 D3 的"必须为每等待者分配独立 `Awaitable`"这一**理由**被本 ADR D3 修正：那只在需要 per-waiter 取消时成立。

## 备选方案（Alternatives considered）

- **保留 `NodeRuntime`、仅去模板并瘦身**：否决——能去掉模板与死 API，但保留了一层纯转发的成员对象；两个节点使用面逐字相同，转发层不换来任何解耦。
- **彻底删除、两个 node 各写一份仲裁与收敛**：否决——约 60–80 行高风险代码变两份且必须逐字同步；ADR-0005 那四张票每张都只改这一段，若为两份则每票都是两处并行修改。
- **`Start()`/`Close()` 返 `bool`**：否决——与 RT_LIFECYCLE_003/007 冲突（D2）。
- **保留 `SharedCompletion` 的 per-waiter 取消**：否决——124 行支撑一个无人使用的能力；其唯一测试是为测该原语自身而写（D3）。
- **让 `NodeBase` 也包 handler 队列**：否决——handler 是可选的，基类应只装每个节点都有的东西（D4）。
