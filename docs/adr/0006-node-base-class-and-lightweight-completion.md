# ADR-0006：以 NodeBase 模板方法取代 NodeRuntime，并将共享完成量轻量化

> **注（2026-08-19）**：本 ADR 的 **D1（NodeBase 模板方法形态）、D3（轻量 SharedCompletion）、D4（HandlerLoop 内件构成）、D6（收敛不可下放）、D8（重入守卫之争）已由 ADR-0008 推翻或作废**——`SharedCompletion`/`BoundedQueue` 已删除，`Close()` 改为只发信号后重入问题根本不再存在。保留的只有「node 生命周期由一个基类承载」这一条。

**状态：** Accepted
**日期：** 2026-08-08
**关联：** ADR-0005（生命周期收敛——本 ADR 保留其 D1/D5 的**行为结论**、只改承载结构，并以 **D8 撤销其 D6**）；SDD `docs/软件设计说明-GJB438C.md` **DD-3 / DD-4 / §4.1.4**（"组合 `NodeRuntime`，不继承"——本 ADR **推翻**其中的结构结论）；SRS RT_LIFECYCLE_003/004/006/007。

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

- **D5（读循环骨架与观测计数并入各 node）〔已由 #140 实施；`NodeRuntime.hpp` 随之删除〕：** 13 行的读循环回到它本来的归属（node 自己的 `Read → decode → dispatch`）；五个观测计数成为各 node 的成员。二者都不构成需要共享的机制。

- **D1′（补正，2026-09-03，#220）：`DoStart()` 跑在锁外，故生命周期多一条暂态 `close_pending_`。** `Start()` 的形状是「置 `starting_` → **放锁** → 跑 `DoStart()` → 重新取锁收尾」，那段放锁窗口里 `Close()` 可以进来（运行时是 M:N 的，`Close()` 的使用契约只要求"由节点外部"，**没要求同线程**）。

  **初版的缺陷**：窗口内的 `Close()` 看到 `lifecycle_ == kCreated`，据此判定"无 fiber、无 transport 可关"，直接落 `kClosed` 并返回**成功**；随后 `Start()` 收尾时**无条件**写回 `kRunning`。
  **后果**：一个**已被 `Close()` 过、调用方已收到成功返回**的节点被复活成 `Running`，而那次关闭的收敛信号**从未发出**（`DoClose()` 没跑）。

  **可达性已判实**（#220）：

  | | 结论 | 依据 |
  |---|---|---|
  | 同一执行域内 | **不可达** | 两个真实 `DoStart()` 全程无挂起点；且 `Coro::makeTask` spawn 走 `boost::fibers::fiber(props, fn)`，该构造器策略是 **`launch::post`**（`boost/fiber/fiber.hpp:125`）——**创建时不切换**到新 fiber |
  | **跨 OS 线程** | **今天就成立** | 窗口内的 `Close()` 走 `has_fibers == false` 分支，**全程只碰基类 mutex 与相位**、不触碰任何 fiber 亲和之物，在外来线程上执行定义良好 |

  **故这是已可复现的缺陷，不是防御性加固**——回归用例在修复前的代码上确实失败。

  ### 处置：`Close()` 在 `starting_` 期间只记账，由 `Start()` 收尾裁决

  ```
  Close()  见 starting_  → 置 close_pending_，【不落相位、不发信号】，返回"已受理"
  Start()  收尾复查：
     DoStart() 成功 + 待关闭 →  kClosing + 锁外补发 DoClose()  + 返 kInvalidState
     DoStart() 失败 + 待关闭 →  kClosed（那次关闭已答应过，不许退回 Created 重试）
                                 返回值仍报【启动失败的成因】
     无待关闭                →  kRunning（原路径）
  ```

  相位序列仍是正规的 `Created → Closing → Closed`，**没有新增对外可见的状态**——`close_pending_` 是私有暂态，外部永远观察不到"半关"。

  **否决的另一条**（`Start()` 末尾只在 `kCreated` 时才写 `kRunning`）：它**仍要在 `DoStart()` 还在跑时对外公开 `kClosed`**，而相位是全库仲裁点——`WaitClosed()` 会据此去 `DoJoin()`，与仍在写 `read_task_`（子类成员、无锁守）的 spawn 过程撞车，甚至在 fiber 尚未 spawn 出来时就宣告收敛完成；`DoClose()` 也只能由外来线程去动 `DoStart()` 正在写的成员。**两条修法都要由 `Start()` 收尾补发信号，那就别在中途先说假话。**

  > **⚠ 同源但未处置的第三条路径**：`WaitClosed()` 在 `starting_` 期间仍会因 `lifecycle_ == kCreated` **立即返回**，宣告"无 fiber 可汇合"。宿主若在另一条线程 `Close()` 后紧接 `WaitClosed()`，拿到的是**提前的"已收敛"**。#220 **既没修好它、也没让它变坏**（修前 `WaitClosed()` 会去 `DoJoin()` 一个可能还是空的 `read_task_`，同样是假收敛）。**待另行处置。**

- **D6（收敛留在基类）：** 收敛（读循环退出 → join handler → Drain 未启动业务归因 `close_drop` → 置 `Closed` → 广播唤醒）留在 `NodeBase`。
  理由：这是本轮改造中唯一**不可下放**的部分。`Awaitable` 是通知原语——`close()` 只保证等待者被唤醒，不保证被唤醒的 fiber 已经跑完并不再触碰 `this`；安全释放要的是后者，只能靠 join。ADR-0005 的 D1/D5 行为结论（读循环兼任收敛者、致命错误自终）**原样保留**，只是承载类改名换姓；其 D6（重入守卫）由本 ADR **D8 撤销**。

- **D7（分票纪律沿用 ADR-0005 D9）：** 本次触及的正是上一轮生命周期重构挂死并被整体回滚的那段代码（根因至今未知）。故仍按"每票只改一件事"拆分，且每票以 `--gtest_repeat` 作门禁（#128 已修复夹具，该手段可用）。任一步挂起即回滚该步而非整批。

- **D8（撤销重入守卫，改为使用契约 + 不提供等待型入口）：** **撤销 ADR-0005 D6。** 不设任何运行时重入守卫——不比对 fiber id、不登记内部工作单元身份、不设"半执行"分支；`Close()` 结尾无条件 `closed_.Wait()`。
  取而代之的是**结构性保证**：内部工作单元本就不存在"需要等待本节点关闭"的合法用途，故不为其提供任何会等待的入口——
  ① 处理器能力面**保留** `HandlerContext::RequestClose()` / `DdsHandlerContext::RequestClose()`，但改调框架的**发信号**路径（`SignalCloseIfFirstCloser()`），**只发起、不等待**，受理即返回；收敛由读-分发循环完成。命名与 `ITransport::RequestClose()`（发信号）/ `WaitClosed()`（等待）的仓内既有约定一致。
  ② 读-分发循环的收敛走私有的 `ConvergeAfterReadLoop()`，从不调用公开关闭接口。
  **明确接受的代价**：调用方若捕获 `node*` 并在处理器内直接调用 `Close()`/`WaitClosed()`，将**静默挂死**而非得到 `kInvalidState`。ADR-0005 D6 当初正是以"违约后果由错误码变为静默挂死、排查成本极高"为由否决了这条路；本决策**推翻该权衡**，理由：守卫的唯一价值是给一个**已确认不会发生**的违约提供诊断，而它的代价是在生命周期这段最难的代码里常驻两处 fiber id 登记/注销与一个跨锁判据——为防呆维持机制，与本 ADR"不造上帝对象、机制各归其位"的主张相悖。契约写入 API 注释与 SRS RT_LIFECYCLE_005。
  连带：**撤销** SRS §3.2.2 中"应移除 `HandlerContext::RequestClose`"的接口变更登记（二者保留）；RT_LIFECYCLE_005 由"应返回 `InvalidState`"改写为使用契约 + 框架不提供等待型入口。

## 影响（Consequences）

- **正面：** 删除 528 行模板头文件与其转发层；去掉从未变化的模板参数与 `byte_size_of` 回调注入；`SharedCompletion` 由 124 行降为约 30 行且全仓 12+ 处受益；`NodeBase` 只装每个节点都有的东西，可选件（handler 队列）与协议特有件（读循环、计数）各归其位。
- **负面（明确接受）：** ⓪ 处理器内误调**等待型**关闭接口由"得到 `kInvalidState`"变为**静默挂死**（D8，明确接受）；① 结构由组合改继承，推翻 DD-3/DD-4/§4.1.4 的已记录结论，须同步文档否则后人会照旧文档判其违规；② `WaitClosed` 失去取消能力（无人使用，但属公开 API 能力移除，破坏性）；③ 改动落在上一轮挂死过的同一段代码上，风险由 D7 的分票纪律与 repeat 门禁承接。
- **对 ADR-0005：** 其 D1/D5 的行为结论保留，本 ADR 只改承载结构；**D6（重入守卫）由本 ADR D8 撤销**；D2（`FiberTask::get()` join）、D8（删观测）不变；其 D3 的"必须为每等待者分配独立 `Awaitable`"这一**理由**被本 ADR D3 修正——那只在需要 per-waiter 取消时成立。

## 备选方案（Alternatives considered）

- **保留 `NodeRuntime`、仅去模板并瘦身**：否决——能去掉模板与死 API，但保留了一层纯转发的成员对象；两个节点使用面逐字相同，转发层不换来任何解耦。
- **彻底删除、两个 node 各写一份仲裁与收敛**：否决——约 60–80 行高风险代码变两份且必须逐字同步；ADR-0005 那四张票每张都只改这一段，若为两份则每票都是两处并行修改。
- **`Start()`/`Close()` 返 `bool`**：否决——与 RT_LIFECYCLE_003/007 冲突（D2）。
- **保留 `SharedCompletion` 的 per-waiter 取消**：否决——124 行支撑一个无人使用的能力；其唯一测试是为测该原语自身而写（D3）。
- **让 `NodeBase` 也包 handler 队列**：否决——handler 是可选的，基类应只装每个节点都有的东西（D4）。
- **保留 ADR-0005 D6 的运行时重入守卫（比对两条内部工作单元 fiber id、返 `kInvalidState`）**：否决——守卫的唯一价值是给一个**已确认不会发生**的违约提供诊断；一旦 `RequestClose()` 改走只发信号的内部路径，内部工作单元便不再有任何会等待的入口，守卫失去被触发的合法途径，只剩防呆（D8）。
- **移除 `HandlerContext::RequestClose()`（原 ADR-0005 D6 的一半）**：否决——`RequestClose`/`WaitClosed` 的"发信号 / 等待"分工是仓内既有约定（`ITransport` 即如此），处理器请求关闭是正当能力，问题只在旧实现让它调了会等待的 `Close()`；改调发信号路径即可，无需砍能力（D8）。
