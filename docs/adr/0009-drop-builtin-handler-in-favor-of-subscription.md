# ADR-0009：废止节点内置 handler 通道，入站业务改由订阅承载

**状态：** Accepted
**日期：** 2026-08-20
**关联：** ADR-0008（接口重设计与键匹配分发——本 ADR 建立在其 `Dispatcher` / `Subscribe(Key)` 之上，并废止与之并存的第二条入站通路）；ADR-0006（节点基类与轻量完成量——`HandlerLoop` 由该 ADR 拆出，已随本 ADR 于 #163 删除）；SRS `docs/需求规格说明书-协程原生.md`（落点：RT_HANDLER 全组、RT_LIFECYCLE_006、§3.6 丢弃归因、§3.2.2 RT_IF_API 接口变更登记）。

## 背景（Context）

ADR-0008 引入 `Dispatcher` 与 `Subscribe(Key)` 之后，入站消息事实上存在**两条并存的通路**：

```
读循环 await(rx_) → decode → Dispatch()
                               ├─ dispatcher 键匹配命中 → 各订阅者的 Ticket 信箱（每人一份副本）
                               └─ 无匹配的业务帧      → handler_loop_.Enqueue（第二条队列 + 第二条 fiber）
```

两条通路各自持有一个 `Coro::Awaitable` 队列，各自有一条消费者 fiber，而 `Ticket` 的信箱与 `HandlerLoop` 的队列**是同一个类型、同一套语义**。差别仅在：订阅者的 fiber 属于宿主，handler 的 fiber 属于节点。

节点持有 handler 通路带来三项本不必由框架承担的职责——严格串行（RT_HANDLER_003）、逃逸异常隔离（RT_HANDLER_006）、协作取消令牌（RT_HANDLER_001 的 `HandlerContext`）——而订阅模型下这些天然落在宿主自己的消费 fiber 里。

## 决策（Decision）

- **D1（废止节点内置 handler 通道）：** 删除 `ProtocolNode::Config::handler`、`HandlerContext` 与 `ProtocolNode::handler_loop_`（`HandlerLoop<Event>` 类本身因 `DdsNode` 仍在用而暂留，见 D2 删除时机）。入站业务帧一律经 `Subscribe(Key)` 交付：宿主按 `FrameType` 等字段显式订阅自己关心的帧，在**自己的 fiber** 上消费。
  `Dispatch()` 因此只剩**一条投递路径**（按键投给全部匹配的订阅者），删去尾部"无匹配业务帧 → `handler_loop_.Enqueue`"及其兜底的 `kNoHandlerConfigured` 归因。**保留**终结帧（`kResponse`/`kResult`）无人认领时的 `kUnmatchedOrLateResponse` 归因——该分支属请求-响应侧，与 handler 无关。

- **D2（消费样板彻底交给调用方，不保留任何辅助件）：** `HandlerLoop` **终将整体删除**，不降级为公开小件、也不另造 `SubscriptionLoop`。宿主自行编写：
  ```cpp
  auto ticket = node.Subscribe({kAny, kAny, FrameType::kCommand});
  auto task = Coro::makeTask([&] {
    for (;;) {
      auto m = Coro::await(ticket.mailbox());
      if (!m) break;                 // 信箱被 CloseAll 关闭 → 退出
      try { /* 业务处理，可自由挂起 */ }
      catch (...) { /* 自行隔离 */ }
    }
  });
  ...
  (void)task.get();                  // 宿主自己 join
  ```
  **明确接受**：该样板会在每个调用方处重复。取舍是"少一个框架件、少一套需要维护的语义"胜过"少几行重复代码"。
  **删除时机——初稿判断有误，已更正（2026-08-25，#163）**：初稿写"因 `DdsNode` 仍在用而暂留"，**该前提不成立**。核对结果：

  - `src/node/DdsNode.cpp` 调用着 `MarkRunning()`、`transport_->Read()`、`transport_->Write()`、`node_->SignalClose()` —— 四者分别被 ADR-0006（前者与末者）与 ADR-0008（中间两者）删除，**编译不过**；
  - `src/node/DdsNode.cpp` **不在 `CMakeLists.txt` 的库源文件清单内**；
  - `tests/handler_loop_test.cpp` 亦不在测试源清单内（只出现在"停摆用例"注释里）。

  即 `DdsNode` 不是活着的使用者，而是**重设计之前的代码**，与那批停摆测试文件同类；`HandlerLoop` 在当时的编译面里**生产端与测试端均无使用者**。谁将来复活 `DdsNode`，都须照新 `ITransport` + `NodeBase` 整体重写，届时按本 ADR 入站本就走订阅，**不会再需要 `HandlerLoop`**。

  故 **`HandlerLoop<Event>` 与 `tests/handler_loop_test.cpp` 已于 #163 直接删除**，未等 `DdsNode` 票。

- **D3（串行、异常隔离、背压降为调用方契约）：** 框架不再保证同节点业务处理的严格串行，不再兜住业务代码的逃逸异常，不再提供业务队列容量。三者由宿主的消费 fiber 自行决定：一条 fiber 顺序消费即得串行，需要并发就自己起多条；异常自己 `try/catch`；队列容量即 `Ticket` 信箱的容量。
  RT_HANDLER_001 / 003 / 006 相应废止。

- **D4（关闭与汇合语义收窄）：** `Dispatcher::CloseAll(kClosed)` 关闭全部订阅信箱这一动作，**本身就是**订阅者的协作取消信号——在途 `await` 恰好终结一次，消费 fiber 自然退出。故：
  - `DoClose()` 去掉 `handler_loop_.CancelAndClose()`，只剩"关本节点读订阅 + `dispatcher_.CloseAll()`"；
  - `DoJoin()` 去掉 `handler_loop_.Join()`，只剩 join 读循环。
  **语义变化（明确记录）**：RT_LIFECYCLE_006 的"`WaitClosed` 返回即全部内部工作单元已退出"，其"内部工作单元"由**两条**（读循环 + handler 消费者）收窄为**一条**（读循环）。订阅者的消费 fiber 属于宿主，节点无从 join——`WaitClosed` 返回后它们可能仍在退出途中（信箱已关，它们至多再跑完手上那一条就退出）。宿主若需要严格汇合，须自己 join 自己的 fiber。
  附带简化：从订阅者 fiber 调 `node.Close()` 是**合法**的——它不是节点的内部工作单元，且新形态下 `Close()` 本就不含等待点。

- **D5（观测项全部删除，接受完整性归因弱化）：** 删除 `ProtocolNode` 上的 `BusinessQueueOverflowCount()` / `HandlerExceptionCount()` / `LastHandlerDuration()` 及其计数，并删除 `ProtocolNode` 内 `DropReason::kNoHandlerConfigured` 的产生点。（**更正**：`DropReason::kNoHandlerConfigured` **枚举值已于 #163 一并删除**——归因清单六项减为五项；`DdsNode` 侧的同名访问器随 D6 暂留于未编译文件中。）
  **明确接受的代价**：**无订阅者认领的业务帧将不再有任何归因记录**——既不计数也不产生 Trace。P5 的"完整性归因"从"每一条被丢弃的入站消息都有命名原因"退化为"框架已知的丢弃有命名原因"。`loss=0` harness 的两条等式（Σ命名原因 = 总丢弃、`drop_records.size() == Σ`）在数值上仍成立，但其**覆盖面**变窄。
  依据：订阅模型下"没有订阅者"不再是异常，而是宿主的正常选择（只订阅关心的帧）；把它记为丢弃会把常态噪声混进丢弃归因。

- **D6（本轮范围仅 `ProtocolNode`，`DdsNode` 另票）：** `DdsNode` 的 `handler_loop_` 与 `DdsHandlerContext` 本轮**不动**，另开票处理。
  代价（明确记录，2026-08-25 更正）：`DdsNode` 及其依赖的 `DdsHandlerContext` 停留在**重设计之前的形态**且不参与编译（详见 D2 的"删除时机"更正）。因此并**不**存在"两种入站模型并存"——只有一种活着的入站模型（订阅），外加一份编译不过的历史代码。
  本轮**未删净的**仅剩：`DdsHandlerContext` 与 `DdsNode` 侧的同名观测访问器，二者都在未编译文件里，随 `DdsNode` 的复活/重写票一并处置。`HandlerLoop<Event>` 与 `DropReason::kNoHandlerConfigured` **已于 #163 删除**。

- **D7（#152 不因本次消失，仅换层）：** `HandlerLoop` 的队列与 `Ticket` 信箱同为 `Coro::Awaitable`，"满时丢最旧、静默、无丢弃计数"的语义（#152）随之整体上移到 `Dispatcher` 的信箱层，**问题不因删除 HandlerLoop 而解决**。#152 仍需独立处置。

## 影响（Consequences）

- **正面：** `ProtocolNode` 的入站通路由两条并存收为一条；`HandlerLoop<Event>` 及其单测整体删除（#163，净删约 490 行）；丢弃归因由六项减为五项；`Dispatch()` 去掉第二分支；`DoClose`/`DoJoin` 各少一步；节点不再承担串行/异常隔离/队列容量三项职责；节点内部工作单元由两条降为一条。
- **负面（明确接受）：** ① 消费样板在每个调用方处重复（D2）；② 严格串行与异常隔离由框架保证降为调用方契约，宿主写错即失去该性质（D3）；③ `WaitClosed` 的汇合覆盖面收窄，宿主须自行 join 其消费 fiber（D4）；④ 无订阅者的业务帧成为**不可见丢弃**，完整性归因覆盖面变窄（D5）；⑤ `DdsNode` 停留在重设计之前的形态且不参与编译，其复活须整体重写（D6）。
- **破坏性 API 变更：** 移除 `ProtocolNode::Config::handler`、`HandlerContext`（含其 `Send()` / `RequestClose()` / `cancellation()`）及 `ProtocolNode` 上的三个观测访问器。`HandlerLoop<Event>` 与 `DropReason::kNoHandlerConfigured` 已于 #163 删除；仅 `DdsHandlerContext` 与 `DdsNode` 的同名访问器保留于未编译文件中（D6）。须在 `CHANGELOG.md` 标注，SRS §3.2.2 接口变更登记同步。
- **测试面：** `config.handler` 在测试中有 **35 处设置、分布 10 个文件**，须逐一改写为订阅 + 自有消费 fiber。其中 `protocol_node_handler_test.cpp` 整个文件的立意（RT_HANDLER 契约）随需求废止而失效，须重定位或删除。

## 备选方案（Alternatives considered）

- **`HandlerLoop` 降级为公开可选辅助件**（节点不再持有，宿主可选用）：否决——留着它就得继续维护其语义与单测，而它的全部能力（有界队列 + 消费 fiber + 取消 + 异常隔离）在订阅模型下都是宿主自己几行就能写的；"少一个需要维护的件"是本 ADR 的主要收益。
- **另造更薄的 `SubscriptionLoop` 小件**：否决——同上，且要新写代码与新写测试，换来的仍只是省去调用方几行样板。
- **给 `Dispatcher` 引入"兜底订阅"语义**（仅在无人命中时投递），保留 handler 作为兜底订阅者：否决——这是 `Dispatcher` 的**语义扩张**而非简化，且并未消除第二条通路，只是把它藏进 dispatcher。
- **保留"无订阅者"归因**（`kNoHandlerConfigured` 改名为"无订阅者"）：否决（用户裁决）——订阅模型下无人认领是常态而非异常，记为丢弃会把常态噪声混进归因。代价见 D5，已明确接受。
- **`ProtocolNode` 与 `DdsNode` 同批改造**：否决——违反"每票只改一件事"（ADR-0005 D9）的既有纪律，且 DDS 侧的订阅语义与本地 `Dispatcher` 是否等价尚未评估。
