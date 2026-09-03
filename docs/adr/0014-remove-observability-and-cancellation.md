# ADR-0014：撤销观测/Trace 与 Cancellation，框架不再提供可观测性

**状态：** Accepted
**日期：** 2026-09-03
**关联：** **推翻 ADR-0003 D13**（P5 观测 + 完整性归因）；ADR-0008 **D10**（已删全部计数接口，本 ADR 删剩下的 trace 一路）；ADR-0006（`Cancellation` 的来源）；SRS **RT_TRACE_001 / RT_TRACE_002 / RT_DATA_BUFFER**（本 ADR 撤销）；SDD **DD-10**（本 ADR 撤销）。

## 背景（Context）

### 观测面早已只剩半条

ADR-0003 **D13** 定的是**双面观测**：**push**（`ITraceSink` 结构化事件）+ **pull**（具名计数 getter 快照），并以"Σ 命名原因 = 总丢弃"的**结构性纪律**保证完整性。

此后逐条塌缩：

| 时间 | 事件 | 结果 |
|---|---|---|
| ADR-0008 **D10** | 删除**全部计数接口** | pull 面整个消失；"loss = 0 等式"不再可直接验证 |
| ADR-0008 **D8** | `BoundedQueue` → `FiberChannel` | 队列丢弃**既无计数也无归因**——`FiberChannel` 无 `size()`、丢弃静默，**连"何时该记"这个时刻都取不到** |
| #152（2026-08-28） | 有界丢弃裁决 | **不加归因、不改容量策略**——loss=0 等式**不再追求** |
| #212（2026-09-01） | `DropReason` 六项减为两项 | 四项的**产生点在设计上被取消** |

**到本 ADR 之前，观测面的实际形态是：一个 `ITraceSink` 接口 + 两个节点里各 4 处 `RecordEvent` 调用 + 一个只剩两项的 `DropReason`。** pull 面没了，完整性纪律放弃了，归因项删得只剩坏帧与无匹配响应。

### `Cancellation` 从未被产品代码使用

`CancellationRegistration` / `CancellationToken` / `CancellationSource` 是 ADR-0006 的遗留物。**产品代码零调用**——排除自身实现与自测后，引用它的只有三个**编译面外**的停摆测试。SDD 的分层图里早已标注"活代码无使用者"。

取消语义在重设计之后由 **`Awaitable::close()`** 承载（整流传播，ADR-0009 **D4**：信箱被关即订阅者的协作取消信号），`Cancellation` 这套没有位置。

## 决策（Decision）

- **D1（撤销观测/Trace 整条能力）：** 删除 `Observability.hpp`（`RecordDrop` / `RecordEvent`）、`ITraceSink.hpp`、`TraceCategories.hpp`、`DropReason.hpp`，以及四处 `trace_sink` 配置字段（`TcpConfig` / `SerialConfig` / `ProtocolNodeConfig` / `DdsNodeConfig`）与两个节点里的 8 处 `RecordEvent` 调用、两个私有 `TraceDrop` 助手。

  **`DropReason` 随之一并删除**：它现在唯一的用途是给 trace 消息提供归因名（`DropReasonName` 写进 `TraceEvent.message`）。**trace 出口没了，它就没有任何消费者。**

- **D2（撤销 `Cancellation`）：** 删除 `Cancellation.hpp` / `Cancellation.cpp` 与 `cancellation_test.cpp`。取消语义由 `Awaitable::close()` 承载，不另设一套。

- **D3（不留"以后再接"的钩子）：** **不保留** `ITraceSink` 空接口、**不保留** `DropReason` 枚举、**不留** `trace_sink` 字段。半留一个接口而无人实现、无人发射，比删干净更糟——它会让读代码的人以为观测面还在，而实际上一个事件也不会发出。

  > 若将来重新需要可观测性，**应当重新设计**：现在这套的形态是"塌缩到只剩半条"的产物，不是一个值得继承的起点。

- **D4（框架内部的丢弃自此完全静默）：** 明确接受，见下。

## 明确接受的代价

1. **框架内部的丢弃完全不可观测、也无从归因。** 三条路径自此静默：

   | 路径 | 现状 |
   |---|---|
   | 读/写队列满 → 丢最旧 | 静默（`FiberChannel` 本就不报，本 ADR 之前也只有 trace 一条出口） |
   | 坏帧（`codec.Decode` 失败） | 静默（原 `kBadFrame`） |
   | 迟到 / 无匹配响应 | 静默（原 `kUnmatchedOrLateResponse`） |

   **排障只能靠宿主自己**在 codec 或订阅侧加日志。框架不提供任何切入点。

2. **丢失的是"框架侧的第一手现场"。** 宿主在 codec 外面看到的是"消息没来"，看不到"它到过、然后被丢了"，更看不到丢在哪一层。**这正是 D13 当初要解决的问题**，本 ADR 明确放弃。

3. **既有的两条 `DropReason` 有真实产生点**（`kBadFrame` / `kUnmatchedOrLateResponse`，各两处），**不是删死代码** —— 是把活着的归因一并撤掉。

## 影响（Consequences）

- **正面：** ① `core/` 少四个头文件、一个实现，四个配置结构体各少一个字段；② 两个节点的读循环与分发路径各少两处旁路调用；③ 不再需要维护"哪些丢弃该归因、归到哪一项"这套判断——#212 的那次收窄本身就说明它在持续腐化。
- **负面（明确接受）：** 见上三条。
- **对 SRS：** **`RT_TRACE_001` / `RT_TRACE_002` 撤销**；**§3.4.4 `RT_DATA_BUFFER`** 的计量部分撤销；追溯矩阵相应行删除。§3.6 的静默丢弃登记改为"框架不提供归因，由宿主自理"。
- **对 SDD：** **`DD-10`「可插拔观测 + 完整性归因」整条撤销**；`CSC_CORE` 登记与 `JK_TRACE` 相应删除。
- **对 CONTEXT.md：** **「丢弃归因」核心概念整条删除**。
- **对 ADR-0003：** **D13 被本 ADR 推翻**（其 Q1 双面观测、Q2 `DropReason` taxonomy、Q3 可审计表一并作废）。

## 备选方案（Alternatives considered）

- **只删 `RecordDrop` 与 `Cancellation`，保留 `ITraceSink` + `RecordEvent`。** **否决理由：** 那是本 ADR 之前的状态再修剪一点，问题不变——观测面仍是"塌缩后的残骸"，仍要维护归因判断，而实际价值已被 #152/#212 两次裁决掏空。要么留一个**完整可信**的观测面，要么删干净；留半条最坏。
- **保留 `ITraceSink` 空接口作为"以后再接"的钩子。** **否决理由：** 见 **D3**——无人实现、无人发射的接口比没有更糟，它让读代码的人误以为观测面还在。
- **保留 `DropReason` 供宿主自用。** **否决理由：** 它是**框架侧**归因的产物，宿主拿不到框架内部的丢弃时刻，这个枚举对宿主没有意义。
