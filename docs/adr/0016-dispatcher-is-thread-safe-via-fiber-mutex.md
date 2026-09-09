# ADR-0016：`Dispatcher` 转为跨线程安全，索引由 fiber 互斥量保护

**状态：** Accepted
**日期：** 2026-09-09
**关联：** **为 ADR-0008 D9（「单线程 fiber 协作模型显式化，普通成员不加锁」）开一处例外**——例外只及于 `Dispatcher`，两条泵、`ProtocolNode` / `DdsNode` 的其余成员一律不变；ADR-0008 **D6**（按键分配）；ADR-0001 已闭合的未决项（boost.fiber 同步件跨线程安全 = 成立，#71）。

## 背景（Context）

ADR-0008 **D9** 记的是一条**推理**：`Subscribe` / `Dispatch` / `~Ticket` 内均无挂起点，各条 fiber 同线程固定亲和，故互不交错，因而不必加锁。

这条推理的前提是「所有调用方都在同一个执行域内」。宿主一旦从**普通工作线程**调 `Subscribe` / `~Ticket`（把凭据交给非 fiber 线程持有，是很自然的用法），前提就塌了，且塌得很硬：

- `Dispatch` 正遍历 `state_->by_mask`，另一线程 `~Ticket → Unsubscribe` 走到 `by_mask.erase(...)` → 失效迭代器；
- 两条 `Subscribe` 并发 rehash 同一个 `unordered_map` → 撕裂的哈希桶；
- `next_id++` 撞车 → 两张凭据拿到同一个 id，注销时误删他人条目。

`tests/dispatcher_concurrency_test.cpp` 在**加锁前**跑，三轮全部 core dump，且在册数稳定少于登记数（784 / 794 / 798，应为 800）——不是理论风险。

### 一处不能忽略的事实：`resolve` 在多线程下**会**让出

D9 与 `Dispatcher.hpp` 的旧注释都写着「`resolve` 仅入队并标记等待者就绪，不引发 fiber 切换」。这话在单线程下成立，多线程下不成立：`Awaitable::resolve` 会进 `ChannelHub::push`，而 `ChannelHub` 与 `FiberChannel` 内部都取 `boost::fibers::mutex`。一有竞争，`resolve` 就是一个挂起点。

这直接决定了锁的选型与临界区的边界，见 D1 / D2。

## 决策（Decision）

- **D1（用 `boost::fibers::mutex`，不用 `std::mutex`）：** 索引锁必须是 fiber 版互斥量。fiber 等待时**让出 fiber 而不阻塞线程**；`std::mutex` 则阻塞整个线程，一旦持锁方在同线程上让出（见上）、而另一条 fiber 在同线程上阻塞等锁，持锁方永远等不到调度 → 自锁。

  与 `NodeBase` 的取舍不同：`NodeBase` 用 `std::mutex`（ADR-0003 D8）成立，是因为它的临界区里确实没有挂起点，且钩子一律锁外调用。

- **D2（锁放进 `State`，不放 `Dispatcher`）：** `Ticket` 只以 `weak_ptr<State>` 持索引，注销走 `static Unsubscribe(State&, ...)`。锁在 `State` 内，`weak_ptr::lock()` 成功即为其续命，故 `~Ticket` 与 `~Dispatcher` 并发亦安全；锁若放在 `Dispatcher` 里则根本护不到这条路径。

- **D3（临界区内不投递）：** `Dispatch` / `CloseAll` 在锁内**只做信箱快照**（纯内存操作、无挂起点），`resolve` / `close` 一律在锁外执行。`Subscribe` 命中「已关闭」时的 `mailbox_->close()` 同样挪到锁外。理由有二：持锁跨挂起点会把索引的持锁期拖成不可控；被唤醒方若回调本件即撞上同一把**不可重入**的互斥量，当场自锁。

- **D4（`key_of` 在锁外求值，且不得回调本件）：** `key_of` 构造后只读（已改为 `const`），不受锁保护。「其内不得回调本件」由原来的约定升级为**硬约束**——锁不可重入。

- **D5（诊断面同样加锁）：** `Size()` / `ProbeCount()` 并发下遍历同一个 map 一样会崩，故一并取锁。`shared_ptr::operator->` 给出非 const 的 `State&`，const 成员函数里可直接取锁，无需把 mutex 标 `mutable`。

- **D6（例外只及于 `Dispatcher`）：** 本 ADR **不**声称 `ProtocolNode` / `DdsNode` / 各传输可跨线程使用。它们的其余成员仍按 ADR-0008 D9 不加锁，交互方法仍须在节点所属执行域内调用。跨线程可调的只有：`Dispatcher` 的全部公开方法、`Ticket` 的注销，以及 `NodeBase` 本就加锁的生命周期方法。

## 明确接受的代价

- **注销与投递之间有一个窗口**：已进入投递快照的凭据，即使随后 `Reset()`，仍会收到那一条消息。快照持 `shared_ptr<Awaitable>`，信箱必然存活，故**不构成悬垂**——只是投进了无人再读的信箱。这与「注销后本凭据不再接收消息」的旧措辞有出入，已在 `Dispatcher.hpp` 的类注释里写明。

  要消掉这个窗口就得把投递放回锁内，代价是 D3 的两条风险全部回来。判断：窗口是良性的（多收一条、无内存错误），自锁不是，故取窗口。

- **`~Ticket` / `Ticket::Reset()` 从「无挂起点」变成「可能让出 fiber」**：持锁期只有一次索引摘除、不含挂起点，故等待有界；但析构期间让出意味着别的 fiber 可能观察到半析构的宿主对象。已在头文件写明，宿主须自行留意。

- **单线程路径上多了一把无竞争的锁**：`boost::fibers::mutex` 在无竞争时是一次原子操作，不进调度器。相对于每条消息本就要做的哈希查找与信箱 push，这一项不显著；未做基准测量，此处不作性能声称。

## 影响（Consequences）

- `Dispatcher` 的公开语义**不变**（签名、返回值、匹配与投递规则一字未改），宿主无需改动调用点。
- ADR-0008 **D9** 的表述需读作「除 `Dispatcher` 外」；`Dispatcher.hpp` 文件头原「本件面向单线程 fiber 协作模型，不加锁」一段已整段改写为「线程安全」小节。
- `ProtocolNode.hpp` / `DdsNode.hpp` 的「与传输、`Dispatcher` 一致……普通成员不加锁」需去掉「与 `Dispatcher` 一致」的并列——`Dispatcher` 已不在此列。
- 新增 `tests/dispatcher_concurrency_test.cpp`（5 个用例）守三条不变量：并发登记/注销后在册数精确、边投递边增删不崩且终态为空、`CloseAll` 与 `Subscribe` 对撞后不留悬挂条目。该组用例在加锁前会稳定失败并 core dump，即负控成立。

## 备选方案（Alternatives considered）

- **把跨线程调用挡在门外（维持 D9，要求宿主自行 post 回执行域）**：语义上最省，但把一件通用件的正确性押在调用方纪律上；而「凭据交给工作线程持有」是宿主的合理用法，节点无从检测违例，崩溃现场也难以归因。否决。
- **用 `std::mutex`**：D1 已述，与 fiber 让出直接冲突，会自锁。否决。
- **把投递留在锁内**：语义更严（无窗口），但引入不可重入自锁与不可控持锁期，见 D3。否决。
- **无锁索引（RCU / 写时复制 `by_mask`）**：投递侧免锁，但订阅侧的每次增删都要复制整张索引，而本件的实际负载恰是「短寿凭据频繁增删」（每次请求-响应都要登记两张）。收益反了。否决。
