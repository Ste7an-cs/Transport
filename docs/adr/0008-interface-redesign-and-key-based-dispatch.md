# ADR-0008：接口重设计——传输/节点接口收窄、请求关联改为按键分配、手搓同步件一律换用 AsyncTask 原语

**状态：** Accepted
**日期：** 2026-08-19
**关联：** 修订 ADR-0004（**D1** 终止表达、**D2** 观测面）；**推翻** ADR-0005 **D1/D2/D5**（收敛在读循环、三方汇合、致命错误自终）；**推翻** ADR-0006 **D1/D3/D4/D6/D8**（NodeBase 模板方法形态、轻量 SharedCompletion、HandlerLoop 内件、收敛不可下放、重入守卫之争）；扩展 ADR-0007 **D1/D3/D4**（泵形态保留，接口表达改写）。
SRS 落点：**推翻 RT_REQUEST_005/006**（关联标识退休窗口、256 在途上限与 `ResourceExhausted`）；改写 RT_LIFECYCLE_005/006（关闭的等待入口与收敛承担者）；改写 RT_TRANSPORT_008/010 的接口承载形式；作废 §3.6 丢弃归因等式的可验证性。
**实施范围：** `ITransport` / `NodeBase` / `ProtocolNode` / `UdpTransport` 已落地；TCP / 串口 / DDS 及其节点暂排除于编译面，按同一形态后续跟进。

## 背景（Context）

ADR-0004 至 0007 逐票推进了传输语义统一、生命周期收敛与泵形态，但每一票都在既有接口上做**增量修补**。累积到本轮，三处结构性问题已无法再靠增量解决：

1. **接口面持续膨胀且职责混杂。** `NodeBase` 长到 16 个成员函数、10 个成员变量，其中 5 个是纯观测 getter，3 个是"子类在钩子中途回调基类"的反向动作（`MarkRunning` / `SignalClose` / `ConvergeAfterReadLoop`）。`ProtocolNode` 有 8 个观测接口，其中 5 个是纯转发。

2. **大量与 AsyncTask 重复的手搓件。** `SharedCompletion` 重造 `Awaitable::close()` 的广播；`BoundedQueue` 重造 `FiberChannel`（有界 FIFO + 消费者协作等待 + 带原因关闭）；`PendingTable` 的核心不过是一张 `map<Key, shared_ptr<Awaitable<T>>>`；`OperationOptions` 与 `transport::Result` 则是两个空别名。它们各自都要维护同步纪律、各自都可能有自己的缺陷（`SharedCompletion` 的 latch 在 #150 中被证实会跨尝试泄漏结果）。

3. **请求关联的表达力不足。** `CorrelationKeyStrategy` 把 Message 压成单一 `uint32` 再比相等，只能全等匹配、只能一对一投递。而真实交互存在"一次请求等两段回帧"（先等同命令码的回应，再等另一命令码的结果）与"多个消费者同时消费同一条消息"（业务等待 + 旁路审计）两类需求，前者要求一次交互登记多段等待，后者要求同一条消息投给多个订阅者——两者在旧机制下都无法表达。

## 决策（Decision）

### 一、传输与节点的接口形态

- **D1（`ITransport` 收窄至七个方法，读写刻意不对称）：**

  | 组 | 方法 | 语义 |
  |---|---|---|
  | 任务 | `Start()` / `Close()` / `WaitClosed()` | 起泵后即返回 / 只发信号 / join 全部内部工作单元 |
  | 数据 | `AsyncRead()` / `AsyncWrite(Datagram)` | 交出读队列的等待器句柄 / 送入一份数据 |
  | 观测 | `LastError()` / `CurrentLinkState()` | 每种介质都答得出的最小公分母 I/O 事实 |

  读是"数据什么时候来"，只能交出等待器，由调用方自行决定超时、取消与是否扇出；写是"把这份数据发到那里去"，调用方给完即返回。因此**写队列是纯内部的**，调用方不感知其存在。删除 `LastSendTime` / `LastReceiveTime`——两个时间戳无人消费，且与 `LastError()` / `CurrentLinkState()` 不同，它们不是"此刻的 I/O 事实"而是历史记录。

- **D2（`Close()` 只发信号，`WaitClosed()` 单独汇合；传输与节点同形）：** 旧 `NodeBase::Close()` 结尾无条件等待收敛，导致内部工作单元调用它即等于等自己退出、静默挂死。ADR-0005 D6 曾试图以重入守卫解决，ADR-0006 D8 又将其撤销，最终落成一条**使用契约**（靠人记住"内部工作单元只能走 `SignalClose()`"）。

  本 ADR 取消该契约：`Close()` 不含任何等待点，**任何 fiber 都可调用**，包括节点自己的读循环与业务处理器。由此删除 `SignalClose()`、`ConvergeAfterReadLoop()`、`MarkRunning()`、`JoinHandler()`、`DrainUnstartedBusiness()` 五个反向回调，`NodeBase` 收为 4 个公开方法 + 3 个钩子（`DoStart` / `DoClose` / `DoJoin`）+ 4 个成员。

- **D3（`WaitClosed()` 不设时限，语义即 join）：** `Awaitable::close()` 只保证等待者被**唤醒**，不保证被唤醒的 fiber 已跑完并不再触碰对象成员；而"可安全释放"要求的恰是后者，只有 `Coro::FiberTask::get()` 给得了，它没有超时。故 deadline 与"可安全释放"二选一，取后者。超时返回后调用方本也无事可做（仍不能析构对象），该参数并无价值。

  推翻 ADR-0005 D1（收敛由读循环兼任）与 D2（三方汇合的完成量协议）：收敛不再由内部 fiber 自行完成，而由 `WaitClosed()` 的调用方经 `DoJoin()` 自上而下 join。

### 二、写侧语义

- **D4（写侧彻底 fire-and-forget，取消最后的同步作答口子）：** `AsyncWrite` 只判两件事：生命周期是否允许写、是否真的入队。**写出的一切结果——目的地能否解析、socket 是否写成——都不回传，只落 `LastError()`。** 链路不可用时数据留在内部队列等待恢复，不拒绝、不丢弃。

  「帧字节进入操作系统发送缓冲才算完成 + 协程背压」这条要求**并非本 ADR 撤销**——它已于 2026-08-06 的需求重审随 ADR-0004 删除（见 SRS §3.1.3「发送侧无框架级缓冲上界」）。本 ADR 变更的是**剩下的那半**：ADR-0007 D3 为 UDP 采纳 fire-and-forget 时仍保留了"目的地非法等参数错误同步作答"的例外，而 `AsyncWrite` 收窄为单参数（`Datagram`）后该例外无处安放；一个半 fire-and-forget、半同步作答的写接口，其错误语义比两种纯粹形态都更难说清，故取消之。

  由此 SRS §3.1.3 中"UDP/DDS 单次发送超出介质或配置允许大小时应在发送前失败"一条**不再成立**——超长报文入队成功，失败只落 `LastError()`。

### 三、节点与传输的关系

- **D5（节点不管传输的生命周期）：** `ProtocolNode` 改为**按引用借用**传输，宿主负责其 `Start()` / `Close()` / `WaitClosed()`。节点读侧走 `AsyncRead()->shared()` 取自己的一路订阅，`DoClose()` 关闭该订阅。

  由此"节点关闭"与"传输关闭"在**职责**上分离（谁创建谁关闭），代价是宿主要多写两行启停，且传输的寿命必须长于节点。

  > **更正（2026-08-27，AsyncTask 升至 `417790c`）**：原文"`DoClose()` **只**关闭该订阅，源队列与其它订阅者**不受影响**"，以及据此推出的"多个节点可共用一条传输"，**表述有误**。
  >
  > 上游 `3818265` 起 `Awaitable::close()` **整流传播**——关闭 hub 表里**全部**消费者队列，源队列与其它订阅者**一并终结**（实测确认）。**这正是本项目要的语义**：节点关闭即读侧终结，宿主随后关传输，**两者一起关**。
  >
  > 修正后的准确表述：多个节点**可以**共用一条传输并各得全量副本，但它们在关闭上**一荣俱荣**——**任一节点 `Close()` 即终结整条读流**，不支持独立关停。"节点关闭与传输关闭彻底分离"这句就**读侧数据流**而言不成立，仅就**生命周期职责归属**而言成立。
  >
  > 新语义下"只退订自己"的做法是**析构句柄**（`~Awaitable()` 内 `hub_->detach()`）；`ProtocolNode` **不走**该路径——它唤不醒正阻塞在 `await(rx_)` 上的读循环。

### 四、请求关联

- **D6（请求关联改为按键分配，新增 `core/Dispatcher.hpp`）：** 删除 `PendingTable` 与 `CorrelationKeyStrategy`，代之以协议无关的 `Dispatcher<T, Fields...>`：

  - 调用方只提供**键提取函数**，给出一条消息各匹配字段的具体值；
  - 订阅时不参与匹配的字段填 `kAny`，**部分匹配由本件实现**，调用方不写通配逻辑、不用哨兵值、不枚举字段组合；
  - `kAny` 以 `std::optional` 的持值状态表达，**不占用字段值域**，故 `session_id` 这类 0..255 全用满的字段同样可以通配；
  - 一条消息投给**全部**键匹配的订阅者，各得一份副本——多消费者与旁路监听由此成立；
  - 内部按"哪些字段被约束"（mask）分层索引，单条消息成本 **O(在用 mask 种数 + 收件人数)**，与订阅者总数无关。

  `ProtocolNode` 的关联逻辑因此收为一行 `make_tuple(session_id, message_id, frm_type)`，并新增 `Subscribe(Key)` 供分段交互与旁路监听。`kResponseMarker` 及"响应命令码归一化"随之删除——帧类型成为键的独立字段后，请求与回应可直接区分。

- **D7（`session_id` 简化为自增计数器）：** 取消 0..255 空闲集、FIFO 退休窗口、RAII 租约与 `kResourceExhausted` 边界。`std::uint8_t` 计数器每次取用后自增、越过 255 自然回绕。它只用于区分近期的并发交互，不再充当并发上限。推翻 RT_REQUEST_005（LRU 退休窗口）与 RT_REQUEST_006（256 全在途即拒绝）。

### 五、共用件与同步纪律

- **D8（手搓同步件一律换用 AsyncTask 原语）：**

  | 删除 | 替代 |
  |---|---|
  | `SharedCompletion<T>` | `Awaitable::close()` 广播 + `FiberTask::get()` 汇合 |
  | `BoundedQueue<T>` | `Coro::Awaitable<T>` + `setCapacity`（`FiberChannel` 本就是有界 FIFO） |
  | `PendingTable<Key,T>` | `Dispatcher`（见 D6） |
  | `OperationOptions` | 超时直接用 `std::chrono::milliseconds` |
  | `transport::Result<T>` / `Status` | `Coro::Result<T>` / `Coro::Result<void>`（前者本就只是别名） |
  | `SendUnit` | 与 `Datagram` 结构相同，合并；字段 `source` 更名 `peer` |

  `Datagram::peer` 的方向由**使用它的接口**决定：读侧是发送方，写侧是目的地。两个结构相同的类型不需要靠类型名编码方向。

- **D9（单线程 fiber 协作模型显式化，普通成员不加锁）：** 两条泵与节点的各条 fiber 均以 `Coro::makeTask` 起，默认亲和为 `fixed(调用线程)`，与公开方法同线程、仅在挂起点交错；而 `Subscribe` / `Dispatch` / session 取用 / socket 状态判定路径内均无挂起点，故互不交错。队列自身（`Coro::Awaitable`）另有其内部同步。

  代价是**公开方法必须在起它的那个执行域内调用**——这本就是 Qt 对象亲和的既有要求（socket 建在该线程上），本 ADR 只是把这条隐含前提显式化，并停止用互斥量假装支持跨线程。

- **D10（删除全部观测计数接口，观测只经 trace）：** `NodeBase` 的 `CloseDropCount` / `LastCloseLatency`、`ProtocolNode` 的 8 个计数与时延 getter、`HandlerLoop` 的 3 个计数、传输的两个时间戳一律删除。丢弃改为只经 `ITraceSink` 上报（`RecordEvent(category="drop")`），一个事实一条出口。要统计就在 sink 里统计。

## 影响（Consequences）

**接口面**

| | 之前 | 之后 |
|---|---|---|
| `ITransport` 方法 | 10 | 7 |
| `NodeBase` 公开方法 / 钩子 / 受保护动作 / 成员 | 6 / 5 / 3 / 10 | 4 / 3 / 0 / 4 |
| `ProtocolNode` 公开方法 | 11 | 5 |
| 手搓同步件 | 6 | 0 |

**已知的能力回退（一并接受，不留隐性欠账）**

1. **写侧无任何同步错误**（D4）。除生命周期外，发起方无从在调用点感知任何写出问题：目的地非法、报文超长、socket 写失败一律只落 `LastError()`。SRS §3.1.3"超出允许大小应在发送前失败"作废。背压能力此前已随 ADR-0004 的需求重审丧失，本轮不再改变。
2. **业务队列失去字节上界与 tail-drop**（D8）。改为静默丢**最旧**且无计数、无归因；对业务帧而言语义是"丢老留新"，与旧的 tail-drop 相反。§3.6「Σ 命名原因 == 总丢弃」的 loss=0 等式因计数面删除而不再可验证（#152）。
3. **`Request` 失去取消令牌**（D8）。在途请求只能等超时，宿主无法半路撤回。
4. **在途交互超过 256 时 `session_id` 重复**（D7）。重复的键意味着两个订阅落入同一桶，一条响应将同时投给二者。若协议存在此量级的并发，应在键中引入更宽的区分字段，而非回到号池。
5. **临时端口在重建后换号**。`UdpConfig::local_port = 0` 时每次重 bind 取得不同端口；静默超时默认 5 秒后，空闲链路会周期性重建，对端若记住源端口将失联。

**测试与迁移**

`Dispatcher`（19）、`ProtocolNode`（23）、`UdpTransport`（22）三组用例已按新形态重写，全量 112 tests 通过。TCP / 串口 / DDS 及其节点、以及八个仍在旧接口上的 node 侧用例暂排除于编译面，清单记在 `CMakeLists.txt` 注释中。

## 备选方案（Alternatives considered）

- **谓词式分配（`std::function<bool(const T&)>`）** —— 表达力最强，部分匹配天然成立。**否决**：每条消息需遍历全部订阅者求值，256 个在途请求即 256 次谓词调用；且无法支持"多个消费者同时消费"之外的索引优化。
- **通配哨兵值 + 调用方枚举泛化层级** —— 键仍是整数、查表 O(1)。**否决**：`session_id` 值域已被 0..255 占满，带内无哨兵可用；且字段一多，调用方要自行列出 2ⁿ 种组合，等于把库该做的事推给使用者。
- **`Key` 锁死为 `uint64_t`** —— 哈希最省、无分配。**否决**：把协议逼进整数编码，字符串键（DDS topic、HTTP 路径）装不下。改为模板化，键的形状与泛化规则同属节点的知识。
- **保留 `Close()` 的等待语义 + 运行时重入守卫**（ADR-0005 D6 的原方案）—— **否决**：守卫要比对 fiber id、要设"半执行"分支，而拆成 `Close()` + `WaitClosed()` 之后问题根本不存在。
- **保留观测计数接口以维持 loss=0 等式** —— **否决**：8 个 getter 中 5 个是纯转发，且计数与 trace 是同一事实的两条出口。等式改由 sink 侧统计重建（尚未实施，见 #152）。
