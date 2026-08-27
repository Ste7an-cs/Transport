# ADR-0013：DDS 按双队列样板跟进，请求-响应用 `correlation_id` 走两阶段

**状态：** Proposed（**第二版**，2026-08-28 裁决后整体改写；第一版"优先用 DDS 自身机制"已作废，见文末「被推翻的第一版」）
**日期：** 2026-08-28
**关联：** ADR-0007（泵 + 读写双队列——本 ADR **沿用**其形态）；ADR-0011（TCP）、ADR-0012（串口）（同一形态的前两次跟进）；ADR-0008 **D1**（`ITransport` 七方法——**本 ADR 完整实现，不分叉**）、**D6**（`Dispatcher` 协议无关模板——复用）；ADR-0010（`RequestForResult` 的两阶段模型——**本 ADR 的请求-响应照此实现**）；SRS **RT_NODE_007**（本 ADR 修订其丢弃策略）。
**实施范围：** `DdsTransport` 与 `DdsNode`。调研见 **#190**，Fast DDS 3.6.1 实测见 **#191**。

## 背景（Context）

UDP（ADR-0007）、TCP（ADR-0011）、串口（ADR-0012）已按「读写双队列」落地。DDS 是最后一个介质。

现状：`DdsTransport` 引用着**已随 ADR-0008 D8 删除的 `BoundedQueue<Sample>`**，另有 `OperationOptions::Clock` / `SendUnit` / `Read()` / `Write(SendUnit)` 等已删符号——**须重写**。且本机已装 **Fast DDS 3.6.1**，而 `FastDdsProvider` 按 2.x 写（命名空间、`TopicDataType` 签名、`ReturnCode_t` 位置均变），**亦须重写**。

### 一处实测事实，决定了写侧的形状

**`DataWriter::write()` 的阻塞是线程级，不是 fiber 级**（#191 M2）：它是纯同步接口、无 continuation，在内部条件变量上 park **调用线程**——实测同一线程连续 **100.05ms**（默认 `max_blocking_time`）。`ASYNCHRONOUS_PUBLISH_MODE` **绕不过去**（#191 G 组同样 178/200 超时）——该模式挪走的是**网络发送**，而 `write()` 仍须先把样本**放进 writer 的 history**，`RELIABLE` + 满时卡住的是**准入**。

在协程模型里这会**卡死整条线程上的所有 fiber**。故写侧必须有一条**专属 OS 线程**。

### 另一处实测事实，决定了读侧可以极简

**外来线程 → fiber 的 `FiberChannel::push` 安全**（#190 Q1，11 组探针）：4 线程并发 8000/8000、20000 条严格连续无空洞、唤醒时延 avg 28µs / max 70µs。机理已核到源码——`push` 只做 `lock` + `push_back` + `notify_all`，**无等待路径**；文档那句 crash 警告只针对 `pop`。

故 **listener 可以在外来线程上直接 push 进 `read_queue_`**，读侧**连泵 fiber 都不需要**。

## 决策（Decision）

- **D1（形态照双队列样板，`DdsTransport` 完整实现 `ITransport`）：** 读写各一条内部队列，公开面即 `ITransport` 七方法，**签名不变、语义不变**。

  **与三介质的唯一形态差异是"谁在推读队列"**：UDP/TCP/串口是**泵 fiber** 从 socket/device 读流取数后推；DDS 是 **provider 的 listener 在外来线程**上取样本后推。**故 DDS 读侧没有泵 fiber。**

- **D2（读侧：listener 直推 `read_queue_`，无泵 fiber）：** `on_data_available` 回调在外来线程上 `take()` 全部可用样本，逐条 `push` 进 `read_queue_`；`AsyncRead()` 交出该队列句柄，与三介质完全一致。

  **明确接受的后果**：样本一经搬走，**DDS 即认为已交付、对 publisher 的背压随之解除**（#190 实测：我方队列满时静默丢弃 3976/5000 而 `push_fail=0`）。**这是本轮裁决明确接受的**——2026-08-28 裁决"不需要考虑优先使用 DDS 自己的机制"。代价见「明确接受的代价」①。

- **D3（写侧：`write_queue_` + 一条专属 OS 线程；`AsyncWrite` 仍是 fire-and-forget）：** `AsyncWrite` 只判生命周期与入队，**入队即返**——`ITransport` 的写契约（ADR-0007 D3）**照旧成立**。一条**专属 OS 线程**从 `write_queue_` 取出并调 `provider_->Publish()`。

  **`write_queue_` 不能是 `FiberChannel`**：消费方是**普通线程**，而 `FiberChannel` 的文档明载"**非协程线程上 `pop` 会 crash**"。须用 `std::mutex` + `std::condition_variable` + `std::deque`。

  **为什么是"专属线程"而不是三介质的"写泵 fiber"**：见背景——`Publish` 会 park 调用线程，用 fiber 会卡死整条线程上的所有 fiber。**这是 DDS 与三介质唯一的实质结构差异**。

  **由此写侧的阻塞对调用方完全不可见**：阻塞发生在专属线程上，业务 fiber 早已返回。写出的一切结果（含 `RETCODE_TIMEOUT`）**不回传，只落 `LastError()`**——与三介质逐字相同。

- **D4（QoS 统一一套，不按模式分）：** `DdsConfig::qos` 保持**单一** `DdsQos`，对所有 topic 一视同仁。`DeclareTopic(topic)` 只管建端点，**不带 QoS 参数**。

  2026-08-28 裁决："qos 都使用一样的"。

- **D5（`Message` 直接复用，只用 DDS 侧字段，其余留缺省）：** `Message` **已经同时带着**两套字段。DDS 路径**只用前一套**：

  | 用 | 留缺省 |
  |---|---|
  | `payload` / `topic` / `source` / `timestamp` | — |
  | `kind` / `correlation_id` / `reply_to`（DDS 交互元数据） | — |
  | — | **`frm_type = kUnknown` / `protocol_id = 0` / `session_id = 0` / `message_id = 0`** |

  **无须新增类型**；`DdsCodec` 的线缆格式恰好只编 DDS 侧那几个，天然对得上。

  线缆格式沿用现有 `DdsCodec`：`[kind:1][corr_len:2 BE][corr][reply_len:2 BE][reply_to][payload]`。**`topic` 不上线缆**——它是 DDS 的寻址维度，入站由 `Datagram.peer` 带出、出站由目的地给出，在线缆上再带一份是重复携带。

- **D6（`Dispatcher` 键为 `{topic, correlation_id, kind}`；公开面只暴露 `topic` 与 `kind`）：**

  ```cpp
  using DdsDispatcher = Dispatcher<Message, std::string /*topic*/,
                                            std::string /*correlation_id*/,
                                            MessageKind /*kind*/>;

  Ticket Subscribe(TopicKey topic, KindKey kind);   // 【一个方法】，两个键均可传 kAny
  ```

  | 用途 | 调用 | 实际键 |
  |---|---|---|
  | 订阅某 topic 的通知 | `Subscribe(topic, kNotify)` | `{topic, kAny, kNotify}` |
  | 收某 topic 的请求 | `Subscribe(topic, kRequest)` | `{topic, kAny, kRequest}` |
  | 收全部 topic 的通知 | `Subscribe(kAny, kNotify)` | `{kAny, kAny, kNotify}` |
  | `RequestForResultDirect` **内部**登记（结果，**只有一条**） | — | `{inbox_topic, corr, kReply}` |

  **不提供"订阅一个"与"订阅所有"两种固定变体**（2026-08-28 裁决）——"所有"由调用方传 `kAny` 表达。**`ServeRequests` 这个名字随之取消**：两个键全开放后它与 `Subscribe(topic, kRequest)` **完全等价**，留两个签名相同的方法只会让人以为有语义差别。

  **`correlation_id` 不进公开接口，内部恒 `kAny`。** 理由：

  - **请求-响应侧**：`corr` 由框架在 `RequestForResult` 内生成。**服务端事先不可能知道客户端会生成什么值**，客户端等应答用的具体值又是内部登记的——故公开出去只有 `kAny` 一个可用值，**那是陷阱不是灵活性**。
  - **发布-订阅侧**：`corr` 本可当"应用自定义子通道标识"（发布时填、订阅时按它过滤），但 **2026-08-28 裁决"发布订阅不需要这个能力"**。

  **连带**：`correlation_id` 自此**只有框架生成的关联符一个来源**，先前"同一字段服务两个目的、`kind` 传 `kAny` 时可能串"的隐患**一并消失**。

  **`topic` 在键内**，故不同 topic 上 `correlation_id` 相同的消息**不会互相误配**。

  **请求-响应用 `correlation_id`，不用 `session_id`/`message_id`**（2026-08-28 裁决）：后者是**外部协议**的匹配键，DDS 侧没有外部协议；`correlation_id` 是 `Message` 里为 DDS 路径预留的字段。

  **复用 `Dispatcher` 不等于"照搬 `ProtocolNode`"**：`Dispatcher<T, Fields...>`（ADR-0008 **D6**）是协议无关模板，调用方自备键提取函数。`ProtocolNode` 选 `(session_id, message_id, frm_type)` 是**它的协议**决定的；DDS 选 `(topic, correlation_id, kind)` 是**DDS 的寻址维度 + 其关联符 + 消息类别**决定的。这是 **DD-4「协议无关基座可复用」**。

- **D7（请求-响应照 `RequestForResultDirect` 实现：单阶段、等结果时重发、不回应）：** 2026-08-28 裁决改判——由 `RequestForResult` 的完整两阶段改为 **`RequestForResultDirect`**（ADR-0010 **D13**）。

  ```
  → kRequest
  ⏱ 等 kReply ──超时──▶ 重发 ──次数耗尽──▶ kTimeout
  ← kReply                                ⇒ 成功（【不回应】）
  ```

  映射：

  | `RequestForResultDirect`（ADR-0010 D13） | **DDS** |
  |---|---|
  | `kCommand`（请求） | `kRequest` |
  | `kResult`（结果，终结） | `kReply` |
  | **无受理阶段** | **无** `kFeedback` |
  | **收到即成功，不回应** | 同 |

  **四条纪律里三条随之不适用**——这正是 `RequestForResultDirect` 与 `RequestForResult` 的分界：

  | ADR-0010 的纪律 | 在本模型下 |
  |---|---|
  | 两个订阅在发出前一起登记（D4） | **不适用**——只有 `kReply` 一个订阅 |
  | 受理后立即 `ack.Reset()`（D5） | **不适用**——无受理阶段 |
  | **等结果阶段不重发**（D2） | **反转**——本模型**恰恰要在等结果阶段重发**（**D13**） |
  | 回应结果帧由收到的帧派生（D8） | **不适用**——收到即成功，**不回应** |

  **仍然沿用的两条**：

  - **先登记订阅、再发出**——这是 `Dispatcher` 用法的**固有要求**（应答可能先于订阅登记到达而被丢弃），与交互模型无关。
  - **重发沿用同一 `correlation_id`、发字节完全相同的原帧，以首帧为准**（ADR-0010 **D3**）。

  **签名里没有 `result_timeout`**：本交互只有**一个**等待阶段，其时限即 `RetryPolicy::timeout`。

  **耗尽返 `kTimeout`，不是 `kNotAccepted`**（ADR-0010 **D12**）：`kNotAccepted` 的语义是"对端**没有受理**"，而本模型**根本不存在受理这一步**。

  > **一处必须写明的依据差异**：ADR-0010 的 `RequestForResultDirect` 之所以要在等结果阶段重发，是因为**命令帧一旦丢包即彻底失败、无任何补救**。而 **DDS 是 `RELIABLE` 的，网络层不会丢**——那为什么还要重发？
  >
  > **因为丢的不是网络，是我方的队列。** 本 ADR **D11** 明确：`read_queue_` 有界 1024、满时**静默丢最旧**；**D2** 的代价 ① 又说明 listener 一搬走样本 DDS 即认为已交付、背压解除。**故请求或应答都可能在我方（或对端）的本地队列里被丢掉**，DDS 的 `RELIABLE` 覆盖不到这一段。**重发正是对这一段的补救。**
  >
  > **代价（明确接受）**：重发要求**对端能容忍重复请求**（幂等，或自行按 `correlation_id` 去重）。这是协议层假设，**框架不校验**（同 ADR-0010 D13）。

- **D8（`DdsNode` 两种模式，共四个方法；无旁路监听）：**

  ```cpp
  // —— 订阅：一个方法，覆盖发布-订阅与服务端收请求两种用途 ——
  Ticket Subscribe(TopicKey topic, KindKey kind);        // 两键均可 kAny，见 D6

  // —— 发布-订阅 ——
  Coro::Result<void> Publish(const std::string& topic, Message msg);

  // —— 请求-响应（客户端）：单阶段，等结果时重发，不回应（D7）——
  //     【没有 result_timeout】——只有一个等待阶段，其时限即 retry.timeout
  Coro::Result<Message> RequestForResultDirect(const std::string& topic, Message req,
                                               RetryPolicy retry);

  // —— 请求-响应（服务端）：【只有一个方法】——
  Coro::Result<void> Reply(const Message& request, Message result);  // 回结果 kReply
  ```

  **服务端没有 `Accept()`**：本模型无受理阶段（**D7**）。**`MessageKind::kFeedback` 在本设计中不使用**——它是 `Message` 为别的路径预留的值。

  **不提供旁路监听**（2026-08-28 裁决）。机制上 `Subscribe(kAny, kAny)` 可达，但**不作为受支持的用法写入接口文档**。

  **多 topic 用多次 `Subscribe`，每 topic 一条消费 fiber**——各自独立信箱，一路慢不拖累另一路（与 ADR-0009 D2 同向：节点只交出凭据，消费策略归宿主）。

- **D9（`CurrentLinkState()` 用 `matched` + `Liveliness`）：** `kDown` = `matched == 0` 或 `alive == 0`；`kUp` = `matched > 0` 且 `alive > 0`；`kEstablishing` = endpoint 已建但 `matched` 尚未 `> 0`（约 240ms 窗口，**无 DDS 原生事件**，由我方状态推出）。

  **必须配 `AUTOMATIC_LIVELINESS`**：实测仅靠 `matched` 时对端被**硬杀**要等 **~19.8 秒**（participant lease 默认 20s）才检出，**期间谎报 `kUp`**；配 `lease = 2s` 后 **2.0s 检出**。

- **D10（`DdsQos` 增两项；`DdsConfig` 不引入我方定义的时间量）：** `DdsQos` 增 `max_blocking_time` 与 `liveliness_lease`。

  **它们是转达给 DDS 的 QoS 参数，语义由 DDS 定义**，与三介质那种"我方定义的判活/退避时间量"性质不同。DDS 无 connect、无退避、无静默判活，故**不需要** `silence_timeout` 那类旋钮。

- **D11（丢弃策略与三介质一致；修订 `RT_NODE_007`）：** `read_queue_` 沿用默认「**有界 1024 + 静默丢最旧**」，与 UDP/TCP/串口逐字相同（SDD **DD-15**）。

  **`RT_NODE_007` 须修订**：它现要求"满时丢弃**最新**样本（tail-drop）并记 `ResourceExhausted`、丢弃**计数**和 Trace"。**三处与现状不符**：① `FiberChannel` 实际丢**最旧**；② 计数器自 ADR-0008 **D10** 起已不存在；③ 丢弃**静默**。改为与 DD-15 一致。

  **`DropReason::kDdsHandoffOverflow` 删除**（五项 → **四项**）：DDS 的 `read_queue_` 与三介质的同类，三介质都不为它单设归因项。

- **D12（配置校验，`Start()` 时一次性）：** 非法返 `kConfiguration`、**停在 `Created`**。

  | 字段 | 约束 |
  |---|---|
  | `domain_id` | `[0, 232]` |
  | `provider` | 非空且已注册 |
  | `max_blocking_time` / `liveliness_lease` | **须为正** |

- **D13（`IDdsProvider` 接口增删）：**

  | 变化 | 用途 |
  |---|---|
  | **新增** `SetDataObserver(std::function<void(Message)>)` | listener 在外来线程上回调，`DdsTransport` 据此 push 进 `read_queue_`（**D2**） |
  | **新增** `MatchedCount() → {matched, alive}` | **D9** 的判活 |
  | **改语义** `Publish` | 由"必成功"改为**可阻塞**（在专属线程上调用，**D3**） |
  | `Subscribe` / `Unsubscribe` / `Init` / `Shutdown` / `Name` | 不变（QoS 统一，无须增参） |

- **D14（`FastDdsProvider` 与 `FastDdsRawType` 按 Fast DDS 3.x 重写）：** 本机为 **3.6.1**，与代码假定的 2.13.x 是**大版本断裂**：包名 `fastrtps` → **`fastdds`**、命名空间 `eprosima::fastrtps::rtps` → **`eprosima::fastdds::rtps`**、`TopicDataType` 的 `serialize`/`deserialize` 由指针改引用并增 `DataRepresentationId_t`、`getSerializedSizeProvider` → **`calculate_serialized_size`**、`getKey` → **`compute_key`**、`fastrtps/types/TypesBase.h` **已不存在**。

  **`CMakeLists.txt:75` 的 `if(FALSE)` 硬禁用同时解除**，并改为 `find_package(fastdds)`。

  > **⚠ 一处会静默反转的缺陷，实现票必须显式处置**：`FastDdsProvider.cpp:105` 的
  > ```cpp
  > if (!writer->write(&copy)) return make_error_code(TransportErrc::kIo);
  > ```
  > 在 3.x 下**语义完全反转**——已复核 `DDSReturnCode.hpp`：`typedef int32_t ReturnCode_t; const ReturnCode_t RETCODE_OK = 0;`，而 `write()` 返回的正是 `ReturnCode_t`。**成功返 0 → `!0` 为真 → 返 `kIo`；失败返非 0 → 返成功。且零警告照常编译。**

## 明确接受的代价

1. **`RELIABLE` QoS 被本地队列架空。** listener 一搬走样本，DDS 即认为已交付、背压解除；我方 `read_queue_` 满时静默丢最旧（#190 实测丢 3976/5000 而 `push_fail=0`）。**这是 2026-08-28 裁决"不需要优先使用 DDS 自己的机制"的直接后果，明确接受。**

2. **丢弃不可观测。** 与三介质同性质（SRS §3.6 已登记的静默丢弃）。另：#191 实测 `KEEP_LAST` 下 **DDS 自己也不报**（丢 4995/5000 而 `on_sample_lost = 0`、`on_sample_rejected = 0`），故即便去查 DDS 侧也查不到。

3. **写侧多一条 OS 线程。** 这是四个介质里唯一需要额外线程的。代价是一次跨线程往返与线程本身的开销；换来的是 `Publish` 的线程级阻塞**不卡任何 fiber**。

4. **写出结果不回传。** `RETCODE_TIMEOUT` 只落 `LastError()`——与三介质一致，但对 DDS 而言意味着**背压信号被丢弃**：调用方无从知道"这条因对端消费不过来而没发出去"。

5. **`kEstablishing` 那约 240ms 窗口无 DDS 原生事件**，由我方状态推出。

6. **全部实测为单机 loopback、单次实跑**；跨主机时延/丢失率未测。

7. **`Shutdown()` 能否打断在途阻塞的 `Publish` —— 未实测**（#191 标注）。若不能，`Close()` 落在一次阻塞的写上时，`WaitClosed()` 最坏要等**一个 `max_blocking_time`**。**实现票须先补测**。

## 影响（Consequences）

- **正面：** ① 四个介质形态统一，`ITransport` 仍是**全介质**的内部缝；② `AsyncRead`/`AsyncWrite` 契约不分叉，"换传输即可运行"的调用方**含 DDS**；③ 请求-响应复用 ADR-0010 已验证的两阶段模型与四条纪律；④ 恢复 DDS 进入编译面。
- **负面（明确接受）：** 见上七条。
- **对 SRS：** **RT_NODE_007** 的丢弃策略修订为"丢最旧 + 静默"（与 DD-15 一致）；`DropReason` 五项减为**四项**；`RT_IF_DDS` 与 `DdsConfig` 登记同步；**RT_IN_INTERFACE_003** 的"非阻塞交接"仍成立（listener 侧 `push` 不阻塞）。
- **对 ADR-0002：** **D4**（交接边界有界 + tail-drop）的"tail-drop"部分**被修订**为丢最旧；有界部分沿用。

## 被推翻的第一版（2026-08-28，同日）

本 ADR 的第一版为「**优先使用 DDS 自身机制**」：拉取式读（`ReadNext()` 取代 `AsyncRead()`）、按模式分两套 QoS、写侧零队列、`DdsTransport` **不继承** `ITransport`、自定义两模式接口与旁路监听。

**整体作废**，依据是 2026-08-28 的裁决：不优先使用 DDS 自身机制、用双队列、参考已实现的介质、接口保持一致。

**第一版留下的两处判断在本版中仍然成立、已被吸收**：

1. **写阻塞是线程级** → 本版 **D3** 的专属 OS 线程。第一版的这一实测（含 `ASYNCHRONOUS_PUBLISH_MODE` 绕不过去）是本版写侧形态的直接依据。
2. **跨线程 `push` 安全、`pop` 不安全** → 本版 **D2**（listener 直推 `read_queue_`）与 **D3**（`write_queue_` 不能用 `FiberChannel`）。

**第一版声称、本版明确放弃的**：把流控交回 DDS QoS。本版接受 `RELIABLE` 被架空（代价 ①）。

## 备选方案（Alternatives considered）

- **写侧用 fiber 写泵（照三介质）。** **否决理由：** `Publish` 的阻塞是**线程级**，fiber 写泵会卡死整条线程上的所有 fiber。实测同一线程连续 park 100.05ms。
- **用 `ASYNCHRONOUS_PUBLISH_MODE` 免掉专属线程。** **否决理由：实测证伪**（#191 G 组 178/200 超时）——该模式挪走的是网络发送，`write()` 仍须先把样本放进 history，卡住的是准入。
- **读侧加一条转发泵 fiber**（listener → handoff → 泵 → `read_queue_`）。**否决理由：** 跨线程 `push` 已实测安全（#190 Q1），listener 可直推；多一跳只增延迟（#191 实测一跳 27µs vs 两跳 33µs）与一条 fiber。
- **请求-响应用 DDS 原生 `SampleIdentity` 关联。** **否决理由：** 会把 Fast DDS 的具体能力捅进 `IDdsProvider` 抽象层，`FakeDdsProvider` 还得模拟。2026-08-28 裁决为自定义 `correlation_id`。
- **提供"订阅一个"与"订阅所有"两种固定变体**（`Subscribe(topic)` + `SubscribeAll()`），以及独立的 `ServeRequests`。**否决理由：** 2026-08-28 裁决——键全开放后"所有"由 `kAny` 表达，固定变体是同一件事的两种写法；而 `ServeRequests` 与 `Subscribe(topic, kRequest)` **签名完全相同**，留两个名字只会让人以为有语义差别。
- **把 `correlation_id` 也暴露在公开订阅接口上。** **否决理由：** 见 **D6**——请求-响应侧它只有 `kAny` 一个可用值（服务端事先不知道客户端会生成什么），发布-订阅侧的"应用自定义子通道"能力**裁决为不需要**。暴露一个只能填一个值的参数是陷阱。
- **`CurrentLinkState()` 仅用 `matched`、不配 Liveliness。** **否决理由：** 对端被硬杀时要等 **~19.8 秒**才检出，期间**谎报 `kUp`**。
