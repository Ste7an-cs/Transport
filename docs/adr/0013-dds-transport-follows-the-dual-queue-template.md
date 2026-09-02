# ADR-0013：DDS 按双队列样板跟进，请求-响应用 `correlation_id` 走单阶段 `Direct`

**状态：** Proposed（**第二版**，2026-08-28 裁决后整体改写；第一版"优先用 DDS 自身机制"已作废，见文末「被推翻的第一版」）
**日期：** 2026-08-28。**2026-08-31 三轮增补**：① 应答 topic 改为每服务共用、`correlation_id` 定为两段式（**D6**）；② 端点声明去掉懒声明、全部前置（**D15**）；③ topic 由**注册接口**给出、不进配置（**D16**）。
**关联：** ADR-0007（泵 + 读写双队列——本 ADR **沿用**其形态）；ADR-0011（TCP）、ADR-0012（串口）（同一形态的前两次跟进）；ADR-0008 **D1**（`ITransport` 七方法——**本 ADR 完整实现，不分叉**）、**D6**（`Dispatcher` 协议无关模板——复用）；ADR-0010 **D13**（`RequestForResultDirect` 的**单阶段**模型——**本 ADR 的请求-响应照此实现**；同 ADR 的两阶段 `RequestForResult` **本 ADR 不用**）；SRS **RT_NODE_007**（本 ADR 修订其丢弃策略）。
**实施范围：** `DdsTransport` 与 `DdsNode`。调研见 **#190**，Fast DDS 3.6.1 实测见 **#191**。

## 背景（Context）

UDP（ADR-0007）、TCP（ADR-0011）、串口（ADR-0012）已按「读写双队列」落地。DDS 是最后一个介质。

现状：`DdsTransport` 引用着**已随 ADR-0008 D8 删除的 `BoundedQueue<Sample>`**，另有 `OperationOptions::Clock` / `SendUnit` / `Read()` / `Write(SendUnit)` 等已删符号——**须重写**。且本机已装 **Fast DDS 3.6.1**，而 `FastDdsProvider` 按 2.x 写（命名空间、`TopicDataType` 签名、`ReturnCode_t` 位置均变），**亦须重写**。

### 一处实测事实，决定了写侧的形状

**`DataWriter::write()` 的阻塞是线程级，不是 fiber 级**：它是纯同步接口、无 continuation，直接 park **调用线程**。在协程模型里这会**卡死整条线程上的所有 fiber**。故写侧必须有一条**专属 OS 线程**。

**阻塞的真实机理是【同进程交付在发布线程上同步执行】**（2026-09-01 实测，#202 复核）。Fast DDS 默认 `INTRAPROCESS_FULL`，**同进程订阅方的 `on_data_available` 直接跑在发布线程上**，`Publish` 被它完整拖住：

| 设置 | `Publish` 耗时 | 回调线程 |
|---|---|---|
| 默认 `INTRAPROCESS_FULL` | **2000–2009ms**（= 订阅回调的睡眠时长） | **与发布线程同一条** |
| `set_library_settings(INTRAPROCESS_OFF)` | **1–2ms** | 不同线程 |

**`max_blocking_time` 根本不参与**——上表第一行里它设为 300ms，`Publish` 照样跑满 2000ms。**这段阻塞没有上界，界由对端回调决定。**

> **⚠ 本 ADR 初稿的归因已被证伪，记此以免再被引用。** 初稿写的是「`RELIABLE` + history 满时卡住的是**准入**，实测同线程连续 park 100.05ms（#191 M2）」。**该归因不成立**：已核 3.6.1 的 `DataWriterHistory::prepare_change`——history 满时**只有 `KEEP_ALL` 才等**，`KEEP_LAST` 直接丢最旧、不等；而 `DdsQos` 铺的正是 `KEEP_LAST` 且不暴露 `KEEP_ALL`。向卡死不 take 的 `RELIABLE` reader 连发 3000 条 60KB（`history_depth` 取 1 与 400），总耗时 56ms/79ms、单次最长 **0ms**、零阻塞。
> **结论方向不变（写侧仍须专属 OS 线程），换的是依据。** 原先那条 `ASYNCHRONOUS_PUBLISH_MODE` 的否决（#191 G 组 178/200 超时）**仍然成立**——它挪走的是网络发送，挡不住同进程同步交付。

### 另一处实测事实，决定了读侧可以极简

**外来线程 → fiber 的 `FiberChannel::push` 安全**（#190 Q1，11 组探针）：4 线程并发 8000/8000、20000 条严格连续无空洞、唤醒时延 avg 28µs / max 70µs。机理已核到源码——`push` 只做 `lock` + `push_back` + `notify_all`，**无等待路径**；文档那句 crash 警告只针对 `pop`。

故 **listener 可以在外来线程上直接 push 进 `read_queue_`**，读侧**连泵 fiber 都不需要**。

## 决策（Decision）

- **D1（形态照双队列样板，`DdsTransport` 完整实现 `ITransport`）：** 读写各一条内部队列，`ITransport` 七方法**签名不变、语义不变、不分叉**。

  公开面**在七方法之外另有两个 DDS 专有的声明方法**（`DeclareWriter` / `DeclareReader`，**D15**）——它们是 DDS 端点模型的必需品，三介质没有对应物；**但它们不改动 `ITransport` 本身**，故"换传输即可运行"的调用方仍不受影响。

  **与三介质的唯一形态差异是"谁在推读队列"**：UDP/TCP/串口是**泵 fiber** 从 socket/device 读流取数后推；DDS 是 **provider 的 listener 在外来线程**上取样本后推。**故 DDS 读侧没有泵 fiber。**

- **D2（读侧：listener 直推 `read_queue_`，无泵 fiber）：** `on_data_available` 回调在外来线程上 `take()` 全部可用样本，逐条 `push` 进 `read_queue_`；`AsyncRead()` 交出该队列句柄，与三介质完全一致。

  **明确接受的后果**：样本一经搬走，**DDS 即认为已交付、对 publisher 的背压随之解除**（#190 实测：我方队列满时静默丢弃 3976/5000 而 `push_fail=0`）。**这是本轮裁决明确接受的**——2026-08-28 裁决"不需要考虑优先使用 DDS 自己的机制"。代价见「明确接受的代价」①。

- **D3（写侧：`write_queue_` + 一条专属 OS 线程；`AsyncWrite` 仍是 fire-and-forget）：** `AsyncWrite` 只判生命周期与入队，**入队即返**——`ITransport` 的写契约（ADR-0007 D3）**照旧成立**。一条**专属 OS 线程**从 `write_queue_` 取出并调 `provider_->Publish()`。

  **`write_queue_` 不能是 `FiberChannel`**：消费方是**普通线程**，而 `FiberChannel` 的文档明载"**非协程线程上 `pop` 会 crash**"。须用 `std::mutex` + `std::condition_variable` + `std::deque`。

  **为什么是"专属线程"而不是三介质的"写泵 fiber"**：见背景——`Publish` 会 park 调用线程，用 fiber 会卡死整条线程上的所有 fiber。**这是 DDS 与三介质唯一的实质结构差异**。

  **由此写侧的阻塞对调用方完全不可见**：阻塞发生在专属线程上，业务 fiber 早已返回。写出的一切结果（含 `RETCODE_TIMEOUT`）**不回传，只落 `LastError()`**——与三介质逐字相同。

  ### ⚠ 专属写线程会顺带跑掉同进程对端的交付回调

  见背景：Fast DDS 默认 `INTRAPROCESS_FULL`，**同进程订阅方的 `on_data_available` 在发布线程上同步执行**。故这条专属写线程实际上**兼跑同进程内所有对端的交付回调**，且**没有上界**。

  由此，「**读侧 listener 必须快且不阻塞**」从建议变成**硬约束**：

  - **我方的 listener 满足**（**D2**）：它只做 `push` 进 `read_queue_`——`lock` + `push_back` + `notify_all`，**无等待路径**，队列满时丢最旧也不阻塞。
  - **同进程内的【非本框架】订阅方不受我方约束**：任何一个慢回调都会**卡住我方整条写队列**（不只是那一条消息）。这是部署面的约束，**框架无法强制**，须写进使用文档。
  - **想彻底隔断**只有一条路：`set_library_settings(INTRAPROCESS_OFF)`（实测 `Publish` 从 2000ms 降到 1–2ms）。**本设计不默认关闭**——那会牺牲同进程通信的零拷贝路径，且属于进程级全局设置，替调用方决定是越权。

- **D4（QoS 统一一套，不按模式分）：** `DdsConfig::qos` 保持**单一** `DdsQos`，对所有 topic 一视同仁。`DeclareWriter` / `DeclareReader`（**D15**）只管建端点，**不带 QoS 参数**。

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

  // 【一个方法】，两个键均可传 kAny。返 Result 而非裸 Ticket —— 见 D8
  Coro::Result<Ticket> Subscribe(TopicKey topic, KindKey kind);
  ```

  | 用途 | 调用 | 实际键 |
  |---|---|---|
  | 订阅某 topic 的通知 | `Subscribe(topic, kNotify)` | `{topic, kAny, kNotify}` |
  | 收某 topic 的请求 | `Subscribe(topic, kRequest)` | `{topic, kAny, kRequest}` |
  | 收全部 topic 的通知 | `Subscribe(kAny, kNotify)` | `{kAny, kAny, kNotify}` |
  | `RequestForResultDirect` **内部**登记（结果，**只有一条**） | — | `{该服务的应答 topic, corr, kReply}` |

  **应答 topic 是【每服务一个、该服务的全体客户端共用】的**（2026-08-31 裁决）。**"每服务一个"是字面意思——它绑在【服务】上，不是绑在【节点】上**：

  ```cpp
  // 请求 topic ──► 该服务的应答 topic，【一服务一条】。两侧【实参一模一样、端点方向相反】（D16）
  RegisterClients ({{"cfg.get", "cfg.get.reply"}, {"log.tail", "log.tail.reply"}});  // 客户端
  RegisterServices({{"cfg.get", "cfg.get.reply"}, {"log.tail", "log.tail.reply"}});  // 服务端
  ```

  `RequestForResultDirect(topic, ...)` 按 `topic` 查已注册的 `Clients` 表取应答 topic；**查不到即 `kConfiguration`，不猜、不回落到某个默认值**。

  **故一个客户端同时调多个服务时，各服务的应答落在各自的 topic 上，互不相扰**——这正是"每服务一个"要保证的。若把它做成节点级的单一字段，多个服务的应答会挤在一起、读入放大从 `N` 倍恶化为 `N×M` 倍（`M` 为服务数），**那不是本裁决的意思**。

  **不用约定式派生**（如 `<topic>/reply`）：省下的是一行配置，代价是 topic 命名空间被框架侵占，且与既有 topic 命名冲突时无从规避。显式表更笨但可控。

  区分**同一服务的不同客户端**则**全靠 `correlation_id`**，故它必须**全局唯一**，为此定死两段式构成：

  ```
  correlation_id = "<uuid>#<request_seq>"
                      ↑          ↑
        节点初始化时生成一次    uint32，从 0 开始自增，每请求一个
  ```

  | 半段 | 谁生成 | 何时 | 保证什么 |
  |---|---|---|---|
  | **uuid** | 节点自身 | **初始化时一次**，此后不变 | **跨节点**不撞——这是共用 topic 得以成立的全部根据 |
  | **`request_seq`**（`uint32`） | 节点内自增计数器 | 每次 `RequestForResultDirect` | **节点内**不撞 |

  **自增半段叫 `request_seq`，【不叫 `session_id`】**：`Message::session_id` 是**外部协议**的匹配键，**D5** 刚明确 DDS 路径把它留缺省 `0`。同一份代码里出现两个语义无关的 `session_id` 是确定的阅读陷阱，故改名。

  **`uint32` 回绕（约 42.9 亿次请求后）明确接受，不加防回绕逻辑**：回绕后重复的是**本节点很久以前**用过的值，而那一条订阅早已注销——`Dispatcher` 里已无对应登记，不会误配。

  **uuid 用 `QUuid::createUuid()`**（2026-08-31 裁决）。`Qt5::Core` 已 `PUBLIC` 链进 `transport` 目标（`CMakeLists.txt:71`），**不引入新依赖**；取字符串用 `toString(QUuid::WithoutBraces)`（36 字符）。
  **不自搓**（`random_device` 在 WSL 下质量存疑，且要自行论证碰撞率），**不引第三方 uuid 库**（为一个字段引依赖不划算）。

  **配 `uuid_override` 保住确定性可测**：`QUuid` 是随机的，而被本设计取代的 `DdsNodeConfig::node_id` 其注释明写「**不用随机数（确定性可测，RT_REQUEST）**」——旧设计特意让 corr 前缀确定，好让测试断言具体值。故 `DdsNodeConfig` 留一个 `uuid_override`：**非空则用它，为空才 `QUuid::createUuid()`**。测试注入固定值，生产留空。一行的事，不该把可测性丢掉。

  `Message::correlation_id` 本就是 `std::string`（`Message.hpp:49`），`36 + 1 + ≤10 = ≤47` 字节，**装得下，不需要改结构**；线缆上 `corr_len` 为 2 字节 BE（**D5**），余量充足。

  **"客户端要过滤是不是给自己的"由 `Dispatcher` 的 `corr` 键天然完成**，不需要应用层写过滤代码：共用 topic 上别人的应答携带别人的 `corr`，与本机登记的 `{该服务的应答 topic, 我的 corr, kReply}` **不匹配**，落到"无订阅者"而被丢弃。**uuid 那一半正是这一步安全的根据**——若只有自增计数，两个节点会各自从 0 开始，**必然互相误配**。

  **不提供"订阅一个"与"订阅所有"两种固定变体**（2026-08-28 裁决）——"所有"由调用方传 `kAny` 表达。**`ServeRequests` 这个名字随之取消**：两个键全开放后它与 `Subscribe(topic, kRequest)` **完全等价**，留两个签名相同的方法只会让人以为有语义差别。

  **`correlation_id` 不进公开接口。** 精确地说：**`Subscribe` 交出去的订阅其 `corr` 位恒为 `kAny`**，而 `RequestForResultDirect` **内部**登记的那一条**用具体值**（见上表末行）。

  > **这个区别是承重的，不是措辞**：共用应答 topic 之所以能区分客户端，**全靠内部登记的 corr 是具体值**。若内部也用 `kAny`，客户端会匹配上该 topic 上**所有人**的应答。

  公开面不暴露 `corr` 的理由：

  - **请求-响应侧**：`corr` 由框架在 `RequestForResultDirect` 内生成。**服务端事先不可能知道客户端会生成什么值**，客户端等应答用的具体值又是内部登记的——故公开出去只有 `kAny` 一个可用值，**那是陷阱不是灵活性**。
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
  //    【返 Coro::Result<Ticket>，不是裸 Ticket】——见下
  Coro::Result<Ticket> Subscribe(TopicKey topic, KindKey kind);   // 两键均可 kAny，见 D6

  // —— 发布-订阅 ——
  Coro::Result<void> Publish(const std::string& topic, Message msg);

  // —— 请求-响应（客户端）：单阶段，等结果时重发，不回应（D7）——
  //     【没有 result_timeout】——只有一个等待阶段，其时限即 retry.timeout
  Coro::Result<Message> RequestForResultDirect(const std::string& topic, Message req,
                                               RetryPolicy retry);

  // —— 请求-响应（服务端）：【只有一个方法】——
  Coro::Result<void> Reply(const Message& request, Message result);  // 回结果 kReply
  ```

  **`Subscribe` 必须返 `Coro::Result<Ticket>`，不能返裸 `Ticket`**：**D16** 规定"topic 未注册为对应角色即返 `kConfiguration`"，而 `Ticket` **装不下错误码**。若返裸 `Ticket`，"忘了注册"只能交出一个空 `Ticket`——其 `Wait` 返的是 **`kInvalidState`**（`Dispatcher.hpp:203`），**错误码不对，且推迟到第一次 `Wait` 才暴露**，与"显式错误"的初衷正相反。

  ### `Subscribe` 的生命周期相位：**只许 `Running`**（#214 定，2026-09-01 改判）

  | 相位 | `Subscribe` |
  |---|---|
  | **`Created`（尚未 `Start()`）** | **`kClosed`——不允许** |
  | `Running` | 放行 |
  | `Closing` / `Closed` | `kClosed` |

  相位判定放在注册校验**之前**，与另外三个方法同序。

  **`Created` 与 `Closed` 都返 `kClosed`，不做区分**——这是**本项目既有的、写进公开文档的约定**：`ProtocolNode.hpp` 的 `@return` 两处均写作 **`kClosed（未启动 / 已关闭）`**，`ProtocolNode.cpp:151` 的注释亦为"未启动 / 关闭中 / 已关闭"。四个交互方法（`Subscribe` / `Publish` / `Reply` / `RequestForResultDirect`）在**两个节点类型上一律齐平**。

  > **曾拟让 `Created` 返 `kInvalidState`**（理由是"调用序错误，与注册方法在非 `Created` 相位返 `kInvalidState` 对称"），**已否决**：那会让 `Subscribe` 成为四个交互方法里**唯一**区分"没启动"与"已关闭"的一个，单方面不守已文档化的承诺，读代码的人反而困惑。
  > **"区分二者"本身有诊断价值**——"忘了 `Start()`"与"节点已关"是两种不同的 bug——但那应当是**四个方法、两个节点类型一起**的改动，须另行裁决，不在本条捎带。

  ### 为什么"`Start()` 之后再订阅"是安全的

  **`DataReader` 建于 `DoStart()`（D15），而 DDS 发现要 ~240ms（D9）** —— `Start()` 返回之后的头 ~240ms，对端根本还没 `matched`，**一条样本都到不了**：

  ```
  Start() 返回 ──────── ~240ms 发现窗口 ────────► 样本开始到达
       │                                              │
       └─ 宿主在这段里随便什么时候 Subscribe 都不丢 ──┘
  ```

  宿主须在 `Start()` 与 `Subscribe()` 之间**干超过 240 毫秒的事**才谈得上丢；推荐写法

  ```cpp
  node.Start();
  auto ticket = node.Subscribe(...);   // 中间连让出都没有
  ```

  离那个边界差好几个数量级。**唯一要避免的是**：`Start()` 之后先去做慢活（读配置、等别的组件就绪）再订阅——那段流量会被 `Dispatch` 因无订阅者而静默丢弃。

  ### 本条曾允许 `Created`，2026-09-01 改判，记此以免反复

  初版（#214）定的是「`Created` / `Running` 放行」，理由是「注册 → 订阅 → `Start()`」为**结构性**零丢失次序，而只许 `Running` 则零丢失靠一条**隐式调度约定**（读循环与调用方同线程亲和、调用方让出才跑）撑着。

  **该论证本身成立，但把风险说大了**：它没算上上面那 ~240ms 的发现窗口——真实余量不是"几条指令"，而是数百毫秒。

  **而 `Created` 放行带来了一处真实危害**（**#217**）：`NodeBase::Close()` 从 `Created` 走时**直接落 `kClosed`、不调 `DoClose()`**，`Dispatcher::CloseAll` 因此**从不执行**。宿主若在 `Created` 期订阅并已 spawn 消费 fiber、随后放弃启动（`Start()` 失败或直接 `Close()`），那条 fiber 的信箱**永远等不到关闭信号**——`Ticket` 持 `weak_ptr`、没有悬垂，纯粹是**唤醒不会来**，表现为静默挂起。

  **收益远小于代价，故退回，并把 `Created` 期订阅明确定为【禁用法】。**

  > **⚠ 本次改判【不能】替代 #217。** `ProtocolNode::Subscribe` 返的是**裸 `Ticket`**（`ProtocolNode.hpp:310`）、**没有错误通道**，拦不住 `Created` 期订阅——那条挂起路径在 `ProtocolNode` 上依然敞着，**#217 仍须独立处置**。

  ### 传输的所有权：节点【借用】，启停归宿主（#204 落地时定，追记于此）

  ```cpp
  DdsNode(DdsTransport& transport, ICodec& codec, DdsNodeConfig config);
  ```

  与 `ProtocolNode`（ADR-0009）一致，但本设计另有一条**独立于一致性的硬理由**：

  > `DdsTransport::WaitClosed()` join 的是那条**专属 OS 线程**（**D3**），而它的最坏等待**没有上界**（「明确接受的代价」7）。节点若在 `DoJoin()` 里调它，就是**阻塞整条 fiber 线程**——与 `NodeBase::WaitClosed()` 的**让出式 join** 正相反。**传输的关闭必须留在宿主那条控制流上。**

  **连带后果**：节点 `Start()` 之前宿主**必须先启传输**，否则 `Declare*` 返 `kInvalidState`。这一条须写进使用文档。

  **服务端没有 `Accept()`**：本模型无受理阶段（**D7**）。**`MessageKind::kFeedback` 在本设计中不使用**——它是 `Message` 为别的路径预留的值。

  **不提供旁路监听**（2026-08-28 裁决）。机制上 `Subscribe(kAny, kAny)` 可达，但**不作为受支持的用法写入接口文档**。

  **多 topic 用多次 `Subscribe`，每 topic 一条消费 fiber**——各自独立信箱，一路慢不拖累另一路（与 ADR-0009 D2 同向：节点只交出凭据，消费策略归宿主）。

- **D9（`CurrentLinkState()` 用 `matched` + `Liveliness`）：** `kDown` = `matched == 0` 或 `alive == 0`；`kUp` = `matched > 0` 且 `alive > 0`；`kEstablishing` = endpoint 已建但 `matched` 尚未 `> 0`（约 240ms 窗口，**无 DDS 原生事件**，由我方状态推出）。

  **必须配 `AUTOMATIC_LIVELINESS`**：实测仅靠 `matched` 时对端被**硬杀**要等 **~19.8 秒**（participant lease 默认 20s）才检出，**期间谎报 `kUp`**；配 `lease = 2s` 后 **2.0s 检出**。

- **D10（`DdsQos` 增两项；`DdsConfig` 不引入我方定义的时间量）：** `DdsQos` 增 `max_blocking_time` 与 `liveliness_lease`。

  **它们是转达给 DDS 的 QoS 参数，语义由 DDS 定义**，与三介质那种"我方定义的判活/退避时间量"性质不同。DDS 无 connect、无退避、无静默判活，故**不需要** `silence_timeout` 那类旋钮。

- **D11（丢弃策略与三介质一致；修订 `RT_NODE_007`）：** `read_queue_` 沿用默认「**有界 1024 + 静默丢最旧**」，与 UDP/TCP/串口逐字相同（SDD **DD-15**）。

  **`RT_NODE_007` 须修订**：它现要求"满时丢弃**最新**样本（tail-drop）并记 `ResourceExhausted`、丢弃**计数**和 Trace"。**三处与现状不符**：① `FiberChannel` 实际丢**最旧**；② 计数器自 ADR-0008 **D10** 起已不存在；③ 丢弃**静默**。改为与 DD-15 一致。

  **`DropReason::kDdsHandoffOverflow` 删除**（五项 → 四项；后又随 **#212** 减为**两项**——`kBusinessQueueOverflow` 与 `kCloseDrop` 的产生点亦已在设计上取消）：DDS 的 `read_queue_` 与三介质的同类，三介质都不为它单设归因项。

- **D12（配置校验，`Start()` 时一次性）：** 非法返 `kConfiguration`、**停在 `Created`**。

  | 检查项 | 约束 |
  |---|---|
  | `DdsConfig::domain_id` | `[0, 232]` |
  | `DdsConfig::provider` | 非空且已注册 |
  | `DdsQos::max_blocking_time` / `liveliness_lease` | **须为正** |
  | **四组注册全空**（非配置字段，见 **D16**） | **返 `kConfiguration`**——一个什么都不收不发的节点必是漏了注册 |

  **配置里已无 topic**（**D16**）：topic 的合法性（非空、键值不同、方向不冲突）在**注册那一刻**就判完了，`Start()` 只补判"四组全空"这一条——它要等注册全部结束才知道。

  **另一组校验在调用时**：`Publish` / `Subscribe` / `RequestForResultDirect` 的 topic 须已注册为对应角色，否则返 `kConfiguration`（对应关系见 **D16**）。

  **没有"topic 须唯一"这类部署约束**：应答 topic **本就由一个服务的全体客户端共用**，唯一性的担子**整个落在 `correlation_id` 的 uuid 半段**上——那是节点**自己**用 `QUuid::createUuid()` 生成的，无需部署方协调，也不会因写错而静默误配。

- **D13（`IDdsProvider` 接口增删）：**

  | 变化 | 用途 |
  |---|---|
  | **新增** `MatchedCount() → {matched, alive}` | **D9** 的判活 |
  | **新增** `DeclareWriter(topic)` | **写侧的端点声明钩子**（**D15**）——见下「补正」 |
  | **改语义** `Publish` | 由"必成功"改为**可阻塞**（在专属线程上调用，**D3**） |
  | `Subscribe` / `Unsubscribe` / `Init` / `Shutdown` / `Name` | **不变**（QoS 统一，无须增参） |

  ### 补正（2026-09-01，#203 落地时发现）

  **本决策初稿只增了 `MatchedCount`，写侧没有声明钩子——这是一处遗漏，不是有意的取舍。**

  读侧之所以成立，是因为 `IDdsProvider::Subscribe(topic, cb)` **碰巧已经存在**，`DeclareReader` 落到它身上即可**真正建出 `DataReader`**。写侧则无对应物：`DdsTransport::DeclareWriter` 只能把 topic 记进一个集合，**真正的 `DataWriter` 仍由 provider 在首次 `Publish` 时惰性建**。

  **后果是把 D15/D16 要消灭的那件事原样放了回来**：**D16** §「为什么必须提前声明」那张表的第三行写的正是"应答 topic 的 `DataWriter` 若第一次 `Reply()` 才建 → 与客户端的 reader 尚未 match → **该服务的第一次应答会丢**"。惰性建只是把它从"`Reply()` 懒声明"挪成"provider 首次 `Publish` 惰性建"，**后果一模一样**。

  **故本决策补一个与 `Subscribe` 对称的写侧钩子：**

  ```cpp
  // IDdsProvider 新增（幂等：同 topic 重复调用直接成功）
  virtual Coro::Result<void> DeclareWriter(const std::string& topic) = 0;
  ```

  `FastDdsProvider` 把现有 `Publish` 里那段 `GetOrCreateTopic` + `create_datawriter` **抽出来**给它，`Publish` 退化为"查已建好的 writer 并 `write`"；`FakeDdsProvider` 同步实现。

  **`Publish` 遇到【未声明】的 topic 返 `kConfiguration`，【不惰性建】**（#209 落地时定，追记于此）：

  - **惰性建会让本钩子形同虚设**——首帧照样在 writer 刚建出、尚未 `matched` 时发出去而丢掉，等于什么都没修。
  - **写侧端点集合"启动即定型"**（**D16**），故运行期冒出一个未声明的 topic **必然**是启动时漏了注册，不是 I/O 故障。返 `kIo` 会把"配错了"伪装成"网络抖了一下"。
  - 已 `Shutdown` 时**优先**判 `kInvalidState`（调用序错误先于配置错误）。
  - 这条错误在 `DdsTransport` 那一层按写侧契约**不回传，只落 `LastError()`**（**D3**）。

  **不设 `UndeclareWriter`**：端点集合**启动即定型、运行期恒定**（**D16**），只在 `Shutdown()` 时整体拆除，没有单独撤销一个 writer 的时机。既有的 `Unsubscribe` 是历史遗留，不为对称而新增一个无使用者的方法。

  **未采用的偏方**：用 `Publish(topic, {})` 发一条空样本来逼出 writer。**否决**——那会往线上真发一条帧，对端收到一条无法解码的空消息。

  **不新增数据观察者接口**——既有的 `IDdsProvider::Subscribe` 已经是所需的那个钩子：

  ```cpp
  // IDdsProvider::Subscribe（现状，不改）
  virtual Coro::Result<void> Subscribe(
      const std::string& topic,
      std::function<void(const std::vector<uint8_t>&)> cb) = 0;
  ```

  `DeclareReader(topic)`（**D15**）落到 provider 就是调它，**闭包捕获 `topic`** 即可填 `Datagram.peer`（**D6**：topic 不上线缆，入站由 `peer` 带出）。

  > **曾拟新增 `SetDataObserver(std::function<void(Message)>)`，已否决**，两个理由：
  > ① **跨层**——`Message` 是 codec **之后**的产物，而 provider 在 codec **之下**；`read_queue_` 装的是 `Datagram`（字节），provider 不该认识 `Message`。
  > ② **重复**——它与既有的 `Subscribe(topic, cb)` 是同一个钩子的两种写法，留两个只会让人猜哪个才是真入口。
  >
  > 若将来确需**单一全局**观察者取代按 topic 回调，其签名**必须带 topic**（`void(const std::string&, const std::vector<uint8_t>&)`），且 `Subscribe` 的 `cb` 参数须同时去掉——**两者只能留一个**。

- **D14（`FastDdsProvider` 与 `FastDdsRawType` 按 Fast DDS 3.x 重写）：** 本机为 **3.6.1**，与代码假定的 2.13.x 是**大版本断裂**：包名 `fastrtps` → **`fastdds`**、命名空间 `eprosima::fastrtps::rtps` → **`eprosima::fastdds::rtps`**、`TopicDataType` 的 `serialize`/`deserialize` 由指针改引用并增 `DataRepresentationId_t`、`getSerializedSizeProvider` → **`calculate_serialized_size`**、`getKey` → **`compute_key`**、`fastrtps/types/TypesBase.h` **已不存在**。

  **`CMakeLists.txt:75` 的 `if(FALSE)` 硬禁用同时解除**，并改为 `find_package(fastdds)`。

  > **⚠ 一处会静默反转的缺陷，实现票必须显式处置**：`FastDdsProvider.cpp:105` 的
  > ```cpp
  > if (!writer->write(&copy)) return make_error_code(TransportErrc::kIo);
  > ```
  > 在 3.x 下**语义完全反转**——已复核 `DDSReturnCode.hpp`：`typedef int32_t ReturnCode_t; const ReturnCode_t RETCODE_OK = 0;`，而 `write()` 返回的正是 `ReturnCode_t`。**成功返 0 → `!0` 为真 → 返 `kIo`；失败返非 0 → 返成功。且零警告照常编译。**

- **D15（topic 端点的声明：全部在 `DoStart()`，【没有懒声明】）：** DDS 的每个 topic 需要建 `DataReader`（决定**什么会到达**）与 `DataWriter`（决定**能发往哪里**）。本设计的处置：

  ```cpp
  // DdsTransport 公开面（均幂等：同 topic 同方向重复调用直接成功）
  Coro::Result<void> DeclareWriter(const std::string& topic);
  Coro::Result<void> DeclareReader(const std::string& topic);
  ```

  **两个方法而非一个**：一个 topic 上本节点通常**只需要一侧**——客户端在请求 topic 上只发不收、在应答 topic 上只收不发，服务端反之。**建成对是浪费，还会招来自收**（代价 9）。

  **两者都须落到 provider 上真正建出端点**，不能只在传输层登记意图：`DeclareReader` → `IDdsProvider::Subscribe(topic, cb)`；`DeclareWriter` → `IDdsProvider::DeclareWriter(topic)`（**D13** 补正）。

  | 谁声明 | 何时 | 声明什么 |
  |---|---|---|
  | `DdsNode::DoStart()` | **启动时一次性，且仅此一处** | 按 **D16** 的四组注册项逐项建**对应方向**的端点 |

  **`Reply()` 不做懒声明**（2026-08-31 裁决）。**运行期不再有任何建端点的路径**——`DeclareWriter` / `DeclareReader` 只由 `DoStart()` 调用。

  **由此服务端的应答目的地改由自己注册的内容决定，不再取信于线缆**：

  ```cpp
  // Reply()：应答 topic 从【自己注册的 Services 表】查，不是从请求里读
  auto it = services_.find(request.topic);
  if (it == services_.end()) return kConfiguration;              // 我根本不服务这个 topic
  const std::string& reply_topic = it->second;                   // 【已在 DoStart() 建好 writer】
  ```

  **`reply_to` 仍留在线缆上，但降为【一致性交叉校验】**：若 `request.reply_to` 非空且与查出的 `reply_topic` **不等**，返 `kInvalidArgument`。

  > **保留它是有价值的，不是冗余**：两侧注册实参写歪时（客户端在 `cfg.get.reply` 上等、服务端注册成 `cfg.reply` 往外发），**若不带 `reply_to`，这种偏差完全不可见**——客户端只会一路超时到 `kTimeout`，看起来像对端没响应。带上它，服务端**当场就能报出**"你等的地方和我发的地方不一样"。这 20 来个字节买的是一类部署错误的可诊断性。

  ### 三处连带收益

  1. **`DataWriter` 不再累积。** 先前"每个出现过的 `reply_to` 永久留一个 writer、不做回收"那条代价**整条消失**——端点集合现在**完全由启动前的注册决定、启动即定型、运行期恒定**。
  2. **运行期无 DDS 端点创建**，故也没有"回应路径上突然吃一个 ~240ms 发现窗口"的问题（**D16**）。
     > **此条需 `IDdsProvider::DeclareWriter` 才真正成立**（**D13** 补正，2026-09-01）：初稿的 provider 接口没有写侧声明钩子，`DeclareWriter` 只能登记意图、`DataWriter` 仍在首次 `Publish` 时惰性建——**读侧成立、写侧不成立**。补上该钩子后两侧对称。
  3. **服务端不再受客户端摆布**：`reply_to` 曾是**客户端说了算**的目的地，服务端照着发。现在它只是个待校验的声明。

  **`DeclareWriter` / `DeclareReader` 仍要求幂等**，但理由变了：不再是"同一 `reply_to` 反复声明"，而是**注册里可能重复**（例如同一 topic 既注册为 `Subscribers`、又是某条 `Clients` 的值）。幂等让 `DoStart()` 不必先去重。

  **QoS 统一之后（D4），两个声明方法都不带 QoS 参数。**

  **明确接受的代价——两侧注册实参歪了只能在运行期发现**：客户端 `RegisterClients` 与服务端 `RegisterServices` 的实参若不一致，`Start()` **无从校验**（跨进程）。表现为：服务端 `Reply()` 返 `kInvalidArgument`（若客户端带了 `reply_to`）或 `kConfiguration`（若服务端根本没注册那个请求 topic），客户端则重发至耗尽后返 `kTimeout`。**两侧各自都有明确错误码，不是静默失败**——这已是不引入配置中心的前提下能做到的最好程度。

- **D16（topic 由【注册接口】给出，不进配置；仍一律在 `DoStart()` 建端点）：** 2026-08-31 裁决——把原先的四个 topic 配置字段换成四个注册方法。**`DdsNode` 的用法不变**：`Publish` / `Subscribe` / `RequestForResultDirect` / `Reply` 的签名与语义**一个字不动**（**D8**），换掉的只是"这些 topic 从哪来"。

  ```cpp
  // DdsNode 注册接口 —— 【须在 Start() 之前调用】
  //   Running / Closing / Closed 一律返 kInvalidState
  //   【批量】：一次给一组，不必一个 topic 调一次
  Coro::Result<void> RegisterPublishers (std::vector<std::string> topics);
  Coro::Result<void> RegisterSubscribers(std::vector<std::string> topics);
  Coro::Result<void> RegisterClients (std::map<std::string, std::string> topics);  // 请求 → 应答
  Coro::Result<void> RegisterServices(std::map<std::string, std::string> topics);  // 请求 → 应答
  ```

  **与原配置项一一对应，方向与端点全不变：**

  | 注册方法 | 角色 | 建的端点 |
  |---|---|---|
  | `RegisterPublishers` | 发布者 | 每个 topic 的 **Writer** |
  | `RegisterSubscribers` | 订阅者 | 每个 topic 的 **Reader** |
  | `RegisterClients` | 请求-响应**客户端** | 键 → **Writer**（发请求）　值 → **Reader**（收应答） |
  | `RegisterServices` | 请求-响应**服务端** | 键 → **Reader**（收请求）　值 → **Writer**（发应答） |

  **方法名用复数**——名字直接说明是批量，免得读者以为要一个 topic 调一次。
  **两个 pair 型用 `std::map` 而非 `vector<pair>`**：天然去重，且排除"同一请求 topic 配了两个不同应答 topic"这种自相矛盾的输入。

  > **⚠ 更正（#204 落地时发现）**：本 ADR 初稿写的是"**从类型上**排除"——**那句话过头了**。`std::map` 只在**单批之内**成立；**跨批次**调用时（`RegisterClients` 可多次调用、累加），`map::insert` 会**静默保留第一个值**，类型层的保证被绕过去。
  > **故须补一条运行期校验**：同一请求 topic 在跨批次被配了**两个不同的**应答 topic → **`kInvalidArgument`**（见下「校验落在注册这一步」）。

  ```cpp
  struct DdsNodeConfig {   // 【只剩两项】
    /// 节点 uuid：非空则用它，为空才 QUuid::createUuid()（D6）。测试注入用。
    std::string uuid_override;
    ITraceSink* trace_sink = nullptr;
  };
  ```

  全部 topic 字段移出配置；历史遗留的 `inbox_topic`、`node_id`、`handler`、`business_queue_max_*` 一并删除。`DdsConfig`（传输层）保持 `domain_id` / `provider` / `qos`，**不含任何 topic**。

  ### 只允许 `Start()` 之前注册

  **端点集合仍然"启动即定型、运行期恒定"**——这一步只把"填结构体"换成"调四个函数"，**没有引入运行期动态端点**。由此：

  - 回应路径、发布路径、订阅路径上**都不会突然冒出一个 ~240ms 的发现窗口**（**D9**）
  - `DoStart()` 仍是**唯一**建端点的地方（**D15**），运行期没有第二个调用点
  - 诊断"我建了哪些端点"仍然只看**启动前那几次注册调用**，不必查运行期状态

  **注册发生在 `Created`**，故它**不是**"启动后动态增删 topic"的能力；要那个能力得另行裁决。

  **`Start()` 失败不清空已注册的内容**：校验失败停在 `Created`（**D12**），此时**注册表原样保留**，调用方补上漏的那几项再 `Start()` 一次即可。否则"四组全空返 `kConfiguration`"之后还得把全部注册重做一遍，无谓。

  ### 批量、可多次调用、整批生效

  - **批量**：一次给一组，`{"a","b","c"}` 一次调完。
  - **可多次调用**：同一方法调多次**累加**（便于按模块分别注册）。
  - **重复项幂等**：同一 topic 同一角色重复出现（无论同批还是跨批）**去重**，不报错。
  - **整批生效或整批不生效**：一批里只要有一项非法，**整批回滚、一项都不落**，返对应错误。半生效的注册会让调用方难以判断该重试哪些。

  ### 校验落在注册这一步

  | 检查 | 返回 |
  |---|---|
  | 不在 `Created` 阶段 | `kInvalidState` |
  | topic 为空串 | `kInvalidArgument` |
  | `Clients` / `Services` 某条的键与值相同 | `kInvalidArgument`（请求与应答同 topic 必然自收自答） |
  | 同一 topic 既是 `Clients` 的键、又是 `Services` 的键 | `kInvalidArgument`——**自己请求自己**，且 `corr` 由自己生成、`Dispatcher` **会真的匹配上**，形成调用方毫无察觉的自问自答 |
  | **同一请求 topic 跨批次被配了两个不同的应答 topic** | `kInvalidArgument`——`std::map` 的去重只在单批内成立，跨批次 `insert` 会静默保留第一个值（见上「更正」） |

  **只拦这一种组合。** 其余"同一 topic 上既有 writer 又有 reader"的组合（判据见「代价 9」）**只造成自收白干、不会误配**，且可能是调用方有意为之（本地回环自测），故**不拦、只在文档记明**。

  **四组全空**：`Start()` 返 `kConfiguration`（**D12**）——一个什么都不收不发的节点必是漏了注册。**这一条仍在 `Start()` 判**，因为"全空"要等注册全部结束才知道。

  ### 角色由"注册了什么"表达，不设 `role` 枚举

  | 调了哪个 | 该节点就是 |
  |---|---|
  | `RegisterPublishers` | 发布者 |
  | `RegisterSubscribers` | 订阅者 |
  | `RegisterClients` | 请求-响应的**客户端** |
  | `RegisterServices` | 请求-响应的**服务端** |

  **四者可任意并存**——一个节点常常兼任（既是服务 A 的服务端，又是服务 B 的客户端，同时还发布心跳）。若另设一个 `role` 枚举，就会出现"`role` 说是服务端、却注册了 `Clients`"这类**自相矛盾的输入**，还得再定优先级规则。让注册本身表达角色，矛盾无从产生。

  ### 请求-响应两侧是同一张表、相反的方向

  ```
    客户端                                              服务端
    RegisterClients({{"cfg.get", "cfg.get.reply"}})     RegisterServices({{"cfg.get", "cfg.get.reply"}})
         │              │                                    │              │
      DataWriter    DataReader                           DataReader     DataWriter
      （发请求）    （收应答）                            （收请求）     （发应答）
  ```

  **两侧传【一模一样的实参】**，各自按角色建各自那一侧——不会填错方向，也不必协调。

  ### 调用与注册的对应校验

  `Publish` / `Subscribe` / `RequestForResultDirect` 的 topic 若未注册为对应角色，返 **`kConfiguration`**：

  | 调用 | 须已注册为 |
  |---|---|
  | `Publish(topic, …)` | `Publishers` |
  | `Subscribe(topic, kNotify)` | `Subscribers` |
  | `Subscribe(topic, kRequest)` | `Services` 的键 |
  | `RequestForResultDirect(topic, …)` | `Clients` 的键 |

  **上表只列了 `kNotify` / `kRequest` 两种 `kind`。其余 `kind` 配【具体 topic】时**（#204 落地时补）：要求该 topic **至少在读侧集合内**——即 `Subscribers` ∪ `Services` 的键 ∪ `Clients` 的值，否则返 `kConfiguration`。**不在读侧的 topic，其消息根本到不了本进程**，订阅它必然是本决策要消灭的那种"静默无效"。该规则是上表两行的**超集**，不与之冲突。

  **`Subscribe` 的 topic 键传 `kAny` 时【跳过该校验】**：`kAny` 不对应任何一个具体 topic，拿它去查注册表必然落空。这不是网开一面——`kAny` 本来**就只在已注册范围内起作用**（见下节），它的作用域已由注册天然限定。

  **不猜、不回落、不懒补**。这让"忘了注册"从一个**静默无效**（端点不存在，消息永远不来，看起来像对端没发）变成一个**显式错误**。

  ### 一处必须写进接口文档的限制

  **`Subscribe(kAny, kind)` 建不了任何 `DataReader`。** DDS 的 reader 是**按 topic** 建的，而 `kAny` 只是**分发键**上的通配符。故"订阅所有 topic"的实际语义是「**已注册为 reader 的 topic 的全部**」——即 `Subscribers` ∪ `Services` 的键 ∪ `Clients` 的值，**不是**"本 domain 上的全部"。未注册的 topic，其消息**根本不会到达本进程**。

  **这是确定会被理解反的一处**，接口文档须明写。

## 明确接受的代价

1. **`RELIABLE` QoS 被本地队列架空。** listener 一搬走样本，DDS 即认为已交付、背压解除；我方 `read_queue_` 满时静默丢最旧（#190 实测丢 3976/5000 而 `push_fail=0`）。**这是 2026-08-28 裁决"不需要优先使用 DDS 自己的机制"的直接后果，明确接受。**

2. **丢弃不可观测。** 与三介质同性质（SRS §3.6 已登记的静默丢弃）。另：#191 实测 `KEEP_LAST` 下 **DDS 自己也不报**（丢 4995/5000 而 `on_sample_lost = 0`、`on_sample_rejected = 0`），故即便去查 DDS 侧也查不到。

3. **写侧多一条 OS 线程。** 这是四个介质里唯一需要额外线程的。代价是一次跨线程往返与线程本身的开销；换来的是 `Publish` 的线程级阻塞**不卡任何 fiber**。

4. **写出结果不回传。** `RETCODE_TIMEOUT` 只落 `LastError()`——与三介质一致，但对 DDS 而言意味着**背压信号被丢弃**：调用方无从知道"这条因对端消费不过来而没发出去"。

5. **`kEstablishing` 那约 240ms 窗口无 DDS 原生事件**，由我方状态推出。

6. **全部实测为单机 loopback、单次实跑**；跨主机时延/丢失率未测。

7. **`Shutdown()` 打不断在途阻塞的 `Publish`，只能等；且等待【无上界】。**（2026-09-01 实测，#202）

   **已补测**：5 轮（订阅方停滞 600ms，阻塞开始后 100ms 调 `Shutdown()`）——`Publish` **一次也没被截断**（600.1–600.5ms 全跑满），`Shutdown()` 耗时 501–508ms 恰好等掉剩下那段，且每轮都是 `Publish` 先返回。已核 3.6.1：`DataWriter` 上**没有任何中止 `write()` 的入口**。

   **故 `Close()` 落在一次阻塞的写上时，`WaitClosed()` 就得等那次写自己结束，没有捷径。**

   > **等待时长【没有上界】，不是"一个 `max_blocking_time`"**（本 ADR 初稿的说法，已证伪）：阻塞来自同进程对端的交付回调（见背景），`max_blocking_time` 不参与——实测它设 300ms 而 `Publish` 跑满 2000ms。**界由同进程内最慢的那个订阅回调决定。**

8. **共用应答 topic 带来读入放大。** 一个服务的应答 topic 由**全体客户端共用**（**D6**），故**每个客户端都会收到该服务的全部应答**，自己那份只是其中之一——`N` 个并发客户端 ⇒ 每客户端约 `N` 倍读入量。这些多余样本会**一路进到 `read_queue_` 并被解码**，然后才在 `Dispatcher` 处因 `corr` 不匹配而落空。

   **后果不只是白干**：`read_queue_` 有界 1024、满时静默丢最旧（**D11**），故**别人的应答有可能把自己的挤掉**。这恰好**加强**了 **D7** 重发的必要性——重发本就是对"丢在我方队列这一段"的补救，而共用 topic 把这一段的压力放大了 `N` 倍。

   **未采用的缓解手段**：DDS 的 `ContentFilteredTopic` 能把按 `corr` 前缀的过滤下推到 DDS 侧、使多余样本根本不进我方队列。**本轮不采用**，因 2026-08-28 裁决"不需要考虑优先使用 DDS 自己的机制"；若实测中放大成为瓶颈，这是**第一顺位**的优化入口。

   **只要不同服务用不同的应答 topic，放大就只限于同一服务内**（**D6**），调多个服务不叠成 `N×M`。
   **但这一点靠调用方保证，框架不强制**：`RegisterClients` 的 map 只排除了"同一请求 topic 配两个应答 topic"，**不排除两个请求 topic 共用一个应答 topic**（`{"a":"r","b":"r"}`）——那样写就**会**叠。

   **上界未实测**：`N` 多大时开始丢自己的应答，**实现票须补测**。

9. **自收只在【同一 topic 既发布又订阅】时发生，且那是配置方显式写出来的。** Fast DDS 默认**不屏蔽同一 participant 内的收发匹配**——已核 3.6.1 头文件：`DomainParticipant` 只提供 `ignore_participant(GUID)`（屏蔽**别的** participant，`DomainParticipant.hpp:703`），**没有 `ignore_local_endpoints` 这类自环开关**。

   **但按角色注册之后（D16），典型用法下不会触发**：每个角色在一个 topic 上**只建它实际需要的那一侧端点**。客户端在 `cfg.get` 上只有 writer、在 `cfg.get.reply` 上只有 reader；服务端反之。

   **精确判据是两个方向集合相交**（**不是**"同时注册为 `Publishers` 与 `Subscribers`"那一种）：

   ```
   writer 侧 = Publishers ∪ Clients 的键 ∪ Services 的值
   reader 侧 = Subscribers ∪ Clients 的值 ∪ Services 的键
   自收 ⟺ writer 侧 ∩ reader 侧 ≠ ∅
   ```

   交集里的每个 topic 都会自收。举两例：`Publishers` ∩ `Subscribers`（本地回环自测）；`Publishers` ∩ `Clients` 的值（往某应答 topic 发布、同时又是该服务的客户端）。

   **其中最危险的一种已被 D16 的方向冲突校验拦下**：`Clients` 的键 ∩ `Services` 的键——自己请求自己，且 `corr` 是自己生成的，`Dispatcher` **会真的匹配上**，形成自问自答而调用方毫无察觉。**其余交集只是白干**（`kind` / `corr` 对不上，落到无订阅者）。

   **落到交集里但未被校验拦下的组合，是调用方显式写下的**，**不是框架强加的**。

   **实现票的处置方向（可行性已核）**：若确需屏蔽，在 listener 里比对 `SampleInfo::sample_identity`（`SampleInfo.hpp:89`）的 writer GUID 前缀与本 participant 的 `guid()`（`DomainParticipant.hpp:1371`），前缀相同即丢弃、不入队。两个符号在 3.6.1 均存在。**本设计不默认屏蔽**——既然只有显式注册两个相反角色才会触发，替调用方决定"你不想收到自己"是越权。

## 影响（Consequences）

- **正面：** ① 四个介质形态统一，`ITransport` 仍是**全介质**的内部缝；② `AsyncRead`/`AsyncWrite` 契约不分叉，"换传输即可运行"的调用方**含 DDS**；③ 请求-响应复用 ADR-0010 **D13** 已验证的**单阶段** `Direct` 模型（其四条纪律**只余两条适用**，见 **D7**）；④ 恢复 DDS 进入编译面。
- **负面（明确接受）：** 见上九条。
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

- **写侧用 fiber 写泵（照三介质）。** **否决理由：** `Publish` 的阻塞是**线程级**，fiber 写泵会卡死整条线程上的所有 fiber。实测同进程订阅方回调睡 2000ms 时 `Publish` 跑满 2000ms，且**回调就在发布线程上**（见背景）。
- **用 `ASYNCHRONOUS_PUBLISH_MODE` 免掉专属线程。** **否决理由：实测证伪**（#191 G 组 178/200 超时）——该模式挪走的是网络发送，`write()` 仍须先把样本放进 history，卡住的是准入。
- **读侧加一条转发泵 fiber**（listener → handoff → 泵 → `read_queue_`）。**否决理由：** 跨线程 `push` 已实测安全（#190 Q1），listener 可直推；多一跳只增延迟（#191 实测一跳 27µs vs 两跳 33µs）与一条 fiber。
- **请求-响应用 DDS 原生 `SampleIdentity` 关联。** **否决理由：** 会把 Fast DDS 的具体能力捅进 `IDdsProvider` 抽象层，`FakeDdsProvider` 还得模拟。2026-08-28 裁决为自定义 `correlation_id`。
- **提供"订阅一个"与"订阅所有"两种固定变体**（`Subscribe(topic)` + `SubscribeAll()`），以及独立的 `ServeRequests`。**否决理由：** 2026-08-28 裁决——键全开放后"所有"由 `kAny` 表达，固定变体是同一件事的两种写法；而 `ServeRequests` 与 `Subscribe(topic, kRequest)` **签名完全相同**，留两个名字只会让人以为有语义差别。
- **把 `correlation_id` 也暴露在公开订阅接口上。** **否决理由：** 见 **D6**——请求-响应侧它只有 `kAny` 一个可用值（服务端事先不知道客户端会生成什么），发布-订阅侧的"应用自定义子通道"能力**裁决为不需要**。暴露一个只能填一个值的参数是陷阱。
- **`CurrentLinkState()` 仅用 `matched`、不配 Liveliness。** **否决理由：** 对端被硬杀时要等 **~19.8 秒**才检出，期间**谎报 `kUp`**。
