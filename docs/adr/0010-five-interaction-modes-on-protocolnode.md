# ADR-0010：外部协议交互模式落到 `ProtocolNode`，每模式一个接口、节点不持交互状态

**状态：** Accepted
**日期：** 2026-08-26
**关联：** ADR-0008（键匹配分发——本 ADR 完全建立在其 `Dispatcher` / `Subscribe(Key)` 之上，不新增机制）；ADR-0003 **D10**（协议无关机制可复用、协议特有语义内联）；SRS 落点：**RT_NODE_002**（本 ADR 将其由 TBD 转为已定义）、RT_NODE_003、RT_REQUEST。

## 背景（Context）

SRS **RT_NODE_002** 长期写着：

> 外部系统协议现有五种交互行为 `noresponse`、`needresponse`、`withfeedback`、`needfeedback`、`repeating` 的精确状态机、时序、重试和终结帧语义为 **TBD，本版不在此细化**。

现在把前四种的语义定下来（`repeating` 本轮仍不讨论）。

线缆层的帧类型早已就位（`Message.hpp`，注意其值当前为**占位值**，真实对接时改为外部协议规定的字节）：

| FrameType | 语义 |
|---|---|
| `kCommand` | 命令（请求） |
| `kResponse` | 即时回应（**中间或终结**） |
| `kResult` | 最终结果（终结） |

关键观察：**`kResponse` 是不是终结帧，取决于当前是哪种交互模式**——`needresponse` 里它是终结，`withfeedback` 里它只是中间受理。终结性是**交互的属性**而非帧的属性。

## 决策（Decision）

- **D1（每种模式一个接口；节点不持有任何交互状态）：** 四种模式各给一个公开方法，模式**不作为参数、不存入节点**。
  每个方法跑在**调用方自己的 fiber** 上，其状态机的全部状态——当前阶段、已发送次数、原始命令帧——都是该函数的**局部变量**，随调用方栈存在。
  由此：节点里**不需要"在途交互表"**，`Dispatcher` 也**不需要认识模式**（它只认键）。这是本 ADR 最重要的结论——它把一个看似需要新机制的需求，化归为纯粹的调用方控制流。

  **命名（2026-08-26 修正）**：不照搬协议原名。`withfeedback` / `needfeedback` 两词本身不表意，并列时无法分辨差别。改按**"等到哪一帧为止"**命名，并保留与协议原名的映射：

  | 来源协议 | 协议行为 | 接口 | 帧序列 | 我方最后动作 |
  |---|---|---|---|---|
  | 外部系统协议 | `noresponse` | `Send`（现有） | → 命令 | 无 |
  | 外部系统协议 | `needresponse` | **`RequestForResponse`** | → 命令 · ⏱← 回应 | 无 |
  | 外部系统协议 | `withfeedback` **与** `needfeedback` | **`RequestForResult`** | → 命令 · ⏱← 回应 · ⏱← 结果 | **回一帧回应** |
  | 外部系统协议 | `repeating` | —（**D11**：本轮不做） | | |
  | **另一种协议** | —（该协议自有的交互） | **`RequestForResultDirect`** | → 命令 · ⏱← 结果（**无中间回应**） | 无 |

  **修正（2026-08-26）**：`withfeedback` 与 `needfeedback` 经核实是**同一个通信模型**——都是「请求 → 等回应 → 等结果 → **回应结果**」。故二者合并为一个接口 `RequestForResult`，**回应结果是该模型固有的最后一步，不是可选项**；原先按"是否回确认"拆出的 `RequestForResultAndConfirm` 取消。

- **D2（四种交互的状态机）：**

  ```
  ① Send（noresponse）
     → kCommand                                        ⇒ 结束（不等待）

  ② RequestForResponse（needresponse）
     → kCommand
     ⏱ 等 kResponse ──超时──▶ 重发 ──次数耗尽──▶ kNotAccepted
     ← kResponse                                       ⇒ 成功

  ③ RequestForResult（withfeedback ＝ needfeedback）
     → kCommand
     ⏱ 等 kResponse ──超时──▶ 重发 ──次数耗尽──▶ kNotAccepted
     ← kResponse（受理）
     ⏱ 等 kResult   ──超时──▶ kTimeout（**不重发**）
     ← kResult
     → kResponse（回应结果 ＝ 该 kResult 帧原样改帧类型）  ⇒ 成功

  ④ RequestForResultDirect（另一种协议）
     → kCommand
     ⏱ 等 kResult   ──超时──▶ **重发** ──次数耗尽──▶ kTimeout
     ← kResult                                         ⇒ 成功（**不回应**）
  ```

  **①②③ 属外部系统协议**：其中②③ 的**重发只发生在等待受理阶段**；③ 的第二阶段（等 `kResult`）超时**直接终结、不重发**——依据：`kResult` 未达意味着**对端正在执行**，重发命令有使其**重复执行**的风险。

  **④ 属另一种协议，不受上述规则约束**：它**没有受理阶段**，唯一的等待就是等 `kResult`，故**必须在该阶段重发**——否则命令帧一旦丢包，交互即彻底失败、无任何补救。两条规则并存不构成矛盾：**"等结果时不重发"是外部系统协议的约束，不是框架的普遍规则**。
  ④ 重发耗尽返 `kTimeout` 而非 `kNotAccepted`——后者的语义是"对端**没有受理**"，而 ④ 根本不存在受理这一步（**D12**）。

- **D3（重发沿用同一 `session_id`，以第一个响应为准）：** 重发的是**字节完全相同**的原帧，`session_id` 不变。因此原订阅横跨全部重发继续有效，**先到的那一帧即完成本阶段**，无需区分它是哪一次尝试的回应。

- **D4（两个订阅必须在发命令之前一起登记）：** ③④ 一进函数即同时登记 `{sid, mid, kResponse}` 与 `{sid, result_mid, kResult}`。
  依据：`kResult` **可能先于** `kResponse` 到达（对端足够快）。若等收到 ack 再登记 result 订阅，该帧会因无匹配而被丢弃。这是现有 `Request` 中"先登记订阅、再发出请求"同一条理由的延伸——**任何"发出去才登记"的写法都有丢失窗口**。

- **D5（阶段一完成后立即注销 ack 订阅）：** 收到首个 `kResponse` 后即 `Reset()` 该凭据。
  依据：重发会引出**重复 `kResponse`**；不注销则它们继续落入信箱、被后续逻辑误读，注销后它们成为无匹配终结帧，按 `kUnmatchedOrLateResponse` 归因丢弃——这才是 D3"以第一个为准"的正确落地。

- **D6（重试次数与超时每次调用传参，不进配置）：** 以 `RetryPolicy{timeout, max_attempts}` 承载第一阶段参数，第二阶段以独立的 `result_timeout` 承载。
  依据：两阶段的等待是**数量级不同**的量——第一阶段是"对端受理"，第二阶段是"对端执行完"；且同一节点上不同命令的耐受度不同，不宜由节点级配置一刀切。

- **D7（`kResult` 的命令码由调用方给出）：** 结果帧的 `message_id` 与请求帧**不同**，其对应关系是协议知识，框架不猜、不做映射规则，由调用方作为参数传入。
  **范围（2026-08-26 修正）**：本条只约束**入站** `kResult` 的订阅键。④ 的**出站确认帧**不适用——它的全部字段由收到的 `kResult` 派生，无调用方参数（见 **D8**）。

- **D8（`RequestForResult` 末尾的回应帧 = 收到的 `kResult` 帧原样改帧类型后回发）：** 该回应帧由**收到的那一帧直接派生**，**不接受任何调用方参数**：

  | 字段 | 取值 |
  |---|---|
  | `payload` | **原样回显** `kResult` 的 payload |
  | `session_id` | 沿用（不变） |
  | `message_id` | 沿用（不变） |
  | `frm_type` | **仅此一处改动**：`kResult` → `kResponse` |
  | CRC | 由 `ICodec::Encode` 重新计算（`SystemCodec.cpp:52`），`ProtocolNode` 不碰 |

  **该帧是 `RequestForResult` 模型固有的最后一步**，不是可选行为，故无独立接口（原 `RequestForResultAndConfirm` 已取消，见 **D1** 的修正）。
  **不破坏"框架不解读 payload"**：payload 是整块拷贝，框架不读其内部结构——`Message::payload` 的定位（"应用字节，框架不解读其语义"）得以保持。
  **不得走 `Send()`**：它会强制 `session_id = NextSession()` 与 `frm_type = kCommand`，覆盖掉需要沿用的值；应走不盖章的私有 `EncodeAndWrite()`。

- **D9（接收侧不建模，维持现状）：** 本 ADR 只定义**发起方**的状态机。节点收到 `kCommand` 后如何回 `kResponse`/`kResult`、④ 中如何等待对方确认，仍由**宿主 `Subscribe` 后自行处理**，框架不提供对应的接收侧辅助。
  依据：接收侧的应答内容与时机是纯业务决策，框架无从代劳；且 ADR-0009 已确立"入站由订阅承载、消费样板交调用方"。

- **D10（`Send` 与 `Request` 本轮均保留，重叠问题推迟）：** 新增三个方法，现有 `Send(Message)` 与 `Request(Message, timeout)` **不动**。
  **明确记录的代价**：`Request`（总超时、无重发）与 `RequestForResponse`（单次超时 + 重发）**语义相近而不相同**，并存期间调用方容易用错。二者的合并/取舍**推迟决定**，不在本轮解决。

- **D12（两阶段失败以不同错误码区分，新增 `kNotAccepted`）：** 在 `TransportErrc` 增加一个值：

  | 失败点 | 返回 | 语义 |
  |---|---|---|
  | 阶段一：重发次数耗尽仍无 `kResponse` | **`kNotAccepted`**（新增） | 对端**始终没有受理** |
  | 阶段二：已受理，等 `kResult` 超时 | `kTimeout`（现有） | 受理了但没出结果 |

  依据：① 阶段一失败的本质**不是"超时"而是"未受理"**——重发 N 次都无回应，与"已受理但执行慢"是两类事实，用同一个码表达会丢失调用方需要的信息；② 只加**一个**值而非 `kAcceptTimeout`/`kResultTimeout` 两个，`kTimeout` 得以保持原义；③ `kNotAccepted` 在本协议之外也讲得通，不是为单一协议造的词。
  **④ `RequestForResultDirect` 的失败返 `kTimeout`**：它没有受理阶段，"未受理"这一事实不存在，重发耗尽的语义就是"始终没拿到结果"。
  **越层性说明**：`TransportErrc` 名字虽带 Transport，实为**全项目共用**的错误枚举（`ProtocolNode` 现已返回其 `kClosed`/`kTimeout`/`kConfiguration`），故加值不构成越层；SRS **RT_ERROR_003** 写的是"**最低**错误类别应包括……"，是下限而非穷举，加值不违反该条。

- **D13（`RequestForResultDirect` 属另一种协议，与外部系统协议的三种并列于同一节点；SRS 落点 RT_NODE_002_g）：** 该交互（请求 → 直接等结果、无中间受理、结果不回应、等结果超时可重发）**不属于**外部系统协议的五种行为，是**另一种协议**自有的交互形态。
  它仍由 `ProtocolNode` 承载而非另起节点类型——`ProtocolNode` 对线缆格式不透明（编解码经 `ICodec` 注入），协议差异体现在**用哪些帧类型、走哪种交互**，不构成新的节点类型。这与 ADR-0003 **D10**"协议无关机制可复用、协议特有语义内联"一致。
  **由此产生的并存**：同一节点上四个交互方法分属**两种协议**，调用方须自行确保所用方法与对端协议匹配；框架不校验（它对协议语义不透明）。该风险记于「影响」。

- **D11（`repeating` 本轮不做）：** 第五种交互行为暂不定义。RT_NODE_002 相应改为"前四种已定义，`repeating` 仍为 TBD"。

## 影响（Consequences）

- **正面：** 一个看似要新机制的需求（多段交互 + 重试）**没有引入任何新机制**——完全由 `Dispatcher` 的既有性质（投递不终结订阅、键可部分匹配）加调用方控制流实现；节点不新增状态、不新增成员、无并发面变化；四种模式各自独立，互不影响。
- **负面（明确接受）：**
  1. `Send`/`Request` 与新接口的语义重叠（D10）。
  2. ~~两个阶段的超时都返回 `kTimeout`，调用方无法区分……~~ **本条已由 D12 解决（2026-08-26）**：阶段一失败返 `kNotAccepted`，阶段二返 `kTimeout`。
  3. 重发按 D3 沿用同一帧，故**要求对端能容忍重复命令**（幂等，或自行按 `session_id` 去重）。这是协议层假设，框架不校验。
  4. `RequestForResult` 与 `RequestForResultDirect` 的调用方必须知道**结果帧的命令码**（D7），比 `Request` 多一项协议知识。
  5. **同一节点上并存两种协议的交互方法**（D13）：`Send`/`RequestForResponse`/`RequestForResult` 属外部系统协议，`RequestForResultDirect` 属另一种协议。调用方须自行确保所用方法与对端协议匹配，**框架不校验**（它对协议语义不透明）。
  6. `RequestForResultDirect` 在等待结果阶段**会重发**，故其对端**同样须容忍重复命令**——与代价 3 同源，但适用面更广（③ 只在受理阶段重发，④ 在唯一的等待阶段重发）。
- **实现注记：** 重发时帧字节完全相同（D3），故可编码一次、重复写出，不必每次 `Encode`。

## 备选方案（Alternatives considered）

- **单一 `Request` 接口 + 模式参数**（如 `Request(msg, Mode::kWithFeedback, ...)`）：否决——参数随模式而异（③④ 才有 `result_mid`/`result_timeout`，④ 才有确认 payload），并入一个签名会产生大量"该模式下无意义"的参数；分成四个方法则每个签名恰好承载该模式所需。
- **模式存入节点、由节点驱动状态机**：否决——需要"在途交互表"保存阶段/重发计数/原始帧，且 `Dispatcher` 要认识模式。而每模式一个接口后，这些状态天然活在调用方栈上（D1），新增机制为零。
- **第二阶段也重发**：否决——`kResult` 意味着对端正在执行，重发命令有使其重复执行的风险（D2）。
- **`kResult` 的命令码由框架按规则推导**（如 `cmd | 0x8000`）：否决——那是协议知识，写死任何规则都会在换协议时失效（D7）。
- **同时给出接收侧的四个对应辅助**：否决——应答内容与时机是业务决策，且与 ADR-0009"消费样板交调用方"的方向相悖（D9）。
- **为 `RequestForResultDirect` 所属的协议另起一个节点类型**：否决——`ProtocolNode` 对线缆格式不透明（编解码经 `ICodec` 注入），两种协议的差异只体现在"用哪些帧类型、走哪种交互"，不构成新的节点类型；另起节点会复制整套生命周期与分发骨架（D13）。
- **按"是否回应结果"把 `RequestForResult` 拆成两个接口**（原 `RequestForResultAndConfirm`）：否决——经核实 `withfeedback` 与 `needfeedback` 是**同一个通信模型**，回应结果是该模型固有的最后一步而非可选项，拆开只会制造一个永远不该被单独调用的接口（D1 修正）。
