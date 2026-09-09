# ADR-0015：`DdsNode` 的注册改为可动态增删，端点随注册变化

**状态：** Accepted
**日期：** 2026-09-08
**关联：** **放开 ADR-0013 D15/D16 的相位约束**（原「注册只在 `Created`、端点只在 `DoStart()` 建」）；**撤销 ADR-0013 D12 的「四组全空即 `kConfiguration`」判据**；ADR-0013 **D13**（`IDdsProvider::DeclareWriter` —— 本 ADR 为其补上对称的 `UndeclareWriter`）、**D6**（服务名派生 topic —— 沿用）；SRS **RT_DESIGN_006**（provider 可替换 —— 本 ADR 的接口扩展对其有破坏性影响）。

## 背景（Context）

ADR-0013 **D16** 把 topic 从配置换成四个注册方法，**D15** 把端点声明全部前置到 `DoStart()`，并明确「运行期不再有任何建端点的路径」。

该形态假设**启动前就知道全部 topic**。真实部署里这一条常常不成立：服务清单来自运行期才拿到的配置、对端能力协商之后才确定、或宿主本身就要按需增删订阅。这类节点在现形态下只能「全停—改注册—重启」，而重启会丢掉在途交互并重新付一遍发现窗口。

### D15 当初解决的是一个已被实测钉死的问题

**新建出的 `DataWriter` 在与对端 `DataReader` 匹配上之前，写出的帧会静默丢失**——`Publish` 照样返回成功。仓库里有跑在真实 Fast DDS 上的用例守着这条事实（`tests/dds/fast_dds_provider_test.cpp`，`FrameWrittenBeforeMatchIsLostButFirstFrameAfterDeclareArrives`）：

```cpp
p.tx.DeclareWriter("first-frame");
ASSERT_EQ(p.tx.MatchedCount().matched, 0);   // 还没有 reader
p.tx.Publish("first-frame", kLost);          // ← 这一帧丢了，而 Publish 返回【成功】
// 对端 Subscribe 并等到 matched 之后
p.tx.Publish("first-frame", kFirst);         // 这一帧才可靠到达
```

发现窗口约 **240ms**。D15 把全部端点前置，就是为了把这个窗口在启动时一次付清；`DdsNode.FirstReplyIsNotLostBecauseWritersAreDeclaredAtStart` 守的正是它。

**本 ADR 明确接受这个洞的回归**，理由与代价见下。

### 一处结构事实：读路径不读注册表

`DecodeAndDispatch` / `Dispatch` **完全不触碰四个注册集合**，只走 `Dispatcher`；读注册表的 `Publish` / `RequestForResultDirect` / `Reply` / `Subscribe` / `IsReaderSideTopic` 全在业务 fiber 上。加之节点各 fiber 同线程固定亲和（`Affinity::fixed`），**动态改注册表不会与读循环撞车，本 ADR 不引入任何锁**。

## 决策（Decision）

- **D1（四个注册方法的相位放开到 `Created ∪ Running`）：** 语义按相位分两段，**方法签名与返回类型不变**：

  | 相位 | 行为 |
  |---|---|
  | `Created` | 只落注册表；端点仍由 `DoStart()` 统一建（**与本 ADR 之前逐字相同**） |
  | `Running` | 落注册表**并当场建出对应方向的端点** |
  | `Closing` / `Closed` | 返 `kClosed` |

  **端点方向的派生规则一字不改**（D15/D6）：`Publishers`→W、`Subscribers`→R、`Clients`→`cfg.<名>.request` W + `cfg.<名>.response` R、`Services`→`cfg.<名>.request` R + `cfg.<名>.response` W。`DoStart()` 与动态路径**共用同一个建端点函数**，不写两份。

- **D2（配套四个注销方法）：** `UnregisterPublishers` / `UnregisterSubscribers` / `UnregisterClients` / `UnregisterServices`，签名与对应的注册方法同形，相位同为 `Created ∪ Running`。`Created` 期只从集合移除（本就没建端点）。

- **D3（拆端点必须重算需求，不得盲拆）：** **一条 topic 可被多个注册项同时需要**——例如 `cfg.get.request` 既是某条 `Services` 的请求 topic，也可能被显式注册进 `Subscribers`。故注销的顺序是：

  ```
  从集合移除 → 重新判定该 topic 是否仍被【任一】注册项需要 → 确已无人需要才拆
  ```

  读侧沿用现成的 `IsReaderSideTopic()`（`Subscribers ∪ 各 Services 的 request ∪ 各 Clients 的 reply`）；写侧新增对称的 `IsWriterSideTopic()`（`Publishers ∪ 各 Clients 的 request ∪ 各 Services 的 reply`）。

  **盲拆的后果是静默的**：拆掉一条仍被需要的 reader，该 topic 上的消息从此不再到达，而框架没有任何归因出口（ADR-0014），排障只能从"消息没来"倒推。

- **D4（`IDdsProvider` 补上 `UndeclareWriter`）：** 读侧拆除已有 `Unsubscribe(topic)`，写侧**此前根本无法表达**。补一个与 `DeclareWriter` 对称的 `UndeclareWriter(topic)`，同样要求幂等（拆一个不存在的 topic 直接成功）。

  `DdsTransport` 相应补上 `UndeclareWriter` / `UndeclareReader` 两个公开方法，与既有的 `DeclareWriter` / `DeclareReader` 对称。**`ITransport` 七方法仍不变**——这四个都是 DDS 专有的声明面，与 D15 的处置一致。

- **D5（撤销「四组全空即 `kConfiguration`」）：** ADR-0013 **D12** 的这条判据依据是"一个什么都不收不发的节点必是漏了注册"。**注册可以在启动之后补上之后，该依据不再成立**——"启动时还不知道有哪些 topic"正是本 ADR 要支持的主要场景。故 `DoStart()` 不再拒绝全空注册。

- **D6（在途交互不做检测，注销即时生效）：** 注销一个正在服务的服务名不会被拒绝，也不等待在途交互结束：

  | 对象 | 注销后 |
  |---|---|
  | 已发出的 `Ticket`（含 `ServeRequests` 的） | **继续有效**——它挂在 `Dispatcher` 上，与注册表无关 |
  | 新到的请求 | 不再到达（reader 已拆） |
  | 对已注销服务的 `Reply()` | 返 `kConfiguration` |
  | 对已注销服务的 `RequestForResultDirect()` | 返 `kConfiguration` |

  **不引入"拒绝注销正在使用的服务"这类检测**：在途与否需要一份额外的引用计数，而它能挡住的只是调用方自己的误用；框架已在每条路径上给出明确错误码，不是静默失败。

- **D7（`Running` 期注册的批量语义与 `Created` 期一致）：** 「整批生效或整批不生效」照旧。次序为**校验 → 逐项建端点 → 提交集合**；中途某项建端点失败，则**拆掉本批已建的端点**并返回该错误，注册表一项不落。

## 明确接受的代价

1. **动态注册出来的写侧端点，首帧会静默丢失。** 这是 D15 消除、本 ADR 放回来的那个洞（背景已列实测）。后果**对四种用法不均等**：

   | 用法 | 首帧丢了的后果 |
   |---|---|
   | `RequestForResultDirect` | 有重发兜底，第二次尝试即补上——**基本无感** |
   | `Reply` | 应答丢一次，客户端重发再问一遍——可恢复 |
   | **`Publish`** | **无重发，永久丢失，且静默** |

   **框架不提供任何"何时安全"的判据**：`IDdsProvider::MatchedCount()` 是**参与者级聚合、不带 topic 参数**，节点上已有别的端点匹配着时它恒为正，对新注册的那条 topic 一无所知。故框架**给不出**"这条新 topic 何时可安全发送"的答案。

   **为什么可以接受（2026-09-08 裁决，复核后重申）：** 该风险**由宿主自行评估与处置**——宿主知道自己这条 topic 的业务含义：是周期上报（丢一帧无所谓、下一帧就补上）、是一次性通知（那就自己压一个延时或改用带重发的交互）、还是根本不在乎。**最差的处置就是不处置、接受这一帧丢失**，而这在绝大多数用法下是可容忍的。框架不替宿主做这个判断，也不为此强加一次等待。

   > 消除它的做法是给 `IDdsProvider` 再加一个**按 topic** 的匹配查询，并据此提供"注册后等到就绪"的能力（备选方案里的 **C**）。**本轮明确不做**：它的价值是替宿主兜住一个宿主自己更清楚的风险，而代价是把一个本可以立即返回的注册动作复杂化。既然 **D4** 已经在动 `IDdsProvider`，将来若实测表明这个洞确实在咬人，补上的边际成本很小。

2. **端点集合不再"启动即定型"。** ADR-0013 D15「连带收益 1」（`DataWriter` 不再累积）与「连带收益 2」（运行期无端点创建，故回应路径上不会突然吃一个 240ms 发现窗口）**均告失效**。有了注销，增长至少是**可控**的，但"运行期端点集合恒定"这个可依赖的性质没有了。

3. **`IDdsProvider` 的扩展对第三方 provider 是破坏性的。** 新增纯虚 `UndeclareWriter` 意味着任何自建 provider 都必须跟着实现。SRS **RT_DESIGN_006**「应支持替换 provider」仍成立，但**替换的成本上升了一个方法**。

4. **注销的正确性依赖 D3 那次重算。** 这是本 ADR 引入的**唯一一处需要全局推理**的逻辑：判断错了会静默拆掉仍被需要的端点。它由 `IsReaderSideTopic` / `IsWriterSideTopic` 两个函数独占承担，测试须覆盖"多注册项共用一条 topic 时逐个注销"的全部组合。

## 影响（Consequences）

- **正面：** ① 启动时不必知道全部 topic；② 增删订阅/服务不再需要重启节点，在途交互不被打断；③ 写侧拆除首次成为可表达的操作。
- **负面（明确接受）：** 见上四条。
- **对 ADR-0013：** **D15** 的相位约束（"运行期不再有任何建端点的路径"）与 **D16** 的"只在 `Created` 受理"被本 ADR 放开；**D12** 的四组全空判据被撤销。三条决策的其余部分（端点方向派生、`reply_to` 降为交叉校验、服务名派生规则）**全部沿用**。
- **对 SRS：** `RT_IF_DDS` 的注册相位描述放宽；追溯矩阵相应更新。
- **对 SDD：** `DdsNode` 的相位表与端点生命周期一节改写；新增 `IsWriterSideTopic` 与注销路径的详细设计。
- **对 README：** 「相位规则」表与 DDS 一节补动态注册与注销的用法，并**显式写出首帧丢失这条代价**。

## 备选方案（Alternatives considered）

- **注册立即返回 + 独立的 `AwaitTopicReady(topic, timeout)`（原方案 C）。** 形状与本项目的 `Close()` / `WaitClosed()` 同构——发起与等收敛分开，要不要等由调用方按用法决定（`Publish` 该等，`RequestForResultDirect` 可以不等）。

  **否决理由（2026-09-08 复核后重申）：** 它替宿主兜住的，是一个**宿主自己更清楚的风险**——某条 topic 丢首帧要不要紧、该怎么补，取决于该 topic 的业务含义，框架无从判断。为此在注册面上多出一个方法与一套超时语义，不划算。**首帧风险交由宿主评估处置，最差即不处置、接受丢失**（见「明确接受的代价」①）。

  > 记此以备将来：这条路仍是消除该洞的正解。**D4** 已在动 `IDdsProvider`，若实测表明这个洞确实在咬人，补上的边际成本只是再多一个方法。
- **注册方法自己等到就绪再返回（原方案 B）。** 调用方拿到成功即可安全发送，不会忘。**否决理由：** 同一个方法在 `Created` 期立即返回、在 `Running` 期会挂起，**两种语义**；且强制所有调用方都付等待成本，即便是有重发兜底、本不需要等的路径。
- **只放开读侧（`Subscribers` / `Services` 的 request reader），写侧仍须前置。** 读侧动态增加没有首帧丢失问题。**否决理由：** `Services` 要 `Reply` 就得有 response writer，`Clients` 要发请求就得有 request writer——纯读侧动态几乎没有可用场景，且这种不对称难以向调用方解释。
- **保留「四组全空即 `kConfiguration`」。** **否决理由：** 见 **D5**——它会挡死"启动时还不知道有哪些 topic"这个本 ADR 的主要场景。
