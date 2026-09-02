# ADR-0011：TCP 按 `UdpTransport` 样板重构，三件套收成一个 `TcpTransport`

**状态：** Proposed
**日期：** 2026-08-27
**关联：** ADR-0007（泵 + 读写双队列——本 ADR 是其 **D6** 所说"TCP 跟进本形态"的兑现，并**否决**了其中"字节流必须另定队列策略"的预设，见 **D6**）；ADR-0005 **D4**（固定间隔重连，本 ADR 沿用）；ADR-0008 **D1/D10**（`ITransport` 七方法、观测面收为 `ITraceSink` 一条出口）；ADR-0010（四种交互的重发次数——是 **D6** 可行性的前提之一）。
**实施范围：** `TcpTransport` 数据面与重连（详细设计与伪代码见 SDD **§5.6.1**：`Connect()` / **重连完整设计** / 读路径 / 写路径 / `Close()` 打断点 / `CurrentLinkState()`）。**`TcpServer` 不在本轮**（见 **D10**）；`ApplyConfig` 热更新**待定**（见 **D11**）。

## 背景（Context）

`UdpTransport` 已按 ADR-0007 落地为「socket 管理泵 + 内层数据泵 + 读写双队列」，是其余介质跟进的样板。TCP 侧仍是重设计之前的三件套，共 2065 行：

| 件 | 行数 | 职责 |
|---|---|---|
| `TcpTransport` | 406 + 102 | 已连接的裸字节管道 |
| `TcpClientTransport` | 856 + 180 | 连接状态机 + 自动重连 + `ApplyConfig` + 诊断面，内部持一个 `TcpTransport` |
| `TcpServer` | 344 + 102 | accept，每连接派生一个 node |

且 `TcpClientTransport.hpp:126` 的 `WaitForState(ConnectionState, OperationOptions)` 引用着**已删除的 `OperationOptions`**——该头文件当前根本编译不过，三件均排除于编译面。

ADR-0007 **D6** 曾就字节流留下一条预设：

> **该推理不适用于字节流介质**：`readAll()` 吐的是任意字节切片而非完整包，丢中段即帧错乱。TCP/串口跟进本形态时**必须**另定策略。

本 ADR 的核心结论是**该预设不成立**（见 **D6**）——字节流可以沿用同一套丢弃策略，因为丢弃的后果在本项目里已有两层现成的补救。

### 一个决定形状的核实

`FiberChannel`（AsyncTask）的能力边界经核实为：

- `push` **只有非阻塞的 drop-oldest 一种模式**，没有阻塞式 push；
- **没有公开的 `size()`**，只有 `capacity()`；
- drop-oldest 是**静默**的——挤掉队首后 `push` 照样返回 `success`。

第三条是关键：**溢出本身不可观测**。因此"大容量 + 溢出即判链路不可用"这条看似稳妥的路子**在当前原语上无法实现**——检测不到溢出，就无从据此断链。真正的选项只剩"无界"与"有界丢弃"两种，而**"有界 + 满时阻塞生产者"若要实现，须先改 AsyncTask**（加 `size()` 让泵自行节流，或加阻塞 push）。

## 决策（Decision）

- **D1（三件收成一个 `TcpTransport`，重连内建）：** 不再分"裸管道 / 重连外壳"两层。重连是 TCP 客户端的**固定语义**（RT_TCP_RECONNECT_001 明确"不设运行时开关"），把它做成外层泵的一部分即可，无需为它单设一个包装类。
  依据：分层的原有理由是"`TcpServer` 复用裸管道"，但服务端连接与客户端连接的**外层泵形态本就不同**（前者不重连，断即终结），复用的是数据面而非泵——数据面在新形态下只是"读流 → push / 出队 → 写"两段代码，不值得为它保留一层类。

- **D2（形态照 `UdpTransport` 样板）：** 外层 socket 管理泵 + 内层数据泵 + 读写双队列；两条队列**不随连接重建而更换**，故重连对调用方完全透明。

  ```
  外层泵（socket 管理）:
    while (未被 Close) {
      connect_waiter_ = coro(socket).connectToHost(host, port)   // 存成员，供 Close 打断 (D15)
      if (await_for(connect_waiter_, timeout))                   // ← 同一个量 (D5)
        generation_ + 1
        socket_ready.discard_pending(); resolve()
        read_stream_ = coro(socket).readAll()                    // 存成员，供 Close 打断 (D15)
        for (;;) { r = await_for(read_stream_, timeout);         // ← 同一个量
                   if (!r) { 归因落 LastError; break }
                   push(切片) }
        read_stream_.reset()
      else
        归因落 LastError
        await_for(close_signal, timeout)                         // ← 退避，同一个量
      connect_waiter_.reset()
      socket->abort()                     // 清理，【不是打断手段】(D15)
    }

  写泵:
    for (;;) {
      item = await(write_queue)                    // 阻塞点①
      for (;;) {
        if (closing) return
        if (已连接) break
        socket_ready.discard_pending(); await(socket_ready)   // 阻塞点②
      }
      n = socket->write(bytes)        // 同步，无挂起点；不等刷出 (D13)
      if (n != size) 记 LastError      // 短写视为链路异常，放弃残余
    }
  ```

  **不自终**（沿用 ADR-0007 D2）：连接失败、读流终止、静默超时一律回外层重试，唯一退出条件是我方 `Close`。

- **D3（整个生命期一个 `QTcpSocket`，`abort()` 后原对象复用）：** 每轮末尾无条件 `abort()`，socket 回到 `UnconnectedState`，下一轮在**同一对象**上再 `connectToHost()`。
  依据：与 UDP 的「bind → close → 再 bind 复用同一 socket」是同一个模式。**不需要每代际新建 socket 对象**——`abort()` 已经把状态、缓冲与挂起的信号一并清干净。

- **D4（判活：断开事件为主，静默超时为辅）：** TCP 有真正的断开事件，它是链路坏了的**主判据**；`silence_timeout` 降为**补充**，用于检测**半开连接**（对端进程消失但 FIN 未达，socket 仍显示 Connected）。
  这与 UDP 相反：UDP 无连接、无断开事件，`silence_timeout` 是其**唯一**主动判据。

- **D5（唯一的时间量：等连上 / 读静默 / 重连退避三处共用一个）：** 沿用 ADR-0005 **D4** 的固定间隔（无倍率、上限、抖动、稳定重置），并**照 `UdpTransport` 合并为单一时间量**。

  ```cpp
  struct TcpConfig {
    std::string   host = "127.0.0.1";
    std::uint16_t port = 0;
    /// 唯一的时间量，三处共用：等连上 / 读静默判链路坏 / 连不上时的重连间隔。
    std::chrono::milliseconds silence_timeout{5000};
    ITraceSink* trace_sink = nullptr;
  };
  ```

  这与 `UdpTransport` 的做法**逐字相同**——其泵注释写着"两处的 timeout 是**同一个量**：有链路时它是'多久没数据算坏'，没链路时它是'多久试一次 bind'"。TCP 只是多了第三处用途（等连上），仍是同一个量。

  **连接不设单独的时限**：`connectToHost` 的等待用的就是这个量，不再有 `connect_timeout` 这个旋钮。**由此三个旋钮收成一个。**

  **`silence_timeout` 须为正，不再有"0 = 禁用"这一档。** UDP 允许 0（禁用静默判活，内部回落到默认值），TCP 不行——**它同时是重连退避间隔**，0 会退化为紧循环（对端主机在而端口未监听时内核立即回 RST，`connect` 微秒级失败，烧 CPU 且向对端刷 SYN）。该校验见 **D14**。

  > **本条推翻本 ADR 初稿的 D5。** 初稿保留 `connect_timeout` / `reconnect_interval` / `silence_timeout` 三个量，理由是"UDP 的 `bind` 同步且瞬时、不需要第三个量；TCP 的 `connect` 是异步的、自带一个超时，该技巧不能整体搬运"。**该理由不成立**——异步的 `connect` 同样可以用这一个量来等，"自带超时"并不要求它是**独立**的旋钮。2026-08-27 裁决：三个合一。

- **D6（队列策略：沿用默认有界 1024 + 丢最旧，不为字节流另设）：** 推翻 ADR-0007 **D6** 的预设。丢中段确实导致帧错乱，但**错帧在本项目里有两层现成的补救**：
  1. **codec 自带重同步**——`ScanSystemFrames`（`SystemCodec.cpp:71`）逐字节扫 4 字节帧头，CRC 不匹配即 `off += 1` 继续扫，未消费的尾巴留在 `buffer_` 与下批字节拼接。丢弃只会毁掉跨越丢弃点的那一帧，**其后的帧照常解出**。
  2. **交互层重发**——`RequestForResponse` / `RequestForResult` / `RequestForResultDirect` 均带重发（ADR-0010），被毁的那一帧由重发补回。

  由此**不需要改 AsyncTask**，也不需要为字节流单设一套策略。代价见下"明确接受的代价"①②。

- **D7（写侧不保证"整条同代际"，故写泵不需要代际号校验）：** 断链时写出去半条就是半条，**由对端自行重同步**，我方不做保证，也**不引入代际号校验**。
  依据：对端与我方用同一套帧格式，重同步能力对称；且半帧毁掉的那次交互由 D6 所述的重发补回。
  **半帧从何而来**：本设计的写泵**自身没有挂起点**（见 **D13**），故不是"写到一半被调度走"；而是 `write()` 交给 Qt 内部缓冲后，其**异步刷出**尚未完成时链路断开，`abort()` 把未刷出的字节丢弃。成因在 Qt 侧，我方无从、也不打算保证。
  由此 `UdpTransport` 头注释中"该不变式只对 UDP 成立（串口/TCP 的写有挂起点）"的那条差异**被消除**——两个写泵在结构上完全同构。

- **D8（`Datagram::peer` 读写两侧一律填固定对端）：** TCP 是点对点，`peer` 没有 UDP 那种"每报文各异的来源/目的地"语义。读侧每个切片的 `peer` 填 `Endpoint::Net(config.host, config.port)`；写侧**忽略**调用方填的 `peer`，一律发往该固定对端。
  写侧不因 `peer != kDefault` 而判 `kInvalidArgument`——那会让"传输无关的调用方"在 TCP 上跑不起来，而 D8 的目的正是让同一个 node 换传输即可运行。

- **D9（删除诊断面 `Generation()` / `AttemptCount()` / `LastFailure()`）：** 三个 TCP 独有的具体方法一并删除，观测只剩 `ITraceSink` 一条出口。
  依据：ADR-0008 **D10** 已把**全部**计数与时延 getter 删除（`NodeBase` 的 close_drop 与关闭时延、`ProtocolNode` 的八个观测接口、传输的收发时间戳），"一个事实一条出口"。这三个是同一批里的漏网者，没有理由单独留存。代际号本身**保留为内部记账**（供 Trace 归类与内部判重），只是不再对外暴露——注意 **D12** 已撤销连接状态机，故它不再有"状态机需要"这一用途。

- **D10（`TcpServer` 不在本轮）：** 服务端 accept 出来的连接**不重连**——断即终结，其外层泵只跑一轮。这是与客户端不同的泵形态，须单独设计。
  **与 D1 的相互作用**：三件收成一件后，`TcpServer` 复活时需要的是"同一个类的不重连模式"（一个策略位），而**不是**恢复旧的分层。该判断留待 `TcpServer` 那一票确认。

- **D11（`ApplyConfig` 热更新：待定）：** 现有 `TcpClientTransport` 支持运行时换端点（掐断当前尝试、立即以新端点重试），UDP 无对应物。**本 ADR 不决定其去留**，待专门讨论后补入。

- **D12（调用方不感知链路连接状态；撤销 MS_CONNECTION / 图 4-11，`CurrentLinkState()` 保留但定位为诊断用）：** 业务调用方**不需要、也不应**感知链路连接状态——重连对交互层完全透明（DD-11），链路不可用时发送**入队等待**（RT_TCP_RECONNECT_003），故调用方不必先查链路再决定是否发送。

  **核实**：`CurrentLinkState()` 在**生产代码中零使用者**——`ProtocolNode` / `NodeBase` / `examples` 均不调用，全部命中位于 `tests/`。"调用方不感知"不只是设计意图，**现状本来就没人在感知**。

  由此两条处置：

  1. **`CurrentLinkState()` 保留**，但文档如实登记其定位：**统一的 I/O 事实查询，不面向业务调用方，仅供诊断与测试观测**。保留的理由是实现**零成本**——与 `UdpTransport.cpp:320` 同法，当场由 `lifecycle_` 与 `socket_->state()` 算出，**不需要任何状态成员**：未 Running 或无 socket → `kDown`；`ConnectedState` → `kUp`；`ConnectingState`/`HostLookupState`/未连接但泵仍会重试 → `kEstablishing`。
     最后一支是 TCP 与 UDP 的**真正分歧**：UDP 未绑定即报 `kDown`（其注释明写"UDP 无连接，故**永不出现** `kEstablishing`"），而 `kEstablishing` 的枚举注释本就写着"正在建立（TCP 连接中 / **退避重连中**）；仅具连接管理的传输会给出"——这一支正是它存在的理由。
     **未采纳"连同 `CurrentLinkState()` 一并从 `ITransport` 删除"**：那是跨介质改动，会波及编译面内的 `UdpTransport`（`link_state_test.cpp` 整个废弃、`udp_transport_test.cpp` 约 11 处断言要改），且此后**无任何途径**得知链路死活。代价与收益不相称。

  2. **撤销 `MS_CONNECTION` 与图 4-11**（`state-connection.mmd`/`.svg` 删除，**节号与图号保留不复用**）。
     **理由：该状态机没有对应的代码实体。** 原图画 `Disconnected/Connecting/Connected/Reconnecting` 四状态 + 跃迁，但本 ADR 的设计里 `TcpTransport` **不持有连接状态枚举、也没有驱动跃迁的代码**——这四个"状态"实际是**外层泵所处的代码位置**。而"泵所处的代码位置"**正是图 4-15 画的东西**，且那张图有代码对应。两图讲同一件事、只有一张有实体，故删去无实体的一张，内容并入 §4.2.13。
     `RT_LIFECYCLE_002` 的四状态自此是**传输内部的设计要求**，其满足性由图 4-15 的泵形态承载。

- **D13（写泵不等刷出：`write()` 交给 Qt 内部缓冲即算完成）：** 写泵把一条报文 `write()` 出去后**不等 `bytesWritten`、不等 `bytesToWrite() == 0`**，直接回去取下一条。

  **依据**：
  1. **写本就是 fire-and-forget**（ADR-0007 **D3**）——写出的一切结果都不回传调用方、只落 `LastError()`。等刷出并不改变这一语义，只是让写泵多挂起一次。
  2. **写泵由此没有挂起点**——`socket_->write()` 是同步的，`setWriteBufferSize` 全仓未设（Qt 默认 0 = 无上限），故 `write()` 接受全部数据后立即返回。这使 `UdpTransport` 头注释中那条不变式——"**取到 socket 到写出之间没有挂起点**"——**对 TCP 也成立**。原注释写着"该不变式只对 UDP 成立（串口/TCP 的写有挂起点）"，本决策消除了这条差异：**两个写泵在结构上完全同构**。
  3. **吞吐不被每条一次往返拖累。**

  **短写的处置**：`writeBufferSize` 为无上限时 `write()` 不会短写；万一返回 `0 ≤ n < size`，视为**链路异常**——**放弃残余、记 `LastError()`**，不循环重试（既然不等刷出，短写就没有可等的东西）。残余丢失落在 **D7** 的既定范围内：写了半条即半条，由对端重同步。

  **明确接受的代价**：**Qt 内部写缓冲无上限**。链路长时间慢或断时，已交给 Qt 的字节会在其内部缓冲里积压——这是**有界的 `write_queue_` 挡不住的部分**（它只挡"还没交给 Qt"的那一段）。断链时 `abort()` 会把这些未刷出的字节一并丢弃，与 **D7** 一致。
  若将来需要给它设上界，可调 `setWriteBufferSize(N)`——但那会让 `write()` **真的开始短写**，届时须连同本条与 D7 一并重新评审。**本轮不设。**

- **D14（配置在 `Start()` 时一次性校验，失败停在 `Created`；这是 SRS「不可重试失败」清单的落点）：** `TcpTransport::Start()` 先校验配置，非法返 `kConfiguration`、**停在 `Created`**，未建 socket、未起泵，允许改配后重试（RT_LIFECYCLE_007）。

  | 字段 | 约束 | 若不校验的后果 |
  |---|---|---|
  | `host` / `port` | 非空 / 非 0 | 连不上且错误信息无意义 |
  | `silence_timeout` | **须为正**（**D5** 合一后是唯一的时间量） | 三重后果：① 零值退化为**紧循环**（RST 微秒级失败，烧 CPU 且向对端刷 SYN）；② 等连上无时限；③ 静默判活失效 |

  **合一使校验反而更关键**（**D5**）：原先 `silence_timeout == 0` 只是"禁用静默判活"这一档可选行为，如今它同时是**等连上的时限**与**重连退避间隔**，零值直接导致紧循环。故 TCP **不设**"0 = 禁用"这一档，与 UDP 不同。

  **与 `ProtocolNode` 的对照**：后者自 #173 起配置面上**已无任何校验项**（时限改为逐次传参，保护移交 `ValidateInteraction` 的参数校验）。`TcpTransport` **有**校验，因为它的配置里存在真正会导致灾难的值——尤以 `reconnect_interval == 0` 的紧循环为甚。**"节点无配置校验"不是可推广的结论**，不要据此推断传输也不该有。

  **本条是 SRS §3.1.7.4「不可重试失败」清单在新形态下的落点**。该清单与"**不自终**"（ADR-0007 D2）表面冲突，实则各管一段：

  | SRS 的"不可重试失败" | 新形态下的处置 |
  |---|---|
  | 无效配置 | **本条**——`Start()` 时一次性拒绝，根本不进入重连循环 |
  | 非法生命周期操作 | `Start()` / `AsyncWrite` 的前置判据返 `kInvalidState` / `kClosed` |
  | 显式关闭 | `while (lifecycle_ < kClosing)` 判据接住，泵正常退出 |
  | 运行时退出 | `readAll()` 已订阅 `QCoreApplication::aboutToQuit`，流随之关闭 → 走断链触发源 |
  | 内部不变量破坏 | 不设专门处置——本设计**无跨轮不变量**（每轮末尾 `abort()` 回到确定状态） |

  **其余一切失败都可重试**，一律降为 `LastError()` 的诊断事实、无限重试。SRS 的"可重试失败"清单全部落在这一支，**包括原本会自终的"致命 socket I/O 错误"**（ADR-0007 D2 已缩小 ADR-0005 D5 自终的适用介质，本设计沿用）。

  详细设计见 SDD **§5.6.1 ①'**。

- **D15（泵的两个等待点由我方句柄打断；`socket_->abort()` 在连接窗口内唤不醒任何等待）：** 管理泵的 `connect_waiter_`（等连上）与 `read_stream_`（读等待）**持为成员**，`Close()` 直接 `close()` 它们；`socket_->abort()` **保留但降为清理动作，不再充当打断手段**。

  **依据是三个实测探针**（2026-08-27，临时加入 `udp_transport_test.cpp` 后已撤）：

  | 探针 | 场景 | 结果 |
  |---|---|---|
  | 1 | socket 处于 **`ConnectingState`** 时 `abort()` | **唤不醒** `waitForConnected()`——挂满 3000ms 超时才返回 |
  | 2 | socket 处于 **`ConnectingState`** 时 `abort()`，对 `readAll()` 流 | **唤不醒**——同样挂满 3000ms |
  | 3 | 持句柄并 `close()` | **1 毫秒内两处均唤醒** |

  **补充（#179 的负向对照实测，2026-08-28）**：socket 处于 **`ConnectedState`** 时，`abort()` **确实能**终结 `readAll()` 流（实测 0ms）——因为它会发 `disconnected` / `errorOccurred`。故上表第 2 行的"唤不醒"**只对连接窗口成立**，不是关于 `abort()` 与 `readAll()` 的普遍结论。
  **但这不改变本决策**：连接窗口（`ConnectingState`）依然无人覆盖，而 `Close()` 可能恰好落在其中。#179 的负向对照同时测得：去掉 `read_stream_->close()` 后，读等待打断退化为**挂满 2949ms**——因为本设计的 `Close()` **根本不调 `abort()`**（它只在泵每轮末尾作清理）。三处句柄打断仍然**缺一不可**。

  成因：Qt 的 `QAbstractSocket::abort()` 在**连接中**的 socket 上**不发 `errorOccurred`**，而 `corosocket` 的 `waitForSignal` / `readAll` 都是靠 socket error 或 `disconnected` 终结的。

  > **本条推翻本 ADR 初稿的说法。** 初稿的 `Close()` 打断点表写着"**`socket_->abort()` 一处覆盖连接等待与读等待两处**，故动作数与 UDP 相同"。**实测证伪**：在连接窗口内它一处也覆盖不了。UDP 没有这个问题——它的 `bind()` 是同步的，不存在"连接中"这一窗口，其 `socket_->close()` 打断活跃读流是实测有效的。
  >
  > 若不改：`Close()` 若恰好落在一次连不上的连接尝试中，最坏要等**一整个 `silence_timeout`**（5s）才收敛——而 D5 合并时间量之后这个窗口反而变长了（原 `connect_timeout` 也是 5s，但原设计同样有此缺陷，只是没被发现）。

  **代价**：多两个成员与两处 `Close()` 动作。`Close()` 的动作数由 UDP 的四个增至**五个**（`close_signal_` / `connect_waiter_` / `read_stream_` / `write_queue_` / `socket_ready_`），另加一次非打断性的 `abort()`。**这是必要的复杂度，不是可省的**——UDP 的"四处打断缺一不可，漏一处即一次收敛挂死"在这里是"五处"。

  ### 补正（2026-09-02，#200 归因后）：持句柄还不够，建完须【复查生命周期】

  持成员句柄只解决了"`Close()` 够得着已存在的等待器"，**没解决"等待器是在 `Close()` 跑完之后才被创建"**——那种情况下 `Close()` 关的是两个 `nullptr`，新建的等待器**没有任何人会唤醒它**。

  ```cpp
  while (lifecycle_ < LifecycleState::kClosing) {   // ① 检查：此刻仍是 Running
      // ★ 若在此让出，Close() 跑完：置 kClosing，close 掉两个【当时还是 null】的句柄
      connect_waiter_ = coro(socket).connectToHost(...);   // ② 建一个【没人会关】的等待器
      auto connected = await_for(connect_waiter_, timeout); // ③ 干等满一个 silence_timeout
  ```

  **这是 check-then-act 竞态**：`lifecycle_` 的检查与等待器的创建之间不是原子的。`Close()` 的内部顺序没有错（它**先**置 `kClosing` **再**关句柄），错在它**关不到未来才出现的句柄**。

  **实证**（#200）：`CoroTcpTransport.CloseTerminatesReadQueueAndBlocksRestart` 在 master 上整组跑约 2.5%–10% 概率失败——`Close()` 后 `AwaitRead(rx, 1000ms)` 拿到 **`kTimeout`** 而非 `kClosed`，该轮耗时 **3011ms ≈ 恰好一个 `silence_timeout`**。读队列只在泵退出循环后才关，而泵正卡在那个无人唤醒的等待器上。

  **故本决策补一条不变式**：

  > **句柄一旦赋给成员，须立即复查 `lifecycle_`；已 `kClosing` 则就地终结，不进 `await`。**

  `connect_waiter_`（②）与 `read_stream_`（②之后建流处）**两处都要**。**只在循环顶部判 `lifecycle_` 是不够的**——那正是被竞态跨过的那一步。

  **裁决（2026-09-02）**：`Close()` **须当场打断**，不接受"最坏等一个 `silence_timeout`"。放宽用例 budget 的方案**已否决**——用例名断言的就是"`Close()` **终结**读队列"，调用方看到的确实是"`Close()` 之后读句柄还会挂数秒"，那是**实现缺陷**，不是用例太严。

## 明确接受的代价

1. **队列丢弃对四种交互的后果不均等。** `Send`（noresponse）是纯 fire-and-forget、**无重试**——丢掉它的字节就是永久丢失，没有任何恢复路径。D6 的第 2 层补救只覆盖三个 `RequestFor*`。这不推翻 D6（noresponse 本就不保证送达），但调用方须知：**在丢弃可能发生的链路上，`Send` 的送达率低于带重发的交互**。

2. **丢弃无归因，§3.6 的完整性恒等式继续破着。** drop-oldest 发生在 `FiberChannel` 内部，`push` 返回 `success`，外部**观测不到**。SRS §3.6 的 `Σ命名原因 == 总丢弃` 与 `drop_records.size() == Σ` 在这条路径上不成立。ADR-0007 D6 已将其记为"活跃隐患"，#152 / #176 是同一件事的两侧。**选择 D6 即等于接受它继续破着**，直到那两票处置。

3. **断链残尾与新链路首字节可能拼成错帧。** 重连对调用方透明，codec 的 `buffer_` 里可能留着旧连接的半帧，与新连接首字节拼接。由 codec 校验与重同步处置（报坏帧后恢复），框架**不**另设 codec 重置动作。此项 SRS §3.1.7「透明重连的已知代价」已登记，本 ADR 不改。

5. **Qt 内部写缓冲无上限（D13）。** 写泵不等刷出，已交给 Qt 的字节在其内部缓冲里积压，**有界的 `write_queue_` 挡不住这一段**。断链时 `abort()` 一并丢弃，与 D7 一致。设上界需 `setWriteBufferSize(N)`，但那会引入真正的短写，本轮不设。

4. **写半帧需对端容忍。** D7 的前提是对端具备与我方对称的重同步能力。若将来对接的外部系统**不具备**该能力，D7 须重新评审。

## 影响（Consequences）

- **正面：** ① TCP 与 UDP 形态统一，两处泵的差异收敛到"Connect 替代 Bind"与"判活主判据"两点；② 公开面按 `ITransport` 七方法收齐，`WaitForState` / `State` / 三个诊断方法一并去除，TCP 不再有"具体诊断非多态缝"这一例外；③ 2065 行预计显著缩减；④ 恢复 TCP 进入编译面与测试面。
- **负面（明确接受）：** 见上五条。
- **对图集：** `MS_CONNECTION` / 图 4-11 撤销（**D12**），新增 `MS_TCP_PUMP` / 图 4-15。SDD §4.2.8 保留为撤销标记（节号不复用），追溯矩阵中 `RT_LIFECYCLE_002` 与 `RT_TCP_RECONNECT_001..005` 的落点由 `MS_CONNECTION` 改指 `MS_TCP_PUMP`。
- **对 ADR-0007：** **D6** 中"字节流必须另定队列策略"的预设**被否决**（依据：codec 重同步 + 交互层重发）；其余不变。
- **对 ADR-0005：** **D4** 固定间隔沿用；其"撤销四套退避参数"的结论在本 ADR 继续有效。
- **对 SRS：** `RT_TCP_RECONNECT_003` 的"投入发送队列等待链路恢复，不拒绝、不丢弃"为准；**§3.1.7 中"链路不可用时发送立即失败，不缓存等待重连"一句作废**（2026-08-27 裁决，该句是 ADR-0004 时代遗留，漏跟 ADR-0007 D3 的改动）。`RT_TCP_RECONNECT_002` 所引"各自总超时（含缺省值，见 3.1.4.4）"须重写——该缺省值已随 #173 删除。诊断面相关条目随 **D9** 调整。

## 备选方案（Alternatives considered）

- **给 AsyncTask 加 `size()`，泵自行节流实现真背压。** 泵在读之前查深度，超水位就**不去 `readAll`**，数据留在内核接收缓冲区，缓冲涨满 → TCP 窗口收缩 → 对端减速。这是教科书式的正确背压，且 `size()` 是通用能力（写侧可据此返 `kResourceExhausted` 并归因，顺手修 #176；UDP 侧 #152 的归因也依赖它）。
  **否决理由：** 需要改 submodule、跨仓协调；而 D6 的两层补救已使丢弃的实际后果可控。**该方案未被排除，只是不在本轮**——若 #152/#176 处置时决定加 `size()`，本 ADR 的 D6 应重新评审。

- **给 AsyncTask 加阻塞式 `push_wait`。** 比 `size()` 更直白。**否决理由：** 引入新的死锁面（`close` 时必须唤醒阻塞在 push 上的生产者），且效果与前者等价——泵挂在 push 上同样不再读 socket。只服务一个场景却带来更大风险。

- **读队列设 `setCapacity(0)` 无界。** 字节流正确性最优（不丢就不会错帧）。**否决理由：** 慢消费者会让队列无限增长且**完全没有反压**，把"丢几帧"换成了"OOM"；而丢几帧有 D6 的两层补救，OOM 没有。

- **保留"裸管道 + 重连外壳"两层分层。** **否决理由：** 见 D1——复用的是数据面而非泵形态，而新形态下数据面只剩两段短代码。

- **连同 `CurrentLinkState()` 一并从 `ITransport` 删除。** 理由与 ADR-0008 **D10** 同构——无人消费即删，观测只留 `ITraceSink` 一条出口；而它确实**零生产使用者**。
  **否决理由：** 跨介质改动，波及编译面内的 `UdpTransport`（`link_state_test.cpp` 整个废弃、`udp_transport_test.cpp` 约 11 处断言要改），且此后**无任何途径**得知链路死活——连诊断与测试的观测点都没了。而保留它的实现成本是**零**（无状态成员，当场由 `lifecycle_` 与 `socket_->state()` 算出）。代价与收益不相称，故保留并如实登记定位（**D12**）。

- **溢出即判链路不可用、断链重连。** **否决理由：** 在当前原语上**无法实现**——drop-oldest 静默、`push` 返 `success`、无 `size()`，溢出不可观测。此方案实为前两个备选的下游，须先改 AsyncTask。
