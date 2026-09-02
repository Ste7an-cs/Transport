# ADR-0012：串口按泵形态跟进，并关闭 TBD-005（串口重开、不自终）

**状态：** Proposed
**日期：** 2026-08-28
**关联：** ADR-0011（TCP 跟进——本 ADR 逐条判定其 15 条决策对串口是否成立，**两条反转、四条不适用**）；ADR-0007 **D1/D2**（泵 + 双队列、不自终——本 ADR 把其适用面扩至串口）；ADR-0002 **D3′** 与 SRS **RT_LIFECYCLE_008**（**本 ADR 推翻其串口一支**）；ADR-0005 **D5**（致命错误自终——适用介质再次缩小）。
**实施范围：** `SerialTransport` 数据面与设备重开。调研见 #186。

## 背景（Context）

`UdpTransport`（ADR-0007）与 `TcpTransport`（ADR-0011）已按「socket 管理泵 + 内层数据泵 + 读写双队列」落地。串口是 ADR-0007 D6 点名的第三个介质，尚未跟进。

现状比 TCP 当初更深：`SerialTransport` 有 **13 处不可编译引用**（7 个已删符号 + 4 个已改签名的 `override`）——`OperationOptions`、`SendUnit`、`SharedCompletion`、`Clock`、`CloseDatagramQueue`、`ClosedDatagramQueue`，以及 `Read()` / `Write(SendUnit)` / `RequestClose()` / `WaitClosed(options)` / `LastSendTime()` / `LastReceiveTime()` / `SendWaiterDepth()`。TCP 当初只有一处。**实质是重写而非修补。**

`SerialConfig` 现有 `device` / `baud_rate` / `data_bits` / `stop_bits` / `parity` 五个字段，**一个时间量都没有**。

### 决定形状的两处实测（#186，PTY 环境）

**① 串口拿不到"设备已死"的信号。** 关掉 pty master 模拟拔线后：

- `readAll()` 流**完全不终止**（挂满 1500ms 报 `timed_out`），`isOpen()` 仍为 `true`；
- 成因是源码级的：`coroiodevice::readAll()` **只连 `readyRead` 与 `aboutToClose`**——没有 `errorOccurred`、没有 `destroyed`、没有 `aboutToQuit`。对照 `corosocket::readAll()` 连了五个（含 socket error 与 `disconnected`），TCP 的断链正是靠它们到达。

**② 拔线后 `errorOccurred` 风暴。** 以 **~950 次/秒**连发（1500ms 内 1416 次）；`port->close()` 实测 **0ms** 止住。

## 决策（Decision）

- **D1（关闭 TBD-005：串口**重开**、不自终；推翻 RT_LIFECYCLE_008 的串口一支）：** 串口采用与 UDP/TCP 相同的泵形态——静默超时 → `close()` → 退避 → `open()` 重试，**无限重试、不自终**，唯一退出条件是我方 `Close`。

  **这是需求变更，不是重构细节。** 现有四份登记的立场是**自终**且互相矛盾：

  | 来源 | 原立场 |
  |---|---|
  | ADR-0002 **D3′** | 自动重连是 TCP 客户端**专属**语义 |
  | SRS **RT_LIFECYCLE_008**（`:307`） | 串口致命错误时节点**自行** `Closing→Closed` |
  | SRS `:605` | 重连策略**为 TBD**，确定前不得宣称与 TCP 相同 |
  | SRS **TBD-005**（`:778`） | "过渡默认已定：致命→`Closing→Closed`" |

  **依据（技术，而非风格）：自终缺一个串口拿不到的输入。** "致命错误 → 自终"要求有一个**"致命错误"的判据**，而由背景 ① 可知，流层面**不存在**这样的信号。唯一能拿到的判据是**静默超时**——拿它去判自终，等于把"对端暂时不发数据"误判为设备死亡。**不是自终不好，是它的前提在串口上不成立。**

  > **一个曾被误当作依据的论点，此处明确否掉**：调研初稿称"保留自终会让串口成为**唯一违反** `ITransport`「仅我方 Close 才终止」契约的传输"。**该说法不成立**——RT_TRANSPORT_008 原文即"我方关闭，**或不具备重连能力的传输发生底层致命错误**"，契约本就为非重连传输开了豁免口。自终**不违反契约**，它只是**实现不了**。

  **附带收益**：背景 ② 的 `errorOccurred` 风暴由泵形态天然处置——静默超时触发 `close()`，风暴立止，退避后重开；自终形态反而要另想办法收拾它。

- **D2（形态照泵样板，沿用 ADR-0007 D1/D2）：** 外层设备管理泵 + 内层数据泵 + 读写双队列；两条队列**不随设备重开而更换**，故重开对调用方完全透明。

  ```
  外层泵（设备管理）:
    while (未被 Close) {
      if (已打开 || Open())               // Open 替代 UDP 的 Bind / TCP 的 Connect
        read_stream_ = coro(port).readAll()
        for (;;) { r = await_for(read_stream_, timeout)
                   if (!r) { 归因落 LastError; break }
                   if (r->isEmpty()) continue          // ← D5：必须跳空切片
                   push(切片) }
        read_stream_.reset()
      else
        await_for(close_signal_, timeout)
      port->close()                        // 替代 UDP 的 close() / TCP 的 abort()
    }
  ```

- **D3（唯一的时间量，沿用 ADR-0011 D5）：** `SerialConfig` **新增** `silence_timeout`（缺省 5s，**须为正**），两处共用：**读静默判链路坏** 与 **重开退避间隔**。

  **比 TCP 少一处用途**——串口的 `open()` 是**同步**的，没有 TCP `connect` 那个异步等待，故不需要"等连上"这一处。这一点上串口回到 **UDP 的形态**（UDP 的 `bind()` 同样同步）。

  **不设"0 = 禁用"这一档**（与 UDP 不同、与 TCP 同）：它同时是退避间隔，零值会退化为紧循环。

- **D4（判活：静默超时是**唯一**主动判据——**反转 ADR-0011 D4**）：** ADR-0011 为 TCP 定的是"对端断开事件为主判据、静默超时为辅"。**串口反转回 UDP 形态**：没有断开事件（背景 ①），`silence_timeout` 是"链路坏了"的**唯一**主动判据。

- **D5（读泵必须显式跳过空切片——串口独有）：** `coroiodevice::readAll()` 的 `readyRead` 处理器是 `ch->push(dev->readAll())`——**无 `bytesAvailable()` 判断、无 `isEmpty()` 守卫**；而其**初次 drain** 那一处**有** `bytesAvailable() > 0` 检查，`corosocket::readAll()` 更是两处都有 `if(!bytes.isEmpty())`。**故空切片是 `coroiodevice` 独有的结构性缺口。**

  > **依据的校准（2026-08-28，#193 实测）**：本条初稿写"实测：设备重开后读流**立刻吐一个 0 字节切片**"（源自 #186）。**#193 的实现复核未能复现**——去掉那一行 `continue` 后用例仍通过，且加计数探针跑完整个串口用例集（含拔线后）**一次空切片都没观测到**（Qt 5.15 / Linux PTY）。
  >
  > **本决策不变，但依据改为结构性的那一条**：守卫确实缺失，空推送在 `readyRead` 携零字节时必然发生；是否触发依 Qt 版本与设备驱动而异。相应地，`tests/serial_transport_test.cpp` 中该用例的定位是**契约断言**，而非已复现故障的回归——测试注释已写明。

  故串口读泵须显式跳过空切片。**UDP/TCP 都不需要这一行**——这是串口独有的一处，且是"照抄样板就会漏"的典型。

  ### ⚠ 但**不能**写成裸 `continue` —— 那会架空 D4（2026-09-02 裁决，#196）

  **D4 与 D5 是分别推导的，二者的相互作用初稿未讨论。** 裸 `continue` 会重新进入 `await_for(read_stream_, timeout)`，**静默超时的计时随之重置**。若真出现**空切片风暴**（`readyRead` 连续携零字节触发）：

  - 读泵**永远等不到静默超时** → **D4 的"唯一主动判据"失效，拔线不重开**
  - 同时它在忙循环 → **烧 CPU**

  **一条防御性的 `continue` 架空了唯一的判活判据。**

  ### 裁决：空切片**不重置**静默计时；`silence_timeout` 的语义精确为"多久没收到【非空】数据"

  ```cpp
  auto deadline = Clock::now() + timeout;
  for (;;) {
    auto remaining = deadline - Clock::now();
    if (remaining <= 0ms) { /* 判链路坏，重开 */ }
    auto chunk = await_for(read_stream_, remaining);   // ← 用【剩余】时限，不是整个 timeout
    if (!chunk) { /* 静默超时或流终止 */ }
    if (chunk->isEmpty()) continue;                    // ← 不重置 deadline
    deadline = Clock::now() + timeout;                 // ← 只有【非空】数据才续期
    ...
  }
  ```

  **这是 D4 的语义细化，不是新增旋钮**：D4 原意就是"多久没**有效数据**算链路坏"，空切片本就不是有效数据。`silence_timeout` 仍是**唯一的时间量**（**D3**），没有引入第二个。

  **否决的两条备选**：

  - **空切片计数阈值**（连续 N 次即判异常）：引入新旋钮，与 **D3**"唯一的时间量"相悖；且 N 取多少无依据。
  - **不跳过、直接判坏**（推翻 D5）：D5 的结构性依据仍在（`coroiodevice::readAll()` 的 `readyRead` 处理器确无守卫），一个空切片就重开设备是过度反应。

  **现状记明**：本环境（Qt 5.15 / Linux PTY）**未观测到空切片**——#193 加计数探针跑完整个串口用例集（含拔线后）一次都没有。故本条修的是**结构性缺口**，不是已复现的故障；实现时须在注释里如实标明这一点。

- **D6（`Close()` 四处打断，`port->close()` 是有效打断手段——**D15 反向不成立**）：** 实测：一条 fiber 挂在 `await_for(read_stream_, 3000ms)`，另一条 50ms 后 `port->close()` → **50ms 处唤醒**（走 `aboutToClose`）。

  串口**没有"连接窗口"**，故 ADR-0011 **D15**（`abort()` 在连接窗口内唤不醒任何等待、须持句柄打断）**不适用**。串口回到 UDP 的**四处**打断，不需要 TCP 的第五处（`connect_waiter_`）。

  为稳妥，`read_stream_` 仍持为成员并在 `Close()` 中 `close()`——成本一行，且与 TCP 形态一致。

- **D7（队列策略沿用，ADR-0011 D6 / SDD DD-15）：** 有界 1024 + 静默丢最旧，**不归因**。依据同样成立：串口是字节流、配对 codec 经确认即 **`SystemCodec`**，其 `ScanSystemFrames` 逐字节重同步只毁跨越丢弃点的那一帧；被毁的帧由三个 `RequestFor*` 的重发补回。`Send`（noresponse）不在第二层补救内，代价同 ADR-0011。

- **D8（写不等刷出，写泵无挂起点——ADR-0011 D13 成立）：** 实测 `QSerialPort::write(4096)` 返回 4096（**不短写**），`bytesToWrite()` 立刻查为 4096（**不同步刷出**），50ms 后归 0——与 `QTcpSocket::write()` **逐条一致**。

  故 `UdpTransport` 注释里"取到 socket 到写出之间没有挂起点，**该不变式只对 UDP 成立（串口/TCP 的写有挂起点）**"这句的**最后一个悬案关闭**：三个写泵在这一点上**结构完全同构**。（该注释已于 `1ff124c` 标注"串口未重构、仍待核"，本 ADR 落地时一并更正。）

  **一处更极端的实测**：设备消失后 `write()` **照样返回成功**、`bytesToWrite()` **永不下降**——故**写路径连判活都做不了**，这进一步坐实 D4（判活只能靠读侧静默超时）。

- **D9（`peer` 一律填固定设备端点，沿用 ADR-0011 D8）：** 串口无 peer 概念。读侧每个切片的 `peer` 填由 `device` 导出的固定端点；写侧**忽略**调用方填的 `peer`，**不判 `kInvalidArgument`**——理由同 ADR-0011 D8（让传输无关的调用方换传输即可运行）。

- **D10（`CurrentLinkState()` 不给 `kEstablishing`）：** 只有 `kDown` / `kUp`，当场由 `lifecycle_` 与 `port->isOpen()` 算出，**不设状态成员**。

  依据：与 UDP 一致——其注释即"UDP 无连接，故**永不出现** `kEstablishing`（退避是连接管理策略，不经本查询暴露）"。串口同理，退避重开期间报 `kDown` 更诚实：此刻**确实收发不了字节**。`LinkState` 的枚举注释"仅具连接管理的传输会给出 `kEstablishing`"因此无需改动。

- **D11（`errorOccurred` 的"致命"边界收窄）：** **线路噪声类错误（`ParityError` / `FramingError` / `BreakConditionError`）只落 `LastError()`，不触发设备重建**；重建**只由静默超时驱动**（D4）。

  依据：旧规则会让一段线路噪声把链路整个重建——而噪声帧本就该由 codec 的 CRC 与重同步处置（D7 的第一层补救），重建反而丢掉更多在途字节。

- **D12（配置校验，沿用 ADR-0011 D14）：** `Start()` 时一次性校验，非法返 `kConfiguration`、**停在 `Created`**。

  | 字段 | 约束 |
  |---|---|
  | `device` | 非空 |
  | `baud_rate` | 非 0 |
  | `data_bits` / `stop_bits` / `parity` | 落各自合法集 |
  | `silence_timeout` | **须为正**（同时是退避间隔，零值退化为紧循环） |

## 不适用于串口的 ADR-0011 决策（四条）

| 决策 | 何以不适用 |
|---|---|
| **D1**（三件合一） | 串口本就一件，无分层可合 |
| **D3**（一个 socket 对象、`abort()` 后复用） | 概念成立且实测通过（同对象 `close()`→`open()` 复用干净），但串口无 `abort()`、只有 `close()`；已并入本 ADR **D2** |
| **D10**（`TcpServer` 不在本轮） | 串口无服务端概念 |
| **D11**（`ApplyConfig` 待定） | 串口无热更新需求登记；**本 ADR 亦不引入** |

## 明确接受的代价

1. **队列丢弃对四种交互的后果不均等**（同 ADR-0011）：`Send`（noresponse）无重试，丢弃即永久丢失。
2. **丢弃不归因**：沿用 2026-08-28 裁决（#176/#152 关闭），SRS §3.6 已按实现校准。
3. **全部实测结论建立在 PTY 上，真实硬件未验。** 仓库历来也没有物理串口实测记录（TBD-005 的"真实硬件验收条件"一半即为此）。本 ADR **如实登记为已知局限**，不阻塞设计；真机验收另行安排。
4. **静默超时会把"对端长时间不发数据"判为链路坏并重开设备。** 这是 D4 反转的固有代价——串口既然拿不到设备信号，就只能用沉默作判据。缺省 5s 对"周期性上报"类协议足够，对"长时间静默、偶发指令"类协议须调大 `silence_timeout`，否则会周期性无谓重开。**调用方须按协议特征配置。**

## 影响（Consequences）

- **正面：** ① 三个介质形态统一，差异收敛到"`Open` 替代 `Bind`/`Connect`"、"判活判据"与"跳空切片"三点；② 恢复串口进入编译面与测试面；③ 关闭 ADR-0007 D6 遗留的最后一个介质，并结掉 `UdpTransport` 注释里最后一个悬案。
- **负面（明确接受）：** 见上四条。
- **对 ADR-0002：** **D3′**"自动重连为 TCP 客户端专属语义"**被推翻**——串口亦重开。
- **对 ADR-0005：** **D5** 自终的适用介质**再次缩小**，不再含串口（继 ADR-0007 移除 UDP 之后）。当前仅余 DDS 与"TCP 服务端已接受连接"两支，且两者均未重构。
- **对 SRS：** **TBD-005 关闭**；**RT_LIFECYCLE_008** 的串口一支改写；`:605`"重连策略为 TBD"作废；`RT_IF_SERIAL` 与 `SerialConfig` 登记同步。
- **对 ADR-0011：** 其 **D4** 与 **D15** 在串口上**反转/不适用**，但**对 TCP 本身无错误**（#186 逐条核对确认）。注意其 **D14** 映射表中"`readAll()` 已订阅 `aboutToQuit`"一行**只对 `corosocket` 成立**，`coroiodevice` 未订阅——该行是 TCP 决策，不随本 ADR 继承。

## 备选方案（Alternatives considered）

- **保留自终（现有登记立场）。** **否决理由：** 见 **D1**——它需要一个"致命错误"判据，而串口在实现层面拿不到；唯一可得的判据是静默超时，用它判自终会把正常静默误判为设备死亡。
- **订阅 `errorOccurred` 作为断链判据。** 表面上补上了缺失的信号。**否决理由：** 实测该信号在拔线后以 **~950 次/秒**风暴式连发，是**噪声而非事件**；且 `coroiodevice::readAll()` 根本没连它，要用须自行订阅并去抖——用一个需要去抖的风暴换一个已有的静默超时，不划算。噪声类错误另由 **D11** 收窄处置。
- **给串口也设 `kEstablishing`。** **否决理由：** 见 **D10**——退避重开期间确实收发不了字节，报 `kDown` 更诚实；且会牵动跨介质的枚举语义。
- **沿用 UDP 的"0 = 禁用静默判活"。** **否决理由：** 串口的 `silence_timeout` **同时是退避间隔**，零值退化为紧循环；且 D4 已定静默超时是**唯一**判据，禁用它等于放弃判活。
