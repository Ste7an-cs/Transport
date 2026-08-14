# UdpTransport 泵形态设计（ADR-0007 样板实现）

**日期：** 2026-08-12
**范围：** 仅 `UdpTransport`。node 侧**不动**（#154 已完成接口适配，形态保持一条读循环）。TCP/串口按同一形态后续跟进。
**依据：** ADR-0007 D1/D2/D3/D5；SRS `RT_IF_UDP`、`RT_TRANSPORT_010`、`RT_TCP_RECONNECT_003`、`RT_LIFECYCLE_008`；SDD §4.2.11 图 4-13、§4.3.5、§5.6。
**对应 issue：** #155（本设计）、#156（静默超时）、#157（UDP 不自终在 node 侧的落地）。

---

## 1. 结构

三条内部工作单元，两条跨 socket 重建而存活的队列：

| 单元 | 职责 |
|---|---|
| **泵 fiber** | 外层：管 socket 的创建/重建/重试；内层：把收到的报文投入 `read_queue`。退出后兼任收敛者 |
| **写泵 fiber** | 从 `write_queue` 取出并写 socket |
| `read_queue` / `write_queue` | 对外数据面。**不随 socket 重建而更换** —— 这正是"重建对调用方透明"的载体 |

`Read()` 交出 `read_queue` 句柄；`Write()` 投入 `write_queue` 即返。

---

## 2. 两个由实测确立的约束

这两条都推翻了先前"想当然"的设计，必须写死在实现里。

### 2.1 socket 对象整个生命期复用一个

`QUdpSocket` 支持 `bind → close → 再 bind`，**且 bind 失败过的对象仍可复用**（实测：占用端口时 `bind=0`，释放后同一对象 `bind=1`，`state=BoundState`）。

因此 socket 在 `Start()` 里创建一次、析构时销毁。**不需要** `QPointer` 置空/复活、`deleteLater()` 时序、每代 socket 的 `socket_ready` 换代——写泵持有的指针在整个生命期稳定，判据只剩 `state() == BoundState`。

### 2.2 未 bind 上时**不得**建读流

`CoroUdpSocket::receiveDatagram()` 建流时有一个立即状态检查：

```cpp
if (current->state() == QAbstractSocket::UnconnectedState) {
    channel->close();            // 当场关闭，错误为 no_message
    scope->disconnectAll();
    return;
}
```

上游有意为之：不给死 socket 建一条假装活着的流。

**实测**：未 bind 的 socket 上 `await_for(stream, 300ms)` **0 ms 返回** `no_message`，而非超时。

**后果**：不能用"读超时"当作 bind 失败后的重试间隔——那会让外层循环变成**不带间隔的紧转**。未 bind 的退避必须用独立的延时原语。

---

## 3. 状态

```cpp
struct State {
  mutable std::mutex mutex;

  UdpConfig config;                 // 启动后不变，不加锁（#156 起含 silence_timeout）
  QUdpSocket* socket;               // 整个生命期一个；Start 建、析构销；设一次后只读

  LifecycleState lifecycle;         // ✓ Created/Running/Closing/Closed
  SharedCompletion<void> closed;    // WaitClosed 多等待者（自守其锁）

  std::shared_ptr<Awaitable<Datagram>> read_queue;    // 跨重建存活
  std::shared_ptr<Awaitable<SendUnit>> write_queue;   // 跨重建存活
  std::shared_ptr<Awaitable<void>>     socket_ready;  // 写泵等就绪；复用一个
  std::shared_ptr<Awaitable<void>>     close_signal;  // 只为打断"未 bind 时的退避"
  std::shared_ptr<FiberTask<void>>     write_pump;    // ✓ 收敛时 join

  std::optional<Clock::time_point> last_send;   // ✓ ITransport 强制观测面
  std::optional<Clock::time_point> last_recv;   // ✓
  std::error_code                  last_error;  // ✓
  std::uint16_t                    local_port;  // ✓ 实际绑定端口
};
```

`✓` = 需 `mutex` 保护。四条 `shared_ptr` 本身只在构造时赋值一次，读取不用锁；其指向的对象自守其锁。

**刻意不设的两项：**

- ~~`bool closing`~~ —— 冗余，`lifecycle >= Closing` 即是。避免两个真值来源不同步。
- ~~当前读流句柄~~ —— 不必存。`Close` 关 socket 即可打断活跃流（**实测验证**：泵在无超时 `await` 上被 `sock.close()` 唤醒，错误 `no_message`）。

---

## 4. 泵 fiber

```cpp
void SocketPump() {
  while (true) {
    { lock; if (lifecycle >= Closing) break; }

    if (socket->state() != BoundState) {
      socket->close();                       // 幂等，重置到可 bind 状态
      if (!socket->bind(config.local_addr, config.local_port, ...)) {
        { lock; last_error = MapSocketError(...); }
        await_for(close_signal, kBindRetryInterval);   // 内部常量 3s（非配置项），Close 可提前打断
        continue;                            // 无限重试，不自终（ADR-0007 D2）
      }
      { lock; local_port = socket->localPort(); }
      socket_ready->resolve();               // 唤醒写泵
    }

    auto s = Coro::coro(socket).receiveDatagram();     // 每代重建（旧流已随 close 死掉）
    const auto wait = config.silence_timeout;          // #156；0 = 禁用

    while (true) {
      auto r = (wait.count() == 0) ? Coro::await(s) : Coro::await_for(s, wait);
      if (!r) break;                         // 静默超时 / 流终止 / 被 Close 打断 → 回外层
      { lock; last_recv = Clock::now(); }
      read_queue->resolve(ToDatagram(*r));
    }
  }

  // 收敛
  write_pump->get();                          // 先 join 写泵，确保它不再碰 socket
  CloseDatagramQueue(read_queue, kClosed);
  { lock; lifecycle = Closed; }
  closed.Complete(Status{});
}
```

---

## 5. 写泵 fiber

两个阻塞点，**串行**（不需要多路等待，AsyncTask 也没有 select）：

```cpp
void WritePump() {
  while (true) {
    auto item = Coro::await(write_queue);     // ── 阻塞点①：等数据 ──
    if (!item) break;                         // 队列被 Close 关闭 → 退出

    for (;;) {                                // ── 阻塞点②：等 socket 就绪 ──
      { lock; if (lifecycle >= Closing) return; }
      if (socket->state() == BoundState) {
        // 不变式：状态检查到写出之间【无挂起点】
        qint64 n = socket->writeDatagram(item->bytes, addr, port);
        { lock; (n < 0) ? last_error = ... : last_send = Clock::now(); }
        // 失败不回传调用方（fire-and-forget），只落 last_error
        break;
      }
      socket_ready->channel()->discard_pending();   // 清历史 resolve 的陈旧事件
      Coro::await(socket_ready);
    }
  }
}
```

---

## 6. 对外接口

```cpp
Status Start() {
  { lock; if (lifecycle != Created) return kInvalidState; lifecycle = Running; }
  socket = new QUdpSocket();
  socket->setProxy(QNetworkProxy::NoProxy);   // #123：不继承环境代理
  BindOnce();                                 // ★ 就地尝试一次，忽略结果
  write_pump = spawn(WritePump);
  spawn(SocketPump);
  return Status{};                            // 首次 bind 未成不算启动失败
}

std::shared_ptr<Awaitable<Datagram>> Read() {
  lock;
  if (lifecycle == Created) return ClosedDatagramQueue(kInvalidState);
  return read_queue;                          // 是否 shared() 由调用方决定
}

Status Write(SendUnit unit) {
  { lock; if (lifecycle != Running) return kClosed; }
  write_queue->resolve(std::move(unit));
  return Status{};                            // 仅表示"已入队"
}

Status RequestClose() {
  { lock; if (lifecycle >= Closing) return Status{}; lifecycle = Closing; }
  close_signal->close(kClosed);               // 打断未 bind 时的退避
  socket->close();                            // 打断活跃读流（实测有效）
  write_queue->close(kClosed);                // 唤醒写泵阻塞点①
  socket_ready->close(kClosed);               // 唤醒写泵阻塞点②
  return Status{};                            // 只发信号，不等收敛
}

Status WaitClosed(OperationOptions o) { return closed.Wait(o); }
```

**四处打断缺一不可** —— 泵可能停在退避或读等待，写泵可能停在两个阻塞点之一。漏一处即一次收敛挂死。

**为什么 `Start()` 要就地 bind 一次（修正，2026-08-14）**：本文初稿写的是"起泵后即返回"，不做就地 bind。**该写法不可行** —— `makeTask` 起的 fiber 要等调用方让出才运行，故 `Start()` 返回时 `LocalPort()` 恒为 0、`CurrentLinkState()` 恒 `kDown`，而既有测试中有 **26 处**在 `Start()` 后立刻取 `LocalPort()` 或断言 `kUp`。就地 bind **不改变任何语义承诺**：失败仍不返错、仍由泵无限重试；泵首轮见 `BoundState` 即跳过重 bind 直接进读循环，不会重复 bind。

---

## 7. 三条不变式

1. **同线程协作**：`makeTask` 默认 `Affinity::fixed(当前线程)`，泵与写泵不并行、只在 await 点交错 → 不需要为"同时读写一个 socket"加保护。
2. **状态检查 → 写出之间无挂起点**：`writeDatagram` 同步，故泵不可能在中途重建 socket。
   ⚠️ **只对 UDP 成立**。串口的 `Write` 有一个挂起点（等 `bytesWritten`），TCP 同理。跟进本形态时写泵必须改用代际号校验。
3. **两条队列跨 socket 重建而存活**：重建只换 socket，不换队列。

---

## 8. 测试

1. bind 失败（端口被占）→ 3s 后重试 → 释放端口后自动 bind 成功并继续收数
2. 首次 bind 失败时 `Start()` 仍返成功
3. 链路不可用时 `Write()` 入队不拒绝；恢复后按序全部发出
   （注：`Close` 后 `Write()` 返 `kClosed`；已入队但未发出的报文随 `write_queue` 关闭而丢弃）
4. `Close()` 在四种停留位置各自都能干净收敛（退避中 / 读等待中 / 写泵阻塞点① / 写泵阻塞点②），无残留 fiber，`WaitClosed` 正常返回
5. 既有 UDP 用例按新语义调整（凡靠"bind 失败使 `Start()` 返错"构造的都要改）

---

## 9. 本轮不处置

- **队列容量与丢弃归因（TBD-009 / #152）**：沿用 AsyncTask 默认「有界 1024 + 静默丢最旧」。丢弃**无归因**，冲击 §3.6 的 loss=0 等式。**不要自行决定容量策略，也不要在别处加补偿逻辑。**
- **静默超时**（#156）、**UDP 不自终在 node 侧的落地**（#157）。
- **`LastSendTime` / `LastReceiveTime`**：曾议删除（生产代码零消费，仅 4 个测试文件约 15 处断言在读），**本轮保留**。
- **写侧 Trace（修正，2026-08-14）**：本文初稿的写泵伪代码含 `RecordEvent(send / send_error)`，**无法实现** —— `UdpConfig` 没有 `trace_sink` 字段（全仓只有 `TcpClientConfig` 与 `DdsTransport` 有）。加该字段属配置面变更且需同步 SDD，本轮不做；写侧失败目前只落 `LastError()`。
- **临时端口漂移**：`local_port` 配 0 时每次重 bind 会拿到**不同**端口（实测 43752 → 43724）。对端若记着源端口会失联。是否需要"重 bind 时沿用上次端口"未定，本轮不处理。
