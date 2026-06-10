# UDP 传输 — 实现设计 spec

> 本 spec 是主设计文档 `2026-06-09-transport-middleware-design.md` §6（UDP 传输）的**实现层细化**。主 spec 已锁定 UDP 的公共 API（`UdpMode`、`UdpConfig`、`IUdpTransport` + `SendTo`、Send 按 mode 的默认目的地语义）；本 spec 锁定实现细节：单类多模式结构、并发模型、按 mode 的 socket 配置、错误语义与测试策略，供后续 plan 直接落地。

**Goal:** 在 Foundation 层（`TransportCore`〔原 `TransportBase`，本 spec 重构为组合组件〕+ `ReceiveQueue`）之上实现 UDP 单播 / 组播 / 广播传输，基于 Standalone Asio 的异步单线程 I/O 模型，单个 `UdpTransport` 类按 mode 分支配置 socket，以真实回环 socket 集成测试验证（组播/广播在不支持的环境下优雅跳过）。

**Tech Stack:** C++17、Standalone Asio（已由 TCP 任务集成，`ASIO_STANDALONE`）、GoogleTest 1.14、Google C++ 风格。

**与 TCP 的关键差异：** UDP 报文天然保边界——**不需要 `IFramer`/`FrameAssembler`**，每个收到的 datagram 直接是一条 `Message`；无连接，故**无重连、无断连语义**。

---

## 1. 范围与文件布局

单个 `UdpTransport` 处理全部三种模式（单播 / 组播 / 广播），满足主 spec §6.1 的单一 `IUdpTransport`。组合 Foundation 的 `TransportCore`（原 `TransportBase`，本 spec 重构为被持有组件）。

```
include/transport/udp/
├── UdpConfig.hpp        # UdpMode + UdpConfig（复述自主 spec §6）
├── IUdpTransport.hpp    # IUdpTransport（+ SendTo，复述自主 spec §6.1）
└── UdpTransport.hpp     # 单类多模式实现
src/udp/
└── UdpTransport.cpp
tests/udp/
├── udp_interfaces_test.cpp   # 配置默认值 + IUdpTransport 抽象关系
└── udp_transport_test.cpp    # 真实回环：单播 e2e + 组播/广播（可跳过）
```

第三方依赖 Standalone Asio 已由 TCP 任务集成到 `transport` 库（`asio_standalone` INTERFACE 目标），UDP 直接复用，无需改动 CMake 的 Asio 部分；仅把新源文件加入对应目标。

---

## 2. 配置结构（复述自主 spec §6）

```cpp
enum class UdpMode { kUnicast, kMulticast, kBroadcast };

struct UdpConfig {
  UdpMode     mode            = UdpMode::kUnicast;
  std::string local_addr      = "0.0.0.0";
  uint16_t    local_port      = 0;     // 0 = 由 OS 分配临时端口（测试用）
  std::string remote_addr;             // Send() 默认目的地（单播/广播）
  uint16_t    remote_port     = 0;
  std::string multicast_group;         // 仅 kMulticast 时有效；Send() 默认目的地
  uint8_t     ttl             = 1;     // 组播 TTL（hops）
};
```

实现补充约定：
- `local_port == 0` 时，`Open()` 成功后可通过 `LocalPort()`（见 §3.3）取回实际绑定端口——测试据此互发。
- 单播/广播：`Send()` 默认目的地 = `remote_addr:remote_port`；组播：默认目的地 = `multicast_group:remote_port`。
- 广播地址由用户填入 `remote_addr`（如 `255.255.255.255` 或子网广播）。

---

## 3. `UdpTransport`（单类多模式，组合 `TransportCore`）

```cpp
class UdpTransport : public IUdpTransport,   // IUdpTransport : public ITransport
                     public std::enable_shared_from_this<UdpTransport> {
 public:
  explicit UdpTransport(UdpConfig config);
  ~UdpTransport() override;

  // 生命周期 + 发送（自有逻辑）
  Status Open() override;   // 按 mode 配置 socket，启动接收循环
  void Close() override;    // 关 socket + core_.Close() + 停 io 线程
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;             // 发往默认目的地
  Status SendTo(const std::vector<uint8_t>& data,
                const std::string& ip, uint16_t port) override;       // 发往运行期目的地

  // 接收侧：一行转发给 core_（TransportCore = 原 TransportBase 的组合化版本）
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }

  uint16_t LocalPort() const;  // 实际绑定端口

 private:
  void StartReceive();   // 投递一次 async_receive_from
  Status SendToEndpoint(const std::vector<uint8_t>& data,
                        const asio::ip::udp::endpoint& dest);

  UdpConfig config_;
  TransportCore core_;     // 编解码 + ReceiveQueue + 交付（io handler 调 core_.DeliverFrame）
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::udp::socket socket_;
  asio::ip::udp::endpoint default_dest_;     // Open() 时按 mode 解析
  asio::ip::udp::endpoint recv_from_;        // async_receive_from 填充发送方
  std::array<uint8_t, 65536> recv_buf_;      // 覆盖最大 UDP 报文
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  uint16_t local_port_ = 0;
};
```

> **设计说明（组合而非继承）：** `UdpTransport` 直接实现 `IUdpTransport`（= `ITransport` + `SendTo`），并**持有**一个 `TransportCore core_` 提供接收交付与编解码机能。接收侧五个方法一行转发给 `core_`；io handler 收到 datagram 调 `core_.DeliverFrame(...)`，发送前调 `core_.EncodeForSend(...)`。这样彻底避免「`TransportBase` 与 `IUdpTransport` 同源 `ITransport`」的菱形——无需虚继承、无多继承谜题，UDP / DDS 一视同仁。
>
> **这是本 spec 锁定的一处 Foundation 重构（UDP plan 的前置任务）：** 把 `TransportBase` 改造为不继承 `ITransport` 的组件 `TransportCore`（成员与方法语义不变，仅去掉 `: public ITransport` 与各 `override`，改作被持有组件；接收侧公共方法 `SetCodec/Receive/OnReceive/AsyncReceive/OnDisconnect` + 生产侧 protected 工具 `EncodeForSend/DeliverFrame/DeliverError/NotifyDisconnect/Close/NowMicros` 全部保留，回调类型用 `ITransport::ReceiveCallback`/`DisconnectCallback`）。同时把现有 `TcpConnectionImpl`（及经它的 `TcpClientImpl`）从「继承 `TransportBase`」改为「持有 `TransportCore` + 五行转发」。要求 Foundation + TCP 既有 57 个测试在重构后全绿，再实现 UDP。`TcpServerImpl` 不受影响（本就未用 `TransportBase`）。

### 3.1 `Open()` 按 mode 配置

公共起步：`socket_.open(udp::v4())`，失败 → `config:` 错误。随后按 mode：

- **kUnicast：**
  - `bind(local_addr:local_port)`；
  - `default_dest_ = {remote_addr, remote_port}`。

- **kBroadcast：**
  - `set_option(reuse_address(true))`；`set_option(socket_base::broadcast(true))`；
  - `bind(local_addr:local_port)`；
  - `default_dest_ = {remote_addr, remote_port}`（remote_addr 为广播地址）。

- **kMulticast：**
  - `set_option(reuse_address(true))`；
  - `bind({udp::v4(), local_port})`（绑 `0.0.0.0:local_port` 以收组播）；
  - `set_option(multicast::join_group(make_address(multicast_group)))`；
  - `set_option(multicast::hops(ttl))`；`set_option(multicast::enable_loopback(true))`（收自身，便于回环测试）；
  - `default_dest_ = {multicast_group, remote_port}`。

任何地址解析失败、bind 失败、选项/`join_group` 失败 → 返回 `config:` 错误（附 asio 错误信息），并保持未打开。成功则记录 `local_port_ = socket_.local_endpoint().port()`，`open_ = true`，`StartReceive()`。

### 3.2 接收 / 发送

- **接收循环（io 线程）：** `socket_.async_receive_from(buffer(recv_buf_), recv_from_, bind_executor(strand_, ...))`。回调：
  - 无错误：把 `recv_buf_[0..n)` 连同 `recv_from_`（`"ip:port"`）交给 `core_.DeliverFrame(datagram, source, "")`（topic 为空；core_ 有 codec 则 `Decode`，无则透传），再 `StartReceive()` 继续。
  - 错误：若 socket 已关（`operation_aborted` 或 `open_==false`）则停止；否则 `core_.DeliverError("io: ...")` 并继续 `StartReceive()`（单个报文错误不致命，如某些平台对早前 send 的 ICMP port-unreachable 会让一次 receive 返回 `connection_refused`）。

- **发送：** `Send(data)` → `SendToEndpoint(data, default_dest_)`；`SendTo(data, ip, port)` → `SendToEndpoint(data, {make_address(ip), port})`。`SendToEndpoint` 先 `core_.EncodeForSend`（core_ 有 codec 则 Encode），再经 `strand_` `async_send_to`（与 TCP 一致用 strand 串行化写）。返回 `Status` 表示「已入队写出」，不代表对端已收（主 spec §10）。未打开（`open_==false`）时返回 `config: socket not open` 错误。

### 3.3 生命周期

- `UdpTransport` 自有 `io_context` + 1 后台 io 线程（构造时起，`Close()`/析构时停）。
- `Close()`：`open_=false`；在 strand 上关 socket；`core_.Close()` 唤醒接收侧；`guard_.reset()` + `ctx_.stop()` + join io 线程。幂等。
- `LocalPort()` 返回 `local_port_`。

---

## 4. 线程模型与错误语义

- 并发与 TCP 一致：每个 `UdpTransport` 一个 `io_context` + 一个 io 线程；async 操作经 strand；handler 用 `shared_from_this` 保活；实例 `make_shared` 持有。
- 接收落入线程安全的 `ReceiveQueue`，应用线程用 `Receive/OnReceive/AsyncReceive` 取。

错误前缀（沿用主 spec §3）：

| 前缀 | UDP 场景 |
|------|----------|
| `config:` | mode/地址非法、bind 失败、`join_group`/socket 选项失败、向未打开 socket 操作 |
| `io:` | datagram 发送/接收运行期错误 |
| `codec:` | 挂载 codec 的 `Encode`/`Decode` 失败 |

UDP 无连接，故不产生 `conn:`/`timeout:`/`frame:`。

---

## 5. 测试策略（真实回环 socket + 优雅跳过）

进程内、127.0.0.1 + 临时端口（`local_port=0`，`LocalPort()` 取回）。用 `ReceiveQueue.Receive(timeout_ms)` 同步等待保证确定、不 sleep-flaky。

**`udp_interfaces_test`：**
- `UdpConfig` 默认值；
- 编译期确认 `IUdpTransport` 继承自 `ITransport`。

**`udp_transport_test`（单播——必须真实 e2e）：**
- 两个单播 `UdpTransport`（A bind 临时端口、B bind 临时端口，互设对方为 remote）；A `Send` → B `Receive` 收到，且 `Message.source` 为 A 的 `"ip:port"`；
- `SendTo(data, "127.0.0.1", B_port)` 忽略默认 remote，B 收到；
- codec（ShiftCodec）在收发两侧正确应用；无 codec 透传；
- 向未打开的 transport `Send` 返回错误。

**`udp_transport_test`（组播——支持则 e2e，否则跳过）：**
- 一个组播 `UdpTransport`（如 group `239.255.0.1`、临时 `local_port`、`enable_loopback`）`Send` → 自身 `Receive` 收到；
- 若 `Open()` 因环境不支持 join/loopback 返回 `config:` 错误，或回环投递在超时内未达 → `GTEST_SKIP() << "multicast loopback not supported in this environment"`，不判失败。

**`udp_transport_test`（广播——支持则 e2e，否则跳过）：**
- 接收端 bind `0.0.0.0:port`；发送端广播模式 `Send` 到 `127.255.255.255:port`（或子网广播）→ 接收端 `Receive` 收到；
- 若 `Open()`/`Send` 因环境不支持广播返回错误，或超时未达 → `GTEST_SKIP()`，不判失败。

> 跳过判据集中在一个小 helper：尝试 `Open()`/收发，若返回 `config:` 错误或 `Receive` 超时即 `GTEST_SKIP`。保证 CI/沙箱稳定。

---

## 6. 与 Foundation / 主 spec 的衔接

- 组合 `TransportCore`（原 `TransportBase`：编解码、三模式接收交付、时间戳）、`ReceiveQueue`。**不**使用 `IFramer`/`FrameAssembler`（UDP 报文保边界）。
- 满足主 spec §6 全部公共 API 与 Send/SendTo 语义。
- 唯一对外新增便利方法 `LocalPort()`（测试/运维）。
- 一处 Foundation 重构（UDP plan 前置任务）：`TransportBase` → 不继承 `ITransport` 的组合组件 `TransportCore`，`TcpConnectionImpl` 由继承改为持有 + 五行转发（见 §3 说明）；以「Foundation + TCP 既有 57 测试在重构后全绿」为通过门槛，再实现 UDP。`UdpTransport` 组合 `TransportCore`、直接实现 `IUdpTransport`，无菱形、无虚继承。

## 7. 后续（不在本 spec 范围）

串口（主 spec §7）、DDS（§8）、TransportFactory + JSON 配置（§9）各自走 spec→plan→实现循环。本 spec 完成后产出 UDP 实现 plan。
