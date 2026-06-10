# TCP 传输 — 实现设计 spec

> 本 spec 是主设计文档 `2026-06-09-transport-middleware-design.md` §5（TCP 传输）的**实现层细化**。主 spec 已锁定 TCP 的公共 API（`TcpClientConfig`/`TcpServerConfig`/`ITcpServer`、客户端与 accepted 连接共用 `ITransport`、广播 `Send`、framer 集成）；本 spec 锁定实现细节：Asio 选型、并发模型、类分解、重连、错误语义与测试策略，供后续 plan 直接落地。

**Goal:** 在 Foundation 层（`TransportCore` + `FrameAssembler` + `ReceiveQueue`）之上实现 TCP 客户端与服务端，基于 Standalone Asio 的异步单线程 I/O 模型，客户端支持指数退避自动重连，全部以真实回环 socket 集成测试验证。

**Tech Stack:** C++17、Standalone Asio（FetchContent 拉取，`ASIO_STANDALONE`，header-only）、GoogleTest 1.14、Google C++ 风格。

---

## 1. 范围与文件布局

本 spec 覆盖 **TCP 客户端 + 服务端**。两者共用连接级 transport `TcpConnectionImpl`，由此自然满足主 spec §5.2「客户端与 accepted 连接共用 `ITransport`」。

```
include/transport/tcp/
├── TcpClientConfig.hpp      # 主 spec §5.1（本 spec 复述）
├── TcpServerConfig.hpp      # 主 spec §5.1（本 spec 复述）
├── ITcpServer.hpp           # 主 spec §5.3（本 spec 复述）
├── TcpConnectionImpl.hpp        # 内部：已连接 socket 的收发循环（client 与 accepted 共用）
├── TcpClientImpl.hpp   # 客户端：connect + 指数退避重连
└── TcpServerImpl.hpp   # 服务端：acceptor + 每连接 TcpConnectionImpl + 广播
src/tcp/
├── TcpConnectionImpl.cpp
├── TcpClientImpl.cpp
└── TcpServerImpl.cpp
tests/tcp/
├── tcp_connection_test.cpp
├── tcp_client_test.cpp
└── tcp_server_test.cpp
```

**第三方依赖：** 在 `CMakeLists.txt` 用 `FetchContent` 拉取 Standalone Asio（`https://github.com/chriskohlhoff/asio.git`，固定 tag），定义编译宏 `ASIO_STANDALONE`，作为 header-only INTERFACE 库链接给 `transport`。Asio 内部网络头依赖 pthread（Foundation 已链 `Threads::Threads`）。

---

## 2. 配置结构（复述自主 spec §5.1）

```cpp
struct TcpClientConfig {
  std::string host;
  uint16_t    port               = 0;
  uint32_t    connect_timeout_ms = 5000;
  bool        auto_reconnect     = true;
  std::optional<LengthFieldFramerConfig> framer;  // 不设则接收为透传模式
};

struct TcpServerConfig {
  std::string bind_addr   = "0.0.0.0";
  uint16_t    port        = 0;     // 0 = 由 OS 分配临时端口（测试用）
  int         max_clients = 10;
  std::optional<LengthFieldFramerConfig> framer;  // 应用于每个 accepted 连接的接收侧
};
```

实现补充约定：
- `framer` 提供时，连接构造其对应的 `LengthFieldFramer`（构造前调用 `LengthFieldFramer::ValidateConfig`，非法则 `Open()` 返回 `config:` 错误）；不提供时 `FrameAssembler(nullptr)` 透传。
- `TcpServerConfig.port == 0` 时，`Open()` 成功后可通过 `LocalPort()`（见 §4.3）取回实际监听端口——测试据此连接。
- `max_clients` 达到上限后，新 accept 的连接立即关闭，不触发 `OnNewConnection`。

---

## 3. 类分解

> 注：本 spec 初稿按「继承 `TransportBase`」描述；接收交付基座后续重构为**组合式组件 `TransportCore`**（见主 spec §3.4 / `2026-06-10-foundation-tcp-architecture.md`）。下文已对齐 as-built：`TcpConnectionImpl` **直接实现 `ITransport`** 并**持有 `TransportCore core_`**。

### 3.1 `TcpConnectionImpl`（内部，实现 `ITransport`，组合 `TransportCore`）

包装一个**已连接**的 `asio::ip::tcp::socket`，实现 `ITransport` 的 `Open/Close/IsOpen/Send`，接收侧方法转发给 `core_`。客户端连上后、服务端 accept 后都用它。

```cpp
class TcpConnectionImpl : public ITransport {   // 直接实现 ITransport
 public:
  // socket 已连接；io 由外部 io_context 驱动（client 自有 / server 共享）
  TcpConnectionImpl(asio::ip::tcp::socket socket,
                std::shared_ptr<IFramer> framer);  // framer 可为 nullptr（透传）

  Status Open() override;     // 启动 async_read 循环（已连接，故仅启动读）
  void Close() override;      // 关闭 socket + core_.Close()
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;  // core_.EncodeForSend → strand async_write
  // 接收侧 Receive/OnReceive/AsyncReceive/SetCodec/OnDisconnect 一行转发给 core_

  std::string PeerId() const;  // "ip:port"，作为 source / client_id

 protected:
  TransportCore core_;        // 接收交付 + 编解码（子类 TcpClientImpl 也用）

 private:
  void StartRead();           // 投递一次 async_read_some
  void OnRead(error_code ec, size_t n);  // Feed 切帧 → core_.DeliverFrame；ec → 断连
  // socket_, strand_, read_buffer_, assembler_(FrameAssembler), peer_id_, open_ 标志
};
```

- **收：** `StartRead` 投递 `async_read_some` 到内部读缓冲；`OnRead` 把读到的字节交给 `FrameAssembler.Feed`，对每个切出的帧调用 `core_.DeliverFrame(frame, PeerId(), "")`（topic 为空，TCP 无 topic）。Feed 返回 `frame:` 错误（帧非法）时投递错误并断开。读到 eof/错误 → 进入断连流程。
- **发：** `Send` 先 `core_.EncodeForSend`，再 `asio::post(strand_, ...)` 经 strand 串行化 `async_write`，避免并发写交叠。返回 `Status` 表示「已入队写出」，不代表对端已收（主 spec §10）。连接已关时返回 `conn:` 错误。
- **断连：** `OnRead` 收到 eof/error → 标记关闭、`core_.Close()` 唤醒接收侧、`core_.NotifyDisconnect("conn: ...")`。`TcpConnectionImpl` 自身不重连（重连是客户端职责，见 §3.2）。

### 3.2 `TcpClientImpl`（继承 `TcpConnectionImpl`）

```cpp
class TcpClientImpl : public TcpConnectionImpl {
 public:
  explicit TcpClientImpl(TcpClientConfig config);
  ~TcpClientImpl() override;

  Status Open() override;   // connect(host:port, connect_timeout_ms) → 基类 StartRead
  void Close() override;    // 停止重连 + 关闭
};
```

- 自己拥有一个 `asio::io_context` + 一个后台 I/O 线程（构造时起，`Close()`/析构时停）。
- `Open()`：异步 `async_connect`，配 `connect_timeout_ms` 定时器；超时或失败返回 `timeout:`/`conn:` 错误。连接成功后启动基类读循环。
- **重连（`auto_reconnect == true`）：** 断线（或初次连接失败）后，以指数退避 `1s → 2s → 4s → … 封顶 30s` 重试，直到连上或 `Close()`。每次掉线触发一次 `OnDisconnect`。重连期间 `Send` 返回 `conn:` 错误。重连成功后接收侧（同一 `ReceiveQueue` / 回调）继续工作。
- `auto_reconnect == false`：断线只触发 `OnDisconnect`，不重连。

### 3.3 `TcpServerImpl`（实现 `ITcpServer`）

```cpp
class TcpServerImpl : public ITcpServer {  // ITcpServer : public ITransport
 public:
  explicit TcpServerImpl(TcpServerConfig config);
  ~TcpServerImpl() override;

  // 监听 socket 生命周期
  Status Open() override;   // bind + listen + 启动 async_accept 循环
  void Close() override;    // 停止 accept、关闭所有客户端、关监听 socket
  bool IsOpen() const override;

  Status Send(const std::vector<uint8_t>& data) override;  // 广播给所有在连客户端

  // 服务端不适用的接收方法（主 spec §5.3）
  Result<Message> Receive(uint32_t) override;          // Fail("config: 用 client_transport 接收")
  void OnReceive(ReceiveCallback) override;            // no-op
  std::future<Result<Message>> AsyncReceive() override;// 立即 Fail("config: ...")
  void SetCodec(std::shared_ptr<ICodec>) override;     // 转发给每个新连接（见下）

  // ITcpServer 扩展
  void OnNewConnection(ConnectionCallback cb) override;
  std::vector<std::shared_ptr<ITransport>> GetClients() const override;
  void DisconnectClient(const std::string& client_id) override;

  uint16_t LocalPort() const;  // 实际监听端口（port==0 时取回 OS 分配值）

 private:
  // io_context_ + 单后台线程（acceptor 与所有客户端连接共享）
  // acceptor_, clients_(map<string client_id, shared_ptr<TcpConnectionImpl>>), mutex_
  // connection_cb_, codec_（透传给新连接）, config_
};
```

- 拥有 **一个** `io_context` + 一个后台 I/O 线程，被 acceptor 和所有 accepted `TcpConnectionImpl` 共享。
- `Open()`：`bind(bind_addr:port)` + `listen` + 启动 `async_accept` 循环。bind/listen 失败返回 `conn:`/`config:` 错误。
- **accept：** 每个 accept 成功：若未超 `max_clients`，用 accepted socket + framer 造 `TcpConnectionImpl`，加入 `clients_`（键 = `PeerId()`），`SetCodec`（若 server 设过 codec），启动其读循环，回调 `OnNewConnection(client_transport)`；超限则直接关闭新 socket。
- 每个 `client_transport` 的断连（read eof）→ 从 `clients_` 移除。
- `Send`：遍历 `clients_` 快照，对每个调用其 `Send`（广播）。无客户端时成功返回（空广播）。
- `SetCodec`：记录 codec，对**之后**新建的连接生效；已存在连接不追溯改写（约定：应在 `Open()` 前或新连接到来前设好 codec）。
- `GetClients`：返回 `clients_` 的 `shared_ptr<ITransport>` 快照（加锁拷贝）。
- `DisconnectClient(id)`：按 `client_id`（"ip:port"）关闭并移除对应连接。

---

## 4. 线程模型与数据流

### 4.1 接收路径
```
io 线程: async_read_some → FrameAssembler.Feed(切帧) → 每帧 core_.DeliverFrame(payload, "ip:port", "")
                                                              ↓ (TransportCore: Decode if codec)
                                                          ReceiveQueue.Push  (线程安全)
应用线程: Receive(timeout) / OnReceive 回调 / AsyncReceive future  取出
```
- `core_.DeliverFrame` 内部按 `TransportCore` 既有逻辑：有 codec 先 `Decode`（失败投递 `codec:` 错误），无 codec 透传；填 `source="ip:port"`、`topic=""`、`timestamp`。

### 4.2 发送路径
```
应用线程: Send(data) → EncodeForSend(codec?) → asio::post(strand) → async_write
```
- 每连接一个 `strand`，串行化该连接的所有 `async_write`，避免并发/交叠写。

### 4.3 io_context 所有权
- **客户端：** 每个 `TcpClientImpl` 自有 io_context + 1 线程。
- **服务端：** `TcpServerImpl` 自有 io_context + 1 线程；acceptor 与所有 accepted `TcpConnectionImpl` 共享之（accepted 连接不再各起线程）。

---

## 5. 错误语义

沿用主 spec §3 错误前缀：

| 前缀 | TCP 场景 |
|------|----------|
| `conn:` | 连接断开、被拒绝、对端 eof、向已关闭连接 `Send` |
| `timeout:` | `connect_timeout_ms` 内未连上 |
| `frame:` | 接收侧分帧非法（帧头/帧长越界），断开连接 |
| `codec:` | 挂载的 codec `Decode`/`Encode` 失败 |
| `config:` | framer 配置非法、server 上误用接收方法、bind 参数非法 |

`Status`/`Result` 一律不抛异常（框架约定）。

---

## 6. 测试策略（真实回环 socket）

TCP 实现本质是真实 I/O，采用 **127.0.0.1 + 临时端口（`port = 0` 由 OS 分配，`LocalPort()` 取回）** 的进程内 client↔server 集成测试。用 `ReceiveQueue.Receive(timeout_ms)` 同步等待让断言确定，避免 sleep 造成 flaky。

**`tcp_connection_test`（可用一对已连接 socketpair / 本地 accept 构造两端）：**
- 单帧收发（透传模式）；
- 带 `LengthFieldFramer` 的多帧 / 跨包分帧装配正确；
- 非法帧（超 `max_frame_size`）→ 投递 `frame:` 错误并断连；
- codec（ShiftCodec）在收发两侧正确应用；
- 对端关闭 → `OnDisconnect` 触发、`Receive` 返回 `conn:`。

**`tcp_client_test`：**
- 连接成功后收发；
- connect 到无监听端口 → `timeout:`/`conn:`；
- `auto_reconnect`: 起 server → client 连上 → 关 server（触发掉线）→ 重起 server → client 在退避后重连成功并继续收发；
- `auto_reconnect == false`: 掉线只触发 `OnDisconnect`，不重连。

**`tcp_server_test`：**
- `OnNewConnection` 对每个客户端触发，拿到独立 `client_transport`；
- 每客户端独立收发（两个 client 互不串扰）；
- `Send` 广播到所有客户端；
- `GetClients` 数量正确；`DisconnectClient` 关闭指定连接（对端 `OnDisconnect`）；
- server 上 `Receive`/`AsyncReceive` 返回 `config:` 错误；
- `max_clients` 上限：第 N+1 个连接被立即关闭。

退避测试为避免等待真实 30s，plan 中将退避基数/封顶设为**可注入参数**（或测试用小基数如 10ms），保持逻辑可测且不 flaky。

---

## 7. 与 Foundation / 主 spec 的衔接

- 组合 `TransportCore`（编解码、三模式接收交付、断连通知、时间戳）、`FrameAssembler`（接收侧分帧）、`LengthFieldFramer`、`ReceiveQueue`。
- 满足主 spec §5 全部公共 API 与行为表。
- 不引入新的对外接口（除 `LocalPort()` 这一测试/运维便利方法）。

## 8. 后续（不在本 spec 范围）

UDP（§6）、串口（§7）、DDS（§8）、TransportFactory + JSON 配置（§9）各自走 spec→plan→实现循环。本 spec 完成后产出 TCP 实现 plan。
