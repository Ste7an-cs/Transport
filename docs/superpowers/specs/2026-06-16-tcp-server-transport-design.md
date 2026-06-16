# TCP Server 底层(接受器 + 共享 TcpConnection)— 设计 spec

> 0.2.0 重构的后续件之一:在纯字节管道架构上补齐 **TCP 服务端**。服务端不是单根字节管道,
> 故设计为**接受器**:每接受一个客户端就产出一个**纯管道 `ITransport`** 交给用户。
> 顺带把客户端与服务端共用的"已连接 socket 收发"抽成共享 `TcpConnection`,消除重复。
> 与本件并列的 **DDS 底层** 另出 spec(下一轮)。本件不触碰 `System`/codec/交互模式。

**Goal:** 提供 `TcpServerTransport`(监听 + 接受,产出 per-connection `ITransport`)与共享的 `TcpConnection`(已连接 socket 的纯字节管道),并把 `TcpClientTransport` 重构为复用 `TcpConnection`。最小 API、纯字节、无广播/客户管理(留给后续 System)。

**Tech Stack:** C++17;Standalone Asio(已 vendored);GoogleTest 1.14(已 vendored);不抛异常。

**配套:** 上游架构 spec `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`。

---

## 1. 动机

刚落地的纯字节管道 `ITransport` 覆盖 TCP 客户端 / 串口 / UDP 三种**点对点**传输。TCP 服务端不同:一个监听口产出 **N 个**客户端连接,不是单根管道。强行塞进单管道 `ITransport`(老 0.1.0 的 `ITcpServer`:`Send`=广播 + `GetClients`/`DisconnectClient`)会污染接口。本设计改为:

```
TcpServerTransport(接受器,非 ITransport)
   └─ 每接受一个客户端 → 产出一个 TcpConnection(纯管道 ITransport)→ OnAccept(conn)
TcpConnection(已连接 socket 的纯字节管道,客户端与服务端共用)
TcpClientTransport(重构:自有线程 + connect/重连,内部复用 TcpConnection)
```

`TcpConnection` 的抽出,使"已连接 socket 上的读循环 + 写队列 + 关闭"**只有一份实现**,服务端 accepted 连接与客户端连接共用。

---

## 2. 第一件:`TcpConnection`(已连接 socket 的纯管道 ITransport)

```cpp
// include/transport/tcp/TcpConnection.hpp
class TcpConnection : public ITransport,
                      public std::enable_shared_from_this<TcpConnection> {
 public:
  // 接管一个已连接的 socket;strand 从 socket 的 executor 派生(不自有 io_context/线程,
  // 由该 socket 所属 io_context 的线程驱动)。
  explicit TcpConnection(asio::ip::tcp::socket socket);
  ~TcpConnection() override;

  Status Open() override;   // 记录 peer_id,触发 OnConnect,启动 async_read_some 读循环
  void   Close() override;  // 关本连接 socket(幂等);不停任何 io_context
  bool   IsOpen() const override;

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;  // 非 kDefault → io: 错误

  void OnBytes(BytesCallback cb) override;
  void OnConnect(std::function<void()> cb) override;
  void OnDisconnect(std::function<void(const std::string&)> cb) override;

 private:
  void StartRead();
  void DoWrite();
  void EnqueueWrite(std::vector<uint8_t> bytes);
  void HandleDisconnect(const std::string& reason);
  // socket_ / strand_(从 socket executor)/ read_buf_ / write_queue_ / writing_
  // / peer_id_ / open_ / disconnected_ / 三个回调
};
```

**语义:**
- 构造即"已连接";`Open()` 记录 `peer_id_`(`socket_.remote_endpoint()` → `"ip:port"`)、若设了 `OnConnect` 则触发一次、启动读循环。
- 读到字节 → `OnBytes(Result::Success(chunk), peer_id_)`(流式,一次回调 = 一段读到的字节,**非一条消息**)。
- 读/写错误 → `HandleDisconnect(reason)`(`disconnected_.exchange(true)` 保证每连接一次):`open_=false`、关 socket、触发 `OnDisconnect("conn: ...")`。
- `Close()` 幂等(关 socket on strand);**不**拥有/停止 io_context —— 谁拥有 io_context(client 或 server)谁负责停线程。
- `Send(bytes,Endpoint)`:`kDefault` → `Send(bytes)`,否则 `Fail("io: addressed send not supported")`。
- 须以 `shared_ptr` 持有;async handler 捕获 `shared_from_this()` 保活。

> 设计取舍:`TcpConnection` 不自有 io 线程是关键——服务端的所有 accepted 连接共享 server 的单 io_context/线程,客户端连接则跑在 client 自有的 io_context 上。两种宿主都只是"提供执行器",连接逻辑一致。

---

## 3. 第二件:`TcpServerTransport`(接受器,不是 ITransport)

```cpp
// include/transport/tcp/TcpServerConfig.hpp
struct TcpServerConfig {
  std::string bind_addr = "0.0.0.0";
  uint16_t    port = 0;          // 0 = OS 分配(测试用),LocalPort() 取回
  int         backlog = asio::socket_base::max_listen_connections;
};

// include/transport/tcp/TcpServerTransport.hpp
class TcpServerTransport : public std::enable_shared_from_this<TcpServerTransport> {
 public:
  explicit TcpServerTransport(TcpServerConfig config);
  ~TcpServerTransport();

  Status Open();    // 开 acceptor + bind + listen + 启动 accept 循环;失败返回 config:/io:
  void   Close();   // 停接受 + Close 所有在册连接 + 停 ctx + join
  bool   IsOpen() const;

  void OnAccept(std::function<void(std::shared_ptr<ITransport> conn)> cb);
  void OnError(std::function<void(const std::string& reason)> cb);  // accept 级错误(非致命,继续接受)

  uint16_t LocalPort() const;   // 实际监听端口(port=0 时取回)

 private:
  void DoAccept();
  TcpServerConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<...> guard_;
  asio::strand<...> strand_;
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket peer_socket_;          // 下一个待接受 socket
  std::vector<std::weak_ptr<TcpConnection>> conns_;  // 仅供 Close 清场
  std::thread io_thread_;
  std::atomic<bool> open_{false}, closing_{false};
  uint16_t local_port_ = 0;
  std::function<void(std::shared_ptr<ITransport>)> accept_cb_;
  std::function<void(const std::string&)> error_cb_;
};
```

**接受流程(每个连接):**
```
DoAccept(): acceptor_.async_accept(peer_socket_, on strand):
  ec != 0:
    operation_aborted → 返回(正在 Close);
    其它 → error_cb_("io: accept: " + msg);DoAccept() 继续。
  ec == 0:
    auto conn = make_shared<TcpConnection>(std::move(peer_socket_));
    conns_.push_back(conn);                 // weak 登记(顺便剔除已失效项)
    if (accept_cb_) accept_cb_(conn);       // 用户在回调里【同步】设好 conn->OnBytes 等
    conn->Open();                           // 再启动读循环 → 不会漏掉早到字节
    DoAccept();                             // 接受下一个
```
> **顺序关键:** 先 `OnAccept(conn)` 后 `conn->Open()`。`accept_cb_` 在 server io 线程上同步执行,用户在其中设置 `conn` 的回调;待 `conn->Open()` 把读 post 到 strand 时,回调已就位。等价于老 `OnNewConnection` 语义。

**生命周期:**
- `Open()`:`acceptor_.open(v4)` → `set_option(reuse_address)` → `bind(bind_addr:port)` → `listen(backlog)` → 取 `local_port_` → `open_=true` → post `DoAccept()`。任一步 asio 错误 → `Fail("config:/io: ...")`。
- `Close()`(幂等,`closing_.exchange`):`open_=false`;post 关 acceptor;遍历 `conns_` 的 weak,`lock()` 成功者 `conn->Close()`;`guard_.reset()`、`ctx_.stop()`、`join()`。
- accepted 连接的写出/读取都在 server 的 strand/io 线程上跑(连接的 strand 从其 socket executor 派生,而 socket 绑定到 `ctx_`)。

**不做(最小面):** 无 `Send`/广播、无 `GetClients`、无 `DisconnectClient`。用户在 `OnAccept` 拿到 `conn` 后自行持有、收发、关闭;群发/按 id 管理等是后续 `System` 层能力。

---

## 4. 第三件:重构 `TcpClientTransport` 复用 `TcpConnection`

`TcpClientTransport` 仍是 `ITransport`,仍自有 `io_context`+线程 + `resolver` + `connect_timer_`/`reconnect_timer_` + 退避状态。变化:**读写不再内联,改为持有并委托一个 `TcpConnection`**。

```cpp
class TcpClientTransport : public ITransport, enable_shared_from_this<...> {
  // 不变:config_/backoff_*/ctx_/guard_/strand_/resolver_/两个 timer/io_thread_/
  //       closing_/open_/三个用户回调
  std::shared_ptr<TcpConnection> conn_;   // 当前连接(每次重连重建)
};
```
- **connect 成功**(`async_connect` handler):`conn_ = make_shared<TcpConnection>(std::move(socket_))`;把已存的用户回调接进去——`conn_->OnBytes(bytes_cb_)`、`conn_->OnConnect(connect_cb_)`、`conn_->OnDisconnect([self](reason){ self->OnConnLost(reason); })`;`conn_->Open()`;`open_=true`;兑现 promise。
- **`Send`**:`open_ && conn_` → `conn_->Send(...)`;否则 `Fail("config: tcp not open")`。
- **`OnConnLost(reason)`**:`open_=false`;若 `auto_reconnect && !closing_` → `ScheduleReconnect()`(退避翻倍、重建 socket_、重新 `StartConnect`);否则把断连透传给用户的 `disconnect_cb_`。(注:用户 `disconnect_cb_` 由 client 持有并在最终放弃重连时调用,使重连期间不打扰用户;与现状行为一致。)
- `OnBytes/OnConnect/OnDisconnect` setter:存入 client 的成员;下次建连时接入新 `conn_`。
- **`Close()`**:`closing_=true`、`open_=false`、post 取消 timers + 若有 `conn_` 则 `conn_->Close()`;`guard_.reset()`、`ctx_.stop()`、`join()`。

> 行为对齐现状:首次 `Open()` 同步阻塞到连成/失败/超时;指数退避自动重连;连接级错误经 `OnDisconnect`。原有两条客户端测试(回环回显、拒连失败)应**不改断言**仍通过。

---

## 5. 错误处理

- 全程 `Result`/`Status`,前缀分类不变。
- `TcpServerTransport::Open` 配置/绑定失败 → `config:`/`io:`;accept 错误 → `OnError`(非致命,继续接受;`operation_aborted` 静默)。
- `TcpConnection` 读写错误 → 该连接 `OnDisconnect("conn: ...")`。
- `TcpClientTransport` 同现状:resolve/连接失败/超时 → `conn:`/`timeout:`。

---

## 6. 线程模型

- `TcpServerTransport`:1 个 `io_context` + 1 线程;acceptor 与**所有** accepted `TcpConnection` 都在这条线程上(各连接 strand 从其 socket executor 派生,socket 绑定到 server 的 `ctx_`)。
- `TcpClientTransport`:1 个 `io_context` + 1 线程;其 `conn_` 在该线程上。
- 所有用户回调(`OnAccept`/`OnError`/连接的 `OnBytes`/`OnConnect`/`OnDisconnect`)在对应 io 线程执行,**须非阻塞**;不可在回调内 `Close()` 拥有该 io 线程的对象(自我 join 死锁)。
- `Close()` 幂等;long-lived async handler 捕获 `shared_from_this()`。

---

## 7. 测试策略

- **`TcpConnection` 单元(回环):** 测试内用 asio 在 127.0.0.1 起一个临时 acceptor,自连一条 `tcp::socket`(把服务端 accepted 的那只 `tcp::socket` move 进 `TcpConnection`,对端那只留作"裸对端"读写)。设 `OnBytes`、`Open()`,裸对端写入 → `TcpConnection` 收到字节切片;`Send` → 裸对端读到;裸对端关闭 → `OnDisconnect` 触发;`Close()` 幂等。(不用 `socketpair`——它是 `AF_UNIX` fd,无法包进 `asio::ip::tcp::socket`。)
- **`TcpServerTransport`(回环):**
  - 单客户端:server `Open()`,裸 asio client(或我们的 `TcpClientTransport`)连入,`OnAccept` 触发拿到 `conn`,`conn` 回显收到字节;断言往返。
  - 多客户端:连入 2 个,各自独立收发不串扰。
  - `Close` 清场:连入后 `server->Close()`,在册连接被关(对端读到 EOF / 连接的 `OnDisconnect` 触发)。
  - `port=0` → `LocalPort()` 取回真实端口。
- **`TcpClientTransport` 回归:** 重构后原有两条测试(`ConnectSendEchoReceive`、`ConnectRefusedFails`)断言不变仍通过;补一条**自动重连**(可选):server 断开后 client 重连成功(若实现稳定且不 flaky)。
- **解耦保持:** 不引入 codec/Message;server/connection 测试只用裸字节。

---

## 8. 文件结构

**新建:**
- `include/transport/tcp/TcpConnection.hpp` + `src/tcp/TcpConnection.cpp`
- `include/transport/tcp/TcpServerConfig.hpp`
- `include/transport/tcp/TcpServerTransport.hpp` + `src/tcp/TcpServerTransport.cpp`
- `tests/transport/tcp_connection_test.cpp`、`tests/transport/tcp_server_test.cpp`

**修改:**
- `include/transport/tcp/TcpClientTransport.hpp` + `src/tcp/TcpClientTransport.cpp`(改为复用 `TcpConnection`)
- `CMakeLists.txt`(加 `TcpConnection.cpp`、`TcpServerTransport.cpp` 到库;加两个测试)

---

## 9. 不做什么(YAGNI / 范围外)

- **不做** 广播 `Send`、`GetClients`、`DisconnectClient`(留给后续 `System`/用户自管)。
- **不做** 任何 codec/Message/交互模式接入(纯字节;用户在 `conn` 上自行组合 `ICodec`)。
- **不做** DDS(并列的下一轮 spec)。
- **不做** TLS、连接限流/超时回收、IPv6 专项(按需后续)。
- **不引入** 新第三方依赖。

---

## 10. 命名备注
- `TcpConnection` 是纯管道 `ITransport`(已连接 socket);客户端/服务端共用。
- `TcpServerTransport` 名字含 "Transport" 但**不实现** `ITransport`——它是产出 transport 的接受器;若觉名实不符,实现期可改 `TcpAcceptor`/`TcpServer`。
