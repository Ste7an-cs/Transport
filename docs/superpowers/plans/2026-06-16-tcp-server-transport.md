# TCP Server 底层(接受器 + 共享 TcpConnection)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在纯字节管道架构上补 TCP 服务端:抽出共享 `TcpConnection`(已连接 socket 的纯管道 `ITransport`),实现 `TcpServerTransport`(监听+接受,产出 per-connection `ITransport`),并把 `TcpClientTransport` 重构为复用 `TcpConnection`。

**Architecture:** `TcpConnection` 不自有 io 线程(strand 从传入 socket 的 executor 派生),由宿主 io_context 驱动;客户端与服务端 accepted 连接共用它。`TcpServerTransport` 不实现 `ITransport`——它是自有 io_context+线程的接受器,每 accept 一个 socket 造一个 `TcpConnection`,先 `OnAccept(conn)`(用户在回调里同步设好 conn 回调)再 `conn->Open()`。最小 API:无广播/客户管理。

**Tech Stack:** C++17;Standalone Asio(已 vendored,`third_party/asio`,`ASIO_STANDALONE`);GoogleTest 1.14(已 vendored);不抛异常(`Result<T>`/`Status`)。

**配套 spec:** `docs/superpowers/specs/2026-06-16-tcp-server-transport-design.md`

---

## 文件结构

**新建:**
- `include/transport/tcp/TcpConnection.hpp` + `src/tcp/TcpConnection.cpp` — 已连接 socket 的纯管道 ITransport(无 connect/重连/线程)。
- `include/transport/tcp/TcpServerConfig.hpp` — 服务端配置。
- `include/transport/tcp/TcpServerTransport.hpp` + `src/tcp/TcpServerTransport.cpp` — 接受器。
- `tests/transport/tcp_connection_test.cpp`、`tests/transport/tcp_server_test.cpp`。

**修改:**
- `include/transport/tcp/TcpClientTransport.hpp` + `src/tcp/TcpClientTransport.cpp` — 改为复用 `TcpConnection`。
- `CMakeLists.txt` — 加两个新源到库、两个新测试到 `transport_tests`。

**不动:** `ITransport.hpp`、`UdpTransport`、`SerialTransport`、所有 codec、`TcpClientConfig.hpp`。

---

## Task 1: `TcpConnection`(已连接 socket 的纯管道 ITransport)

**Files:**
- Create: `include/transport/tcp/TcpConnection.hpp`、`src/tcp/TcpConnection.cpp`
- Test: `tests/transport/tcp_connection_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

新建 `tests/transport/tcp_connection_test.cpp`:

```cpp
#include "transport/tcp/TcpConnection.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpConnection;

namespace {
// 在 127.0.0.1 起临时 acceptor,造一对已连接 socket:
// accepted 绑定到 ctx(给 TcpConnection 用),peer 绑定到 pctx(裸对端,测试直接读写)。
struct ConnectedPair {
  asio::ip::tcp::socket accepted;
  asio::ip::tcp::socket peer;
};
ConnectedPair MakeConnectedPair(asio::io_context& ctx, asio::io_context& pctx) {
  asio::ip::tcp::acceptor acc(
      ctx, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
  const uint16_t port = acc.local_endpoint().port();
  asio::ip::tcp::socket peer(pctx);
  std::thread ct([&] {
    peer.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
  });
  asio::ip::tcp::socket accepted = acc.accept();  // 阻塞接受(测试线程)
  ct.join();
  return {std::move(accepted), std::move(peer)};
}

struct Sink {
  std::mutex m; std::condition_variable cv; std::vector<uint8_t> acc; std::string from;
  void Wire(std::shared_ptr<TcpConnection> c) {
    c->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string& f) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      acc.insert(acc.end(), r.value.begin(), r.value.end()); from = f;
      cv.notify_all();
    });
  }
  bool WaitBytes(std::size_t n, int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [&] { return acc.size() >= n; });
  }
};
}  // namespace

TEST(TcpConnection, ReceivesBytesFromPeer) {
  asio::io_context ctx, pctx;
  auto pair = MakeConnectedPair(ctx, pctx);
  auto conn = std::make_shared<TcpConnection>(std::move(pair.accepted));
  Sink sink; sink.Wire(conn);
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  auto wg = asio::make_work_guard(ctx);
  std::thread io([&] { ctx.run(); });

  const uint8_t out[] = {1, 2, 3};
  asio::write(pair.peer, asio::buffer(out, 3));
  ASSERT_TRUE(sink.WaitBytes(3, 1000));
  EXPECT_EQ(sink.acc, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_NE(sink.from.find("127.0.0.1:"), std::string::npos);

  conn->Close();
  wg.reset(); ctx.stop(); io.join();
  pair.peer.close();
}

TEST(TcpConnection, SendWritesToPeer) {
  asio::io_context ctx, pctx;
  auto pair = MakeConnectedPair(ctx, pctx);
  auto conn = std::make_shared<TcpConnection>(std::move(pair.accepted));
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  auto wg = asio::make_work_guard(ctx);
  std::thread io([&] { ctx.run(); });

  ASSERT_TRUE(static_cast<bool>(conn->Send({7, 8, 9, 10})));
  std::vector<uint8_t> got(4);
  asio::read(pair.peer, asio::buffer(got));
  EXPECT_EQ(got, (std::vector<uint8_t>{7, 8, 9, 10}));

  conn->Close();
  wg.reset(); ctx.stop(); io.join();
  pair.peer.close();
}

TEST(TcpConnection, PeerCloseTriggersDisconnect) {
  asio::io_context ctx, pctx;
  auto pair = MakeConnectedPair(ctx, pctx);
  auto conn = std::make_shared<TcpConnection>(std::move(pair.accepted));
  std::mutex m; std::condition_variable cv; std::string reason;
  conn->OnDisconnect([&](const std::string& r) {
    std::lock_guard<std::mutex> lk(m); reason = r; cv.notify_all();
  });
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  auto wg = asio::make_work_guard(ctx);
  std::thread io([&] { ctx.run(); });

  pair.peer.close();  // 对端关闭 → conn 读到 EOF
  {
    std::unique_lock<std::mutex> lk(m);
    ASSERT_TRUE(cv.wait_for(lk, std::chrono::milliseconds(1000), [&] { return !reason.empty(); }));
  }
  EXPECT_EQ(reason.rfind("conn:", 0), 0u);

  conn->Close();
  wg.reset(); ctx.stop(); io.join();
}
```

把 `tests/transport/tcp_connection_test.cpp` 加入 `CMakeLists.txt` 的 `add_executable(transport_tests ...)`。

- [ ] **Step 2: 运行,确认失败**

Run: `cd /home/ubuntu/david/transport && cmake --build build -j$(nproc) 2>&1 | head -20`（若 `build/` 不存在先 `cmake -S . -B build >/dev/null`）
Expected: 找不到 `transport/tcp/TcpConnection.hpp`。

- [ ] **Step 3: 写 `include/transport/tcp/TcpConnection.hpp`**

```cpp
#pragma once

// TcpConnection.hpp — 已连接 socket 的纯字节管道 ITransport(客户端/服务端共用)。
// 不自有 io_context/线程:strand 从传入 socket 的 executor 派生,由该 socket 所属
// io_context 的线程驱动。读循环把字节切片经 OnBytes 交付;无 connect/重连。
// 须以 shared_ptr 持有(async handler 捕获 shared_from_this 保活)。

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

namespace transport {

class TcpConnection : public ITransport,
                      public std::enable_shared_from_this<TcpConnection> {
 public:
  explicit TcpConnection(asio::ip::tcp::socket socket);
  ~TcpConnection() override;

  Status Open() override;   // 记录 peer_id,触发 OnConnect,启动读循环
  void   Close() override;  // 关本连接 socket(幂等,不触发 OnDisconnect);不停 io_context
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

  const std::string& PeerId() const { return peer_id_; }

 private:
  void StartRead();
  void DoWrite();
  void EnqueueWrite(std::vector<uint8_t> bytes);
  void HandleDisconnect(const std::string& reason);

  asio::ip::tcp::socket socket_;
  asio::strand<asio::any_io_executor> strand_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::string peer_id_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> disconnected_{false};

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 4: 写 `src/tcp/TcpConnection.cpp`**

```cpp
#include "transport/tcp/TcpConnection.hpp"

#include <utility>
#include <variant>

// TcpConnection.cpp — 见 .hpp。读到的字节切片经 OnBytes 直接交付(无分帧)。
// Close 与 HandleDisconnect:operation_aborted(主动关闭引起)在读写 handler 里被跳过,
// 故 Close 不会触发 OnDisconnect;真实对端断开(eof/reset)才经 HandleDisconnect 上报一次。

namespace transport {

TcpConnection::TcpConnection(asio::ip::tcp::socket socket)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())) {}

TcpConnection::~TcpConnection() {
  asio::error_code ig;
  socket_.close(ig);  // 析构时已无 pending async op(否则 self 会保活);直接关 fd。
}

Status TcpConnection::Open() {
  asio::error_code ec;
  auto ep = socket_.remote_endpoint(ec);
  if (!ec) peer_id_ = ep.address().to_string() + ":" + std::to_string(ep.port());
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartRead();
  });
  return Status::Success(std::monostate{});
}

void TcpConnection::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              if (ec == asio::error::operation_aborted) return;  // 主动关闭,不上报
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            if (bytes_cb_) {
              std::vector<uint8_t> chunk(read_buf_.begin(), read_buf_.begin() + n);
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(chunk)), peer_id_);
            }
            StartRead();
          }));
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: connection not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpConnection::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void TcpConnection::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              if (ec == asio::error::operation_aborted) return;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

void TcpConnection::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
}

void TcpConnection::Close() {
  if (closing_.exchange(true)) return;
  disconnected_.store(true);  // 抑制后续 abort 触发 OnDisconnect
  open_.store(false);
  auto self = shared_from_this();   // 调用方持有 shared_ptr,析构期不会走到这里
  asio::post(strand_, [this, self]() {
    asio::error_code ig;
    socket_.close(ig);  // 取消 pending async_read/write(→ operation_aborted,被跳过)
  });
}

}  // namespace transport
```

把 `src/tcp/TcpConnection.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 5: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R TcpConnection 2>&1 | grep -iE "passed|failed"`
Expected: 3 个 `TcpConnection.*` 全通过。

- [ ] **Step 6: 提交**

```bash
git add include/transport/tcp/TcpConnection.hpp src/tcp/TcpConnection.cpp tests/transport/tcp_connection_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TcpConnection(已连接 socket 纯字节管道,客户端/服务端共用)"
```

---

## Task 2: `TcpServerTransport` + `TcpServerConfig`(接受器)

**Files:**
- Create: `include/transport/tcp/TcpServerConfig.hpp`、`include/transport/tcp/TcpServerTransport.hpp`、`src/tcp/TcpServerTransport.cpp`
- Test: `tests/transport/tcp_server_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写 `include/transport/tcp/TcpServerConfig.hpp`**

```cpp
#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct TcpServerConfig {
  std::string bind_addr = "0.0.0.0";
  uint16_t    port = 0;     // 0 = OS 分配(LocalPort() 取回)
  int         backlog = 0;  // <=0 → 用 asio 默认 max_listen_connections
};

}  // namespace transport
```

- [ ] **Step 2: 写失败测试**

新建 `tests/transport/tcp_server_test.cpp`:

```cpp
#include "transport/tcp/TcpServerTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::ITransport;
using transport::Result;
using transport::TcpServerConfig;
using transport::TcpServerTransport;

namespace {
// 裸 asio 客户端:连接 + 阻塞收发(独立于本库的 TcpClientTransport)。
struct RawClient {
  asio::io_context ctx;
  asio::ip::tcp::socket sock{ctx};
  void Connect(uint16_t port) {
    sock.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
  }
  void Write(const std::vector<uint8_t>& d) { asio::write(sock, asio::buffer(d)); }
  std::vector<uint8_t> Read(std::size_t n) {
    std::vector<uint8_t> b(n); asio::read(sock, asio::buffer(b)); return b;
  }
};

// 在 OnAccept 回调里把连接接成 echo:收到字节原样回发。须在回调内【同步】设好。
void WireEcho(std::shared_ptr<ITransport> conn) {
  auto weak = std::weak_ptr<ITransport>(conn);
  conn->OnBytes([weak](Result<std::vector<uint8_t>> r, const std::string&) {
    if (!r) return;
    if (auto c = weak.lock()) c->Send(r.value);
  });
}
}  // namespace

TEST(TcpServerTransport, SingleClientEcho) {
  auto srv = std::make_shared<TcpServerTransport>(TcpServerConfig{"127.0.0.1", 0, 0});
  srv->OnAccept([](std::shared_ptr<ITransport> conn) { WireEcho(conn); });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  const uint16_t port = srv->LocalPort();
  ASSERT_GT(port, 0);

  RawClient cli; cli.Connect(port);
  cli.Write({10, 20, 30});
  EXPECT_EQ(cli.Read(3), (std::vector<uint8_t>{10, 20, 30}));

  cli.sock.close();
  srv->Close();
}

TEST(TcpServerTransport, TwoClientsIndependent) {
  auto srv = std::make_shared<TcpServerTransport>(TcpServerConfig{"127.0.0.1", 0, 0});
  srv->OnAccept([](std::shared_ptr<ITransport> conn) { WireEcho(conn); });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  const uint16_t port = srv->LocalPort();

  RawClient a, b; a.Connect(port); b.Connect(port);
  a.Write({1}); b.Write({2});
  EXPECT_EQ(a.Read(1), (std::vector<uint8_t>{1}));
  EXPECT_EQ(b.Read(1), (std::vector<uint8_t>{2}));

  a.sock.close(); b.sock.close();
  srv->Close();
}

TEST(TcpServerTransport, CloseTearsDownConnections) {
  auto srv = std::make_shared<TcpServerTransport>(TcpServerConfig{"127.0.0.1", 0, 0});
  srv->OnAccept([](std::shared_ptr<ITransport> conn) { WireEcho(conn); });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  const uint16_t port = srv->LocalPort();

  RawClient cli; cli.Connect(port);
  cli.Write({5});
  EXPECT_EQ(cli.Read(1), (std::vector<uint8_t>{5}));  // 确认已接入

  srv->Close();  // 关服务端 → 关所有在册连接

  // 对端再读应得 EOF(0 字节 / eof 错误)
  asio::error_code ec;
  uint8_t tmp[1];
  std::size_t n = asio::read(cli.sock, asio::buffer(tmp, 1), ec);
  EXPECT_EQ(n, 0u);
  EXPECT_TRUE(ec == asio::error::eof || ec == asio::error::connection_reset || (bool)ec);
}
```

把 `tests/transport/tcp_server_test.cpp` 加入 `CMakeLists.txt` 的 `add_executable(transport_tests ...)`。

- [ ] **Step 3: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 找不到 `transport/tcp/TcpServerTransport.hpp`。

- [ ] **Step 4: 写 `include/transport/tcp/TcpServerTransport.hpp`**

```cpp
#pragma once

// TcpServerTransport.hpp — TCP 接受器(不是 ITransport)。自有 io_context + 1 线程:
// 监听 + 接受;每 accept 一个 socket 造一个 TcpConnection,先 OnAccept(conn)(用户在回调里
// 同步设好 conn 回调)再 conn->Open()。最小 API:无广播/GetClients/DisconnectClient。
// 须以 shared_ptr 持有。

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpConnection.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

namespace transport {

class TcpServerTransport : public std::enable_shared_from_this<TcpServerTransport> {
 public:
  explicit TcpServerTransport(TcpServerConfig config);
  ~TcpServerTransport();

  Status Open();    // 开 acceptor + bind + listen + 启动 accept 循环
  void   Close();   // 停接受 + 关所有在册连接 + 排空 io 线程
  bool   IsOpen() const { return open_.load(); }

  void OnAccept(std::function<void(std::shared_ptr<ITransport>)> cb) {
    accept_cb_ = std::move(cb);
  }
  void OnError(std::function<void(const std::string&)> cb) { error_cb_ = std::move(cb); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void DoAccept();

  TcpServerConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::tcp::acceptor acceptor_;
  asio::ip::tcp::socket peer_socket_;
  std::vector<std::weak_ptr<TcpConnection>> conns_;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;

  std::function<void(std::shared_ptr<ITransport>)> accept_cb_;
  std::function<void(const std::string&)> error_cb_;
};

}  // namespace transport
```

- [ ] **Step 5: 写 `src/tcp/TcpServerTransport.cpp`**

```cpp
#include "transport/tcp/TcpServerTransport.hpp"

#include <algorithm>
#include <utility>
#include <variant>

// TcpServerTransport.cpp — 见 .hpp。
// Close:在 strand 上关 acceptor + 逐个 Close 在册连接(关其 socket),再释放 work guard,
// 不调 ctx_.stop() —— 让 io 线程把"关连接 → 读被 abort"的 pending 处理排空后自然返回,
// 保证对端确实看到连接关闭(EOF),再 join。

namespace transport {

TcpServerTransport::TcpServerTransport(TcpServerConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      acceptor_(ctx_),
      peer_socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpServerTransport::~TcpServerTransport() { Close(); }

Status TcpServerTransport::Open() {
  asio::error_code ec;
  auto addr = asio::ip::make_address(config_.bind_addr, ec);
  if (ec) return Status::Fail("config: invalid bind_addr");
  asio::ip::tcp::endpoint ep(addr, config_.port);

  acceptor_.open(ep.protocol(), ec);
  if (ec) return Status::Fail("io: acceptor open: " + ec.message());
  acceptor_.set_option(asio::socket_base::reuse_address(true), ec);
  acceptor_.bind(ep, ec);
  if (ec) return Status::Fail("config: bind: " + ec.message());
  const int backlog = config_.backlog > 0
                          ? config_.backlog
                          : asio::socket_base::max_listen_connections;
  acceptor_.listen(backlog, ec);
  if (ec) return Status::Fail("io: listen: " + ec.message());

  asio::error_code lec;
  auto le = acceptor_.local_endpoint(lec);
  if (lec) return Status::Fail("io: local_endpoint: " + lec.message());
  local_port_ = le.port();
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { DoAccept(); });
  return Status::Success(std::monostate{});
}

void TcpServerTransport::DoAccept() {
  auto self = shared_from_this();
  acceptor_.async_accept(
      peer_socket_,
      asio::bind_executor(strand_, [this, self](asio::error_code ec) {
        if (ec) {
          if (ec == asio::error::operation_aborted) return;  // 正在 Close
          if (error_cb_) error_cb_("io: accept: " + ec.message());
          if (open_.load()) DoAccept();
          return;
        }
        auto conn = std::make_shared<TcpConnection>(std::move(peer_socket_));
        peer_socket_ = asio::ip::tcp::socket(ctx_);  // 复位待下次 accept
        conns_.erase(
            std::remove_if(conns_.begin(), conns_.end(),
                           [](const std::weak_ptr<TcpConnection>& w) { return w.expired(); }),
            conns_.end());
        conns_.push_back(conn);
        if (accept_cb_) accept_cb_(conn);  // 用户在此【同步】设好 conn 回调
        conn->Open();                      // 再启动读循环 → 不漏早到字节
        DoAccept();
      }));
}

void TcpServerTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    acceptor_.close(ig);
    for (auto& w : conns_)
      if (auto c = w.lock()) c->Close();
    conns_.clear();
  });
  guard_.reset();  // 不 stop():让 io 线程排空(关连接 → 读 abort)后自然返回
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

把 `src/tcp/TcpServerTransport.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 6: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R TcpServerTransport 2>&1 | grep -iE "passed|failed"`
Expected: 3 个 `TcpServerTransport.*` 全通过。

- [ ] **Step 7: 提交**

```bash
git add include/transport/tcp/TcpServerConfig.hpp include/transport/tcp/TcpServerTransport.hpp src/tcp/TcpServerTransport.cpp tests/transport/tcp_server_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TcpServerTransport(接受器,产出 per-connection ITransport,Close 清场)"
```

---

## Task 3: 重构 `TcpClientTransport` 复用 `TcpConnection`

**Files:**
- Modify: `include/transport/tcp/TcpClientTransport.hpp`、`src/tcp/TcpClientTransport.cpp`
- Test: `tests/transport/tcp_transport_test.cpp`(**已存在,不改**;作回归门)

目标:把内联读写换成持有/委托 `TcpConnection`;**现有两条测试(`ConnectSendEchoReceive`、`ConnectRefusedFails`)断言不变仍通过**。

- [ ] **Step 1: 改写 `include/transport/tcp/TcpClientTransport.hpp`**

整体替换为(删 `read_buf_`/`write_queue_`/`writing_`/`peer_id_`/`link_up_` 与 `StartRead`/`DoWrite`/`EnqueueWrite`/`HandleDisconnect`;加 `conn_` 与 `OnConnLost`;include `TcpConnection.hpp`):

```cpp
#pragma once

// TcpClientTransport.hpp — TCP 客户端纯字节管道。自有 io_context + 1 线程:
// connect + 连接超时 + 指数退避自动重连。连接建立后持有一个 TcpConnection,
// 收发委托给它(读写实现复用 TcpConnection,不再内联)。须以 shared_ptr 持有。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpConnection.hpp"

namespace transport {

class TcpClientTransport : public ITransport,
                           public std::enable_shared_from_this<TcpClientTransport> {
 public:
  explicit TcpClientTransport(
      TcpClientConfig config,
      std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
      std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  void StartConnect(std::shared_ptr<std::promise<Status>> prom);
  void ScheduleReconnect();
  void OnConnLost(const std::string& reason);  // 连接级断开(由 conn_ 的 OnDisconnect 路由进来)

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_, backoff_cap_, backoff_cur_;

  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::tcp::socket socket_;
  asio::ip::tcp::resolver resolver_;
  asio::steady_timer connect_timer_, reconnect_timer_;
  std::shared_ptr<TcpConnection> conn_;  // 当前连接;每次重连重建。仅在 strand_ 上访问。
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 2: 改写 `src/tcp/TcpClientTransport.cpp`**

整体替换为:

```cpp
#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

// TcpClientTransport.cpp — 见 .hpp。connect 成功后用已连接 socket 造 TcpConnection,
// 接入用户回调并委托收发;断开经 conn_ 的 OnDisconnect 路由到 OnConnLost(退避重连)。
// conn_ 只在 strand_ 上访问(Send 也 post 到 strand_),避免与 io 线程上的重建竞争。

namespace transport {

TcpClientTransport::TcpClientTransport(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : config_(std::move(config)),
      backoff_base_(backoff_base),
      backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_),
      resolver_(ctx_),
      connect_timer_(ctx_),
      reconnect_timer_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpClientTransport::~TcpClientTransport() { Close(); }

Status TcpClientTransport::Open() {
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = shared_from_this();
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();
}

void TcpClientTransport::StartConnect(std::shared_ptr<std::promise<Status>> prom) {
  auto self = shared_from_this();
  asio::error_code rec;
  auto endpoints = resolver_.resolve(config_.host, std::to_string(config_.port), rec);
  if (rec) {
    if (prom) prom->set_value(Status::Fail("conn: resolve: " + rec.message()));
    else ScheduleReconnect();
    return;
  }

  asio::error_code ig;
  socket_.close(ig);
  socket_ = asio::ip::tcp::socket(ctx_);

  auto timed_out = std::make_shared<bool>(false);
  connect_timer_.expires_after(std::chrono::milliseconds(config_.connect_timeout_ms));
  connect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self, timed_out](asio::error_code ec) {
        if (ec) return;
        *timed_out = true;
        asio::error_code ig2;
        socket_.close(ig2);
      }));

  asio::async_connect(
      socket_, endpoints,
      asio::bind_executor(
          strand_, [this, self, prom, timed_out](asio::error_code ec,
                                                 const asio::ip::tcp::endpoint&) {
            connect_timer_.cancel();
            if (!ec) {
              backoff_cur_ = backoff_base_;
              conn_ = std::make_shared<TcpConnection>(std::move(socket_));
              socket_ = asio::ip::tcp::socket(ctx_);  // 复位备用
              conn_->OnBytes(bytes_cb_);
              conn_->OnConnect(connect_cb_);
              std::weak_ptr<TcpClientTransport> wself = self;
              conn_->OnDisconnect([wself](const std::string& reason) {
                auto s = wself.lock();
                if (!s) return;
                asio::post(s->strand_, [s, reason]() { s->OnConnLost(reason); });
              });
              open_.store(true);
              if (prom) prom->set_value(Status::Success(std::monostate{}));
              conn_->Open();
              return;
            }
            std::string reason = *timed_out ? "timeout: connect timed out"
                                            : ("conn: " + ec.message());
            if (prom) prom->set_value(Status::Fail(reason));
            if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
          }));
}

void TcpClientTransport::OnConnLost(const std::string& reason) {
  open_.store(false);
  conn_.reset();
  if (config_.auto_reconnect && !closing_.load()) {
    ScheduleReconnect();
  } else if (disconnect_cb_) {
    disconnect_cb_(reason);
  }
}

void TcpClientTransport::ScheduleReconnect() {
  if (closing_.load() || !config_.auto_reconnect) return;
  reconnect_timer_.expires_after(backoff_cur_);
  auto self = shared_from_this();
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
        StartConnect(nullptr);
      }));
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: tcp not open");
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(bytes);
  asio::post(strand_, [this, self, buf]() {
    if (conn_) conn_->Send(*buf);  // conn_ 只在 strand_ 上访问
  });
  return Status::Success(std::monostate{});
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpClientTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    connect_timer_.cancel();
    reconnect_timer_.cancel();
    socket_.close(ig);
    if (conn_) conn_->Close();
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

- [ ] **Step 3: 运行现有客户端测试,确认仍绿(回归门)**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R TcpClientTransport 2>&1 | grep -iE "passed|failed"`
Expected: `TcpClientTransport.ConnectSendEchoReceive`、`TcpClientTransport.ConnectRefusedFails` 仍通过(断言未改)。

- [ ] **Step 4: 全量测试**

Run: `ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"`
Expected: `100% tests passed`(原 27 + TcpConnection 3 + TcpServer 3 = 33)。

- [ ] **Step 5: 提交**

```bash
git add include/transport/tcp/TcpClientTransport.hpp src/tcp/TcpClientTransport.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "refactor: TcpClientTransport 复用 TcpConnection(读写单一实现,行为不变)"
```

---

## Task 4: 干净构建零告警 + 全量验证

**Files:** 无(仅验证)

- [ ] **Step 1: 干净从零构建,零告警**

Run:
```bash
cd /home/ubuntu/david/transport
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---warnings above (should be none)---"
```
Expected: 无 `warning`/`error:` 行。

- [ ] **Step 2: 全量测试(连跑两次查 flaky)**

Run:
```bash
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 两次都 `100% tests passed`(33 个)。

- [ ] **Step 3: 解耦 / 残留检查**

Run:
```bash
grep -rn "ICodec\|Message" include/transport/tcp src/tcp || echo "(tcp 层不依赖 codec/Message —— 解耦保持)"
```
Expected: 无输出(tcp 层只进出裸字节)。

> 本任务无新增文件,若前序任务均已提交且工作树干净,无需额外提交。

---

## 完成标准
- `TcpConnection`(已连接 socket 纯管道)+ `TcpServerTransport`(接受器,最小 API)落地,各自独立测试通过。
- `TcpClientTransport` 重构为复用 `TcpConnection`,**原有两条测试断言不变仍通过**(行为回归)。
- 服务端产出 per-connection `ITransport`;`Close` 清场使对端见 EOF。
- 干净构建零告警;全量 33 测试稳定通过;tcp 层不引入 codec/Message(解耦保持)。
- 范围外(未做):广播/GetClients/DisconnectClient、DDS、codec/System 接入。
