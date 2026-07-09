# QtNetwork 传输迁移(协程化第一期)实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 把 UDP/TCP/串口传输从 standalone Asio 换成 QtNetwork,`ITransport` 接口不变,移除 asio,构建接 Qt5。

**Architecture:** 每个传输重写成 QObject-free 的 QtNetwork 包装:`QUdpSocket`/`QTcpSocket`/`QTcpServer`/`QSerialPort` + functor `connect`;不自持 io 线程,活在宿主 Qt 事件循环线程,`OnBytes` 由 `readyRead` 信号触发。逐传输迁移(asio 与 Qt 暂共存),最后移除 asio。

**Tech Stack:** C++17,Qt5(5.15.3;Core/Network/SerialPort),GoogleTest(自定义 QCoreApplication main + 事件泵),socat(串口回环测试)。配套 spec:`docs/superpowers/specs/2026-07-08-qtnetwork-transport-design.md`。

## Global Constraints

- C++17,**不抛异常**;`Result`/`Status`,错误前缀 `timeout:`/`conn:`/`codec:`/`frame:`/`io:`/`config:` 不变。
- **`ITransport` 接口签名与 `BytesCallback` 一字不改**;仅实现从 asio 换 QtNetwork。
- 传输**不自持 io 线程**,活在宿主 Qt 事件循环线程;`OnBytes`/`OnConnect`/`OnDisconnect` 在该线程串行触发。
- 传输类**不加 `Q_OBJECT`**(用 functor connect);socket 作成员(`std::unique_ptr<Q*>`),析构即断连。
- Qt5 `find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)`。
- 迁移中 asio 与 Qt 暂共存(未迁移的传输仍用 asio);**Task 7** 才移除 asio。
- 串口配置校验保持严格(波特率等失败即 `config:` 错)。
- 现有非传输测试(codec/comm/dds)**不回归**。提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**无 Co-Authored-By**。不提交 `build/`。
- 前置(已装):`libqt5serialport5-dev`、`socat`。

---

## File Structure

| 文件 | 责任 | 任务 |
|---|---|---|
| `CMakeLists.txt` | 接 Qt5、切自定义 test main、(末)去 asio | T1/T7 |
| `tests/test_main.cpp`(新) | `QCoreApplication` + gtest 入口 | T1 |
| `tests/transport/qt_test_util.hpp`(新) | `pumpUntil`/字节助手 | T1 |
| `include/transport/udp/UdpTransport.hpp` + `src/udp/UdpTransport.cpp` | QUdpSocket 实现 | T2 |
| `include/transport/tcp/TcpConnection.hpp` + `src/tcp/TcpConnection.cpp` | QTcpSocket 连接 | T3 |
| `include/transport/tcp/TcpServerTransport.hpp` + `src/tcp/TcpServerTransport.cpp` | QTcpServer 接受器 | T4 |
| `include/transport/tcp/TcpClientTransport.hpp` + `src/tcp/TcpClientTransport.cpp` | QTcpSocket 客户端+重连 | T5 |
| `include/transport/serial/SerialTransport.hpp` + `src/serial/SerialTransport.cpp` | QSerialPort 实现 | T6 |
| 各 `tests/transport/*_test.cpp` | 重写为 QtNetwork + 事件泵 | T2–T6 |
| `third_party/asio` + CMake asio 段 + `combination_smoke_test.cpp` | 移除 asio | T7 |

---

### Task 1: 构建脚手架(Qt5 + 自定义 main + 事件泵)

**Files:**
- Modify: `CMakeLists.txt`
- Create: `tests/test_main.cpp`, `tests/transport/qt_test_util.hpp`

**Interfaces:**
- Produces:`qtutil::pumpUntil(std::function<bool()> pred, int timeout_ms=2000)->bool`;`qtutil::B(std::initializer_list<uint8_t>)->std::vector<uint8_t>`。

- [ ] **Step 1: CMake 接 Qt5 + 切自定义 main**

`CMakeLists.txt`:`find_package(Threads REQUIRED)` 之后加:
```cmake
find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)
target_link_libraries(transport PUBLIC Qt5::Core Qt5::Network Qt5::SerialPort)
```
在 `add_executable(transport_tests` 源列表**首行**加 `tests/test_main.cpp`。把 `target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)` 改为 `GTest::gtest`(去掉 `_main`,改用自定义 main):
```cmake
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest)
```

- [ ] **Step 2: 写 `tests/test_main.cpp`**
```cpp
#include <QCoreApplication>
#include <gtest/gtest.h>

// 传输测试需 Qt 事件循环:进程级 QCoreApplication(不 exec,靠 processEvents 泵)。
int main(int argc, char** argv) {
  QCoreApplication app(argc, argv);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

- [ ] **Step 3: 写 `tests/transport/qt_test_util.hpp`**
```cpp
#pragma once
#include <QCoreApplication>
#include <QDeadlineTimer>
#include <QEventLoop>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <vector>

namespace qtutil {
// 泵 Qt 事件直到 pred() 为真或超时;返回 pred 最终是否满足。
inline bool pumpUntil(std::function<bool()> pred, int timeout_ms = 2000) {
  QDeadlineTimer dl(timeout_ms);
  while (!pred() && !dl.hasExpired())
    QCoreApplication::processEvents(QEventLoop::AllEvents, 10);
  return pred();
}
inline std::vector<uint8_t> B(std::initializer_list<uint8_t> l) { return std::vector<uint8_t>(l); }
}  // namespace qtutil
```

- [ ] **Step 4: 构建 + 全量回归**

Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON && cmake --build build -j$(nproc) && ctest --test-dir build 2>&1 | tail -3`
Expected: 配置阶段打印找到 Qt5;构建通过(此刻传输仍用 asio,Qt 已链接但未用);`100% tests passed`(119,自定义 main 驱动现有测试不变)。

- [ ] **Step 5: 提交**
```bash
git add CMakeLists.txt tests/test_main.cpp tests/transport/qt_test_util.hpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "build: 接 Qt5(Core/Network/SerialPort)+ 自定义 QCoreApplication 测试 main + 事件泵助手"
```

---

### Task 2: `UdpTransport` → QtNetwork

**Files:**
- Modify: `include/transport/udp/UdpTransport.hpp`, `src/udp/UdpTransport.cpp`
- Modify: `tests/transport/udp_transport_test.cpp`

**Interfaces:**
- Consumes:`qtutil::pumpUntil`/`qtutil::B`(T1)。
- Produces:`UdpTransport(UdpConfig)`;`ITransport` 接口不变;`uint16_t LocalPort() const`。

- [ ] **Step 1: 重写测试 `tests/transport/udp_transport_test.cpp`(失败态)**
```cpp
#include "transport/udp/UdpTransport.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Result;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;
using qtutil::pumpUntil; using qtutil::B;

TEST(UdpTransport, UnicastLoopbackRoundtrip) {
  UdpConfig ca; ca.mode = UdpMode::kUnicast; ca.local_addr = "127.0.0.1"; ca.local_port = 0;
  auto a = std::make_shared<UdpTransport>(ca);
  std::vector<uint8_t> got; std::string from;
  a->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f){ if(r){ got = r.value; from = f; } });
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  uint16_t aport = a->LocalPort();

  UdpConfig cb; cb.mode = UdpMode::kUnicast; cb.local_addr = "127.0.0.1"; cb.local_port = 0;
  cb.remote_addr = "127.0.0.1"; cb.remote_port = aport;
  auto b = std::make_shared<UdpTransport>(cb);
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Send(B({1, 2, 3}))));

  EXPECT_TRUE(pumpUntil([&]{ return !got.empty(); }));
  EXPECT_EQ(got, B({1, 2, 3}));
  EXPECT_NE(from.find("127.0.0.1:"), std::string::npos);
  a->Close(); b->Close();
}

TEST(UdpTransport, TopicEndpointRejected) {
  UdpConfig c; c.local_addr = "127.0.0.1"; c.local_port = 0;
  auto a = std::make_shared<UdpTransport>(c);
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  auto st = a->Send(B({9}), Endpoint::Topic("t"));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_NE(st.error.find("config:"), std::string::npos);
  a->Close();
}
```
Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— UdpTransport.hpp 仍是 asio 版,签名/行为不匹配(或缺 QHostAddress)。

- [ ] **Step 2: 重写 `include/transport/udp/UdpTransport.hpp`**
```cpp
#pragma once

// UdpTransport.hpp — UDP 纯字节管道(QtNetwork/QUdpSocket)。单播/组播/广播。
// 活在宿主 Qt 事件循环线程;每个 datagram 经 OnBytes 交付裸字节 + from("ip:port")。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <QHostAddress>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/udp/UdpConfig.hpp"

class QUdpSocket;

namespace transport {

class UdpTransport : public ITransport {
 public:
  explicit UdpTransport(UdpConfig config);
  ~UdpTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void onReadyRead();
  Status sendTo(const std::vector<uint8_t>& bytes, const QHostAddress& addr, quint16 port);

  UdpConfig config_;
  std::unique_ptr<QUdpSocket> sock_;
  QHostAddress dest_addr_;
  quint16 dest_port_ = 0;
  bool open_ = false;
  uint16_t local_port_ = 0;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 3: 重写 `src/udp/UdpTransport.cpp`**
```cpp
#include "transport/udp/UdpTransport.hpp"

#include <variant>

#include <QAbstractSocket>
#include <QNetworkDatagram>
#include <QUdpSocket>

namespace transport {

UdpTransport::UdpTransport(UdpConfig config) : config_(std::move(config)) {}
UdpTransport::~UdpTransport() { Close(); }

Status UdpTransport::Open() {
  sock_ = std::make_unique<QUdpSocket>();
  QHostAddress local;
  QUdpSocket::BindMode mode = QUdpSocket::DefaultForPlatform;
  if (config_.mode == UdpMode::kMulticast) {
    local = QHostAddress::AnyIPv4;
    mode = QUdpSocket::ShareAddress | QUdpSocket::ReuseAddressHint;
  } else {
    if (!local.setAddress(QString::fromStdString(config_.local_addr)))
      return Status::Fail("config: invalid local_addr");
  }
  if (config_.mode == UdpMode::kBroadcast) mode = QUdpSocket::ReuseAddressHint;
  if (!sock_->bind(local, config_.local_port, mode))
    return Status::Fail("config: bind: " + sock_->errorString().toStdString());

  if (config_.mode == UdpMode::kMulticast) {
    QHostAddress group(QString::fromStdString(config_.multicast_group));
    if (group.isNull()) return Status::Fail("config: invalid multicast_group");
    if (!sock_->joinMulticastGroup(group))
      return Status::Fail("config: join_group: " + sock_->errorString().toStdString());
    sock_->setSocketOption(QAbstractSocket::MulticastTtlOption, int(config_.ttl));
    sock_->setSocketOption(QAbstractSocket::MulticastLoopbackOption, 1);
    dest_addr_ = group;
    dest_port_ = config_.remote_port;
  } else if (!config_.remote_addr.empty()) {
    if (!dest_addr_.setAddress(QString::fromStdString(config_.remote_addr)))
      return Status::Fail("config: invalid remote_addr");
    dest_port_ = config_.remote_port;
  }

  local_port_ = sock_->localPort();
  QObject::connect(sock_.get(), &QUdpSocket::readyRead, [this] { onReadyRead(); });
  open_ = true;
  if (connect_cb_) connect_cb_();  // UDP 无连接,Open 成功即视作已连
  return Status::Success(std::monostate{});
}

void UdpTransport::onReadyRead() {
  while (sock_ && sock_->hasPendingDatagrams()) {
    QNetworkDatagram dg = sock_->receiveDatagram();
    QByteArray d = dg.data();
    std::vector<uint8_t> bytes(d.begin(), d.end());
    std::string from = dg.senderAddress().toString().toStdString() + ":" +
                       std::to_string(dg.senderPort());
    if (bytes_cb_)
      bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), from);
  }
}

Status UdpTransport::sendTo(const std::vector<uint8_t>& bytes, const QHostAddress& addr, quint16 port) {
  if (!open_ || !sock_) return Status::Fail("config: socket not open");
  QByteArray d(reinterpret_cast<const char*>(bytes.data()), int(bytes.size()));
  if (sock_->writeDatagram(d, addr, port) < 0)
    return Status::Fail("io: send: " + sock_->errorString().toStdString());
  return Status::Success(std::monostate{});
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes) {
  return sendTo(bytes, dest_addr_, dest_port_);
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return sendTo(bytes, dest_addr_, dest_port_);
    case Endpoint::Kind::kNet: {
      QHostAddress a;
      if (!a.setAddress(QString::fromStdString(to.host)))
        return Status::Fail("config: invalid address");
      return sendTo(bytes, a, static_cast<quint16>(to.port));
    }
    case Endpoint::Kind::kTopic:
      return Status::Fail("config: udp expects net endpoint");
  }
  return Status::Fail("config: unknown endpoint kind");
}

void UdpTransport::Close() {
  open_ = false;
  if (sock_) { sock_->close(); sock_.reset(); }  // 析构断开所有 connect
}

}  // namespace transport
```

- [ ] **Step 4: 构建 + 跑 UDP 测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "UdpTransport" --output-on-failure`
Expected: 2 个 `UdpTransport.*` 通过。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build 2>&1 | tail -3`
Expected: `100% tests passed`(仍 119;UDP 测试数不变)。
```bash
git add include/transport/udp/UdpTransport.hpp src/udp/UdpTransport.cpp tests/transport/udp_transport_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: UdpTransport 改用 QtNetwork(QUdpSocket)"
```

---

### Task 3: `TcpConnection` → QtNetwork

**Files:**
- Modify: `include/transport/tcp/TcpConnection.hpp`, `src/tcp/TcpConnection.cpp`
- Modify: `tests/transport/tcp_connection_test.cpp`

**Interfaces:**
- Produces:`TcpConnection(QTcpSocket* sock)`(接管已连接 socket,转移所有权);`ITransport` 接口不变。

- [ ] **Step 1: 重写测试 `tests/transport/tcp_connection_test.cpp`(失败态)**
```cpp
#include "transport/tcp/TcpConnection.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <QTcpServer>
#include <QTcpSocket>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpConnection;
using qtutil::pumpUntil; using qtutil::B;

// 用一个 QTcpServer 造一对已连接 socket,验证 TcpConnection 收发。
TEST(TcpConnection, LoopbackRoundtrip) {
  QTcpServer server;
  ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  quint16 port = server.serverPort();

  QTcpSocket* client = new QTcpSocket();
  client->connectToHost(QHostAddress::LocalHost, port);
  ASSERT_TRUE(pumpUntil([&]{ return server.hasPendingConnections() &&
                                     client->state() == QAbstractSocket::ConnectedState; }));
  QTcpSocket* accepted = server.nextPendingConnection();
  ASSERT_NE(accepted, nullptr);

  auto conn = std::make_shared<TcpConnection>(accepted);
  std::vector<uint8_t> got;
  conn->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) got = r.value; });
  ASSERT_TRUE(static_cast<bool>(conn->Open()));

  client->write("hi", 2); client->flush();
  EXPECT_TRUE(pumpUntil([&]{ return got.size() == 2; }));
  EXPECT_EQ(got, B({'h', 'i'}));

  // conn->Send 回去,client 收到
  QByteArray back;
  QObject::connect(client, &QTcpSocket::readyRead, [&]{ back += client->readAll(); });
  ASSERT_TRUE(static_cast<bool>(conn->Send(B({'y', 'o'}))));
  EXPECT_TRUE(pumpUntil([&]{ return back.size() == 2; }));
  EXPECT_EQ(back, QByteArray("yo"));

  conn->Close();
  client->close(); client->deleteLater();
}

TEST(TcpConnection, PeerCloseTriggersDisconnect) {
  QTcpServer server; ASSERT_TRUE(server.listen(QHostAddress::LocalHost, 0));
  QTcpSocket* client = new QTcpSocket();
  client->connectToHost(QHostAddress::LocalHost, server.serverPort());
  ASSERT_TRUE(pumpUntil([&]{ return server.hasPendingConnections(); }));
  auto conn = std::make_shared<TcpConnection>(server.nextPendingConnection());
  bool disc = false;
  conn->OnDisconnect([&](const std::string&){ disc = true; });
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  client->disconnectFromHost();               // 对端主动断开
  EXPECT_TRUE(pumpUntil([&]{ return disc; }));  // 真断开 → OnDisconnect 一次
  client->deleteLater();
}
```
Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— TcpConnection 仍是 asio 版(构造签名/成员不匹配)。

- [ ] **Step 2: 重写 `include/transport/tcp/TcpConnection.hpp`**
```cpp
#pragma once

// TcpConnection.hpp — 一条已连接的 TCP 字节管道(QtNetwork/QTcpSocket)。
// client 与 server-accepted 共用。字节流:一次 read 切片经 OnBytes 交付(切帧归上层)。
// 主动 Close 不报 OnDisconnect;真实断开报一次。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

class QTcpSocket;

namespace transport {

class TcpConnection : public ITransport {
 public:
  explicit TcpConnection(QTcpSocket* sock);  // 接管已连接 socket 的所有权
  ~TcpConnection() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

 private:
  void onReadyRead();
  void handleDisconnect(const std::string& reason);

  std::unique_ptr<QTcpSocket> sock_;
  std::string peer_id_;
  bool open_ = false;
  bool disconnected_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 3: 重写 `src/tcp/TcpConnection.cpp`**
```cpp
#include "transport/tcp/TcpConnection.hpp"

#include <variant>

#include <QTcpSocket>

namespace transport {

TcpConnection::TcpConnection(QTcpSocket* sock) : sock_(sock) {}
TcpConnection::~TcpConnection() { Close(); }

Status TcpConnection::Open() {
  peer_id_ = sock_->peerAddress().toString().toStdString() + ":" +
             std::to_string(sock_->peerPort());
  QObject::connect(sock_.get(), &QTcpSocket::readyRead, [this] { onReadyRead(); });
  QObject::connect(sock_.get(), &QTcpSocket::disconnected,
                   [this] { handleDisconnect("conn: disconnected"); });
  QObject::connect(sock_.get(),
                   QOverload<QAbstractSocket::SocketError>::of(&QAbstractSocket::errorOccurred),
                   [this] { handleDisconnect("conn: " + sock_->errorString().toStdString()); });
  open_ = true;
  if (connect_cb_) connect_cb_();
  return Status::Success(std::monostate{});
}

void TcpConnection::onReadyRead() {
  if (!sock_) return;
  QByteArray d = sock_->readAll();
  if (d.isEmpty()) return;
  std::vector<uint8_t> bytes(d.begin(), d.end());
  if (bytes_cb_)
    bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), peer_id_);
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes) {
  if (!open_ || !sock_) return Status::Fail("config: connection not open");
  sock_->write(reinterpret_cast<const char*>(bytes.data()), qint64(bytes.size()));
  return Status::Success(std::monostate{});  // QTcpSocket 内部有写缓冲,不代表对端已收
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpConnection::handleDisconnect(const std::string& reason) {
  if (disconnected_) return;   // 一次性闸(disconnected + errorOccurred 只报一次)
  disconnected_ = true;
  open_ = false;
  if (disconnect_cb_) disconnect_cb_(reason);
}

void TcpConnection::Close() {
  disconnected_ = true;        // 主动关不算断线,不回 OnDisconnect
  open_ = false;
  if (sock_) { sock_->abort(); sock_.reset(); }
}

}  // namespace transport
```

- [ ] **Step 4: 构建 + 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "TcpConnection" --output-on-failure`
Expected: 2 个 `TcpConnection.*` 通过。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build 2>&1 | tail -3` → `100% tests passed`。
```bash
git add include/transport/tcp/TcpConnection.hpp src/tcp/TcpConnection.cpp tests/transport/tcp_connection_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TcpConnection 改用 QtNetwork(QTcpSocket)"
```

---

### Task 4: `TcpServerTransport` → QtNetwork

**Files:**
- Modify: `include/transport/tcp/TcpServerTransport.hpp`, `src/tcp/TcpServerTransport.cpp`, `include/transport/tcp/TcpServerConfig.hpp`(注释)
- Modify: `tests/transport/tcp_server_test.cpp`

**Interfaces:**
- Consumes:`TcpConnection(QTcpSocket*)`(T3)。
- Produces:`TcpServerTransport(TcpServerConfig)`;`void OnAccept(std::function<void(std::shared_ptr<ITransport>)>)`;`void OnError(std::function<void(const std::string&)>)`;`uint16_t LocalPort() const`。

- [ ] **Step 1: 重写测试 `tests/transport/tcp_server_test.cpp`(失败态)**
```cpp
#include "transport/tcp/TcpServerTransport.hpp"
#include "transport/tcp/TcpConnection.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <vector>
#include <QTcpSocket>
#include <gtest/gtest.h>

using transport::ITransport;
using transport::Result;
using transport::TcpServerConfig;
using transport::TcpServerTransport;
using qtutil::pumpUntil; using qtutil::B;

TEST(TcpServerTransport, AcceptsAndEchoes) {
  TcpServerConfig cfg; cfg.bind_addr = "127.0.0.1"; cfg.port = 0;
  auto server = std::make_shared<TcpServerTransport>(cfg);
  std::vector<std::shared_ptr<ITransport>> accepted;
  std::vector<uint8_t> srv_got;
  server->OnAccept([&](std::shared_ptr<ITransport> conn) {
    conn->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) srv_got = r.value; });
    (void)conn->Open();
    accepted.push_back(conn);  // 保活
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));
  uint16_t port = server->LocalPort();

  QTcpSocket client;
  client.connectToHost(QHostAddress::LocalHost, port);
  ASSERT_TRUE(pumpUntil([&]{ return !accepted.empty() &&
                                    client.state() == QAbstractSocket::ConnectedState; }));
  client.write("ab", 2); client.flush();
  EXPECT_TRUE(pumpUntil([&]{ return srv_got.size() == 2; }));
  EXPECT_EQ(srv_got, B({'a', 'b'}));
  server->Close();
}
```
Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— TcpServerTransport 仍是 asio 版。

- [ ] **Step 2: 重写 `include/transport/tcp/TcpServerTransport.hpp`**
```cpp
#pragma once

// TcpServerTransport.hpp — TCP 服务端接受器(QtNetwork/QTcpServer)。非 ITransport。
// 每 accept 一个连接造一个 TcpConnection 经 OnAccept 交付,用户在其上独立收发。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

class QTcpServer;

namespace transport {

class TcpConnection;

class TcpServerTransport {
 public:
  explicit TcpServerTransport(TcpServerConfig config);
  ~TcpServerTransport();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_; }

  void OnAccept(std::function<void(std::shared_ptr<ITransport>)> cb) { accept_cb_ = std::move(cb); }
  void OnError(std::function<void(const std::string&)> cb) { error_cb_ = std::move(cb); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void onNewConnection();

  TcpServerConfig config_;
  std::unique_ptr<QTcpServer> server_;
  bool open_ = false;
  uint16_t local_port_ = 0;
  std::vector<std::weak_ptr<TcpConnection>> conns_;
  std::function<void(std::shared_ptr<ITransport>)> accept_cb_;
  std::function<void(const std::string&)> error_cb_;
};

}  // namespace transport
```

- [ ] **Step 3: 重写 `src/tcp/TcpServerTransport.cpp`**
```cpp
#include "transport/tcp/TcpServerTransport.hpp"

#include <variant>

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>

#include "transport/tcp/TcpConnection.hpp"

namespace transport {

TcpServerTransport::TcpServerTransport(TcpServerConfig config) : config_(std::move(config)) {}
TcpServerTransport::~TcpServerTransport() { Close(); }

Status TcpServerTransport::Open() {
  server_ = std::make_unique<QTcpServer>();
  if (config_.backlog > 0) server_->setMaxPendingConnections(config_.backlog);
  QHostAddress addr;
  if (!addr.setAddress(QString::fromStdString(config_.bind_addr)))
    return Status::Fail("config: invalid bind_addr");
  if (!server_->listen(addr, config_.port))
    return Status::Fail("io: listen: " + server_->errorString().toStdString());
  local_port_ = server_->serverPort();
  QObject::connect(server_.get(), &QTcpServer::newConnection, [this] { onNewConnection(); });
  open_ = true;
  return Status::Success(std::monostate{});
}

void TcpServerTransport::onNewConnection() {
  while (server_ && server_->hasPendingConnections()) {
    QTcpSocket* s = server_->nextPendingConnection();  // 默认归 server 所有(child)
    s->setParent(nullptr);                             // 解除父子,交 TcpConnection 独占(防双删)
    auto conn = std::make_shared<TcpConnection>(s);
    // 清理失效 weak
    conns_.erase(std::remove_if(conns_.begin(), conns_.end(),
                   [](const std::weak_ptr<TcpConnection>& w){ return w.expired(); }), conns_.end());
    conns_.push_back(conn);                 // 只存 weak(保活是用户的事)
    if (accept_cb_) accept_cb_(conn);       // 用户须在回调里 Open + 保活
  }
}

void TcpServerTransport::Close() {
  open_ = false;
  if (server_) { server_->close(); server_.reset(); }
  for (auto& w : conns_) if (auto c = w.lock()) c->Close();
  conns_.clear();
}

}  // namespace transport
```
注:`std::remove_if` 需 `#include <algorithm>`(加到 .cpp 顶部——上面代码已在循环里 `setParent(nullptr)` 解除父子,`TcpConnection` 的 `unique_ptr` 独占该 socket,避免 QTcpServer 与之双删)。

- [ ] **Step 4: `TcpServerConfig.hpp` 注释**:把 `// <=0 → 用 asio 默认 max_listen_connections` 改为 `// <=0 → Qt 默认(setMaxPendingConnections 默认 30)`。

- [ ] **Step 5: 构建 + 测试 + 回归 + 提交**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "TcpServerTransport" --output-on-failure && ctest --test-dir build 2>&1 | tail -3`
Expected: `TcpServerTransport.*` 通过;全量 `100% tests passed`。
```bash
git add include/transport/tcp/TcpServerTransport.hpp src/tcp/TcpServerTransport.cpp include/transport/tcp/TcpServerConfig.hpp tests/transport/tcp_server_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TcpServerTransport 改用 QtNetwork(QTcpServer)"
```

---

### Task 5: `TcpClientTransport` → QtNetwork(连接 + 超时 + 重连)

**Files:**
- Modify: `include/transport/tcp/TcpClientTransport.hpp`, `src/tcp/TcpClientTransport.cpp`
- Modify: `tests/transport/tcp_transport_test.cpp`

**Interfaces:**
- Consumes:`TcpServerTransport`(T4)、`TcpConnection`(T3)。
- Produces:`TcpClientTransport(TcpClientConfig)`;`ITransport` 接口不变。

- [ ] **Step 1: 重写测试 `tests/transport/tcp_transport_test.cpp`(失败态)**
```cpp
#include "transport/tcp/TcpClientTransport.hpp"
#include "transport/tcp/TcpServerTransport.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <vector>
#include <gtest/gtest.h>

using transport::ITransport;
using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientTransport;
using transport::TcpServerConfig;
using transport::TcpServerTransport;
using qtutil::pumpUntil; using qtutil::B;

TEST(TcpClientTransport, ClientServerRoundtrip) {
  TcpServerConfig scfg; scfg.bind_addr = "127.0.0.1"; scfg.port = 0;
  auto server = std::make_shared<TcpServerTransport>(scfg);
  std::vector<std::shared_ptr<ITransport>> keep;
  std::vector<uint8_t> srv_got;
  server->OnAccept([&](std::shared_ptr<ITransport> c) {
    c->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r){ srv_got = r.value; (void)c->Send(r.value); } });  // 回显
    (void)c->Open(); keep.push_back(c);
  });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig ccfg; ccfg.host = "127.0.0.1"; ccfg.port = server->LocalPort();
  auto client = std::make_shared<TcpClientTransport>(ccfg);
  std::vector<uint8_t> cli_got; bool connected = false;
  client->OnConnect([&]{ connected = true; });
  client->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) cli_got = r.value; });
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  EXPECT_TRUE(pumpUntil([&]{ return connected; }));

  ASSERT_TRUE(static_cast<bool>(client->Send(B({7, 8}))));
  EXPECT_TRUE(pumpUntil([&]{ return srv_got.size() == 2 && cli_got.size() == 2; }));
  EXPECT_EQ(srv_got, B({7, 8}));
  EXPECT_EQ(cli_got, B({7, 8}));  // 回显收到
  client->Close(); server->Close();
}

TEST(TcpClientTransport, ConnectRefusedFails) {
  TcpClientConfig ccfg; ccfg.host = "127.0.0.1"; ccfg.port = 1;  // 极可能拒连
  ccfg.connect_timeout_ms = 800; ccfg.auto_reconnect = false;
  auto client = std::make_shared<TcpClientTransport>(ccfg);
  bool disc = false;
  client->OnDisconnect([&](const std::string&){ disc = true; });
  auto st = client->Open();
  // Open 返回失败,或稍后经 OnDisconnect 上报;两者其一即可
  if (!st) { SUCCEED(); } else { EXPECT_TRUE(pumpUntil([&]{ return disc; }, 2000)); }
  client->Close();
}
```
Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— TcpClientTransport 仍是 asio 版。

- [ ] **Step 2: 重写 `include/transport/tcp/TcpClientTransport.hpp`**
```cpp
#pragma once

// TcpClientTransport.hpp — 客户端 TCP 字节管道(QtNetwork/QTcpSocket)。
// connectToHost + 连接超时(QTimer)+ 可选指数退避自动重连。活在宿主 Qt 事件循环线程。

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"

class QTcpSocket;
class QTimer;

namespace transport {

class TcpClientTransport : public ITransport {
 public:
  explicit TcpClientTransport(TcpClientConfig config,
                              std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
                              std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

 private:
  void startConnect();
  void onConnected();
  void onReadyRead();
  void onDisconnected(const std::string& reason);
  void scheduleReconnect();

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_, backoff_cap_, backoff_cur_;
  std::unique_ptr<QTcpSocket> sock_;
  std::unique_ptr<QTimer> connect_timer_;
  std::unique_ptr<QTimer> reconnect_timer_;
  std::string peer_id_;
  bool open_ = false;
  bool closing_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 3: 重写 `src/tcp/TcpClientTransport.cpp`**
```cpp
#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <variant>

#include <QTcpSocket>
#include <QTimer>

namespace transport {

TcpClientTransport::TcpClientTransport(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : config_(std::move(config)), backoff_base_(backoff_base), backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base) {}

TcpClientTransport::~TcpClientTransport() { Close(); }

Status TcpClientTransport::Open() {
  connect_timer_ = std::make_unique<QTimer>();  connect_timer_->setSingleShot(true);
  reconnect_timer_ = std::make_unique<QTimer>(); reconnect_timer_->setSingleShot(true);
  QObject::connect(reconnect_timer_.get(), &QTimer::timeout, [this] { startConnect(); });
  open_ = true;
  startConnect();
  return Status::Success(std::monostate{});  // 连接结果经 OnConnect/OnDisconnect 上报
}

void TcpClientTransport::startConnect() {
  if (closing_) return;
  sock_ = std::make_unique<QTcpSocket>();
  QObject::connect(sock_.get(), &QTcpSocket::connected, [this] { onConnected(); });
  QObject::connect(sock_.get(), &QTcpSocket::readyRead, [this] { onReadyRead(); });
  QObject::connect(sock_.get(), &QTcpSocket::disconnected,
                   [this] { onDisconnected("conn: disconnected"); });
  QObject::connect(sock_.get(),
                   QOverload<QAbstractSocket::SocketError>::of(&QAbstractSocket::errorOccurred),
                   [this] { onDisconnected("conn: " + sock_->errorString().toStdString()); });
  QObject::connect(connect_timer_.get(), &QTimer::timeout, [this] {
    if (sock_ && sock_->state() != QAbstractSocket::ConnectedState) {
      sock_->abort();
      onDisconnected("timeout: connect timed out");
    }
  });
  connect_timer_->start(int(config_.connect_timeout_ms));
  sock_->connectToHost(QString::fromStdString(config_.host), config_.port);
}

void TcpClientTransport::onConnected() {
  connect_timer_->stop();
  backoff_cur_ = backoff_base_;
  peer_id_ = sock_->peerAddress().toString().toStdString() + ":" + std::to_string(sock_->peerPort());
  if (connect_cb_) connect_cb_();
}

void TcpClientTransport::onReadyRead() {
  if (!sock_) return;
  QByteArray d = sock_->readAll();
  if (d.isEmpty()) return;
  std::vector<uint8_t> bytes(d.begin(), d.end());
  if (bytes_cb_) bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), peer_id_);
}

void TcpClientTransport::onDisconnected(const std::string& reason) {
  connect_timer_->stop();
  if (sock_) { sock_->disconnect(); }  // 断开所有 connect,防重入
  if (config_.auto_reconnect && !closing_) {
    scheduleReconnect();               // 自动重连:不打扰用户
  } else if (disconnect_cb_) {
    disconnect_cb_(reason);
  }
}

void TcpClientTransport::scheduleReconnect() {
  if (closing_ || !config_.auto_reconnect) return;
  reconnect_timer_->start(int(backoff_cur_.count()));
  backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!sock_ || sock_->state() != QAbstractSocket::ConnectedState)
    return Status::Fail("config: tcp not open");
  sock_->write(reinterpret_cast<const char*>(bytes.data()), qint64(bytes.size()));
  return Status::Success(std::monostate{});
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpClientTransport::Close() {
  closing_ = true;
  open_ = false;
  if (connect_timer_) connect_timer_->stop();
  if (reconnect_timer_) reconnect_timer_->stop();
  if (sock_) { sock_->abort(); sock_.reset(); }
}

}  // namespace transport
```

- [ ] **Step 4: 构建 + 测试 + 回归 + 提交**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "TcpClientTransport" --output-on-failure && ctest --test-dir build 2>&1 | tail -3`
Expected: `TcpClientTransport.*` 通过;全量绿。
```bash
git add include/transport/tcp/TcpClientTransport.hpp src/tcp/TcpClientTransport.cpp tests/transport/tcp_transport_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TcpClientTransport 改用 QtNetwork(QTcpSocket)+ QTimer 超时/退避重连"
```

---

### Task 6: `SerialTransport` → QtNetwork(QSerialPort)

**Files:**
- Modify: `include/transport/serial/SerialTransport.hpp`, `src/serial/SerialTransport.cpp`
- Modify: `tests/transport/serial_transport_test.cpp`

**Interfaces:**
- Produces:`SerialTransport(SerialConfig)`;`ITransport` 接口不变。

- [ ] **Step 1: 重写测试 `tests/transport/serial_transport_test.cpp`(失败态,socat 回环)**
```cpp
#include "transport/serial/SerialTransport.hpp"
#include "qt_test_util.hpp"
#include <array>
#include <cstdio>
#include <memory>
#include <regex>
#include <string>
#include <QProcess>
#include <gtest/gtest.h>

using transport::Result;
using transport::SerialConfig;
using transport::SerialTransport;
using qtutil::pumpUntil; using qtutil::B;

namespace {
// 起 socat 造一对虚拟串口,解析出两个 /dev/pts/N。socat 不可用返回 false。
bool StartSocat(QProcess& p, std::string& dev_a, std::string& dev_b) {
  p.start("socat", {"-d", "-d", "pty,raw,echo=0", "pty,raw,echo=0"});
  if (!p.waitForStarted(1000)) return false;
  std::string err; std::regex re("(/dev/pts/[0-9]+)");
  for (int i = 0; i < 50 && dev_b.empty(); ++i) {
    p.waitForReadyRead(100);
    err += p.readAllStandardError().toStdString();
    std::smatch m; auto begin = err.cbegin();
    std::vector<std::string> found;
    for (std::sregex_iterator it(err.begin(), err.end(), re), e; it != e; ++it) found.push_back((*it)[1]);
    if (found.size() >= 2) { dev_a = found[0]; dev_b = found[1]; }
  }
  return !dev_a.empty() && !dev_b.empty();
}
}  // namespace

TEST(SerialTransport, SocatLoopbackRoundtrip) {
  QProcess socat; std::string da, db;
  if (!StartSocat(socat, da, db)) { GTEST_SKIP() << "socat unavailable"; }

  SerialConfig ca; ca.device = da; ca.baud_rate = 115200;
  SerialConfig cb; cb.device = db; cb.baud_rate = 115200;
  auto a = std::make_shared<SerialTransport>(ca);
  auto b = std::make_shared<SerialTransport>(cb);
  std::vector<uint8_t> got;
  b->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) got = r.value; });
  if (!a->Open() || !b->Open()) { GTEST_SKIP() << "pty open/config failed in env"; }

  ASSERT_TRUE(static_cast<bool>(a->Send(B({0xC0, 0xDE}))));
  EXPECT_TRUE(pumpUntil([&]{ return got.size() == 2; }, 3000));
  EXPECT_EQ(got, B({0xC0, 0xDE}));
  a->Close(); b->Close();
  socat.kill(); socat.waitForFinished(1000);
}

TEST(SerialTransport, OpenNonexistentFails) {
  SerialConfig c; c.device = "/dev/nonexistent_tty_zzz";
  auto s = std::make_shared<SerialTransport>(c);
  auto st = s->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_NE(st.error.find("config:"), std::string::npos);
}
```
Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— SerialTransport 仍是 asio 版。

- [ ] **Step 2: 重写 `include/transport/serial/SerialTransport.hpp`**
```cpp
#pragma once

// SerialTransport.hpp — 串口字节管道(QtNetwork/QSerialPort)。活在宿主 Qt 事件循环线程。
// 读到的切片经 OnBytes 交付(切帧归上层 codec);from=设备路径。

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/serial/SerialConfig.hpp"

class QSerialPort;

namespace transport {

class SerialTransport : public ITransport {
 public:
  explicit SerialTransport(SerialConfig config);
  ~SerialTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_; }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

 private:
  void onReadyRead();

  SerialConfig config_;
  std::unique_ptr<QSerialPort> port_;
  bool open_ = false;
  bool disconnected_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 3: 重写 `src/serial/SerialTransport.cpp`**
```cpp
#include "transport/serial/SerialTransport.hpp"

#include <variant>

#include <QSerialPort>

namespace transport {

SerialTransport::SerialTransport(SerialConfig config) : config_(std::move(config)) {}
SerialTransport::~SerialTransport() { Close(); }

Status SerialTransport::Open() {
  port_ = std::make_unique<QSerialPort>();
  port_->setPortName(QString::fromStdString(config_.device));
  if (!port_->open(QIODevice::ReadWrite))
    return Status::Fail("config: open " + config_.device + ": " + port_->errorString().toStdString());

  auto fail = [this](const std::string& m) { port_->close(); return Status::Fail(m); };
  if (!port_->setBaudRate(int(config_.baud_rate))) return fail("config: baud_rate");
  if (!port_->setDataBits(static_cast<QSerialPort::DataBits>(config_.data_bits)))
    return fail("config: data_bits");
  QSerialPort::StopBits sb = (config_.stop_bits == 2) ? QSerialPort::TwoStop
                           : (config_.stop_bits == 1) ? QSerialPort::OneStop
                           : QSerialPort::UnknownStopBits;
  if (sb == QSerialPort::UnknownStopBits) return fail("config: stop_bits must be 1 or 2");
  if (!port_->setStopBits(sb)) return fail("config: stop_bits");
  QSerialPort::Parity par = (config_.parity == 'N') ? QSerialPort::NoParity
                          : (config_.parity == 'E') ? QSerialPort::EvenParity
                          : (config_.parity == 'O') ? QSerialPort::OddParity
                          : QSerialPort::UnknownParity;
  if (par == QSerialPort::UnknownParity) return fail("config: parity must be N/E/O");
  if (!port_->setParity(par)) return fail("config: parity");
  port_->setFlowControl(QSerialPort::NoFlowControl);

  QObject::connect(port_.get(), &QSerialPort::readyRead, [this] { onReadyRead(); });
  QObject::connect(port_.get(),
                   QOverload<QSerialPort::SerialPortError>::of(&QSerialPort::errorOccurred),
                   [this](QSerialPort::SerialPortError e) {
                     if (e == QSerialPort::NoError || disconnected_) return;
                     if (e == QSerialPort::ResourceError || e == QSerialPort::PermissionError) {
                       disconnected_ = true; open_ = false;
                       if (disconnect_cb_) disconnect_cb_("conn: " + port_->errorString().toStdString());
                     }
                   });
  open_ = true;
  if (connect_cb_) connect_cb_();
  return Status::Success(std::monostate{});
}

void SerialTransport::onReadyRead() {
  if (!port_) return;
  QByteArray d = port_->readAll();
  if (d.isEmpty()) return;
  std::vector<uint8_t> bytes(d.begin(), d.end());
  if (bytes_cb_) bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), config_.device);
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_ || !port_) return Status::Fail("config: serial not open");
  if (port_->write(reinterpret_cast<const char*>(bytes.data()), qint64(bytes.size())) < 0)
    return Status::Fail("io: write: " + port_->errorString().toStdString());
  return Status::Success(std::monostate{});
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void SerialTransport::Close() {
  disconnected_ = true;
  open_ = false;
  if (port_) { port_->close(); port_.reset(); }
}

}  // namespace transport
```

- [ ] **Step 4: 构建 + 测试 + 回归 + 提交**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "SerialTransport" --output-on-failure && ctest --test-dir build 2>&1 | tail -3`
Expected:`SerialTransport.*` 通过(socat 环回;若 socat/pty 不行则该用例 SKIP,不算失败);全量绿。
```bash
git add include/transport/serial/SerialTransport.hpp src/serial/SerialTransport.cpp tests/transport/serial_transport_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: SerialTransport 改用 QtNetwork(QSerialPort);串口测试用 socat 回环"
```

---

### Task 7: 移除 asio + 收尾

**Files:**
- Modify: `CMakeLists.txt`, `tests/transport/combination_smoke_test.cpp`
- Delete(可选): `third_party/asio`

**Interfaces:** 无新增。

- [ ] **Step 1: 确认无残留 asio 引用**

Run: `grep -rn "asio" include src | grep -v "默认\|注释" || echo "(无 asio 代码引用)"`
Expected:`(无 asio 代码引用)`(所有 5 传输已迁移)。若有,补掉。

- [ ] **Step 2: CMake 去 asio**

`CMakeLists.txt`:删掉 asio 段:
```cmake
# asio (standalone, header-only) —— vendored
add_library(asio_standalone INTERFACE)
target_include_directories(asio_standalone INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/include)
target_compile_definitions(asio_standalone INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(asio_standalone INTERFACE Threads::Threads)
target_link_libraries(transport PUBLIC asio_standalone)
```
保留 `target_link_libraries(transport PUBLIC Threads::Threads)`(ThreadExecutor 仍用线程)。

- [ ] **Step 3: 重写 `tests/transport/combination_smoke_test.cpp`**

该测试原验证"传输+codec"组合冒烟。改为:用 QtNetwork UDP 回环 + `SystemDatagramCodec`(报文版)跑一条 encode→send→recv→decode 往返。完整替换文件内容:
```cpp
#include "transport/udp/UdpTransport.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <gtest/gtest.h>

using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;
using qtutil::pumpUntil;

// 冒烟:UDP(QtNetwork)+ SystemDatagramCodec 一条帧 encode→send→recv→decode 往返。
TEST(CombinationSmoke, UdpSystemDatagramRoundtrip) {
  UdpConfig ra; ra.mode = UdpMode::kUnicast; ra.local_addr = "127.0.0.1"; ra.local_port = 0;
  auto recv = std::make_shared<UdpTransport>(ra);
  SystemDatagramCodec codec;
  std::vector<Message> decoded;
  recv->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){
    if (r) { auto m = codec.Decode(r.value.data(), r.value.size()); if (m) decoded = m.value; }
  });
  ASSERT_TRUE(static_cast<bool>(recv->Open()));

  UdpConfig sa; sa.mode = UdpMode::kUnicast; sa.local_addr = "127.0.0.1"; sa.local_port = 0;
  sa.remote_addr = "127.0.0.1"; sa.remote_port = recv->LocalPort();
  auto send = std::make_shared<UdpTransport>(sa);
  ASSERT_TRUE(static_cast<bool>(send->Open()));

  Message m; m.frm_type = FrameType::kCommand; m.protocol_id = 1; m.session_id = 5;
  m.message_id = 0x0042; m.payload = {0xAA, 0xBB};
  auto bytes = codec.Encode(m);
  ASSERT_TRUE(static_cast<bool>(bytes));
  ASSERT_TRUE(static_cast<bool>(send->Send(bytes.value)));

  EXPECT_TRUE(pumpUntil([&]{ return !decoded.empty(); }));
  ASSERT_EQ(decoded.size(), 1u);
  EXPECT_EQ(decoded[0].message_id, 0x0042);
  EXPECT_EQ(decoded[0].payload, (std::vector<uint8_t>{0xAA, 0xBB}));
  recv->Close(); send->Close();
}
```

- [ ] **Step 4: 构建 + 全量 + 严格告警**

Run: `rm -rf build && cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "error|warning" | head; ctest --test-dir build 2>&1 | tail -3`
Expected: 零 error/warning;`100% tests passed`(数量:原 119 中 UDP/TCP/串口/combination 用例被替换,总数可能微调,以实际为准且全绿)。

- [ ] **Step 5: 删 vendored asio(可选)+ 提交**
```bash
git rm -r third_party/asio 2>/dev/null || true
git add CMakeLists.txt tests/transport/combination_smoke_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "refactor: 移除 asio(传输已全迁 QtNetwork);combination_smoke 改 UDP+SystemDatagramCodec"
```

---

## 实现后(计划外)

- 同步 SRS/SDD/README/CHANGELOG(传输改 QtNetwork、线程模型、构建需 Qt5);终态全分支 review → finishing-a-development-branch。
- **第二期**(协程引擎 + 协程 ProtocolNode,AsyncTask)另立 spec/plan。

## Self-Review 记录

- **Spec 覆盖**:§3 五传输 → T2–T6;§5 构建接 Qt → T1;§6 测试(自定义 main/事件泵/socat)→ T1+各任务测试;§7 风险(QtSerialPort/socat/backlog)→ 已装 + T4 注释 + T6 SKIP 兜底;去 asio → T7。
- **占位扫描**:无。每步含完整代码与命令。
- **类型一致**:`TcpConnection(QTcpSocket*)` 在 T3 定义、T4 使用一致;`TcpServerTransport::OnAccept(std::function<void(std::shared_ptr<ITransport>)>)`/`LocalPort()` T4 定义、T5 测试使用一致;`qtutil::pumpUntil`/`B` T1 定义、各测试使用一致;各传输 `ITransport` 覆写签名与接口一致。
- **潜在坑已标注**:T4 `nextPendingConnection()` 的 socket 需 `setParent(nullptr)` 再交 `TcpConnection` 接管(已在 Step 3 注明);Qt5 `errorOccurred` 信号(5.15 有;若 5.12 用 `error` 老信号,实现期按装的 5.15.3 即可)。
