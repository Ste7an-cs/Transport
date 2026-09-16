#pragma once

/**
 * @file ListeningTcpTransport.hpp
 * @brief **示例自带**的 TCP 监听侧字节管道 —— `ITransport` 的一个本地实现。
 *
 * ## 为什么它在 `examples/` 里而不是在库里
 *
 * 库里的 `TcpTransport` 是**客户端**传输(ADR-0011 D1/D2:连出去 + 内建透明重连);
 * `TcpServer` 本轮不做(ADR-0011 **D10**,其 `.cpp` 不在任一构建的源清单里)。而
 * `examples/tcp_server` 要在**服务端角色**上挂一个真 `ProtocolNode`,节点又只接
 * `ITransport&` —— 于是缺一个"监听 + 接受 + 收发字节"的实现,只好示例自带一个。
 *
 * **它是示例的脚手架,不是库代码**:不进 `libtransport.a`、不进公共头、不改任何既有 API。
 * `examples/tcp_server/main.cpp` 里除了"构造它"这一句之外,**全程只用公开面**
 * (`Subscribe` / `Ticket::Wait` / `Send`)—— 真正要演示的是那一部分。
 *
 * @note `ITransport` 是**内部传输契约、非用户 API**(README「内部传输契约」)。宿主正常
 *       情况下只需创建、`Start()`、`Close()` / `WaitClosed()` 现成的四种传输;
 *       只有"库里没有这种介质/角色"时才需要像这样自己实现一个。
 *
 * ## 形态(刻意做到最小,只够示例用)
 *
 * - `Start()`:`listen(bind:port)` + spawn 一条 accept 循环 fiber。
 * - 每接受一条连接:spawn 一条读 fiber,把字节切片投进 `read_queue_`。
 *   **队列不随连接更换**,故客户端重连对上层节点透明(与 `TcpTransport` 同形)。
 * - `AsyncWrite()`:直接交给当前这条已连接的 socket,`peer` **被忽略**——TCP 点对点
 *   (与 `TcpTransport` 的 ADR-0011 **D8** 同形)。返回成功**不表示已发出**
 *   (fire-and-forget,ADR-0007 **D3**)。
 * - 只服务**最近一条**连接:新连接到来时旧连接的读 fiber 随其流终止而自然退出。
 *   示例是一对一形态,不做多路复用。
 *
 * 与库内各传输一致:面向**单线程 fiber 协作**,公开方法须在起它的那个执行域内调用。
 */

#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <QByteArray>

#include "await/awaitable.hpp"
#include "task/fibertask.h"

#include "transport/core/Endpoint.hpp"
#include "transport/core/TransportTypes.hpp"
#include "transport/io/ITransport.hpp"

class QTcpServer;
class QTcpSocket;

namespace example {

/// @brief 监听侧 TCP 字节管道(示例本地实现,见文件头)。
class ListeningTcpTransport final : public transport::ITransport {
 public:
  /// @param bind_addr 本地绑定地址(如 `0.0.0.0`)。
  /// @param port      监听端口;0 表示由 OS 分配(用 `LocalPort()` 取回)。
  ListeningTcpTransport(std::string bind_addr, std::uint16_t port);
  ~ListeningTcpTransport() override;

  ListeningTcpTransport(const ListeningTcpTransport&) = delete;
  ListeningTcpTransport& operator=(const ListeningTcpTransport&) = delete;

  /// @brief 监听并 spawn accept 循环。地址非法返 `kConfiguration`、监听失败返 `kConnection`
  ///        (成因同时落 `LastError()`)。
  Coro::Result<void> Start() override;
  /// @brief **只发信号,不等待收敛**:停监听、关读队列与当前读流。幂等。
  Coro::Result<void> Close() override;
  /// @brief join accept 循环与全部读 fiber;返回即可安全析构。
  void WaitClosed() override;

  [[nodiscard]] std::shared_ptr<Coro::Awaitable<transport::Datagram>> AsyncRead()
      override;
  [[nodiscard]] Coro::Result<void> AsyncWrite(transport::Datagram datagram) override;

  [[nodiscard]] std::error_code LastError() const override;
  [[nodiscard]] transport::LinkState CurrentLinkState() const override;

  /// @brief 实际监听端口(`port = 0` 时由 OS 分配后经此取回);未监听为 0。
  [[nodiscard]] std::uint16_t LocalPort() const;

 private:
  void RunAcceptLoop();
  void RunReadLoop(QTcpSocket* socket);

  std::string bind_addr_;
  std::uint16_t port_{0};

  transport::LifecycleState lifecycle_{transport::LifecycleState::kCreated};
  std::error_code last_error_;

  std::unique_ptr<QTcpServer> server_;
  QTcpSocket* socket_{nullptr};  ///< 最近一条已接受连接;归 `server_` 所有。

  /// 对外读队列:**不随连接更换**——这正是"重连对上层透明"的载体。
  std::shared_ptr<Coro::Awaitable<transport::Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<transport::Datagram>>()};
  std::shared_ptr<Coro::Awaitable<QTcpSocket*>> incoming_;
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream_;

  std::shared_ptr<Coro::FiberTask<void>> accept_task_;
  std::vector<std::shared_ptr<Coro::FiberTask<void>>> read_tasks_;
};

}  // namespace example
