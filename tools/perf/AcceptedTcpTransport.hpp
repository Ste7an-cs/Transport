#pragma once

/**
 * @file AcceptedTcpTransport.hpp
 * @brief **工具自带**的 TCP 服务端字节管道——`ITransport` 的一个本地实现。
 *
 * ## 为什么它在这里而不是在库里
 *
 * 库里的 `TcpTransport` 是**客户端**传输(ADR-0011 D1/D2:连出去 + 内建透明重连),
 * `TcpServer` 本轮不做(ADR-0011 **D10**,其 `.cpp` 不在任一构建的源清单里)。而
 * ADR-0018 的双机形态要求 TCP 的**服务端角色**也挂一个真 `ProtocolNode`,节点又只接
 * `ITransport&`——于是缺一个"监听 + 接受 + 收发字节"的实现。
 *
 * **它是工具代码,不是库代码**:不进 `libtransport.a`、不进公共头、不改任何既有 API。
 * 这与 **D10**(不为测量给库加计数 / 钩子 / 观测接口)不冲突——本类既不观测库、也不
 * 要求库交出任何内部事实,它只是把一条已接受的 socket 包成字节管道。
 *
 * ## 形态
 *
 * - `Start()`:`listen(bind:port)` + spawn 一条 accept 循环 fiber。
 * - 每接受一条连接:spawn 一条读 fiber,把 `readAll()` 流的字节切片投进 `read_queue`。
 *   **队列不随连接更换**,故客户端重连对上层节点透明(与 `TcpTransport` 同形)。
 * - `AsyncWrite()`:直接交给当前已连接的 socket(Qt 内部写缓冲即"入队"),`peer` 被
 *   忽略——TCP 点对点。返回成功**不表示已发出**,与 ADR-0007 **D3** 的写侧语义一致。
 * - 只服务**最近一条**连接:新连接到来时旧连接的读 fiber 自然随其流终止退出。性能
 *   基准是一对一形态,不需要多路复用。
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

namespace perf {

/// @brief 监听侧 TCP 字节管道(工具本地实现,见文件头)。
class AcceptedTcpTransport final : public transport::ITransport {
 public:
  /// @param bind_addr 本地绑定地址(如 `0.0.0.0`)。
  /// @param port      监听端口;0 表示由 OS 分配(用 `LocalPort()` 取回)。
  AcceptedTcpTransport(std::string bind_addr, std::uint16_t port);
  ~AcceptedTcpTransport() override;

  AcceptedTcpTransport(const AcceptedTcpTransport&) = delete;
  AcceptedTcpTransport& operator=(const AcceptedTcpTransport&) = delete;

  /// @brief 监听并 spawn accept 循环。监听失败返 `kConfiguration`(见 `LastError()`)。
  Coro::Result<void> Start() override;
  /// @brief 只发信号:停监听、关读队列与各读流。幂等。
  Coro::Result<void> Close() override;
  /// @brief join accept 循环与全部读 fiber。
  void WaitClosed() override;

  [[nodiscard]] std::shared_ptr<Coro::Awaitable<transport::Datagram>> AsyncRead()
      override;
  [[nodiscard]] Coro::Result<void> AsyncWrite(transport::Datagram datagram)
      override;

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

  std::shared_ptr<Coro::Awaitable<transport::Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<transport::Datagram>>()};
  std::shared_ptr<Coro::Awaitable<QTcpSocket*>> incoming_;
  std::shared_ptr<Coro::Awaitable<QByteArray>> read_stream_;

  std::shared_ptr<Coro::FiberTask<void>> accept_task_;
  std::vector<std::shared_ptr<Coro::FiberTask<void>>> read_tasks_;
};

}  // namespace perf
