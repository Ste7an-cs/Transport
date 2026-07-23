#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/coro/ITransport.hpp"

class QAbstractSocket;

namespace transport::coro {

// 协程原生 TCP 传输——只覆盖已建立连接上的收发语义。
//
// 发送路径遵守 ITransport 契约的可观察发送完成语义:
//   * RT_TRANSPORT_008 一次发送在帧字节全部离开框架用户态发送缓冲、进入操作系统
//     发送缓冲后才报告成功;背压经协程 await 自然传导回发起方,不 fire-and-forget。
//   * RT_TRANSPORT_007 同一 fiber 的先后发送按程序序上线;跨 fiber 并发发送被
//     串行化为一致全序。
//   * RT_TRANSPORT_004 同一实例同一时刻至多一个有效写;违反返回 InvalidState,
//     单帧字节不与另一帧交错。
//   * RT_TRANSPORT_004.4 流式部分写失败返回 Io/Connection 并关闭本物理连接,
//     不自动重发残缺帧。
//   * RT_REQUEST_004.4 写入已开始后被取消/超时,底层尽力把帧写完(健康连接不
//     截断);取消/超时的本地返回码由发起方(请求层)裁决,不由本类截断连接。
//
// 连接建立、自动重连、运行时重配置不在本类职责:构造时接管一个已连接的
// QAbstractSocket。客户端与服务端已接受连接共享本实现。不向用户暴露 corosocket。
class TcpTransport final : public ITransport {
 public:
  using Clock = OperationOptions::Clock;

  // 接管一个已建立连接的 socket(须与调用方处于同一执行域/线程)。
  explicit TcpTransport(QAbstractSocket* connected_socket);
  ~TcpTransport() override;

  TcpTransport(const TcpTransport&) = delete;
  TcpTransport& operator=(const TcpTransport&) = delete;

  Status Start() override;
  Result<Datagram> Read(OperationOptions options = {}) override;
  Status Write(SendUnit unit) override;
  Status RequestClose() override;
  Status WaitClosed(OperationOptions options = {}) override;

  // 发送侧可观测——I/O 事实,非"连接健康"裁决(判活留给协议层)。
  std::size_t SendWaiterDepth() const;
  std::optional<Clock::time_point> LastSendTime() const;
  std::optional<Clock::time_point> LastReceiveTime() const;
  std::error_code LastError() const;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport::coro
