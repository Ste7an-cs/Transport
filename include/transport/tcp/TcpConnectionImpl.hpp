#pragma once

// -----------------------------------------------------------------------------
// TcpConnectionImpl.hpp — 已连接 socket 的收发实现（ITransport）
// 继承 TransportBase；包装一个已连接的 asio tcp socket：async_read 循环经
// FrameAssembler 切帧 → DeliverFrame，写经 strand 串行化。客户端与服务端
// accepted 连接共用它。不拥有 io 线程（由所属 io_context 驱动）。
// -----------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include <asio.hpp>

#include "transport/IFramer.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportBase.hpp"
#include "transport/framing/FrameAssembler.hpp"

namespace transport {

// 已连接 socket 的收发循环：客户端连上后、服务端 accept 后均用它。
// io 由 socket 所属的 io_context 驱动（本类不拥有线程）。须以 shared_ptr 持有。
class TcpConnectionImpl : public TransportBase,
                      public std::enable_shared_from_this<TcpConnectionImpl> {
 public:
  TcpConnectionImpl(asio::ip::tcp::socket socket, std::shared_ptr<IFramer> framer);

  Status Open() override;   // 启动 async_read 循环
  void Close() override;    // 关闭 socket + CloseQueue()
  bool IsOpen() const override;
  Status Send(const std::vector<uint8_t>& data) override;

  const std::string& PeerId() const { return peer_id_; }

 protected:
  // 断连处理。基类：投递错误 + 关队列 + 通知（accepted 连接为终态）。
  // 子类（client）覆盖以触发重连而不关队列。每个连接周期只生效一次。
  virtual void HandleDisconnect(const std::string& reason);

  void StartRead();   // 启动一次 async_read_some；子类（client）连接成功后调用

  asio::ip::tcp::socket socket_;
  asio::strand<asio::any_io_executor> strand_;
  std::atomic<bool> open_{false};

 private:
  void DoWrite();

  FrameAssembler assembler_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::string peer_id_;
  std::atomic<bool> disconnected_{false};
};

}  // namespace transport
