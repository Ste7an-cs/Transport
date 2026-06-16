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
