#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/ITcpServer.hpp"
#include "transport/tcp/TcpConnection.hpp"
#include "transport/tcp/TcpServerConfig.hpp"

namespace transport {

// TCP 服务端：自有 io_context + 1 线程；acceptor 每 accept 造一个共享该 io_context
// 的 TcpConnection。须以 shared_ptr 持有。
class TcpServerTransport
    : public ITcpServer,
      public std::enable_shared_from_this<TcpServerTransport> {
 public:
  explicit TcpServerTransport(TcpServerConfig config);
  ~TcpServerTransport() override;

  Status Open() override;
  void Close() override;
  bool IsOpen() const override;

  Status Send(const std::vector<uint8_t>& data) override;  // 广播

  // 服务端不适用的接收方法（主 spec §5.3）
  Result<Message> Receive(uint32_t timeout_ms) override;
  void OnReceive(ReceiveCallback cb) override;
  std::future<Result<Message>> AsyncReceive() override;
  void OnDisconnect(DisconnectCallback cb) override;
  void SetCodec(std::shared_ptr<ICodec> codec) override;

  // ITcpServer
  void OnNewConnection(ConnectionCallback cb) override;
  std::vector<std::shared_ptr<ITransport>> GetClients() const override;
  void DisconnectClient(const std::string& client_id) override;

  uint16_t LocalPort() const { return local_port_; }

 private:
  void DoAccept();
  void RemoveClient(const std::string& id);

  TcpServerConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::ip::tcp::acceptor acceptor_;
  std::thread io_thread_;

  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<TcpConnection>> clients_;
  ConnectionCallback connection_cb_;
  DisconnectCallback disconnect_cb_;
  std::shared_ptr<ICodec> codec_;

  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;
};

}  // namespace transport
