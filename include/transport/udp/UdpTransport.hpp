#pragma once

// UdpTransport.hpp — UDP 纯字节管道(单播/组播/广播)。改写自旧 UdpImpl:
// 去掉 TransportCore/codec/topic;收到的每个 datagram 经 OnBytes 交付裸字节 + from。

#include <array>
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
#include "transport/udp/UdpConfig.hpp"

namespace transport {

class UdpTransport : public ITransport,
                     public std::enable_shared_from_this<UdpTransport> {
 public:
  explicit UdpTransport(UdpConfig config);
  ~UdpTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override;

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void StartReceive();
  Result<asio::ip::udp::endpoint> ResolveDest(const Endpoint& to);
  Status SendRaw(std::vector<uint8_t> bytes, const asio::ip::udp::endpoint& dest);

  UdpConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::udp::socket socket_;
  asio::ip::udp::endpoint default_dest_;
  asio::ip::udp::endpoint recv_from_;
  std::array<uint8_t, 65536> recv_buf_;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
