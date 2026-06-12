#pragma once

// -----------------------------------------------------------------------------
// UdpImpl.hpp — UDP 传输实现（单类处理单播/组播/广播）
// 实现 ITransport(Endpoint::Net 运行期寻址)；组合 TransportCore（无 framer，每个 datagram 即一条 Message）。
// 自有 io_context + 1 后台 io 线程；Open() 按 mode 配置 socket。须以 shared_ptr 持有。
// -----------------------------------------------------------------------------

#include <array>
#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/TransportCore.hpp"
#include "transport/udp/UdpConfig.hpp"

namespace transport {

class UdpImpl : public ITransport,
                public std::enable_shared_from_this<UdpImpl> {
 public:
  explicit UdpImpl(UdpConfig config);
  ~UdpImpl() override;

  Status Open() override;   // 按 mode 配置 socket，启动接收循环
  void Close() override;
  bool IsOpen() const override;
  using ITransport::Send;
  Status Send(const std::vector<uint8_t>& data) override;             // 默认目的地
  Status Send(const std::vector<uint8_t>& data,
              const Endpoint& to) override;                           // kNet 运行期目的地

  // 接收侧：转发给 core_
  Result<Message> Receive(uint32_t timeout_ms) override { return core_.Receive(timeout_ms); }
  void OnReceive(ReceiveCallback cb) override { core_.OnReceive(std::move(cb)); }
  std::future<Result<Message>> AsyncReceive() override { return core_.AsyncReceive(); }
  void OnDisconnect(DisconnectCallback cb) override { core_.OnDisconnect(std::move(cb)); }
  void SetCodec(std::shared_ptr<ICodec> codec) override { core_.SetCodec(std::move(codec)); }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void StartReceive();
  Status SendToEndpoint(const std::vector<uint8_t>& data,
                        const asio::ip::udp::endpoint& dest);

  UdpConfig config_;
  TransportCore core_;
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
};

}  // namespace transport
