#include "transport/tcp/TcpClientTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientTransport;

namespace {

// 一个极简回环 echo/accept 服务器，仅用于测试客户端
class MiniServer {
 public:
  MiniServer()
      : acceptor_(ctx_, asio::ip::tcp::endpoint(
                            asio::ip::make_address("127.0.0.1"), 0)),
        guard_(ctx_.get_executor()) {
    port_ = acceptor_.local_endpoint().port();
    DoAccept();
    th_ = std::thread([this] { ctx_.run(); });
  }
  ~MiniServer() { Stop(); }

  uint16_t port() const { return port_; }

  // 向最近接受的连接写字节
  void WriteToPeer(const std::vector<uint8_t>& d) {
    std::unique_lock<std::mutex> lk(m_);
    // 等待 accept 完成：客户端 connect 可能先于服务端 accept handler 返回。
    cv_.wait_for(lk, std::chrono::seconds(2),
                 [this] { return peer_ != nullptr; });
    if (peer_ && peer_->is_open()) asio::write(*peer_, asio::buffer(d));
  }

  void DropPeer() {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait_for(lk, std::chrono::seconds(2),
                 [this] { return peer_ != nullptr; });
    if (peer_) { asio::error_code ec; peer_->close(ec); peer_.reset(); }
  }

  void Stop() {
    if (stopped_) return;
    stopped_ = true;
    asio::post(ctx_, [this] {
      asio::error_code ec;
      acceptor_.close(ec);
      if (peer_) { peer_->close(ec); peer_.reset(); }
    });
    guard_.reset();
    ctx_.stop();
    if (th_.joinable()) th_.join();
  }

 private:
  void DoAccept() {
    acceptor_.async_accept([this](asio::error_code ec, asio::ip::tcp::socket s) {
      if (ec) return;
      {
        std::lock_guard<std::mutex> lk(m_);
        peer_ = std::make_unique<asio::ip::tcp::socket>(std::move(s));
      }
      cv_.notify_all();
      DoAccept();
    });
  }

  asio::io_context ctx_;
  asio::ip::tcp::acceptor acceptor_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  std::unique_ptr<asio::ip::tcp::socket> peer_;
  std::mutex m_;
  std::condition_variable cv_;
  std::thread th_;
  uint16_t port_ = 0;
  bool stopped_ = false;
};

TcpClientConfig ClientCfg(uint16_t port, bool reconnect) {
  TcpClientConfig c;
  c.host = "127.0.0.1";
  c.port = port;
  c.connect_timeout_ms = 1000;
  c.auto_reconnect = reconnect;
  return c;
}

}  // namespace

TEST(TcpClient, ConnectAndReceive) {
  MiniServer server;
  // 退避参数无关紧要；用默认
  auto client = std::make_shared<TcpClientTransport>(ClientCfg(server.port(), true));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  server.WriteToPeer({7, 8, 9});
  auto r = client->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7, 8, 9}));
  client->Close();
}

TEST(TcpClient, ConnectRefusedReturnsError) {
  // 连接一个没有监听者的端口
  auto client = std::make_shared<TcpClientTransport>(ClientCfg(1, false));
  client->SetHost("127.0.0.1");
  auto st = client->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  // ECONNREFUSED → conn:，或极少数环境超时 → timeout:
  bool prefixed = st.error.rfind("conn:", 0) == 0 || st.error.rfind("timeout:", 0) == 0;
  EXPECT_TRUE(prefixed);
  client->Close();
}

TEST(TcpClient, AutoReconnectResumesAfterServerDrop) {
  MiniServer server;
  uint16_t port = server.port();
  // 用小退避基数（10ms）+ 小封顶（100ms）避免久等
  auto client = std::make_shared<TcpClientTransport>(
      ClientCfg(port, true), std::chrono::milliseconds(10),
      std::chrono::milliseconds(100));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  int drops = 0;
  client->OnDisconnect([&](const std::string&) { ++drops; });

  server.DropPeer();  // 触发掉线 + 重连
  // 等待重连：MiniServer 仍在 accept，client 会重新连上
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  EXPECT_GE(drops, 1);

  // 重连后应能继续收数据
  server.WriteToPeer({1, 2, 3});
  auto r = client->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  client->Close();
}

TEST(TcpClient, NoReconnectWhenDisabled) {
  MiniServer server;
  auto client = std::make_shared<TcpClientTransport>(ClientCfg(server.port(), false));
  ASSERT_TRUE(static_cast<bool>(client->Open()));

  bool disconnected = false;
  client->OnDisconnect([&](const std::string&) { disconnected = true; });

  server.DropPeer();
  auto r = client->Receive(1000);  // 掉线 → conn: 错误
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
  for (int i = 0; i < 100 && !disconnected; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_TRUE(disconnected);
  client->Close();
}
