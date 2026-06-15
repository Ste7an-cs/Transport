#include "transport/tcp/TcpClientTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientTransport;

namespace {
struct EchoServer {
  asio::io_context ctx;
  asio::ip::tcp::acceptor acc{ctx};
  std::thread th;
  asio::ip::tcp::socket sock{ctx};
  std::array<uint8_t, 1024> buf{};
  uint16_t Start() {
    acc.open(asio::ip::tcp::v4());
    acc.bind(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    acc.listen();
    uint16_t port = acc.local_endpoint().port();
    acc.async_accept(sock, [this](asio::error_code ec) {
      if (ec) return;
      DoRead();
    });
    th = std::thread([this] { ctx.run(); });
    return port;
  }
  void DoRead() {
    sock.async_read_some(asio::buffer(buf), [this](asio::error_code ec, std::size_t n) {
      if (ec) return;
      asio::write(sock, asio::buffer(buf, n));
      DoRead();
    });
  }
  void Stop() { asio::post(ctx, [this] { ctx.stop(); }); if (th.joinable()) th.join(); }
};

struct Sink {
  std::mutex m; std::condition_variable cv;
  std::vector<uint8_t> acc; bool connected = false;
  void Wire(std::shared_ptr<TcpClientTransport> t) {
    t->OnConnect([this] { std::lock_guard<std::mutex> lk(m); connected = true; cv.notify_all(); });
    t->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string&) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      acc.insert(acc.end(), r.value.begin(), r.value.end());
      cv.notify_all();
    });
  }
  bool WaitBytes(std::size_t n, int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [&] { return acc.size() >= n; });
  }
};
}  // namespace

TEST(TcpClientTransport, ConnectSendEchoReceive) {
  EchoServer srv; uint16_t port = srv.Start();

  TcpClientConfig cfg; cfg.host = "127.0.0.1"; cfg.port = port;
  auto t = std::make_shared<TcpClientTransport>(cfg);
  Sink sink; sink.Wire(t);
  ASSERT_TRUE(static_cast<bool>(t->Open()));

  ASSERT_TRUE(static_cast<bool>(t->Send({10, 20, 30})));
  ASSERT_TRUE(sink.WaitBytes(3, 1000));
  EXPECT_EQ(sink.acc, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_TRUE(sink.connected);

  t->Close();
  srv.Stop();
}

TEST(TcpClientTransport, ConnectRefusedFails) {
  TcpClientConfig cfg; cfg.host = "127.0.0.1"; cfg.port = 1;
  cfg.connect_timeout_ms = 500;
  auto t = std::make_shared<TcpClientTransport>(cfg);
  auto st = t->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  t->Close();
}
