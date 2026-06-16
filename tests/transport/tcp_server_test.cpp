#include "transport/tcp/TcpServerTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::ITransport;
using transport::Result;
using transport::TcpServerConfig;
using transport::TcpServerTransport;

namespace {
struct RawClient {
  asio::io_context ctx;
  asio::ip::tcp::socket sock{ctx};
  void Connect(uint16_t port) {
    sock.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
  }
  void Write(const std::vector<uint8_t>& d) { asio::write(sock, asio::buffer(d)); }
  std::vector<uint8_t> Read(std::size_t n) {
    std::vector<uint8_t> b(n); asio::read(sock, asio::buffer(b)); return b;
  }
};

void WireEcho(std::shared_ptr<ITransport> conn) {
  auto weak = std::weak_ptr<ITransport>(conn);
  conn->OnBytes([weak](Result<std::vector<uint8_t>> r, const std::string&) {
    if (!r) return;
    if (auto c = weak.lock()) c->Send(r.value);
  });
}
}  // namespace

TEST(TcpServerTransport, SingleClientEcho) {
  auto srv = std::make_shared<TcpServerTransport>(TcpServerConfig{"127.0.0.1", 0, 0});
  srv->OnAccept([](std::shared_ptr<ITransport> conn) { WireEcho(conn); });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  const uint16_t port = srv->LocalPort();
  ASSERT_GT(port, 0);

  RawClient cli; cli.Connect(port);
  cli.Write({10, 20, 30});
  EXPECT_EQ(cli.Read(3), (std::vector<uint8_t>{10, 20, 30}));

  cli.sock.close();
  srv->Close();
}

TEST(TcpServerTransport, TwoClientsIndependent) {
  auto srv = std::make_shared<TcpServerTransport>(TcpServerConfig{"127.0.0.1", 0, 0});
  srv->OnAccept([](std::shared_ptr<ITransport> conn) { WireEcho(conn); });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  const uint16_t port = srv->LocalPort();

  RawClient a, b; a.Connect(port); b.Connect(port);
  a.Write({1}); b.Write({2});
  EXPECT_EQ(a.Read(1), (std::vector<uint8_t>{1}));
  EXPECT_EQ(b.Read(1), (std::vector<uint8_t>{2}));

  a.sock.close(); b.sock.close();
  srv->Close();
}

TEST(TcpServerTransport, CloseTearsDownConnections) {
  auto srv = std::make_shared<TcpServerTransport>(TcpServerConfig{"127.0.0.1", 0, 0});
  srv->OnAccept([](std::shared_ptr<ITransport> conn) { WireEcho(conn); });
  ASSERT_TRUE(static_cast<bool>(srv->Open()));
  const uint16_t port = srv->LocalPort();

  RawClient cli; cli.Connect(port);
  cli.Write({5});
  EXPECT_EQ(cli.Read(1), (std::vector<uint8_t>{5}));

  srv->Close();

  asio::error_code ec;
  uint8_t tmp[1];
  std::size_t n = asio::read(cli.sock, asio::buffer(tmp, 1), ec);
  EXPECT_EQ(n, 0u);
  EXPECT_TRUE(ec == asio::error::eof || ec == asio::error::connection_reset || (bool)ec);
}
