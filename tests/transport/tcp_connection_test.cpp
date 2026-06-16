#include "transport/tcp/TcpConnection.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpConnection;

namespace {
struct ConnectedPair {
  asio::ip::tcp::socket accepted;
  asio::ip::tcp::socket peer;
};
ConnectedPair MakeConnectedPair(asio::io_context& ctx, asio::io_context& pctx) {
  asio::ip::tcp::acceptor acc(
      ctx, asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
  const uint16_t port = acc.local_endpoint().port();
  asio::ip::tcp::socket peer(pctx);
  std::thread ct([&] {
    peer.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
  });
  asio::ip::tcp::socket accepted = acc.accept();
  ct.join();
  return {std::move(accepted), std::move(peer)};
}

struct Sink {
  std::mutex m; std::condition_variable cv; std::vector<uint8_t> acc; std::string from;
  void Wire(std::shared_ptr<TcpConnection> c) {
    c->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string& f) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      acc.insert(acc.end(), r.value.begin(), r.value.end()); from = f;
      cv.notify_all();
    });
  }
  bool WaitBytes(std::size_t n, int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [&] { return acc.size() >= n; });
  }
};
}  // namespace

TEST(TcpConnection, ReceivesBytesFromPeer) {
  asio::io_context ctx, pctx;
  auto pair = MakeConnectedPair(ctx, pctx);
  auto conn = std::make_shared<TcpConnection>(std::move(pair.accepted));
  Sink sink; sink.Wire(conn);
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  auto wg = asio::make_work_guard(ctx);
  std::thread io([&] { ctx.run(); });

  const uint8_t out[] = {1, 2, 3};
  asio::write(pair.peer, asio::buffer(out, 3));
  ASSERT_TRUE(sink.WaitBytes(3, 1000));
  EXPECT_EQ(sink.acc, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_NE(sink.from.find("127.0.0.1:"), std::string::npos);

  conn->Close();
  wg.reset(); ctx.stop(); io.join();
  pair.peer.close();
}

TEST(TcpConnection, SendWritesToPeer) {
  asio::io_context ctx, pctx;
  auto pair = MakeConnectedPair(ctx, pctx);
  auto conn = std::make_shared<TcpConnection>(std::move(pair.accepted));
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  auto wg = asio::make_work_guard(ctx);
  std::thread io([&] { ctx.run(); });

  ASSERT_TRUE(static_cast<bool>(conn->Send({7, 8, 9, 10})));
  std::vector<uint8_t> got(4);
  asio::read(pair.peer, asio::buffer(got));
  EXPECT_EQ(got, (std::vector<uint8_t>{7, 8, 9, 10}));

  conn->Close();
  wg.reset(); ctx.stop(); io.join();
  pair.peer.close();
}

TEST(TcpConnection, PeerCloseTriggersDisconnect) {
  asio::io_context ctx, pctx;
  auto pair = MakeConnectedPair(ctx, pctx);
  auto conn = std::make_shared<TcpConnection>(std::move(pair.accepted));
  std::mutex m; std::condition_variable cv; std::string reason;
  conn->OnDisconnect([&](const std::string& r) {
    std::lock_guard<std::mutex> lk(m); reason = r; cv.notify_all();
  });
  ASSERT_TRUE(static_cast<bool>(conn->Open()));
  auto wg = asio::make_work_guard(ctx);
  std::thread io([&] { ctx.run(); });

  pair.peer.close();
  {
    std::unique_lock<std::mutex> lk(m);
    ASSERT_TRUE(cv.wait_for(lk, std::chrono::milliseconds(1000), [&] { return !reason.empty(); }));
  }
  EXPECT_EQ(reason.rfind("conn:", 0), 0u);

  conn->Close();
  wg.reset(); ctx.stop(); io.join();
}
