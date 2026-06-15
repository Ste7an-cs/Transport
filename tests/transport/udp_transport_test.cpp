#include "transport/udp/UdpTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Result;
using transport::UdpConfig;
using transport::UdpTransport;

namespace {
struct Sink {
  std::mutex m; std::condition_variable cv;
  std::vector<uint8_t> last; std::string from; bool got = false;
  void Wire(std::shared_ptr<UdpTransport> t) {
    t->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string& f) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      last = r.value; from = f; got = true; cv.notify_all();
    });
  }
  bool Wait(int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [this] { return got; });
  }
};
}  // namespace

TEST(UdpTransport, UnicastLoopbackDelivery) {
  UdpConfig rxc; rxc.mode = transport::UdpMode::kUnicast;
  rxc.local_addr = "127.0.0.1"; rxc.local_port = 0;
  auto rx = std::make_shared<UdpTransport>(rxc);
  Sink sink; sink.Wire(rx);
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  const uint16_t rx_port = rx->LocalPort();
  ASSERT_GT(rx_port, 0);

  UdpConfig txc; txc.mode = transport::UdpMode::kUnicast;
  txc.local_addr = "127.0.0.1"; txc.local_port = 0;
  auto tx = std::make_shared<UdpTransport>(txc);
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(
      tx->Send({1, 2, 3}, Endpoint::Net("127.0.0.1", rx_port))));
  ASSERT_TRUE(sink.Wait(1000));
  EXPECT_EQ(sink.last, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_NE(sink.from.find("127.0.0.1:"), std::string::npos);
  tx->Close(); rx->Close();
}

TEST(UdpTransport, DefaultDestUsesRemoteConfig) {
  UdpConfig rxc; rxc.local_addr = "127.0.0.1"; rxc.local_port = 0;
  auto rx = std::make_shared<UdpTransport>(rxc);
  Sink sink; sink.Wire(rx);
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  const uint16_t rx_port = rx->LocalPort();

  UdpConfig txc; txc.local_addr = "127.0.0.1"; txc.local_port = 0;
  txc.remote_addr = "127.0.0.1"; txc.remote_port = rx_port;
  auto tx = std::make_shared<UdpTransport>(txc);
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({9})));
  ASSERT_TRUE(sink.Wait(1000));
  EXPECT_EQ(sink.last, (std::vector<uint8_t>{9}));
  tx->Close(); rx->Close();
}
