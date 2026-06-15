#include "transport/serial/SerialTransport.hpp"

#include <fcntl.h>
#include <pty.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gtest/gtest.h>

using transport::Result;
using transport::SerialConfig;
using transport::SerialTransport;

namespace {
struct Sink {
  std::mutex m; std::condition_variable cv; std::vector<uint8_t> acc;
  void Wire(std::shared_ptr<SerialTransport> t) {
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

TEST(SerialTransport, OpenSendReceiveOverPty) {
  int master = -1, slave = -1;
  ASSERT_EQ(openpty(&master, &slave, nullptr, nullptr, nullptr), 0);
  char slave_name[256];
  ASSERT_EQ(ttyname_r(slave, slave_name, sizeof(slave_name)), 0);

  SerialConfig cfg; cfg.device = slave_name; cfg.baud_rate = 115200;
  auto t = std::make_shared<SerialTransport>(cfg);
  Sink sink; sink.Wire(t);
  ASSERT_TRUE(static_cast<bool>(t->Open()));

  const uint8_t out[] = {0x41, 0x42, 0x43};
  ASSERT_EQ(write(master, out, 3), 3);
  ASSERT_TRUE(sink.WaitBytes(3, 1000));
  EXPECT_EQ(sink.acc, (std::vector<uint8_t>{0x41, 0x42, 0x43}));

  ASSERT_TRUE(static_cast<bool>(t->Send({0x31, 0x32})));
  uint8_t in[2] = {0, 0};
  ASSERT_EQ(read(master, in, 2), 2);
  EXPECT_EQ(in[0], 0x31); EXPECT_EQ(in[1], 0x32);

  t->Close();
  close(master);
}
