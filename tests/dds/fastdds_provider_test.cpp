#include "transport/dds/DdsTransport.hpp"

#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsTransport;
using transport::Endpoint;
using transport::Result;

namespace {
DdsConfig RealCfg(int domain) {
  DdsConfig c; c.provider = "fastdds"; c.domain_id = domain; return c;
}
}  // namespace

TEST(FastDdsIntegration, PubSubRoundtrip) {
  auto rx = std::make_shared<DdsTransport>(RealCfg(71));
  auto tx = std::make_shared<DdsTransport>(RealCfg(71));

  auto ro = rx->Open();
  auto to = tx->Open();
  if (!ro || !to) GTEST_SKIP() << "FastDDS participant unavailable";

  std::mutex m; std::vector<uint8_t> got; std::string from;
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f) {
    if (!r) return;
    std::lock_guard<std::mutex> lk(m); got = r.value; from = f;
  });
  ASSERT_TRUE(static_cast<bool>(rx->Subscribe("icalc")));

  bool received = false;
  for (int i = 0; i < 50 && !received; ++i) {
    ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3}, Endpoint::Topic("icalc"))));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::lock_guard<std::mutex> lk(m);
    received = !got.empty();
  }
  {
    std::lock_guard<std::mutex> lk(m);
    ASSERT_TRUE(received) << "no sample received within timeout";
    EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_EQ(from, "icalc");
  }
  tx->Close(); rx->Close();
}
