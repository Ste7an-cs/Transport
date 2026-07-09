#include "transport/udp/UdpTransport.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Result;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;
using qtutil::pumpUntil; using qtutil::B;

TEST(UdpTransport, UnicastLoopbackRoundtrip) {
  UdpConfig ca; ca.mode = UdpMode::kUnicast; ca.local_addr = "127.0.0.1"; ca.local_port = 0;
  auto a = std::make_shared<UdpTransport>(ca);
  std::vector<uint8_t> got; std::string from;
  a->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string& f){ if(r){ got = r.value; from = f; } });
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  uint16_t aport = a->LocalPort();

  UdpConfig cb; cb.mode = UdpMode::kUnicast; cb.local_addr = "127.0.0.1"; cb.local_port = 0;
  cb.remote_addr = "127.0.0.1"; cb.remote_port = aport;
  auto b = std::make_shared<UdpTransport>(cb);
  ASSERT_TRUE(static_cast<bool>(b->Open()));
  ASSERT_TRUE(static_cast<bool>(b->Send(B({1, 2, 3}))));

  EXPECT_TRUE(pumpUntil([&]{ return !got.empty(); }));
  EXPECT_EQ(got, B({1, 2, 3}));
  EXPECT_NE(from.find("127.0.0.1:"), std::string::npos);
  a->Close(); b->Close();
}

TEST(UdpTransport, TopicEndpointRejected) {
  UdpConfig c; c.local_addr = "127.0.0.1"; c.local_port = 0;
  auto a = std::make_shared<UdpTransport>(c);
  ASSERT_TRUE(static_cast<bool>(a->Open()));
  auto st = a->Send(B({9}), Endpoint::Topic("t"));
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_NE(st.error.find("config:"), std::string::npos);
  a->Close();
}

// 广播冒烟:kBroadcast 模式绑定成功,且向广播地址 Send 成功。
// 若底层未启用 SO_BROADCAST,向 255.255.255.255 发送会以 EACCES 失败;
// Send 成功即证明广播发送路径可用(不断言投递——广播投递依赖网络,不在回环上验证)。
TEST(UdpTransport, BroadcastSendSucceeds) {
  UdpConfig c; c.mode = UdpMode::kBroadcast; c.local_addr = "0.0.0.0"; c.local_port = 0;
  c.remote_addr = "255.255.255.255"; c.remote_port = 45454;
  auto s = std::make_shared<UdpTransport>(c);
  ASSERT_TRUE(static_cast<bool>(s->Open()));
  EXPECT_TRUE(static_cast<bool>(s->Send(B({0x01}))));
  s->Close();
}
