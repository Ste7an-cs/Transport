#include "transport/udp/UdpImpl.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/Endpoint.hpp"
#include "transport/ICodec.hpp"

using transport::Endpoint;
using transport::ICodec;
using transport::Result;
using transport::UdpConfig;
using transport::UdpImpl;
using transport::UdpMode;

namespace {

UdpConfig UnicastCfg(const std::string& remote_ip, uint16_t remote_port) {
  UdpConfig c;
  c.mode = UdpMode::kUnicast;
  c.local_addr = "127.0.0.1";
  c.local_port = 0;          // OS 分配
  c.remote_addr = remote_ip; // 可空（只收时）
  c.remote_port = remote_port;
  return c;
}

class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto o = d;
    for (auto& b : o) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto o = d;
    for (auto& b : o) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
};

}  // namespace

TEST(UdpTransport, UnicastSendReceive) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));  // 只收，无默认 remote
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", rport));
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_FALSE(r.value.source.empty());  // "127.0.0.1:<tx port>"
  tx->Close();
  rx->Close();
}

TEST(UdpTransport, EndpointNetOverridesDefault) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  auto tx = std::make_shared<UdpImpl>(UnicastCfg("", 0));  // 无默认 remote
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({4, 5}, Endpoint::Net("127.0.0.1", rport))));
  auto r = rx->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{4, 5}));
  tx->Close();
  rx->Close();
}

TEST(UdpTransport, CodecAppliedBothDirections) {
  auto rx = std::make_shared<UdpImpl>(UnicastCfg("", 0));
  rx->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  uint16_t rport = rx->LocalPort();

  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", rport));
  tx->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({1, 2, 3})));  // Encode +1 → 线上 {2,3,4}
  auto r = rx->Receive(1000);                            // Decode -1 → {1,2,3}
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  tx->Close();
  rx->Close();
}

TEST(UdpTransport, SendBeforeOpenFails) {
  auto tx = std::make_shared<UdpImpl>(UnicastCfg("127.0.0.1", 9999));
  auto st = tx->Send({1});
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(UdpTransport, MulticastLoopbackOrSkip) {
  UdpConfig c;
  c.mode = UdpMode::kMulticast;
  c.multicast_group = "239.255.0.1";
  c.local_port = 0;
  auto m = std::make_shared<UdpImpl>(c);
  auto opened = m->Open();
  if (!opened) GTEST_SKIP() << "multicast not supported: " << opened.error;

  uint16_t port = m->LocalPort();
  // 经 enable_loopback 发给自身的组播组:本端口
  auto sent = m->Send({7, 8, 9}, Endpoint::Net("239.255.0.1", port));
  if (!sent) {
    m->Close();
    GTEST_SKIP() << "multicast send failed: " << sent.error;
  }
  auto r = m->Receive(500);
  if (!r) {
    m->Close();
    GTEST_SKIP() << "multicast loopback not delivered in this environment";
  }
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{7, 8, 9}));
  m->Close();
}

TEST(UdpTransport, BroadcastLoopbackOrSkip) {
  // 接收端：普通单播绑 0.0.0.0:P
  UdpConfig rc;
  rc.mode = UdpMode::kUnicast;
  rc.local_addr = "0.0.0.0";
  rc.local_port = 0;
  auto rx = std::make_shared<UdpImpl>(rc);
  if (!rx->Open()) GTEST_SKIP() << "udp open failed";
  uint16_t port = rx->LocalPort();

  // 发送端：广播模式发 127.255.255.255:P
  UdpConfig bc;
  bc.mode = UdpMode::kBroadcast;
  bc.local_addr = "0.0.0.0";
  bc.local_port = 0;
  bc.remote_addr = "127.255.255.255";
  bc.remote_port = port;
  auto tx = std::make_shared<UdpImpl>(bc);
  auto opened = tx->Open();
  if (!opened) {
    rx->Close();
    GTEST_SKIP() << "broadcast not supported: " << opened.error;
  }
  auto sent = tx->Send({5, 5});
  if (!sent) {
    tx->Close();
    rx->Close();
    GTEST_SKIP() << "broadcast send failed: " << sent.error;
  }
  auto r = rx->Receive(500);
  if (!r) {
    tx->Close();
    rx->Close();
    GTEST_SKIP() << "broadcast not delivered in this environment";
  }
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{5, 5}));
  tx->Close();
  rx->Close();
}
