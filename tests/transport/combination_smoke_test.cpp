#include "transport/udp/UdpTransport.hpp"
#include "transport/codec/SystemDatagramCodec.hpp"
#include "qt_test_util.hpp"
#include <memory>
#include <gtest/gtest.h>

using transport::Endpoint;
using transport::FrameType;
using transport::Message;
using transport::Result;
using transport::SystemDatagramCodec;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;
using qtutil::pumpUntil;

// 冒烟:UDP(QtNetwork)+ SystemDatagramCodec 一条帧 encode→send→recv→decode 往返。
TEST(CombinationSmoke, UdpSystemDatagramRoundtrip) {
  UdpConfig ra; ra.mode = UdpMode::kUnicast; ra.local_addr = "127.0.0.1"; ra.local_port = 0;
  auto recv = std::make_shared<UdpTransport>(ra);
  SystemDatagramCodec codec;
  std::vector<Message> decoded;
  recv->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){
    if (r) { auto m = codec.Decode(r.value.data(), r.value.size()); if (m) decoded = m.value; }
  });
  ASSERT_TRUE(static_cast<bool>(recv->Open()));

  UdpConfig sa; sa.mode = UdpMode::kUnicast; sa.local_addr = "127.0.0.1"; sa.local_port = 0;
  sa.remote_addr = "127.0.0.1"; sa.remote_port = recv->LocalPort();
  auto send = std::make_shared<UdpTransport>(sa);
  ASSERT_TRUE(static_cast<bool>(send->Open()));

  Message m; m.frm_type = FrameType::kCommand; m.protocol_id = 1; m.session_id = 5;
  m.message_id = 0x0042; m.payload = {0xAA, 0xBB};
  auto bytes = codec.Encode(m);
  ASSERT_TRUE(static_cast<bool>(bytes));
  ASSERT_TRUE(static_cast<bool>(send->Send(bytes.value)));

  EXPECT_TRUE(pumpUntil([&]{ return !decoded.empty(); }));
  ASSERT_EQ(decoded.size(), 1u);
  EXPECT_EQ(decoded[0].message_id, 0x0042);
  EXPECT_EQ(decoded[0].payload, (std::vector<uint8_t>{0xAA, 0xBB}));
  recv->Close(); send->Close();
}
