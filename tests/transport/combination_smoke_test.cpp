#include "transport/codec/DdsCodec.hpp"
#include "transport/udp/UdpTransport.hpp"
#include "qt_test_util.hpp"

#include <memory>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Message;
using transport::MessageKind;
using transport::Result;
using transport::DdsCodec;
using transport::UdpConfig;
using transport::UdpTransport;
using qtutil::pumpUntil;

TEST(CombinationSmoke, UdpBytesThroughSystemCodecRoundtrip) {
  auto rx_codec = std::make_shared<DdsCodec>();
  std::vector<Message> got;

  UdpConfig rxc; rxc.local_addr = "127.0.0.1"; rxc.local_port = 0;
  auto rx = std::make_shared<UdpTransport>(rxc);
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&) {
    if (!r) return;
    auto msgs = rx_codec->Decode(r.value.data(), r.value.size());
    if (!msgs) return;
    for (auto& mm : msgs.value) got.push_back(std::move(mm));
  });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  const uint16_t rx_port = rx->LocalPort();

  DdsCodec tx_codec;
  Message out; out.kind = MessageKind::kRequest; out.correlation_id = "x-1";
  out.topic = "calc"; out.payload = {4, 5, 6};
  auto bytes = tx_codec.Encode(out);
  ASSERT_TRUE(static_cast<bool>(bytes));

  UdpConfig txc; txc.local_addr = "127.0.0.1"; txc.local_port = 0;
  auto tx = std::make_shared<UdpTransport>(txc);
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(
      tx->Send(bytes.value, Endpoint::Net("127.0.0.1", rx_port))));

  ASSERT_TRUE(pumpUntil([&] { return !got.empty(); }));
  ASSERT_EQ(got.size(), 1u);
  EXPECT_EQ(got[0].kind, MessageKind::kRequest);
  EXPECT_EQ(got[0].correlation_id, "x-1");
  EXPECT_EQ(got[0].payload, (std::vector<uint8_t>{4, 5, 6}));

  tx->Close(); rx->Close();
}
