#include "transport/comm/ProtocolPolicy.hpp"

#include <string>
#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Message;
using transport::ProtocolPolicy;

namespace {
Message FromSource(const std::string& src) { Message m; m.source = src; return m; }
}  // namespace

TEST(ProtocolPolicy, ReplyToSourceWhenEnabled) {
  ProtocolPolicy p(1, /*reply_to_source=*/true);
  Endpoint e = p.ReplyTo(FromSource("192.168.1.5:7000"));
  EXPECT_EQ(e.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(e.host, "192.168.1.5");
  EXPECT_EQ(e.port, 7000);
}

TEST(ProtocolPolicy, ReplyDefaultWhenDisabled) {
  ProtocolPolicy p(1, /*reply_to_source=*/false);
  EXPECT_EQ(p.ReplyTo(FromSource("192.168.1.5:7000")).kind, Endpoint::Kind::kDefault);
}

TEST(ProtocolPolicy, ReplyDefaultOnMalformedSource) {
  ProtocolPolicy p(1, true);
  EXPECT_EQ(p.ReplyTo(FromSource("")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("nocolon")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("host:")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("host:abc")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("host:0")).kind, Endpoint::Kind::kDefault);
}

TEST(ProtocolPolicy, ReplyToIpv6LastColon) {
  ProtocolPolicy p(1, true);
  Endpoint e = p.ReplyTo(FromSource("::1:9000"));
  EXPECT_EQ(e.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(e.host, "::1");
  EXPECT_EQ(e.port, 9000);
}
