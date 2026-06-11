#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsProvider.hpp"
#include "transport/dds/IDdsTransport.hpp"
#include "transport/dds/RawMessage.hpp"

#include <type_traits>

#include <gtest/gtest.h>

TEST(DdsConfig, Defaults) {
  transport::DdsConfig c;
  EXPECT_TRUE(c.mode == transport::DdsMode::kPubSub);
  EXPECT_TRUE(c.topics.empty());
  EXPECT_EQ(c.domain_id, 0);
  EXPECT_TRUE(c.qos.reliability == transport::DdsQos::Reliability::kReliable);
  EXPECT_TRUE(c.qos.durability == transport::DdsQos::Durability::kVolatile);
  EXPECT_EQ(c.qos.history_depth, 10u);
  EXPECT_EQ(c.provider, "FastDDS");
}

TEST(DdsConfig, RawMessageDefaults) {
  transport::RawMessage m;
  EXPECT_TRUE(m.request_id.empty());
  EXPECT_TRUE(m.reply_topic.empty());
  EXPECT_TRUE(m.payload.empty());
}

TEST(DdsConfig, IDdsTransportIsTransport) {
  EXPECT_TRUE((std::is_base_of<transport::ITransport,
                               transport::IDdsTransport>::value));
}
