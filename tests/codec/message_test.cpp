#include "transport/core/Message.hpp"

#include <gtest/gtest.h>

using transport::Message;
using transport::MessageKind;

TEST(Message, DefaultsToOnewayEmptyCorrelation) {
  Message m;
  EXPECT_EQ(m.kind, MessageKind::kOneway);
  EXPECT_TRUE(m.correlation_id.empty());
  EXPECT_TRUE(m.payload.empty());
  EXPECT_TRUE(m.topic.empty());
}

TEST(Message, HoldsKindAndCorrelation) {
  Message m;
  m.kind = MessageKind::kRequest;
  m.correlation_id = "req-1";
  m.payload = {1, 2, 3};
  m.topic = "calc";
  EXPECT_EQ(m.kind, MessageKind::kRequest);
  EXPECT_EQ(m.correlation_id, "req-1");
  EXPECT_EQ(m.payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(m.topic, "calc");
}
