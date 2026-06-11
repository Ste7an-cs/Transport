#include "transport/serial/SerialConfig.hpp"

#include <gtest/gtest.h>

TEST(SerialConfig, Defaults) {
  transport::SerialConfig c;
  EXPECT_EQ(c.baud_rate, 115200u);
  EXPECT_EQ(static_cast<int>(c.data_bits), 8);
  EXPECT_EQ(static_cast<int>(c.stop_bits), 1);
  EXPECT_EQ(c.parity, 'N');
  EXPECT_FALSE(c.framer.has_value());
}
