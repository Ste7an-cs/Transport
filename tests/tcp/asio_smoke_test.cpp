#include <asio/version.hpp>

#include <gtest/gtest.h>

TEST(Asio, VersionMacroPresent) {
  EXPECT_GT(ASIO_VERSION, 0);
}
