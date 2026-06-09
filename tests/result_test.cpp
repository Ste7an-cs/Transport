#include "transport/Result.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::Result;
using transport::Status;

TEST(Result, SuccessHoldsValueAndIsTruthy) {
  auto r = Result<int>::Success(42);
  EXPECT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.ok);
  EXPECT_EQ(r.value, 42);
  EXPECT_TRUE(r.error.empty());
}

TEST(Result, FailIsFalsyAndCarriesMessage) {
  auto r = Result<std::vector<uint8_t>>::Fail("io: boom");
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error, "io: boom");
  EXPECT_TRUE(r.value.empty());  // 默认构造的 value
}

TEST(Result, StatusSuccessAndFail) {
  Status ok = Status::Success(std::monostate{});
  EXPECT_TRUE(static_cast<bool>(ok));
  Status bad = Status::Fail("config: nope");
  EXPECT_FALSE(static_cast<bool>(bad));
  EXPECT_EQ(bad.error, "config: nope");
}
