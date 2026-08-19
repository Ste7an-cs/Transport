#include "detail/result.hpp"

#include <array>
#include <system_error>
#include <gtest/gtest.h>

#include "transport/core/Error.hpp"

using transport::TransportErrc;
using transport::make_error_code;

TEST(CoroResult, UsesAsyncTaskValueAndVoidResults) {
  Coro::Result<int> value{42};
  ASSERT_TRUE(value);
  EXPECT_EQ(value.value(), 42);

  Coro::Result<void> ok;
  EXPECT_TRUE(ok);

  Coro::Result<int> failed{make_error_code(TransportErrc::kIo)};
  ASSERT_FALSE(failed);
  EXPECT_EQ(failed.error(), make_error_code(TransportErrc::kIo));
}

TEST(CoroError, AllRequiredErrorsAreStableAndDiagnostic) {
  constexpr std::array<TransportErrc, 13> errors{
      TransportErrc::kInvalidArgument, TransportErrc::kInvalidState,
      TransportErrc::kConfiguration, TransportErrc::kConnection,
      TransportErrc::kClosed, TransportErrc::kTimeout,
      TransportErrc::kCancelled, TransportErrc::kIo,
      TransportErrc::kFrame, TransportErrc::kCodec,
      TransportErrc::kResourceExhausted, TransportErrc::kUnsupported,
      TransportErrc::kInternal};

  int expected = 1;
  for (const auto error : errors) {
    const std::error_code code = make_error_code(error);
    EXPECT_STREQ(code.category().name(), "transport.error");
    EXPECT_EQ(code.value(), expected++);
    EXPECT_FALSE(code.message().empty());
  }
}

TEST(CoroError, SupportsStandardErrorCodeConstruction) {
  const std::error_code code = TransportErrc::kClosed;
  EXPECT_EQ(code, make_error_code(TransportErrc::kClosed));
}
