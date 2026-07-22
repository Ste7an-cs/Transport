#pragma once

#include <system_error>

namespace transport::coro {

enum class TransportErrc {
  kInvalidArgument = 1,
  kInvalidState,
  kConfiguration,
  kConnection,
  kClosed,
  kTimeout,
  kCancelled,
  kIo,
  kFrame,
  kCodec,
  kResourceExhausted,
  kUnsupported,
  kInternal,
};

const std::error_category& transport_error_category() noexcept;
std::error_code make_error_code(TransportErrc error) noexcept;

}  // namespace transport::coro

namespace std {
template <>
struct is_error_code_enum<transport::coro::TransportErrc> : true_type {};
}  // namespace std
