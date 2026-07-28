#include "transport/Error.hpp"

#include <string>

namespace transport {
namespace {

class TransportErrorCategory final : public std::error_category {
 public:
  const char* name() const noexcept override { return "transport.error"; }

  std::string message(int value) const override {
    switch (static_cast<TransportErrc>(value)) {
      case TransportErrc::kInvalidArgument: return "invalid argument";
      case TransportErrc::kInvalidState: return "invalid state";
      case TransportErrc::kConfiguration: return "configuration error";
      case TransportErrc::kConnection: return "connection error";
      case TransportErrc::kClosed: return "closed";
      case TransportErrc::kTimeout: return "operation timed out";
      case TransportErrc::kCancelled: return "operation cancelled";
      case TransportErrc::kIo: return "I/O error";
      case TransportErrc::kFrame: return "frame error";
      case TransportErrc::kCodec: return "codec error";
      case TransportErrc::kResourceExhausted: return "resource exhausted";
      case TransportErrc::kUnsupported: return "unsupported operation";
      case TransportErrc::kInternal: return "internal error";
    }
    return "unknown transport error";
  }
};

}  // namespace

const std::error_category& transport_error_category() noexcept {
  static const TransportErrorCategory category;
  return category;
}

std::error_code make_error_code(TransportErrc error) noexcept {
  return {static_cast<int>(error), transport_error_category()};
}

}  // namespace transport
