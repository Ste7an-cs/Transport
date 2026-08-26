#pragma once

#include <system_error>

namespace transport {

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
  /// 对端**始终没有受理**:重发次数耗尽仍未收到受理帧(ADR-0010 D12)。与 kTimeout 相区别
  /// ——后者的语义是"已受理但没出结果"。**须追加在末尾**:插入中间会改动既有枚举值。
  kNotAccepted,
};

const std::error_category& transport_error_category() noexcept;
std::error_code make_error_code(TransportErrc error) noexcept;

}  // namespace transport

namespace std {
template <>
struct is_error_code_enum<transport::TransportErrc> : true_type {};
}  // namespace std
