#pragma once

// -----------------------------------------------------------------------------
// message_test_util.hpp — `Message::payload` 改为 `QByteArray` 之后(ADR-0020 D1)各用例
// 共用的小助手。
//
// Qt5 的 `QByteArray` **没有 `initializer_list` 构造**,故 `payload = {0x01, 0x02}` 这类
// 写法全仓失效。这里统一收口成 `Pay(...)`,不让每个用例各写各的。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <initializer_list>
#include <string>
#include <vector>

#include <QByteArray>

#include "transport/core/Message.hpp"

namespace testutil {

/// @brief 花括号字节 → payload:`Pay({0x01, 0x02})`。
inline QByteArray Pay(std::initializer_list<std::uint8_t> bytes) {
  QByteArray out;
  out.reserve(static_cast<int>(bytes.size()));
  for (std::uint8_t b : bytes) {
    out.append(static_cast<char>(b));
  }
  return out;
}

/// @brief `std::vector<std::uint8_t>` → payload(线缆字节与 payload 之间转手时用)。
inline QByteArray Pay(const std::vector<std::uint8_t>& bytes) {
  return QByteArray(reinterpret_cast<const char*>(bytes.data()),
                    static_cast<int>(bytes.size()));
}

/// @brief 文本 → payload(**不含**结尾 '\0')。
inline QByteArray Pay(const std::string& text) {
  return QByteArray(text.data(), static_cast<int>(text.size()));
}

/// @brief payload / frame → `std::vector<std::uint8_t>`,供与线缆字节逐字节比对。
inline std::vector<std::uint8_t> ToVec(const QByteArray& bytes) {
  const auto* p = reinterpret_cast<const std::uint8_t*>(bytes.constData());
  return std::vector<std::uint8_t>(p, p + bytes.size());
}

/// @brief payload → `std::string`(DDS 用例里 payload 装的就是文本)。
inline std::string ToText(const QByteArray& bytes) {
  return std::string(bytes.constData(), static_cast<std::size_t>(bytes.size()));
}

/// @brief `msg.payload` 是否**确为** `msg.frame` 的视图(ADR-0020 **D2** 的零拷贝判据)。
///
/// 判据是**地址落在 frame 的数据块区间内**,而不是"内容相等"——后者拷贝出来的也成立,
/// 证不了零拷贝。空 payload 不适用(`fromRawData(p, 0)` 不指进 frame),故用例一律用
/// 非空 payload 检验这一条。
inline bool PayloadIsViewOfFrame(const transport::Message& msg) {
  const char* frame_begin = msg.frame.constData();
  const char* frame_end = frame_begin + msg.frame.size();
  const char* payload_begin = msg.payload.constData();
  return payload_begin >= frame_begin && payload_begin < frame_end &&
         payload_begin + msg.payload.size() <= frame_end;
}

}  // namespace testutil
