#pragma once

// -----------------------------------------------------------------------------
// Result.hpp — 统一返回类型 Result<T> / Status
// 框架不抛异常：所有可能失败的操作返回 Result<T>（成功值 + ok 标志 + 错误串）。
// Status = Result<std::monostate>，用于「只表示成败」的操作。
// 错误串按前缀分类：timeout: / conn: / codec: / frame: / io: / config:
// -----------------------------------------------------------------------------

#include <string>
#include <utility>
#include <variant>

namespace transport {

// 所有可能失败的操作返回 Result<T>；框架不抛异常。
// 错误字符串前缀分类：timeout: / conn: / codec: / frame: / io: / config:
// 约束：T 必须可默认构造（Fail 时以 T{} 初始化 value）。
// [[nodiscard]]：框架靠返回值传错误（不抛异常），忽略 Result/Status 会静默丢错——
// 加此属性令「忽略返回值」在编译期告警。
template <typename T>
struct [[nodiscard]] Result {
  T value{};
  bool ok = false;
  std::string error;

  explicit operator bool() const { return ok; }

  static Result<T> Success(T v) { return {std::move(v), true, ""}; }
  static Result<T> Fail(std::string msg) { return {T{}, false, std::move(msg)}; }
};

using Status = Result<std::monostate>;

}  // namespace transport
