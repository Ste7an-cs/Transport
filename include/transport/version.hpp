#pragma once

// -----------------------------------------------------------------------------
// version.hpp — 库版本号（语义化版本，单一真源）
// kVersionMajor/Minor/Patch 为编译期常量；LibraryVersion() 返回 "MAJOR.MINOR.PATCH"。
// -----------------------------------------------------------------------------

#include <string>

namespace transport {

inline constexpr int kVersionMajor = 0;
inline constexpr int kVersionMinor = 3;
inline constexpr int kVersionPatch = 0;
inline constexpr char kVersion[] = "0.3.0";  // = kVersionMajor.kVersionMinor.kVersionPatch

std::string LibraryVersion();  // 返回 kVersion

}  // namespace transport
