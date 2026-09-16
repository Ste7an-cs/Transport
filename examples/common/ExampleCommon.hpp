#pragma once

/**
 * @file ExampleCommon.hpp
 * @brief 六个示例共用的**纯样板**小工具:命令行取值、字节打印、错误打印、横幅。
 *
 * **本文件里没有任何框架知识**——它只是为了让六个 `main.cpp` 把版面留给真正要演示的东西
 * (装配、四种交互、endpoint、payload 视图、codec 视图顺序、DdsNode 相位)。头文件形态,
 * 不产生额外编译单元。
 */

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <system_error>
#include <vector>

#include <QByteArray>

namespace example {

// ── 命令行 ────────────────────────────────────────────────────────────────

/// @brief 取 `--name <值>`;没给就返回 `fallback`。
inline std::string OptionOr(int argc, char** argv, const std::string& name,
                            std::string fallback) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (name == argv[i]) {
      return std::string(argv[i + 1]);
    }
  }
  return fallback;
}

/// @brief 取**全部** `--name <值>`(可重复给,如 udp_fanout 的 `--peer`)。
inline std::vector<std::string> OptionAll(int argc, char** argv,
                                          const std::string& name) {
  std::vector<std::string> out;
  for (int i = 1; i + 1 < argc; ++i) {
    if (name == argv[i]) {
      out.emplace_back(argv[i + 1]);
    }
  }
  return out;
}

/// @brief 取 `--name <整数>`;没给或解析不了就返回 `fallback`。
inline int IntOptionOr(int argc, char** argv, const std::string& name,
                       int fallback) {
  const std::string text = OptionOr(argc, argv, name, std::string());
  if (text.empty()) {
    return fallback;
  }
  return std::atoi(text.c_str());
}

/// @brief 有没有给 `--name` 这个开关(不带值)。
inline bool HasFlag(int argc, char** argv, const std::string& name) {
  for (int i = 1; i < argc; ++i) {
    if (name == argv[i]) {
      return true;
    }
  }
  return false;
}

/// @brief 拆 `ip:port`;拆不开返回 `false`。
inline bool ParseHostPort(const std::string& text, std::string* host,
                          std::uint16_t* port) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) {
    return false;
  }
  *host = text.substr(0, colon);
  *port = static_cast<std::uint16_t>(std::atoi(text.c_str() + colon + 1));
  return *port != 0;
}

// ── 打印 ──────────────────────────────────────────────────────────────────

/// @brief 把 payload 当**文本**看(示例的 payload 都是可打印的 ASCII)。
inline std::string Text(const QByteArray& bytes) {
  return std::string(bytes.constData(), static_cast<std::size_t>(bytes.size()));
}

/// @brief **打印用**的 payload 形式:可见 ASCII 原样,其余一律escape 成 `\xNN`。
///
/// 为什么不直接 `Text()` 去打:`Log()` 走 `printf("%s")`,**遇 `\0` 就截断**,后面的内容
/// 会凭空消失、看起来像程序出了别的毛病。而 payload **确实可能含 `\0`** —— 真实
/// Fast DDS **跨进程**收到的样本里就有(见 examples/dds_pubsub 的说明)。
/// 打印一律用本函数,取数据一律用 `Text()`。
inline std::string Printable(const QByteArray& bytes) {
  static const char kDigits[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(static_cast<std::size_t>(bytes.size()));
  for (int i = 0; i < bytes.size(); ++i) {
    const auto byte = static_cast<unsigned char>(bytes.at(i));
    if (byte >= 0x20 && byte != 0x7F) {  // 可见 ASCII 与 UTF-8 续字节原样留着
      out.push_back(static_cast<char>(byte));
      continue;
    }
    out += "\\x";
    out.push_back(kDigits[byte >> 4]);
    out.push_back(kDigits[byte & 0x0F]);
  }
  return out;
}

/// @brief 把字节打成 `AA BB CC` 形式,供看帧用。
inline std::string Hex(const QByteArray& bytes) {
  static const char kDigits[] = "0123456789ABCDEF";
  std::string out;
  out.reserve(static_cast<std::size_t>(bytes.size()) * 3);
  for (int i = 0; i < bytes.size(); ++i) {
    const auto byte = static_cast<unsigned char>(bytes.at(i));
    if (i != 0) {
      out.push_back(' ');
    }
    out.push_back(kDigits[byte >> 4]);
    out.push_back(kDigits[byte & 0x0F]);
  }
  return out;
}

/// @brief 把一个数打成 `0xNNNN`(命令码一律按十六进制看)。
inline std::string Hex16(unsigned int value) {
  static const char kDigits[] = "0123456789ABCDEF";
  std::string out = "0x";
  for (int shift = 12; shift >= 0; shift -= 4) {
    out.push_back(kDigits[(value >> shift) & 0x0F]);
  }
  return out;
}

/// @brief `QByteArray` 化一段文本(出站 payload 是**拥有型**的,随手构造即可)。
inline QByteArray Pay(const std::string& text) {
  return QByteArray(text.data(), static_cast<int>(text.size()));
}

/// @brief 错误码的可读形式——框架的预期失败一律走 `Coro::Result`,不抛异常。
inline std::string Err(const std::error_code& error) {
  return error.message();
}

/// @brief 打印"本例演示什么 + 怎么跑"。每个示例开头调一次。
inline void PrintBanner(const std::string& title,
                        const std::vector<std::string>& lines) {
  std::printf("\n========================================================\n");
  std::printf("  %s\n", title.c_str());
  std::printf("========================================================\n");
  for (const std::string& line : lines) {
    std::printf("%s\n", line.c_str());
  }
  std::printf("--------------------------------------------------------\n\n");
  std::fflush(stdout);
}

/// @brief 带前缀的一行日志(示例全程用它,便于两个终端对照)。
inline void Log(const std::string& text) {
  std::printf("%s\n", text.c_str());
  std::fflush(stdout);
}

}  // namespace example
