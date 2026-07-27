# 编码规范（Coding Standards）

本项目 C++ 代码遵循以下规范。评审(`/code-review` 的 Standards 轴)与实现均以此为准;文档化的仓库规范优先于通用 smell baseline。

## 基线

- **Google C++ Style Guide** —— 命名、格式、头文件顺序、作用域、所有权等以其为基线:<https://google.github.io/styleguide/cppguide.html>。
  - 类型 `PascalCase`;函数/方法 `PascalCase`(沿用现有代码,如 `Start`/`Read`/`Write`);变量 `snake_case`;成员变量 `snake_case_` 结尾下划线;常量 `kCamelCase`;宏全大写。
  - 头文件用 `#pragma once`(沿用现有);包含顺序自成一组、按既有风格。
  - 优先值语义与明确所有权;避免非必要继承(参照 CONTEXT 的组合优先取向)。

## Doxygen 风格注释

- **公共头文件**(`include/transport/**`)的文件、类、公共函数须有 **Doxygen 风格**注释:
  - 文件级 `@file` + `@brief`;
  - 类级 `@brief` 说明职责;
  - 公共函数 `@brief` + 必要的 `@param` / `@return`,并在语义非平凡处点明可观察行为与错误类别(引用相应 `RT_` 需求号)。
- 实现文件(`src/**`)注释聚焦"为什么",不复述签名。
- 参照 `third_party/AsyncTask/coro/await/corosocket.hpp` 既有的 `@file`/`@brief` 风格。

## 与既有约定并存(不被上述覆盖)

- **不抛异常**:预期失败用结构化 `Result<T>` / `Status`;第三方异常在框架边界捕获转换。
- `Result<T>` / `Status` 标 `[[nodiscard]]`;纯查询接口(返回值即目的)也建议 `[[nodiscard]]`。
- **错误类别机器可判别**(`InvalidArgument`/`InvalidState`/`Connection`/`Closed`/`Timeout`/`Cancelled`/`Io`/`Frame`/`Codec`/`ResourceExhausted`/`Unsupported`/`Internal` 等),不得靠解析字符串前缀分类。
- 命名使用 `CONTEXT.md` 的 ubiquitous language(发送完成语义 / 发送排序 / 节点所属执行域 / Datagram / 错误类别 等),不漂移到同义词。

## 工具已强制的跳过

由编译器/格式化工具强制的项(`-Wall -Wextra` 告警、clang-format 若启用)不在人工评审重复挑剔。
