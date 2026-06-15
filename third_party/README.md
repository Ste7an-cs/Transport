# third_party —— 内置（vendored）第三方依赖

这些依赖已随仓库一起提交，构建时**不再联网拉取**（此前 FetchContent 每次 clone 易被代理卡住）。
CMakeLists.txt 直接引用本目录，离线即可完整构建+测试。

| 依赖 | 版本 | 形态 | 来源 |
|---|---|---|---|
| `asio/` | 1.30.2 (`asio-1-30-2`) | header-only standalone，仅保留 `include/` | https://github.com/chriskohlhoff/asio （release tarball） |
| `nlohmann/json.hpp` | 3.11.3 | 单头文件 `single_include` | https://github.com/nlohmann/json |
| `googletest/` | 1.14.0 (`v1.14.0`) | 源码，已剔除 `docs/test/samples/.git` | https://github.com/google/googletest |

升级方式：替换对应目录后更新上表版本号即可（无需改 CMake 逻辑）。
asio 以 `ASIO_STANDALONE` 模式使用；json 以 `nlohmann_json::nlohmann_json` INTERFACE 目标链接；
googletest 经 `add_subdirectory` 引入。

## 未内置：Fast DDS（唯一可选外部依赖）

Fast DDS 2.13.x（编译型库，传递依赖 `fastcdr` / `foonathan_memory` / `tinyxml2`）体积大、与平台 ABI
强相关，**有意不内置**，由 CMake `find_package(fastrtps/fastcdr QUIET)` 自动探测：

- **未装** → 跳过 `FastDdsProvider` 及其 8 个真实互通测试，离线构建 142/142 通过（DDS 逻辑仍可用 `FakeDdsProvider`）。
- **已装** → 自动启用，150/150 通过。

仅当需要真实 DDS 互通时，在目标机一次性安装 Fast DDS 2.13.x 即可（非每次构建拉取）。详见根 `README.md`「构建」节。
