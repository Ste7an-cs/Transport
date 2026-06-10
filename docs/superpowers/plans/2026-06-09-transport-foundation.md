# Transport 中间件 — Foundation 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 搭建通信中间件的基础层——核心接口、长度字段分帧、流式分帧装配、FIFO 三模式接收交付、传输基类，以及 CMake + GoogleTest 工程骨架；其余各传输（TCP/UDP/串口/DDS/工厂）在后续计划中基于本层实现。

**Architecture:** 纯接口与数据结构放在 `include/transport/`（零第三方依赖）。`LengthFieldFramer` 实现「固定 header + 长度字段」分帧；`FrameAssembler` 用滚动缓冲驱动任意 `IFramer` 切帧；`ReceiveQueue` 提供同步/回调/future 三种互斥的交付模式；`TransportBase` 组合 codec + `ReceiveQueue`，实现 `ITransport` 的接收/编解码/断连通用部分，将 `Open/Close/IsOpen/Send` 留给各传输子类。

**Tech Stack:** C++17、CMake（≥3.16）、GoogleTest 1.14（FetchContent 拉取）、Google C++ 风格。

**与 spec 的两处实现层细化（已在本计划锁定）：**
1. `IFramer::TryExtract` 返回 `Result<FrameResult>`（而非裸 `FrameResult`），以便用 `ok=false` 报 `frame:` 错误；`ok=true && has_frame=false` 表示「数据不足，等待更多」。
2. 内部辅助类型 `FrameAssembler` / `ReceiveQueue` / `TransportBase` 的头文件统一放在 `include/transport/`（`framing/`、`core/`）下，便于统一 include 路径；它们属内部实现，非稳定公共 API。

---

## 文件结构

```
transport/
├── CMakeLists.txt                                  # 工程骨架 + GoogleTest + transport 静态库
├── include/transport/
│   ├── version.hpp                                 # 库版本字符串
│   ├── Result.hpp                                  # Result<T> / Status
│   ├── Message.hpp                                 # Message 数据结构
│   ├── ICodec.hpp                                  # 编解码接口
│   ├── IFramer.hpp                                 # 分帧接口 + FrameResult
│   ├── ITransport.hpp                              # 传输公共接口
│   ├── framing/
│   │   ├── LengthFieldFramer.hpp                   # 长度字段分帧（公共）
│   │   └── FrameAssembler.hpp                      # 滚动缓冲 + IFramer 驱动（内部，header-only）
│   └── core/
│       ├── ReceiveQueue.hpp                        # FIFO 三模式交付（内部）
│       └── TransportBase.hpp                       # ITransport 通用实现基类（内部，header-only）
├── src/
│   ├── core/
│   │   ├── version.cpp
│   │   └── ReceiveQueue.cpp
│   └── framing/
│       └── LengthFieldFramer.cpp
└── tests/
    ├── version_test.cpp
    ├── result_test.cpp
    ├── interfaces_test.cpp
    ├── framing/
    │   ├── length_field_framer_test.cpp
    │   └── frame_assembler_test.cpp
    └── core/
        ├── receive_queue_test.cpp
        └── transport_base_test.cpp
```

每个任务负责其中一组文件，并在创建源文件时把它加入 `CMakeLists.txt` 对应的目标列表。

---

## Task 1: CMake 工程骨架 + GoogleTest + 版本冒烟测试

**Files:**
- Create: `CMakeLists.txt`
- Create: `include/transport/version.hpp`
- Create: `src/core/version.cpp`
- Test: `tests/version_test.cpp`

- [ ] **Step 1: 写失败测试**

`tests/version_test.cpp`:

```cpp
#include "transport/version.hpp"

#include <gtest/gtest.h>

TEST(Version, ReturnsNonEmpty) {
  EXPECT_FALSE(transport::LibraryVersion().empty());
}
```

- [ ] **Step 2: 创建 CMakeLists.txt**

`CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.16)
project(transport LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TRANSPORT_BUILD_TESTS "Build unit tests" ON)

add_library(transport STATIC
  src/core/version.cpp
)
target_include_directories(transport PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_compile_features(transport PUBLIC cxx_std_17)

if(TRANSPORT_BUILD_TESTS)
  enable_testing()
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  include(GoogleTest)

  add_executable(transport_tests
    tests/version_test.cpp
  )
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)
  gtest_discover_tests(transport_tests)
endif()
```

- [ ] **Step 3: 写 version 头与实现**

`include/transport/version.hpp`:

```cpp
#pragma once

#include <string>

namespace transport {

std::string LibraryVersion();

}  // namespace transport
```

`src/core/version.cpp`:

```cpp
#include "transport/version.hpp"

namespace transport {

std::string LibraryVersion() { return "0.1.0"; }

}  // namespace transport
```

- [ ] **Step 4: 配置、构建、运行测试**

Run:
```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```
Expected: 配置阶段拉取 googletest 成功；`Version.ReturnsNonEmpty` PASS（1 test passed）。

- [ ] **Step 5: 提交**

```bash
git add CMakeLists.txt include/transport/version.hpp src/core/version.cpp tests/version_test.cpp
git commit -m "build: CMake 骨架 + GoogleTest + 版本冒烟测试"
```

> 备注：将 `build/` 加入 `.gitignore`（见 Task 1b）。

---

## Task 1b: .gitignore

**Files:**
- Create: `.gitignore`

- [ ] **Step 1: 创建 .gitignore**

`.gitignore`:

```gitignore
/build/
*.o
*.a
compile_commands.json
```

- [ ] **Step 2: 提交**

```bash
git add .gitignore
git commit -m "chore: 添加 .gitignore"
```

---

## Task 2: `Result<T>` / `Status`

**Files:**
- Create: `include/transport/Result.hpp`
- Test: `tests/result_test.cpp`
- Modify: `CMakeLists.txt`（把测试源加入 `transport_tests`）

- [ ] **Step 1: 写失败测试**

`tests/result_test.cpp`:

```cpp
#include "transport/Result.hpp"

#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::Result;
using transport::Status;

TEST(Result, SuccessHoldsValueAndIsTruthy) {
  auto r = Result<int>::Success(42);
  EXPECT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.ok);
  EXPECT_EQ(r.value, 42);
  EXPECT_TRUE(r.error.empty());
}

TEST(Result, FailIsFalsyAndCarriesMessage) {
  auto r = Result<std::vector<uint8_t>>::Fail("io: boom");
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error, "io: boom");
  EXPECT_TRUE(r.value.empty());  // 默认构造的 value
}

TEST(Result, StatusSuccessAndFail) {
  Status ok = Status::Success(std::monostate{});
  EXPECT_TRUE(static_cast<bool>(ok));
  Status bad = Status::Fail("config: nope");
  EXPECT_FALSE(static_cast<bool>(bad));
  EXPECT_EQ(bad.error, "config: nope");
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `CMakeLists.txt` 的 `add_executable(transport_tests ...)` 列表中，于 `tests/version_test.cpp` 后新增一行，使其变为：

```cmake
  add_executable(transport_tests
    tests/version_test.cpp
    tests/result_test.cpp
  )
```

- [ ] **Step 3: 运行测试，确认失败**

Run: `cmake --build build -j 2>&1 | head -30`
Expected: 编译失败 —— 找不到 `transport/Result.hpp`。

- [ ] **Step 4: 写 Result.hpp**

`include/transport/Result.hpp`:

```cpp
#pragma once

#include <string>
#include <utility>
#include <variant>

namespace transport {

// 所有可能失败的操作返回 Result<T>；框架不抛异常。
// 错误字符串前缀分类：timeout: / conn: / codec: / frame: / io: / config:
// 约束：T 必须可默认构造（Fail 时以 T{} 初始化 value）。
template <typename T>
struct Result {
  T value{};
  bool ok = false;
  std::string error;

  explicit operator bool() const { return ok; }

  static Result<T> Success(T v) { return {std::move(v), true, ""}; }
  static Result<T> Fail(std::string msg) { return {T{}, false, std::move(msg)}; }
};

using Status = Result<std::monostate>;

}  // namespace transport
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure`
Expected: `Result.*` 与 `Version.*` 全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add include/transport/Result.hpp tests/result_test.cpp CMakeLists.txt
git commit -m "feat: Result<T> / Status 核心类型"
```

---

## Task 3: 核心接口头（`Message` / `ICodec` / `IFramer` / `ITransport`）

**Files:**
- Create: `include/transport/Message.hpp`
- Create: `include/transport/ICodec.hpp`
- Create: `include/transport/IFramer.hpp`
- Create: `include/transport/ITransport.hpp`
- Test: `tests/interfaces_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试**

`tests/interfaces_test.cpp`:

```cpp
#include "transport/ICodec.hpp"
#include "transport/IFramer.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

namespace {

// 一个最简的恒等 codec，验证 ICodec 可被实现
class IdentityCodec : public transport::ICodec {
 public:
  transport::Result<std::vector<uint8_t>> Encode(
      const std::vector<uint8_t>& data) override {
    return transport::Result<std::vector<uint8_t>>::Success(data);
  }
  transport::Result<std::vector<uint8_t>> Decode(
      const std::vector<uint8_t>& data) override {
    return transport::Result<std::vector<uint8_t>>::Success(data);
  }
};

}  // namespace

TEST(Interfaces, MessageFieldsDefault) {
  transport::Message m;
  EXPECT_TRUE(m.payload.empty());
  EXPECT_TRUE(m.topic.empty());
  EXPECT_TRUE(m.source.empty());
  EXPECT_EQ(m.timestamp, 0);
}

TEST(Interfaces, CodecCanBeImplementedAndCalled) {
  std::unique_ptr<transport::ICodec> codec = std::make_unique<IdentityCodec>();
  std::vector<uint8_t> in{1, 2, 3};
  auto enc = codec->Encode(in);
  ASSERT_TRUE(static_cast<bool>(enc));
  EXPECT_EQ(enc.value, in);
  auto dec = codec->Decode(enc.value);
  ASSERT_TRUE(static_cast<bool>(dec));
  EXPECT_EQ(dec.value, in);
}

TEST(Interfaces, FrameResultDefaults) {
  transport::FrameResult fr;
  EXPECT_EQ(fr.consumed, 0u);
  EXPECT_FALSE(fr.has_frame);
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 列表中追加 `tests/interfaces_test.cpp`。

- [ ] **Step 3: 运行测试，确认失败**

Run: `cmake --build build -j 2>&1 | head -30`
Expected: 编译失败 —— 找不到这四个头文件。

- [ ] **Step 4: 写四个接口头**

`include/transport/Message.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

// 经 ICodec.Decode 处理后交付给应用层的一条消息。
struct Message {
  std::vector<uint8_t> payload;       // 解码后的字节流（含用户 header + body）
  std::string topic;                  // DDS topic / 逻辑通道名；TCP/UDP/串口为空
  std::string source;                 // 发送方标识："ip:port"、topic 名、设备路径等
  int64_t timestamp = 0;              // 接收时间戳（微秒，由框架填充）
};

}  // namespace transport
```

`include/transport/ICodec.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <vector>

#include "transport/Result.hpp"

namespace transport {

// 发送/接收边界由框架自动调用；未设置时原始字节直接透传。
class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发送前调用：将数据编码为字节流（用户在此组装 header + body）
  virtual Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& data) = 0;

  // 接收后调用：将一帧完整字节流解码
  virtual Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& data) = 0;
};

}  // namespace transport
```

`include/transport/IFramer.hpp`:

```cpp
#pragma once

#include <cstddef>
#include <cstdint>

#include "transport/Result.hpp"

namespace transport {

struct FrameResult {
  size_t consumed = 0;     // 本次消耗字节数；帧 = buf[0 .. consumed)
  bool has_frame = false;  // 是否切出一整帧
};

// 流式传输（TCP/串口）接收侧分帧：从滚动缓冲识别并切出一帧。
class IFramer {
 public:
  virtual ~IFramer() = default;

  // 返回值约定：
  //   ok == false                  -> frame: 错误（帧头非法/帧长越界），调用方应断开
  //   ok == true, has_frame == true -> 成功切出一帧 buf[0 .. consumed)
  //   ok == true, has_frame == false-> 数据不足，等待更多字节（consumed = 0）
  virtual Result<FrameResult> TryExtract(const uint8_t* buf, size_t len) = 0;
};

}  // namespace transport
```

`include/transport/ITransport.hpp`:

```cpp
#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

class ITransport {
 public:
  virtual ~ITransport() = default;

  using ReceiveCallback = std::function<void(Result<Message>)>;
  using DisconnectCallback = std::function<void(const std::string& reason)>;

  // 生命周期
  virtual Status Open() = 0;
  virtual void Close() = 0;
  virtual bool IsOpen() const = 0;

  // 发送（若已设置 ICodec，自动 Encode 后传输）。data 自带长度。
  virtual Status Send(const std::vector<uint8_t>& data) = 0;

  // 同步接收（阻塞至收到数据或超时；timeout_ms == 0 表示永久阻塞）
  virtual Result<Message> Receive(uint32_t timeout_ms = 0) = 0;

  // 异步接收 —— 回调模式（回调在内部 I/O 线程执行，必须非阻塞）
  virtual void OnReceive(ReceiveCallback cb) = 0;

  // 异步接收 —— future 模式（每次调用消费一条到来的消息）
  virtual std::future<Result<Message>> AsyncReceive() = 0;

  // 断连通知（TCP 客户端、串口适用）
  virtual void OnDisconnect(DisconnectCallback cb) = 0;

  // 挂载编解码器；未设置时原始字节直接透传
  virtual void SetCodec(std::shared_ptr<ICodec> codec) = 0;
};

}  // namespace transport
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure`
Expected: `Interfaces.*` 全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add include/transport/Message.hpp include/transport/ICodec.hpp \
        include/transport/IFramer.hpp include/transport/ITransport.hpp \
        tests/interfaces_test.cpp CMakeLists.txt
git commit -m "feat: 核心接口 Message/ICodec/IFramer/ITransport"
```

---

## Task 4: `LengthFieldFramer`（长度字段分帧）

**Files:**
- Create: `include/transport/framing/LengthFieldFramer.hpp`
- Create: `src/framing/LengthFieldFramer.cpp`
- Test: `tests/framing/length_field_framer_test.cpp`
- Modify: `CMakeLists.txt`（加库源 + 测试源）

- [ ] **Step 1: 写失败测试**

`tests/framing/length_field_framer_test.cpp`:

```cpp
#include "transport/framing/LengthFieldFramer.hpp"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameResult;
using transport::LengthFieldFramer;
using transport::LengthFieldFramerConfig;

namespace {

// header_size=8, length_offset=4, length_size=4, big-endian, body 长度=length 值
LengthFieldFramerConfig BeConfig() {
  LengthFieldFramerConfig c;
  c.header_size = 8;
  c.length_offset = 4;
  c.length_size = 4;
  c.big_endian = true;
  c.length_includes_header = false;
  c.max_frame_size = 1024;
  return c;
}

// 构造一帧：8 字节 header（offset4..7 存 body_len, BE）+ body_len 字节 body
std::vector<uint8_t> BuildFrame(uint32_t body_len, uint8_t fill = 0xAB) {
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
  buf[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
  buf[6] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
  buf[7] = static_cast<uint8_t>(body_len & 0xFF);
  buf.insert(buf.end(), body_len, fill);
  return buf;
}

}  // namespace

TEST(LengthFieldFramer, NeedMoreWhenLessThanHeader) {
  LengthFieldFramer f(BeConfig());
  std::vector<uint8_t> buf(5, 0x00);
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_FALSE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 0u);
}

TEST(LengthFieldFramer, ExtractsFullFrameBigEndian) {
  LengthFieldFramer f(BeConfig());
  auto buf = BuildFrame(3);  // total = 8 + 3 = 11
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 11u);
}

TEST(LengthFieldFramer, NeedMoreWhenBodyIncomplete) {
  LengthFieldFramer f(BeConfig());
  auto buf = BuildFrame(10);
  buf.resize(8 + 4);  // 只到了一部分 body
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_FALSE(r.value.has_frame);
}

TEST(LengthFieldFramer, LittleEndianLengthField) {
  auto c = BeConfig();
  c.big_endian = false;
  LengthFieldFramer f(c);
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = 0x02;  // LE: body_len = 2
  buf.insert(buf.end(), 2, 0xCD);
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 10u);
}

TEST(LengthFieldFramer, LengthIncludesHeader) {
  auto c = BeConfig();
  c.length_includes_header = true;
  LengthFieldFramer f(c);
  std::vector<uint8_t> buf(8, 0x00);
  buf[7] = 11;  // 总帧长 = 11（含 header）
  buf.insert(buf.end(), 3, 0xEE);
  auto r = f.TryExtract(buf.data(), buf.size());
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.has_frame);
  EXPECT_EQ(r.value.consumed, 11u);
}

TEST(LengthFieldFramer, ErrorWhenExceedsMaxFrameSize) {
  auto c = BeConfig();
  c.max_frame_size = 16;
  LengthFieldFramer f(c);
  auto buf = BuildFrame(100);  // total 108 > 16
  auto r = f.TryExtract(buf.data(), buf.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);  // 以 "frame:" 开头
}

TEST(LengthFieldFramer, ValidateConfigRejectsBadLengthSize) {
  auto c = BeConfig();
  c.length_size = 3;
  EXPECT_FALSE(static_cast<bool>(LengthFieldFramer::ValidateConfig(c)));
}

TEST(LengthFieldFramer, ValidateConfigRejectsLengthBeyondHeader) {
  auto c = BeConfig();
  c.length_offset = 6;
  c.length_size = 4;  // 6 + 4 > 8
  EXPECT_FALSE(static_cast<bool>(LengthFieldFramer::ValidateConfig(c)));
}

TEST(LengthFieldFramer, ValidateConfigAcceptsGoodConfig) {
  EXPECT_TRUE(static_cast<bool>(LengthFieldFramer::ValidateConfig(BeConfig())));
}
```

- [ ] **Step 2: 把库源与测试源加入 CMake**

在 `add_library(transport STATIC ...)` 列表追加 `src/framing/LengthFieldFramer.cpp`；
在 `add_executable(transport_tests ...)` 列表追加 `tests/framing/length_field_framer_test.cpp`。

- [ ] **Step 3: 运行测试，确认失败**

Run: `cmake --build build -j 2>&1 | head -30`
Expected: 编译失败 —— 找不到 `LengthFieldFramer.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/framing/LengthFieldFramer.hpp`:

```cpp
#pragma once

#include <cstddef>
#include <cstdint>

#include "transport/IFramer.hpp"
#include "transport/Result.hpp"

namespace transport {

// 适配「固定长 header + header 内长度字段」协议。
struct LengthFieldFramerConfig {
  size_t header_size = 0;             // 固定 header 总长（字节）
  size_t length_offset = 0;           // 长度字段在 header 内的偏移
  size_t length_size = 4;             // 长度字段字节数（2 / 4 / 8）
  bool big_endian = true;             // 长度字段字节序
  bool length_includes_header = false;// 长度值是否已包含 header 本身
  size_t max_frame_size = 16 * 1024 * 1024;  // 帧长上限，超出报 frame: 错误
};

class LengthFieldFramer : public IFramer {
 public:
  // 配置校验（供 TransportFactory 在创建时调用）
  static Status ValidateConfig(const LengthFieldFramerConfig& config);

  explicit LengthFieldFramer(LengthFieldFramerConfig config);

  Result<FrameResult> TryExtract(const uint8_t* buf, size_t len) override;

 private:
  LengthFieldFramerConfig config_;
};

}  // namespace transport
```

- [ ] **Step 5: 写实现**

`src/framing/LengthFieldFramer.cpp`:

```cpp
#include "transport/framing/LengthFieldFramer.hpp"

#include <variant>

namespace transport {

Status LengthFieldFramer::ValidateConfig(const LengthFieldFramerConfig& c) {
  if (c.header_size == 0) {
    return Status::Fail("config: header_size must be > 0");
  }
  if (c.length_size != 2 && c.length_size != 4 && c.length_size != 8) {
    return Status::Fail("config: length_size must be 2, 4, or 8");
  }
  if (c.length_offset + c.length_size > c.header_size) {
    return Status::Fail("config: length field exceeds header_size");
  }
  if (c.max_frame_size < c.header_size) {
    return Status::Fail("config: max_frame_size smaller than header_size");
  }
  return Status::Success(std::monostate{});
}

LengthFieldFramer::LengthFieldFramer(LengthFieldFramerConfig config)
    : config_(config) {}

Result<FrameResult> LengthFieldFramer::TryExtract(const uint8_t* buf, size_t len) {
  if (len < config_.header_size) {
    return Result<FrameResult>::Success(FrameResult{0, false});
  }

  uint64_t value = 0;
  const uint8_t* p = buf + config_.length_offset;
  if (config_.big_endian) {
    for (size_t i = 0; i < config_.length_size; ++i) {
      value = (value << 8) | static_cast<uint64_t>(p[i]);
    }
  } else {
    for (size_t i = 0; i < config_.length_size; ++i) {
      value |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
  }

  const uint64_t frame_size =
      config_.length_includes_header ? value : config_.header_size + value;

  if (frame_size < config_.header_size) {
    return Result<FrameResult>::Fail(
        "frame: declared frame size smaller than header");
  }
  if (frame_size > config_.max_frame_size) {
    return Result<FrameResult>::Fail("frame: frame size exceeds max_frame_size");
  }
  if (len < frame_size) {
    return Result<FrameResult>::Success(FrameResult{0, false});
  }
  return Result<FrameResult>::Success(
      FrameResult{static_cast<size_t>(frame_size), true});
}

}  // namespace transport
```

- [ ] **Step 6: 运行测试，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure`
Expected: `LengthFieldFramer.*` 全部 PASS。

- [ ] **Step 7: 提交**

```bash
git add include/transport/framing/LengthFieldFramer.hpp \
        src/framing/LengthFieldFramer.cpp \
        tests/framing/length_field_framer_test.cpp CMakeLists.txt
git commit -m "feat: LengthFieldFramer 长度字段分帧"
```

---

## Task 5: `FrameAssembler`（滚动缓冲 + IFramer 驱动）

**Files:**
- Create: `include/transport/framing/FrameAssembler.hpp`（header-only）
- Test: `tests/framing/frame_assembler_test.cpp`
- Modify: `CMakeLists.txt`（仅加测试源）

- [ ] **Step 1: 写失败测试**

`tests/framing/frame_assembler_test.cpp`:

```cpp
#include "transport/framing/FrameAssembler.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/framing/LengthFieldFramer.hpp"

using transport::FrameAssembler;
using transport::LengthFieldFramer;
using transport::LengthFieldFramerConfig;

namespace {

LengthFieldFramerConfig BeConfig() {
  LengthFieldFramerConfig c;
  c.header_size = 8;
  c.length_offset = 4;
  c.length_size = 4;
  c.big_endian = true;
  c.max_frame_size = 1024;
  return c;
}

std::vector<uint8_t> BuildFrame(uint32_t body_len, uint8_t fill) {
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
  buf[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
  buf[6] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
  buf[7] = static_cast<uint8_t>(body_len & 0xFF);
  buf.insert(buf.end(), body_len, fill);
  return buf;
}

}  // namespace

TEST(FrameAssembler, PassthroughWhenNoFramer) {
  FrameAssembler a(nullptr);
  std::vector<uint8_t> chunk{1, 2, 3, 4};
  auto r = a.Feed(chunk.data(), chunk.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 1u);
  EXPECT_EQ(r.value[0], chunk);
}

TEST(FrameAssembler, AssemblesAcrossPartialReads) {
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  FrameAssembler a(framer);
  auto frame = BuildFrame(5, 0xAB);  // 13 字节

  // 先喂前 6 字节：不足，无完整帧
  auto r1 = a.Feed(frame.data(), 6);
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value.empty());

  // 再喂剩余：切出 1 帧
  auto r2 = a.Feed(frame.data() + 6, frame.size() - 6);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0], frame);
}

TEST(FrameAssembler, SplitsTwoFramesInOneFeed) {
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  FrameAssembler a(framer);
  auto f1 = BuildFrame(2, 0x11);
  auto f2 = BuildFrame(3, 0x22);
  std::vector<uint8_t> both = f1;
  both.insert(both.end(), f2.begin(), f2.end());

  auto r = a.Feed(both.data(), both.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0], f1);
  EXPECT_EQ(r.value[1], f2);
}

TEST(FrameAssembler, ByteByByteDelivery) {
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  FrameAssembler a(framer);
  auto frame = BuildFrame(4, 0x33);

  size_t total = 0;
  for (size_t i = 0; i + 1 < frame.size(); ++i) {
    auto r = a.Feed(frame.data() + i, 1);
    ASSERT_TRUE(static_cast<bool>(r));
    total += r.value.size();
  }
  EXPECT_EQ(total, 0u);  // 最后一个字节前不应切出帧
  auto last = a.Feed(frame.data() + frame.size() - 1, 1);
  ASSERT_TRUE(static_cast<bool>(last));
  ASSERT_EQ(last.value.size(), 1u);
  EXPECT_EQ(last.value[0], frame);
}

TEST(FrameAssembler, PropagatesFramerError) {
  auto c = BeConfig();
  c.max_frame_size = 4;
  auto framer = std::make_shared<LengthFieldFramer>(c);
  FrameAssembler a(framer);
  auto frame = BuildFrame(100, 0x44);  // 触发 frame: 错误
  auto r = a.Feed(frame.data(), frame.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 列表追加 `tests/framing/frame_assembler_test.cpp`。

- [ ] **Step 3: 运行测试，确认失败**

Run: `cmake --build build -j 2>&1 | head -30`
Expected: 编译失败 —— 找不到 `FrameAssembler.hpp`。

- [ ] **Step 4: 写头文件（header-only）**

`include/transport/framing/FrameAssembler.hpp`:

```cpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "transport/IFramer.hpp"
#include "transport/Result.hpp"

namespace transport {

// 接收侧装配器：把字节追加进滚动缓冲，用 IFramer 循环切出完整帧。
// framer 为 nullptr 时进入透传模式：每次 Feed 的数据原样作为一帧返回。
class FrameAssembler {
 public:
  using Frames = std::vector<std::vector<uint8_t>>;

  explicit FrameAssembler(std::shared_ptr<IFramer> framer)
      : framer_(std::move(framer)) {}

  // 返回本次喂入后切出的所有完整帧；遇 frame: 错误返回 Fail（调用方应断开）。
  Result<Frames> Feed(const uint8_t* data, size_t len) {
    Frames frames;

    if (!framer_) {  // 透传模式
      if (len > 0) frames.emplace_back(data, data + len);
      return Result<Frames>::Success(std::move(frames));
    }

    buffer_.insert(buffer_.end(), data, data + len);
    size_t offset = 0;
    while (offset < buffer_.size()) {
      auto r = framer_->TryExtract(buffer_.data() + offset, buffer_.size() - offset);
      if (!r) {
        return Result<Frames>::Fail(r.error);
      }
      if (!r.value.has_frame) {
        break;
      }
      const size_t consumed = r.value.consumed;
      frames.emplace_back(buffer_.begin() + offset,
                          buffer_.begin() + offset + consumed);
      offset += consumed;
    }
    if (offset > 0) {
      buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
    }
    return Result<Frames>::Success(std::move(frames));
  }

 private:
  std::shared_ptr<IFramer> framer_;
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure`
Expected: `FrameAssembler.*` 全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add include/transport/framing/FrameAssembler.hpp \
        tests/framing/frame_assembler_test.cpp CMakeLists.txt
git commit -m "feat: FrameAssembler 接收侧分帧装配"
```

---

## Task 6: `ReceiveQueue`（FIFO + 同步/回调/future 三模式）

**Files:**
- Create: `include/transport/core/ReceiveQueue.hpp`
- Create: `src/core/ReceiveQueue.cpp`
- Test: `tests/core/receive_queue_test.cpp`
- Modify: `CMakeLists.txt`（加库源 + 测试源）

- [ ] **Step 1: 写失败测试**

`tests/core/receive_queue_test.cpp`:

```cpp
#include "transport/core/ReceiveQueue.hpp"

#include <chrono>
#include <future>
#include <thread>

#include <gtest/gtest.h>

#include "transport/Message.hpp"
#include "transport/Result.hpp"

using transport::Message;
using transport::ReceiveQueue;
using transport::Result;

namespace {

Result<Message> MakeMsg(uint8_t tag) {
  Message m;
  m.payload = {tag};
  return Result<Message>::Success(std::move(m));
}

}  // namespace

TEST(ReceiveQueue, SyncPushThenReceive) {
  ReceiveQueue q;
  q.Push(MakeMsg(7));
  auto r = q.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.payload.size(), 1u);
  EXPECT_EQ(r.value.payload[0], 7);
}

TEST(ReceiveQueue, SyncReceiveTimesOutOnEmpty) {
  ReceiveQueue q;
  auto r = q.Receive(20);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("timeout:", 0), 0u);
}

TEST(ReceiveQueue, CallbackInvokedOnPush) {
  ReceiveQueue q;
  std::vector<uint8_t> got;
  auto st = q.SetCallback([&](Result<Message> m) {
    if (m) got.push_back(m.value.payload[0]);
  });
  ASSERT_TRUE(static_cast<bool>(st));
  q.Push(MakeMsg(1));
  q.Push(MakeMsg(2));
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0], 1);
  EXPECT_EQ(got[1], 2);
}

TEST(ReceiveQueue, CallbackDrainsBacklog) {
  ReceiveQueue q;
  q.Push(MakeMsg(9));  // 设回调前先到达
  std::vector<uint8_t> got;
  q.SetCallback([&](Result<Message> m) {
    if (m) got.push_back(m.value.payload[0]);
  });
  ASSERT_EQ(got.size(), 1u);
  EXPECT_EQ(got[0], 9);
}

TEST(ReceiveQueue, FutureReadyWhenMessageAlreadyQueued) {
  ReceiveQueue q;
  q.Push(MakeMsg(5));
  auto fut = q.AsyncReceive();
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload[0], 5);
}

TEST(ReceiveQueue, FutureFulfilledOnLaterPush) {
  ReceiveQueue q;
  auto fut = q.AsyncReceive();
  q.Push(MakeMsg(8));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload[0], 8);
}

TEST(ReceiveQueue, FutureFifoOrder) {
  ReceiveQueue q;
  auto f1 = q.AsyncReceive();
  auto f2 = q.AsyncReceive();
  q.Push(MakeMsg(1));
  q.Push(MakeMsg(2));
  EXPECT_EQ(f1.get().value.payload[0], 1);
  EXPECT_EQ(f2.get().value.payload[0], 2);
}

TEST(ReceiveQueue, ModeExclusivitySyncThenCallbackFails) {
  ReceiveQueue q;
  (void)q.Receive(1);  // 锁定 kSync（超时返回）
  auto st = q.SetCallback([](Result<Message>) {});
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
}

TEST(ReceiveQueue, ModeExclusivityCallbackThenAsyncFails) {
  ReceiveQueue q;
  q.SetCallback([](Result<Message>) {});
  auto fut = q.AsyncReceive();
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("config:", 0), 0u);
}

TEST(ReceiveQueue, CloseUnblocksSyncReceive) {
  ReceiveQueue q;
  std::thread closer([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    q.Close();
  });
  auto r = q.Receive(0);  // 永久阻塞，直到 Close
  closer.join();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
}

TEST(ReceiveQueue, CloseFulfillsPendingFutures) {
  ReceiveQueue q;
  auto fut = q.AsyncReceive();
  q.Close();
  auto r = fut.get();
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
}
```

- [ ] **Step 2: 把库源与测试源加入 CMake**

在 `add_library(transport STATIC ...)` 追加 `src/core/ReceiveQueue.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/core/receive_queue_test.cpp`。

由于测试用到 `std::thread`，确保链接 pthread —— 在 `CMakeLists.txt` 末尾（`if(TRANSPORT_BUILD_TESTS)` 块内、`gtest_discover_tests` 之前）加：

```cmake
  find_package(Threads REQUIRED)
  target_link_libraries(transport PUBLIC Threads::Threads)
```

- [ ] **Step 3: 运行测试，确认失败**

Run: `cmake -S . -B build && cmake --build build -j 2>&1 | head -30`
Expected: 编译失败 —— 找不到 `ReceiveQueue.hpp`。

- [ ] **Step 4: 写头文件**

`include/transport/core/ReceiveQueue.hpp`:

```cpp
#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <mutex>

#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

// FIFO 消息队列 + 三种交付模式（同步/回调/future）。
// 模式互斥：由首个消费侧调用锁定，之后调用其它模式返回 config: 错误。
// 线程安全：生产侧 Push 由 I/O 线程调用，消费侧由应用线程调用。
class ReceiveQueue {
 public:
  using Callback = std::function<void(Result<Message>)>;
  enum class Mode { kNone, kSync, kCallback, kFuture };

  ReceiveQueue() = default;
  ~ReceiveQueue();

  ReceiveQueue(const ReceiveQueue&) = delete;
  ReceiveQueue& operator=(const ReceiveQueue&) = delete;

  // 生产侧：投递一条消息（成功或失败）
  void Push(Result<Message> msg);

  // 消费侧（三选一，互斥）
  Result<Message> Receive(uint32_t timeout_ms);  // 锁定 kSync
  Status SetCallback(Callback cb);               // 锁定 kCallback
  std::future<Result<Message>> AsyncReceive();   // 锁定 kFuture

  // 关闭：唤醒同步等待者、兑现未决 future，均以 conn: 错误结束
  void Close();

  Mode CurrentMode();

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  Mode mode_ = Mode::kNone;
  bool closed_ = false;

  std::deque<Result<Message>> messages_;               // kSync / kFuture 暂存
  Callback callback_;                                  // kCallback
  std::deque<std::promise<Result<Message>>> pending_;  // kFuture 未决
};

}  // namespace transport
```

- [ ] **Step 5: 写实现**

`src/core/ReceiveQueue.cpp`:

```cpp
#include "transport/core/ReceiveQueue.hpp"

#include <chrono>
#include <utility>
#include <variant>

namespace transport {

ReceiveQueue::~ReceiveQueue() { Close(); }

void ReceiveQueue::Push(Result<Message> msg) {
  Callback cb_copy;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) return;

    if (mode_ == Mode::kFuture && !pending_.empty()) {
      auto promise = std::move(pending_.front());
      pending_.pop_front();
      lock.unlock();
      promise.set_value(std::move(msg));
      return;
    }
    if (mode_ == Mode::kCallback) {
      cb_copy = callback_;  // 锁外调用，避免回调内重入死锁
    } else {
      messages_.push_back(std::move(msg));
      cv_.notify_one();
      return;
    }
  }
  if (cb_copy) cb_copy(std::move(msg));
}

Result<Message> ReceiveQueue::Receive(uint32_t timeout_ms) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (mode_ != Mode::kNone && mode_ != Mode::kSync) {
    return Result<Message>::Fail(
        "config: receive mode already set to a different mode");
  }
  mode_ = Mode::kSync;

  auto ready = [this] { return !messages_.empty() || closed_; };
  if (timeout_ms == 0) {
    cv_.wait(lock, ready);
  } else if (!cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), ready)) {
    return Result<Message>::Fail("timeout: receive timed out");
  }

  if (messages_.empty()) {  // 被 Close 唤醒
    return Result<Message>::Fail("conn: receive queue closed");
  }
  Result<Message> msg = std::move(messages_.front());
  messages_.pop_front();
  return msg;
}

Status ReceiveQueue::SetCallback(Callback cb) {
  std::deque<Result<Message>> backlog;
  Callback local;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) return Status::Fail("conn: receive queue closed");
    if (mode_ != Mode::kNone && mode_ != Mode::kCallback) {
      return Status::Fail("config: receive mode already set to a different mode");
    }
    mode_ = Mode::kCallback;
    callback_ = std::move(cb);
    local = callback_;
    backlog.swap(messages_);  // 设回调前积压的消息
  }
  for (auto& m : backlog) {
    if (local) local(std::move(m));
  }
  return Status::Success(std::monostate{});
}

std::future<Result<Message>> ReceiveQueue::AsyncReceive() {
  std::promise<Result<Message>> promise;
  auto fut = promise.get_future();

  std::lock_guard<std::mutex> lock(mutex_);
  if (mode_ != Mode::kNone && mode_ != Mode::kFuture) {
    promise.set_value(Result<Message>::Fail(
        "config: receive mode already set to a different mode"));
    return fut;
  }
  mode_ = Mode::kFuture;

  if (closed_) {
    promise.set_value(Result<Message>::Fail("conn: receive queue closed"));
  } else if (!messages_.empty()) {
    promise.set_value(std::move(messages_.front()));
    messages_.pop_front();
  } else {
    pending_.push_back(std::move(promise));
  }
  return fut;
}

void ReceiveQueue::Close() {
  std::deque<std::promise<Result<Message>>> pend;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) return;
    closed_ = true;
    pend.swap(pending_);
    cv_.notify_all();
  }
  for (auto& p : pend) {
    p.set_value(Result<Message>::Fail("conn: receive queue closed"));
  }
}

ReceiveQueue::Mode ReceiveQueue::CurrentMode() {
  std::lock_guard<std::mutex> lock(mutex_);
  return mode_;
}

}  // namespace transport
```

- [ ] **Step 6: 运行测试，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure`
Expected: `ReceiveQueue.*` 全部 PASS。

- [ ] **Step 7: 提交**

```bash
git add include/transport/core/ReceiveQueue.hpp src/core/ReceiveQueue.cpp \
        tests/core/receive_queue_test.cpp CMakeLists.txt
git commit -m "feat: ReceiveQueue FIFO 三模式接收交付"
```

---

## Task 7: `TransportBase`（ITransport 通用实现基类）

**Files:**
- Create: `include/transport/core/TransportBase.hpp`（header-only）
- Test: `tests/core/transport_base_test.cpp`
- Modify: `CMakeLists.txt`（仅加测试源）

- [ ] **Step 1: 写失败测试**

`tests/core/transport_base_test.cpp`:

```cpp
#include "transport/core/TransportBase.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Message;
using transport::Result;
using transport::Status;
using transport::TransportBase;

namespace {

// 暴露 protected 辅助方法的可测试子类
class FakeTransport : public TransportBase {
 public:
  Status Open() override { return Status::Success(std::monostate{}); }
  void Close() override { CloseQueue(); }
  bool IsOpen() const override { return true; }
  Status Send(const std::vector<uint8_t>& data) override {
    auto enc = EncodeForSend(data);
    if (!enc) return Status::Fail(enc.error);
    last_sent = enc.value;
    return Status::Success(std::monostate{});
  }

  // 把 protected 辅助暴露给测试
  void TestDeliver(std::vector<uint8_t> frame, const std::string& source,
                   const std::string& topic) {
    DeliverFrame(std::move(frame), source, topic);
  }
  void TestNotifyDisconnect(const std::string& reason) {
    NotifyDisconnect(reason);
  }

  std::vector<uint8_t> last_sent;
};

// 在每个字节上 +1（Encode）/ -1（Decode）的可逆 codec
class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(
      const std::vector<uint8_t>& data) override {
    std::vector<uint8_t> out = data;
    for (auto& b : out) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
  Result<std::vector<uint8_t>> Decode(
      const std::vector<uint8_t>& data) override {
    std::vector<uint8_t> out = data;
    for (auto& b : out) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
};

// 始终失败的 Decode，用于验证 codec 错误投递
class FailDecodeCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(
      const std::vector<uint8_t>& data) override {
    return Result<std::vector<uint8_t>>::Success(data);
  }
  Result<std::vector<uint8_t>> Decode(
      const std::vector<uint8_t>&) override {
    return Result<std::vector<uint8_t>>::Fail("codec: bad frame");
  }
};

}  // namespace

TEST(TransportBase, PassthroughDeliversRawBytes) {
  FakeTransport t;
  t.TestDeliver({10, 20, 30}, "1.2.3.4:5", "topicA");
  auto r = t.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_EQ(r.value.source, "1.2.3.4:5");
  EXPECT_EQ(r.value.topic, "topicA");
  EXPECT_GT(r.value.timestamp, 0);
}

TEST(TransportBase, EncodeForSendIsIdentityWithoutCodec) {
  FakeTransport t;
  auto st = t.Send({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(st));
  EXPECT_EQ(t.last_sent, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportBase, CodecAppliedOnSend) {
  FakeTransport t;
  t.SetCodec(std::make_shared<ShiftCodec>());
  auto st = t.Send({1, 2, 3});
  ASSERT_TRUE(static_cast<bool>(st));
  EXPECT_EQ(t.last_sent, (std::vector<uint8_t>{2, 3, 4}));
}

TEST(TransportBase, CodecAppliedOnReceive) {
  FakeTransport t;
  t.SetCodec(std::make_shared<ShiftCodec>());
  t.TestDeliver({2, 3, 4}, "src", "");
  auto r = t.Receive(100);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(TransportBase, DecodeFailureDeliversFail) {
  FakeTransport t;
  t.SetCodec(std::make_shared<FailDecodeCodec>());
  t.TestDeliver({9, 9}, "src", "");
  auto r = t.Receive(100);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("codec:", 0), 0u);
}

TEST(TransportBase, DisconnectCallbackInvoked) {
  FakeTransport t;
  std::string reason;
  t.OnDisconnect([&](const std::string& r) { reason = r; });
  t.TestNotifyDisconnect("conn: peer closed");
  EXPECT_EQ(reason, "conn: peer closed");
}
```

- [ ] **Step 2: 把测试源加入 CMake**

在 `add_executable(transport_tests ...)` 追加 `tests/core/transport_base_test.cpp`。

- [ ] **Step 3: 运行测试，确认失败**

Run: `cmake --build build -j 2>&1 | head -30`
Expected: 编译失败 —— 找不到 `TransportBase.hpp`。

- [ ] **Step 4: 写头文件（header-only）**

`include/transport/core/TransportBase.hpp`:

```cpp
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/core/ReceiveQueue.hpp"

namespace transport {

// ITransport 的通用实现：编解码挂载、三模式接收交付、断连通知。
// 子类只需实现 Open/Close/IsOpen/Send，并在收到字节时调用 DeliverFrame。
class TransportBase : public ITransport {
 public:
  void SetCodec(std::shared_ptr<ICodec> codec) override {
    codec_ = std::move(codec);
  }
  Result<Message> Receive(uint32_t timeout_ms) override {
    return queue_.Receive(timeout_ms);
  }
  void OnReceive(ReceiveCallback cb) override {
    queue_.SetCallback(std::move(cb));
  }
  std::future<Result<Message>> AsyncReceive() override {
    return queue_.AsyncReceive();
  }
  void OnDisconnect(DisconnectCallback cb) override {
    disconnect_cb_ = std::move(cb);
  }

 protected:
  // 发送前编码；无 codec 时透传。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data) {
    if (!codec_) return Result<std::vector<uint8_t>>::Success(data);
    return codec_->Encode(data);
  }

  // 收到一帧/一报文：解码（无 codec 透传）后构造 Message 投递；解码失败投递 Fail。
  void DeliverFrame(std::vector<uint8_t> frame, const std::string& source,
                    const std::string& topic) {
    std::vector<uint8_t> payload;
    if (codec_) {
      auto decoded = codec_->Decode(frame);
      if (!decoded) {
        queue_.Push(Result<Message>::Fail(decoded.error));
        return;
      }
      payload = std::move(decoded.value);
    } else {
      payload = std::move(frame);
    }
    Message msg;
    msg.payload = std::move(payload);
    msg.source = source;
    msg.topic = topic;
    msg.timestamp = NowMicros();
    queue_.Push(Result<Message>::Success(std::move(msg)));
  }

  // 投递一个连接级错误（如对端断开）到接收侧。
  void DeliverError(std::string error) {
    queue_.Push(Result<Message>::Fail(std::move(error)));
  }

  void NotifyDisconnect(const std::string& reason) {
    if (disconnect_cb_) disconnect_cb_(reason);
  }

  // 关闭接收队列（唤醒等待者）。子类 Close() 应调用。
  void CloseQueue() { queue_.Close(); }

  static int64_t NowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  std::shared_ptr<ICodec> codec_;
  ReceiveQueue queue_;
  DisconnectCallback disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure`
Expected: `TransportBase.*` 全部 PASS；全量测试套件绿色。

- [ ] **Step 6: 提交**

```bash
git add include/transport/core/TransportBase.hpp \
        tests/core/transport_base_test.cpp CMakeLists.txt
git commit -m "feat: TransportBase ITransport 通用实现基类"
```

---

## Task 8: Foundation 收尾验证

**Files:**
- 无新增（仅全量验证 + README 占位）
- Create: `README.md`

- [ ] **Step 1: 全量干净构建**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```
Expected: 全部测试 PASS（version / result / interfaces / length_field_framer / frame_assembler / receive_queue / transport_base）。

- [ ] **Step 2: 写 README 占位**

`README.md`:

```markdown
# transport — C++ 通信中间件

将数据传输与编解码解耦的 C++17 通信中间件库。支持 TCP / UDP / DDS / 串口。

## 构建

```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## 状态

- [x] Foundation：核心接口、分帧、接收交付、传输基类
- [ ] TCP（client / server）
- [ ] UDP（单播 / 组播 / 广播）
- [ ] 串口
- [ ] DDS（Fast DDS，pub-sub / req-resp）
- [ ] TransportFactory + JSON 配置

设计文档见 `docs/superpowers/specs/2026-06-09-transport-middleware-design.md`。
```

- [ ] **Step 3: 提交**

```bash
git add README.md
git commit -m "docs: README 与构建说明（Foundation 完成）"
```

---

## 后续计划（不在本计划范围）

1. **TCP** — `TcpClientConfig`/`TcpServerConfig`、`ITcpServer`、`TcpClientImpl`/`TcpServerImpl`（Asio），继承 `TransportBase`，接收侧用 `FrameAssembler`。
2. **UDP** — `IUdpTransport` + `SendTo` + 单播/组播/广播。
3. **串口** — termios + pty 回环测试。
4. **DDS** — `RawMessage` + `FastDdsRawType`（自定义 TopicDataType）+ `FastDdsProvider` + `DdsTransport` + `DdsProviderRegistry` + `FakeDdsProvider`。
5. **TransportFactory + JSON 配置** —— 统一创建入口，串联全部传输。

每个后续子系统将各自走 spec→plan→实现循环（或直接基于本 spec 出 plan）。
