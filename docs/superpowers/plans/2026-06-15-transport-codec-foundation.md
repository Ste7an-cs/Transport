# 底层通信框架重构(Transport + ICodec 两层)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把富 `ITransport` 拆成两个解耦的底层层:`Transport`(纯字节管道)只收发裸字节;`ICodec`(线缆格式)只做 `Message ↔ 字节`(分帧+序列化+承载 `kind`/`correlation_id` 交互元数据)。上层 `System`(交互模式)留作后续。

**Architecture:** 自底向上三层 `Transport / ICodec / System`,本轮做下两层。先**拆除**旧的 `TransportCore`/`IFramer`/topic 路由/DDS/服务端(它们与新签名不兼容),把库收缩到只剩 `Result`/`Endpoint`/`Message`/`version`;再建 `ICodec` 层(3 个内置 codec);再建 `Transport` 层(TCP 客户端/串口/UDP 纯管道);最后用一个组合冒烟测试证明两层可拼。两层在代码上互不依赖。

**Tech Stack:** C++17;Standalone Asio(已 vendored,`third_party/asio`);GoogleTest 1.14(已 vendored);不抛异常(全程 `Result<T>`/`Status`)。

**配套 spec:** `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`

---

## 文件结构

**保留(不动或仅引用):**
- `include/transport/Result.hpp` — `Result<T>`/`Status`(已带 `[[nodiscard]]`)。
- `include/transport/Endpoint.hpp` — `Endpoint`。
- `include/transport/version.hpp` / `src/core/version.cpp`。

**修改:**
- `include/transport/Message.hpp` — 加 `MessageKind` 枚举 + `kind`/`correlation_id` 字段。
- `include/transport/ICodec.hpp` — 改为 `Encode(Message)→bytes` / `Decode(bytes)→0..N Message`。
- `include/transport/ITransport.hpp` — 改为纯字节管道接口。
- `CMakeLists.txt` — 删旧源/旧测试,加新源/新测试。
- 各 `*Config.hpp` — 删 `framer`/`enable_topic_routing` 字段。

**新建:**
- `include/transport/codec/SystemCodec.hpp` + `src/codec/SystemCodec.cpp`
- `include/transport/codec/LengthFieldCodec.hpp` + `src/codec/LengthFieldCodec.cpp`
- `include/transport/codec/DatagramCodec.hpp`(header-only)
- `include/transport/tcp/TcpClientTransport.hpp` + `src/tcp/TcpClientTransport.cpp`
- `include/transport/serial/SerialTransport.hpp` + `src/serial/SerialTransport.cpp`
- `include/transport/udp/UdpTransport.hpp` + `src/udp/UdpTransport.cpp`
- `tests/codec/system_codec_test.cpp`、`tests/codec/length_field_codec_test.cpp`、`tests/codec/datagram_codec_test.cpp`
- `tests/transport/udp_transport_test.cpp`、`tests/transport/tcp_transport_test.cpp`、`tests/transport/serial_transport_test.cpp`
- `tests/transport/combination_smoke_test.cpp`

**删除(连同其测试):**
- `include/transport/core/TransportCore.hpp`、`ReceiveQueue.hpp`、`TopicEnvelope.hpp`、`StreamSend.hpp`;`src/core/ReceiveQueue.cpp`
- `include/transport/IFramer.hpp`、`framing/LengthFieldFramer.hpp`、`framing/FrameAssembler.hpp`;`src/framing/LengthFieldFramer.cpp`
- `include/transport/tcp/TcpConnectionImpl.hpp`、`TcpClientImpl.hpp`、`TcpServerImpl.hpp`、`ITcpServer.hpp`、`TcpServerConfig.hpp`;对应 `src/tcp/*Impl.cpp`
- `include/transport/udp/UdpImpl.hpp`;`src/udp/UdpImpl.cpp`
- `include/transport/serial/SerialImpl.hpp`;`src/serial/SerialImpl.cpp`
- `include/transport/dds/*`(全部);`src/dds/*`(全部)
- `include/transport/TransportFactory.hpp`;`src/TransportFactory.cpp`(本轮移除;transport 直接 `make_shared` 构造即可,精简工厂留后续按需)
- `tests/` 下:`framing/`、`core/receive_queue_test.cpp`、`core/transport_core_test.cpp`、`tcp/`、`udp/`、`serial/`、`dds/`、`factory/`、`interfaces_test.cpp`(旧富接口断言)

---

## Task 1: 拆除旧架构 + 收缩 CMake 到最小可构建

**Files:**
- Delete: 见上「删除」清单(全部旧实现 + 旧测试)
- Modify: `CMakeLists.txt`
- Keep: `Result.hpp`、`Endpoint.hpp`、`version.hpp`、`src/core/version.cpp`、`tests/version_test.cpp`、`tests/result_test.cpp`

- [ ] **Step 1: 删除旧实现与旧测试文件**

```bash
cd /home/ubuntu/david/transport
git rm -r \
  include/transport/core/TransportCore.hpp include/transport/core/ReceiveQueue.hpp \
  include/transport/core/TopicEnvelope.hpp include/transport/core/StreamSend.hpp \
  src/core/ReceiveQueue.cpp \
  include/transport/IFramer.hpp \
  include/transport/framing/LengthFieldFramer.hpp include/transport/framing/FrameAssembler.hpp \
  src/framing/LengthFieldFramer.cpp \
  include/transport/tcp/TcpConnectionImpl.hpp include/transport/tcp/TcpClientImpl.hpp \
  include/transport/tcp/TcpServerImpl.hpp include/transport/tcp/ITcpServer.hpp \
  include/transport/tcp/TcpServerConfig.hpp \
  src/tcp/TcpConnectionImpl.cpp src/tcp/TcpClientImpl.cpp src/tcp/TcpServerImpl.cpp \
  include/transport/udp/UdpImpl.hpp src/udp/UdpImpl.cpp \
  include/transport/serial/SerialImpl.hpp src/serial/SerialImpl.cpp \
  include/transport/TransportFactory.hpp src/TransportFactory.cpp \
  tests/interfaces_test.cpp \
  tests/framing tests/core/receive_queue_test.cpp tests/core/transport_core_test.cpp \
  tests/tcp tests/udp tests/serial tests/dds tests/factory
git rm -r include/transport/dds src/dds
```

> 若个别路径不存在(例如 `tests/core/` 下还有其他文件),用 `git rm` 单独删可存在的,跳过不存在的;目标是清掉上面「文件结构/删除」清单里的全部条目。`ITransport.hpp`/`ICodec.hpp`/`Message.hpp` **保留**(后续任务改写)。各 `*Config.hpp`(`TcpClientConfig.hpp`/`UdpConfig.hpp`/`SerialConfig.hpp`)**保留**(后续任务改字段)。

- [ ] **Step 2: 临时改写 ITransport.hpp / ICodec.hpp 为空壳,避免悬空引用**

把 `include/transport/ITransport.hpp` 暂时替换为最小占位(Task 7 再定型):

```cpp
#pragma once
// 占位:纯字节管道接口在 Task 7 定型。
namespace transport {
class ITransport;
}  // namespace transport
```

把 `include/transport/ICodec.hpp` 暂时替换为最小占位(Task 3 再定型):

```cpp
#pragma once
// 占位:线缆格式接口在 Task 3 定型。
namespace transport {
class ICodec;
}  // namespace transport
```

- [ ] **Step 3: 改写 CMakeLists.txt 为最小可构建**

把 `CMakeLists.txt` 整体替换为:

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
target_include_directories(transport PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_include_directories(transport PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(transport PUBLIC cxx_std_17)

find_package(Threads REQUIRED)

# asio (standalone, header-only) —— vendored
add_library(asio_standalone INTERFACE)
target_include_directories(asio_standalone INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/third_party/asio/include)
target_compile_definitions(asio_standalone INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(asio_standalone INTERFACE Threads::Threads)
target_link_libraries(transport PUBLIC asio_standalone)
target_link_libraries(transport PUBLIC Threads::Threads)

if(TRANSPORT_BUILD_TESTS)
  enable_testing()
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  add_subdirectory(third_party/googletest)
  include(GoogleTest)

  add_executable(transport_tests
    tests/version_test.cpp
    tests/result_test.cpp
  )
  target_link_libraries(transport_tests PRIVATE transport GTest::gtest_main)
  gtest_discover_tests(transport_tests)
endif()
```

> 注:旧 CMake 引用了 `util`(`target_link_libraries(transport_tests PRIVATE util)`)与 FastDDS 分支,新版一并去掉。

- [ ] **Step 4: 配置、构建、运行测试,确认绿**

Run:
```bash
cd /home/ubuntu/david/transport
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | tail -2
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 构建成功;`100% tests passed`(只剩 `version`/`result` 用例)。

- [ ] **Step 5: 提交**

```bash
git add -A
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "refactor!: 拆除旧 TransportCore/IFramer/topic路由/DDS/服务端,收缩到最小可构建(0.2.0 重构起点)"
```

---

## Task 2: `Message` 加交互元数据

**Files:**
- Modify: `include/transport/Message.hpp`
- Test: `tests/codec/message_test.cpp`(新建)

- [ ] **Step 1: 写失败测试**

新建 `tests/codec/message_test.cpp`:

```cpp
#include "transport/Message.hpp"

#include <gtest/gtest.h>

using transport::Message;
using transport::MessageKind;

TEST(Message, DefaultsToOnewayEmptyCorrelation) {
  Message m;
  EXPECT_EQ(m.kind, MessageKind::kOneway);
  EXPECT_TRUE(m.correlation_id.empty());
  EXPECT_TRUE(m.payload.empty());
  EXPECT_TRUE(m.topic.empty());
}

TEST(Message, HoldsKindAndCorrelation) {
  Message m;
  m.kind = MessageKind::kRequest;
  m.correlation_id = "req-1";
  m.payload = {1, 2, 3};
  m.topic = "calc";
  EXPECT_EQ(m.kind, MessageKind::kRequest);
  EXPECT_EQ(m.correlation_id, "req-1");
  EXPECT_EQ(m.payload, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(m.topic, "calc");
}
```

把测试加入 `CMakeLists.txt` 的 `add_executable(transport_tests ...)`:在 `tests/result_test.cpp` 后加一行 `tests/codec/message_test.cpp`。

- [ ] **Step 2: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 编译失败 —— `MessageKind` 未定义。

- [ ] **Step 3: 改写 Message.hpp**

把 `include/transport/Message.hpp` 整体替换为:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// Message.hpp — 一条消息的数据模型(含交互元数据)
// payload/topic + kind/correlation_id(交互语义,由 ICodec 上线缆,由 System 消费)。
// source/timestamp 由上层 System 填(本轮底层留默认)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

enum class MessageKind {
  kOneway,    // 单向(无需应答)
  kRequest,   // 请求(期待应答/反馈)
  kReply,     // 终结应答(请求-应答的应答,或请求-结果反馈的最终结果)
  kFeedback,  // 中间结果反馈(可多次,非终结)
  kNotify,    // 订阅通知(主动推送)
};

struct Message {
  std::vector<uint8_t> payload;                 // 应用字节
  std::string topic;                            // 操作/通道名;否则空
  std::string source;                           // 来源标识;本轮底层留空
  int64_t timestamp = 0;                        // 本轮留 0
  MessageKind kind = MessageKind::kOneway;      // 交互种类
  std::string correlation_id;                   // 配对请求↔应答/反馈;非请求为空
};

}  // namespace transport
```

- [ ] **Step 4: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R Message 2>&1 | grep -iE "passed|failed"`
Expected: `Message.*` 全通过。

- [ ] **Step 5: 提交**

```bash
git add include/transport/Message.hpp tests/codec/message_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: Message 加 MessageKind + kind/correlation_id 交互元数据"
```

---

## Task 3: `ICodec` 新接口

**Files:**
- Modify: `include/transport/ICodec.hpp`

- [ ] **Step 1: 改写 ICodec.hpp**

把 `include/transport/ICodec.hpp` 整体替换为:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// ICodec.hpp — 线缆格式扩展点(分帧 + 序列化 + 承载交互元数据)
// Encode: 一条 Message → 一段线缆字节(一对一)。
// Decode: 喂入字节切片 → 切出 0..N 条完整 Message(内部维护滚动缓冲,有状态)。
// 不依赖 Transport;由(未来)transport io 线程单线程喂,无需线程安全。
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/Message.hpp"
#include "transport/Result.hpp"

namespace transport {

class ICodec {
 public:
  virtual ~ICodec() = default;

  // 发:一条消息 → 一段线缆字节。
  virtual Result<std::vector<uint8_t>> Encode(const Message& msg) = 0;

  // 收:喂入字节切片 → 切出 0..N 条完整消息(半包返回空、粘包返回多条)。
  // 解析错误(坏帧头/越界)→ Fail("frame: ..." / "codec: ...")。
  virtual Result<std::vector<Message>> Decode(const uint8_t* data,
                                              std::size_t len) = 0;
};

}  // namespace transport
```

- [ ] **Step 2: 构建,确认仍绿(无实现引用此接口)**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -2`
Expected: 构建成功(接口纯头,无人实现暂不影响)。

- [ ] **Step 3: 提交**

```bash
git add include/transport/ICodec.hpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ICodec 改为 Encode(Message)->bytes / Decode(bytes)->0..N Message"
```

---

## Task 4: `LengthFieldCodec`(吸收长度字段分帧)

**Files:**
- Create: `include/transport/codec/LengthFieldCodec.hpp`、`src/codec/LengthFieldCodec.cpp`
- Test: `tests/codec/length_field_codec_test.cpp`
- Modify: `CMakeLists.txt`

行为:`Encode(msg)` 透传 `msg.payload`(用户帧含自身 header/长度);`Decode` = 滚动缓冲 + 「固定 header+长度字段」切帧,每帧 → `Message{payload=frame, kind=kOneway}`。

- [ ] **Step 1: 写失败测试**

新建 `tests/codec/length_field_codec_test.cpp`:

```cpp
#include "transport/codec/LengthFieldCodec.hpp"

#include <gtest/gtest.h>

using transport::LengthFieldCodec;
using transport::LengthFieldCodecConfig;
using transport::Message;

namespace {
// header: [type:4][len:4 BE]，len 为 body 长(不含 header)。
LengthFieldCodecConfig BeCfg() {
  LengthFieldCodecConfig c;
  c.header_size = 8; c.length_offset = 4; c.length_size = 4;
  c.big_endian = true; c.length_includes_header = false;
  return c;
}
// 造一帧:8 字节 header（type=0,len=n）+ n 字节 body(填 fill)
std::vector<uint8_t> Frame(uint32_t n, uint8_t fill) {
  std::vector<uint8_t> f(8 + n, fill);
  f[0]=f[1]=f[2]=f[3]=0;
  f[4]=(n>>24)&0xFF; f[5]=(n>>16)&0xFF; f[6]=(n>>8)&0xFF; f[7]=n&0xFF;
  return f;
}
}  // namespace

TEST(LengthFieldCodec, EncodePassesThroughPayload) {
  LengthFieldCodec codec(BeCfg());
  Message m; m.payload = {1, 2, 3};
  auto r = codec.Encode(m);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(LengthFieldCodec, DecodeSingleFrame) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(3, 0xAB);
  auto r = codec.Decode(frame.data(), frame.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 1u);
  EXPECT_EQ(r.value[0].payload, frame);     // 整帧(含 header)作为 payload
  EXPECT_EQ(r.value[0].kind, transport::MessageKind::kOneway);
}

TEST(LengthFieldCodec, DecodeAcrossPartialFeeds) {
  LengthFieldCodec codec(BeCfg());
  auto frame = Frame(5, 0x11);
  auto r1 = codec.Decode(frame.data(), 6);          // 半包
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value.empty());
  auto r2 = codec.Decode(frame.data() + 6, frame.size() - 6);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0].payload, frame);
}

TEST(LengthFieldCodec, DecodeGluedFrames) {
  LengthFieldCodec codec(BeCfg());
  auto a = Frame(2, 0x01), b = Frame(3, 0x02);
  std::vector<uint8_t> glued = a; glued.insert(glued.end(), b.begin(), b.end());
  auto r = codec.Decode(glued.data(), glued.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0].payload, a);
  EXPECT_EQ(r.value[1].payload, b);
}

TEST(LengthFieldCodec, DecodeOversizeFails) {
  LengthFieldCodecConfig c = BeCfg(); c.max_frame_size = 8;  // 任何带 body 的帧都越界
  LengthFieldCodec codec(c);
  auto frame = Frame(100, 0x44);
  auto r = codec.Decode(frame.data(), frame.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
```

把测试加入 `CMakeLists.txt` 的 `transport_tests` 源列表(`tests/codec/length_field_codec_test.cpp`)。

- [ ] **Step 2: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 编译失败 —— 找不到 `transport/codec/LengthFieldCodec.hpp`。

- [ ] **Step 3: 写 LengthFieldCodec.hpp**

新建 `include/transport/codec/LengthFieldCodec.hpp`:

```cpp
#pragma once

// LengthFieldCodec.hpp — 「固定 header + 长度字段」分帧 codec。
// Encode 透传 payload;Decode 用滚动缓冲按长度字段切帧,每帧作为一条 kOneway Message。
// 由原 LengthFieldFramer + FrameAssembler 合并而来。

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Result.hpp"

namespace transport {

struct LengthFieldCodecConfig {
  std::size_t header_size = 0;
  std::size_t length_offset = 0;
  std::size_t length_size = 4;            // 2 / 4 / 8
  bool big_endian = true;
  bool length_includes_header = false;
  std::size_t max_frame_size = 16 * 1024 * 1024;
};

class LengthFieldCodec : public ICodec {
 public:
  // 配置非法(header_size==0 / length_size∉{2,4,8} / 长度字段越出 header /
  // max_frame_size < header_size)→ 构造仍成功,但首次 Decode 返回 config: 错误。
  explicit LengthFieldCodec(LengthFieldCodecConfig config);

  Result<std::vector<uint8_t>> Encode(const Message& msg) override;
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override;

 private:
  LengthFieldCodecConfig config_;
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
```

- [ ] **Step 4: 写 LengthFieldCodec.cpp**

新建 `src/codec/LengthFieldCodec.cpp`:

```cpp
#include "transport/codec/LengthFieldCodec.hpp"

#include <utility>

// LengthFieldCodec.cpp — 见 LengthFieldCodec.hpp。
// Decode:把字节追加进滚动缓冲,循环按 header 内长度字段切出完整帧。

namespace transport {

namespace {
Status ValidateConfig(const LengthFieldCodecConfig& c) {
  if (c.header_size == 0) return Status::Fail("config: header_size must be > 0");
  if (c.length_size != 2 && c.length_size != 4 && c.length_size != 8)
    return Status::Fail("config: length_size must be 2, 4, or 8");
  if (c.length_offset + c.length_size > c.header_size)
    return Status::Fail("config: length field exceeds header_size");
  if (c.max_frame_size < c.header_size)
    return Status::Fail("config: max_frame_size smaller than header_size");
  return Status::Success(std::monostate{});
}
}  // namespace

LengthFieldCodec::LengthFieldCodec(LengthFieldCodecConfig config)
    : config_(config) {}

Result<std::vector<uint8_t>> LengthFieldCodec::Encode(const Message& msg) {
  return Result<std::vector<uint8_t>>::Success(msg.payload);  // 透传
}

Result<std::vector<Message>> LengthFieldCodec::Decode(const uint8_t* data,
                                                      std::size_t len) {
  using R = Result<std::vector<Message>>;
  if (auto v = ValidateConfig(config_); !v) return R::Fail(v.error);

  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  std::size_t offset = 0;
  while (buffer_.size() - offset >= config_.header_size) {
    const uint8_t* p = buffer_.data() + offset + config_.length_offset;
    uint64_t value = 0;
    if (config_.big_endian)
      for (std::size_t i = 0; i < config_.length_size; ++i)
        value = (value << 8) | static_cast<uint64_t>(p[i]);
    else
      for (std::size_t i = 0; i < config_.length_size; ++i)
        value |= static_cast<uint64_t>(p[i]) << (8 * i);

    const uint64_t frame_size =
        config_.length_includes_header ? value : config_.header_size + value;
    if (frame_size < config_.header_size)
      return R::Fail("frame: declared frame size smaller than header");
    if (frame_size > config_.max_frame_size)
      return R::Fail("frame: frame size exceeds max_frame_size");
    if (buffer_.size() - offset < frame_size) break;  // 不足一帧,等更多

    Message m;
    m.payload.assign(buffer_.begin() + offset,
                     buffer_.begin() + offset + static_cast<std::size_t>(frame_size));
    out.push_back(std::move(m));
    offset += static_cast<std::size_t>(frame_size);
  }
  if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
  return R::Success(std::move(out));
}

}  // namespace transport
```

把 `src/codec/LengthFieldCodec.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 5: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R LengthFieldCodec 2>&1 | grep -iE "passed|failed"`
Expected: `LengthFieldCodec.*` 全通过。

- [ ] **Step 6: 提交**

```bash
git add include/transport/codec/LengthFieldCodec.hpp src/codec/LengthFieldCodec.cpp tests/codec/length_field_codec_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: LengthFieldCodec(长度字段分帧,吸收 LengthFieldFramer+FrameAssembler)"
```

---

## Task 5: `SystemCodec`(默认线缆格式,承载交互元数据)

**Files:**
- Create: `include/transport/codec/SystemCodec.hpp`、`src/codec/SystemCodec.cpp`
- Test: `tests/codec/system_codec_test.cpp`
- Modify: `CMakeLists.txt`

线缆格式:`[frame_len:4 BE][kind:1][corr_len:2 BE][corr_id][topic_len:2 BE][topic][payload...]`,`frame_len` 为其后全部字节数。kind 字节:0=kOneway,1=kRequest,2=kReply,3=kFeedback,4=kNotify。

- [ ] **Step 1: 写失败测试**

新建 `tests/codec/system_codec_test.cpp`:

```cpp
#include "transport/codec/SystemCodec.hpp"

#include <gtest/gtest.h>

using transport::Message;
using transport::MessageKind;
using transport::SystemCodec;

namespace {
Message Make(MessageKind k, std::string corr, std::string topic,
             std::vector<uint8_t> payload) {
  Message m; m.kind = k; m.correlation_id = std::move(corr);
  m.topic = std::move(topic); m.payload = std::move(payload);
  return m;
}
}  // namespace

TEST(SystemCodec, RoundtripAllFields) {
  SystemCodec enc, dec;
  auto m = Make(MessageKind::kRequest, "req-1", "calc", {9, 8, 7});
  auto bytes = enc.Encode(m);
  ASSERT_TRUE(static_cast<bool>(bytes));
  auto msgs = dec.Decode(bytes.value.data(), bytes.value.size());
  ASSERT_TRUE(static_cast<bool>(msgs));
  ASSERT_EQ(msgs.value.size(), 1u);
  EXPECT_EQ(msgs.value[0].kind, MessageKind::kRequest);
  EXPECT_EQ(msgs.value[0].correlation_id, "req-1");
  EXPECT_EQ(msgs.value[0].topic, "calc");
  EXPECT_EQ(msgs.value[0].payload, (std::vector<uint8_t>{9, 8, 7}));
}

TEST(SystemCodec, RoundtripEmptyCorrelationAndTopic) {
  SystemCodec enc, dec;
  auto m = Make(MessageKind::kOneway, "", "", {1});
  auto bytes = enc.Encode(m);
  ASSERT_TRUE(static_cast<bool>(bytes));
  auto msgs = dec.Decode(bytes.value.data(), bytes.value.size());
  ASSERT_TRUE(static_cast<bool>(msgs));
  ASSERT_EQ(msgs.value.size(), 1u);
  EXPECT_EQ(msgs.value[0].kind, MessageKind::kOneway);
  EXPECT_TRUE(msgs.value[0].correlation_id.empty());
  EXPECT_TRUE(msgs.value[0].topic.empty());
  EXPECT_EQ(msgs.value[0].payload, (std::vector<uint8_t>{1}));
}

TEST(SystemCodec, DecodeAcrossPartialFeeds) {
  SystemCodec enc, dec;
  auto bytes = enc.Encode(Make(MessageKind::kReply, "r", "t", {5, 6}));
  ASSERT_TRUE(static_cast<bool>(bytes));
  const auto& b = bytes.value;
  auto r1 = dec.Decode(b.data(), 3);                 // 半包
  ASSERT_TRUE(static_cast<bool>(r1));
  EXPECT_TRUE(r1.value.empty());
  auto r2 = dec.Decode(b.data() + 3, b.size() - 3);
  ASSERT_TRUE(static_cast<bool>(r2));
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0].correlation_id, "r");
}

TEST(SystemCodec, DecodeGluedFrames) {
  SystemCodec enc, dec;
  auto a = enc.Encode(Make(MessageKind::kFeedback, "c", "t", {1}));
  auto bb = enc.Encode(Make(MessageKind::kReply, "c", "t", {2}));
  ASSERT_TRUE(static_cast<bool>(a)); ASSERT_TRUE(static_cast<bool>(bb));
  std::vector<uint8_t> glued = a.value;
  glued.insert(glued.end(), bb.value.begin(), bb.value.end());
  auto r = dec.Decode(glued.data(), glued.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0].kind, MessageKind::kFeedback);
  EXPECT_EQ(r.value[1].kind, MessageKind::kReply);
}

TEST(SystemCodec, DecodeBadKindByteFails) {
  SystemCodec dec;
  // frame_len=1, body=[kind=9](非法)
  std::vector<uint8_t> bad = {0, 0, 0, 1, 9};
  auto r = dec.Decode(bad.data(), bad.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("codec:", 0), 0u);
}

TEST(SystemCodec, DecodeInnerLengthExceedsFrameFails) {
  SystemCodec dec;
  // frame_len=4, body=[kind=0][corr_len=0xFFFF...] 内层长度越界
  std::vector<uint8_t> bad = {0, 0, 0, 4, 0, 0xFF, 0xFF, 0x00};
  auto r = dec.Decode(bad.data(), bad.size());
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
```

把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 2: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 编译失败 —— 找不到 `transport/codec/SystemCodec.hpp`。

- [ ] **Step 3: 写 SystemCodec.hpp**

新建 `include/transport/codec/SystemCodec.hpp`:

```cpp
#pragma once

// SystemCodec.hpp — 默认线缆格式,承载完整交互元数据。
// [frame_len:4 BE][kind:1][corr_len:2 BE][corr_id][topic_len:2 BE][topic][payload]
// frame_len = 其后全部字节数(不含自身 4 字节)。流式按 frame_len 切帧,报文式整段一条。

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Result.hpp"

namespace transport {

class SystemCodec : public ICodec {
 public:
  static constexpr std::size_t kMaxFrame = 16 * 1024 * 1024;

  Result<std::vector<uint8_t>> Encode(const Message& msg) override;
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override;

 private:
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
```

- [ ] **Step 4: 写 SystemCodec.cpp**

新建 `src/codec/SystemCodec.cpp`:

```cpp
#include "transport/codec/SystemCodec.hpp"

#include <utility>

// SystemCodec.cpp — 见 SystemCodec.hpp。

namespace transport {

namespace {

void PutU16(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(v & 0xFF));
}
void PutU32(std::vector<uint8_t>& out, uint32_t v) {
  out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
  out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
  out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(v & 0xFF));
}
uint16_t GetU16(const uint8_t* p) {
  return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}
uint32_t GetU32(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

uint8_t KindToByte(MessageKind k) { return static_cast<uint8_t>(k); }

bool ByteToKind(uint8_t b, MessageKind* out) {
  if (b > static_cast<uint8_t>(MessageKind::kNotify)) return false;
  *out = static_cast<MessageKind>(b);
  return true;
}

}  // namespace

Result<std::vector<uint8_t>> SystemCodec::Encode(const Message& msg) {
  std::vector<uint8_t> body;
  body.push_back(KindToByte(msg.kind));
  PutU16(body, static_cast<uint16_t>(msg.correlation_id.size()));
  body.insert(body.end(), msg.correlation_id.begin(), msg.correlation_id.end());
  PutU16(body, static_cast<uint16_t>(msg.topic.size()));
  body.insert(body.end(), msg.topic.begin(), msg.topic.end());
  body.insert(body.end(), msg.payload.begin(), msg.payload.end());

  std::vector<uint8_t> out;
  out.reserve(4 + body.size());
  PutU32(out, static_cast<uint32_t>(body.size()));
  out.insert(out.end(), body.begin(), body.end());
  return Result<std::vector<uint8_t>>::Success(std::move(out));
}

Result<std::vector<Message>> SystemCodec::Decode(const uint8_t* data,
                                                 std::size_t len) {
  using R = Result<std::vector<Message>>;
  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  std::size_t offset = 0;
  while (buffer_.size() - offset >= 4) {
    const std::size_t frame_len = GetU32(buffer_.data() + offset);
    if (frame_len > kMaxFrame) return R::Fail("frame: frame length exceeds max");
    if (buffer_.size() - offset - 4 < frame_len) break;  // 不足一帧

    const uint8_t* b = buffer_.data() + offset + 4;
    std::size_t pos = 0;
    auto need = [&](std::size_t n) { return pos + n <= frame_len; };

    if (!need(1)) return R::Fail("frame: frame too short for kind");
    MessageKind kind;
    if (!ByteToKind(b[pos], &kind)) return R::Fail("codec: unknown message kind");
    pos += 1;

    if (!need(2)) return R::Fail("frame: corr_len exceeds frame");
    const std::size_t corr_len = GetU16(b + pos); pos += 2;
    if (!need(corr_len)) return R::Fail("frame: corr_id exceeds frame");
    std::string corr(reinterpret_cast<const char*>(b + pos), corr_len);
    pos += corr_len;

    if (!need(2)) return R::Fail("frame: topic_len exceeds frame");
    const std::size_t topic_len = GetU16(b + pos); pos += 2;
    if (!need(topic_len)) return R::Fail("frame: topic exceeds frame");
    std::string topic(reinterpret_cast<const char*>(b + pos), topic_len);
    pos += topic_len;

    Message m;
    m.kind = kind;
    m.correlation_id = std::move(corr);
    m.topic = std::move(topic);
    m.payload.assign(b + pos, b + frame_len);
    out.push_back(std::move(m));

    offset += 4 + frame_len;
  }
  if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
  return R::Success(std::move(out));
}

}  // namespace transport
```

把 `src/codec/SystemCodec.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 5: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R SystemCodec 2>&1 | grep -iE "passed|failed"`
Expected: `SystemCodec.*` 全通过。

- [ ] **Step 6: 提交**

```bash
git add include/transport/codec/SystemCodec.hpp src/codec/SystemCodec.cpp tests/codec/system_codec_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: SystemCodec(默认线缆格式,承载 kind+correlation_id+topic+payload)"
```

---

## Task 6: `DatagramCodec`(报文直通,header-only)

**Files:**
- Create: `include/transport/codec/DatagramCodec.hpp`
- Test: `tests/codec/datagram_codec_test.cpp`
- Modify: `CMakeLists.txt`

行为:`Encode(msg)` = `msg.payload`;`Decode(data,len)` = 一条 `Message{payload=copy, kind=kOneway}`(`len==0` 返回空)。

- [ ] **Step 1: 写失败测试**

新建 `tests/codec/datagram_codec_test.cpp`:

```cpp
#include "transport/codec/DatagramCodec.hpp"

#include <gtest/gtest.h>

using transport::DatagramCodec;
using transport::Message;

TEST(DatagramCodec, EncodePassthrough) {
  DatagramCodec c;
  Message m; m.payload = {7, 8, 9};
  auto r = c.Encode(m);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value, (std::vector<uint8_t>{7, 8, 9}));
}

TEST(DatagramCodec, DecodeWholeChunkAsOneMessage) {
  DatagramCodec c;
  std::vector<uint8_t> dg = {1, 2, 3, 4};
  auto r = c.Decode(dg.data(), dg.size());
  ASSERT_TRUE(static_cast<bool>(r));
  ASSERT_EQ(r.value.size(), 1u);
  EXPECT_EQ(r.value[0].payload, dg);
  EXPECT_EQ(r.value[0].kind, transport::MessageKind::kOneway);
}

TEST(DatagramCodec, DecodeEmptyYieldsNoMessage) {
  DatagramCodec c;
  auto r = c.Decode(nullptr, 0);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_TRUE(r.value.empty());
}
```

把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 2: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 找不到 `transport/codec/DatagramCodec.hpp`。

- [ ] **Step 3: 写 DatagramCodec.hpp**

新建 `include/transport/codec/DatagramCodec.hpp`:

```cpp
#pragma once

// DatagramCodec.hpp — 报文直通 codec(header-only)。报文式传输(UDP)无分帧:
// Encode 透传 payload;Decode 把整段字节当作一条 kOneway 消息。

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Result.hpp"

namespace transport {

class DatagramCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    return Result<std::vector<uint8_t>>::Success(msg.payload);
  }
  Result<std::vector<Message>> Decode(const uint8_t* data,
                                      std::size_t len) override {
    std::vector<Message> out;
    if (len > 0) {
      Message m;
      m.payload.assign(data, data + len);
      out.push_back(std::move(m));
    }
    return Result<std::vector<Message>>::Success(std::move(out));
  }
};

}  // namespace transport
```

- [ ] **Step 4: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R DatagramCodec 2>&1 | grep -iE "passed|failed"`
Expected: `DatagramCodec.*` 全通过。

- [ ] **Step 5: 提交**

```bash
git add include/transport/codec/DatagramCodec.hpp tests/codec/datagram_codec_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: DatagramCodec(报文直通,header-only)"
```

---

## Task 7: `ITransport` 纯字节管道接口

**Files:**
- Modify: `include/transport/ITransport.hpp`

- [ ] **Step 1: 改写 ITransport.hpp**

把 `include/transport/ITransport.hpp` 整体替换为:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// ITransport.hpp — 纯字节管道接口
// 只收发裸字节,不知道 Message/ICodec/分帧/交互模式。各实现自有 io 线程 + strand。
// 收侧经 OnBytes 回调(io 线程,串行)交付本次读到的字节切片(流式)或整 datagram(报文式)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/Result.hpp"

namespace transport {

class ITransport {
 public:
  virtual ~ITransport() = default;

  // bytes = 收到的字节(失败时为错误);from = 来源标识("ip:port"/设备路径),失败时空。
  using BytesCallback =
      std::function<void(Result<std::vector<uint8_t>> bytes, const std::string& from)>;

  virtual Status Open() = 0;
  virtual void   Close() = 0;
  virtual bool   IsOpen() const = 0;

  virtual Status Send(const std::vector<uint8_t>& bytes) = 0;
  virtual Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) = 0;

  virtual void OnBytes(BytesCallback cb) = 0;
  virtual void OnConnect(std::function<void()> cb) = 0;
  virtual void OnDisconnect(std::function<void(const std::string& reason)> cb) = 0;
};

}  // namespace transport
```

- [ ] **Step 2: 构建,确认仍绿**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -2`
Expected: 构建成功(暂无实现)。

- [ ] **Step 3: 提交**

```bash
git add include/transport/ITransport.hpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ITransport 改为纯字节管道接口(OnBytes/OnConnect/OnDisconnect)"
```

---

## Task 8: `UdpTransport`(纯管道,改写自 UdpImpl)

**Files:**
- Create: `include/transport/udp/UdpTransport.hpp`、`src/udp/UdpTransport.cpp`
- Modify: `include/transport/udp/UdpConfig.hpp`(删 `enable_topic_routing`)
- Test: `tests/transport/udp_transport_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 改 UdpConfig.hpp 删 topic 字段**

把 `include/transport/udp/UdpConfig.hpp` 中这一行删除:
```cpp
  bool        enable_topic_routing = false;  // 开启 topic→codec 多路复用
```

- [ ] **Step 2: 写失败测试**

新建 `tests/transport/udp_transport_test.cpp`:

```cpp
#include "transport/udp/UdpTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Message;
using transport::Result;
using transport::UdpConfig;
using transport::UdpTransport;

namespace {
// 同步收一条字节的辅助
struct Sink {
  std::mutex m; std::condition_variable cv;
  std::vector<uint8_t> last; std::string from; bool got = false;
  void Wire(std::shared_ptr<UdpTransport> t) {
    t->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string& f) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      last = r.value; from = f; got = true; cv.notify_all();
    });
  }
  bool Wait(int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [this] { return got; });
  }
};
}  // namespace

TEST(UdpTransport, UnicastLoopbackDelivery) {
  UdpConfig rxc; rxc.mode = transport::UdpMode::kUnicast;
  rxc.local_addr = "127.0.0.1"; rxc.local_port = 0;
  auto rx = std::make_shared<UdpTransport>(rxc);
  Sink sink; sink.Wire(rx);
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  const uint16_t rx_port = rx->LocalPort();
  ASSERT_GT(rx_port, 0);

  UdpConfig txc; txc.mode = transport::UdpMode::kUnicast;
  txc.local_addr = "127.0.0.1"; txc.local_port = 0;
  auto tx = std::make_shared<UdpTransport>(txc);
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(
      tx->Send({1, 2, 3}, Endpoint::Net("127.0.0.1", rx_port))));
  ASSERT_TRUE(sink.Wait(1000));
  EXPECT_EQ(sink.last, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_NE(sink.from.find("127.0.0.1:"), std::string::npos);
  tx->Close(); rx->Close();
}

TEST(UdpTransport, DefaultDestUsesRemoteConfig) {
  UdpConfig rxc; rxc.local_addr = "127.0.0.1"; rxc.local_port = 0;
  auto rx = std::make_shared<UdpTransport>(rxc);
  Sink sink; sink.Wire(rx);
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  const uint16_t rx_port = rx->LocalPort();

  UdpConfig txc; txc.local_addr = "127.0.0.1"; txc.local_port = 0;
  txc.remote_addr = "127.0.0.1"; txc.remote_port = rx_port;
  auto tx = std::make_shared<UdpTransport>(txc);
  ASSERT_TRUE(static_cast<bool>(tx->Open()));

  ASSERT_TRUE(static_cast<bool>(tx->Send({9})));  // 默认目的地
  ASSERT_TRUE(sink.Wait(1000));
  EXPECT_EQ(sink.last, (std::vector<uint8_t>{9}));
  tx->Close(); rx->Close();
}
```

把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 3: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 找不到 `transport/udp/UdpTransport.hpp`。

- [ ] **Step 4: 写 UdpTransport.hpp**

新建 `include/transport/udp/UdpTransport.hpp`:

```cpp
#pragma once

// UdpTransport.hpp — UDP 纯字节管道(单播/组播/广播)。改写自旧 UdpImpl:
// 去掉 TransportCore/codec/topic;收到的每个 datagram 经 OnBytes 交付裸字节 + from。

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/udp/UdpConfig.hpp"

namespace transport {

class UdpTransport : public ITransport,
                     public std::enable_shared_from_this<UdpTransport> {
 public:
  explicit UdpTransport(UdpConfig config);
  ~UdpTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override;

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

  uint16_t LocalPort() const { return local_port_; }

 private:
  void StartReceive();
  Result<asio::ip::udp::endpoint> ResolveDest(const Endpoint& to);
  Status SendRaw(std::vector<uint8_t> bytes, const asio::ip::udp::endpoint& dest);

  UdpConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::udp::socket socket_;
  asio::ip::udp::endpoint default_dest_;
  asio::ip::udp::endpoint recv_from_;
  std::array<uint8_t, 65536> recv_buf_;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  uint16_t local_port_ = 0;

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 5: 写 UdpTransport.cpp**

新建 `src/udp/UdpTransport.cpp`(改写自旧 `UdpImpl.cpp`,去掉 codec/topic):

```cpp
#include "transport/udp/UdpTransport.hpp"

#include <utility>
#include <variant>

// UdpTransport.cpp — UDP 纯字节管道。自有 io_context + 1 io 线程;收发经 strand。
// 报文保边界 → 每个 datagram 经 OnBytes 交付裸字节 + from("ip:port")。

namespace transport {

UdpTransport::UdpTransport(UdpConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

UdpTransport::~UdpTransport() { Close(); }

bool UdpTransport::IsOpen() const { return open_.load(); }

Status UdpTransport::Open() {
  asio::error_code ec;
  socket_.open(asio::ip::udp::v4(), ec);
  if (ec) return Status::Fail("config: udp open: " + ec.message());

  if (config_.mode == UdpMode::kBroadcast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
    socket_.set_option(asio::socket_base::broadcast(true), ec);
    if (ec) return Status::Fail("config: broadcast option: " + ec.message());
  } else if (config_.mode == UdpMode::kMulticast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
  }

  asio::ip::address local;
  if (config_.mode == UdpMode::kMulticast) {
    local = asio::ip::address_v4::any();
  } else {
    local = asio::ip::make_address(config_.local_addr, ec);
    if (ec) return Status::Fail("config: invalid local_addr");
  }
  socket_.bind(asio::ip::udp::endpoint(local, config_.local_port), ec);
  if (ec) return Status::Fail("config: bind: " + ec.message());

  if (config_.mode == UdpMode::kMulticast) {
    auto group = asio::ip::make_address(config_.multicast_group, ec);
    if (ec) return Status::Fail("config: invalid multicast_group");
    socket_.set_option(asio::ip::multicast::join_group(group), ec);
    if (ec) return Status::Fail("config: join_group: " + ec.message());
    asio::error_code ig;
    socket_.set_option(asio::ip::multicast::hops(config_.ttl), ig);
    socket_.set_option(asio::ip::multicast::enable_loopback(true), ig);
    default_dest_ = asio::ip::udp::endpoint(group, config_.remote_port);
  } else if (!config_.remote_addr.empty()) {
    auto raddr = asio::ip::make_address(config_.remote_addr, ec);
    if (ec) return Status::Fail("config: invalid remote_addr");
    default_dest_ = asio::ip::udp::endpoint(raddr, config_.remote_port);
  }

  asio::error_code lec;
  auto le = socket_.local_endpoint(lec);
  if (lec) return Status::Fail("config: local_endpoint: " + lec.message());
  local_port_ = le.port();
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartReceive();
  });
  return Status::Success(std::monostate{});
}

void UdpTransport::StartReceive() {
  auto self = shared_from_this();
  socket_.async_receive_from(
      asio::buffer(recv_buf_), recv_from_,
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (!open_.load()) return;
            if (ec) {
              if (ec == asio::error::operation_aborted) return;
              if (bytes_cb_)
                bytes_cb_(Result<std::vector<uint8_t>>::Fail("io: receive: " + ec.message()), "");
              StartReceive();
              return;
            }
            std::string from = recv_from_.address().to_string() + ":" +
                               std::to_string(recv_from_.port());
            std::vector<uint8_t> dg(recv_buf_.begin(), recv_buf_.begin() + n);
            if (bytes_cb_)
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(dg)), from);
            StartReceive();
          }));
}

Result<asio::ip::udp::endpoint> UdpTransport::ResolveDest(const Endpoint& to) {
  using R = Result<asio::ip::udp::endpoint>;
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return R::Success(default_dest_);
    case Endpoint::Kind::kNet: {
      asio::error_code ec;
      auto addr = asio::ip::make_address(to.host, ec);
      if (ec) return R::Fail("config: invalid address");
      return R::Success(asio::ip::udp::endpoint(addr, to.port));
    }
    case Endpoint::Kind::kTopic:
      return R::Fail("config: udp expects net endpoint");
  }
  return R::Fail("config: unknown endpoint kind");
}

Status UdpTransport::SendRaw(std::vector<uint8_t> bytes,
                             const asio::ip::udp::endpoint& dest) {
  if (!open_.load()) return Status::Fail("config: socket not open");
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf, dest]() {
    asio::error_code ec;
    socket_.send_to(asio::buffer(*buf), dest, 0, ec);
    if (ec && bytes_cb_)
      bytes_cb_(Result<std::vector<uint8_t>>::Fail("io: send: " + ec.message()), "");
  });
  return Status::Success(std::monostate{});
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes) {
  return SendRaw(bytes, default_dest_);
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  auto dest = ResolveDest(to);
  if (!dest) return Status::Fail(dest.error);
  return SendRaw(bytes, dest.value);
}

void UdpTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    socket_.close(ig);
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

把 `src/udp/UdpTransport.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 6: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R UdpTransport 2>&1 | grep -iE "passed|failed"`
Expected: `UdpTransport.*` 全通过。

- [ ] **Step 7: 提交**

```bash
git add include/transport/udp/UdpTransport.hpp src/udp/UdpTransport.cpp include/transport/udp/UdpConfig.hpp tests/transport/udp_transport_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: UdpTransport 纯字节管道(改写自 UdpImpl,去 codec/topic)"
```

---

## Task 9: `TcpClientTransport`(纯管道,合并 TcpClient+Connection)

**Files:**
- Create: `include/transport/tcp/TcpClientTransport.hpp`、`src/tcp/TcpClientTransport.cpp`
- Modify: `include/transport/tcp/TcpClientConfig.hpp`(删 `framer`/`enable_topic_routing` 若有)
- Test: `tests/transport/tcp_transport_test.cpp`
- Modify: `CMakeLists.txt`

> 旧设计中 `TcpClientImpl` 继承 `TcpConnectionImpl`(为与服务端共用)。本轮无服务端,**合并为单类** `TcpClientTransport`:自持 `io_context`+线程,connect+超时+指数退避重连,async_read_some 直接把字节切片经 `OnBytes` 交付(无分帧)。

- [ ] **Step 1: 改 TcpClientConfig.hpp**

打开 `include/transport/tcp/TcpClientConfig.hpp`,删除其中的 `std::optional<LengthFieldFramerConfig> framer;` 与 `bool enable_topic_routing` 字段及对 `framing/LengthFieldFramer.hpp` 的 `#include`(若存在)。保留 `host`/`port`/`connect_timeout_ms`/`auto_reconnect`。确认文件内容为:

```cpp
#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct TcpClientConfig {
  std::string host = "127.0.0.1";
  uint16_t    port = 0;
  uint32_t    connect_timeout_ms = 5000;
  bool        auto_reconnect = false;
};

}  // namespace transport
```

- [ ] **Step 2: 写失败测试**

新建 `tests/transport/tcp_transport_test.cpp`:

```cpp
#include "transport/tcp/TcpClientTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <asio.hpp>
#include <gtest/gtest.h>

using transport::Result;
using transport::TcpClientConfig;
using transport::TcpClientTransport;

namespace {
// 起一个回环 server,接受一个连接,回显收到的字节。返回监听端口。
struct EchoServer {
  asio::io_context ctx;
  asio::ip::tcp::acceptor acc{ctx};
  std::thread th;
  asio::ip::tcp::socket sock{ctx};
  std::array<uint8_t, 1024> buf{};
  uint16_t Start() {
    acc.open(asio::ip::tcp::v4());
    acc.bind(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0));
    acc.listen();
    uint16_t port = acc.local_endpoint().port();
    acc.async_accept(sock, [this](asio::error_code ec) {
      if (ec) return;
      DoRead();
    });
    th = std::thread([this] { ctx.run(); });
    return port;
  }
  void DoRead() {
    sock.async_read_some(asio::buffer(buf), [this](asio::error_code ec, std::size_t n) {
      if (ec) return;
      asio::write(sock, asio::buffer(buf, n));  // 回显
      DoRead();
    });
  }
  void Stop() { asio::post(ctx, [this] { ctx.stop(); }); if (th.joinable()) th.join(); }
};

struct Sink {
  std::mutex m; std::condition_variable cv;
  std::vector<uint8_t> acc; bool connected = false;
  void Wire(std::shared_ptr<TcpClientTransport> t) {
    t->OnConnect([this] { std::lock_guard<std::mutex> lk(m); connected = true; cv.notify_all(); });
    t->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string&) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      acc.insert(acc.end(), r.value.begin(), r.value.end());
      cv.notify_all();
    });
  }
  bool WaitBytes(std::size_t n, int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [&] { return acc.size() >= n; });
  }
};
}  // namespace

TEST(TcpClientTransport, ConnectSendEchoReceive) {
  EchoServer srv; uint16_t port = srv.Start();

  TcpClientConfig cfg; cfg.host = "127.0.0.1"; cfg.port = port;
  auto t = std::make_shared<TcpClientTransport>(cfg);
  Sink sink; sink.Wire(t);
  ASSERT_TRUE(static_cast<bool>(t->Open()));

  ASSERT_TRUE(static_cast<bool>(t->Send({10, 20, 30})));
  ASSERT_TRUE(sink.WaitBytes(3, 1000));
  EXPECT_EQ(sink.acc, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_TRUE(sink.connected);

  t->Close();
  srv.Stop();
}

TEST(TcpClientTransport, ConnectRefusedFails) {
  TcpClientConfig cfg; cfg.host = "127.0.0.1"; cfg.port = 1;  // 几乎必拒
  cfg.connect_timeout_ms = 500;
  auto t = std::make_shared<TcpClientTransport>(cfg);
  auto st = t->Open();
  EXPECT_FALSE(static_cast<bool>(st));   // conn:/timeout:
  t->Close();
}
```

把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 3: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 找不到 `transport/tcp/TcpClientTransport.hpp`。

- [ ] **Step 4: 写 TcpClientTransport.hpp**

新建 `include/transport/tcp/TcpClientTransport.hpp`:

```cpp
#pragma once

// TcpClientTransport.hpp — TCP 客户端纯字节管道(合并旧 TcpClientImpl+TcpConnectionImpl)。
// 自有 io_context + 1 线程;connect + 连接超时 + 指数退避自动重连;
// async_read_some 把字节切片经 OnBytes 交付(无分帧)。须以 shared_ptr 持有。

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/tcp/TcpClientConfig.hpp"

namespace transport {

class TcpClientTransport : public ITransport,
                           public std::enable_shared_from_this<TcpClientTransport> {
 public:
  explicit TcpClientTransport(
      TcpClientConfig config,
      std::chrono::milliseconds backoff_base = std::chrono::milliseconds(1000),
      std::chrono::milliseconds backoff_cap = std::chrono::milliseconds(30000));
  ~TcpClientTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  void StartConnect(std::shared_ptr<std::promise<Status>> prom);
  void ScheduleReconnect();
  void StartRead();
  void HandleDisconnect(const std::string& reason);
  void DoWrite();
  void EnqueueWrite(std::vector<uint8_t> bytes);

  TcpClientConfig config_;
  std::chrono::milliseconds backoff_base_, backoff_cap_, backoff_cur_;

  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::ip::tcp::socket socket_;
  asio::ip::tcp::resolver resolver_;
  asio::steady_timer connect_timer_, reconnect_timer_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::string peer_id_;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> link_up_{false};

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 5: 写 TcpClientTransport.cpp**

新建 `src/tcp/TcpClientTransport.cpp`(综合旧 `TcpClientImpl.cpp` 的连接/重连 + 旧 `TcpConnectionImpl.cpp` 的读写,去分帧/codec):

```cpp
#include "transport/tcp/TcpClientTransport.hpp"

#include <algorithm>
#include <future>
#include <utility>
#include <variant>

// TcpClientTransport.cpp — 见 .hpp。自有 io 线程;connect+超时+退避重连;
// 读到的字节切片经 OnBytes 直接交付(无分帧);写经 strand 串行 write_queue。

namespace transport {

TcpClientTransport::TcpClientTransport(TcpClientConfig config,
                                       std::chrono::milliseconds backoff_base,
                                       std::chrono::milliseconds backoff_cap)
    : config_(std::move(config)),
      backoff_base_(backoff_base),
      backoff_cap_(backoff_cap),
      backoff_cur_(backoff_base),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_),
      resolver_(ctx_),
      connect_timer_(ctx_),
      reconnect_timer_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

TcpClientTransport::~TcpClientTransport() { Close(); }

Status TcpClientTransport::Open() {
  auto prom = std::make_shared<std::promise<Status>>();
  auto fut = prom->get_future();
  auto self = shared_from_this();
  asio::post(strand_, [this, self, prom]() { StartConnect(prom); });
  return fut.get();
}

void TcpClientTransport::StartConnect(std::shared_ptr<std::promise<Status>> prom) {
  auto self = shared_from_this();
  asio::error_code rec;
  auto endpoints = resolver_.resolve(config_.host, std::to_string(config_.port), rec);
  if (rec) {
    if (prom) prom->set_value(Status::Fail("conn: resolve: " + rec.message()));
    else ScheduleReconnect();
    return;
  }

  asio::error_code ig;
  socket_.close(ig);
  socket_ = asio::ip::tcp::socket(ctx_);

  auto timed_out = std::make_shared<bool>(false);
  connect_timer_.expires_after(std::chrono::milliseconds(config_.connect_timeout_ms));
  connect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self, timed_out](asio::error_code ec) {
        if (ec) return;
        *timed_out = true;
        asio::error_code ig2;
        socket_.close(ig2);
      }));

  asio::async_connect(
      socket_, endpoints,
      asio::bind_executor(
          strand_, [this, self, prom, timed_out](asio::error_code ec,
                                                 const asio::ip::tcp::endpoint& ep) {
            connect_timer_.cancel();
            if (!ec) {
              link_up_.store(true);
              open_.store(true);
              backoff_cur_ = backoff_base_;
              peer_id_ = ep.address().to_string() + ":" + std::to_string(ep.port());
              if (prom) prom->set_value(Status::Success(std::monostate{}));
              if (connect_cb_) connect_cb_();
              StartRead();
              return;
            }
            std::string reason = *timed_out ? "timeout: connect timed out"
                                            : ("conn: " + ec.message());
            if (prom) prom->set_value(Status::Fail(reason));
            if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
          }));
}

void TcpClientTransport::ScheduleReconnect() {
  if (closing_.load() || !config_.auto_reconnect) return;
  reconnect_timer_.expires_after(backoff_cur_);
  auto self = shared_from_this();
  reconnect_timer_.async_wait(asio::bind_executor(
      strand_, [this, self](asio::error_code ec) {
        if (ec || closing_.load()) return;
        backoff_cur_ = std::min(backoff_cur_ * 2, backoff_cap_);
        StartConnect(nullptr);
      }));
}

void TcpClientTransport::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            if (bytes_cb_) {
              std::vector<uint8_t> chunk(read_buf_.begin(), read_buf_.begin() + n);
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(chunk)), peer_id_);
            }
            StartRead();
          }));
}

void TcpClientTransport::HandleDisconnect(const std::string& reason) {
  if (!link_up_.exchange(false)) return;
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
  if (config_.auto_reconnect && !closing_.load()) ScheduleReconnect();
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: tcp not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status TcpClientTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void TcpClientTransport::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void TcpClientTransport::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

void TcpClientTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  link_up_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    connect_timer_.cancel();
    reconnect_timer_.cancel();
    socket_.close(ig);
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

把 `src/tcp/TcpClientTransport.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 6: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R TcpClientTransport 2>&1 | grep -iE "passed|failed"`
Expected: `TcpClientTransport.*` 全通过。

- [ ] **Step 7: 提交**

```bash
git add include/transport/tcp/TcpClientTransport.hpp src/tcp/TcpClientTransport.cpp include/transport/tcp/TcpClientConfig.hpp tests/transport/tcp_transport_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TcpClientTransport 纯字节管道(合并 TcpClientImpl+TcpConnectionImpl,去分帧/codec)"
```

---

## Task 10: `SerialTransport`(纯管道,改写自 SerialImpl)

**Files:**
- Create: `include/transport/serial/SerialTransport.hpp`、`src/serial/SerialTransport.cpp`
- Modify: `include/transport/serial/SerialConfig.hpp`(删 `framer`/`enable_topic_routing`)
- Test: `tests/transport/serial_transport_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 改 SerialConfig.hpp**

把 `include/transport/serial/SerialConfig.hpp` 整体替换为(删 framer/topic 及其 include):

```cpp
#pragma once

#include <cstdint>
#include <string>

namespace transport {

struct SerialConfig {
  std::string device;
  uint32_t    baud_rate = 115200;
  uint8_t     data_bits = 8;
  uint8_t     stop_bits = 1;       // 1 或 2
  char        parity    = 'N';     // 'N'/'E'/'O'
};

}  // namespace transport
```

- [ ] **Step 2: 写失败测试**

新建 `tests/transport/serial_transport_test.cpp`(用 `openpty` 造虚拟串口对):

```cpp
#include "transport/serial/SerialTransport.hpp"

#include <fcntl.h>
#include <pty.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gtest/gtest.h>

using transport::Result;
using transport::SerialConfig;
using transport::SerialTransport;

namespace {
struct Sink {
  std::mutex m; std::condition_variable cv; std::vector<uint8_t> acc;
  void Wire(std::shared_ptr<SerialTransport> t) {
    t->OnBytes([this](Result<std::vector<uint8_t>> r, const std::string&) {
      if (!r) return;
      std::lock_guard<std::mutex> lk(m);
      acc.insert(acc.end(), r.value.begin(), r.value.end());
      cv.notify_all();
    });
  }
  bool WaitBytes(std::size_t n, int ms) {
    std::unique_lock<std::mutex> lk(m);
    return cv.wait_for(lk, std::chrono::milliseconds(ms), [&] { return acc.size() >= n; });
  }
};
}  // namespace

TEST(SerialTransport, OpenSendReceiveOverPty) {
  int master = -1, slave = -1;
  ASSERT_EQ(openpty(&master, &slave, nullptr, nullptr, nullptr), 0);
  char slave_name[256];
  ASSERT_EQ(ttyname_r(slave, slave_name, sizeof(slave_name)), 0);

  SerialConfig cfg; cfg.device = slave_name; cfg.baud_rate = 115200;
  auto t = std::make_shared<SerialTransport>(cfg);
  Sink sink; sink.Wire(t);
  ASSERT_TRUE(static_cast<bool>(t->Open()));

  // 从 master 写入 → SerialTransport 应收到
  const uint8_t out[] = {0x41, 0x42, 0x43};
  ASSERT_EQ(write(master, out, 3), 3);
  ASSERT_TRUE(sink.WaitBytes(3, 1000));
  EXPECT_EQ(sink.acc, (std::vector<uint8_t>{0x41, 0x42, 0x43}));

  // SerialTransport 发送 → master 应读到
  ASSERT_TRUE(static_cast<bool>(t->Send({0x31, 0x32})));
  uint8_t in[2] = {0, 0};
  ASSERT_EQ(read(master, in, 2), 2);
  EXPECT_EQ(in[0], 0x31); EXPECT_EQ(in[1], 0x32);

  t->Close();
  close(master);
}
```

> 注:`openpty` 需链接 `util` 库。把 `transport_tests` 的链接加上 `util`(见下 CMake 步)。

把测试加入 `CMakeLists.txt` 的 `transport_tests`,并在测试目标链接 `util`:
```cmake
  target_link_libraries(transport_tests PRIVATE util)
```

- [ ] **Step 3: 运行,确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | head -20`
Expected: 找不到 `transport/serial/SerialTransport.hpp`。

- [ ] **Step 4: 写 SerialTransport.hpp**

新建 `include/transport/serial/SerialTransport.hpp`:

```cpp
#pragma once

// SerialTransport.hpp — 串口纯字节管道(改写自 SerialImpl,去 codec/分帧/topic)。
// 自有 io_context + 1 io 线程;读到的字节切片经 OnBytes 交付(from=设备路径)。

#include <array>
#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/serial/SerialConfig.hpp"

namespace transport {

class SerialTransport : public ITransport,
                        public std::enable_shared_from_this<SerialTransport> {
 public:
  explicit SerialTransport(SerialConfig config);
  ~SerialTransport() override;

  Status Open() override;
  void   Close() override;
  bool   IsOpen() const override { return open_.load(); }

  Status Send(const std::vector<uint8_t>& bytes) override;
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  void StartRead();
  void DoWrite();
  void EnqueueWrite(std::vector<uint8_t> bytes);
  void HandleDisconnect(const std::string& reason);

  SerialConfig config_;
  asio::io_context ctx_;
  asio::executor_work_guard<asio::io_context::executor_type> guard_;
  asio::strand<asio::io_context::executor_type> strand_;
  asio::serial_port port_;
  std::array<uint8_t, 4096> read_buf_;
  std::deque<std::vector<uint8_t>> write_queue_;
  bool writing_ = false;
  std::thread io_thread_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::atomic<bool> disconnected_{false};

  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

}  // namespace transport
```

- [ ] **Step 5: 写 SerialTransport.cpp**

新建 `src/serial/SerialTransport.cpp`(改写自旧 `SerialImpl.cpp`,去 assembler/codec/topic):

```cpp
#include "transport/serial/SerialTransport.hpp"

#include <utility>
#include <variant>

// SerialTransport.cpp — 见 .hpp。读到的字节切片经 OnBytes 直接交付(无分帧)。

namespace transport {

SerialTransport::SerialTransport(SerialConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      port_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

SerialTransport::~SerialTransport() { Close(); }

Status SerialTransport::Open() {
  asio::error_code ec;
  port_.open(config_.device, ec);
  if (ec) return Status::Fail("config: open " + config_.device + ": " + ec.message());

  auto fail = [&](const std::string& msg) {
    asio::error_code ig; port_.close(ig);
    return Status::Fail(msg);
  };
  using sb = asio::serial_port_base;
  port_.set_option(sb::baud_rate(config_.baud_rate), ec);
  if (ec) return fail("config: baud_rate: " + ec.message());
  port_.set_option(sb::character_size(config_.data_bits), ec);
  if (ec) return fail("config: data_bits: " + ec.message());

  sb::stop_bits::type sbits;
  if (config_.stop_bits == 1) sbits = sb::stop_bits::one;
  else if (config_.stop_bits == 2) sbits = sb::stop_bits::two;
  else return fail("config: stop_bits must be 1 or 2");
  port_.set_option(sb::stop_bits(sbits), ec);
  if (ec) return fail("config: stop_bits: " + ec.message());

  sb::parity::type par;
  switch (config_.parity) {
    case 'N': par = sb::parity::none; break;
    case 'E': par = sb::parity::even; break;
    case 'O': par = sb::parity::odd; break;
    default: return fail("config: parity must be N/E/O");
  }
  port_.set_option(sb::parity(par), ec);
  if (ec) return fail("config: parity: " + ec.message());

  asio::error_code fc_ec;
  port_.set_option(sb::flow_control(sb::flow_control::none), fc_ec);  // best-effort

  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartRead();
  });
  return Status::Success(std::monostate{});
}

void SerialTransport::StartRead() {
  auto self = shared_from_this();
  port_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            if (bytes_cb_) {
              std::vector<uint8_t> chunk(read_buf_.begin(), read_buf_.begin() + n);
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(chunk)),
                        config_.device);
            }
            StartRead();
          }));
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: serial not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void SerialTransport::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void SerialTransport::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      port_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

void SerialTransport::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;
  open_.store(false);
  asio::error_code ig; port_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
}

void SerialTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig; port_.close(ig);
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
```

把 `src/serial/SerialTransport.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`。

- [ ] **Step 6: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R SerialTransport 2>&1 | grep -iE "passed|failed"`
Expected: `SerialTransport.*` 全通过。

- [ ] **Step 7: 提交**

```bash
git add include/transport/serial/SerialTransport.hpp src/serial/SerialTransport.cpp include/transport/serial/SerialConfig.hpp tests/transport/serial_transport_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: SerialTransport 纯字节管道(改写自 SerialImpl,去分帧/codec)"
```

---

## Task 11: 两层组合冒烟测试

**Files:**
- Test: `tests/transport/combination_smoke_test.cpp`
- Modify: `CMakeLists.txt`

验证 spec §5 契约:把某 transport 收到的字节喂给 codec,还原出的 `Message` == 发送端 `Encode` 输入。用 UDP 回环 + `SystemCodec`(报文式,一次 Encode → 一个 datagram → 一次 Decode 出一条)。

- [ ] **Step 1: 写测试**

新建 `tests/transport/combination_smoke_test.cpp`:

```cpp
#include "transport/codec/SystemCodec.hpp"
#include "transport/udp/UdpTransport.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Message;
using transport::MessageKind;
using transport::Result;
using transport::SystemCodec;
using transport::UdpConfig;
using transport::UdpTransport;

TEST(CombinationSmoke, UdpBytesThroughSystemCodecRoundtrip) {
  // rx:收字节 → 喂 codec → 攒出的 Message
  auto rx_codec = std::make_shared<SystemCodec>();
  std::mutex m; std::condition_variable cv;
  std::vector<Message> got;

  UdpConfig rxc; rxc.local_addr = "127.0.0.1"; rxc.local_port = 0;
  auto rx = std::make_shared<UdpTransport>(rxc);
  rx->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&) {
    if (!r) return;
    auto msgs = rx_codec->Decode(r.value.data(), r.value.size());
    if (!msgs) return;
    std::lock_guard<std::mutex> lk(m);
    for (auto& mm : msgs.value) got.push_back(std::move(mm));
    cv.notify_all();
  });
  ASSERT_TRUE(static_cast<bool>(rx->Open()));
  const uint16_t rx_port = rx->LocalPort();

  // tx:Message → codec.Encode → transport.Send
  SystemCodec tx_codec;
  Message out; out.kind = MessageKind::kRequest; out.correlation_id = "x-1";
  out.topic = "calc"; out.payload = {4, 5, 6};
  auto bytes = tx_codec.Encode(out);
  ASSERT_TRUE(static_cast<bool>(bytes));

  UdpConfig txc; txc.local_addr = "127.0.0.1"; txc.local_port = 0;
  auto tx = std::make_shared<UdpTransport>(txc);
  ASSERT_TRUE(static_cast<bool>(tx->Open()));
  ASSERT_TRUE(static_cast<bool>(
      tx->Send(bytes.value, Endpoint::Net("127.0.0.1", rx_port))));

  {
    std::unique_lock<std::mutex> lk(m);
    ASSERT_TRUE(cv.wait_for(lk, std::chrono::milliseconds(1000),
                            [&] { return !got.empty(); }));
  }
  ASSERT_EQ(got.size(), 1u);
  EXPECT_EQ(got[0].kind, MessageKind::kRequest);
  EXPECT_EQ(got[0].correlation_id, "x-1");
  EXPECT_EQ(got[0].topic, "calc");
  EXPECT_EQ(got[0].payload, (std::vector<uint8_t>{4, 5, 6}));

  tx->Close(); rx->Close();
}
```

把测试加入 `CMakeLists.txt` 的 `transport_tests`。

- [ ] **Step 2: 运行,确认通过**

Run: `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R CombinationSmoke 2>&1 | grep -iE "passed|failed"`
Expected: `CombinationSmoke.*` 通过。

- [ ] **Step 3: 全量测试 + 干净构建确认零告警**

Run:
```bash
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---"
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 无 warning/error;`100% tests passed`。

- [ ] **Step 4: 提交**

```bash
git add tests/transport/combination_smoke_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "test: 两层组合冒烟(UDP 字节 → SystemCodec → Message 往返)"
```

---

## 完成标准

- 旧富 `ITransport`/`TransportCore`/`IFramer`/topic 路由/DDS/服务端全部移除;构建中无残留引用。
- `Message` 带 `kind`/`correlation_id`;`ICodec` 为 `Encode(Message)`/`Decode→0..N`;`ITransport` 为纯字节管道。
- 三个 codec(`SystemCodec`/`LengthFieldCodec`/`DatagramCodec`)与三个 transport(`Udp`/`TcpClient`/`Serial`)各自独立可测、全绿。
- 组合冒烟证明两层可拼。
- 干净构建零告警、全量测试 100% 通过。
- `Transport` 层测试不引入 `ICodec`/`Message`(组合冒烟除外);`ICodec` 层测试不链接任何 transport —— 解耦达成。
