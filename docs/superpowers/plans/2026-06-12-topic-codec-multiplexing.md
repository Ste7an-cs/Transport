# Topic 路由编解码多路复用 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 单条连接/传输上按 topic 多路复用多种帧格式,框架按 topic 自动选 codec 编解码,并提供 `Send(const Message&)` 让发送侧与接收侧的 `Message` 对称。

**Architecture:** `TransportCore` 的单 codec 升级为 `topic→codec` 注册表 + 默认 codec;新增纯函数 wire 封装 `TopicEnvelope`(UDP 报文体 `[topic_len][topic][body]`,TCP/串口流帧再外包 `[frame_len:4]` 框架自有分帧);`ITransport` 加 `Send(Message)`(基类默认)与 `SetCodec(topic,codec)`(基类 no-op);四种传输各自覆写。opt-in:DDS 始终启用(原生 topic 零 wire 改动),TCP/UDP/串口由 config `enable_topic_routing`(默认 false)控制 in-band envelope。

**Tech Stack:** C++17,无新第三方依赖;GoogleTest 1.14(vendored)。spec 见 `docs/superpowers/specs/2026-06-12-topic-codec-multiplexing-design.md`。

---

## 关键不变式(所有任务共享)

- `Message.payload` 收发两侧恒为「应用原始字节」;codec 永远由框架在中间层施加。
- **路由开启时,codec 只编解码 body**(不再自带长度);框架的 `frame_len`(TCP/串口)或报文边界(UDP)/原生 topic(DDS)负责分帧。
- 路由关闭时,行为与今天逐字节一致(回归必须通过)。
- C++ 名字隐藏:每个覆写了 `Send`/`SetCodec` 重载的 `*Impl` 必须有 `using ITransport::Send;` 和 `using ITransport::SetCodec;`。

## 文件结构

| 文件 | 责任 | 任务 |
|---|---|---|
| `include/transport/core/TopicEnvelope.hpp`(新,header-only) | wire 封装纯函数 `PackTopic`/`UnpackTopic`/`FrameStream` + 流式 `TopicFrameAssembler` | T1 |
| `include/transport/core/TransportCore.hpp`(改) | 单 codec → `topic→codec` 注册表;`EncodeForSend(data,topic)`;`DeliverFrame` 按 topic 选 codec | T2 |
| `include/transport/ITransport.hpp`(改) | `Send(Message,Endpoint=Default())` 基类默认;`SetCodec(topic,codec)` 基类 no-op | T3 |
| `include/transport/{tcp/TcpClientConfig,tcp/TcpServerConfig,udp/UdpConfig,serial/SerialConfig}.hpp`(改) | `bool enable_topic_routing = false;` | T4/T5/T6 |
| `include/transport/tcp/TcpConnectionImpl.{hpp}` + `src/tcp/TcpConnectionImpl.cpp`(改) | routing 标志、流帧收发、`Send(Message)`、`SetCodec(topic,codec)` | T4 |
| `src/tcp/TcpClientImpl.cpp` + `src/tcp/TcpServerImpl.cpp`(改) | 把 `enable_topic_routing` 透传进 `TcpConnectionImpl` 构造 | T4 |
| `include/transport/udp/UdpImpl.hpp` + `src/udp/UdpImpl.cpp`(改) | UDP 报文 envelope 收发、`Send(Message)`、`SetCodec(topic,codec)` | T5 |
| `include/transport/serial/SerialImpl.hpp` + `src/serial/SerialImpl.cpp`(改) | 串口流帧收发(同 TCP) | T6 |
| `include/transport/dds/DdsImpl.hpp` + `src/dds/DdsImpl.cpp`(改) | `Send(Message)`、`SendToTopic` 按 topic 编码、`SetCodec(topic,codec)` | T7 |
| `tests/core/topic_envelope_test.cpp`(新) | T1 单元测试 | T1 |
| `tests/core/transport_core_codec_test.cpp`(新) | T2 单元测试 | T2 |
| `tests/topic_routing_test.cpp`(新) | T3 基类默认 + UDP 端到端;后续任务追加 TCP/串口/DDS 段 | T3/T5/T6/T7 |
| `CMakeLists.txt`(改) | 注册新增测试源 | T1/T2/T3 |
| `README.md` + 主 spec + as-built spec(改) | 文档同步 | T8 |

---

## Task 1: TopicEnvelope wire 封装(纯函数 + 流式装配器)

**Files:**
- Create: `include/transport/core/TopicEnvelope.hpp`
- Create: `tests/core/topic_envelope_test.cpp`
- Modify: `CMakeLists.txt`(在 `add_executable(transport_tests ...)` 源列表加一行)

- [ ] **Step 1: 写失败测试** — `tests/core/topic_envelope_test.cpp`

```cpp
#include "transport/core/TopicEnvelope.hpp"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameStream;
using transport::PackTopic;
using transport::TopicFrame;
using transport::TopicFrameAssembler;
using transport::UnpackTopic;
using Bytes = std::vector<uint8_t>;

TEST(TopicEnvelope, PackUnpackRoundTrip) {
  Bytes body{1, 2, 3, 4};
  auto packed = PackTopic("cmd", body);
  // [0,3]['c','m','d'][1,2,3,4]
  ASSERT_EQ(packed.size(), 2u + 3u + 4u);
  EXPECT_EQ(packed[0], 0u);
  EXPECT_EQ(packed[1], 3u);
  auto r = UnpackTopic(packed.data(), packed.size());
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "cmd");
  EXPECT_EQ(r.value.body, body);
}

TEST(TopicEnvelope, EmptyTopicAllowed) {
  Bytes body{9, 9};
  auto packed = PackTopic("", body);
  EXPECT_EQ(packed[0], 0u);
  EXPECT_EQ(packed[1], 0u);
  auto r = UnpackTopic(packed.data(), packed.size());
  ASSERT_TRUE(r.ok);
  EXPECT_TRUE(r.value.topic.empty());
  EXPECT_EQ(r.value.body, body);
}

TEST(TopicEnvelope, UnpackTooShortFails) {
  Bytes one{0};
  auto r = UnpackTopic(one.data(), one.size());
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}

TEST(TopicEnvelope, UnpackTopicLenExceedsFails) {
  Bytes bad{0, 5, 'a', 'b'};  // 声称 topic 5 字节,实际只有 2
  auto r = UnpackTopic(bad.data(), bad.size());
  EXPECT_FALSE(r.ok);
}

TEST(TopicEnvelope, StreamAssemblerSplitAcrossFeeds) {
  auto f1 = FrameStream("a", Bytes{1, 1});
  auto f2 = FrameStream("bb", Bytes{2, 2, 2});
  Bytes wire;
  wire.insert(wire.end(), f1.begin(), f1.end());
  wire.insert(wire.end(), f2.begin(), f2.end());

  TopicFrameAssembler asm_;
  // 拆成两段喂入,跨边界
  size_t mid = f1.size() + 1;
  auto r1 = asm_.Feed(wire.data(), mid);
  ASSERT_TRUE(r1.ok);
  ASSERT_EQ(r1.value.size(), 1u);  // 第一帧完整
  EXPECT_EQ(r1.value[0].topic, "a");
  EXPECT_EQ(r1.value[0].body, (Bytes{1, 1}));

  auto r2 = asm_.Feed(wire.data() + mid, wire.size() - mid);
  ASSERT_TRUE(r2.ok);
  ASSERT_EQ(r2.value.size(), 1u);
  EXPECT_EQ(r2.value[0].topic, "bb");
  EXPECT_EQ(r2.value[0].body, (Bytes{2, 2, 2}));
}

TEST(TopicEnvelope, StreamAssemblerMultipleInOneFeed) {
  auto f1 = FrameStream("x", Bytes{7});
  auto f2 = FrameStream("y", Bytes{8});
  Bytes wire(f1);
  wire.insert(wire.end(), f2.begin(), f2.end());
  TopicFrameAssembler asm_;
  auto r = asm_.Feed(wire.data(), wire.size());
  ASSERT_TRUE(r.ok);
  ASSERT_EQ(r.value.size(), 2u);
  EXPECT_EQ(r.value[0].topic, "x");
  EXPECT_EQ(r.value[1].topic, "y");
}

TEST(TopicEnvelope, StreamAssemblerOverflowFails) {
  // 构造一个 frame_len 超过上限的伪帧头
  Bytes bad{0xFF, 0xFF, 0xFF, 0xFF};
  TopicFrameAssembler asm_;
  auto r = asm_.Feed(bad.data(), bad.size());
  EXPECT_FALSE(r.ok);
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: 编译失败,`fatal error: transport/core/TopicEnvelope.hpp: No such file or directory`(头文件未创建)。

- [ ] **Step 3: 实现** — 新建 `include/transport/core/TopicEnvelope.hpp`

```cpp
#pragma once

// -----------------------------------------------------------------------------
// TopicEnvelope.hpp — topic 路由的 wire 封装(纯函数 + 流式装配器,header-only)
// UDP 报文体 / 流帧内层:[topic_len:2 BE][topic][body]
// TCP/串口流帧:[frame_len:4 BE][topic envelope] —— 路由模式框架自有分帧。
// codec 只编解码 body;本层只负责 topic 与分帧。详见 spec §6。
// -----------------------------------------------------------------------------

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "transport/Result.hpp"

namespace transport {

struct TopicFrame {
  std::string topic;
  std::vector<uint8_t> body;
};

// [topic_len:2 BE][topic][body]
inline std::vector<uint8_t> PackTopic(const std::string& topic,
                                      const std::vector<uint8_t>& body) {
  std::vector<uint8_t> out;
  out.reserve(2 + topic.size() + body.size());
  const uint16_t n = static_cast<uint16_t>(topic.size());
  out.push_back(static_cast<uint8_t>((n >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(n & 0xFF));
  out.insert(out.end(), topic.begin(), topic.end());
  out.insert(out.end(), body.begin(), body.end());
  return out;
}

// 解析 [topic_len:2 BE][topic][body];越界返回 Fail("frame: ...")
inline Result<TopicFrame> UnpackTopic(const uint8_t* data, size_t len) {
  if (len < 2)
    return Result<TopicFrame>::Fail("frame: topic envelope too short");
  const size_t n = (static_cast<size_t>(data[0]) << 8) | data[1];
  if (2 + n > len)
    return Result<TopicFrame>::Fail("frame: topic length exceeds envelope");
  TopicFrame f;
  f.topic.assign(reinterpret_cast<const char*>(data + 2), n);
  f.body.assign(data + 2 + n, data + len);
  return Result<TopicFrame>::Success(std::move(f));
}

// topic 是否能放进 2 字节长度字段(上限 65535)
inline bool TopicFitsEnvelope(const std::string& topic) {
  return topic.size() <= 0xFFFF;
}

// [frame_len:4 BE][PackTopic 输出]  —— frame_len 不含自身 4 字节
inline std::vector<uint8_t> FrameStream(const std::string& topic,
                                        const std::vector<uint8_t>& body) {
  auto env = PackTopic(topic, body);
  std::vector<uint8_t> out;
  out.reserve(4 + env.size());
  const uint32_t n = static_cast<uint32_t>(env.size());
  out.push_back(static_cast<uint8_t>((n >> 24) & 0xFF));
  out.push_back(static_cast<uint8_t>((n >> 16) & 0xFF));
  out.push_back(static_cast<uint8_t>((n >> 8) & 0xFF));
  out.push_back(static_cast<uint8_t>(n & 0xFF));
  out.insert(out.end(), env.begin(), env.end());
  return out;
}

// 流式装配器:累积字节,按 frame_len 切出 (topic, body)。镜像 FrameAssembler。
class TopicFrameAssembler {
 public:
  static constexpr size_t kMaxFrame = 16 * 1024 * 1024;

  Result<std::vector<TopicFrame>> Feed(const uint8_t* data, size_t len) {
    std::vector<TopicFrame> out;
    buffer_.insert(buffer_.end(), data, data + len);
    size_t offset = 0;
    while (buffer_.size() - offset >= 4) {
      const uint8_t* p = buffer_.data() + offset;
      const size_t flen = (static_cast<size_t>(p[0]) << 24) |
                          (static_cast<size_t>(p[1]) << 16) |
                          (static_cast<size_t>(p[2]) << 8) |
                          static_cast<size_t>(p[3]);
      if (flen > kMaxFrame)
        return Result<std::vector<TopicFrame>>::Fail(
            "frame: frame length exceeds max");
      if (buffer_.size() - offset - 4 < flen) break;  // 不足一帧,等更多
      auto tf = UnpackTopic(p + 4, flen);
      if (!tf) return Result<std::vector<TopicFrame>>::Fail(tf.error);
      out.push_back(std::move(tf.value));
      offset += 4 + flen;
    }
    if (offset > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + offset);
    return Result<std::vector<TopicFrame>>::Success(std::move(out));
  }

 private:
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
```

- [ ] **Step 4: 注册测试源** — `CMakeLists.txt`,在 `add_executable(transport_tests` 的源列表里(`tests/core/transport_core_test.cpp` 之后)加一行:

```cmake
    tests/core/topic_envelope_test.cpp
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R TopicEnvelope --output-on-failure 2>&1 | tail -15`
Expected: 7 个 TopicEnvelope.* 全部 PASS。

- [ ] **Step 6: 提交**

```bash
git add include/transport/core/TopicEnvelope.hpp tests/core/topic_envelope_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TopicEnvelope wire 封装(PackTopic/UnpackTopic/FrameStream + 流式装配器)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 2: TransportCore topic→codec 注册表

**Files:**
- Modify: `include/transport/core/TransportCore.hpp`
- Create: `tests/core/transport_core_codec_test.cpp`
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试** — `tests/core/transport_core_codec_test.cpp`

```cpp
#include "transport/core/TransportCore.hpp"

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"

using transport::ICodec;
using transport::Message;
using transport::Result;
using transport::TransportCore;
using Bytes = std::vector<uint8_t>;

namespace {
// 在 body 前加一个 tag 字节;Decode 校验并剥除。用于证明「按 topic 选对了 codec」。
class TagCodec : public ICodec {
 public:
  explicit TagCodec(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o;
    o.reserve(d.size() + 1);
    o.push_back(tag_);
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};
}  // namespace

TEST(TransportCoreCodec, EncodeForSendSelectsByTopic) {
  TransportCore core;
  core.SetCodec("a", std::make_shared<TagCodec>(0xAA));
  core.SetCodec("b", std::make_shared<TagCodec>(0xBB));
  auto ea = core.EncodeForSend(Bytes{1}, "a");
  ASSERT_TRUE(ea.ok);
  EXPECT_EQ(ea.value, (Bytes{0xAA, 1}));
  auto eb = core.EncodeForSend(Bytes{1}, "b");
  ASSERT_TRUE(eb.ok);
  EXPECT_EQ(eb.value, (Bytes{0xBB, 1}));
}

TEST(TransportCoreCodec, UnregisteredTopicFallsBackToDefault) {
  TransportCore core;
  core.SetCodec(std::make_shared<TagCodec>(0xDD));  // 默认
  auto e = core.EncodeForSend(Bytes{5}, "unknown");
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value, (Bytes{0xDD, 5}));
}

TEST(TransportCoreCodec, NoCodecPassthrough) {
  TransportCore core;
  auto e = core.EncodeForSend(Bytes{5, 6}, "x");
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value, (Bytes{5, 6}));  // 无 codec → 透传
}

TEST(TransportCoreCodec, DeliverFrameDecodesByTopic) {
  TransportCore core;
  core.SetCodec("a", std::make_shared<TagCodec>(0xAA));
  core.SetCodec("b", std::make_shared<TagCodec>(0xBB));
  core.DeliverFrame(Bytes{0xBB, 7, 8}, "src", "b");
  auto m = core.Receive(100);
  ASSERT_TRUE(m.ok);
  EXPECT_EQ(m.value.topic, "b");
  EXPECT_EQ(m.value.payload, (Bytes{7, 8}));  // 用 CodecB 解码剥除 tag
}

TEST(TransportCoreCodec, LegacySingleCodecUnchanged) {
  TransportCore core;
  core.SetCodec(std::make_shared<TagCodec>(0xCC));
  auto e = core.EncodeForSend(Bytes{1, 2});  // 旧无 topic 重载
  ASSERT_TRUE(e.ok);
  EXPECT_EQ(e.value, (Bytes{0xCC, 1, 2}));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake -S . -B build > /dev/null 2>&1; (cd build && cmake --build . --target transport_tests -j$(nproc)) 2>&1 | tail -15`
Expected: 编译失败 —— `EncodeForSend` 无 `(data, topic)` 重载、`SetCodec(topic, codec)` 不存在。

- [ ] **Step 3: 实现** — 修改 `include/transport/core/TransportCore.hpp`

加头文件 `#include <map>`(在 `#include <future>` 后)。

将私有成员 `std::shared_ptr<ICodec> codec_;` 替换为:

```cpp
  std::shared_ptr<ICodec> default_codec_;
  std::map<std::string, std::shared_ptr<ICodec>> codecs_;
```

将 `void SetCodec(std::shared_ptr<ICodec> codec) { codec_ = std::move(codec); }` 替换为:

```cpp
  // 默认 codec：topic 未注册时的兜底；不设则透传。
  void SetCodec(std::shared_ptr<ICodec> codec) { default_codec_ = std::move(codec); }
  // 为某 topic 注册专用 codec。
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) {
    codecs_[topic] = std::move(codec);
  }
  // 选 codec：优先 topic 专用,回退默认(可能为 nullptr → 透传)。
  std::shared_ptr<ICodec> CodecFor(const std::string& topic) const {
    auto it = codecs_.find(topic);
    if (it != codecs_.end()) return it->second;
    return default_codec_;
  }
```

将 `EncodeForSend` 替换为(保留旧无 topic 重载转调新重载):

```cpp
  // 发送前编码;无 topic 走默认 codec。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data) {
    return EncodeForSend(data, "");
  }
  // 按 topic 选 codec 编码;无 codec 时透传。
  Result<std::vector<uint8_t>> EncodeForSend(const std::vector<uint8_t>& data,
                                             const std::string& topic) {
    auto codec = CodecFor(topic);
    if (!codec) return Result<std::vector<uint8_t>>::Success(data);
    return codec->Encode(data);
  }
```

将 `DecodeForReceive` 体内 `if (!codec_)` / `codec_->Decode` 改为按默认 codec:

```cpp
  Result<std::vector<uint8_t>> DecodeForReceive(
      const std::vector<uint8_t>& frame) {
    auto codec = CodecFor("");
    if (!codec) return Result<std::vector<uint8_t>>::Success(frame);
    return codec->Decode(frame);
  }
```

将 `DeliverFrame` 体内 `if (codec_) { auto decoded = codec_->Decode(frame); ... }` 的 `codec_` 改为按传入 topic 选:

```cpp
  void DeliverFrame(std::vector<uint8_t> frame, const std::string& source,
                    const std::string& topic) {
    std::vector<uint8_t> payload;
    auto codec = CodecFor(topic);
    if (codec) {
      auto decoded = codec->Decode(frame);
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
```

- [ ] **Step 4: 注册测试源** — `CMakeLists.txt`,在 `tests/core/topic_envelope_test.cpp` 后加:

```cmake
    tests/core/transport_core_codec_test.cpp
```

- [ ] **Step 5: 跑测试确认通过(含全量回归)**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build 2>&1 | tail -4`
Expected: `TransportCoreCodec.*` 5 个全 PASS;**且既有所有测试不回归**(单 codec 行为不变)。

- [ ] **Step 6: 提交**

```bash
git add include/transport/core/TransportCore.hpp tests/core/transport_core_codec_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TransportCore 单 codec → topic→codec 注册表(EncodeForSend(data,topic)/DeliverFrame 按 topic 选 codec)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 3: ITransport 加 Send(Message) 基类默认 + SetCodec(topic,codec) 基类 no-op

**Files:**
- Modify: `include/transport/ITransport.hpp`
- Create: `tests/topic_routing_test.cpp`(本任务建文件,后续任务追加段)
- Modify: `CMakeLists.txt`

- [ ] **Step 1: 写失败测试** — `tests/topic_routing_test.cpp`

```cpp
#include "transport/ITransport.hpp"

#include <cstdint>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using transport::Endpoint;
using transport::ICodec;
using transport::ITransport;
using transport::Message;
using transport::Result;
using transport::Status;
using Bytes = std::vector<uint8_t>;

namespace {
// 最小 ITransport:只记录最后一次 Send(payload) 的入参,其余接收侧空实现。
// 不覆写 Send(Message)/SetCodec(topic,codec) → 走 ITransport 基类默认。
class RecordingTransport : public ITransport {
 public:
  Status Open() override { return Status::Success({}); }
  void Close() override {}
  bool IsOpen() const override { return true; }
  Status Send(const std::vector<uint8_t>& data) override {
    last_payload = data;
    ++send_calls;
    return Status::Success({});
  }
  Result<Message> Receive(uint32_t) override {
    return Result<Message>::Fail("io: not supported");
  }
  void OnReceive(ReceiveCallback) override {}
  std::future<Result<Message>> AsyncReceive() override {
    std::promise<Result<Message>> p;
    p.set_value(Result<Message>::Fail("io: not supported"));
    return p.get_future();
  }
  void OnDisconnect(DisconnectCallback) override {}
  void SetCodec(std::shared_ptr<ICodec>) override {}

  Bytes last_payload;
  int send_calls = 0;
};
}  // namespace

TEST(SendMessageBaseDefault, EmptyTopicDegradesToSendPayload) {
  RecordingTransport t;
  Message m;
  m.payload = Bytes{1, 2, 3};  // topic 空
  auto st = t.Send(m);
  EXPECT_TRUE(st.ok);
  EXPECT_EQ(t.send_calls, 1);
  EXPECT_EQ(t.last_payload, (Bytes{1, 2, 3}));
}

TEST(SendMessageBaseDefault, NonEmptyTopicNotSupported) {
  RecordingTransport t;
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = t.Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "io: topic routing not supported");
  EXPECT_EQ(t.send_calls, 0);
}

TEST(SendMessageBaseDefault, SetCodecTopicIsNoopOnBase) {
  RecordingTransport t;
  // 基类 no-op:不抛、不崩,纯粹忽略。
  t.SetCodec("x", nullptr);
  SUCCEED();
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake -S . -B build > /dev/null 2>&1; (cd build && cmake --build . --target transport_tests -j$(nproc)) 2>&1 | tail -15`
Expected: 编译失败 —— `t.Send(m)`(Message 重载不存在)与 `t.SetCodec("x", nullptr)`(topic 重载不存在)。

- [ ] **Step 3: 实现** — 修改 `include/transport/ITransport.hpp`

在现有 `virtual Status Send(const std::vector<uint8_t>& data, const Endpoint& to) { ... }` 之后,加 `Send(Message)` 重载:

```cpp
  // topic 路由发送(非纯虚,基类默认):
  //   topic 为空 → 退化 Send(payload, to);否则 → Fail(该实现不支持路由)。
  // TCP/UDP/串口/DDS 覆写以支持 topic→codec 路由。
  virtual Status Send(const Message& msg,
                      const Endpoint& to = Endpoint::Default()) {
    if (msg.topic.empty()) return Send(msg.payload, to);
    return Status::Fail("io: topic routing not supported");
  }
```

在现有纯虚 `virtual void SetCodec(std::shared_ptr<ICodec> codec) = 0;` 之后,加带 topic 的重载(基类 no-op):

```cpp
  // 为某 topic 注册 codec(topic 路由);基类 no-op,路由能力的实现覆写转发给
  // 自己的 TransportCore。
  virtual void SetCodec(const std::string& topic,
                        std::shared_ptr<ICodec> codec) {
    (void)topic;
    (void)codec;
  }
```

(`Message`、`Endpoint`、`<string>`、`<memory>` 均已被 ITransport.hpp 包含,无需新增 include。)

- [ ] **Step 4: 注册测试源** — `CMakeLists.txt`,在 `add_executable(transport_tests` 源列表里(`tests/result_test.cpp` 附近)加:

```cmake
    tests/topic_routing_test.cpp
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R SendMessageBaseDefault --output-on-failure 2>&1 | tail -10`
Expected: `SendMessageBaseDefault.*` 3 个 PASS。

- [ ] **Step 6: 提交**

```bash
git add include/transport/ITransport.hpp tests/topic_routing_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ITransport::Send(Message) 基类默认 + SetCodec(topic,codec) 基类 no-op

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 4: TCP topic 路由

**Files:**
- Modify: `include/transport/tcp/TcpClientConfig.hpp`,`include/transport/tcp/TcpServerConfig.hpp`
- Modify: `include/transport/tcp/TcpConnectionImpl.hpp`,`src/tcp/TcpConnectionImpl.cpp`
- Modify: `src/tcp/TcpClientImpl.cpp`,`src/tcp/TcpServerImpl.cpp`
- Modify: `tests/topic_routing_test.cpp`(追加 TCP 段)

- [ ] **Step 1: 写失败测试** — 在 `tests/topic_routing_test.cpp` 末尾追加

本段 API 已对照 `tests/tcp/tcp_server_test.cpp` 核实:`TcpServerImpl`/`TcpClientImpl` 直接构造;`TcpServerConfig` 字段为 `bind_addr`/`port`;OS 端口由 `server->LocalPort()` 取;accepted 连接经 `OnNewConnection` 异步交付,用轮询 `WaitFor` 等待。

```cpp
// ---- TCP topic 路由(回环 client+server) ----
#include <atomic>
#include <chrono>
#include <functional>
#include <thread>

#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerConfig.hpp"
#include "transport/tcp/TcpServerImpl.hpp"

namespace {
// 在 body 前加 tag 字节;Decode 校验剥除。证明「按 topic 选对了 codec」。
// 此 helper 同时供下方 UDP / 串口段复用(Task 5/6),勿重复定义。
class TagCodec2 : public ICodec {
 public:
  explicit TagCodec2(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o{tag_};
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};

void WaitFor(std::function<bool()> pred, int ms = 1000) {
  for (int i = 0; i < ms / 5 && !pred(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}
}  // namespace

TEST(TcpTopicRouting, TwoTopicsOverOneConnection) {
  using namespace transport;
  TcpServerConfig scfg;
  scfg.bind_addr = "127.0.0.1";
  scfg.port = 0;  // OS 分配
  scfg.enable_topic_routing = true;
  auto server = std::make_shared<TcpServerImpl>(scfg);

  std::shared_ptr<ITransport> accepted;
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) {
    accepted = c;
    ++conns;
  });
  ASSERT_TRUE(server->Open().ok);

  TcpClientConfig ccfg;
  ccfg.host = "127.0.0.1";
  ccfg.port = server->LocalPort();
  ccfg.connect_timeout_ms = 1000;
  ccfg.auto_reconnect = false;
  ccfg.enable_topic_routing = true;
  auto client = std::make_shared<TcpClientImpl>(ccfg);
  ASSERT_TRUE(client->Open().ok);
  WaitFor([&] { return conns.load() >= 1 && accepted != nullptr; });
  ASSERT_NE(accepted, nullptr);

  // 发送端(client)编码、接收端(accepted)解码,两端都要注册同名 codec。
  for (ITransport* t : {static_cast<ITransport*>(client.get()), accepted.get()}) {
    t->SetCodec("a", std::make_shared<TagCodec2>(0xAA));
    t->SetCodec("b", std::make_shared<TagCodec2>(0xBB));
  }

  Message ma;
  ma.payload = Bytes{1, 2};
  ma.topic = "a";
  Message mb;
  mb.payload = Bytes{3, 4, 5};
  mb.topic = "b";
  ASSERT_TRUE(client->Send(ma).ok);
  ASSERT_TRUE(client->Send(mb).ok);

  auto r1 = accepted->Receive(2000);
  ASSERT_TRUE(r1.ok);
  EXPECT_EQ(r1.value.topic, "a");
  EXPECT_EQ(r1.value.payload, (Bytes{1, 2}));
  auto r2 = accepted->Receive(2000);
  ASSERT_TRUE(r2.ok);
  EXPECT_EQ(r2.value.topic, "b");
  EXPECT_EQ(r2.value.payload, (Bytes{3, 4, 5}));

  client->Close();
  server->Close();
}

TEST(TcpTopicRouting, RoutingOffRejectsTopicSend) {
  using namespace transport;
  TcpClientConfig ccfg;
  ccfg.host = "127.0.0.1";
  ccfg.port = 1;  // 不连接;routing-off 分支在 open 检查前先因 topic 非空返回
  ccfg.auto_reconnect = false;
  ccfg.enable_topic_routing = false;
  auto client = std::make_shared<TcpClientImpl>(ccfg);
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = client->Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "config: topic routing not enabled");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake -S . -B build > /dev/null 2>&1; (cd build && cmake --build . --target transport_tests -j$(nproc)) 2>&1 | tail -15`
Expected: 编译/链接失败或运行失败 —— `TcpClientConfig`/`TcpServerConfig` 无 `enable_topic_routing` 成员;`Send(Message)` 未在 TCP 覆写(走基类默认 → 收不到正确帧)。

- [ ] **Step 3a: 实现 config 开关** — `include/transport/tcp/TcpClientConfig.hpp` 与 `TcpServerConfig.hpp` 各在 struct 末尾加:

```cpp
  bool enable_topic_routing = false;  // 开启 topic→codec 多路复用(in-band envelope)
```

- [ ] **Step 3b: 实现 TcpConnectionImpl** — `include/transport/tcp/TcpConnectionImpl.hpp`

加 include(文件头部):`#include "transport/Message.hpp"`、`#include "transport/core/TopicEnvelope.hpp"`。

构造函数声明加第三参(带默认值,保持既有调用点可选):

```cpp
  TcpConnectionImpl(asio::ip::tcp::socket socket, std::shared_ptr<IFramer> framer,
                    bool enable_topic_routing = false);
```

public 区,`Status Send(const std::vector<uint8_t>& data) override;` 后加:

```cpp
  Status Send(const Message& msg,
              const Endpoint& to = Endpoint::Default()) override;
```

`using ITransport::Send;` 已存在;在其旁补 `using ITransport::SetCodec;`。`SetCodec` 单参覆写之后加 topic 重载:

```cpp
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) override {
    core_.SetCodec(topic, std::move(codec));
  }
```

private 区,声明写入助手与流式装配器、routing 标志:

```cpp
  void EnqueueWrite(std::vector<uint8_t> bytes);  // 入队 + 触发 DoWrite

  bool enable_topic_routing_;
  TopicFrameAssembler topic_assembler_;
```

- [ ] **Step 3c: 实现 TcpConnectionImpl.cpp** — `src/tcp/TcpConnectionImpl.cpp`

加 include:`#include "transport/core/TopicEnvelope.hpp"`。

构造函数加初始化(在初始化列表末尾追加 `enable_topic_routing_`):

```cpp
TcpConnectionImpl::TcpConnectionImpl(asio::ip::tcp::socket socket,
                             std::shared_ptr<IFramer> framer,
                             bool enable_topic_routing)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())),
      assembler_(std::move(framer)),
      peer_id_(EndpointId(socket_)),
      enable_topic_routing_(enable_topic_routing) {}
```

> 注意成员初始化顺序须与 .hpp 声明顺序一致。`enable_topic_routing_` 与 `topic_assembler_` 在 private 末尾声明,`peer_id_` 之后;按声明顺序排初始化列表。`topic_assembler_` 默认构造无需列出。

`StartRead()` 的完成回调中,把切帧分支按 routing 开关分流:

```cpp
            if (enable_topic_routing_) {
              auto tfs = topic_assembler_.Feed(read_buf_.data(), n);
              if (!tfs) {
                HandleDisconnect(tfs.error);
                return;
              }
              for (auto& tf : tfs.value) {
                core_.DeliverFrame(std::move(tf.body), peer_id_, tf.topic);
              }
            } else {
              auto frames = assembler_.Feed(read_buf_.data(), n);
              if (!frames) {
                HandleDisconnect(frames.error);
                return;
              }
              for (auto& f : frames.value) {
                core_.DeliverFrame(std::move(f), peer_id_, "");
              }
            }
            StartRead();
```

把 `Send(data)` 改为复用 `EnqueueWrite`,并在 routing 开启时退化到 `Send(Message)`:

```cpp
Status TcpConnectionImpl::Send(const std::vector<uint8_t>& data) {
  if (enable_topic_routing_) {
    Message m;
    m.payload = data;  // topic 空
    return Send(m, Endpoint::Default());
  }
  if (!open_.load()) return Status::Fail("conn: not connected");
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  EnqueueWrite(std::move(enc.value));
  return Status::Success(std::monostate{});
}

Status TcpConnectionImpl::Send(const Message& msg, const Endpoint& to) {
  if (!enable_topic_routing_) {
    if (!msg.topic.empty())
      return Status::Fail("config: topic routing not enabled");
    return Send(msg.payload);  // 退化(TCP 地址即连接,忽略 to)
  }
  (void)to;  // TCP 地址即连接
  if (!open_.load()) return Status::Fail("conn: not connected");
  auto enc = core_.EncodeForSend(msg.payload, msg.topic);
  if (!enc) return Status::Fail(enc.error);
  EnqueueWrite(FrameStream(msg.topic, enc.value));
  return Status::Success(std::monostate{});
}

void TcpConnectionImpl::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}
```

(原 `Send(data)` 里的 `asio::post(...write_queue_...)` 逻辑已移入 `EnqueueWrite`。)

- [ ] **Step 3d: 透传开关到构造** — `src/tcp/TcpServerImpl.cpp` 把
`conn = std::make_shared<TcpConnectionImpl>(std::move(sock), framer);`
改为
`conn = std::make_shared<TcpConnectionImpl>(std::move(sock), framer, config_.enable_topic_routing);`

`src/tcp/TcpClientImpl.cpp` 构造基类处(约 26-29 行)把
`TcpConnectionImpl(asio::ip::tcp::socket(ctx), config.framer ? ... : nullptr)`
改为加第三参 `config.enable_topic_routing`:

```cpp
      TcpConnectionImpl(asio::ip::tcp::socket(ctx),
                    config.framer
                        ? std::make_shared<LengthFieldFramer>(*config.framer)
                        : nullptr,
                    config.enable_topic_routing),
```

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "TcpTopicRouting|Tcp" --output-on-failure 2>&1 | tail -15 && ctest --test-dir build 2>&1 | tail -4`
Expected: `TcpTopicRouting.*` PASS;既有 TCP 测试(routing 默认关)全部不回归。

- [ ] **Step 5: 提交**

```bash
git add include/transport/tcp/ src/tcp/ tests/topic_routing_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: TCP topic 路由(enable_topic_routing + 流帧 envelope 收发 + Send(Message))

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 5: UDP topic 路由

**Files:**
- Modify: `include/transport/udp/UdpConfig.hpp`,`include/transport/udp/UdpImpl.hpp`,`src/udp/UdpImpl.cpp`
- Modify: `tests/topic_routing_test.cpp`(追加 UDP 段)

- [ ] **Step 1: 写失败测试** — 在 `tests/topic_routing_test.cpp` 末尾追加

```cpp
// ---- UDP topic 路由(回环) ----
#include "transport/udp/UdpConfig.hpp"
#include "transport/udp/UdpImpl.hpp"

TEST(UdpTopicRouting, RoundTripSelectsCodecByTopic) {
  using namespace transport;
  // 接收端
  UdpConfig rcfg;
  rcfg.mode = UdpMode::kUnicast;
  rcfg.local_addr = "127.0.0.1";
  rcfg.local_port = 0;
  rcfg.enable_topic_routing = true;
  auto rx = std::make_shared<UdpImpl>(rcfg);
  ASSERT_TRUE(rx->Open().ok);
  rx->SetCodec("a", std::make_shared<TagCodec2>(0xAA));
  rx->SetCodec("b", std::make_shared<TagCodec2>(0xBB));

  // 发送端,默认目的地指向接收端口
  UdpConfig scfg;
  scfg.mode = UdpMode::kUnicast;
  scfg.local_addr = "127.0.0.1";
  scfg.local_port = 0;
  scfg.remote_addr = "127.0.0.1";
  scfg.remote_port = rx->LocalPort();
  scfg.enable_topic_routing = true;
  auto tx = std::make_shared<UdpImpl>(scfg);
  ASSERT_TRUE(tx->Open().ok);
  tx->SetCodec("a", std::make_shared<TagCodec2>(0xAA));
  tx->SetCodec("b", std::make_shared<TagCodec2>(0xBB));

  Message mb;
  mb.payload = Bytes{9, 8, 7};
  mb.topic = "b";
  ASSERT_TRUE(tx->Send(mb).ok);

  auto r = rx->Receive(2000);
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (Bytes{9, 8, 7}));

  tx->Close();
  rx->Close();
}

TEST(UdpTopicRouting, RoutingOffRejectsTopicSend) {
  using namespace transport;
  UdpConfig cfg;
  cfg.mode = UdpMode::kUnicast;
  cfg.local_addr = "127.0.0.1";
  cfg.local_port = 0;
  cfg.enable_topic_routing = false;
  auto u = std::make_shared<UdpImpl>(cfg);
  ASSERT_TRUE(u->Open().ok);
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = u->Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "config: topic routing not enabled");
  u->Close();
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake -S . -B build > /dev/null 2>&1; (cd build && cmake --build . --target transport_tests -j$(nproc)) 2>&1 | tail -15`
Expected: `UdpConfig` 无 `enable_topic_routing`;UDP 未覆写 `Send(Message)`。

- [ ] **Step 3a: config 开关** — `include/transport/udp/UdpConfig.hpp` 在 struct 末尾加:

```cpp
  bool        enable_topic_routing = false;  // 开启 topic→codec 多路复用
```

- [ ] **Step 3b: UdpImpl.hpp** — `include/transport/udp/UdpImpl.hpp`

加 include:`#include "transport/core/TopicEnvelope.hpp"`(`Message.hpp` 已含)。

public,`Send(data,Endpoint)` 声明后加:

```cpp
  Status Send(const Message& msg,
              const Endpoint& to = Endpoint::Default()) override;
```

补 `using ITransport::SetCodec;`(在 `using ITransport::Send;` 旁),并在 `SetCodec` 单参后加:

```cpp
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) override {
    core_.SetCodec(topic, std::move(codec));
  }
```

private,声明两个助手(替换原 `SendToEndpoint`):

```cpp
  Result<asio::ip::udp::endpoint> ResolveDest(const Endpoint& to);
  Status SendRaw(std::vector<uint8_t> bytes,
                 const asio::ip::udp::endpoint& dest);
```

(删除原 `Status SendToEndpoint(const std::vector<uint8_t>&, const asio::ip::udp::endpoint&);` 声明。)

- [ ] **Step 3c: UdpImpl.cpp** — `src/udp/UdpImpl.cpp`

加 include:`#include "transport/core/TopicEnvelope.hpp"`。

`StartReceive()` 的完成回调里,把交付分流(原 `core_.DeliverFrame(std::move(datagram), source, "")` 一行替换):

```cpp
            std::vector<uint8_t> datagram(recv_buf_.begin(),
                                          recv_buf_.begin() + n);
            if (config_.enable_topic_routing) {
              auto tf = UnpackTopic(datagram.data(), datagram.size());
              if (!tf) {
                core_.DeliverError(tf.error);  // 坏报文,不致命
              } else {
                core_.DeliverFrame(std::move(tf.value.body), source,
                                   tf.value.topic);
              }
            } else {
              core_.DeliverFrame(std::move(datagram), source, "");
            }
            StartReceive();
```

把原 `SendToEndpoint` 替换为 `ResolveDest` + `SendRaw`,并重写 `Send` 三个重载:

```cpp
Result<asio::ip::udp::endpoint> UdpImpl::ResolveDest(const Endpoint& to) {
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

Status UdpImpl::SendRaw(std::vector<uint8_t> bytes,
                        const asio::ip::udp::endpoint& dest) {
  if (!open_.load()) return Status::Fail("config: socket not open");
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf, dest]() {
    asio::error_code ec;
    socket_.send_to(asio::buffer(*buf), dest, 0, ec);
    if (ec) core_.DeliverError("io: send: " + ec.message());
  });
  return Status::Success(std::monostate{});
}

Status UdpImpl::Send(const std::vector<uint8_t>& data) {
  if (config_.enable_topic_routing) {
    Message m;
    m.payload = data;
    return Send(m, Endpoint::Default());
  }
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  return SendRaw(std::move(enc.value), default_dest_);
}

Status UdpImpl::Send(const std::vector<uint8_t>& data, const Endpoint& to) {
  if (config_.enable_topic_routing) {
    Message m;
    m.payload = data;
    return Send(m, to);
  }
  auto dest = ResolveDest(to);
  if (!dest) return Status::Fail(dest.error);
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  return SendRaw(std::move(enc.value), dest.value);
}

Status UdpImpl::Send(const Message& msg, const Endpoint& to) {
  if (!config_.enable_topic_routing) {
    if (!msg.topic.empty())
      return Status::Fail("config: topic routing not enabled");
    return Send(msg.payload, to);
  }
  auto dest = ResolveDest(to);
  if (!dest) return Status::Fail(dest.error);
  auto enc = core_.EncodeForSend(msg.payload, msg.topic);
  if (!enc) return Status::Fail(enc.error);
  return SendRaw(PackTopic(msg.topic, enc.value), dest.value);
}
```

> 行为保持:routing 关闭时 `Send(data)`→默认目的地、`Send(data,Net)`→定向、`Send(data,Topic)`→`config: udp expects net endpoint`,与今天一致(`ResolveDest` 复现原 switch)。

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "UdpTopicRouting|Udp" --output-on-failure 2>&1 | tail -15 && ctest --test-dir build 2>&1 | tail -4`
Expected: `UdpTopicRouting.*` PASS;既有 UDP 测试不回归。

- [ ] **Step 5: 提交**

```bash
git add include/transport/udp/ src/udp/ tests/topic_routing_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: UDP topic 路由(报文 envelope 收发 + Send(Message) + ResolveDest/SendRaw 重构)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 6: 串口 topic 路由

**Files:**
- Modify: `include/transport/serial/SerialConfig.hpp`,`include/transport/serial/SerialImpl.hpp`,`src/serial/SerialImpl.cpp`
- Create: `tests/serial/serial_topic_routing_test.cpp`(用 openpty,与 `tests/serial/serial_transport_test.cpp` 同款 `Pty` 搭建)
- Modify: `CMakeLists.txt`(在 `tests/serial/serial_transport_test.cpp` 后注册)

> 离线开关行为放进根 `tests/topic_routing_test.cpp`(下方第一段);需要 pty 的端到端收发放进 `tests/serial/serial_topic_routing_test.cpp`(第二段,`Pty` 结构照搬既有串口测试)。

- [ ] **Step 1a: 写离线开关测试** — 在 `tests/topic_routing_test.cpp` 末尾追加

```cpp
// ---- 串口 topic 路由(开关行为,离线可测) ----
#include "transport/serial/SerialConfig.hpp"
#include "transport/serial/SerialImpl.hpp"

TEST(SerialTopicRouting, RoutingOffRejectsTopicSend) {
  using namespace transport;
  SerialConfig cfg;
  cfg.device = "/dev/null";  // 不依赖真实串口;routing-off 分支在 open 检查前返回
  cfg.enable_topic_routing = false;
  auto s = std::make_shared<SerialImpl>(cfg);
  Message m;
  m.payload = Bytes{1};
  m.topic = "x";
  auto st = s->Send(m);
  EXPECT_FALSE(st.ok);
  EXPECT_EQ(st.error, "config: topic routing not enabled");
}
```

- [ ] **Step 1b: 写 pty 端到端测试** — 新建 `tests/serial/serial_topic_routing_test.cpp`

`Pty` 结构与 `Cfg` 助手照搬 `tests/serial/serial_transport_test.cpp` 的写法(openpty 主从端;SerialImpl 打开从端 by name,测试读写主端 fd)。验证**接收路径**:测试向主端写入一帧 `FrameStream("b", CodecB.Encode(body))`,SerialImpl(从端,路由开)应解码出 `Message{topic="b", payload=body}`;并验证**发送路径**:SerialImpl `Send(Message{topic="a"})` 后,主端读到的字节等于 `FrameStream("a", CodecA.Encode(payload))`。

```cpp
#include "transport/serial/SerialImpl.hpp"

#include <pty.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/core/TopicEnvelope.hpp"

using transport::FrameStream;
using transport::ICodec;
using transport::Message;
using transport::Result;
using transport::SerialConfig;
using transport::SerialImpl;
using Bytes = std::vector<uint8_t>;

namespace {
// 照搬 serial_transport_test.cpp 的 Pty:SerialImpl 开从端 by name,测试读写主端。
struct Pty {
  int master = -1;
  std::string slave_name;
  Pty() {
    int slave_fd = -1;
    char name[256] = {0};
    if (openpty(&master, &slave_fd, name, nullptr, nullptr) == 0) {
      slave_name = name;
      ::close(slave_fd);
    }
  }
  ~Pty() {
    if (master >= 0) ::close(master);
  }
  bool ok() const { return master >= 0 && !slave_name.empty(); }
  void WriteMaster(const Bytes& d) { ASSERT_GE(::write(master, d.data(), d.size()), 0); }
};

class TagCodec : public ICodec {
 public:
  explicit TagCodec(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o{tag_};
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};

SerialConfig RoutingCfg(const std::string& dev) {
  SerialConfig c;
  c.device = dev;
  c.baud_rate = 115200;
  c.enable_topic_routing = true;
  return c;
}
}  // namespace

TEST(SerialTopicRoutingPty, ReceiveDecodesByTopic) {
  Pty pty;
  if (!pty.ok()) GTEST_SKIP() << "openpty unavailable";
  auto s = std::make_shared<SerialImpl>(RoutingCfg(pty.slave_name));
  ASSERT_TRUE(s->Open().ok);
  s->SetCodec("b", std::make_shared<TagCodec>(0xBB));

  // 主端写入一帧:FrameStream("b", CodecB.Encode({7,8})) = [len][0,1]['b'][0xBB,7,8]
  Bytes encoded{0xBB, 7, 8};
  pty.WriteMaster(FrameStream("b", encoded));

  auto r = s->Receive(2000);
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (Bytes{7, 8}));
  s->Close();
}
```

> 说明:发送路径已由 `TcpTopicRouting`(与串口共用同一 `FrameStream`/`EnqueueWrite` 流帧逻辑)充分覆盖,故此处只补串口独有的接收分帧分支;如需也校验串口发送字节,可加一个 `Send(Message{topic="a"})` 后 `::read(master, ...)` 比对 `FrameStream("a", {0xAA,...})` 的用例(可选)。

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake -S . -B build > /dev/null 2>&1; (cd build && cmake --build . --target transport_tests -j$(nproc)) 2>&1 | tail -15`
Expected: `SerialConfig` 无 `enable_topic_routing`;串口未覆写 `Send(Message)`。

- [ ] **Step 3a: config 开关** — `include/transport/serial/SerialConfig.hpp` 在 struct 末尾(`framer` 后)加:

```cpp
  bool enable_topic_routing = false;  // 开启 topic→codec 多路复用(忽略 framer)
```

- [ ] **Step 3b: SerialImpl.hpp** — `include/transport/serial/SerialImpl.hpp`

加 include:`#include "transport/Message.hpp"`、`#include "transport/core/TopicEnvelope.hpp"`。

public,`Send(data)` 后加 `Send(Message)`;补 `using ITransport::Send;`(若尚无)与 `using ITransport::SetCodec;`;`SetCodec` 单参后加 topic 重载;声明与 TCP 同构的 `EnqueueWrite`、`enable_topic_routing_`、`topic_assembler_`。具体:

```cpp
  using ITransport::Send;
  using ITransport::SetCodec;
  Status Send(const std::vector<uint8_t>& data) override;
  Status Send(const Message& msg,
              const Endpoint& to = Endpoint::Default()) override;
  void SetCodec(std::shared_ptr<ICodec> codec) override { core_.SetCodec(std::move(codec)); }
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) override {
    core_.SetCodec(topic, std::move(codec));
  }
```

private 加:

```cpp
  void EnqueueWrite(std::vector<uint8_t> bytes);
  bool enable_topic_routing_;
  TopicFrameAssembler topic_assembler_;
```

(若原 `SetCodec` 单参已存在,替换为上面这组;`using` 行注意避免重复。)

- [ ] **Step 3c: SerialImpl.cpp** — `src/serial/SerialImpl.cpp`

加 include:`#include "transport/core/TopicEnvelope.hpp"`。

构造函数初始化列表追加 `enable_topic_routing_(config_.enable_topic_routing)`(注意放在与 .hpp 声明顺序一致的位置)。

`StartRead()` 完成回调里按开关分流(替换原 `assembler_.Feed` 块):

```cpp
            if (enable_topic_routing_) {
              auto tfs = topic_assembler_.Feed(read_buf_.data(), n);
              if (!tfs) {
                HandleDisconnect(tfs.error);
                return;
              }
              for (auto& tf : tfs.value) {
                core_.DeliverFrame(std::move(tf.body), config_.device, tf.topic);
              }
            } else {
              auto frames = assembler_.Feed(read_buf_.data(), n);
              if (!frames) {
                HandleDisconnect(frames.error);
                return;
              }
              for (auto& f : frames.value) {
                core_.DeliverFrame(std::move(f), config_.device, "");
              }
            }
            StartRead();
```

`Send` 重写(与 TCP 同构;注意串口未开错误串沿用既有 `"config: serial not open"`):

```cpp
Status SerialImpl::Send(const std::vector<uint8_t>& data) {
  if (enable_topic_routing_) {
    Message m;
    m.payload = data;
    return Send(m, Endpoint::Default());
  }
  if (!open_.load()) return Status::Fail("config: serial not open");
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  EnqueueWrite(std::move(enc.value));
  return Status::Success(std::monostate{});
}

Status SerialImpl::Send(const Message& msg, const Endpoint& to) {
  if (!enable_topic_routing_) {
    if (!msg.topic.empty())
      return Status::Fail("config: topic routing not enabled");
    return Send(msg.payload);
  }
  (void)to;
  if (!open_.load()) return Status::Fail("config: serial not open");
  auto enc = core_.EncodeForSend(msg.payload, msg.topic);
  if (!enc) return Status::Fail(enc.error);
  EnqueueWrite(FrameStream(msg.topic, enc.value));
  return Status::Success(std::monostate{});
}

void SerialImpl::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}
```

> 注:`SerialTopicRouting.RoutingOffRejectsTopicSend` 用 `/dev/null` 且未 Open。当前 `Send(Message)` routing-off 分支在判 `msg.topic` 非空后立即返回 `config: topic routing not enabled`,**先于** open 检查,故测试不依赖串口真正打开。保持该判断顺序。

- [ ] **Step 3d: 注册 pty 测试源** — `CMakeLists.txt`,在 `tests/serial/serial_transport_test.cpp` 一行之后加:

```cmake
    tests/serial/serial_topic_routing_test.cpp
```

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "SerialTopicRouting|Serial" --output-on-failure 2>&1 | tail -15 && ctest --test-dir build 2>&1 | tail -4`
Expected: `SerialTopicRouting.*` / `SerialTopicRoutingPty.*` PASS(无 pty 环境则后者 SKIP);既有串口测试不回归。

- [ ] **Step 5: 提交**

```bash
git add include/transport/serial/ src/serial/ tests/topic_routing_test.cpp tests/serial/serial_topic_routing_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: 串口 topic 路由(流帧 envelope 收发 + Send(Message),与 TCP 同构)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 7: DDS topic 路由(原生 topic,零 wire 改动)

**Files:**
- Modify: `include/transport/dds/DdsImpl.hpp`,`src/dds/DdsImpl.cpp`
- Create: `tests/dds/dds_topic_routing_test.cpp`(放在 `tests/dds/` 以复用 `FakeDdsProvider.hpp` 的 include 路径)
- Modify: `CMakeLists.txt`(在 dds 测试源附近注册新文件)

> DDS 无 `enable_topic_routing` 开关:始终启用注册表,topic 即原生 topic,不加 in-band envelope。
> 本段 setup 已对照 `tests/dds/dds_impl_pubsub_test.cpp` 核实:`FakeDdsProvider::Bus` 共享总线 + 经 `DdsImpl` 构造函数第二参注入 `std::make_unique<FakeDdsProvider>(bus)`;`tx`/`rx` 两端;`DdsConfig{mode=kPubSub, topics={...}}`,provider 默认名不变(Fake 经构造注入,不走注册表)。

- [ ] **Step 1: 写失败测试** — 新建 `tests/dds/dds_topic_routing_test.cpp`

```cpp
#include "transport/dds/DdsImpl.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "FakeDdsProvider.hpp"
#include "transport/ICodec.hpp"
#include "transport/Message.hpp"

using transport::DdsConfig;
using transport::DdsImpl;
using transport::DdsMode;
using transport::FakeDdsProvider;
using transport::ICodec;
using transport::Message;
using transport::Result;
using Bytes = std::vector<uint8_t>;

namespace {
DdsConfig PubSubCfg(std::vector<std::string> topics) {
  DdsConfig c;
  c.mode = DdsMode::kPubSub;
  c.topics = std::move(topics);
  return c;
}
std::shared_ptr<DdsImpl> Make(std::shared_ptr<FakeDdsProvider::Bus> bus,
                              DdsConfig cfg) {
  return std::make_shared<DdsImpl>(std::move(cfg),
                                   std::make_unique<FakeDdsProvider>(bus));
}
class TagCodec : public ICodec {
 public:
  explicit TagCodec(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o{tag_};
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};
}  // namespace

TEST(DdsTopicRouting, SendMessageEncodesByTopic) {
  auto bus = std::make_shared<FakeDdsProvider::Bus>();
  auto tx = Make(bus, PubSubCfg({"b"}));
  auto rx = Make(bus, PubSubCfg({"b"}));
  ASSERT_TRUE(tx->Open().ok);
  ASSERT_TRUE(rx->Open().ok);
  // tx 编码、rx 解码,两端都注册 "b"→CodecB。
  tx->SetCodec("b", std::make_shared<TagCodec>(0xBB));
  rx->SetCodec("b", std::make_shared<TagCodec>(0xBB));
  ASSERT_TRUE(rx->Subscribe("b").ok);

  Message m;
  m.payload = Bytes{4, 5};
  m.topic = "b";
  ASSERT_TRUE(tx->Send(m).ok);  // 发往原生 topic "b",按 "b" 选 codec 编码

  auto r = rx->Receive(1000);
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (Bytes{4, 5}));  // CodecB 解码还原,无前缀残留
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cmake -S . -B build > /dev/null 2>&1; (cd build && cmake --build . --target transport_tests -j$(nproc)) 2>&1 | tail -15`
Expected: DDS 未覆写 `Send(Message)`(走基类默认 → topic 非空即 `io: topic routing not supported`),测试失败。

- [ ] **Step 3a: DdsImpl.hpp** — `include/transport/dds/DdsImpl.hpp`

public,`Send(data,Endpoint)` 声明后加 `Send(Message)`;补 `using ITransport::SetCodec;`;`SetCodec` 单参后加 topic 重载:

```cpp
  Status Send(const Message& msg,
              const Endpoint& to = Endpoint::Default()) override;
```

```cpp
  using ITransport::SetCodec;
  void SetCodec(std::shared_ptr<ICodec> c) override { core_.SetCodec(std::move(c)); }
  void SetCodec(const std::string& topic, std::shared_ptr<ICodec> codec) override {
    core_.SetCodec(topic, std::move(codec));
  }
```

(`Message.hpp` 已被 DdsImpl.hpp 包含。)

- [ ] **Step 3b: DdsImpl.cpp** — `src/dds/DdsImpl.cpp`

把 `SendToTopic` 改为按 topic 选 codec 编码(原 `core_.EncodeForSend(data)` → 带 topic):

```cpp
Status DdsImpl::SendToTopic(const std::vector<uint8_t>& data,
                            const std::string& topic) {
  if (auto st = RequireMode(DdsMode::kPubSub); !st) return st;
  if (auto st = RequireOpen(); !st) return st;
  auto enc = core_.EncodeForSend(data, topic);  // 按 topic 选 codec
  if (!enc) return Status::Fail(enc.error);
  return provider_->Publish(topic, enc.value);
}
```

新增 `Send(Message)` 覆写(topic 空 → 退化既有 Endpoint 语义;非空 → 发往该原生 topic):

```cpp
Status DdsImpl::Send(const Message& msg, const Endpoint& to) {
  if (msg.topic.empty()) return Send(msg.payload, to);
  return SendToTopic(msg.payload, msg.topic);
}
```

(`Send(data)` → `SendToTopic(data, topics[0])` 保持不变,现在自动按 `topics[0]` 选 codec;Subscribe 的 `core_.DeliverFrame(payload, topic, topic)` 已使解码按 topic 选 codec——T2 已就位,无需改 Subscribe。)

- [ ] **Step 3c: 注册测试源** — `CMakeLists.txt`,在 `tests/dds/dds_impl_reqresp_test.cpp` 一行之后加:

```cmake
    tests/dds/dds_topic_routing_test.cpp
```

(该文件用 FakeDdsProvider,与现有 `dds_impl_pubsub_test.cpp` 同样**不**受 `TRANSPORT_HAS_FASTDDS` 门控,放入主源列表。)

- [ ] **Step 4: 跑测试确认通过 + 全量回归**

Run: `cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "DdsTopicRouting|Dds|dds" --output-on-failure 2>&1 | tail -15 && ctest --test-dir build 2>&1 | tail -4`
Expected: `DdsTopicRouting.*` PASS;既有 DDS 测试不回归。

- [ ] **Step 5: 提交**

```bash
git add include/transport/dds/ src/dds/ tests/dds/dds_topic_routing_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: DDS topic 路由(Send(Message) 发往原生 topic + SendToTopic 按 topic 编码)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Task 8: 文档同步

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-06-09-transport-middleware-design.md`(主 spec)
- Modify: `docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md`(基础架构 spec,含 `TransportCore` 描述)

- [ ] **Step 1: README —— 加「单连接多帧格式(topic 路由)」用法**

在 README 现有 UDP/DDS 用法示例附近,新增一节,示例代码(确保与最终 API 一致):

```cpp
// 一条 TCP 连接上跑多种帧格式,按 topic 选 codec
TcpClientConfig cfg;
cfg.host = "10.0.0.7"; cfg.port = 9000;
cfg.enable_topic_routing = true;            // 开启 topic 路由
auto t = TransportFactory::Create(cfg);
t->SetCodec("telemetry", std::make_shared<TelemetryCodec>());
t->SetCodec("command",   std::make_shared<CommandCodec>());
t->Open();

Message msg;
msg.payload = serialize(my_telemetry);      // 应用原始字节
msg.topic   = "telemetry";                   // 选 codec + in-band 通道
t->Send(msg);

auto m = t->Receive();                        // m.topic 标明通道, m.payload 已解码
t->Send(m.value);                             // echo:按同 topic 重新编码往返
```

并在 README 说明:`enable_topic_routing` 默认关闭(逐字节兼容旧格式);DDS 无需开关(原生 topic);wire 见 spec §6。

- [ ] **Step 2: 主 spec `2026-06-09`** — `ITransport` 接口定义处加 `Send(const Message&, Endpoint=Default())` 与 `SetCodec(topic, codec)`;新增「topic 路由与 envelope wire 格式」小节(引用本设计 §6 的两种 envelope 形态);三个流式/数据报 config 加 `enable_topic_routing`;changelog 加 2026-06-12 条目(topic 路由编解码多路复用,opt-in,DDS 始终启用)。

- [ ] **Step 3: as-built 架构 spec `2026-06-10`** — `TransportCore` 描述由「单 codec」改为「topic→codec 注册表 + 默认 codec」;若有类图,`TransportCore` 增 `+SetCodec(topic,codec)` / `+EncodeForSend(data,topic)`,并标注新增 header-only `TopicEnvelope`。

- [ ] **Step 4: 残留引用检查**

Run: `cd /home/ubuntu/david/transport && grep -rn "单 codec\|single codec\|只能挂一个" docs README.md || echo "clean"`
Expected: 无与「单 codec」矛盾的过时描述(或仅历史 changelog 中保留)。

- [ ] **Step 5: 提交**

```bash
git add README.md docs/superpowers/specs/
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "docs: topic 路由编解码多路复用 同步 README/主 spec/as-built

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## 完成后

四个传输全部覆盖、注册表 + envelope + `Send(Message)` 到位、opt-in 兼容旧格式、文档同步。最终全量构建 + 测试:

```bash
cmake -S . -B build > /dev/null && cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build 2>&1 | tail -4
```

按 subagent-driven-development 的终止步骤:全部任务完成后 → 派发最终整支 reviewer → 用 superpowers:finishing-a-development-branch 收尾(合并 master + 推送,与项目一贯做法一致)。
