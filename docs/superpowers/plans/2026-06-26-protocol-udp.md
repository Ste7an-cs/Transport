# 外部协议跑 UDP 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让外部协议(`SystemCodec` 帧)在 UDP 上正确跑 1:多——无状态报文 codec + 应答回送来源 + 发送可指定目的地。

**Architecture:** 抽出共享帧核(`EncodeSystemFrame`/`ScanSystemFrames`),新增无状态 `SystemDatagramCodec`(报文版);`ProtocolConfig.reply_to_source` → `ProtocolPolicy.ReplyTo` 回 `Net(来源 ip:port)`;`ProtocolNode` 5 个发送方法加可选 `const Endpoint& to`。引擎/policy 接口不动,TCP/串口/DDS 不变。

**Tech Stack:** C++17,不抛异常,GoogleTest,CMake。配套 spec:`docs/superpowers/specs/2026-06-26-protocol-udp-design.md`。

## Global Constraints

- C++17,**不抛异常**;`Result`/`Status`。`SystemDatagramCodec` header-only;接口层零第三方依赖。
- 帧逻辑**共用、不复制**:流式 `SystemCodec` 与报文 `SystemDatagramCodec` 共用 `EncodeSystemFrame`/`ScanSystemFrames`。
- 默认 `reply_to_source=false`、发送 `to=Endpoint::Default()` → TCP/串口/1:1 及现有调用点**零改动**;TCP/串口对非 Default Endpoint 返回 `io: addressed send not supported`,故它们必须留默认。
- `SystemCodec` 现有字节级行为**逐字不变**(`system_codec_test` 即回归证明)。
- 现有 100 个测试不回归。提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**无 Co-Authored-By**。不提交 `build/`。
- 文档(SRS/SDD/README/CHANGELOG)+ demo 同步留到实现后。

---

## File Structure

| 文件 | 责任 | 任务 |
|---|---|---|
| `include/transport/codec/SystemCodec.hpp`(改) | 加 `EncodeSystemFrame`/`ScanSystemFrames` 声明;移除类内 kHeaderLen/kMaxBody | T1 |
| `src/codec/SystemCodec.cpp`(改) | 定义两自由函数;Encode/Decode 改为调用(行为不变) | T1 |
| `include/transport/codec/SystemDatagramCodec.hpp`(新) | 无状态报文帧 codec(header-only) | T1 |
| `tests/codec/system_datagram_codec_test.cpp`(新) | 报文 codec 单元测试 | T1 |
| `include/transport/comm/ProtocolPolicy.hpp`(改) | 构造加 `reply_to_source`;`ReplyTo` 解析 source | T2 |
| `include/transport/comm/ProtocolNode.hpp`(改) | `ProtocolConfig` 加 `reply_to_source`;构造传入 policy | T2 |
| `src/comm/ProtocolNode.cpp`(改) | 构造把 `config.reply_to_source` 传 `ProtocolPolicy` | T2 |
| `tests/comm/protocol_policy_test.cpp`(新) | `ReplyTo` 解析/开关单元测试 | T2 |
| `include/transport/comm/ProtocolNode.hpp`(改) | 5 发送方法加 `const Endpoint& to` | T3 |
| `src/comm/ProtocolNode.cpp`(改) | 发送方法转发 `to` 给引擎原语 | T3 |
| `tests/comm/protocol_node_test.cpp`(改) | 目的地透传用例 | T3 |
| `CMakeLists.txt`(改) | 注册两个新测试文件 | T1/T2 |

---

### Task 1: 共享帧核 + `SystemDatagramCodec`

**Files:**
- Modify: `include/transport/codec/SystemCodec.hpp`
- Modify: `src/codec/SystemCodec.cpp`
- Create: `include/transport/codec/SystemDatagramCodec.hpp`
- Create: `tests/codec/system_datagram_codec_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces:
  - `Result<std::vector<uint8_t>> transport::EncodeSystemFrame(const Message&, const CrcFn&)`
  - `std::size_t transport::ScanSystemFrames(const uint8_t* data, std::size_t len, const CrcFn&, std::vector<Message>& out)` —— 扫描 `data[0..len)`,push 完整帧到 out,返回已消费字节数。
  - `class transport::SystemDatagramCodec : public ICodec`(构造 `(CrcFn = DefaultCrc16)`;`Decode` 无状态、丢弃残留)。

- [ ] **Step 1: 写失败测试 `tests/codec/system_datagram_codec_test.cpp`**

```cpp
#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/codec/SystemCodec.hpp"

#include <cstdint>
#include <vector>
#include <gtest/gtest.h>

using transport::FrameType;
using transport::Message;
using transport::SystemCodec;
using transport::SystemDatagramCodec;

namespace {
// 确定性注入 CRC:body 字节和(便于字节级断言)。
uint16_t SumCrc(const uint8_t* b, std::size_t n) {
  uint16_t s = 0;
  for (std::size_t i = 0; i < n; ++i) s = static_cast<uint16_t>(s + b[i]);
  return s;
}
Message Cmd(uint8_t proto, uint8_t sess, uint16_t mid, std::vector<uint8_t> p) {
  Message m; m.frm_type = FrameType::kCommand; m.protocol_id = proto;
  m.session_id = sess; m.message_id = mid; m.payload = std::move(p); return m;
}
std::vector<Message> Decode(SystemDatagramCodec& c, const std::vector<uint8_t>& b) {
  auto r = c.Decode(b.data(), b.size());
  EXPECT_TRUE(static_cast<bool>(r));
  return r.value;
}
}  // namespace

TEST(SystemDatagramCodec, EncodeMatchesStreaming) {
  SystemCodec stream(SumCrc);
  SystemDatagramCodec dgram(SumCrc);
  Message m = Cmd(1, 7, 0x0102, {0xAA, 0xBB});
  auto a = stream.Encode(m); auto b = dgram.Encode(m);
  ASSERT_TRUE(static_cast<bool>(a)); ASSERT_TRUE(static_cast<bool>(b));
  EXPECT_EQ(a.value, b.value);                 // 编码两者字节一致
}

TEST(SystemDatagramCodec, DecodesSingleWholeFrame) {
  SystemDatagramCodec c(SumCrc);
  auto frame = SystemCodec(SumCrc).Encode(Cmd(2, 9, 0x0033, {1, 2, 3})).value;
  auto out = Decode(c, frame);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].frm_type, FrameType::kCommand);
  EXPECT_EQ(out[0].session_id, 9);
  EXPECT_EQ(out[0].message_id, 0x0033);
  EXPECT_EQ(out[0].payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(SystemDatagramCodec, DecodesMultipleFramesInOneDatagram) {
  SystemCodec enc(SumCrc);
  auto f1 = enc.Encode(Cmd(1, 1, 0x0001, {0xA})).value;
  auto f2 = enc.Encode(Cmd(1, 2, 0x0002, {0xB})).value;
  std::vector<uint8_t> both = f1; both.insert(both.end(), f2.begin(), f2.end());
  SystemDatagramCodec c(SumCrc);
  auto out = Decode(c, both);
  ASSERT_EQ(out.size(), 2u);
  EXPECT_EQ(out[0].message_id, 0x0001);
  EXPECT_EQ(out[1].message_id, 0x0002);
}

TEST(SystemDatagramCodec, StatelessNoCarryOverAcrossDatagrams) {
  SystemDatagramCodec c(SumCrc);
  auto whole = SystemCodec(SumCrc).Encode(Cmd(1, 1, 0x0001, {9, 9, 9})).value;
  std::vector<uint8_t> truncated(whole.begin(), whole.end() - 1);   // 缺最后一字节
  EXPECT_EQ(Decode(c, truncated).size(), 0u);                       // 半截 → 0,且不保留
  auto good = SystemCodec(SumCrc).Encode(Cmd(1, 2, 0x0002, {7})).value;
  auto out = Decode(c, good);                                       // 下一报文不被污染
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].message_id, 0x0002);
}

TEST(SystemDatagramCodec, CrcMismatchDropped) {
  SystemDatagramCodec c(SumCrc);
  auto frame = SystemCodec(SumCrc).Encode(Cmd(1, 1, 0x0001, {5})).value;
  frame[11] ^= 0xFF;                                                // 篡改 crc 字段
  EXPECT_EQ(Decode(c, frame).size(), 0u);
}

TEST(SystemDatagramCodec, EmptyYieldsNone) {
  SystemDatagramCodec c(SumCrc);
  EXPECT_EQ(Decode(c, {}).size(), 0u);
}
```

注册到 `CMakeLists.txt`(`tests/codec/datagram_codec_test.cpp` 行之后):
```
    tests/codec/system_datagram_codec_test.cpp
```
Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— 找不到 `transport/codec/SystemDatagramCodec.hpp`。

- [ ] **Step 2: 抽出共享帧核 —— 改 `SystemCodec.hpp`**

(a) 顶部 include 区补(`ScanSystemFrames` 签名用到 `Message`):
```cpp
#include "transport/Message.hpp"
```
(b) `DefaultCrc16` 声明之后、`class SystemCodec` 之前,加两个自由函数声明:
```cpp
// 共享帧核:SystemCodec(流式)与 SystemDatagramCodec(报文)共用,逻辑不复制。
Result<std::vector<uint8_t>> EncodeSystemFrame(const Message& msg, const CrcFn& crc);
// 从 data[0..len) 扫描尽可能多的完整帧 push 进 out,返回已消费字节数(剩余为未完成/残留)。
std::size_t ScanSystemFrames(const uint8_t* data, std::size_t len, const CrcFn& crc,
                             std::vector<Message>& out);
```
(c) 删除类内私有的两行常量(移到 .cpp 文件作用域):
```cpp
  static constexpr std::size_t kHeaderLen = 15;
  static constexpr std::size_t kMaxBody = 65535;
```

- [ ] **Step 3: 改 `src/codec/SystemCodec.cpp` —— 定义自由函数,Encode/Decode 改为调用**

(a) 在匿名命名空间(`constexpr std::array<...> kHeadFlag...` 同块)加两个常量:
```cpp
constexpr std::size_t kHeaderLen = 15;
constexpr std::size_t kMaxBody = 65535;
```
(b) 把 `SystemCodec::Encode` 整个函数体搬进自由函数 `EncodeSystemFrame`,`crc_` 换成参数 `crc`;`SystemCodec::Encode` 改为转发。即把现有 `Encode` 实现替换为:
```cpp
Result<std::vector<uint8_t>> EncodeSystemFrame(const Message& msg, const CrcFn& crc) {
  using R = Result<std::vector<uint8_t>>;
  if (msg.payload.size() + 2 > kMaxBody) return R::Fail("frame: payload too long");

  std::vector<uint8_t> body;
  body.reserve(2 + msg.payload.size());
  PutU16LE(body, msg.message_id);
  body.insert(body.end(), msg.payload.begin(), msg.payload.end());
  const uint16_t crc_v = crc(body.data(), body.size());

  std::vector<uint8_t> out;
  out.reserve(kHeaderLen + body.size());
  out.insert(out.end(), kHeadFlag.begin(), kHeadFlag.end());
  out.push_back(static_cast<uint8_t>(msg.frm_type));
  out.push_back(msg.protocol_id);
  out.push_back(msg.session_id);
  out.insert(out.end(), {0, 0, 0, 0});
  PutU16LE(out, crc_v);
  PutU16LE(out, static_cast<uint16_t>(body.size()));
  out.insert(out.end(), body.begin(), body.end());
  return R::Success(std::move(out));
}

Result<std::vector<uint8_t>> SystemCodec::Encode(const Message& msg) {
  return EncodeSystemFrame(msg, crc_);
}
```
(c) 把 `SystemCodec::Decode` 的 while 扫描循环搬进自由函数 `ScanSystemFrames`(对 `data[0..len)` 而非成员 buffer),返回 off;`SystemCodec::Decode` 改为 append→scan→erase。替换现有 `Decode` 实现为:
```cpp
std::size_t ScanSystemFrames(const uint8_t* data, std::size_t len, const CrcFn& crc,
                             std::vector<Message>& out) {
  std::size_t off = 0;
  while (true) {
    std::size_t flag = off;
    bool found = false;
    while (flag + 4 <= len) {
      if (MatchFlag(data + flag)) { found = true; break; }
      ++flag;
    }
    if (!found) { off = flag; break; }
    off = flag;

    if (len - off < kHeaderLen) break;
    const uint8_t* h = data + off;
    const uint16_t crc_in = GetU16LE(h + 11);
    const uint16_t frm_len = GetU16LE(h + 13);
    if (len - off - kHeaderLen < frm_len) break;

    const uint8_t* bd = h + kHeaderLen;
    if (crc(bd, frm_len) != crc_in) { off += 1; continue; }

    Message m;
    m.frm_type = static_cast<FrameType>(h[4]);
    switch (m.frm_type) {
      case FrameType::kUnknown: case FrameType::kCommand: case FrameType::kResponse:
      case FrameType::kResult: case FrameType::kState: case FrameType::kHeartbeat: break;
      default: m.frm_type = FrameType::kUnknown;
    }
    m.protocol_id = h[5];
    m.session_id = h[6];
    if (frm_len >= 2) {
      m.message_id = GetU16LE(bd);
      m.payload.assign(bd + 2, bd + frm_len);
    }
    out.push_back(std::move(m));
    off += kHeaderLen + frm_len;
  }
  return off;
}

Result<std::vector<Message>> SystemCodec::Decode(const uint8_t* data, std::size_t len) {
  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  const std::size_t consumed = ScanSystemFrames(buffer_.data(), buffer_.size(), crc_, out);
  if (consumed > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + consumed);
  return Result<std::vector<Message>>::Success(std::move(out));
}
```

- [ ] **Step 4: 写 `include/transport/codec/SystemDatagramCodec.hpp`**

```cpp
#pragma once

// SystemDatagramCodec.hpp — 外部协议帧的【无状态报文版】codec(header-only)。
// 与流式 SystemCodec 共用 EncodeSystemFrame/ScanSystemFrames;每次 Decode 只解这一个
// datagram、吐出其中整帧,残留直接丢弃、零跨报文保留 → 适配 UDP(多对端安全)。

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/codec/SystemCodec.hpp"   // CrcFn / DefaultCrc16 / EncodeSystemFrame / ScanSystemFrames

namespace transport {

class SystemDatagramCodec : public ICodec {
 public:
  explicit SystemDatagramCodec(CrcFn crc = DefaultCrc16) : crc_(std::move(crc)) {}

  Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    return EncodeSystemFrame(msg, crc_);
  }
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override {
    std::vector<Message> out;
    (void)ScanSystemFrames(data, len, crc_, out);   // 残留丢弃,不跨报文
    return Result<std::vector<Message>>::Success(std::move(out));
  }

 private:
  CrcFn crc_;
};

}  // namespace transport
```

- [ ] **Step 5: 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "SystemDatagramCodec|SystemCodec" --output-on-failure`
Expected: 新 `SystemDatagramCodec.*`(6)全过;现有 `SystemCodec.*` 全过(共享核重构后行为不变)。零告警。

- [ ] **Step 6: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(100 + 6 = 106)。
```bash
git add include/transport/codec/SystemCodec.hpp src/codec/SystemCodec.cpp \
        include/transport/codec/SystemDatagramCodec.hpp tests/codec/system_datagram_codec_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: SystemDatagramCodec(无状态报文版外部帧)+ 抽共享帧核 EncodeSystemFrame/ScanSystemFrames"
```

---

### Task 2: `reply_to_source`(应答回送来源)

**Files:**
- Modify: `include/transport/comm/ProtocolPolicy.hpp`
- Modify: `include/transport/comm/ProtocolNode.hpp`(`ProtocolConfig`)
- Modify: `src/comm/ProtocolNode.cpp`(构造)
- Create: `tests/comm/protocol_policy_test.cpp`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Produces:
  - `ProtocolPolicy(uint8_t protocol_id, bool reply_to_source = false)`;`ReplyTo(req)` 在 `reply_to_source` 且 `req.source` 形如 `host:port` 时返回 `Endpoint::Net(host, port)`,否则 `Endpoint::Default()`。
  - `ProtocolConfig` 新增字段 `bool reply_to_source = false;`。

- [ ] **Step 1: 写失败测试 `tests/comm/protocol_policy_test.cpp`**

```cpp
#include "transport/comm/ProtocolPolicy.hpp"

#include <string>
#include <gtest/gtest.h>

using transport::Endpoint;
using transport::Message;
using transport::ProtocolPolicy;

namespace {
Message FromSource(const std::string& src) { Message m; m.source = src; return m; }
}  // namespace

TEST(ProtocolPolicy, ReplyToSourceWhenEnabled) {
  ProtocolPolicy p(1, /*reply_to_source=*/true);
  Endpoint e = p.ReplyTo(FromSource("192.168.1.5:7000"));
  EXPECT_EQ(e.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(e.host, "192.168.1.5");
  EXPECT_EQ(e.port, 7000);
}

TEST(ProtocolPolicy, ReplyDefaultWhenDisabled) {
  ProtocolPolicy p(1, /*reply_to_source=*/false);
  EXPECT_EQ(p.ReplyTo(FromSource("192.168.1.5:7000")).kind, Endpoint::Kind::kDefault);
}

TEST(ProtocolPolicy, ReplyDefaultOnMalformedSource) {
  ProtocolPolicy p(1, true);
  EXPECT_EQ(p.ReplyTo(FromSource("")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("nocolon")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("host:")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("host:abc")).kind, Endpoint::Kind::kDefault);
  EXPECT_EQ(p.ReplyTo(FromSource("host:0")).kind, Endpoint::Kind::kDefault);
}

TEST(ProtocolPolicy, ReplyToIpv6LastColon) {
  ProtocolPolicy p(1, true);
  Endpoint e = p.ReplyTo(FromSource("::1:9000"));
  EXPECT_EQ(e.kind, Endpoint::Kind::kNet);
  EXPECT_EQ(e.host, "::1");
  EXPECT_EQ(e.port, 9000);
}
```

注册到 `CMakeLists.txt`(comm 测试区,`tests/comm/protocol_node_test.cpp` 行之后):
```
    tests/comm/protocol_policy_test.cpp
```
Run: `cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON >/dev/null && cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `ProtocolPolicy` 构造不接受第二参数。

- [ ] **Step 2: 改 `include/transport/comm/ProtocolPolicy.hpp`**

(a) 顶部 include 区补:
```cpp
#include <charconv>
```
(b) 构造改为带开关:
```cpp
  explicit ProtocolPolicy(uint8_t protocol_id, bool reply_to_source = false)
      : protocol_id_(protocol_id), reply_to_source_(reply_to_source) {}
```
(c) `ReplyTo` 改为解析 source:
```cpp
  Endpoint ReplyTo(const Message& req) override {
    if (!reply_to_source_) return Endpoint::Default();
    const std::string& s = req.source;
    const auto pos = s.rfind(':');                       // 按最后一个 ':' 切分(兼容 IPv4 与 ::1)
    if (pos == std::string::npos || pos == 0 || pos + 1 >= s.size()) return Endpoint::Default();
    unsigned long port = 0;
    const char* first = s.c_str() + pos + 1;
    const char* last = s.c_str() + s.size();
    auto res = std::from_chars(first, last, port);
    if (res.ec != std::errc() || res.ptr != last || port == 0 || port > 65535)
      return Endpoint::Default();
    return Endpoint::Net(s.substr(0, pos), static_cast<uint16_t>(port));
  }
```
(d) 私有成员区补:
```cpp
  bool reply_to_source_ = false;
```

- [ ] **Step 3: 改 `ProtocolConfig` + 构造透传**

(a) `include/transport/comm/ProtocolNode.hpp` 的 `ProtocolConfig` 加字段:
```cpp
  bool     reply_to_source = false;    // 1:多 UDP 置 true:应答/ack 回到入站来源 ip:port
```
(b) `src/comm/ProtocolNode.cpp` 构造里建 policy 处加第二参数:
```cpp
          std::unique_ptr<InteractionPolicy>(new ProtocolPolicy(config.protocol_id, config.reply_to_source)),
```

- [ ] **Step 4: 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "ProtocolPolicy" --output-on-failure`
Expected: 4 个 `ProtocolPolicy.*` 全过。零告警。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(106 + 4 = 110)。
```bash
git add include/transport/comm/ProtocolPolicy.hpp include/transport/comm/ProtocolNode.hpp \
        src/comm/ProtocolNode.cpp tests/comm/protocol_policy_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ProtocolConfig.reply_to_source — 应答/ack 回送入站来源 ip:port(ProtocolPolicy.ReplyTo 解析 source)"
```

---

### Task 3: 发送方法加目的地 `const Endpoint& to`

**Files:**
- Modify: `include/transport/comm/ProtocolNode.hpp`(5 发送方法签名)
- Modify: `src/comm/ProtocolNode.cpp`(转发 `to`)
- Modify: `tests/comm/protocol_node_test.cpp`(目的地透传用例)

**Interfaces:**
- Consumes:引擎原语 `Fire(out, tag, to)` / `RequestAwait(out, spec, to)` / `StartPeriodic(out, tag, interval, to)`(均已收 `const Endpoint& to`)。
- Produces:`ProtocolNode` 5 个发送方法各加可选尾参 `const Endpoint& to = Endpoint::Default()`。

- [ ] **Step 1: 写失败测试**(加到 `tests/comm/protocol_node_test.cpp`)

顶部 include 区补:
```cpp
#include "transport/Endpoint.hpp"
```
匿名命名空间内(`RecordingTransport` 之后)加一个记录目的地的传输:
```cpp
// 记录最近一次带 Endpoint 的 Send 的目的地(用于验证发送方法透传 to)。
class DestRecorder : public FakeTransport {
 public:
  transport::Endpoint last_to;
  transport::Status Send(const std::vector<uint8_t>& bytes, const transport::Endpoint& to) override {
    last_to = to;
    return FakeTransport::Send(bytes);
  }
};
```
文件末尾(命名空间 `}` 前)加用例:
```cpp
TEST(ProtocolNode, SendForwardsDestinationEndpoint) {
  auto rec = std::make_shared<DestRecorder>();
  auto peer = std::make_shared<FakeTransport>();
  FakeTransport::Link(rec, peer);
  auto node = std::make_shared<TestNode>(rec, nullptr, Cfg(), std::make_unique<InlineExecutor>());
  (void)node->Open();
  (void)node->SendNoResponse(0x10, P({1}), transport::Endpoint::Net("9.9.9.9", 1234));
  EXPECT_EQ(rec->last_to.kind, transport::Endpoint::Kind::kNet);
  EXPECT_EQ(rec->last_to.host, "9.9.9.9");
  EXPECT_EQ(rec->last_to.port, 1234);
  node->Close();
}
```
Run: `cmake --build build -j$(nproc) 2>&1 | head`
Expected: FAIL —— `SendNoResponse` 不接受第三参数。

- [ ] **Step 2: 改 `include/transport/comm/ProtocolNode.hpp`**

(a) 顶部 include 区补(签名用到 `Endpoint`,显式包含):
```cpp
#include "transport/Endpoint.hpp"
```
(b) 5 个发送方法签名各加可选尾参:
```cpp
  Status SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload,
                        const Endpoint& to = Endpoint::Default());
  Status Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response,
                 const Endpoint& to = Endpoint::Default());
  Status RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result,
                           const Endpoint& to = Endpoint::Default());
  Status RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                             ReplyFn on_response, ReplyFn on_result,
                             const Endpoint& to = Endpoint::Default());
  uint32_t StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms,
                          const Endpoint& to = Endpoint::Default());
```

- [ ] **Step 3: 改 `src/comm/ProtocolNode.cpp` —— 转发 `to`**

把 5 个发送方法的定义改为带 `to` 形参并转发给引擎原语(默认实参只写在声明处):
```cpp
Status ProtocolNode::SendNoResponse(uint16_t cmd, std::vector<uint8_t> payload, const Endpoint& to) {
  return engine_->Fire(Cmd(cmd, std::move(payload)), Tag(FrameType::kCommand), to);
}

Status ProtocolNode::Request(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_response,
                             const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResponse);
  s.on_terminal = std::move(on_response); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s), to);
}

Status ProtocolNode::RequestWithResult(uint16_t cmd, std::vector<uint8_t> payload, ReplyFn on_result,
                                       const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand); s.terminal_tag = Tag(FrameType::kResult);
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s), to);
}

Status ProtocolNode::RequestNeedFeedback(uint16_t cmd, std::vector<uint8_t> payload,
                                         ReplyFn on_response, ReplyFn on_result, const Endpoint& to) {
  RequestSpec s; s.request_tag = Tag(FrameType::kCommand);
  s.intermediate_tag = Tag(FrameType::kResponse); s.terminal_tag = Tag(FrameType::kResult);
  s.auto_ack_tag = Tag(FrameType::kResponse);
  s.on_intermediate = [cb = std::move(on_response)](const Message& m) { if (cb) cb(Result<Message>::Success(m)); };
  s.on_terminal = std::move(on_result); s.timeout_ms = config_.response_timeout_ms; s.max_retries = config_.max_retries;
  return engine_->RequestAwait(Cmd(cmd, std::move(payload)), std::move(s), to);
}

uint32_t ProtocolNode::StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms,
                                      const Endpoint& to) {
  return engine_->StartPeriodic(Cmd(cmd, std::move(payload)), Tag(FrameType::kState), interval_ms, to);
}
```

- [ ] **Step 4: 跑测试**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -3 && ctest --test-dir build -R "ProtocolNode.SendForwardsDestinationEndpoint" --output-on-failure`
Expected: PASS。

- [ ] **Step 5: 全量回归 + 提交**

Run: `ctest --test-dir build --output-on-failure 2>&1 | tail -3`
Expected: `100% tests passed`(110 + 1 = 111)。
```bash
git add include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp tests/comm/protocol_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ProtocolNode 发送方法加可选目的地 Endpoint(1:多 UDP 主动多发);默认 Default 不改旧调用"
```

---

## 实现后(计划外,单独一轮)

- 同步 SRS(FR-3 codec 列表加 `SystemDatagramCodec` + FR-7 加 UDP 寻址/reply_to_source)、SDD(§8 codec + §7.5 ProtocolNode)、README(codec 表 + ProtocolNode 用法补 UDP 1:多)、CHANGELOG。
- demo 可补一个 UDP 1:多变体(可选)。
- 终态全分支 review → finishing-a-development-branch。

## Self-Review 记录

- **Spec 覆盖:** §3 SystemDatagramCodec + 共享核 → T1;§4 reply_to_source(含客户端 auto-ack 同路径)→ T2;§5 per-send Endpoint → T3;§6 装配 = 三者组合(无单独任务,文档/ demo 时演示)。
- **占位扫描:** 无。所有步骤含完整代码与确切命令。
- **类型一致:** `EncodeSystemFrame`/`ScanSystemFrames` 签名在 T1 声明=定义=`SystemDatagramCodec` 调用一致;`ProtocolPolicy(uint8_t,bool)` 在 T2 声明=`ProtocolNode` 构造调用一致;5 发送方法 `const Endpoint& to=Default()` 在 T3 hpp 声明=cpp 定义一致(默认实参仅声明处)。
- **回归保障:** T1 共享核重构后 `system_codec_test` 不变即证明;T2/T3 默认参数使现有调用点零改动。
