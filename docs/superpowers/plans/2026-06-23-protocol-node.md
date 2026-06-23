# 外部协议栈(SystemCodec 帧 + ProtocolNode)Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `SystemCodec` 改造成 `AA BB CC DD` 外部协议帧 codec(CRC 注入 + resync),并新建 `ProtocolNode` 实现该协议交互模式(5 模式 + 重发 ≤3 + repeating + 心跳 + 收发双角色),匹配键 (session_id, message_id)。

**Architecture:** `Message` 加性扩展协议字段;`SystemCodec` 改为有状态流式协议帧 codec(CRC 经 `CrcFn` 注入);`ProtocolNode` 独立类复用 `ITransport`+`ICodec`(默认 SystemCodec)+`IExecutor`,io 线程内联 Decode→Post→单 worker 串行分发。`CommNode`/`DdsNode`/`DdsCodec` 不动;`comm_node_test` 改用 `DdsCodec`。

**Tech Stack:** C++17;GoogleTest 1.14(vendored);不抛异常(`Result<T>`/`Status`,`[[nodiscard]]`);标准库线程/条件变量。

**配套 spec:** `docs/superpowers/specs/2026-06-23-protocol-node-design.md`

## Global Constraints

- 不抛异常;一律 `Result<T>`/`Status`,保留 `[[nodiscard]]`。C++17。
- 不引入新第三方依赖;不动 `CommNode`/`DdsNode`/`DdsCodec`/DDS 测试。
- 帧:小端;`head_flag = AA BB CC DD`;头固定 15 字节;`frm_body = [message_id:2 LE][payload]`;`frm_len ≤ 65535`;`reserve = 0`;CRC 校验整个 `frm_body`、经 `CrcFn` 注入。
- 坏帧(同步头/CRC 不符)→ resync(前移 1 字节重扫),解码不报致命错;仅编码 body 越界报 `frame:`。
- `frm_type` 用占位枚举值(0–5),CRC 用占位 `DefaultCrc16` —— 真实外部字节值/算法实现前替换/注入。
- 匹配键 (session_id, message_id);超时重发上限 3,超限失败 `timeout:`。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,**不加 Co-Authored-By**;不提交 `build/`。

---

## 文件结构

**修改:**
- `include/transport/Message.hpp`(加 `FrameType` + 4 协议字段)。
- `include/transport/codec/SystemCodec.hpp` + `src/codec/SystemCodec.cpp`(协议帧 + CRC 注入 + resync)。
- `tests/codec/system_codec_test.cpp`(重写为协议帧测试)。
- `tests/comm/comm_node_test.cpp`(`SystemCodec` → `DdsCodec`)。
- `CMakeLists.txt`(加 `src/comm/ProtocolNode.cpp`、`tests/comm/protocol_node_test.cpp`)。

**新建:**
- `include/transport/comm/ProtocolNode.hpp` + `src/comm/ProtocolNode.cpp`。
- `tests/comm/protocol_node_test.cpp`。

**复用(不动):** `IExecutor`/`ThreadExecutor`、`DdsCodec`、`tests/comm/fake_transport.hpp`、`tests/comm/inline_executor.hpp`。

---

## Task 1: `Message` 协议字段 + `SystemCodec` 协议帧 codec

**Files:** Modify `include/transport/Message.hpp`、`include/transport/codec/SystemCodec.hpp`、`src/codec/SystemCodec.cpp`、`tests/codec/system_codec_test.cpp`、`tests/comm/comm_node_test.cpp`。

**Interfaces:**
- Produces: `transport::FrameType`(枚举);`Message` 增 `frm_type`/`protocol_id`/`session_id`/`message_id`;`using CrcFn = std::function<uint16_t(const uint8_t*, std::size_t)>`;`uint16_t DefaultCrc16(const uint8_t*, std::size_t)`;`SystemCodec(CrcFn crc = DefaultCrc16)`,Encode/Decode 走协议帧。

- [ ] **Step 1: 改 `include/transport/Message.hpp`**

在 `MessageKind` 之后、`struct Message` 之前加 `FrameType`;并在 `Message` 末尾加 4 字段:
```cpp
enum class FrameType : uint8_t {   // 占位值,实现前替换为外部真实字节值
  kUnknown   = 0,
  kCommand   = 1,
  kResponse  = 2,
  kResult    = 3,
  kState     = 4,
  kHeartbeat = 5,
};

struct Message {
  std::vector<uint8_t> payload;
  std::string topic;
  std::string source;
  int64_t timestamp = 0;
  MessageKind kind = MessageKind::kOneway;
  std::string correlation_id;
  std::string reply_to;
  FrameType frm_type    = FrameType::kUnknown;  // 协议:帧类型
  uint8_t   protocol_id = 0;                    // 协议:外部系统 id
  uint8_t   session_id  = 0;                    // 协议:会话 id
  uint16_t  message_id  = 0;                    // 协议:帧唯一 id
};
```

- [ ] **Step 2: 写失败测试 `tests/codec/system_codec_test.cpp`(整体重写)**
```cpp
#include "transport/codec/SystemCodec.hpp"

#include <cstdint>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameType;
using transport::Message;
using transport::SystemCodec;

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
}  // namespace

TEST(SystemCodec, EncodeProducesProtocolFrame) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(0x07, 0x09, 0x0201, {0xAA, 0xBB}));
  ASSERT_TRUE(static_cast<bool>(enc));
  const auto& f = enc.value;
  // 头 15 + body(2 message_id + 2 payload)= 19
  ASSERT_EQ(f.size(), 19u);
  EXPECT_EQ(f[0], 0xAA); EXPECT_EQ(f[1], 0xBB); EXPECT_EQ(f[2], 0xCC); EXPECT_EQ(f[3], 0xDD);
  EXPECT_EQ(f[4], static_cast<uint8_t>(FrameType::kCommand));
  EXPECT_EQ(f[5], 0x07);                          // protocol_id
  EXPECT_EQ(f[6], 0x09);                          // session_id
  EXPECT_EQ(f[7], 0); EXPECT_EQ(f[8], 0); EXPECT_EQ(f[9], 0); EXPECT_EQ(f[10], 0);  // reserve
  // body = [01 02][AA BB];sum = 0x01+0x02+0xAA+0xBB = 0x168
  const uint16_t crc = 0x0168;
  EXPECT_EQ(f[11], static_cast<uint8_t>(crc & 0xFF));        // crc LE
  EXPECT_EQ(f[12], static_cast<uint8_t>((crc >> 8) & 0xFF));
  EXPECT_EQ(f[13], 4); EXPECT_EQ(f[14], 0);                  // frm_len = 4 LE
  EXPECT_EQ(f[15], 0x01); EXPECT_EQ(f[16], 0x02);            // message_id 0x0201 LE
  EXPECT_EQ(f[17], 0xAA); EXPECT_EQ(f[18], 0xBB);            // payload
}

TEST(SystemCodec, EncodeDecodeRoundtrip) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(3, 5, 0x1234, {1, 2, 3, 4}));
  ASSERT_TRUE(static_cast<bool>(enc));
  auto dec = c.Decode(enc.value.data(), enc.value.size());
  ASSERT_TRUE(static_cast<bool>(dec));
  ASSERT_EQ(dec.value.size(), 1u);
  const Message& m = dec.value[0];
  EXPECT_EQ(m.frm_type, FrameType::kCommand);
  EXPECT_EQ(m.protocol_id, 3);
  EXPECT_EQ(m.session_id, 5);
  EXPECT_EQ(m.message_id, 0x1234);
  EXPECT_EQ(m.payload, (std::vector<uint8_t>{1, 2, 3, 4}));
}

TEST(SystemCodec, DecodeSplitAcrossReads) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(1, 2, 7, {9, 9, 9}));
  ASSERT_TRUE(static_cast<bool>(enc));
  const auto& f = enc.value;
  auto d1 = c.Decode(f.data(), 6);                    // 半包
  ASSERT_TRUE(static_cast<bool>(d1)); EXPECT_TRUE(d1.value.empty());
  auto d2 = c.Decode(f.data() + 6, f.size() - 6);     // 补齐
  ASSERT_TRUE(static_cast<bool>(d2)); ASSERT_EQ(d2.value.size(), 1u);
  EXPECT_EQ(d2.value[0].payload, (std::vector<uint8_t>{9, 9, 9}));
}

TEST(SystemCodec, DecodeMultipleFramesOneRead) {
  SystemCodec c(SumCrc);
  auto a = c.Encode(Cmd(1, 1, 1, {0xA})); auto b = c.Encode(Cmd(1, 2, 2, {0xB}));
  std::vector<uint8_t> both = a.value; both.insert(both.end(), b.value.begin(), b.value.end());
  auto dec = c.Decode(both.data(), both.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value.size(), 2u);
  EXPECT_EQ(dec.value[0].message_id, 1); EXPECT_EQ(dec.value[1].message_id, 2);
}

TEST(SystemCodec, ResyncOnBadHeadFlag) {
  SystemCodec c(SumCrc);
  auto enc = c.Encode(Cmd(1, 2, 3, {7, 7}));
  std::vector<uint8_t> junk = {0x00, 0x11, 0xAA, 0xBB, 0x22};  // 含半个假同步头
  junk.insert(junk.end(), enc.value.begin(), enc.value.end());
  auto dec = c.Decode(junk.data(), junk.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value.size(), 1u);
  EXPECT_EQ(dec.value[0].payload, (std::vector<uint8_t>{7, 7}));
}

TEST(SystemCodec, ResyncOnCrcMismatch) {
  SystemCodec c(SumCrc);
  auto bad = c.Encode(Cmd(1, 2, 3, {5, 5})); bad.value[15] ^= 0xFF;  // 破坏 body → CRC 不符
  auto good = c.Encode(Cmd(1, 2, 4, {6, 6}));
  std::vector<uint8_t> s = bad.value; s.insert(s.end(), good.value.begin(), good.value.end());
  auto dec = c.Decode(s.data(), s.size());
  ASSERT_TRUE(static_cast<bool>(dec)); ASSERT_EQ(dec.value.size(), 1u);  // 坏帧跳过,好帧解出
  EXPECT_EQ(dec.value[0].message_id, 4);
}

TEST(SystemCodec, EncodeRejectsOversizePayload) {
  SystemCodec c(SumCrc);
  Message m = Cmd(1, 1, 1, std::vector<uint8_t>(65534, 0));  // 65534 + 2 > 65535
  auto enc = c.Encode(m);
  ASSERT_FALSE(static_cast<bool>(enc));
  EXPECT_EQ(enc.error.rfind("frame:", 0), 0u);
}
```

- [ ] **Step 3: 运行,确认失败** `cd /home/ubuntu/david/transport && cmake -S . -B build >/dev/null 2>&1; cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 编译错误(`SystemCodec(CrcFn)` 构造不存在 / `FrameType` 未定义 等)。

- [ ] **Step 4: 改 `include/transport/codec/SystemCodec.hpp`**
```cpp
#pragma once

// SystemCodec.hpp — 外部协议帧 codec(有状态流式)。
// [head_flag:4=AA BB CC DD][frm_type:1][protocol_id:1][session_id:1][reserve:4=0]
// [crc:2 LE][frm_len:2 LE][frm_body: message_id:2 LE | payload]
// CRC 经 CrcFn 注入,校验整个 frm_body;坏帧 resync(前移 1 字节重扫)。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Result.hpp"

namespace transport {

using CrcFn = std::function<uint16_t(const uint8_t* body, std::size_t len)>;

// 默认占位 CRC16-CCITT(poly 0x1021, init 0xFFFF);真实算法实现前经构造注入替换。
uint16_t DefaultCrc16(const uint8_t* body, std::size_t len);

class SystemCodec : public ICodec {
 public:
  explicit SystemCodec(CrcFn crc = DefaultCrc16);

  Result<std::vector<uint8_t>> Encode(const Message& msg) override;
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override;

 private:
  static constexpr std::size_t kHeaderLen = 15;
  static constexpr std::size_t kMaxBody = 65535;
  CrcFn crc_;
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
```

- [ ] **Step 5: 改 `src/codec/SystemCodec.cpp`(整体重写)**
```cpp
#include "transport/codec/SystemCodec.hpp"

#include <array>
#include <utility>

// SystemCodec.cpp — 见 .hpp。小端;坏帧 resync;CRC 注入。

namespace transport {

namespace {
constexpr std::array<uint8_t, 4> kHeadFlag{0xAA, 0xBB, 0xCC, 0xDD};

void PutU16LE(std::vector<uint8_t>& out, uint16_t v) {
  out.push_back(static_cast<uint8_t>(v & 0xFF));
  out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
}
uint16_t GetU16LE(const uint8_t* p) {
  return static_cast<uint16_t>(p[0] | (static_cast<uint16_t>(p[1]) << 8));
}
bool MatchFlag(const uint8_t* p) {
  return p[0] == kHeadFlag[0] && p[1] == kHeadFlag[1] &&
         p[2] == kHeadFlag[2] && p[3] == kHeadFlag[3];
}
}  // namespace

uint16_t DefaultCrc16(const uint8_t* body, std::size_t len) {
  uint16_t crc = 0xFFFF;
  for (std::size_t i = 0; i < len; ++i) {
    crc ^= static_cast<uint16_t>(static_cast<uint16_t>(body[i]) << 8);
    for (int b = 0; b < 8; ++b)
      crc = (crc & 0x8000) ? static_cast<uint16_t>((crc << 1) ^ 0x1021)
                           : static_cast<uint16_t>(crc << 1);
  }
  return crc;
}

SystemCodec::SystemCodec(CrcFn crc) : crc_(std::move(crc)) {}

Result<std::vector<uint8_t>> SystemCodec::Encode(const Message& msg) {
  using R = Result<std::vector<uint8_t>>;
  if (msg.payload.size() + 2 > kMaxBody) return R::Fail("frame: payload too long");

  std::vector<uint8_t> body;
  body.reserve(2 + msg.payload.size());
  PutU16LE(body, msg.message_id);
  body.insert(body.end(), msg.payload.begin(), msg.payload.end());
  const uint16_t crc = crc_(body.data(), body.size());

  std::vector<uint8_t> out;
  out.reserve(kHeaderLen + body.size());
  out.insert(out.end(), kHeadFlag.begin(), kHeadFlag.end());          // head_flag
  out.push_back(static_cast<uint8_t>(msg.frm_type));                  // frm_type
  out.push_back(msg.protocol_id);                                    // protocol_id
  out.push_back(msg.session_id);                                     // session_id
  out.insert(out.end(), {0, 0, 0, 0});                               // reserve
  PutU16LE(out, crc);                                                // crc
  PutU16LE(out, static_cast<uint16_t>(body.size()));                 // frm_len
  out.insert(out.end(), body.begin(), body.end());                   // frm_body
  return R::Success(std::move(out));
}

Result<std::vector<Message>> SystemCodec::Decode(const uint8_t* data, std::size_t len) {
  using R = Result<std::vector<Message>>;
  buffer_.insert(buffer_.end(), data, data + len);
  std::vector<Message> out;
  std::size_t off = 0;

  while (true) {
    // 1. 同步:从 off 起找 head_flag。
    std::size_t flag = off;
    bool found = false;
    while (flag + 4 <= buffer_.size()) {
      if (MatchFlag(buffer_.data() + flag)) { found = true; break; }
      ++flag;
    }
    if (!found) { off = flag; break; }   // 保留末尾 ≤3 字节(可能半个同步头)
    off = flag;

    if (buffer_.size() - off < kHeaderLen) break;                 // 头不全,等
    const uint8_t* h = buffer_.data() + off;
    const uint16_t crc_in = GetU16LE(h + 11);
    const uint16_t frm_len = GetU16LE(h + 13);
    if (buffer_.size() - off - kHeaderLen < frm_len) break;       // body 不全,等

    const uint8_t* bd = h + kHeaderLen;
    if (crc_(bd, frm_len) != crc_in) { off += 1; continue; }      // CRC 不符 → resync

    Message m;
    m.frm_type = static_cast<FrameType>(h[4]);
    switch (m.frm_type) {                                         // 未知字节 → kUnknown
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

  if (off > 0) buffer_.erase(buffer_.begin(), buffer_.begin() + off);
  return R::Success(std::move(out));
}

}  // namespace transport
```

- [ ] **Step 6: 改 `tests/comm/comm_node_test.cpp` —— SystemCodec → DdsCodec**

把第 3 行 include、第 20 行 using、以及全部 `std::make_unique<SystemCodec>()` 改成 DdsCodec:
```bash
cd /home/ubuntu/david/transport
sed -i 's#transport/codec/SystemCodec.hpp#transport/codec/DdsCodec.hpp#' tests/comm/comm_node_test.cpp
sed -i 's#using transport::SystemCodec;#using transport::DdsCodec;#' tests/comm/comm_node_test.cpp
sed -i 's#std::make_unique<SystemCodec>()#std::make_unique<DdsCodec>()#g' tests/comm/comm_node_test.cpp
```
(DdsCodec 带 kind/corr/reply_to、无状态、每次 Decode 整段一条;FakeTransport 一次 Send 一次完整交付 → CommNode req-resp 不变。)

- [ ] **Step 7: 运行,确认通过** `cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:"; ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"`。Expected: 无 warning/error;`74 tests passed`(原 74:DdsNode 轮次后总数;system_codec_test 由旧 N 条变 7 条、comm_node 仍 9 条 —— 实际以两次连跑一致为准,关键是 0 failed)。

> 注:若总数因 system_codec_test 用例数变化而非 74,以"0 failed + 两次连跑一致"为通过标准。

- [ ] **Step 8: 提交**
```bash
git add include/transport/Message.hpp include/transport/codec/SystemCodec.hpp src/codec/SystemCodec.cpp tests/codec/system_codec_test.cpp tests/comm/comm_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: SystemCodec 改为 AA BB CC DD 协议帧(CRC 注入+resync)+ Message 协议字段;comm_node_test 换 DdsCodec"
```

---

## Task 2: `ProtocolNode` 核心(noresponse + needresponse + 重发 + 接收角色)

**Files:** Create `include/transport/comm/ProtocolNode.hpp`、`src/comm/ProtocolNode.cpp`、`tests/comm/protocol_node_test.cpp`;Modify `CMakeLists.txt`。

**Interfaces:**
- Consumes: `SystemCodec`、`FrameType`、`Message` 协议字段(Task 1);`IExecutor`/`ThreadExecutor`、`ITransport`。
- Produces: `transport::ProtocolConfig`;`transport::Responder`(`Response`/`Result`);`transport::ProtocolNode`,ctor `(shared_ptr<ITransport>, unique_ptr<ICodec>=nullptr→SystemCodec, ProtocolConfig, unique_ptr<IExecutor>=nullptr→ThreadExecutor, size_t=1024)`;`Open/Close/IsOpen`、`SendNoResponse`、`Request(payload,on_response)`、protected `OnCommand`/`OnHeartbeat`/`OnError`。Task 3/4 续加 `RequestWithResult`/`RequestNeedFeedback`/`StartRepeating`/`StopRepeating`。

- [ ] **Step 1: 写失败测试 `tests/comm/protocol_node_test.cpp`**
```cpp
#include "transport/comm/ProtocolNode.hpp"

#include "fake_transport.hpp"
#include "inline_executor.hpp"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

using transport::FrameType;
using transport::ICodec;
using transport::IExecutor;
using transport::Message;
using transport::ProtocolConfig;
using transport::ProtocolNode;
using transport::Responder;
using transport::Result;
using testutil::FakeTransport;
using testutil::InlineExecutor;

namespace {
std::vector<uint8_t> P(std::initializer_list<uint8_t> l) { return std::vector<uint8_t>(l); }

// 测试子类:记录收到的 COMMAND,可设回应行为。
class TestNode : public ProtocolNode {
 public:
  using ProtocolNode::ProtocolNode;
  int commands = 0;
  int heartbeats = 0;
  std::function<void(const Message&, Responder)> on_cmd;
  void OnCommand(const Message& m, Responder r) override {
    ++commands; if (on_cmd) on_cmd(m, std::move(r));
  }
  void OnHeartbeat(const Message&) override { ++heartbeats; }
};

ProtocolConfig Cfg(uint8_t proto = 1, uint32_t timeout = 1000, uint32_t retries = 3) {
  ProtocolConfig c; c.protocol_id = proto; c.response_timeout_ms = timeout;
  c.max_retries = retries; c.heartbeat_interval_ms = 0; return c;
}

// 一对经 FakeTransport 互联 + InlineExecutor(确定性)的 TestNode。保留 exec 裸指针驱动定时器。
struct Pair {
  std::shared_ptr<FakeTransport> ta = std::make_shared<FakeTransport>();
  std::shared_ptr<FakeTransport> tb = std::make_shared<FakeTransport>();
  std::shared_ptr<TestNode> a, b;
  InlineExecutor* exa = nullptr; InlineExecutor* exb = nullptr;
  Pair(ProtocolConfig ca = Cfg(), ProtocolConfig cb = Cfg()) {
    FakeTransport::Link(ta, tb);
    auto ea = std::make_unique<InlineExecutor>(); exa = ea.get();
    auto eb = std::make_unique<InlineExecutor>(); exb = eb.get();
    a = std::make_shared<TestNode>(ta, nullptr, ca, std::move(ea));
    b = std::make_shared<TestNode>(tb, nullptr, cb, std::move(eb));
  }
  void Open() { (void)b->Open(); (void)a->Open(); }
  void Close() { a->Close(); b->Close(); }
};
}  // namespace

TEST(ProtocolNode, NoResponseSends) {
  Pair p; p.Open();
  ASSERT_TRUE(static_cast<bool>(p.a->SendNoResponse(P({1, 2, 3}))));
  EXPECT_EQ(p.b->commands, 1);  // 对端收到 COMMAND(InlineExecutor 同步)
  p.Close();
}

TEST(ProtocolNode, NeedResponseCompletesOnResponse) {
  Pair p; p.Open();
  p.b->on_cmd = [](const Message& m, Responder r) {
    auto out = m.payload; out.push_back(0xEE); (void)r.Response(out);
  };
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      p.a->Request(P({5}), [&](Result<Message> rr) { got = std::move(rr); })));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{5, 0xEE}));
  EXPECT_EQ(got.value.frm_type, FrameType::kResponse);
  p.Close();
}

TEST(ProtocolNode, ReceiverRoleHandlesIncomingCommand) {
  Pair p; p.Open();
  Message seen;
  p.b->on_cmd = [&](const Message& m, Responder r) { seen = m; (void)r.Response(P({9})); };
  Result<Message> got = Result<Message>::Fail("none");
  (void)p.a->Request(P({0x42}), [&](Result<Message> rr) { got = std::move(rr); });
  EXPECT_EQ(seen.frm_type, FrameType::kCommand);
  EXPECT_EQ(seen.protocol_id, 1);
  EXPECT_EQ(seen.payload, (std::vector<uint8_t>{0x42}));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{9}));
  p.Close();
}

TEST(ProtocolNode, TimeoutRetransmitsUpToThreeThenFails) {
  Pair p(Cfg(1, 50, 3), Cfg(2, 50, 3));
  p.Open();
  // 对端只计数不回应。
  Result<Message> got = Result<Message>::Success(Message{});
  (void)p.a->Request(P({1}), [&](Result<Message> rr) { got = std::move(rr); });
  EXPECT_EQ(p.b->commands, 1);                 // 初次发送
  p.exa->FireAll();                            // 超时 → 重发1
  EXPECT_EQ(p.b->commands, 2);
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 3);  // 重发2
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 4);  // 重发3(达上限)
  p.exa->FireAll();                            // 再超时 → 失败
  ASSERT_FALSE(static_cast<bool>(got));
  EXPECT_EQ(got.error.rfind("timeout:", 0), 0u);
  EXPECT_EQ(p.b->commands, 4);                 // 不再重发
  p.Close();
}

TEST(ProtocolNode, WorksWithThreadExecutor) {
  auto ta = std::make_shared<FakeTransport>(), tb = std::make_shared<FakeTransport>();
  FakeTransport::Link(ta, tb);
  auto a = std::make_shared<TestNode>(ta, nullptr, Cfg(), nullptr);  // 默认 SystemCodec+ThreadExecutor
  auto b = std::make_shared<TestNode>(tb, nullptr, Cfg(), nullptr);
  b->on_cmd = [](const Message& m, Responder r) {
    auto out = m.payload; out.push_back(0xEE); (void)r.Response(out);
  };
  (void)b->Open(); (void)a->Open();
  std::promise<Result<Message>> prom; auto fut = prom.get_future();
  ASSERT_TRUE(static_cast<bool>(
      a->Request(P({3}), [&](Result<Message> r) { prom.set_value(std::move(r)); })));
  auto r = fut.get();
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{3, 0xEE}));
  a->Close(); b->Close();
}
```
需要 `#include <functional>`/`#include <future>`。把 `tests/comm/protocol_node_test.cpp` 加入 `CMakeLists.txt` 的 `add_executable(transport_tests ...)`(在 `tests/comm/dds_node_test.cpp` 后)。

- [ ] **Step 2: 运行,确认失败** `cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `transport/comm/ProtocolNode.hpp`。

- [ ] **Step 3: 写 `include/transport/comm/ProtocolNode.hpp`**
```cpp
#pragma once

// ProtocolNode.hpp — 外部协议交互节点。复用 ITransport + ICodec(默认 SystemCodec)+ IExecutor。
// io 线程 Decode → executor.Post → 单 worker 串行 Dispatch(按 frm_type)。匹配键 (session_id, message_id)。
// 须以 shared_ptr 持有。Task 2:noresponse/needresponse/重发/接收角色;Task 3/4 续加其余模式。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"

namespace transport {

struct ProtocolConfig {
  uint8_t  protocol_id = 0;             // 出站帧的外部系统 id
  uint32_t response_timeout_ms = 1000;  // 等待回应/结果超时
  uint32_t max_retries = 3;             // 超时重发上限(超过即失败)
  uint32_t heartbeat_interval_ms = 0;   // 0 = 关闭心跳
};

class ProtocolNode;

class Responder {  // 接收角色应答句柄:回填该请求 (session_id, message_id)
 public:
  Status Response(std::vector<uint8_t> payload);  // 发 RESPONSE
  Status Result(std::vector<uint8_t> payload);     // 发 RESULT

 private:
  friend class ProtocolNode;
  Responder(std::weak_ptr<ProtocolNode> node, uint8_t session, uint16_t message)
      : node_(std::move(node)), session_(session), message_(message) {}
  std::weak_ptr<ProtocolNode> node_;
  uint8_t session_;
  uint16_t message_;
};

class ProtocolNode : public std::enable_shared_from_this<ProtocolNode> {
 public:
  using ReplyFn = std::function<void(Result<Message>)>;

  ProtocolNode(std::shared_ptr<ITransport> transport,
               std::unique_ptr<ICodec> codec,            // null → SystemCodec(DefaultCrc16)
               ProtocolConfig config,
               std::unique_ptr<IExecutor> executor = nullptr,  // null → ThreadExecutor
               std::size_t queue_capacity = 1024);
  virtual ~ProtocolNode();

  Status Open();
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  Status SendNoResponse(std::vector<uint8_t> payload);          // noresponse
  Status Request(std::vector<uint8_t> payload, ReplyFn on_response);  // needresponse

 protected:
  virtual void OnCommand(const Message& cmd, Responder responder) {}
  virtual void OnHeartbeat(const Message& hb) {}
  virtual void OnError(const std::string& error) {}

 private:
  friend class Responder;
  enum class Mode { kNeedResponse };   // Task 3 续加 kWithResult / kNeedFeedback
  struct Pending {
    Mode mode;
    std::vector<uint8_t> payload;      // 重发用
    uint8_t session; uint16_t message;
    ReplyFn on_response;
    ReplyFn on_result;
    uint32_t retries = 0;
    IExecutor::TimerId timer = 0;
    bool got_response = false;
  };

  Status SendFrame(FrameType type, uint8_t session, uint16_t message,
                   const std::vector<uint8_t>& payload);
  std::pair<uint8_t, uint16_t> NextId();
  static uint32_t Key(uint8_t s, uint16_t m) {
    return (static_cast<uint32_t>(s) << 16) | m;
  }
  void Dispatch(Message msg);
  void OnTimeout(uint32_t key);
  void HandleDisconnect(const std::string& reason);

  std::shared_ptr<ITransport> transport_;
  std::unique_ptr<ICodec> codec_;
  std::unique_ptr<IExecutor> executor_;
  ProtocolConfig config_;
  std::atomic<bool> open_{false};
  std::atomic<bool> closing_{false};
  std::mutex mu_;
  std::map<uint32_t, Pending> pending_;
  uint8_t session_ctr_ = 0;
  uint16_t message_ctr_ = 0;
};

}  // namespace transport
```

- [ ] **Step 4: 写 `src/comm/ProtocolNode.cpp`**
```cpp
#include "transport/comm/ProtocolNode.hpp"

#include <chrono>
#include <utility>
#include <variant>

#include "transport/codec/SystemCodec.hpp"
#include "transport/comm/ThreadExecutor.hpp"

// ProtocolNode.cpp — 见 .hpp。posted 任务/transport 回调捕获 weak_ptr;
// pending_ 由 mu_ 保护;Encode+Send 在锁外;超时与 Dispatch 同在 worker → 恰好一次。

namespace transport {

namespace {
Status Ok() { return Status::Success(std::monostate{}); }
}  // namespace

Status Responder::Response(std::vector<uint8_t> payload) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendFrame(FrameType::kResponse, session_, message_, payload);
}
Status Responder::Result(std::vector<uint8_t> payload) {
  auto s = node_.lock();
  if (!s) return Status::Fail("conn: node gone");
  return s->SendFrame(FrameType::kResult, session_, message_, payload);
}

ProtocolNode::ProtocolNode(std::shared_ptr<ITransport> transport, std::unique_ptr<ICodec> codec,
                           ProtocolConfig config, std::unique_ptr<IExecutor> executor,
                           std::size_t queue_capacity)
    : transport_(std::move(transport)),
      codec_(codec ? std::move(codec) : std::unique_ptr<ICodec>(new SystemCodec())),
      executor_(executor ? std::move(executor)
                         : std::unique_ptr<IExecutor>(new ThreadExecutor(queue_capacity))),
      config_(config) {}

ProtocolNode::~ProtocolNode() { Close(); }

std::pair<uint8_t, uint16_t> ProtocolNode::NextId() {
  std::lock_guard<std::mutex> lk(mu_);
  uint8_t s = session_ctr_++;
  uint16_t m = message_ctr_++;
  return {s, m};
}

Status ProtocolNode::Open() {
  executor_->Start();
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  transport_->OnBytes([wself](Result<std::vector<uint8_t>> r, const std::string&) {
    auto s = wself.lock();
    if (!s) return;
    if (!r) {
      std::string e = r.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) s2->OnError(e); });
      return;
    }
    auto msgs = s->codec_->Decode(r.value.data(), r.value.size());
    if (!msgs) {
      std::string e = msgs.error;
      s->executor_->Post([wself, e] { if (auto s2 = wself.lock()) s2->OnError(e); });
      return;
    }
    for (auto& m : msgs.value)
      s->executor_->Post([wself, msg = std::move(m)]() mutable {
        if (auto s2 = wself.lock()) s2->Dispatch(std::move(msg));
      });
  });
  transport_->OnConnect([] {});
  transport_->OnDisconnect([wself](const std::string& reason) {
    if (auto s = wself.lock())
      s->executor_->Post([wself, reason] { if (auto s2 = wself.lock()) s2->HandleDisconnect(reason); });
  });
  open_.store(true);
  auto st = transport_->Open();
  if (!st) { open_.store(false); executor_->Stop(); return st; }
  return Ok();
}

void ProtocolNode::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  std::map<uint32_t, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    auto& p = kv.second;
    if (p.mode == Mode::kNeedResponse && p.on_response)
      p.on_response(Result<Message>::Fail("conn: node closed"));
  }
  executor_->Stop();
  transport_->Close();
}

Status ProtocolNode::SendFrame(FrameType type, uint8_t session, uint16_t message,
                               const std::vector<uint8_t>& payload) {
  if (!open_.load()) return Status::Fail("config: node not open");
  Message m;
  m.frm_type = type;
  m.protocol_id = config_.protocol_id;
  m.session_id = session;
  m.message_id = message;
  m.payload = payload;
  auto bytes = codec_->Encode(m);
  if (!bytes) return Status::Fail(bytes.error);
  return transport_->Send(bytes.value);
}

Status ProtocolNode::SendNoResponse(std::vector<uint8_t> payload) {
  auto id = NextId();
  return SendFrame(FrameType::kCommand, id.first, id.second, payload);
}

Status ProtocolNode::Request(std::vector<uint8_t> payload, ReplyFn on_response) {
  if (!open_.load()) {
    if (on_response) on_response(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  auto id = NextId();
  const uint32_t key = Key(id.first, id.second);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_.load() || !open_.load()) {
      if (on_response) on_response(Result<Message>::Fail("conn: node closing"));
      return Status::Fail("conn: node closing");
    }
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.response_timeout_ms),
        [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
    Pending p;
    p.mode = Mode::kNeedResponse; p.payload = payload;
    p.session = id.first; p.message = id.second;
    p.on_response = std::move(on_response); p.timer = timer;
    pending_[key] = std::move(p);
  }
  Status st = SendFrame(FrameType::kCommand, id.first, id.second, payload);
  if (!st) {
    ReplyFn cb;
    { std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(key);
      if (it != pending_.end()) { executor_->Cancel(it->second.timer); cb = std::move(it->second.on_response); pending_.erase(it); } }
    if (cb) cb(Result<Message>::Fail(st.error));
    return st;
  }
  return Ok();
}

void ProtocolNode::OnTimeout(uint32_t key) {
  ReplyFn fail_cb;
  bool resend = false; FrameType rt = FrameType::kCommand;
  uint8_t s = 0; uint16_t m = 0; std::vector<uint8_t> payload;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(key);
    if (it == pending_.end()) return;
    Pending& p = it->second;
    if (p.retries < config_.max_retries) {
      ++p.retries;
      std::weak_ptr<ProtocolNode> wself = weak_from_this();
      p.timer = executor_->ScheduleAt(
          std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.response_timeout_ms),
          [wself, key] { if (auto s2 = wself.lock()) s2->OnTimeout(key); });
      resend = true; s = p.session; m = p.message; payload = p.payload;
    } else {
      fail_cb = std::move(p.on_response);
      pending_.erase(it);
    }
  }
  if (resend) (void)SendFrame(rt, s, m, payload);
  if (fail_cb) fail_cb(Result<Message>::Fail("timeout: request timed out"));
}

void ProtocolNode::Dispatch(Message msg) {
  switch (msg.frm_type) {
    case FrameType::kCommand:
    case FrameType::kState:
      OnCommand(msg, Responder(weak_from_this(), msg.session_id, msg.message_id));
      break;
    case FrameType::kResponse: {
      const uint32_t key = Key(msg.session_id, msg.message_id);
      ReplyFn cb;
      { std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(key);
        if (it != pending_.end() && it->second.mode == Mode::kNeedResponse) {
          cb = std::move(it->second.on_response);
          executor_->Cancel(it->second.timer);
          pending_.erase(it);
        } }
      if (cb) cb(Result<Message>::Success(std::move(msg)));
      break;
    }
    case FrameType::kResult:
      break;  // Task 3 处理
    case FrameType::kHeartbeat:
      OnHeartbeat(msg);
      break;
    case FrameType::kUnknown:
      OnError("codec: unknown frame type");
      break;
  }
}

void ProtocolNode::HandleDisconnect(const std::string& reason) {
  std::map<uint32_t, Pending> taken;
  { std::lock_guard<std::mutex> lk(mu_); taken.swap(pending_); }
  for (auto& kv : taken) {
    executor_->Cancel(kv.second.timer);
    auto& p = kv.second;
    if (p.mode == Mode::kNeedResponse && p.on_response)
      p.on_response(Result<Message>::Fail(reason));
  }
}

}  // namespace transport
```
把 `src/comm/ProtocolNode.cpp` 加入 `CMakeLists.txt` 的 `add_library(transport STATIC ...)`(在 `src/comm/DdsNode.cpp` 后)。

- [ ] **Step 5: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R ProtocolNode 2>&1 | grep -iE "passed|failed"`。Expected: 5 个 `ProtocolNode.*` 用例通过。

- [ ] **Step 6: 提交**
```bash
git add include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp tests/comm/protocol_node_test.cpp CMakeLists.txt
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ProtocolNode 核心(noresponse/needresponse/超时重发≤3/接收角色;(session,message) 匹配)"
```

---

## Task 3: `withfeedback` + `needfeedback`

**Files:** Modify `include/transport/comm/ProtocolNode.hpp`、`src/comm/ProtocolNode.cpp`、`tests/comm/protocol_node_test.cpp`。

**Interfaces:**
- Consumes: Task 2 的 `ProtocolNode`(`Mode`、`Pending`、`Dispatch`、`OnTimeout`)。
- Produces: `RequestWithResult(payload, on_result)`(withfeedback,等 RESULT);`RequestNeedFeedback(payload, on_response, on_result)`(needfeedback,RESPONSE→RESULT→自动回 RESPONSE)。

- [ ] **Step 1: 加失败测试到 `tests/comm/protocol_node_test.cpp`**(追加两个 TEST)
```cpp
TEST(ProtocolNode, WithFeedbackCompletesOnResult) {
  Pair p; p.Open();
  p.b->on_cmd = [](const Message& m, Responder r) {
    auto out = m.payload; out.push_back(0xCC); (void)r.Result(out);  // 直接发 RESULT
  };
  Result<Message> got = Result<Message>::Fail("none");
  ASSERT_TRUE(static_cast<bool>(
      p.a->RequestWithResult(P({6}), [&](Result<Message> rr) { got = std::move(rr); })));
  ASSERT_TRUE(static_cast<bool>(got));
  EXPECT_EQ(got.value.frm_type, FrameType::kResult);
  EXPECT_EQ(got.value.payload, (std::vector<uint8_t>{6, 0xCC}));
  p.Close();
}

TEST(ProtocolNode, NeedFeedbackResponseThenResultThenAck) {
  Pair p; p.Open();
  // 对端:收 COMMAND → 回 RESPONSE,再回 RESULT(发起方收 RESULT 后会自动回 RESPONSE ack)。
  p.b->on_cmd = [](const Message& m, Responder r) {
    (void)r.Response(P({0x01}));
    (void)r.Result(m.payload);
  };
  std::vector<uint8_t> resp, res; int order = 0; int resp_at = 0, res_at = 0;
  ASSERT_TRUE(static_cast<bool>(p.a->RequestNeedFeedback(
      P({7}),
      [&](Result<Message> rr) { if (rr) { resp = rr.value.payload; resp_at = ++order; } },
      [&](Result<Message> rr) { if (rr) { res = rr.value.payload; res_at = ++order; } })));
  EXPECT_EQ(resp, (std::vector<uint8_t>{0x01}));   // 中间 RESPONSE
  EXPECT_EQ(res, (std::vector<uint8_t>{7}));         // 最终 RESULT
  EXPECT_LT(resp_at, res_at);                        // 先 RESPONSE 后 RESULT
  p.Close();
}
```

- [ ] **Step 2: 运行,确认失败** `cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `RequestWithResult` / `RequestNeedFeedback`。

- [ ] **Step 3: 改 `ProtocolNode.hpp`** —— `Mode` 加两值,声明两个新方法。

把 `enum class Mode { kNeedResponse };` 改为:
```cpp
  enum class Mode { kNeedResponse, kWithResult, kNeedFeedback };
```
在 `Status Request(...)` 声明之后加:
```cpp
  Status RequestWithResult(std::vector<uint8_t> payload, ReplyFn on_result);          // withfeedback
  Status RequestNeedFeedback(std::vector<uint8_t> payload,
                             ReplyFn on_response, ReplyFn on_result);                  // needfeedback
```
新增私有辅助声明(在 `Status Request(...)` 私有区附近,`OnTimeout` 声明后):
```cpp
  Status RequestImpl(Mode mode, std::vector<uint8_t> payload,
                     ReplyFn on_response, ReplyFn on_result);
```

- [ ] **Step 4: 改 `src/comm/ProtocolNode.cpp`**

(4a) 把 `Request` 改为转调 `RequestImpl`,并实现 `RequestImpl` + 两个新入口。用以下整体替换原 `Request` 函数:
```cpp
Status ProtocolNode::Request(std::vector<uint8_t> payload, ReplyFn on_response) {
  return RequestImpl(Mode::kNeedResponse, std::move(payload), std::move(on_response), nullptr);
}
Status ProtocolNode::RequestWithResult(std::vector<uint8_t> payload, ReplyFn on_result) {
  return RequestImpl(Mode::kWithResult, std::move(payload), nullptr, std::move(on_result));
}
Status ProtocolNode::RequestNeedFeedback(std::vector<uint8_t> payload,
                                         ReplyFn on_response, ReplyFn on_result) {
  return RequestImpl(Mode::kNeedFeedback, std::move(payload),
                     std::move(on_response), std::move(on_result));
}

Status ProtocolNode::RequestImpl(Mode mode, std::vector<uint8_t> payload,
                                 ReplyFn on_response, ReplyFn on_result) {
  if (!open_.load()) {
    if (on_response) on_response(Result<Message>::Fail("config: node not open"));
    if (on_result) on_result(Result<Message>::Fail("config: node not open"));
    return Status::Fail("config: node not open");
  }
  auto id = NextId();
  const uint32_t key = Key(id.first, id.second);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (closing_.load() || !open_.load()) {
      if (on_response) on_response(Result<Message>::Fail("conn: node closing"));
      if (on_result) on_result(Result<Message>::Fail("conn: node closing"));
      return Status::Fail("conn: node closing");
    }
    IExecutor::TimerId timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.response_timeout_ms),
        [wself, key] { if (auto s = wself.lock()) s->OnTimeout(key); });
    Pending p;
    p.mode = mode; p.payload = payload;
    p.session = id.first; p.message = id.second;
    p.on_response = std::move(on_response); p.on_result = std::move(on_result);
    p.timer = timer;
    pending_[key] = std::move(p);
  }
  Status st = SendFrame(FrameType::kCommand, id.first, id.second, payload);
  if (!st) {
    ReplyFn cr, cf;
    { std::lock_guard<std::mutex> lk(mu_);
      auto it = pending_.find(key);
      if (it != pending_.end()) {
        executor_->Cancel(it->second.timer);
        cr = std::move(it->second.on_response); cf = std::move(it->second.on_result);
        pending_.erase(it);
      } }
    if (cr) cr(Result<Message>::Fail(st.error));
    if (cf) cf(Result<Message>::Fail(st.error));
    return st;
  }
  return Ok();
}
```
(删除 Task 2 里旧的独立 `Request` 实现 —— 上面已含新版 `Request`。)

(4b) `Dispatch` 的 `kResponse` 与 `kResult` 分支改为支持三模式。把 Task 2 的 `kResponse`/`kResult` 两个 case 整体替换为:
```cpp
    case FrameType::kResponse: {
      const uint32_t key = Key(msg.session_id, msg.message_id);
      ReplyFn cb; bool intermediate = false;
      { std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
          Pending& p = it->second;
          if (p.mode == Mode::kNeedResponse) {
            cb = std::move(p.on_response); executor_->Cancel(p.timer); pending_.erase(it);
          } else if (p.mode == Mode::kNeedFeedback && !p.got_response) {
            p.got_response = true; cb = p.on_response; intermediate = true;  // 中间回应,保留挂起
          }
        } }
      if (cb) cb(Result<Message>::Success(std::move(msg)));
      (void)intermediate;
      break;
    }
    case FrameType::kResult: {
      const uint32_t key = Key(msg.session_id, msg.message_id);
      ReplyFn cb; bool ack = false; uint8_t s = 0; uint16_t m = 0;
      { std::lock_guard<std::mutex> lk(mu_);
        auto it = pending_.find(key);
        if (it != pending_.end()) {
          Pending& p = it->second;
          if (p.mode == Mode::kWithResult || p.mode == Mode::kNeedFeedback) {
            cb = std::move(p.on_result); executor_->Cancel(p.timer);
            ack = (p.mode == Mode::kNeedFeedback); s = p.session; m = p.message;
            pending_.erase(it);
          }
        } }
      if (ack) (void)SendFrame(FrameType::kResponse, s, m, {});  // needfeedback 自动回 RESPONSE
      if (cb) cb(Result<Message>::Success(std::move(msg)));
      break;
    }
```

(4c) `OnTimeout`:needfeedback 收到 RESPONSE 后再超时 → 失败不重发。把 `OnTimeout` 里 `if (p.retries < config_.max_retries)` 那段的判断改为:
```cpp
    const bool no_retransmit = (p.mode == Mode::kNeedFeedback && p.got_response);
    if (!no_retransmit && p.retries < config_.max_retries) {
      ++p.retries;
      std::weak_ptr<ProtocolNode> wself = weak_from_this();
      p.timer = executor_->ScheduleAt(
          std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.response_timeout_ms),
          [wself, key] { if (auto s2 = wself.lock()) s2->OnTimeout(key); });
      resend = true; s = p.session; m = p.message; payload = p.payload;
    } else {
      fail_cb = p.on_result ? std::move(p.on_result) : std::move(p.on_response);
      pending_.erase(it);
    }
```
(失败时优先用 `on_result`(withfeedback/needfeedback),否则 `on_response`(needresponse)。)

(4d) `Close`/`HandleDisconnect` 的终结同时触发 `on_result`。把两处 `if (p.mode == Mode::kNeedResponse && p.on_response) p.on_response(...)` 改为:
```cpp
    if (p.on_response) p.on_response(Result<Message>::Fail("conn: node closed"));
    if (p.on_result) p.on_result(Result<Message>::Fail("conn: node closed"));
```
(`HandleDisconnect` 里相应用其 `reason`。)

- [ ] **Step 5: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R ProtocolNode 2>&1 | grep -iE "passed|failed"`。Expected: 7 个 `ProtocolNode.*` 通过。

- [ ] **Step 6: 提交**
```bash
git add include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp tests/comm/protocol_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ProtocolNode withfeedback(等 RESULT)+ needfeedback(RESPONSE→RESULT→自动 ack)"
```

---

## Task 4: `repeating` + 心跳

**Files:** Modify `include/transport/comm/ProtocolNode.hpp`、`src/comm/ProtocolNode.cpp`、`tests/comm/protocol_node_test.cpp`。

**Interfaces:**
- Consumes: Task 2/3 的 `ProtocolNode`。
- Produces: `uint32_t StartRepeating(std::vector<uint8_t> payload, uint32_t interval_ms)`、`void StopRepeating(uint32_t handle)`;心跳由 `config_.heartbeat_interval_ms` 驱动(`Open` 起、`Close` 停)。

- [ ] **Step 1: 加失败测试到 `tests/comm/protocol_node_test.cpp`**(追加两个 TEST)
```cpp
TEST(ProtocolNode, RepeatingSendsPeriodicallyUntilStopped) {
  Pair p; p.Open();
  uint32_t h = p.a->StartRepeating(P({0xAB}), 100);
  EXPECT_EQ(p.b->commands, 1);   // 起始即发一帧 STATE
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 2);  // 到点再发
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 3);
  p.a->StopRepeating(h);
  p.exa->FireAll(); EXPECT_EQ(p.b->commands, 3);  // 停后不再发
  p.Close();
}

TEST(ProtocolNode, HeartbeatPeriodic) {
  ProtocolConfig ca = Cfg(); ca.heartbeat_interval_ms = 100;  // a 周期发心跳
  Pair p(ca, Cfg());
  p.Open();
  EXPECT_EQ(p.b->heartbeats, 0);   // 尚未到点
  p.exa->FireAll();                // a 的心跳定时器触发 → 发 HEARTBEAT → b 收
  EXPECT_GE(p.b->heartbeats, 1);   // b 的 OnHeartbeat 被调
  p.Close();
}
```

- [ ] **Step 2: 运行,确认失败** `cmake --build build -j$(nproc) 2>&1 | head -20`。Expected: 找不到 `StartRepeating`/`StopRepeating`。

- [ ] **Step 3: 改 `ProtocolNode.hpp`** —— 声明 repeating/心跳 + 成员。

在 public 区 `RequestNeedFeedback(...)` 后加:
```cpp
  uint32_t StartRepeating(std::vector<uint8_t> payload, uint32_t interval_ms);
  void     StopRepeating(uint32_t handle);
```
在私有方法区加:
```cpp
  void ScheduleHeartbeat();
  void FireRepeat(uint32_t handle);
```
在私有成员区加:
```cpp
  struct Repeat { std::vector<uint8_t> payload; uint32_t interval_ms; IExecutor::TimerId timer = 0; };
  std::map<uint32_t, Repeat> repeats_;
  uint32_t repeat_next_ = 1;
  IExecutor::TimerId heartbeat_timer_ = 0;
```

- [ ] **Step 4: 改 `src/comm/ProtocolNode.cpp`**

(4a) `Open()` 成功返回前(`return Ok();` 之前)加启动心跳:
```cpp
  if (config_.heartbeat_interval_ms > 0) ScheduleHeartbeat();
  return Ok();
```

(4b) 实现新函数(放在文件内 `HandleDisconnect` 之后):
```cpp
void ProtocolNode::ScheduleHeartbeat() {
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  heartbeat_timer_ = executor_->ScheduleAt(
      std::chrono::steady_clock::now() + std::chrono::milliseconds(config_.heartbeat_interval_ms),
      [wself] {
        auto s = wself.lock();
        if (!s || !s->open_.load()) return;
        auto id = s->NextId();
        (void)s->SendFrame(FrameType::kHeartbeat, id.first, id.second, {});
        s->ScheduleHeartbeat();  // 重排
      });
}

uint32_t ProtocolNode::StartRepeating(std::vector<uint8_t> payload, uint32_t interval_ms) {
  uint32_t handle;
  {
    std::lock_guard<std::mutex> lk(mu_);
    handle = repeat_next_++;
    repeats_[handle] = Repeat{payload, interval_ms, 0};
  }
  // 起始即发一帧,并排下一次。
  auto id = NextId();
  (void)SendFrame(FrameType::kState, id.first, id.second, payload);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = repeats_.find(handle);
  if (it != repeats_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval_ms),
        [wself, handle] { if (auto s = wself.lock()) s->FireRepeat(handle); });
  return handle;
}

void ProtocolNode::FireRepeat(uint32_t handle) {
  std::vector<uint8_t> payload; uint32_t interval = 0; bool alive = false;
  { std::lock_guard<std::mutex> lk(mu_);
    auto it = repeats_.find(handle);
    if (it != repeats_.end()) { payload = it->second.payload; interval = it->second.interval_ms; alive = true; } }
  if (!alive || !open_.load()) return;
  auto id = NextId();
  (void)SendFrame(FrameType::kState, id.first, id.second, payload);
  std::weak_ptr<ProtocolNode> wself = weak_from_this();
  std::lock_guard<std::mutex> lk(mu_);
  auto it = repeats_.find(handle);
  if (it != repeats_.end())
    it->second.timer = executor_->ScheduleAt(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(interval),
        [wself, handle] { if (auto s = wself.lock()) s->FireRepeat(handle); });
}

void ProtocolNode::StopRepeating(uint32_t handle) {
  IExecutor::TimerId t = 0;
  { std::lock_guard<std::mutex> lk(mu_);
    auto it = repeats_.find(handle);
    if (it != repeats_.end()) { t = it->second.timer; repeats_.erase(it); } }
  if (t) executor_->Cancel(t);
}
```

(4c) `Close()` 里、`executor_->Stop()` 之前,停心跳与全部 repeating 定时器:
```cpp
  { std::lock_guard<std::mutex> lk(mu_);
    if (heartbeat_timer_) { executor_->Cancel(heartbeat_timer_); heartbeat_timer_ = 0; }
    for (auto& kv : repeats_) if (kv.second.timer) executor_->Cancel(kv.second.timer);
    repeats_.clear(); }
```
(放在 `taken.swap(pending_)` 之后、`executor_->Stop()` 之前。)

- [ ] **Step 5: 运行,确认通过** `cmake --build build -j$(nproc) >/dev/null && ctest --test-dir build -R ProtocolNode 2>&1 | grep -iE "passed|failed"`。Expected: 9 个 `ProtocolNode.*` 通过。

- [ ] **Step 6: 提交**
```bash
git add include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp tests/comm/protocol_node_test.cpp
git -c user.name=Ste7an-cs -c user.email=ste7ann@gmail.com commit -m "feat: ProtocolNode repeating(定时发 STATE,可停)+ 周期心跳"
```

---

## Task 5: 全量验证

**Files:** 无(仅验证)

- [ ] **Step 1: 干净构建零告警**
```bash
cd /home/ubuntu/david/transport
rm -rf build && cmake -S . -B build >/dev/null && cmake --build build -j$(nproc) 2>&1 | grep -iE "warning|error:" ; echo "---warnings (none)---"
```
Expected: 无 `warning`/`error:`。

- [ ] **Step 2: 全量测试连跑两次(查 flaky;含 ThreadExecutor 真实线程用例)**
```bash
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
ctest --test-dir build 2>&1 | grep -iE "tests passed|failed"
```
Expected: 两次都 `100% tests passed`、`0 failed`(ProtocolNode 新增 9 条;DDS/CommNode 测试不回归)。

- [ ] **Step 3: 解耦检查(ProtocolNode 只依赖接口 + 同层 SystemCodec/ThreadExecutor)**
```bash
grep -rn "UdpTransport\|TcpClientTransport\|SerialTransport\|DdsTransport\|FakeDdsProvider\|FastDds" include/transport/comm/ProtocolNode.hpp src/comm/ProtocolNode.cpp || echo "(ProtocolNode 只依赖 ITransport/ICodec/IExecutor + 同层 SystemCodec/ThreadExecutor —— 解耦保持)"
```
Expected: 无输出。

> 本任务无新增文件;若前序均已提交且工作树干净,无需额外提交。

---

## 完成标准

- `Message` 加协议字段;`SystemCodec` = `AA BB CC DD` 协议帧(CRC 注入、resync),字节级 + 拆包/粘包/resync 测试全绿;`comm_node_test` 改 `DdsCodec` 后不回归。
- `ProtocolNode`:`noresponse` / `needresponse`(超时重发 ≤3 后失败)/ `withfeedback`(等 RESULT)/ `needfeedback`(RESPONSE→RESULT→自动 ack)/ `repeating`(定时发 STATE,可停)+ 周期心跳 + 收发双角色(`OnCommand`+`Responder`),(session_id, message_id) 匹配,在 `InlineExecutor` 与 `ThreadExecutor` 下都通过。
- 干净构建零告警;全量两次稳定通过;`CommNode`/`DdsNode`/`DdsCodec`/DDS 测试不动;ProtocolNode 只依赖接口 + 同层默认实现。
- 待实现前替换:`FrameType` 真实字节值、CRC 真实算法(经 `CrcFn` 注入)。
- 范围外(未做):`noresponsewithcheck`、多外部系统路由、QoS、分片。
```
