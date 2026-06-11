# TransportFactory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现所有传输实例的统一创建入口：5 个类型化 `Create` 重载 + `CreateFromFile`（JSON → 实例数组，严格校验，`Result<vector>` 带条目定位错误），并经 `Create(DdsConfig)` 路径根治静态库下 FastDDS 注册器被裁剪问题。

**Architecture:** `TransportFactory.hpp` 公共头只依赖各 config 与接口头（零第三方类型）；`src/TransportFactory.cpp` 内做两件事——①五个 `Create` = `make_shared<对应 *Impl>`（DDS 路径 `#ifdef TRANSPORT_HAS_FASTDDS` 下先 `RegisterFastDdsProvider()`）；②JSON 解析（nlohmann/json 非抛异常模式，`Fields` 提取器统一做类型检查 + 已知键登记 + 未知键报错，错误串 `config: transports[i].field: ...`，任一条目失败整体失败）。

**Tech Stack:** C++17、nlohmann/json（`find_package(nlohmann_json 3.11 QUIET)` 优先，FetchContent 兜底；PRIVATE 链接）、GoogleTest 1.14。

**依据 spec：** `docs/superpowers/specs/2026-06-12-transport-factory-design.md`。

---

## 文件结构

```
include/transport/TransportFactory.hpp   # 公共头（零第三方类型）
src/TransportFactory.cpp                 # 5×Create + JSON 解析（nlohmann 封在此）
tests/factory/
├── factory_create_test.cpp              # 类型化 Create + TCP 回环冒烟 + FastDDS 注册钩子
└── factory_json_test.cpp                # CreateFromFile 正例 + 错误矩阵（gtest TempDir 夹具）
（修改：CMakeLists.txt）
```

---

## Task 1: 公共头 + 5 个类型化 `Create` + 冒烟测试

**Files:**
- Create: `include/transport/TransportFactory.hpp`
- Create: `src/TransportFactory.cpp`（本任务只含 5 个 Create；CreateFromFile 由 Task 2 在同文件补全）
- Test: `tests/factory/factory_create_test.cpp`
- Modify: `CMakeLists.txt`（库源 + 测试源）

- [ ] **Step 1: 写失败测试**

`tests/factory/factory_create_test.cpp`:

```cpp
#include "transport/TransportFactory.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/DdsImpl.hpp"
#include "transport/dds/DdsProviderRegistry.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"

using namespace transport;

namespace {
void WaitFor(std::function<bool()> pred, int ms = 2000) {
  for (int i = 0; i < ms / 5 && !pred(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}
}  // namespace

TEST(FactoryCreate, TypedOverloadsReturnConcreteImpls) {
  TcpClientConfig tc;
  tc.host = "127.0.0.1";
  tc.port = 1;
  auto t1 = TransportFactory::Create(tc);
  ASSERT_NE(t1, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<TcpClientImpl>(t1), nullptr);

  TcpServerConfig sc;
  auto t2 = TransportFactory::Create(sc);
  ASSERT_NE(t2, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<TcpServerImpl>(t2), nullptr);

  UdpConfig uc;
  auto t3 = TransportFactory::Create(uc);
  ASSERT_NE(t3, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<UdpImpl>(t3), nullptr);

  DdsConfig dc;
  dc.topics = {"t"};
  auto t4 = TransportFactory::Create(dc);
  ASSERT_NE(t4, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<DdsImpl>(t4), nullptr);

  SerialConfig se;
  se.device = "/dev/null";
  auto t5 = TransportFactory::Create(se);
  ASSERT_NE(t5, nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SerialImpl>(t5), nullptr);
}

TEST(FactoryCreate, TcpLoopbackSmoke) {
  TcpServerConfig sc;
  sc.bind_addr = "127.0.0.1";
  sc.port = 0;
  auto server_i = TransportFactory::Create(sc);
  auto server = std::dynamic_pointer_cast<TcpServerImpl>(server_i);
  ASSERT_NE(server, nullptr);
  std::atomic<int> conns{0};
  server->OnNewConnection([&](std::shared_ptr<ITransport>) { ++conns; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = server->LocalPort();
  cc.auto_reconnect = false;
  auto client = TransportFactory::Create(cc);
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  WaitFor([&] { return conns.load() >= 1; });

  ASSERT_TRUE(static_cast<bool>(server->Send({4, 2})));  // 广播
  auto r = client->Receive(2000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{4, 2}));
  client->Close();
  server->Close();
}

#ifdef TRANSPORT_HAS_FASTDDS
TEST(FactoryCreate, DdsCreateEnsuresFastDdsRegistered) {
  DdsConfig dc;
  dc.topics = {"t"};
  (void)TransportFactory::Create(dc);  // 应触发 RegisterFastDdsProvider()
  auto p = DdsProviderRegistry::Create("FastDDS");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->ProviderName(), "FastDDS");
}
#endif
```

- [ ] **Step 2: 把库源/测试源加入 CMake**

在 `add_library(transport STATIC ...)` 追加 `src/TransportFactory.cpp`；
在 `add_executable(transport_tests ...)` 追加 `tests/factory/factory_create_test.cpp`。

- [ ] **Step 3: 运行，确认失败**

Run: `cmake --build build -j 2>&1 | head -20`
Expected: 编译失败——找不到 `TransportFactory.hpp`。

- [ ] **Step 4: 写公共头**

`include/transport/TransportFactory.hpp`:

```cpp
#pragma once

// -----------------------------------------------------------------------------
// TransportFactory.hpp — 所有传输实例的统一创建入口
// 类型化 Create：构造不失败（配置校验在 Open()），返回最具体接口。
// CreateFromFile：JSON 配置文件 → 实例数组；解析/校验失败返回 config: 错误
//（含条目定位，如 "config: transports[2].port: ..."），任一条目失败整体失败。
// 本头零第三方类型；JSON 解析（nlohmann）封在 TransportFactory.cpp。
// -----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"
#include "transport/dds/DdsConfig.hpp"
#include "transport/dds/IDdsTransport.hpp"
#include "transport/serial/SerialConfig.hpp"
#include "transport/tcp/ITcpServer.hpp"
#include "transport/tcp/TcpClientConfig.hpp"
#include "transport/tcp/TcpServerConfig.hpp"
#include "transport/udp/IUdpTransport.hpp"
#include "transport/udp/UdpConfig.hpp"

namespace transport {

class TransportFactory {
 public:
  // ---- 代码配置方式 ----
  static std::shared_ptr<ITransport> Create(const TcpClientConfig& config);
  static std::shared_ptr<ITcpServer> Create(const TcpServerConfig& config);
  static std::shared_ptr<IUdpTransport> Create(const UdpConfig& config);
  static std::shared_ptr<IDdsTransport> Create(const DdsConfig& config);
  static std::shared_ptr<ITransport> Create(const SerialConfig& config);

  // ---- 配置文件方式（JSON）----
  static Result<std::vector<std::shared_ptr<ITransport>>> CreateFromFile(
      const std::string& path);
};

}  // namespace transport
```

- [ ] **Step 5: 写实现（本任务部分：5 个 Create）**

`src/TransportFactory.cpp`:

```cpp
#include "transport/TransportFactory.hpp"

#include <utility>
#include <variant>

#include "transport/dds/DdsImpl.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"
#ifdef TRANSPORT_HAS_FASTDDS
#include "dds/FastDdsProvider.hpp"  // RegisterFastDdsProvider（src 内部头）
#endif

// TransportFactory.cpp — 统一创建入口（见 TransportFactory.hpp）。
// 类型化 Create = make_shared<对应 *Impl>；DDS 路径显式注册 FastDDS provider
//（静态库下匿名静态注册器可能被链接器裁剪，工厂是确定被引用的符号，在此根治）。

namespace transport {

std::shared_ptr<ITransport> TransportFactory::Create(
    const TcpClientConfig& config) {
  return std::make_shared<TcpClientImpl>(config);
}

std::shared_ptr<ITcpServer> TransportFactory::Create(
    const TcpServerConfig& config) {
  return std::make_shared<TcpServerImpl>(config);
}

std::shared_ptr<IUdpTransport> TransportFactory::Create(
    const UdpConfig& config) {
  return std::make_shared<UdpImpl>(config);
}

std::shared_ptr<IDdsTransport> TransportFactory::Create(
    const DdsConfig& config) {
#ifdef TRANSPORT_HAS_FASTDDS
  RegisterFastDdsProvider();  // 幂等
#endif
  return std::make_shared<DdsImpl>(config);
}

std::shared_ptr<ITransport> TransportFactory::Create(
    const SerialConfig& config) {
  return std::make_shared<SerialImpl>(config);
}

}  // namespace transport
```

> `CreateFromFile` 在 Task 2 于本文件补全（静态库中未被引用的缺失符号不影响 Task 1 链接）。

- [ ] **Step 6: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R FactoryCreate`
Expected: `FactoryCreate.*`（3 个，含 FastDDS 门控 1 个）PASS；全量套件保持绿色。

- [ ] **Step 7: 提交**

```bash
git add include/transport/TransportFactory.hpp src/TransportFactory.cpp \
        tests/factory/factory_create_test.cpp CMakeLists.txt
git commit -m "feat: TransportFactory 类型化 Create（5 重载 + FastDDS 注册钩子 + TCP 回环冒烟）"
```

---

## Task 2: `CreateFromFile`（JSON 严格解析 + 错误矩阵）

**Files:**
- Modify: `src/TransportFactory.cpp`（追加 JSON 解析与 CreateFromFile）
- Test: `tests/factory/factory_json_test.cpp`
- Modify: `CMakeLists.txt`（nlohmann/json 集成 + 测试源）

- [ ] **Step 1: CMake 集成 nlohmann/json**

在 `CMakeLists.txt` 顶层（FastDDS 门控块之后）加：

```cmake
find_package(nlohmann_json 3.11 QUIET)
if(NOT nlohmann_json_FOUND)
  FetchContent_Declare(
    json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
  )
  FetchContent_MakeAvailable(json)
endif()
target_link_libraries(transport PRIVATE nlohmann_json::nlohmann_json)
```

在 `add_executable(transport_tests ...)` 追加 `tests/factory/factory_json_test.cpp`。

- [ ] **Step 2: 写失败测试**

`tests/factory/factory_json_test.cpp`:

```cpp
#include "transport/TransportFactory.hpp"

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/DdsImpl.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"

using namespace transport;

namespace {

// 写临时 JSON 文件，返回路径
std::string WriteJson(const std::string& name, const std::string& content) {
  std::string path = std::string(::testing::TempDir()) + name;
  std::ofstream out(path, std::ios::trunc);
  out << content;
  return path;
}

// 断言失败且错误串含全部片段
void ExpectConfigError(const Result<std::vector<std::shared_ptr<ITransport>>>& r,
                       std::initializer_list<const char*> fragments) {
  ASSERT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("config:", 0), 0u) << r.error;
  for (const char* f : fragments)
    EXPECT_NE(r.error.find(f), std::string::npos) << r.error << " 缺少: " << f;
}

}  // namespace

TEST(FactoryJson, AllFiveTypesParsed) {
  auto path = WriteJson("all5.json", R"({
    "transports": [
      { "type": "tcp_client", "host": "127.0.0.1", "port": 9000, "auto_reconnect": false,
        "framer": { "header_size": 8, "length_offset": 4, "length_size": 4, "big_endian": true } },
      { "type": "tcp_server", "bind_addr": "127.0.0.1", "port": 9001, "max_clients": 5 },
      { "type": "udp", "mode": "multicast", "multicast_group": "239.0.0.1", "local_port": 5000, "remote_port": 5000 },
      { "type": "dds", "mode": "pubsub", "topics": ["sensor"], "domain_id": 0,
        "qos": { "reliability": "best_effort", "durability": "transient_local", "history_depth": 5 } },
      { "type": "serial", "device": "/dev/ttyS0", "baud_rate": 9600, "parity": "E" }
    ]
  })");
  auto r = TransportFactory::CreateFromFile(path);
  ASSERT_TRUE(static_cast<bool>(r)) << r.error;
  ASSERT_EQ(r.value.size(), 5u);
  EXPECT_NE(std::dynamic_pointer_cast<TcpClientImpl>(r.value[0]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<TcpServerImpl>(r.value[1]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<UdpImpl>(r.value[2]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<DdsImpl>(r.value[3]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SerialImpl>(r.value[4]), nullptr);
}

TEST(FactoryJson, MinimalEntriesUseDefaults) {
  auto path = WriteJson("minimal.json", R"({
    "transports": [
      { "type": "udp" },
      { "type": "serial", "device": "/dev/ttyS9" },
      { "type": "tcp_server", "port": 9002 }
    ]
  })");
  auto r = TransportFactory::CreateFromFile(path);
  ASSERT_TRUE(static_cast<bool>(r)) << r.error;
  EXPECT_EQ(r.value.size(), 3u);
}

TEST(FactoryJson, FramerAppliedBehaviorally) {
  // JSON 配置带 framer 的 server；客户端裸发分两段的整帧 → accepted 端应交付一条完整帧
  auto path = WriteJson("framer.json", R"({
    "transports": [
      { "type": "tcp_server", "bind_addr": "127.0.0.1", "port": 0,
        "framer": { "header_size": 8, "length_offset": 4, "length_size": 4,
                    "big_endian": true, "max_frame_size": 1024 } }
    ]
  })");
  auto r = TransportFactory::CreateFromFile(path);
  ASSERT_TRUE(static_cast<bool>(r)) << r.error;
  auto server = std::dynamic_pointer_cast<TcpServerImpl>(r.value[0]);
  ASSERT_NE(server, nullptr);
  std::shared_ptr<ITransport> accepted;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) { accepted = c; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = server->LocalPort();
  cc.auto_reconnect = false;
  auto client = TransportFactory::Create(cc);
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  for (int i = 0; i < 400 && !accepted; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_NE(accepted, nullptr);

  // 13 字节帧（8 header + 5 body），分两次发
  std::vector<uint8_t> frame(8, 0x00);
  frame[7] = 5;
  frame.insert(frame.end(), 5, 0xAB);
  ASSERT_TRUE(static_cast<bool>(client->Send(
      std::vector<uint8_t>(frame.begin(), frame.begin() + 6))));
  ASSERT_TRUE(static_cast<bool>(client->Send(
      std::vector<uint8_t>(frame.begin() + 6, frame.end()))));

  auto m = accepted->Receive(2000);
  ASSERT_TRUE(static_cast<bool>(m));
  EXPECT_EQ(m.value.payload, frame);  // 一条完整帧（透传则是两段）
  client->Close();
  server->Close();
}

// ---------- 错误矩阵 ----------

TEST(FactoryJson, FileNotFound) {
  auto r = TransportFactory::CreateFromFile("/nonexistent/x.json");
  ExpectConfigError(r, {"cannot open"});
}

TEST(FactoryJson, InvalidJsonSyntax) {
  auto path = WriteJson("bad.json", "{ not json !!!");
  ExpectConfigError(TransportFactory::CreateFromFile(path), {"invalid JSON"});
}

TEST(FactoryJson, MissingTransportsArray) {
  auto path = WriteJson("notr.json", R"({"foo": 1})");
  ExpectConfigError(TransportFactory::CreateFromFile(path), {"transports"});
}

TEST(FactoryJson, UnknownType) {
  auto path = WriteJson("unktype.json",
                        R"({"transports":[{"type":"carrier_pigeon"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "unknown type"});
}

TEST(FactoryJson, UnknownFieldStrict) {
  auto path = WriteJson("unkfield.json",
      R"({"transports":[{"type":"tcp_client","host":"h","port":1,"prot":2}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "prot", "unknown field"});
}

TEST(FactoryJson, UnknownFieldInFramer) {
  auto path = WriteJson("unkframer.json", R"({"transports":[
    {"type":"tcp_server","port":1,"framer":{"header_size":8,"big_endia":true}}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"framer", "big_endia", "unknown field"});
}

TEST(FactoryJson, WrongFieldType) {
  auto path = WriteJson("wrongtype.json",
      R"({"transports":[{"type":"tcp_client","host":"h","port":"9000"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "port"});
}

TEST(FactoryJson, InvalidEnum) {
  auto path = WriteJson("badenum.json",
                        R"({"transports":[{"type":"udp","mode":"anycast"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "mode"});
}

TEST(FactoryJson, MissingRequiredField) {
  auto path = WriteJson("noreq.json",
                        R"({"transports":[{"type":"tcp_client","port":1}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "host", "required"});
}

TEST(FactoryJson, OutOfRangeValue) {
  auto path = WriteJson("range.json",
      R"({"transports":[{"type":"tcp_client","host":"h","port":70000}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "port", "range"});
}

TEST(FactoryJson, SecondEntryBadFailsWhole) {
  auto path = WriteJson("partial.json", R"({"transports":[
    {"type":"tcp_server","port":9003},
    {"type":"nope"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[1]", "unknown type"});
}
```

测试还需 `<chrono>` 与 `<thread>`（FramerAppliedBehaviorally 用到），在文件顶部 include 区补：

```cpp
#include <chrono>
#include <thread>
```

- [ ] **Step 3: 运行，确认失败**

Run: `cmake -S . -B build && cmake --build build -j 2>&1 | tail -5`
Expected: 链接失败——`CreateFromFile` 未定义。

- [ ] **Step 4: 在 `src/TransportFactory.cpp` 追加 JSON 解析与 CreateFromFile**

在 `#include "transport/TransportFactory.hpp"` 之后的 include 区补：

```cpp
#include <cstdint>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
```

在 `namespace transport {` 内、5 个 Create 之后追加：

```cpp
namespace {

using nlohmann::json;

// 解析上下文：记录首个错误（config: + 条目/字段定位）
struct Ctx {
  std::string where;  // 如 "transports[2]" 或 "transports[2].framer"
  std::string err;
  bool ok() const { return err.empty(); }
  void Fail(const std::string& field, const std::string& msg) {
    if (!err.empty()) return;
    err = "config: " + where + (field.empty() ? "" : "." + field) + ": " + msg;
  }
};

// 单个 JSON 对象的字段提取器：类型检查 + 已知键登记 + 未知键报错（严格模式）
class Fields {
 public:
  Fields(const json& obj, Ctx& ctx) : obj_(obj), ctx_(ctx) {}

  void Str(const char* key, std::string* out, bool required = false) {
    auto it = Find(key, required);
    if (it == obj_.end()) return;
    if (!it->is_string()) return ctx_.Fail(key, "expected string");
    *out = it->get<std::string>();
  }

  void StrArray(const char* key, std::vector<std::string>* out,
                bool required = false) {
    auto it = Find(key, required);
    if (it == obj_.end()) return;
    if (!it->is_array()) return ctx_.Fail(key, "expected array of strings");
    out->clear();
    for (const auto& e : *it) {
      if (!e.is_string()) return ctx_.Fail(key, "expected array of strings");
      out->push_back(e.get<std::string>());
    }
  }

  void Bool(const char* key, bool* out) {
    auto it = Find(key, false);
    if (it == obj_.end()) return;
    if (!it->is_boolean()) return ctx_.Fail(key, "expected boolean");
    *out = it->get<bool>();
  }

  template <typename T>
  void Uint(const char* key, T* out, uint64_t max, bool required = false) {
    auto it = Find(key, required);
    if (it == obj_.end()) return;
    if (!it->is_number_unsigned())
      return ctx_.Fail(key, "expected unsigned integer");
    uint64_t v = it->get<uint64_t>();
    if (v > max) return ctx_.Fail(key, "value out of range");
    *out = static_cast<T>(v);
  }

  void Int(const char* key, int* out) {
    auto it = Find(key, false);
    if (it == obj_.end()) return;
    if (!it->is_number_integer()) return ctx_.Fail(key, "expected integer");
    *out = it->get<int>();
  }

  // 子对象；不存在返回 nullptr（已登记为已知键）
  const json* Sub(const char* key) {
    auto it = Find(key, false);
    if (it == obj_.end()) return nullptr;
    if (!it->is_object()) {
      ctx_.Fail(key, "expected object");
      return nullptr;
    }
    return &*it;
  }

  // 全部字段读取完后调用：未知键 → 报错（严格校验）
  void CheckUnknown() {
    for (auto it = obj_.begin(); it != obj_.end(); ++it)
      if (!known_.count(it.key()))
        return ctx_.Fail(it.key(), "unknown field");
  }

 private:
  json::const_iterator Find(const char* key, bool required) {
    known_.insert(key);
    auto it = obj_.find(key);
    if (it == obj_.end() && required)
      ctx_.Fail(key, "required field missing");
    return it;
  }

  const json& obj_;
  Ctx& ctx_;
  std::set<std::string> known_;
};

void ParseFramer(const json& j, Ctx& ctx,
                 std::optional<LengthFieldFramerConfig>* out) {
  LengthFieldFramerConfig f;
  Fields p(j, ctx);
  p.Uint("header_size", &f.header_size, UINT64_MAX);
  p.Uint("length_offset", &f.length_offset, UINT64_MAX);
  p.Uint("length_size", &f.length_size, UINT64_MAX);
  p.Bool("big_endian", &f.big_endian);
  p.Bool("length_includes_header", &f.length_includes_header);
  p.Uint("max_frame_size", &f.max_frame_size, UINT64_MAX);
  p.CheckUnknown();
  if (ctx.ok()) *out = f;
}

void ParseQos(const json& j, Ctx& ctx, DdsQos* out) {
  Fields p(j, ctx);
  std::string rel, dur;
  p.Str("reliability", &rel);
  p.Str("durability", &dur);
  p.Uint("history_depth", &out->history_depth, 0xFFFFFFFFull);
  p.CheckUnknown();
  if (!ctx.ok()) return;
  if (!rel.empty()) {
    if (rel == "reliable") out->reliability = DdsQos::Reliability::kReliable;
    else if (rel == "best_effort") out->reliability = DdsQos::Reliability::kBestEffort;
    else return ctx.Fail("reliability", "invalid enum value: " + rel);
  }
  if (!dur.empty()) {
    if (dur == "volatile") out->durability = DdsQos::Durability::kVolatile;
    else if (dur == "transient_local") out->durability = DdsQos::Durability::kTransientLocal;
    else return ctx.Fail("durability", "invalid enum value: " + dur);
  }
}

// 解析一个 transports[i] 条目；失败时 ctx.err 已置
std::shared_ptr<ITransport> ParseEntry(const json& e, Ctx& ctx) {
  if (!e.is_object()) {
    ctx.Fail("", "expected object");
    return nullptr;
  }
  Fields p(e, ctx);
  std::string type;
  p.Str("type", &type, /*required=*/true);
  if (!ctx.ok()) return nullptr;

  if (type == "tcp_client") {
    TcpClientConfig c;
    p.Str("host", &c.host, true);
    p.Uint("port", &c.port, 0xFFFF, true);
    p.Uint("connect_timeout_ms", &c.connect_timeout_ms, 0xFFFFFFFFull);
    p.Bool("auto_reconnect", &c.auto_reconnect);
    if (const json* f = p.Sub("framer")) {
      Ctx sub{ctx.where + ".framer", ""};
      ParseFramer(*f, sub, &c.framer);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    return TransportFactory::Create(c);
  }

  if (type == "tcp_server") {
    TcpServerConfig c;
    p.Str("bind_addr", &c.bind_addr);
    p.Uint("port", &c.port, 0xFFFF, true);
    p.Int("max_clients", &c.max_clients);
    if (const json* f = p.Sub("framer")) {
      Ctx sub{ctx.where + ".framer", ""};
      ParseFramer(*f, sub, &c.framer);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    return TransportFactory::Create(c);
  }

  if (type == "udp") {
    UdpConfig c;
    std::string mode;
    p.Str("mode", &mode);
    p.Str("local_addr", &c.local_addr);
    p.Uint("local_port", &c.local_port, 0xFFFF);
    p.Str("remote_addr", &c.remote_addr);
    p.Uint("remote_port", &c.remote_port, 0xFFFF);
    p.Str("multicast_group", &c.multicast_group);
    p.Uint("ttl", &c.ttl, 0xFF);
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    if (!mode.empty()) {
      if (mode == "unicast") c.mode = UdpMode::kUnicast;
      else if (mode == "multicast") c.mode = UdpMode::kMulticast;
      else if (mode == "broadcast") c.mode = UdpMode::kBroadcast;
      else { ctx.Fail("mode", "invalid enum value: " + mode); return nullptr; }
    }
    return TransportFactory::Create(c);
  }

  if (type == "dds") {
    DdsConfig c;
    std::string mode;
    p.Str("mode", &mode);
    p.StrArray("topics", &c.topics, true);
    p.Int("domain_id", &c.domain_id);
    p.Str("provider", &c.provider);
    if (const json* q = p.Sub("qos")) {
      Ctx sub{ctx.where + ".qos", ""};
      ParseQos(*q, sub, &c.qos);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    if (!mode.empty()) {
      if (mode == "pubsub") c.mode = DdsMode::kPubSub;
      else if (mode == "reqresp") c.mode = DdsMode::kReqResp;
      else { ctx.Fail("mode", "invalid enum value: " + mode); return nullptr; }
    }
    return TransportFactory::Create(c);
  }

  if (type == "serial") {
    SerialConfig c;
    std::string parity;
    p.Str("device", &c.device, true);
    p.Uint("baud_rate", &c.baud_rate, 0xFFFFFFFFull);
    p.Uint("data_bits", &c.data_bits, 0xFF);
    p.Uint("stop_bits", &c.stop_bits, 0xFF);
    p.Str("parity", &parity);
    if (const json* f = p.Sub("framer")) {
      Ctx sub{ctx.where + ".framer", ""};
      ParseFramer(*f, sub, &c.framer);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    if (!parity.empty()) {
      if (parity.size() != 1 ||
          (parity[0] != 'N' && parity[0] != 'E' && parity[0] != 'O')) {
        ctx.Fail("parity", "invalid enum value (expect \"N\"/\"E\"/\"O\"): " + parity);
        return nullptr;
      }
      c.parity = parity[0];
    }
    return TransportFactory::Create(c);
  }

  ctx.Fail("type", "unknown type: " + type);
  return nullptr;
}

}  // namespace

Result<std::vector<std::shared_ptr<ITransport>>>
TransportFactory::CreateFromFile(const std::string& path) {
  using R = Result<std::vector<std::shared_ptr<ITransport>>>;

  std::ifstream in(path);
  if (!in) return R::Fail("config: cannot open file: " + path);

  // 非抛异常解析（框架不抛异常约定）
  json root = json::parse(in, /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (root.is_discarded()) return R::Fail("config: invalid JSON: " + path);
  if (!root.is_object() || !root.contains("transports") ||
      !root["transports"].is_array()) {
    return R::Fail("config: missing top-level \"transports\" array");
  }

  std::vector<std::shared_ptr<ITransport>> out;
  const json& arr = root["transports"];
  for (size_t i = 0; i < arr.size(); ++i) {
    Ctx ctx{"transports[" + std::to_string(i) + "]", ""};
    auto t = ParseEntry(arr[i], ctx);
    if (!ctx.ok()) return R::Fail(ctx.err);  // 任一条目失败 → 整体失败
    out.push_back(std::move(t));
  }
  return R::Success(std::move(out));
}

}  // namespace transport
```

> 注意：追加代码要放在原有 5 个 Create 之后、原 `}  // namespace transport` 之前（上面片段已含收尾的 namespace 闭合，替换原闭合行）。`ParseEntry` 调用 `TransportFactory::Create`，类型化重载复用、不重复构造逻辑。

- [ ] **Step 5: 运行，确认通过**

Run: `cmake --build build -j && ctest --test-dir build --output-on-failure -R FactoryJson`
Expected: `FactoryJson.*`（14 个）全部 PASS；全量套件保持绿色。

- [ ] **Step 6: 提交**

```bash
git add src/TransportFactory.cpp tests/factory/factory_json_test.cpp CMakeLists.txt
git commit -m "feat: TransportFactory::CreateFromFile（JSON 严格解析 + 条目定位错误 + 整体失败语义）"
```

---

## Task 3: 收尾验证 + 全文档收官同步

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-06-09-transport-middleware-design.md`（§3.4 状态）
- Modify: `docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md`（补 Factory）

- [ ] **Step 1: 干净全量构建 + Factory 套件稳定性**

Run:
```bash
rm -rf build
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
for i in $(seq 1 10); do ctest --test-dir build -R 'Factory' --output-on-failure || break; done
```
Expected: 全部 PASS；Factory 套件 10 次稳定。

> 网络兜底：若 FetchContent 克隆 asio 挂起（代理偶发），用 worktree/旧 build 的 `_deps/asio-src`、`_deps/googletest-src` 复制到 /tmp 并以 `-DFETCHCONTENT_SOURCE_DIR_ASIO=... -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=...` 离线配置（DDS 轮已验证此法）。

- [ ] **Step 2: README 收官**

- 「状态」最后一项改 `- [x] TransportFactory + JSON 配置`；
- 「特点」已实现传输行末尾「（工厂规划中。）」改为「+ **TransportFactory 统一创建入口（含 JSON 配置文件）**。」；
- 「用法」开头的引言行（"当前经具体实现类构造……TransportFactory 入口规划中"）改为推荐工厂：

```markdown
> 推荐经 `TransportFactory` 创建（也可直接 `std::make_shared<*Impl>(config)`）。所有实例须以 `std::shared_ptr` 持有。
```

- 「用法」节末尾（三种接收模式之后）追加：

````markdown
### TransportFactory（统一创建 + JSON 配置文件）

```cpp
#include "transport/TransportFactory.hpp"
using namespace transport;

// 代码配置：返回最具体接口
TcpClientConfig cc; cc.host = "127.0.0.1"; cc.port = 9000;
auto client = TransportFactory::Create(cc);

// JSON 配置文件：一次创建多个实例（格式见主 spec §9.1）
auto r = TransportFactory::CreateFromFile("transports.json");
if (!r) { /* r.error 形如 "config: transports[2].port: ..." */ }
for (auto& t : r.value) t->Open();
```
````

- [ ] **Step 3: 文档收官同步**

- 主 spec §3.4 状态行：`TransportFactory` 移入已实现，改为「……`TransportFactory` 已实现（**主 spec 全部规划范围完成**）」；
- as-built 架构文档（`2026-06-10-foundation-tcp-architecture.md`）：标题/引言补 Factory；类图加：

```
  class TransportFactory {
    <<factory>>
    +Create(各 config)$ 
    +CreateFromFile(path)$ Result~vector~
  }
  TransportFactory ..> TcpClientImpl : 创建
  TransportFactory ..> TcpServerImpl : 创建
  TransportFactory ..> UdpImpl : 创建
  TransportFactory ..> DdsImpl : 创建
  TransportFactory ..> SerialImpl : 创建
```

  并在 §2 依赖说明补一条：Factory 依赖全部 `*Impl` 与 config（创建关系 `..>`），JSON 解析（nlohmann）PRIVATE 封装、不进公共头；`Create(DdsConfig)` 显式 `RegisterFastDdsProvider()` 根治静态库注册裁剪。

- [ ] **Step 4: 提交**

```bash
git add README.md docs/superpowers/specs/2026-06-09-transport-middleware-design.md \
        docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md
git commit -m "docs: README/主 spec/as-built 收官——TransportFactory 完成，主 spec 全部规划范围落地"
```

---

## 后续

主 spec 规划范围至此全部完成。远期 roadmap（不排期）：自研 SHM 传输、协程调度层（C++20 + asio awaitable，已留档）、`Result` 增强（ErrorCode 枚举，已留档）。
