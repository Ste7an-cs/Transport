# TransportFactory — 实现设计 spec

> 本 spec 是主设计文档 `2026-06-09-transport-middleware-design.md` §9（TransportFactory）的**实现层细化**，并修订主 spec 两点：①`CreateFromFile` 返回 `Result<vector>`（原裸 vector 无错误通道，违背框架不抛异常约定）；②JSON 映射规则明确化（枚举字符串、qos/framer 子对象、严格校验）。

**Goal:** 实现所有传输实例的统一创建入口：5 个类型化 `Create` 重载（返回最具体接口）+ `CreateFromFile`（JSON 配置文件 → 实例数组，解析/校验失败返回带条目定位的 `config:` 错误），并借 `Create(DdsConfig)` 路径解决静态库下 FastDDS 自动注册被裁剪的问题。

**Tech Stack:** C++17、nlohmann/json（`find_package` 优先，FetchContent 兜底；仅 `TransportFactory.cpp` 内部使用，公共头零第三方类型）、GoogleTest 1.14。

---

## 1. 范围与文件布局

```
include/transport/TransportFactory.hpp   # 公共头：仅依赖各 config 与接口头
src/TransportFactory.cpp                 # 5 个 Create + JSON 解析（nlohmann 封在此）
tests/factory/
├── factory_create_test.cpp              # 类型化 Create + 回环冒烟
└── factory_json_test.cpp                # CreateFromFile 正例 + 错误矩阵（临时文件夹具）
```

nlohmann/json 获取：`find_package(nlohmann_json 3.11 QUIET)`；未找到则 FetchContent（header-only，目标 `nlohmann_json::nlohmann_json`，PRIVATE 链接给 `transport`——不进公共接口）。

---

## 2. 公共 API（修订主 spec §9）

```cpp
class TransportFactory {
 public:
  // ---- 代码配置方式：构造不失败（配置校验在 Open()），返回最具体接口 ----
  static std::shared_ptr<ITransport>    Create(const TcpClientConfig& config);
  static std::shared_ptr<ITcpServer>    Create(const TcpServerConfig& config);
  static std::shared_ptr<IUdpTransport> Create(const UdpConfig& config);
  static std::shared_ptr<IDdsTransport> Create(const DdsConfig& config);
  static std::shared_ptr<ITransport>    Create(const SerialConfig& config);

  // ---- 配置文件方式（JSON）----
  // 解析/校验失败返回 Fail("config: ...")，错误串含条目序号与字段名
  //（如 "config: transports[2].port: expected unsigned integer"）。
  // 任一条目失败 → 整体失败（不返回部分结果，避免半初始化状态）。
  static Result<std::vector<std::shared_ptr<ITransport>>> CreateFromFile(
      const std::string& path);
};
```

实现要点：
- 每个 `Create` = `std::make_shared<对应 *Impl>(config)`（TcpClientImpl / TcpServerImpl / UdpImpl / DdsImpl / SerialImpl）。
- **`Create(DdsConfig)`（与 JSON 的 `"dds"` 路径）在 `TRANSPORT_HAS_FASTDDS` 编译时先调用 `RegisterFastDdsProvider()`**（幂等）——静态库链接下匿名静态注册器可能被裁剪，工厂是确定会被引用的符号，在此显式注册即根治（DDS spec 预留的钩子）。无 FastDDS 编译时不调用；`DdsImpl` 仍可构造（用户可注册自有 provider），未注册则其 `Open()` 报 `config: provider not registered`。
- 公共头只 include 各 config 头与接口头；nlohmann 类型不出现在任何公共头。

---

## 3. JSON 映射规则（修订主 spec §9.1 的明确化）

顶层结构：`{ "transports": [ { "type": "...", ...字段 }, ... ] }`。

### 3.1 type 与字段映射

| `type` | 映射 config | 字段（= 结构体字段名，snake_case） |
|---|---|---|
| `"tcp_client"` | `TcpClientConfig` | `host`(必填), `port`(必填), `connect_timeout_ms`, `auto_reconnect`, `framer`{} |
| `"tcp_server"` | `TcpServerConfig` | `bind_addr`, `port`(必填), `max_clients`, `framer`{} |
| `"udp"` | `UdpConfig` | `mode`, `local_addr`, `local_port`, `remote_addr`, `remote_port`, `multicast_group`, `ttl` |
| `"dds"` | `DdsConfig` | `mode`, `topics`(必填,字符串数组), `domain_id`, `qos`{}, `provider` |
| `"serial"` | `SerialConfig` | `device`(必填), `baud_rate`, `data_bits`, `stop_bits`, `parity`(单字符串), `framer`{} |

- **缺省字段 → 结构体默认值**（与代码配置一致）。"必填"指无合理默认、缺失即 `config:` 错误（`host`/`port`/`device`/`topics`）；其余 Open() 期校验不前移。
- `framer` 子对象（tcp_client/tcp_server/serial 可选）：`header_size`, `length_offset`, `length_size`, `big_endian`, `length_includes_header`, `max_frame_size` → `LengthFieldFramerConfig`；出现即设置 `config.framer`。
- `qos` 子对象（dds 可选）：`reliability`: `"reliable"`/`"best_effort"`；`durability`: `"volatile"`/`"transient_local"`；`history_depth`: 正整数 → `DdsQos`。
- 枚举字符串：udp `mode`: `"unicast"`/`"multicast"`/`"broadcast"`；dds `mode`: `"pubsub"`/`"reqresp"`；serial `parity`: `"N"`/`"E"`/`"O"`。

### 3.2 严格校验（决策：拼错即报错，不静默）

以下一律 `Fail("config: ...")` 并带定位（条目序号 + 字段名）：
- 文件打不开 / JSON 语法错误；
- 顶层缺 `transports` 或不是数组；
- 条目缺 `type` / `type` 未知；
- **未知字段**（含子对象内）——拼写错误立即暴露，不静默用默认值；
- 字段 JSON 类型不符（如 `port` 给了字符串）、数值越界（如 `port` > 65535、`ttl` > 255）；
- 枚举字符串非法；必填字段缺失。

任一条目失败 → 整个 `CreateFromFile` 失败（已构造的实例随 shared_ptr 析构，未 Open 无副作用）。

---

## 4. 测试策略

**`factory_create_test`（类型化 Create）：**
- 5 个重载各返回非空，且 `dynamic_pointer_cast` 到对应具体接口/实现成功；
- 回环冒烟：factory 创建 TcpServerImpl+TcpClientImpl（`dynamic_pointer_cast<TcpServerImpl>` 取 `LocalPort()`），真实连接收发一条——证明工厂创建的实例可用；
- `TRANSPORT_HAS_FASTDDS` 下：`Create(DdsConfig)` 后 `DdsProviderRegistry::Create("FastDDS")` 非空（注册钩子生效）。

**`factory_json_test`（CreateFromFile，临时文件夹具写入 build 目录或 /tmp）：**
- 正例：含全 5 类型的配置 → `Result` ok、5 个实例、动态类型逐一正确；
- framer 生效（行为验证）：JSON 配置带 framer 的 tcp_server + tcp_client 对，回环发跨包帧 → 完整帧交付；
- qos/枚举/默认值：udp `"multicast"` 解析正确（Open 后行为或字段可观察处验证）、缺省字段用默认值（如 serial 不写 `baud_rate`）；
- 错误矩阵（逐项断言 `!result` 且错误串含 `config:` 与定位）：文件不存在 / 语法错 / 缺 `transports` / 未知 `type` / 未知字段（顶层与 framer 子对象各一）/ 字段类型错 / 枚举非法 / 必填缺失 / 越界值；
- 整体失败语义：两条目中第二条坏 → 返回 Fail（无部分结果）。

---

## 5. 主 spec 同步修改

- §9：`CreateFromFile` 签名改 `Result<std::vector<std::shared_ptr<ITransport>>>`，注明错误定位格式；
- §9.1：补枚举字符串表、`qos`/`framer` 子对象、严格校验与整体失败语义；
- changelog 增补一条（签名修订理由：无错误通道 + 不抛异常约定）。

## 6. 与既有部分的衔接

- 仅依赖已实现的 5 个 `*Impl` 与各 config；不改任何传输代码。
- 完成后主 spec §3.4 状态行全绿（`TransportFactory` 移入已实现）；as-built 架构文档补 `TransportFactory ..> 各 Impl` 创建关系；README「状态」最后一项勾掉、用法节补 CreateFromFile 示例。
- 本子系统完成即达成主 spec 全部规划范围。
