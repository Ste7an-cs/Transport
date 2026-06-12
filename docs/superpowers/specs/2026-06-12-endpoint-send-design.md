# Endpoint 统一寻址发送 — 设计 spec

> 修订主 spec `2026-06-09-transport-middleware-design.md` 的发送接口:此前 UDP 寻址靠
> `IUdpTransport::SendTo(data, ip, port)`、DDS 寻址靠 `IDdsTransport::Send(data, topic)`,
> 命名/形态不一,且持有 `shared_ptr<ITransport>` 基类句柄时无法寻址发送(必须 downcast)。
> 本设计引入中立的 `Endpoint` 值类型,在 `ITransport` 上提供**唯一**的寻址发送重载,
> 并删除上述两个旧寻址 API(用户决策:只保留 Endpoint 形态)。

**Goal:** 基类句柄即可按目的地发送;跨 transport 发送 API 统一为
`Send(data)` + `Send(data, Endpoint)` 两个形态;与接收侧 `Message.source/topic` 对称。

**Tech Stack:** C++17,无新第三方依赖;GoogleTest 1.14(vendored)。

---

## 1. 新类型 `Endpoint`(`include/transport/Endpoint.hpp`)

纯值类型,零第三方依赖,镜像 `Message` 的两种寻址(source="ip:port" / topic):

```cpp
struct Endpoint {
  enum class Kind { kDefault, kNet, kTopic };
  Kind kind = Kind::kDefault;
  std::string host;     // kNet: ip
  uint16_t    port{0};  // kNet
  std::string topic;    // kTopic

  static Endpoint Default();                           // 用 config 默认目的地
  static Endpoint Net(std::string ip, uint16_t port);  // UDP 寻址
  static Endpoint Topic(std::string name);             // DDS 寻址
};
```

命名工厂让调用点自解释:`t->Send(data, Endpoint::Topic("cmd"))`、
`t->Send(data, Endpoint::Net("10.0.0.2", 9000))`。

收→发对称:收到 `Message` 后可用 `Endpoint::Topic(msg.topic)` 或解析 `msg.source`
构造 `Endpoint::Net(...)` 直接回发。

## 2. `ITransport` 变化(唯一的接口面变化)

```cpp
virtual Status Send(const std::vector<uint8_t>& data) = 0;   // 不变
// 新增:非纯虚,基类默认实现——
//   kDefault → 退化调 Send(data);其余 kind → Fail("io: addressed send not supported")
virtual Status Send(const std::vector<uint8_t>& data, const Endpoint& to);
```

各实现行为矩阵:

| 实现 | kDefault | kNet | kTopic |
|---|---|---|---|
| UdpImpl(覆写) | = Send(data) | sendto(ip,port) | `Fail("config: udp expects net endpoint")` |
| DdsImpl(覆写) | 发默认 topic | `Fail("config: dds expects topic endpoint")` | 发往该 topic |
| TCP/串口(不覆写) | = Send(data) | `Fail("io: addressed send not supported")` | 同左 |

实现注意:每个 `*Impl` 类内加 `using ITransport::Send;`(或同时覆写两个重载),
防止 C++ 名字隐藏导致具体类型句柄上单参 `Send` 不可见。错误前缀沿用既有约定
(`config:` 表示用错了种类,`io:` 表示该传输根本不支持寻址)。

## 3. 删除旧寻址 API(破坏性变更,用户已确认)

- **`IUdpTransport` 整个删除**(`SendTo` 是其唯一成员,删后掏空):
  - `include/transport/udp/IUdpTransport.hpp` 删除;
  - `UdpImpl` 直接继承 `ITransport`;
  - `TransportFactory::Create(UdpConfig)` 返回值 `shared_ptr<IUdpTransport>` → `shared_ptr<ITransport>`。
- **`IDdsTransport::Send(data, topic)` 删除**(连同 `using ITransport::Send;` 兼容声明):
  - `Subscribe / Unsubscribe / SendRequest / OnRequest / Mode / Provider` **保留**——
    订阅与 req-resp 语义无法泛化;`SendRequest` 的 topic 参数不变。
- 工程内所有 `SendTo` / `Send(data, topic)` 调用点(实现、测试、文档示例)改写为 Endpoint 形态。

## 4. 测试策略

新增 `tests/endpoint_send_test.cpp`:

- UDP:`Send(data, Endpoint::Net(...))` 真实回环收发;`Endpoint::Topic` → `config:` Fail;
- DDS(FakeDdsProvider):`Send(data, Endpoint::Topic(...))` 送达订阅方;`kDefault` 走默认 topic;
  `Endpoint::Net` → `config:` Fail;
- TCP 客户端:`Endpoint::Net/Topic` → `io: addressed send not supported`;`kDefault` 退化等价 `Send(data)`;
- 基类句柄多态:`shared_ptr<ITransport>` 持有 UdpImpl/DdsImpl,寻址发送成功(本设计的核心收益);
- 名字隐藏回归:具体类型(`shared_ptr<UdpImpl>` 等)上两个重载均可调用。

既有测试改写:`udp_transport_test` 的 SendTo 用例、`dds_impl_pubsub_test` /
`factory_json_test` 的 `Send(data, topic)` 用例换 Endpoint 形态,断言不变。

## 5. 文档同步

- 主 spec §接口定义:`ITransport` 加 Endpoint 重载;UDP/DDS 节删除旧寻址 API;changelog 记录破坏性变更与理由;
- as-built 架构 spec:类图删 `IUdpTransport`,`UdpImpl --|> ITransport`;`ITransport` 加 `Send(data, Endpoint)`;
- README:UDP/DDS 用法示例改 Endpoint 形态;TransportFactory 一节 `Create(UdpConfig)` 返回类型更新。

## 6. 不做什么(YAGNI)

- 不给 TCP 服务端做 Endpoint 寻址(按 client_id 定向发送可未来按需加 `Kind::kClient`,本次不做);
- 不把 `Subscribe/SendRequest/OnRequest` 泛化进 `ITransport`;
- 不引入 `std::variant`(三字段 + Kind 枚举足够,保持 ABI 简单可读)。
