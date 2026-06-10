# transport — C++ 通信中间件

一个将**数据传输**与**数据编解码**解耦的 C++17 通信中间件库。库只负责跨多种协议搬运原始字节流，**不关心数据的类型、格式与内容**；编解码由用户实现的 `ICodec` 承担，框架在收发边界自动调用。

> **非目标：** 不解析/解释/转换消息内容（`ICodec` 之外），不是消息代理或路由守护进程。

---

## 用途（这是什么、解决什么）

把「**怎么传**」（TCP / UDP / 串口 / DDS 的连接、分帧、收发、线程）与「**传什么**」（你的协议帧格式）彻底分开：

- 你写一个 `ICodec`（如何把业务对象 ↔ 字节）和（流式协议下）一个分帧配置，剩下的连接管理、重连、分帧装配、三种接收交付、线程安全队列都由库处理。
- 应用层只面向统一的 `ITransport` 接口编程，**换协议不改业务代码**（TCP↔UDP 仅换 config 与构造类型）。
- 适用：本机进程间通信 + 跨网络通信（混合），Linux 桌面/服务器。

---

## 特点

- **统一抽象 `ITransport`**：生命周期 + 发送 + 三模式接收 + 断连通知 + 编解码挂载，所有传输同一套用法。
- **已实现传输**：TCP（客户端 / 服务端）、UDP（单播 / 组播 / 广播）。（串口 / DDS / 工厂规划中。）
- **可插拔编解码 `ICodec`**：框架在 `Send` 前 `Encode`、收到后 `Decode`；不设则字节透传。
- **可插拔分帧 `IFramer`**：流式传输（TCP/串口）接收侧把字节流切回整帧；内置「长度字段」实现 `LengthFieldFramer`；UDP/DDS 报文天然保边界，自动跳过。
- **三种接收交付模式**（互斥）：同步阻塞 `Receive`、回调 `OnReceive`、future `AsyncReceive`。
- **不抛异常**：所有可失败操作返回 `Result<T>`，错误串带前缀分类：`timeout:` / `conn:` / `codec:` / `frame:` / `io:` / `config:`。
- **TCP 客户端自动重连**（指数退避，封顶）；**TCP 服务端广播**；**UDP 运行期指定目的地** `SendTo`。
- **异步单线程 I/O**：每个传输实例自有 `io_context` + 1 后台线程（Standalone Asio），接收落入**线程安全**的 FIFO 队列。
- **接口层零第三方依赖**：`include/` 只含纯接口与数据结构；Asio / Fast DDS 关在实现层，消费者头文件不被污染。

---

## 设计时考虑的需求点

| 需求 | 设计应对 |
|------|----------|
| 传输与内容解耦 | 库只搬字节；编解码下放到用户实现的 `ICodec`，框架在收发边界自动调用 |
| 多协议、一套用法 | 所有传输实现统一接口 `ITransport`；扩展能力用 `I*Transport`（如 `ITcpServer.OnNewConnection`、`IUdpTransport.SendTo`） |
| 不同应用风格的接收 | 同步 / 回调 / future 三模式，单实例上互斥，由首次接收调用锁定 |
| 流式协议的粘包/半包 | 接收侧 `FrameAssembler` + `IFramer` 用滚动缓冲切帧；报文式传输跳过分帧 |
| 健壮的错误传递 | 全程 `Result<T>`、不抛异常；错误前缀分类，连接级错误也经接收队列投递 |
| 网络抖动 | TCP 客户端指数退避自动重连，重连期间接收队列保活 |
| 线程安全与简单心智 | 每实例单 io 线程 + strand 串行化 socket 操作；接收队列自带锁，应用线程安全消费 |
| 接口干净、可替换、可测 | 接口 / 实现分离（`I*` ↔ `*Impl`）；可写 fake 接口测上层；第三方库不进接口头 |
| 复用接收机能又不被继承绑死 | 接收交付 + 编解码抽成**被持有的组件 `TransportCore`**（组合优于继承），各传输持有它并转发——从根上消除「扩展接口 + 复用基座」的 `ITransport` 菱形 |
| 一个用户同时持有多类 / 多实例 | 所有实例独立、`std::shared_ptr` 持有，互不干扰 |

架构与依赖关系详见 `docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md`（UML 类图 / 时序图）与主 spec `2026-06-09-transport-middleware-design.md` §3。

---

## 构建

```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

依赖（CMake 经 FetchContent 自动拉取）：Standalone Asio 1.30、GoogleTest 1.14。C++17。

---

## 用法

> 当前经具体实现类构造（`std::make_shared<*Impl>(config)`）；统一的 `TransportFactory` 入口规划中。所有实例须以 `std::shared_ptr` 持有。

### 自定义编解码（可选）

```cpp
#include "transport/ICodec.hpp"
using namespace transport;

class MyCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& body) override {
    // 在此组装 header + body 等；返回 Result<...>::Success(bytes) 或 ::Fail("codec:...")
    return Result<std::vector<uint8_t>>::Success(body);
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& frame) override {
    return Result<std::vector<uint8_t>>::Success(frame);
  }
};
```

### TCP 客户端（自动重连 + 长度字段分帧）

```cpp
#include "transport/tcp/TcpClientImpl.hpp"
using namespace transport;

TcpClientConfig cfg;
cfg.host = "127.0.0.1";
cfg.port = 9000;
cfg.auto_reconnect = true;                 // 断线指数退避重连

LengthFieldFramerConfig f;                  // 可选：不设则透传（读到多少即一条 Message）
f.header_size = 8; f.length_offset = 4; f.length_size = 4; f.big_endian = true;
cfg.framer = f;

auto client = std::make_shared<TcpClientImpl>(cfg);
client->SetCodec(std::make_shared<MyCodec>());          // 可选
if (auto st = client->Open(); !st) {
  // st.error 形如 "conn:..." / "timeout:..."
}
client->Send({1, 2, 3});                                // 自动 Encode 后写出

Result<Message> m = client->Receive(1000);              // 同步接收，timeout_ms（0=永久）
if (m) { /* use m.value.payload / m.value.source */ }
client->Close();
```

### TCP 服务端（每客户端独立 transport + 广播）

```cpp
#include "transport/tcp/TcpServerImpl.hpp"
using namespace transport;

TcpServerConfig cfg;
cfg.bind_addr = "0.0.0.0";
cfg.port = 9000;

auto server = std::make_shared<TcpServerImpl>(cfg);
server->OnNewConnection([](std::shared_ptr<ITransport> client) {
  // 每个客户端是独立的 ITransport，自己收发
  client->OnReceive([client](Result<Message> m) {
    if (m) client->Send(m.value.payload);               // echo 回去（回调在 I/O 线程，须非阻塞）
  });
});
server->Open();
// server->Send(bytes);          // 在服务端对象上 Send = 广播给所有已连接客户端
// server->GetClients();         // 当前连接快照
// server->DisconnectClient(id); // 按 "ip:port" 主动断开
```

### UDP（单播 / 组播 / 广播 + 运行期目的地）

```cpp
#include "transport/udp/UdpImpl.hpp"
using namespace transport;

UdpConfig cfg;
cfg.mode = UdpMode::kUnicast;               // 或 kMulticast / kBroadcast
cfg.local_addr = "0.0.0.0";
cfg.local_port = 5000;
cfg.remote_addr = "127.0.0.1";              // Send() 的默认目的地
cfg.remote_port = 6000;
// 组播：cfg.mode=kMulticast; cfg.multicast_group="239.0.0.1"; cfg.ttl=1;

auto udp = std::make_shared<UdpImpl>(cfg);
udp->Open();
udp->Send({1, 2, 3});                       // 发往默认目的地（按 mode）
udp->SendTo({4, 5, 6}, "10.0.0.7", 7000);   // 运行期指定目的地，忽略默认 remote
```

### 三种接收模式（任选其一，单实例上互斥）

```cpp
// 1) 同步阻塞
Result<Message> m = t->Receive(1000);                   // timeout_ms；0=永久阻塞

// 2) 回调（在 I/O 线程执行，必须非阻塞）
t->OnReceive([](Result<Message> m) { /* ... */ });

// 3) future（每次调用消费一条到来的消息；多个未决 future 按到达 FIFO 兑现）
std::future<Result<Message>> fut = t->AsyncReceive();
Result<Message> r = fut.get();

// 断连通知（TCP 客户端 / 串口适用）
t->OnDisconnect([](const std::string& reason) { /* reason 形如 "conn:..." */ });
```

`Result<T>` 用法：`if (r) use(r.value); else log(r.error);`（`operator bool` == `r.ok`）。
`Message` 字段：`payload`（字节）、`source`（"ip:port" 等）、`topic`（DDS 用，TCP/UDP 为空）、`timestamp`（接收微秒时间戳）。

---

## 状态

- [x] Foundation：核心接口、分帧、接收交付（`TransportCore`）、`Result`/`Message`
- [x] TCP（client / server）
- [x] UDP（单播 / 组播 / 广播）
- [ ] 串口
- [ ] DDS（Fast DDS，pub-sub / req-resp）
- [ ] TransportFactory + JSON 配置

---

## 文档

- 框架总设计：`docs/superpowers/specs/2026-06-09-transport-middleware-design.md`
- 已实现部分 as-built 架构（UML 类图 / 时序图）：`docs/superpowers/specs/2026-06-10-foundation-tcp-architecture.md`
- 各子系统 spec / plan：`docs/superpowers/specs/`、`docs/superpowers/plans/`
