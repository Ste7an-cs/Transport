# transport — C++ 通信中间件

C++17 通信中间件库，把**传输**、**编解码**、**交互**三层彻底解耦，以 **AsyncTask 协程运行时**（boost.fiber）为异步执行环境。

| 层 | 职责 |
|---|---|
| **Transport**（纯字节管道） | 跨 TCP / UDP / 串口 / DDS 搬运原始字节，不解释消息类型、请求关联或 payload 语义。内部一条管理泵负责链路的建立、重建与重试，对外只交出读队列的等待器句柄与写入口。 |
| **ICodec**（线缆格式） | 在收发边界把逻辑 `Message` ↔ 线缆字节（分帧 + 序列化 + 校验 + 重同步）。流式跨切片拼帧、报文式保边界。**应用可自行提供并装配。** |
| **node**（交互层） | 组合前两层，交付请求-响应与发布-订阅。协议语义内联各 node，不设共享交互引擎。 |

**非目标**：不解析 payload 业务语义；不是消息代理或通用路由守护进程；不内建加密、认证与访问控制。

---

## 快速开始

编程主入口是**交互层 node**。传输由**宿主**创建、启动、关闭，节点按引用借用。

### 外部协议：请求-响应（`ProtocolNode`）

```cpp
#include "transport/io/tcp/TcpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"
#include "transport/codec/SystemCodec.hpp"

using namespace transport;
using namespace std::chrono_literals;

TcpConfig cfg;
cfg.host = "127.0.0.1";
cfg.port = 9000;
cfg.silence_timeout = 5000ms;          // 唯一的时间量：等连上 / 读静默 / 重连间隔

TcpTransport transport(cfg);
(void)transport.Start();               // 宿主启动传输

ProtocolNode node(transport, std::make_unique<SystemCodec>(), ProtocolNodeConfig{});
(void)node.Start();

Message req;
req.payload = {0x01, 0x02};
auto rsp = node.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
if (rsp) { /* 用 rsp.value().payload */ }

node.Close();        node.WaitClosed();      // 先关节点
transport.Close();   transport.WaitClosed(); // 再关传输
```

四种交互模式：

| 方法 | 语义 |
|---|---|
| `Send(msg)` | 单向，不等回应 |
| `RequestForResponse(req, retry)` | 等一个回应帧 |
| `RequestForResult(req, retry, result_mid, result_timeout)` | 两阶段：先等受理，再等结果 |
| `RequestForResultDirect(req, retry, result_mid)` | 单阶段：直接等结果，超时即重发 |

`RetryPolicy{timeout, max_attempts}` **逐次传参**，节点配置面上没有任何时限缺省值；`timeout` 须为正、`max_attempts` 须 ≥ 1（含首发）。

### 订阅入站消息

节点只交出**凭据**，消费在调用方自己的 fiber 上：

```cpp
auto sub = node.Subscribe(AnyOfType(FrameType::kCommand));   // 须在 Start() 之后
if (!sub) { /* kClosed：未启动 / 已关闭 */ }
auto ticket = std::move(sub).value();

auto worker = Coro::makeTask([&] {
  for (;;) {
    auto m = Coro::await(ticket.mailbox());
    if (!m) break;                    // 信箱被节点关闭 → 退出
    try { Handle(m.value()); }
    catch (...) { /* 自行隔离 */ }
  }
});
...
(void)worker.get();                   // 宿主自己 join，勿依赖 WaitClosed
```

订阅键的具名工厂：`ResponseTo(request)` / `FrameOf(session_id, message_id, type)` / `AnyOfType(type)`；不参与匹配的字段填 `kAny`。**一条消息投给全部键匹配的订阅者，各得一份副本。**

### DDS：发布-订阅与请求-响应（`DdsNode`）

topic 由**注册接口**给出，且只在 `Start()` 之前受理：

```cpp
DdsTransport transport(dds_config);
(void)transport.Start();

DdsNode node(transport, std::make_unique<DdsCodec>(), DdsNodeConfig{});

(void)node.RegisterPublishers ({"telemetry"});   // 发布：topic
(void)node.RegisterSubscribers({"telemetry"});   // 订阅：topic
(void)node.RegisterClients    ({"get"});         // 请求-响应客户端：服务名
(void)node.RegisterServices   ({"get"});         // 请求-响应服务端：服务名

(void)node.Start();                              // 端点在此一次性建出
```

**请求-响应只说服务名**，两个 topic 由框架派生（`cfg.` 为固定前缀）：

```
请求 topic = cfg.<服务名>.request        应答 topic = cfg.<服务名>.response
```

两侧用同一个派生函数，故不可能配歪。客户端与服务端**传一模一样的服务名**。

```cpp
// 客户端
auto result = node.RequestForResultDirect("get", req, RetryPolicy{2000ms, 3});

// 服务端
auto serving = node.ServeRequests("get");
auto worker  = Coro::makeTask([&] {
  for (;;) {
    auto r = serving.value().Wait();
    if (!r) break;
    (void)node.Reply(r.value(), Handle(r.value()));
  }
});

// 发布-订阅
(void)node.Publish("telemetry", msg);
auto notes = node.Subscribe(TopicKey{"telemetry"}, KindKey{MessageKind::kNotify});
```

**相位规则**：四个注册方法**只在 `Created`** 受理，`Subscribe` / `Publish` / `RequestForResultDirect` / `ServeRequests` / `Reply` **只在 `Running`** 受理。全流程即「注册 → `Start()` → 订阅/收发」。

> ⚠ 框架占用 `cfg.*.request` / `cfg.*.response` 这一命名空间：它与 `RegisterPublishers` / `RegisterSubscribers` 收的普通 topic 处在同一平面，`RegisterSubscribers({"cfg.get.request"})` 与 `RegisterServices({"get"})` 指的是同一条 topic。框架不拦。

---

## 内部传输契约（`ITransport`）

介质无关的内部缝，**非用户 API**——宿主只需创建、`Start()`、`Close()` / `WaitClosed()`。

```cpp
class ITransport {
 public:
  virtual Coro::Result<void> Start()      = 0;   // 起内部管理泵后即返回
  virtual Coro::Result<void> Close()      = 0;   // 只发信号，不等待收敛。幂等
  virtual void               WaitClosed() = 0;   // join 全部内部工作单元

  virtual std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead()          = 0;
  virtual Coro::Result<void>                        AsyncWrite(Datagram) = 0;

  virtual std::error_code LastError()        const = 0;
  virtual LinkState       CurrentLinkState() const = 0;
};
```

**读写刻意不对称。** 读是"数据什么时候来"，只能交出等待器句柄，超时、取消与是否 `shared()` 扇出由调用方自理——不设单读守卫，多个消费者直接 await 同一句柄是抢占关系。写是"把这份数据发到那里去"，调用方给完即返回。

**写为彻底的 fire-and-forget**：`AsyncWrite` 只判生命周期与入队，返回成功仅表示已受理；目的地能否解析、socket 是否写成一律不回传，只落 `LastError()`。链路不可用时数据留在内部队列等待恢复，不拒绝、不丢弃。**由此不提供背压。**

`Datagram{bytes, peer}` 读写共用：读到的 `peer` 是发送方，写出的 `peer` 是目的地（`Endpoint::Default()` 表示"发往本传输配置的默认对端"，故传输无关的调用方恒可传它）。

`WaitClosed()` 不设时限也不返回结果：`Awaitable::close()` 只保证唤醒等待者，而"可安全释放"要求 fiber 已跑完，只有 `FiberTask::get()` 给得了。

> **一条传输可被多个节点共用**，各得全量副本；但**任一节点关闭即终结整条读流**（`Awaitable::close()` 整流传播，有意为之），不支持独立关停——共用的诸节点须一起关。

---

## 构建

```bash
git submodule update --init --recursive third_party/AsyncTask
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

**前置依赖：**

- **Qt5**（5.12+；Core / Network / SerialPort，如 `libqt5serialport5-dev`）
- **AsyncTask**（boost.fiber 协程运行时，`third_party/AsyncTask` 子模块）+ 已编译 boost `fiber/context/thread/chrono`。子模块未初始化时 configure 直接 `FATAL_ERROR`
- **Fast DDS 3.6.1**（唯一可选外部依赖）：未装时 `find_package` 自动跳过 `FastDdsProvider`，其余能力照常构建可测；装后自动启用（带 `TRANSPORT_HAS_FASTDDS`）

显式禁用 DDS：

```bash
cmake -S . -B build -DCMAKE_DISABLE_FIND_PACKAGE_fastdds=ON
```

GoogleTest vendored 在 `third_party/`。产物为**单一 `transport` 静态库**与**单一 `transport_tests` 可执行文件**（全部用例在 AsyncTask fiber 调度器内跑）。C++17，目标平台 Linux。

---

## 关键约束

- **三层解耦**：传输不依赖逻辑消息或协议语义；codec 是公共扩展点；node 组合三者。〔RT_IN_INTERFACE_001/002〕
- **AsyncTask 强制运行时**：不设独立业务调度体系；M:N 协作式，同一节点的状态与关联串行，不同节点可并行。〔RT_DESIGN_002、RT_CORO_RUNTIME〕
- **无共享交互引擎**：协议语义归各 node，公共只复用协议无关的 `Dispatcher` 与生命周期基类。〔RT_DESIGN_003、RT_NODE_003〕
- **不抛异常**：预期失败用 `Coro::Result<T>`（`[[nodiscard]]`）+ 机器可判别的 `TransportErrc`。〔RT_ERROR_001/002/003〕
- **节点不管传输的生命周期**：宿主创建、启动、关闭传输，节点按引用借用。
- **框架不提供可观测性**：内部丢弃（队列满丢最旧 / 坏帧 / 迟到·无匹配响应）完全静默，排障须由宿主在 codec 或订阅侧自行加日志。
- **底层回调不碰节点状态**：Qt I/O 与 DDS listener 回调安全转交节点执行域，不在回调线程执行业务处理。〔RT_NODE_004〕

---

## 文档

- **需求规格说明书（SRS）**：[`docs/需求规格说明书-协程原生.md`](docs/需求规格说明书-协程原生.md) —— 可观察/可验收行为，标识前缀 `RT_`
- **软件设计说明（SDD, GJB438C）**：[`docs/软件设计说明-GJB438C.md`](docs/软件设计说明-GJB438C.md) —— 部件、接口、详细设计与追溯矩阵
- **架构决策记录（ADR）**：[`docs/adr/`](docs/adr/) —— 每条决策的依据、否决的备选与明确接受的代价
- **项目术语（单一权威）**：[`CONTEXT.md`](CONTEXT.md)
- **编码规范**：[`CODING_STANDARDS.md`](CODING_STANDARDS.md)
- **变更日志**：[`CHANGELOG.md`](CHANGELOG.md)
