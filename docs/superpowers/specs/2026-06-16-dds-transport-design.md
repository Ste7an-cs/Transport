# DDS 底层(pub-sub 字节传输 + provider 抽象)— 设计 spec

> 0.2.0 重构的最后一个底层件:在纯字节管道架构上补 **DDS**。DDS 是按 topic 的 pub-sub、
> 报文式传输,故设计为 **`ITransport` + `Subscribe` 扩展**;真实 DDS 栈经 `IDdsProvider`
> 隔离(`FakeDdsProvider` 进程内总线默认/测试,`FastDdsProvider` 可选、`find_package`、不内置)。
> **只做 pub-sub**;请求-应答留给后续 `System`(用双 topic + `correlation_id` 在上层搭)。
> 0.1.0 的 `DdsImpl`(含 req-resp + RawMessage)由 `v0.1.0` 标签保留备查,本设计是其简化重建。

**Goal:** `DdsTransport` 以统一的 `ITransport` 用法做 DDS pub-sub:`Send(bytes, Endpoint::Topic)` 发布、`Subscribe(topic)` 订阅、`OnBytes(bytes, from=topic)` 收;真实 DDS 经 provider 隔离,逻辑零外部依赖可全测;FastDDS 可选,未装构建照常绿。

**Tech Stack:** C++17;Standalone Asio(已 vendored,本件 DDS 交付路径不强依赖它);GoogleTest 1.14(已 vendored);Fast DDS 2.13+(可选外部依赖,`find_package(fastrtps/fastcdr)`,**不内置**);不抛异常。

**配套:** 架构 spec `docs/superpowers/specs/2026-06-15-system-codec-transport-design.md`;TCP server spec `docs/superpowers/specs/2026-06-16-tcp-server-transport-design.md`。

---

## 1. 动机与映射

纯字节管道 `ITransport` 已覆盖 TCP 客户端/服务端、串口、UDP。DDS 不同:按**命名 topic** 的 pub-sub,报文式(每样本保边界,无流式分帧),非点对点。

映射:DDS 本质是"把字节发到 topic / 从订阅的 topic 收字节",可作 `ITransport`,只多一个 `Subscribe` 能力 → **`IDdsTransport : ITransport` + `Subscribe/Unsubscribe`**。req-resp 不在此层:`System` 后续用 `Subscribe("x_Reply")` + 发到 `"x_Request"` + `correlation_id`(经 `SystemCodec` 上线缆)在上层搭。故 DDS 底层只搬**不透明字节 / topic**,FastDDS 的 `TopicDataType` 退化为"只携带 `[]byte`"。

```
DdsTransport : IDdsTransport(: ITransport)              ← 用户面(统一 ITransport 用法 + Subscribe)
   持有 unique_ptr<IDdsProvider> ──┬─ FakeDdsProvider(进程内总线,按 domain 共享;默认/测试)
                                   └─ FastDdsProvider(可选,find_package,不内置)
   DdsProviderRegistry: name → 工厂(内建 Fake 总注册;FastDDS 编进来时注册)
```

---

## 2. 共享数据类型(沿用)
`Result`/`Status`(不抛异常,前缀分类),`Endpoint`(用 `kTopic` + `Topic(name)`;`kNet` 对 DDS 非法)。`Message` 本件不出现(DDS 底层只进出裸字节;Message 是 codec/System 的事)。

---

## 3. `DdsConfig`

```cpp
// include/transport/dds/DdsConfig.hpp
struct DdsQos {
  enum class Reliability { kBestEffort, kReliable };
  enum class Durability  { kVolatile, kTransientLocal };
  Reliability reliability = Reliability::kReliable;
  Durability  durability  = Durability::kVolatile;
  uint32_t    history_depth = 10;
};

struct DdsConfig {
  int         domain_id = 0;
  std::string default_topic;          // Send(bytes) 无 endpoint 时的目的 topic
  std::string provider = "fake";      // registry 名;真实互通用 "fastdds"
  DdsQos      qos;
};
```

---

## 4. `IDdsTransport` 扩展接口 + `DdsTransport`

```cpp
// include/transport/dds/IDdsTransport.hpp
class IDdsTransport : public ITransport {
 public:
  virtual Status Subscribe(const std::string& topic) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;
};

// include/transport/dds/DdsTransport.hpp
class DdsTransport : public IDdsTransport,
                     public std::enable_shared_from_this<DdsTransport> {
 public:
  // provider 为空 → Open() 时按 config_.provider 经 registry 建(默认路径);
  // 注入非空 provider 用于测试(DI)。
  explicit DdsTransport(DdsConfig config,
                        std::unique_ptr<IDdsProvider> provider = nullptr);
  ~DdsTransport() override;

  Status Open() override;   // 建/Init provider;成功后触发 OnConnect
  void   Close() override;  // Unsubscribe 全部 + provider Shutdown
  bool   IsOpen() const override;

  Status Send(const std::vector<uint8_t>& bytes) override;             // → default_topic
  Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) override;

  Status Subscribe(const std::string& topic) override;
  Status Unsubscribe(const std::string& topic) override;

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }
};
```

**语义:**
- `Send(bytes, Endpoint::Topic("x"))` → `provider_->Publish("x", bytes)`;`Send(bytes)` 或 `Send(bytes, Endpoint::Default())` → 发 `config_.default_topic`(为空则 `Fail("config: no default topic")`);`Send(bytes, Endpoint::Net(...))` → `Fail("config: dds expects topic endpoint")`。
- `Subscribe(topic)` → `provider_->Subscribe(topic, cb)`,其中 `cb` **捕获该 topic**,样本到达时调 `bytes_cb_(Result::Success(bytes), /*from=*/topic)`。`Unsubscribe(topic)` → `provider_->Unsubscribe(topic)`。
- `Open()`:若 `provider_` 为空,`RegisterBuiltinProviders()`(幂等)后 `DdsProviderRegistry::Create(config_.provider)`;为空 → `Fail("config: provider not registered: ...")`。`provider_->Init(config_)`;成功 `open_=true` 并触发 `connect_cb_`。
- `Close()`:幂等;`provider_->Shutdown()`;DDS 无连接概念,`disconnect_cb_` 本层不主动触发(留作 provider 致命错误的将来用途)。
- **保活防环:** 订阅回调被 provider 长期持有 → 回调捕获 `weak_ptr<DdsTransport>`(`provider_->callback→weak(transport)`,`transport→unique_ptr(provider)`,无环)。

---

## 5. `OnBytes` 线程模型(DDS 专属,已定)

DDS 是按 topic 并行的:不同 topic 的样本来自不同 reader 线程。**本设计不做全局串行**(那是高频多 topic 的吞吐/延迟瓶颈),而是:

- **同一 `from`(topic)的 `OnBytes` 按序、不并发**(DDS 保证单 reader 内有序);
- **不同 topic 的 `OnBytes` 可并发**;
- **直接在 provider 的 listener 线程上调 `OnBytes`**(零额外交付线程);背压按 topic 局部化(慢回调只拖慢该 topic 的收包,与"阻塞 OnBytes 拖慢本连接"语义一致)。
- **回调须非阻塞。** 耗时业务子类/用户自行派发到别处。

> 这是 `ITransport` "OnBytes 串行不并发" 契约对 DDS 的**明确放宽**(头注释/文档需标注):点对点 transport 单连接串行;DDS 是"**同 topic 串行、跨 topic 并发**"。下游安全由**按 topic 注册 codec**(`SetCodec(topic, codec)`,架构既有)保证 —— DDS 正是 per-topic codec 的主场,每个 topic 的有序流喂各自的 codec,无跨线程竞争。

---

## 6. `IDdsProvider` + 内建实现 + 注册表

```cpp
// include/transport/dds/IDdsProvider.hpp
class IDdsProvider {
 public:
  virtual ~IDdsProvider() = default;
  virtual Status Init(const DdsConfig& config) = 0;
  virtual void   Shutdown() = 0;
  virtual Status Publish(const std::string& topic, const std::vector<uint8_t>& bytes) = 0;
  // cb 在样本到达时被 provider 调用(FastDDS=该 topic 的 listener 线程;Fake=发布线程)。
  virtual Status Subscribe(const std::string& topic,
                           std::function<void(const std::vector<uint8_t>&)> cb) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;
  virtual std::string Name() const = 0;
};
```

### 6.1 `FakeDdsProvider`(进程内总线;默认 + 测试)
- 一个**按 `domain_id` 共享的进程内总线**(静态、加锁):`Init` 把本 provider 接入 `domain_id` 的总线;`Publish(topic, bytes)` → 调该 domain 下订阅了该 topic 的所有回调;`Subscribe/Unsubscribe` 增删本 provider 在该 topic 的回调。
- 让**同进程**两个 `DdsTransport`(同 domain)互通 → DDS 逻辑零外部依赖即可全测。
- 交付:`Publish` **同步**调订阅回调(发布线程上)—— 测试确定、无需额外线程;domain 隔离(不同 `domain_id` 互不可见)。
- 总线状态用 mutex 保护(可能多线程发布/订阅)。

### 6.2 `FastDdsProvider`(可选,`find_package`,不内置)
- `find_package(fastrtps/fastcdr QUIET)`;探测到才编译。每 topic 懒建一对 `DataWriter`/`DataReader`(共享 participant=domain)。
- **极简自定义 `TopicDataType` 只携带 `[]byte`**:wire `[uint32 LE len][bytes]`(或直接用 serialized payload);无 `request_id`/`reply_topic`。所有 topic 共用此 type。
- `on_data_available` listener 取样本字节,调对应 topic 的 `cb`(FastDDS reader 线程上 → §5 甲)。
- QoS 由 `DdsConfig.qos` 映射(reliability/durability/history)。
- 互通要求对端注册同名 type 并遵循此布局。

### 6.3 `DdsProviderRegistry`
```cpp
class DdsProviderRegistry {
 public:
  using Factory = std::function<std::unique_ptr<IDdsProvider>()>;
  static void Register(const std::string& name, Factory f);
  static std::unique_ptr<IDdsProvider> Create(const std::string& name);  // 未注册→nullptr
};
// RegisterBuiltinProviders():幂等;注册 "fake";#ifdef TRANSPORT_HAS_FASTDDS 注册 "fastdds"。
// DdsTransport::Open 显式调用它(避免静态库下匿名自注册被链接器裁剪 —— 0.1.0 的已知坑)。
```

---

## 7. 错误处理
- 全程 `Result`/`Status`,前缀分类。
- `config:`:Send 用错 endpoint(`kNet`)、`default_topic` 空、provider 未注册、未 Open。
- `io:`:provider 底层失败(participant/writer/reader 建失败、发布失败等)。
- 坏样本/解码是 codec/System 的事;DDS 底层只交付原始字节。

---

## 8. 测试策略
- **`DdsTransport` 经 `FakeDdsProvider`(核心,零外部依赖):**
  - 发布→订阅:同进程同 domain 两个 `DdsTransport`,tx `Send(bytes, Endpoint::Topic("t"))`,rx `Subscribe("t")` → rx `OnBytes` 收到 bytes 且 `from=="t"`。
  - 默认 topic:`Send(bytes)` 发 `default_topic`;订阅该 topic 的 rx 收到。
  - 多 topic 区分:rx 订阅 t1/t2,tx 分别发,rx `OnBytes` 的 `from` 正确区分。
  - `Endpoint::Net` 拒绝 → `config: dds expects topic endpoint`;空 default_topic 的 `Send(bytes)` → `config: no default topic`。
  - `Unsubscribe` 停投。
  - provider 未注册名 → `Open` 返回 `config:`。
- **`FakeDdsProvider` 总线单测:** publish/subscribe/unsubscribe;**domain 隔离**(不同 domain_id 互不可见)。
- **`FastDdsProvider` 互通(仅 `TRANSPORT_HAS_FASTDDS` 时编译运行):** 同 domain 双 participant pub-sub 往返;未装则跳过,构建/全测照常绿。
- **解耦:** DDS 底层测试只用裸字节,不引入 `ICodec`/`Message`。

---

## 9. 文件结构

**新建:**
- `include/transport/dds/DdsConfig.hpp`
- `include/transport/dds/IDdsTransport.hpp`
- `include/transport/dds/IDdsProvider.hpp`
- `include/transport/dds/DdsProviderRegistry.hpp` + `src/dds/DdsProviderRegistry.cpp`
- `include/transport/dds/FakeDdsProvider.hpp` + `src/dds/FakeDdsProvider.cpp`
- `include/transport/dds/DdsTransport.hpp` + `src/dds/DdsTransport.cpp`
- `src/dds/FastDdsProvider.{hpp,cpp}` + `src/dds/FastDdsRawType.{hpp,cpp}`(仅 FastDDS 编入;置于 src/,强耦合不进公共头)
- 测试:`tests/dds/dds_transport_test.cpp`、`tests/dds/fake_dds_provider_test.cpp`、`tests/dds/fastdds_provider_test.cpp`(仅 FastDDS)

**修改:**
- `CMakeLists.txt`:`find_package(fastcdr/fastrtps QUIET)`;探测到则 `TRANSPORT_HAS_FASTDDS` + 编 `FastDds*` + 链接 + 加 FastDDS 测试;DDS 核心源(DdsTransport/registry/Fake)与 Fake 测试**无条件**编入。

---

## 10. 不做什么(YAGNI / 范围外)
- **不做** req-resp(`System` 层用双 topic + correlation_id 搭)。
- **不做** QoS 高级项(partition、deadline、liveliness、内容过滤、ownership)。
- **不做** 发现/传输调优、安全插件。
- **不做** Message/codec/交互模式接入(DDS 底层纯字节;按 topic codec 是 System 的事)。
- **不内置** FastDDS(保持 `find_package` 可选)。

---

## 11. 命名备注
- `DdsTransport` 实现 `IDdsTransport : ITransport` —— 统一 ITransport 用法 + 两个订阅方法。
- `from`(`OnBytes` 第二参)对 DDS = **topic**;对 TCP/UDP = "ip:port";对串口 = 设备路径。语义"来源标识",按 transport 解释。
- `FakeDdsProvider`/`FastDdsProvider`/`DdsProviderRegistry` 沿用 0.1.0 命名(简化重建)。
