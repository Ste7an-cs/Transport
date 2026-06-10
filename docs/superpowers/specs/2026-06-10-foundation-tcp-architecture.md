# Foundation + TCP 整体设计与依赖关系（as-built）

> 本文用 UML 说明**已实现部分**（Foundation 层 + TCP 传输）的真实类结构与运行期协作，是对主 spec §3.4 框架级视图的「落地详图」。命名遵循约定：`I*` = 接口，`*Impl` = 具体实现。

---

## 1. 结构依赖（UML 类图）

```mermaid
classDiagram
  direction TB

  %% ===== Foundation 核心 =====
  class Result~T~ {
    +T value
    +bool ok
    +string error
    +Success(T)$
    +Fail(string)$
  }
  class Message {
    +vector~uint8~ payload
    +string topic
    +string source
    +int64 timestamp
  }
  class ICodec {
    <<interface>>
    +Encode(bytes) Result
    +Decode(bytes) Result
  }
  class IFramer {
    <<interface>>
    +TryExtract(buf,len) Result~FrameResult~
  }
  class LengthFieldFramer {
    +ValidateConfig(cfg)$ Status
    +TryExtract(buf,len) Result
  }
  class FrameAssembler {
    +Feed(data,len) Result~Frames~
    -shared_ptr~IFramer~ framer_
  }
  class ReceiveQueue {
    +Push(msg)
    +Receive(timeout) Result~Message~
    +SetCallback(cb) Status
    +AsyncReceive() future
    +Close()
  }
  class ITransport {
    <<interface>>
    +Open() +Close() +IsOpen()
    +Send(bytes)
    +Receive() +OnReceive() +AsyncReceive()
    +OnDisconnect() +SetCodec()
  }
  class TransportBase {
    #shared_ptr~ICodec~ codec_
    #ReceiveQueue queue_
    #EncodeForSend(bytes) Result
    #DeliverFrame(frame,source,topic)
    #DeliverError(err)
    #NotifyDisconnect(reason)
    #CloseQueue()
  }

  IFramer <|.. LengthFieldFramer : 实现
  ITransport <|.. TransportBase : 实现接收侧
  FrameAssembler o-- IFramer : 持有 shared_ptr
  TransportBase *-- ReceiveQueue : 拥有
  TransportBase o-- ICodec : 持有 shared_ptr
  TransportBase ..> Message : 产出
  ICodec ..> Result : 返回
  IFramer ..> Result : 返回

  %% ===== TCP 传输 =====
  class ITcpServer {
    <<interface>>
    +OnNewConnection(cb)
    +GetClients()
    +DisconnectClient(id)
  }
  class TcpConnectionImpl {
    +Open() +Close() +Send()
    #StartRead() #DoWrite()
    #HandleDisconnect(reason)
    -FrameAssembler assembler_
    -tcp_socket socket_
  }
  class TcpClientImpl {
    +Open() +Close()
    -StartConnect() -ScheduleReconnect()
    -io_context+thread (自有)
  }
  class IoContextHolder {
    +io_context ctx
  }
  class TcpServerImpl {
    +Open() +Close()
    +Send() 广播
    +OnNewConnection() +GetClients() +DisconnectClient()
    -acceptor + io_context+thread
  }
  class TcpClientConfig {
    +host +port
    +connect_timeout_ms +auto_reconnect
    +optional~framer~
  }
  class TcpServerConfig {
    +bind_addr +port +max_clients
    +optional~framer~
  }

  ITransport <|-- ITcpServer : 扩展
  TransportBase <|-- TcpConnectionImpl : 继承（复用接收侧）
  TcpConnectionImpl <|-- TcpClientImpl : 继承
  IoContextHolder <|-- TcpClientImpl : 首基类(构造序)
  ITcpServer <|.. TcpServerImpl : 实现
  TcpConnectionImpl *-- FrameAssembler : 拥有(接收侧分帧)
  TcpServerImpl o-- TcpConnectionImpl : clients map(每客户端)
  TcpClientImpl ..> TcpClientConfig : 使用
  TcpServerImpl ..> TcpServerConfig : 使用
  TcpConnectionImpl ..> Message : DeliverFrame
```

---

## 2. 依赖关系说明

### 2.1 Foundation 内部

- **`Result<T>` / `Status`**：所有可能失败的操作的返回值，框架不抛异常。被 `ICodec` / `IFramer` / `ITransport` / `ReceiveQueue` 普遍依赖（图中只画了 codec/framer 的 `..> Result` 以免线太密）。
- **`ICodec`（接口）**：发送/接收边界的编解码扩展点，由**用户实现**。`TransportBase` 以 `shared_ptr<ICodec>` 持有它（聚合 `o--`），发送前 `Encode`、接收后 `Decode`。
- **`IFramer`（接口）← `LengthFieldFramer`**：流式分帧扩展点。`LengthFieldFramer` 是内置实现（长度字段协议）。
- **`FrameAssembler`**：滚动缓冲 + 持有一个 `shared_ptr<IFramer>`（聚合），把字节流循环切成整帧；framer 为空则透传。**仅流式传输接收侧需要**。
- **`ReceiveQueue`**：FIFO + 同步/回调/future 三模式互斥交付。被 `TransportBase` **组合拥有**（`*--`，值成员）。
- **`ITransport`（接口）← `TransportBase`**：`TransportBase` 实现 `ITransport` 的「接收侧 + 编解码」通用部分（`Receive/OnReceive/AsyncReceive/SetCodec/OnDisconnect`），并给子类 protected 工具（`EncodeForSend/DeliverFrame/DeliverError/NotifyDisconnect/CloseQueue`）；把 `Open/Close/IsOpen/Send` 留给子类。产出 `Message` 投入 `queue_`。

### 2.2 TCP 对 Foundation 的依赖

- **`ITcpServer` ──▷ `ITransport`**：服务端扩展接口（加 `OnNewConnection/GetClients/DisconnectClient`）。
- **`TcpConnectionImpl` ──▷ `TransportBase`**：已连接 socket 的收发实现，**继承**复用接收侧；**拥有** `FrameAssembler`（`*--`）做接收侧分帧；收到帧调 `DeliverFrame` → `queue_`。client 与 server-accepted 连接共用它。
- **`TcpClientImpl` ──▷ `TcpConnectionImpl`（+ `IoContextHolder`）**：客户端 = 连接实现 + connect/超时/指数退避重连，自有 `io_context`+线程；首基类 `IoContextHolder` 保证 `io_context` 先于 `TcpConnectionImpl` 的 socket 构造。
- **`TcpServerImpl` ⋯▷ `ITcpServer`**：实现服务端接口；**聚合**多个 `TcpConnectionImpl`（`clients_` map，每客户端一个独立 `ITransport`）；**不经 `TransportBase`**（只 accept + 广播，自身不维护接收队列）。
- **`TcpClientConfig` / `TcpServerConfig`**：含 `std::optional<LengthFieldFramerConfig> framer`——决定连接是否启用分帧。

### 2.3 依赖方向（原则）

```mermaid
flowchart LR
  TCP["TCP 实现（*Impl）"] -->|依赖| FND["Foundation（接口 + 通用件）"]
  TCP -.->|仅实现层| asio["Asio"]
  FND -.->|零第三方依赖| none["（无）"]
```

- **依赖倒置**：实现依赖接口，不反向。`TcpServerImpl` 面向 `ITcpServer`/`ITransport`，应用层只见接口。
- **Foundation 零第三方依赖、不依赖 TCP**；**TCP 依赖 Foundation**；**Asio 只出现在 TCP 实现层**（`*Impl.cpp`），头文件 `ITcpServer.hpp` 等不含 Asio。
- 单向无环：`应用层 → 接口 → 实现 → 底层库`。

---

## 3. 运行期协作（UML 时序图：TCP 接收一帧）

```mermaid
sequenceDiagram
  autonumber
  participant Sock as asio socket（io 线程）
  participant Conn as TcpConnectionImpl
  participant FA as FrameAssembler
  participant FR as LengthFieldFramer
  participant Base as TransportBase
  participant Q as ReceiveQueue
  participant App as 应用线程

  Sock->>Conn: async_read_some 完成 → OnRead(bytes)
  Conn->>FA: Feed(bytes)
  loop 缓冲区中每一帧
    FA->>FR: TryExtract(buf, len)
    FR-->>FA: Result<FrameResult>(consumed, has_frame)
  end
  FA-->>Conn: frames[]（0..N 个完整帧）
  loop 每个完整帧
    Conn->>Base: DeliverFrame(frame, source, "")
    alt 已设 ICodec
      Base->>Base: Decode(frame)
    end
    Base->>Q: Push(Result<Message>)
  end
  Conn->>Sock: 再投递 async_read_some

  App->>Q: Receive(timeout) / OnReceive / AsyncReceive
  Q-->>App: Result<Message>
```

要点：分帧（`FrameAssembler`+`IFramer`）与交付（`TransportBase`+`ReceiveQueue`）解耦；I/O 线程只负责切帧+入队，应用线程按三模式之一出队。UDP/DDS 报文式传输将跳过 `FrameAssembler`/`IFramer`，直接 `DeliverFrame`（见主 spec §14.3）。

---

> 框架级（含规划中的 UDP/DDS/串口/工厂）视图见主 spec `2026-06-09-transport-middleware-design.md` §3.1 / §3.4。
