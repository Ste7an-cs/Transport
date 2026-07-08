# QtNetwork 传输迁移(协程化第一期)— 设计

**日期：** 2026-07-08
**状态：** 设计中,待用户确认
**背景大局：** 交互层协程原生化的两期迁移之第一期。第二期(协程引擎 + 协程 ProtocolNode,用 AsyncTask)另立 spec。

## 1. 背景与目标

把 **UDP / TCP / 串口** 传输的底层实现从 standalone Asio 换成 **QtNetwork**,为第二期"协程原生引擎(基于 AsyncTask/boost.fiber,跑在 Qt 事件循环线程)"铺路。

**本期目标(第一期):**
- `ITransport` 接口与回调契约**不变**;仅把 5 个具体传输的实现从 asio 换成 QtNetwork,**移除 asio**。
- CMake 接入 Qt5(Core/Network/SerialPort)。
- 传输直测(loopback)改写为 QtNetwork + Qt 事件循环。
- **本期零协程、零 AsyncTask**(那些留给第二期);用现有异步栈即可验收新传输。

**本期不做(范围外):**
- 协程引擎、协程节点、AsyncTask 集成(第二期)。
- DDS 及异步 `InteractionEngine`/`IExecutor`/`ThreadExecutor`/`ProtocolNode`/`DdsNode` —— 一律不动(两引擎并存:异步栈继续服务 DDS)。
- codec、`Message`、`Endpoint`、`ProtocolPolicy` —— 不动。

## 2. 关键决策

| 决策 | 选择 |
|---|---|
| Qt 版本 | **Qt5**(系统 5.15.3;对齐 AsyncTask ≥5.12)。组件 Core/Network/SerialPort |
| **前置依赖** | **QtSerialPort 未安装** → 串口传输前需 `sudo apt install libqt5serialport5-dev`(见 §7 风险) |
| 线程模型 | 传输**不自持 io 线程**,活在**宿主 Qt 事件循环线程**上;`OnBytes`/`OnConnect`/`OnDisconnect` 经 Qt 信号在该线程触发。**宿主须运行 Qt 事件循环**(测试里用 `QCoreApplication`+事件泵提供) |
| Qt 对象连接 | 用 **functor `connect`**(lambda 捕获 `this`),传输类**不加 `Q_OBJECT`** → 无需 moc/AUTOMOC |
| 生命周期 | socket 作为传输成员(`std::unique_ptr<Q*>`),析构即断开所有 `connect` → `this` 捕获安全,**去掉 asio 版的 `shared_from_this`/`weak_ptr` 内部纪律**(简化) |
| asio | 仅存在于这 5 个传输文件 → 迁移后整仓不再用 asio(vendored asio 可另行清理,本期可留) |

**接口契约不变、语义微调(写进 ITransport 注释):** `OnBytes` 从"传输自有 io 线程"改为"**宿主 Qt 事件循环线程**"串行触发;"非阻塞、不可在回调内 Close 本对象"等约束照旧。报文/流式语义照旧(UDP 一 datagram 一次回调;TCP/串口一次 read 切片一次回调)。

## 3. 各传输的 QtNetwork 映射

所有实现都**大幅变短**(Qt 事件循环替代 io_context/strand/work-guard;信号替代 async_read 循环;QTcpSocket 自带写缓冲替代手写写队列)。

### 3.1 `UdpTransport`(`QUdpSocket`)
- **Open**:`new QUdpSocket`;按 `UdpMode` 绑定——单播/广播 `bind(local_addr, local_port)`;组播 `bind(AnyIPv4, local_port, ShareAddress|ReuseAddressHint)` + `joinMulticastGroup(group)`,设 `setMulticastLoopbackOption`/TTL。解析默认目的地(`remote_addr:remote_port` 或组播组)。`connect(sock, &QUdpSocket::readyRead, [this]{ onReadyRead(); })`。回 `OnConnect`(UDP 无连接,Open 成功即视作已连)。失败返回 `config:`。
- **onReadyRead**:`while (sock->hasPendingDatagrams()) { QNetworkDatagram dg = sock->receiveDatagram(); OnBytes(bytes(dg.data()), dg.senderAddress()+":"+senderPort()); }` —— 一 datagram 一次回调,保边界。
- **Send(bytes)**→`writeDatagram(bytes, default_dest_addr, default_dest_port)`;**Send(bytes, Net)**→按该地址;**Topic**→`config: udp expects net endpoint`。单包发失败经 `OnBytes` 投 `Fail`(不致命)。
- **Close**:断连接、`close()`、置 open_=false。

### 3.2 `TcpConnection`(`QTcpSocket`)
- 包一个**已连接**的 `QTcpSocket`(client connect 得来 / server accept 得来)。
- **Open**:记 peer `"ip:port"`;`connect(readyRead → OnBytes(readAll(), peer))`;`connect(disconnected / errorOccurred → HandleDisconnect once)`;回 `OnConnect`。
- **Send**→`sock->write(bytes)`(QTcpSocket 内部有写缓冲,**无需手写写队列**)。
- **断连语义照旧**:主动 `Close` 置一次性闸后再 `abort()`/`close()` → 不回 `OnDisconnect`;真实断开(disconnected/error)回一次。
- 供 client 与 server 复用。

### 3.3 `TcpClientTransport`(`QTcpSocket`)
- 持一个 `QTcpSocket`(它本身即"连接",无需 asio 那样单独 accept)。可内部复用 `TcpConnection` 的读写,或直接内联。
- **Open**:`connectToHost(host, port)`;`connect(connected → OnConnect + 起读)`;**连接超时** = `QTimer`(到点 `abort()` → 连接失败,报 `timeout:`);**auto_reconnect** = `disconnected → QTimer 指数退避(backoff_base ×2 封顶 backoff_cap)→ 重连`。`connect_timeout_ms`/`auto_reconnect`/退避沿用现 `TcpClientConfig`。
- **Send**→`write`。**Close**→停定时器、`abort()`。

### 3.4 `TcpServerTransport`(`QTcpServer`)——接受器,非 ITransport(照旧)
- 持 `QTcpServer`。**Open**:`listen(QHostAddress(bind_addr), port)`;`connect(newConnection → while (hasPendingConnections()) { QTcpSocket* s = nextPendingConnection(); auto conn = make_shared<TcpConnection>(s); OnAccept(conn); conn->Open(); })`。`LocalPort()`→`serverPort()`。
- **backlog**:Qt 用 `setMaxPendingConnections(n)`(语义≈待接受队列上限,非 listen backlog);`backlog<=0` 用 Qt 默认(30)。**注明与 asio 的语义差异**。
- `conns_` 仍存 weak(仅供 Close 通知),`OnError` 上报监听错误。

### 3.5 `SerialTransport`(`QSerialPort`)
- 持 `QSerialPort`。**Open**:`setPortName(device)`;`setBaudRate/​setDataBits(5-8)/​setStopBits(1|2)/​setParity(N/E/O)/​setFlowControl(None)`;`open(ReadWrite)`。逐项失败关端口、返回 `config:`。`connect(readyRead → OnBytes(readAll(), device))`;`connect(errorOccurred → HandleDisconnect)`。
- **Send**→`write`。**Close**→`close()`。字节流语义,切帧归上层 codec。

## 4. `ITransport` 契约(接口不变,补线程注释)
- 方法签名、`BytesCallback`、`Endpoint` 寻址语义**全不变**。
- 注释补:回调在**宿主 Qt 事件循环线程**串行触发;传输须在有 Qt 事件循环的线程上创建与使用(Qt 对象线程亲和)。

## 5. 构建(CMake)
- `find_package(Qt5 5.12 REQUIRED COMPONENTS Core Network SerialPort)`。
- transport 库 `target_link_libraries(... Qt5::Core Qt5::Network Qt5::SerialPort)`。
- 移除传输源文件对 asio 的 include/链接;`TcpServerConfig.hpp` 里"asio 默认 backlog"注释改为 Qt 说法。
- 传输类无 `Q_OBJECT` → **不需 AUTOMOC**(functor connect)。
- 测试目标同样链接 Qt。

## 6. 测试(GoogleTest + Qt 事件泵)
- 改写:`udp_transport_test` / `tcp_transport_test` / `tcp_connection_test` / `tcp_server_test` / `serial_transport_test` / `combination_smoke_test`。
- **自定义 test main**:`QCoreApplication app(argc,argv); InitGoogleTest(&argc,argv); return RUN_ALL_TESTS();`(取代 `GTest::gtest_main`——异步 I/O 需事件循环)。
- **事件泵助手** `pumpUntil(pred, timeout_ms)`:循环 `app.processEvents()` 直到 `pred()` 为真或超时,供断言"字节已到达/已连接/已断开"。
- 用例覆盖:UDP 单播回环收发、TCP client↔server 回环收发 + 断连、串口(见 §7 风险)、寻址错误(`Topic` → config)、组播/广播(best-effort)。

## 7. 风险与待办
1. **QtSerialPort 未装** → 实现前 `sudo apt install libqt5serialport5-dev`,否则串口传输不能编译。**阻塞项。**
2. **串口测试**:现测试用 `openpty` 造虚拟串口对。`QSerialPort` 在 pty 上设波特率可能失败(pty 非真实 tty)。缓解:串口测试改为"open/配置/错误路径 + best-effort 回环(socat pty 对)",或标注为环境相关跳过。实现期定夺。
3. **TcpServer backlog 语义**:asio listen backlog vs Qt `setMaxPendingConnections`,语义不同,注释说明,行为对齐"默认即可"。
4. **破坏性**:整仓构建从此需 Qt5;所有传输不再自持线程 → 用它们的宿主须运行 Qt 事件循环(含现有异步 `ProtocolNode` 走这些传输时)。下一个版本按破坏性(0.3.0)。

## 8. 约束
- C++17,不抛异常,`Result`/`Status` 前缀分类不变。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,无 Co-Authored-By。不提交 `build/`。
- 文档(SRS/SDD/README/CHANGELOG)同步留到实现后。
