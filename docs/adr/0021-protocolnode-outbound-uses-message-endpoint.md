# ADR-0021：`ProtocolNode` 出站目的地取自 `Message::endpoint`，解开 UDP 一对多

**状态：** Accepted
**日期：** 2026-09-11
**关联：** **补齐 ADR-0020 D3 的出站半边**（该 D3 定义了"发送时是目的地"，却只规定了入站填充）；ADR-0003 **D12**（UDP 写侧 `kDefault`/`kNet` 的解析规则——本 ADR 使其对 node 层可达）；ADR-0011 **D8** / ADR-0012 **D9**（TCP / 串口写泵一律忽略 `peer`——故本 ADR 对它们是空操作）；ADR-0009 **D1**（关联键不含对端——见「明确接受的代价」1）。

## 背景（Context）

`UdpTransport` 的写泵**早就支持按报文指定目的地**：

```cpp
//   kDefault → config 默认对端;  kNet → 按 ip:port(ADR-0003 D12)
if (unit.peer.kind == Endpoint::Kind::kDefault) { …配置默认对端… }
else if (unit.peer.kind == Endpoint::Kind::kNet) { …按 ip:port… }
```

但 `ProtocolNode::EncodeAndWrite` **把目的地写死成 `Endpoint::Default()`**：

```cpp
// 目的地恒填 `Endpoint::Default()`,交给传输解析成它自己配置的默认对端:本类传输无关,
// 不知道也不该知道对端是 ip:port 还是 topic。
transport_.AsyncWrite({std::move(bytes), Endpoint::Default()});
```

**该理由在 ADR-0020 之前是成立的**——那时 `Message` 的发送侧根本没有目的地字段可传，节点确实无从表达"发给谁"。

ADR-0020 加了 `Message::endpoint` 并定义它"**发送时是目的地**"，但其 **D3 只规定了入站填充**（"收包时开始填它"），**出站一个字没提**。于是字段有了、传输层能力有了，中间这一段仍然断着——**`ProtocolNode` 是 UDP 一对多的唯一堵点**。

## 决策（Decision）

- **D1（出站目的地取 `msg.endpoint`）：**

  ```cpp
  transport_.AsyncWrite({std::move(bytes), msg.endpoint});
  ```

  **`Endpoint` 正是"传输无关地表达对端"的那个类型**——原注释说"不知道也不该知道对端是 ip:port 还是 topic"，而 `Endpoint` 的全部意义就是让节点**不必知道**却仍能**原样转交**。节点依旧不解释它，只是不再丢弃它。

- **D2（向后兼容，不设开关）：** `Message::endpoint` 的默认值是 `Endpoint::Default()`，故**不填的调用方得到与今天逐字相同的行为**。不需要配置项、不需要迁移期。

  | 介质 | 本 ADR 之后 |
  |---|---|
  | **UDP** | `kDefault` → 配置的默认对端（同今天）；**`kNet` → 发往该 ip:port（新能力）** |
  | TCP / 串口 | **无变化**——写泵本就忽略 `peer`（ADR-0011 D8 / ADR-0012 D9） |

- **D3（四个出站方法一视同仁）：** `Send` 与三个 `RequestFor*` 都经 `EncodeAndWrite`，故都取 `msg.endpoint`。**不为请求-响应另设规则**——"发给谁"与"期不期待应答"是正交的两件事。

- **D4（外部协议服务端由此在 UDP 上真正可用）：** ADR-0019 让 `Send` 透传 `session_id`，服务端得以用公开面回出合法应答帧；但在 UDP 上那帧此前**只会发往配置的默认对端**，回不到真正的请求方。合起来才闭环：

  ```cpp
  Message rsp;
  rsp.frm_type   = FrameType::kResponse;
  rsp.session_id = req.session_id;      // ADR-0019:透传
  rsp.message_id = req.message_id;
  rsp.endpoint   = req.endpoint;        // ★ 本 ADR:回到请求方
  (void)node.Send(std::move(rsp));
  ```

  **这是本 ADR 最实际的用途**，README 的服务端示例须补上那一行。

## 明确接受的代价

1. **⚠ 请求-响应的关联键不含对端。** `MessageDispatcher` 的键是 `(session_id, message_id, frm_type)`（ADR-0009 **D1**），**没有对端这一维**。一对多之下，理论上 peer B 的应答可以终结一条发往 peer A 的请求。

   **实际风险有限，但不为零：**
   - `session_id` 是**节点全局**的自增计数器，并非按对端分配。故并发发往 A 与 B 的两条请求天然拿到不同的 `session_id`，**正常对端不会撞**。
   - 真正的风险是**行为异常或恶意的对端**用了别人的 `session_id` 作答。**框架无法防御**——除非把对端纳入键。

   **本轮不改键。** 纳入对端需要给 `Dispatcher` 加第四个键字段，会改动全部订阅语义；而它挡住的是"对端说谎"这类协议层信任问题，框架本就不承诺防御。**调用方若在不可信的一对多网络上跑请求-响应，须自行校验应答的来源**（`rsp.endpoint` 现在拿得到——这也正是 ADR-0020 D3 入站填充的价值）。

2. **转发一条收到的 `Message` 时，目的地会变成原发送方。** 取一条入站消息、改改 payload 再 `Send`，此前发往配置的默认对端，此后**发回原发送方**（因为 `endpoint` 还带着来源）。这与"`endpoint` 发送时即目的地"一致，但**对既有代码是行为改变**。要发往别处就显式改写该字段。

3. **UDP 之外三种介质拿不到任何新能力。** TCP 点对点、串口单设备，写泵本就忽略 `peer`；DDS 不走 `ProtocolNode`。本 ADR 的收益**只在 UDP 上兑现**。

## 影响（Consequences）

- **正面：** ① UDP 一对多首次可用——传输层的能力终于对 node 层可达；② 外部协议服务端在 UDP 上闭环（与 ADR-0019 合起来）；③ 删掉一处"字段存在却被丢弃"的不一致。
- **负面（明确接受）：** 见上三条。
- **对 ADR-0020：** 其 **D3** 加补正，指明出站半边由本 ADR 补齐。
- **对 SRS：** `RT_TRANSPORT_006`（UDP 多对端通过网络地址区分）此前在 node 层无从兑现，现补一句说明经 `Message::endpoint` 表达。
- **对 SDD：** `CSU_PROTOCOLNODE` 的出站路径说明改写。
- **对 README：** 「写一个外部协议服务端」补 `rsp.endpoint = req.endpoint` 那一行并说明为何必需；UDP 一节补一对多的收发写法。

## 备选方案（Alternatives considered）

- **维持现状，一对多由宿主自建多个 `ProtocolNode`（每对端一个 UDP 传输）。** **否决理由：** 每个对端一条 socket、一份节点状态，而 UDP 本就无连接；且 `RT_TRANSPORT_006` 明写"UDP 多对端通过网络地址区分"，逐对端建节点是在绕开该需求而非满足它。
- **给 `ProtocolNode` 加 `SendTo(endpoint, msg)` 之类的重载。** 显式、不改既有语义。**否决理由：** `Message` 里已经有一个语义正确的字段，再加一条平行的传参路径会造出"两处都能指定目的地、冲突时听谁的"的问题。**字段已经在那儿，用它就是了。**
- **把对端纳入 `Dispatcher` 的关联键。** 可根除代价 1。**否决理由：** 需要第四个键字段、改动全部订阅语义，而它防的是"对端说谎"这类协议层信任问题；框架不承诺防御，且 `rsp.endpoint` 已让调用方**自行校验**成为可能。**若将来一对多的请求-响应成为主力用法，应当重开此议。**
