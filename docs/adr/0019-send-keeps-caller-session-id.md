# ADR-0019：`ProtocolNode::Send` 不再盖 `session_id`，改由调用方填

**状态：** Accepted
**日期：** 2026-09-11
**关联：** **改变 ADR-0010 所定的 `Send` 盖章规则**（三个 `RequestFor*` 的盖章不变）；ADR-0009 **D1**（`Dispatcher` 按键投递——`session_id` 是键的第一位，故谁来填它决定了应答能否被匹配）；ADR-0018 **D5/D10**（性能测试的服务端形态——本 ADR 正是被它暴露出来的）。

## 背景（Context）

`ProtocolNode` 的公开面只有 `RequestForResponse` / `RequestForResult` / `RequestForResultDirect` / `Send` / `Subscribe`，**没有 `Reply`**。

而 `Send` 当前**无条件覆盖** `session_id`：

```cpp
Coro::Result<void> ProtocolNode::Send(Message msg) {
  if (msg.frm_type == FrameType::kUnknown) msg.frm_type = FrameType::kCommand;
  msg.protocol_id = config_.protocol_id;
  msg.session_id  = NextSession();          // ← 调用方填的被丢弃
  return EncodeAndWrite(msg);
}
```

客户端等应答时，`Dispatcher` 的键是 `(session_id, message_id, frm_type)`（ADR-0009 D1）。**故应答帧必须回带请求的 `session_id` 才会被匹配。** 覆盖之后，`Send` 发不出一个合法的应答帧。

### 后果：外部协议的服务端在本框架里写不出来

想用 `ProtocolNode` 做外部协议**服务端**的宿主，收到 `kCommand` 之后**无法用公开面回应**——只能绕到节点下面，自己 `ICodec::Encode` + `ITransport::AsyncWrite`。仓库里既有的 e2e 测试正是这么绕的（`PeerSession`）。

**框架本身也早已为同一个原因绕开了 `Send`。** `RequestForResult` 末尾那帧自动补发的应答（ADR-0010 D8）就写着：

```cpp
// 不走 Send():它会强制盖新 session_id 与 kCommand。
Message reply = result_msg.value();
reply.frm_type = FrameType::kResponse;
EncodeAndWrite(reply);
```

**一个连自己都要绕开的接口，说明盖章规则定错了地方。**

## 决策（Decision）

- **D1（`Send` 不再触碰 `session_id`）：** 由调用方在 `Message` 里填。`Send` 保留的盖章只剩两项：`protocol_id`（取自节点配置）与 `frm_type`（仅当调用方留 `kUnknown` 时补 `kCommand`）。

  由此 `Send` 成为**唯一一个"调用方完全掌控关联字段"的出站方法**——它本就不登记任何订阅、不参与关联，没有理由替调用方决定关联键。

- **D2（三个 `RequestFor*` 的自增照旧，一个字不改）：** 它们**必须**自己分配 `session_id`——要用它登记订阅、作为唯一关联键。调用方填的会继续被覆盖。

  **自增计数器与回绕语义不变**（`std::uint8_t`，越过 255 回绕，不做冲突检测）。变的只是"谁在用它"：此前四个出站方法都用，现在只有三个请求方法用。

- **D3（`protocol_id` 仍然盖，不放开）：** `ProtocolNodeConfig` 只有一个 `protocol_id`，节点发出的每一帧都该是它。放开它没有已知用例，反而多一处可填错的地方。

  > 服务端回应时需要回带请求的 `protocol_id`——但那与节点自己的配置**本就相同**（同一个节点只服务一个 `protocol_id`），故盖与不盖结果一致。

- **D4（框架内部那处绕行改回走 `Send`）：** `RequestForResult` 的自动补发应答，其绕行理由随 D1 消失一半（`session_id` 不再被盖）。但 `frm_type` 那半仍在——不过调用方（此处即框架自己）已显式设为 `kResponse`，而 `Send` 只在 `kUnknown` 时才补 `kCommand`，故**两半都不再成立**，可以改回 `Send`。

  **这既是简化，也是对 D1 的自检**：框架自己的应答路径若能走公开的 `Send`，就说明宿主也能。

## 明确接受的代价

1. **💥 破坏性：`Send` 出去的 `session_id` 不再自增。** 此前每次 `Send` 都带一个递增值，现在**默认恒为 0**（`Message::session_id` 的默认值），除非调用方填。

   **对纯 noresponse 用法无影响**——`Send` 不登记订阅，发送方从不消费这个字段。但**对端**若按 `session_id` 区分帧（如订阅键用 `FrameOf(session, ...)`，或对端协议要求会话号递增），行为会变。**这类宿主必须自己填。**

2. **默认值 0 是个安静的坑。** 忘了填不会报错，只会让每一帧都带 `session_id = 0`。框架**不校验**、也无从校验——0 是合法值。

   > 不设"未填即报错"：`Message` 是纯数据结构，没有"未设置"这个状态可判；引入 `std::optional<uint8_t>` 会污染贯穿三层的公共类型，代价远大于收益。

3. **盖章规则从此不再整齐。** 原先一句话就能说清（"调用方填 payload 与 message_id，节点盖其余"），现在要按方法分两档：`Send` 只盖两项，三个 `RequestFor*` 盖三项。文档须逐处写清。

## 影响（Consequences）

- **正面：** ① 外部协议的**服务端**首次可以只用公开面写出来，不必绕到 codec+transport 层；② 框架自己那处绕行可以消掉；③ `Send` 的语义更诚实——它不参与关联，就不该替调用方决定关联键。
- **负面（明确接受）：** 见上三条。
- **对测试：** `SessionIdIncrementsAndWrapsAround` 当前测的正是 `Send` 的自增（258 次、验回绕）。该用例须**改指向三个 `RequestFor*`**——自增与回绕语义仍然成立，只是换了载体，不是删掉。另须新增：`Send` 原样透传调用方填的 `session_id`。
- **对 SRS：** `RT_NODE` 里盖章规则的描述按 D1/D2 分档改写。
- **对 SDD：** `CSU_PROTOCOLNODE` 的出站盖章表分档；ADR-0010 D8 那处绕行的设计说明随 D4 更新。
- **对 README：** 「四种交互模式」开头那句"节点盖 `frm_type` / `protocol_id` / `session_id`"须分档；`Message` 字段归属表里 `session_id` 的【框架盖】标注须加限定。
- **对 ADR-0018：** 其 **D5** 所述"`ProtocolNode` 服务端必须绕到节点下面"随本 ADR 失效，性能测试的服务端可以只用公开面；**D10「本轮只加测试代码、不动库」亦被本 ADR 突破**。

## 备选方案（Alternatives considered）

- **给 `ProtocolNode` 加 `Reply(request, response)`。** 语义更明确，还能顺带派生 `frm_type` / `message_id`，调用方更不容易填错。**否决理由：** 它要替调用方决定"应答该长什么样"，而外部协议的应答形态各异（`RequestForResult` 的受理帧与结果帧就不同）；一旦派生规则不合某协议，宿主还是得绕。**放开 `session_id` 是更小、更通用的解**——`Reply` 能做的，填字段 + `Send` 都能做。

  > **2026-09-11 裁决：不加 `Reply`**，也不作为 D1 之外的补充加法。此条**已关闭**，不是悬着的备选——公开面就此定为「填字段 + `Send`」这一条路。
- **维持现状，服务端继续绕到 codec+transport。** **否决理由：** 那等于承认"本框架只能做客户端"。且框架自己都绕不过去（见背景），这是接口的问题，不是用法的问题。
- **`Send` 仅在调用方留 0 时才盖自增值。** 保留旧行为的便利，又允许覆盖。**否决理由：** 0 是合法的 `session_id`，"填 0" 与"没填"不可区分——这会造出一个**永远无法显式发出 `session_id = 0`** 的接口，比默认 0 更坏。
- **把 `protocol_id` 一并放开（D3 的反面）。** **否决理由：** 无已知用例，节点配置里本就只有一个值，放开只多一处可填错的地方。
