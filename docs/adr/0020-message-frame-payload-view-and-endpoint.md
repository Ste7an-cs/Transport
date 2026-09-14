# ADR-0020：`Message` 增加 `frame`、`payload` 接收时为其视图、`topic`+`source` 合并为 `endpoint`

**状态：** Accepted
**日期：** 2026-09-11
**关联：** 改变 `Message` 的字段构成与所有权语义（贯穿三层的公共类型）；ADR-0009 **D1**（`Dispatcher` 按键投递——`DdsNode` 的键第一位取自 `topic`，本 ADR 改为 `endpoint.topic`）；ADR-0013 **D6**（服务名与 topic 刻意区分——本 ADR 以 `Endpoint::kService` 在类型上保住该区分）；ADR-0014（框架无观测面——`frame` 使宿主首次能拿到原始整帧，是排障的替代手段）。

## 背景（Context）

`Message` 当前的两处形状带来限制：

1. **接收方拿不到原始帧。** `Decode` 只把 `payload` 拷出来，帧头、`session_id` 之外的线缆细节、CRC 全部丢弃。宿主无法做原样透传转发、无法按原字节重发、排障时也看不到线上到底是什么（框架又无观测面，ADR-0014）。
2. **`topic` 与 `source` 语义重叠。** 前者是"操作/通道名"，后者是"来源标识"，而 `DdsNode` 收包时两个字段**填的是同一个值**（`msg.source = msg.topic = datagram.peer.topic`）。同时 `ProtocolNode` **根本不填任何一个**——UDP 收到的报文，业务层看不到发送方是谁。

传输层的 `Datagram::peer` 早已给出了正确形状：**同一个 `Endpoint` 字段，读时是发送方、写时是目的地**（ADR-0008 D8）。`Message` 应与之对齐。

### 一处必须先认清的事实：本 ADR **不省内存，反而多拷**

- **现状**：`Decode` 把 payload 拷出来——**一次**。
- **改后**：把**整帧**（帧头 + CRC + payload）拷进 `frame`——**一次，且比 payload 大**；payload 做视图——**零次**。

净效果是**多拷了帧头与 CRC 那些字节**。且流式 codec 的帧躺在滚动缓冲里、缓冲会被消费与压缩，**那一次拷贝无法避免**。

**故本 ADR 买到的是"接收方能拿到原始整帧"，以及"payload 不再产生第二次拷贝"**——不是内存总量的节省。2026-09-11 裁决：**两者都要**。

## 决策（Decision）

- **D1（`payload` 与 `frame` 均改为 `QByteArray`）：** `std::vector<std::uint8_t>` **拥有**内存，无法指向别处；C++17 **没有 `std::span`**。而 `QByteArray` 隐式共享 + `fromRawData` 两者兼备，是本目标在 C++17 下**唯一**可行的载体。

  ```cpp
  struct Message {
    QByteArray frame;      ///< 接收：完整一帧（帧头 → payload 末）。发送：空，Encode 忽略。
    QByteArray payload;    ///< 发送：用户数据（拥有）。接收：指进 frame 的视图（不拥有）。
    Endpoint   endpoint;   ///< 发送：目的地。接收：来源。
    // …其余字段不变
  };
  ```

  `Message.hpp` 由此 `#include <QByteArray>`——**Qt 进入公共核心类型**。库本就 PUBLIC 依赖 Qt5::Core，故不新增依赖，但公共头的耦合面确实扩大了。

- **D2（接收时 `payload` 由 `fromRawData` 指进 `frame`）：**

  ```cpp
  msg.frame   = QByteArray(reinterpret_cast<const char*>(frame_begin), frame_len);
  msg.payload = QByteArray::fromRawData(msg.frame.constData() + payload_offset, payload_len);
  ```

  **拷贝安全性已论证**：`QByteArray` 的数据块在堆上，拷贝 `frame` 只是引用计数加一、**块地址不变**；移动 `frame` 只转移 d 指针、块地址同样不变。故 `Message` 被拷贝（`Dispatcher` 给每个订阅者各一份）或被移动（`std::vector` 扩容、入队出队）时，`payload` 视图**始终指向同一块仍被引用的内存**。

  **前提是 `payload` 必须在 `frame` 落到其最终 `QByteArray` 之后才建立**——codec 实现须遵守。

- **D3（`topic` 与 `source` 合并为 `endpoint`）：** 语义与 `Datagram::peer` **逐字相同**：发送时是目的地，接收时是来源。两个旧字段删除。

  **`ProtocolNode` 收包时开始填它**（`msg.endpoint = datagram.peer`）——这是白拿的改进：UDP 的发送方地址首次对业务层可见。

- **D4（`Dispatcher` 的键取 `endpoint.topic`）：** `DdsNode` 的键提取由 `msg.topic` 改为 `msg.endpoint.topic`，其余两位不变。**不给 `Endpoint` 加 `operator==` 与 `std::hash`**——键只需要那个字符串，为此让整个 `Endpoint` 可哈希是多余的负担。

- **D5（`Endpoint` 增加 `kService`）：** 请求-响应的**服务名不是 topic**——它派生出 `cfg.<名>.request` 与 `cfg.<名>.response` 两个 topic（ADR-0013 **D6**）。塞进 `kTopic` 会把 D6 刻意分开的两个概念混回去。

  ```cpp
  enum class Kind { kDefault, kNet, kTopic, kService };
  static Endpoint Service(std::string name);   // 名字存在既有的 topic 字段里
  ```

  **名字复用既有的 `topic` 字段**，由 `kind` 区分——不为此把结构体加宽到第五个字段。

- **D6（`DdsNode` 只有"收 `Message` 的方法"去掉寻址参数）：**

  | 方法 | 变化 |
  |---|---|
  | `Publish(topic, msg)` | → **`Publish(msg)`**，topic 取自 `msg.endpoint`（须 `kTopic`，否则 `kInvalidArgument`） |
  | `RequestForResultDirect(服务名, req, retry)` | → **`RequestForResultDirect(req, retry)`**，服务名取自 `req.endpoint`（须 `kService`） |
  | `Reply(request, result)` | 签名不变；内部反查由 `request.topic` 改为 `request.endpoint.topic` |
  | `ServeRequests(服务名)` / `Subscribe(TopicKey, KindKey)` | **保留参数不变** |

  **后两者保留是有理由的**：它们**不接收 `Message`**，没有 `endpoint` 可取；`ServeRequests` 返回的是 `Ticket`，`Subscribe` 的第一参是**订阅键**（可为 `kAny`）而非寻址目的地。

- **D7（`Encode` 完全忽略 `frame`）：** 发送路径只读 `payload`。转发一条收到的 `Message` 时，`frame` 非空但不参与编码——重新编码出的帧由当前字段生成。

  > **不做"`frame` 非空则原样重发"**：那会让同一个 `Send` 有两种行为，且与"改了字段却不生效"的直觉冲突。按原字节重发是另一件事，若要做应另设显式接口。

- **D8（`Message::OwnedPayload()`——唯一的逃生口，非侵入式）：**

  ```cpp
  /// @brief 返回 payload 的【拥有型】深拷贝，供需要活得比本 `Message` 久的消费者使用。
  [[nodiscard]] QByteArray OwnedPayload() const {
    return QByteArray(payload.constData(), payload.size());
  }
  ```

  **恒为深拷贝，不做"已是拥有型就直接返回"的优化。** 那种优化要判别"这个 `QByteArray` 是不是 `fromRawData` 出来的"，而 Qt5 **没有公开 API 可判**；退而用 `frame.isEmpty()` 作判据则依赖一条可被违反的不变量（调用方自行构造的 `Message` 可以两者都填）。**正确性优先**——本方法只在调用方显式索要所有权时才被调用，那一次拷贝是它自己要的。

  **判据只有一句：只在需要让 payload 活得比 `Message` 久时才调。**

  | 场景 | 用法 |
  |---|---|
  | 在 `Message` 存活的作用域内用完 | **直接用 `msg.payload`**，零拷贝 |
  | 存进容器 / 成员变量、待会儿再处理 | `OwnedPayload()` |
  | 交给别的线程或 fiber | `OwnedPayload()` |
  | 从函数里把 payload 返回出去 | `OwnedPayload()` |

  > **⚠ 这条判据必须写进文档并强调。** 若调用方"保险起见"处处调 `OwnedPayload()`，零拷贝收益**全部消失**，而帧头与 CRC 的额外拷贝还在——**那就比不做本 ADR 更差**。判据是"需要活得更久才调"，**不是"不确定就调"**。

## 明确接受的代价

1. **⚠ `payload` 悬垂是静默的内存错误。** 视图只在**其所属 `Message` 存活、且 `frame` 未被改写**期间有效。以下都会让它失效，且**不会报错**：

   | 动作 | 后果 |
   |---|---|
   | `msg.frame.clear()` 或对 `frame` 写入（触发 COW 分离） | 原块可能被释放 → payload 悬垂 |
   | 把 `payload` 拷出来单独保存，让 `Message` 析构 | 同上 |
   | 跨线程/跨 fiber 传 `payload` 而不带 `Message` | 同上 |

   **缓解见 D8**；`payload` 的 Doxygen 加 `@warning`。**框架不校验、也无从校验。**

   > 对 `payload` **写入**是安全的——`fromRawData` 的 `QByteArray` 在非 const 访问时会自行深拷贝转为拥有型。危险只在读侧。

2. **💥 破坏性：`payload` 的类型由 `std::vector<std::uint8_t>` 变为 `QByteArray`。** 全仓 99 处使用点（产品 13、测试 86），其中约 23 处是 `payload = {0x01, 0x02}` 这类花括号初始化——`QByteArray` 在 Qt5 **没有 `initializer_list` 构造**，这些都要改写。**宿主代码同样要改。**

3. **💥 破坏性：`topic` 与 `source` 两个字段删除。** 读写它们的宿主代码必须改用 `endpoint`。

4. **💥 破坏性：`DdsNode::Publish` 与 `RequestForResultDirect` 的签名变化。**

5. **`Message.hpp` 引入 `<QByteArray>`。** 任何 include 该头的翻译单元自此拉进 Qt 头。库本就依赖 Qt5，但公共核心类型此前是纯标准库的。

6. **`ICodec` 的契约扩大。** 每个自定义 codec 的 `Decode` 都要填 `frame` 并正确建立 payload 视图。**本 ADR 定为必填**——选填会让"接收方能拿到原始帧"这条承诺时有时无，比不做更坏。README 的自定义 codec 一节须补这条纪律。

## 影响（Consequences）

- **正面：** ① 接收方首次能拿到原始整帧（透传转发、按原字节重发、排障——后者在框架无观测面之后尤其有用）；② payload 不再产生第二次拷贝；③ `endpoint` 与 `Datagram::peer` 语义对齐，两层用同一套寻址概念；④ `ProtocolNode` 的入站来源地址首次可见。
- **负面（明确接受）：** 见上六条，其中 **代价 1 是本 ADR 唯一的内存安全风险**。
- **对 SRS：** `RT_DATA_MESSAGE` 的字段构成与所有权语义改写；新增"payload 视图的有效期"约束。
- **对 SDD：** `Message` 的数据设计、`ICodec` 契约、`DdsNode` 的键提取与三个方法签名、`ProtocolNode` 的入站填充。
- **对 README：** `Message` 字段归属表整体改写；**新增 `payload` 有效期与 `OwnedPayload()` 的判据表（D8）**——这是宿主最容易用错的一处；自定义 codec 一节补 `frame` 填充纪律与视图建立顺序；DDS 一节的调用示例去掉 topic 参数。
- **对 CONTEXT.md：** 「逻辑消息」词条补 `frame`/`endpoint`；新增「payload 视图」词条。

## 备选方案（Alternatives considered）

- **`payload` 存 `offset + size`，取用时由访问器算出视图。** **拷贝绝对安全**（偏移是值语义），没有悬垂风险。**否决理由：** 发送时 payload 没有 `frame` 可依附，仍需一个拥有型字段 → 同一语义两种表示，接口更乱。**2026-09-11 裁决取安全性较低但形状统一的做法甲。**
- **`payload` 恒为拥有型，接收时 `frame.mid(off, len)`。** 简单、安全。**否决理由：** Qt5 的 `mid()` 对子区间是**深拷贝**——那就只剩"拿到原始帧"，"省掉第二次拷贝"落空。裁决为**两者都要**。
- **`payload` 保持 `std::vector<std::uint8_t>`。** 不动 99 处使用点。**否决理由：** vector 拥有内存、无法指向 `frame`，与 D2 的目标直接冲突。C++17 无 `std::span` 可替代。
- **`frame` 设为选填（codec 可不填）。** 迁移更平缓。**否决理由：** 见代价 6——承诺时有时无比不做更坏，调用方无法依赖它。
- **同时提供侵入式的 `Message::Detach()`**（就地把 payload 转为拥有型、`frame` 可一并丢弃，此后整条 `Message` 可安全保存）。转发、排队、重投这类要保存**整条消息**（还要带 `endpoint` / `session_id` 等元数据）的场景，用它比用 `OwnedPayload()` 再手工重建一条 `Message` 顺手。**否决理由（2026-09-11 裁决）：** 只保留 `OwnedPayload()` 一个逃生口。两个接口会带来"该用哪个"的选择负担，而 `Detach()` 的场景可由调用方自行拼出（取 `OwnedPayload()` 后赋回 `msg.payload` 并清空 `frame`）。**此条已关闭，不是悬着的备选。**
- **服务名直接塞进 `Endpoint::kTopic`。** 最省事。**否决理由：** 把 ADR-0013 **D6** 刻意分开的"服务名"与"topic"混为一谈；`kService` 让类型系统替我们守住这条区分。
- **给 `Endpoint` 加 `operator==` / `std::hash` 以整体入键。** **否决理由：** 键只需要那个字符串（**D4**），为此让整个结构体可哈希是多余负担，且会诱使将来把 `host`/`port` 也塞进键。
