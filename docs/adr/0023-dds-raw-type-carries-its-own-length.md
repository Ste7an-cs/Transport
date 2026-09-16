# ADR-0023：`FastDdsRawType` 自带长度前缀，消除 RTPS 对齐填充被当作 payload 的缺陷

**状态：** Accepted
**日期：** 2026-09-15
**关联：** 修复 **#258**（跨进程 DDS 的数据损坏）；ADR-0013 **D3**（`INTRAPROCESS_FULL` 的同进程交付——**正是它让既有测试对本缺陷完全盲**）、**D14**（Fast DDS 3.x 的 `TopicDataType` 签名）。

## 背景（Context）

**跨进程 DDS 收到的 `payload` 末尾会多出零字节。** 这是数据损坏，不是显示问题。

实测（`examples/dds_service`，两个进程）：

```
[应答] payload="echo:ping\x00\x00\x00\x00"(13 字节,整帧 56 字节)
```

真实 payload 是 `"echo:ping"`（**9 字节**），收到 **13 字节**。算术对得上：`DdsCodec` 头部 `1 + 2 + 38 + 2 + 0 = 43`，加 payload 9 = **52**；整帧 **56** —— 正是 52 按 **4 字节对齐**补到 56，**那 4 个填充字节被当成了 payload**。

### 机理：两处叠加，单看哪一处都不错

**① RTPS 把 DATA 子消息的序列化载荷按 4 字节对齐**，接收侧 `SerializedPayload_t::length` 反映的是**含填充**的长度。而 `FastDdsRawType::deserialize` 照单全收：

```cpp
msg->payload.assign(payload.data, payload.data + payload.length);   // ← 含填充
```

发送侧 `serialize` 写的 `payload.length = total` 是**精确值**，没有问题——**信息是在 RTPS 这一层丢的**。

**② `DdsCodec` 的线缆格式里 payload 没有长度字段**——它是"剩下的全部"：

```
[kind:1][corr_len:2 BE][corr][reply_len:2 BE][reply_to][payload]
```

故 codec **无从分辨** payload 与尾部填充。

### 为什么既有测试完全看不到

`dds_node_fastdds_e2e_test` 的两端在**同一进程**。Fast DDS 默认 `INTRAPROCESS_FULL`，同进程交付**根本不走序列化**（ADR-0013 **D3** 记录的正是这条机理），故不产生填充；且那两条用例的样本长度**恰好是 4 的倍数**。

**这是一个"只在跨进程、且长度非 4 倍数时才出现"的缺陷，现有测试形态对它完全盲。**

## 决策（Decision）

- **D1（在 `FastDdsRawType` 里自带长度前缀）：** 序列化时在载荷前写一个 **4 字节大端**长度，反序列化时按它取：

  ```cpp
  // serialize
  payload.data[0..3] = BE32(total);
  memcpy(payload.data + 4, msg->payload.data(), total);
  payload.length = 4 + total;

  // deserialize
  const uint32_t n = BE32(payload.data);          // 真实长度
  msg->payload.assign(payload.data + 4, payload.data + 4 + n);
  ```

  `calculate_serialized_size` 相应返回 `4 + size`。

  **修在与 RTPS 打交道的那一层**：填充是 RTPS 引入的，这是信息丢失的**源头**。修在这里，对**任何** codec 都生效——不只 `DdsCodec`，将来任何自定义 DDS codec 也不必各自防一遍。

  **字节序取大端**，与 `DdsCodec` 现有的 `corr_len:2 BE` / `reply_len:2 BE` 一致，不在同一条链路上混用两种字节序。

- **D2（长度前缀是 DDS provider 层的私事，不进 `DdsCodec` 的线缆格式）：** `DdsCodec` 的布局**一个字节不改**。它拿到的仍是"一条完整 sample 的字节"，只是这次长度是准的。

  > **由此 `DdsCodec` 无需感知本 ADR。** 分层在这里是对的：codec 管消息格式，provider 管怎么把一段字节原样送到对端——**"原样"此前不成立，本 ADR 让它成立**。

- **D3（校验长度前缀，不信任对端）：** `deserialize` 须判 `4 + n <= payload.length`；不满足即**返回 `false`**（Fast DDS 会丢弃该 sample）。这能在遇到旧版本对端或损坏样本时**明确失败**，而不是读越界。

- **D4（必须补一条能看见本缺陷的用例）：** 既有的同进程 e2e **看不到**它。新用例须同时满足：

  | 条件 | 为什么 |
  |---|---|
  | **强制走序列化** | 跨进程，或 `set_library_settings(INTRAPROCESS_OFF)` |
  | **payload 长度非 4 的倍数** | 4 的倍数时即便有缺陷也看不出来 |
  | **断言逐字节相等、且长度相等** | 只比前缀会漏掉尾部多出的零 |

  > **这条不可省。** 缺陷之所以活到今天，正是因为测试形态对它盲；修完若不补，下次重构照样会踩回去。

## 明确接受的代价

1. **💥 线缆不兼容:新旧版本的节点不能互通。** 旧版本发出的样本没有长度前缀，新版本会把前 4 字节当长度解析（多半越界而被 **D3** 拒掉）；反之旧版本会把长度前缀当 payload 的头 4 字节。**必须两端一起升级。**

   > 本库尚未有外部部署，且该缺陷本身就让跨进程收发不可靠——**"兼容一个本来就在损坏数据的旧版本"没有意义**。

2. **每样本多 4 字节。** 相对于 `DdsCodec` 头部已有的 43 字节（含 uuid 型 `correlation_id`）可以忽略。

3. **`FakeDdsProvider` 与本 ADR 无关，故两条路径的行为从此有细微差异。** `fake` 是进程内总线、不序列化，本就没有填充问题；它**不加**长度前缀。由此"用 `fake` 测过了"**不能推出** "`fastdds` 上也对"——这正是本缺陷的由来。**D4 的用例必须走真实 provider。**

## 影响（Consequences）

- **正面：** ① 跨进程 DDS 的 payload 首次逐字节可靠；② 修在 provider 层，对任何 codec 生效；③ 补上一类现有测试形态覆盖不到的缺陷，并留下能看见它的用例。
- **负面（明确接受）：** 见上三条，其中 **代价 1（线缆不兼容）需要两端一起升级**。
- **对 SDD：** `CSU_IO` 的 DDS provider 一节补长度前缀；测试策略一节记明"同进程 e2e 看不到序列化层缺陷"。
- **对 CHANGELOG：** 记为**修复 + 破坏性**（线缆不兼容）。

## 备选方案（Alternatives considered）

- **在 `DdsCodec` 的线缆格式里给 payload 加显式长度字段。** **否决理由：** ① 只修好这一个 codec，任何自定义 DDS codec 仍会踩；② 缺陷的源头在 provider 层（RTPS 丢了长度），修在 codec 层是**在错误的抽象层次上打补丁**；③ 同样是线缆不兼容，代价一样。
- **改用 IDL 生成的 `sequence<octet>` 类型，让 CDR 承载长度。** 这是"标准 DDS 做法"，还能与其它 DDS 工具互通。**否决理由：** 要引入 fastddsgen 生成代码与 fastcdr 编解码，改动面远大于 4 字节前缀；而**互通本就不是目标**——`DdsCodec` 的布局已经是私有格式。记此以备将来若需与第三方 DDS 系统互通时重开。
- **剥掉尾部零字节。** **否决理由：** 合法 payload 本身就可能以零字节结尾，**无从分辨**。这不是"不够优雅"，是**不可行**。
- **只在文档里写明"DDS payload 可能带尾部填充，宿主自行处理"。** **否决理由：** 把一个框架层的数据损坏转嫁给每个宿主，且宿主同样无从分辨。
