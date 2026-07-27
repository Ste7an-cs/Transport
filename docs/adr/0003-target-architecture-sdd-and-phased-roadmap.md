# ADR-0003：目标架构 SDD 与分期路线图（清洁重建）

**状态：** Accepted（草案 / 目标架构，实现随路线图推进）
**日期：** 2026-07-27
**关联：** `docs/设计说明书-协程原生.md`（本 ADR 确立的 SDD + 路线图）；`docs/需求规格说明书-协程原生.md`（SRS v3，需求基线）；ADR-0001（协程原生架构总纲）、ADR-0002（发送完成/丢弃归因/生命周期细化）；as-built 存档于 tag `v0.3.0`。

## 背景（Context）

此前按"每次针对一个点展开 spec 再实现"推进（#19 发送语义已完成），缺**整体设计**与**分期规划**：组件如何分解、缝在哪、node 如何组合没有单一权威源，也没有"先做什么后做什么"的依赖顺序。本 ADR 记录一轮 grilling 形成的顶层决策，据此产出目标架构 SDD + 路线图，供 `/to-tickets` 逐期拆解、逐步实现 SRS 全部需求。

## 决策（Decision）

- **D1（文档形态）：** 新建单文档 `docs/设计说明书-协程原生.md` 兼任**目标架构 SDD + 分期路线图**，作为设计单一权威源（SRS 4.2 把设计留给 SDD，而协程原生 SDD 此前不存在）。否决 epic issue（设计细节不宜塞 issue body）与纯排期文档（缺整体设计）。

- **D2（清洁重建，不写迁移）：** 本 SDD 描述**目标**，不含从 as-built 的迁移关系。as-built 旧架构（`comm/` 引擎/执行器/策略、第二期 `coro/` 引擎、回调式传输、旧文档/示例）**从 master 删除**，已存档于 tag `v0.3.0`。理由：迁移包袱会污染目标设计的表达；旧代码有 tag 留档即可恢复。

- **D3（纵向薄切片优先）：** 路线图按**纵向薄切片**推进，而非横向逐层。首切片打通 TCP 上最小 request-response（TcpTransport→codec→内联 PendingTable→最小 ProtocolNode），**最早证实"无共享引擎、语义内联各 node"这一最大架构赌注**（RT_DESIGN_003 / ADR-0001 D1-D2）。否决横向（所有传输→所有 codec→请求→节点，端到端最晚、组合风险后置）。

- **D4（最大化复用）：** 抢救**与交互架构无关的纯逻辑**（codec 线缆逻辑：帧布局/流式扫描/重同步/`CrcFn`/报文；DDS provider 适配：`IDdsProvider`/Fake/FastDDS/`FastDdsRawType`/registry），并**改造沿用** `Message`/`ICodec`/`ITraceSink`（对齐目标与 coro，不重设计）；`Endpoint` 原样保留。只删被 RT_DESIGN_002/003 否决的架构外壳。**例外**：as-built 顶层 `Result.hpp` 用 `config:`/`conn:` 字符串前缀分类错误，被 RT_ERROR_002 禁止，改用 coro 的 `TransportErrc` 机器可判别模型。

- **D5（命名空间提升）：** as-built 删除后 `coro` 子层失去对照物，P0 将目标提升到 `transport::` 顶层（`transport::ProtocolNode`/`TcpTransport`/`ITransport`/`TransportErrc`），codec→`transport::codec`、provider→`transport::dds`；`include/transport/coro/*`→`include/transport/*`。CMake 由 `transport`+`transport_coro` 合并为单一 `transport`。

- **D6（分期路线图）：** P0 清理+骨架 → P1 TCP 最小请求-响应（证实架构）→ P2 节点加厚（处理器/有界队列/生命周期）→ P3 连接管理（TcpClientTransport，#20）→ P4 其余介质（UDP/串口/DDS + provider 交接）→ P5 观测+完整性归因（D6 命名丢弃计数器 + loss=0 验证）→ P6 性能+硬化+两机验收。P3 在 P4 之前（先把 TCP 加固到生产级再铺介质）。

- **D7（每期验收门槛）：** 功能期（P1–P4）= Fake 确定性契约 + **单机真实介质回环** + 每条原子 RT_* 追溯到测试；两机集成/性能/24h 稳定性集中在 P6；命名丢弃计数器各期就地埋、loss=0 harness 在 P5。沿用 #19 的"Fake 契约 + 真实回环双实现"范式。

## 影响（Consequences）

- **正面：** 有了单一权威的整体设计 + 有序路线图，`/to-tickets` 可逐期拆票、票带清晰验收；纵向薄切片让最大架构风险（无引擎内联 node）在 P1 即被证实；最大化复用避免重写 CRC/扫描、FastDDS CDR-bypass 等难写对的纯逻辑；命名提升让公共 API 干净。
- **代价/风险：** P0 是一次较大的删除 + 重命名 + 移植，须以 tag `v0.3.0` 为恢复基线并保持每步可构建；改造沿用的 `Message`/`ICodec`/`ITraceSink` 与目标 coro 契合度需在 P1 落地验证；五模式状态机/帧常量/性能基线受外部依赖（TBD-001/003/004）就地占位推进。
- **可逆性：** D3（切法）、D6（顺序）相对可逆（可调期序）；D2（删 as-built）、D5（命名提升）落地后回退成本高（但有 tag 留档）。
- **落点：** `docs/设计说明书-协程原生.md`（§2 设计、§3 模块布局与清理清单、§4 路线图 P0–P6）。

## 尚未解决

- 目标 `Message`/`ICodec`/`ITraceSink` 改造后精确签名（P0/P1 定）。
- socket QThread ↔ 节点执行域绑定机制（RT_IN_INTERFACE_005，P1）。
- DDS 交接投递机制/容量与 QoS 一致性（P4）。
- `PendingTable` 同 key 并发多待（当前键空间不需要）。
