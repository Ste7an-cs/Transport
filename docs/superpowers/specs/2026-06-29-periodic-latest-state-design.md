# 周期发送取最新状态(消息工厂 + 推送更新)— 设计

**日期：** 2026-06-29
**状态：** 设计已确认,待写 plan
**配套：** `docs/superpowers/specs/2026-06-24-interaction-engine-design.md`、`docs/superpowers/specs/2026-06-23-protocol-node.md`

## 1. 背景与目标

当前周期发送(`ProtocolNode::StartRepeating`、引擎 `StartPeriodic`)把 payload **冻结**在启动时刻:引擎 `Periodic` 存一份固定 `Message out`,`FirePeriodic` 每拍重发同样字节。无法发送实时状态。

**目标:** 让周期发送的每帧在**发送前取最新状态**,两种模型都支持,且 DDS 也具备周期发布能力:
- **A 拉(pull)**:启动时给一个**状态提供者回调**,引擎每拍发送前调它取最新 payload。
- **B 推(push)**:启动时给初值 + 返回 handle,应用状态变更时**主动更新**,定时器发当前值。
- **DDS**:`DdsNode` 当前**无**周期发布;本次新增(固定 / 工厂 / 更新),tag=`kNotify`(同 `Send`)。

**非目标(YAGNI):** 变速周期(运行期改 interval)、按拍携带序号/时间戳(应用自己在 payload 里放)、回调抛异常处理(全库不抛异常,回调亦不得抛)。

## 2. 关键决策

| 决策 | 选择 |
|---|---|
| 引擎周期载体 | `Periodic` 由固定 `Message` 改为**消息工厂** `std::function<Message()>`;每拍 `make()` 产出 |
| A 拉 | 工厂版 `StartPeriodic(make, tag, interval, to)`;`make()` 每拍发送前在 executor 线程、**锁外**调用 |
| B 推 | `UpdatePeriodic(handle, Message)` 锁内换 `make` 为返回新值;返回 `bool`(handle 不存→false) |
| 向后兼容 | 固定版 `StartPeriodic(Message, ...)` 保留,内部包装 `make = [m]{ return m; }` —— 行为逐字不变 |
| 更新无节点态 | 节点 `Update*` 显式带重建 Message 所需信息(ProtocolNode 带 `cmd`,DdsNode 带整 `Message`),**不引入 handle→cmd 节点映射**(避免与引擎周期生命周期二次同步) |

## 3. 引擎 `InteractionEngine`

```cpp
// Periodic 载体改为消息工厂
struct Periodic { std::function<Message()> make; FrameTag tag; Endpoint to;
                  uint32_t interval_ms; IExecutor::TimerId timer = 0; };

// A:工厂版(主)——每拍 make() 取最新
uint32_t StartPeriodic(std::function<Message()> make, FrameTag tag, uint32_t interval_ms,
                       const Endpoint& to = Endpoint::Default());
// 兼容:固定版——包装成 make = [m]{ return m; }
uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                       const Endpoint& to = Endpoint::Default());
// B:推送更新——把 handle 的 make 换成返回 out;handle 不存返回 false
bool UpdatePeriodic(uint32_t handle, Message out);
void StopPeriodic(uint32_t handle);   // 不变
```

- **`FirePeriodic`(并发纪律)**:沿用现有「锁内取、锁外发」——锁内**拷贝** `make`(`std::function` 拷贝)、`tag`、`to`、`interval`;**锁外** `Message out = make(); Fire(out, tag, to);` 再锁内重排定时器。`make()` 是用户代码,必须锁外调(防慢/重入)。`StartPeriodic` 的「立即一帧」也走 `make()`(首拍即最新)。
- **`UpdatePeriodic`**:锁内 `find(handle)`,在则 `it->second.make = [m=std::move(out)]{ return m; }` 返回 true,否则 false。
- **重载消歧**:`Message` 与 `std::function<Message()>` 类型不同,传值/传 lambda 各自精确匹配,无歧义。
- **契约**:`make()`/`state_fn` 在 executor(worker/定时器)线程、每拍发送前、锁外被调 → 须**线程安全**(读应用共享态自行加锁)、**非阻塞**、**快**、**不抛异常**;`UpdatePeriodic` 可任意线程调(锁内换 `make`)。

## 4. `ProtocolNode`(STATE 帧)

```cpp
// 现有(固定快照,B 的初值):
uint32_t StartRepeating(uint16_t cmd, std::vector<uint8_t> payload, uint32_t interval_ms,
                        const Endpoint& to = Endpoint::Default());
// A 新增(每拍拉最新 STATE):
uint32_t StartRepeating(uint16_t cmd, std::function<std::vector<uint8_t>()> state_fn,
                        uint32_t interval_ms, const Endpoint& to = Endpoint::Default());
// B 新增(推送更新当前 STATE):
bool     UpdateRepeating(uint32_t handle, uint16_t cmd, std::vector<uint8_t> payload);
void     StopRepeating(uint32_t handle);   // 不变
```

- A 内部:`make = [cmd, fn = std::move(state_fn)]{ return Cmd(cmd, fn()); }` → `engine_->StartPeriodic(make, Tag(kState), interval, to)`。
- B 内部:`engine_->UpdatePeriodic(handle, Cmd(cmd, std::move(payload)))`(透传返回值)。
- `cmd` 在 `Update` 处显式再给(调用方已知的命令码常量)→ 无需节点态映射。`tag=kState`、`message_id=cmd` 每拍由 `Cmd()`/`Fire` 重新盖,调用方只给 payload。

## 5. `DdsNode`(周期发布,本次新增)

`DdsNode` 当前无周期能力。新增周期发布(发布即 `Fire(kNotify, Topic)`,故周期发布 tag=`kNotify`):

```cpp
// 固定 / B 初值:
uint32_t StartPublishing(Message msg, uint32_t interval_ms, const Endpoint& to);
// A 拉:每拍产出最新样本
uint32_t StartPublishing(std::function<Message()> sample_fn, uint32_t interval_ms, const Endpoint& to);
// B 推:更新当前样本
bool     UpdatePublishing(uint32_t handle, Message msg);
void     StopPublishing(uint32_t handle);
```

- 内部:`StartPublishing(msg,…)`→`engine_->StartPeriodic(std::move(msg), Tag(kNotify), interval, to)`;工厂版→`engine_->StartPeriodic(std::move(sample_fn), Tag(kNotify), interval, to)`;`UpdatePublishing`→`engine_->UpdatePeriodic(handle, std::move(msg))`;`StopPublishing`→`engine_->StopPeriodic(handle)`。
- `to` 用 `Endpoint::Topic(t)` 指定发布 topic(同 `Send`)。DDS 样本 Message 自含(payload 等),更新带整 `Message`、无需额外参数。

## 6. 用法

```cpp
// ProtocolNode 拉(A):每 100ms 发最新传感器状态
node->StartRepeating(cmd::kTelemetry, [sensor]{ return sensor->ReadLatest(); }, 100);
// ProtocolNode 推(B):事件驱动更新
uint32_t h = node->StartRepeating(cmd::kTelemetry, {/*初值*/}, 100);
/* 状态变时 */ node->UpdateRepeating(h, cmd::kTelemetry, latest);

// DdsNode 拉(A):周期发布最新遥测样本
dds->StartPublishing([src]{ return src->LatestSample(); }, 200, Endpoint::Topic("telemetry"));
```

## 7. 文件

**修改**
- `include/transport/comm/InteractionEngine.hpp` + `src/comm/InteractionEngine.cpp`:`Periodic.make`、工厂版 `StartPeriodic`、固定版包装、`UpdatePeriodic`、`FirePeriodic`(make 锁外调)。
- `include/transport/comm/ProtocolNode.hpp` + `src/comm/ProtocolNode.cpp`:`StartRepeating` 工厂重载、`UpdateRepeating`。
- `include/transport/comm/DdsNode.hpp` + `src/comm/DdsNode.cpp`:`StartPublishing`(固定/工厂)、`UpdatePublishing`、`StopPublishing`。
- `tests/comm/interaction_engine_test.cpp`、`protocol_node_test.cpp`、`dds_node_test.cpp`:周期工厂每拍取新值、UpdatePeriodic 改变后续帧、固定版行为不变、Stop 后不再发。
- CMake 无需改(无新文件)。

**不回归:** 固定 `StartPeriodic`/`StartRepeating` 行为逐字不变(现有 periodic/心跳测试即证明);新增均为重载/新方法。

## 8. 约束

- C++17,不抛异常,`Result`/`Status`。
- 并发纪律不破:`make()`/`state_fn` 锁外、executor 线程调;`UpdatePeriodic` 锁内换 `make`;`Close` 仍取消全部 periodic 定时器(不变)。
- 提交作者 `Ste7an-cs <ste7ann@gmail.com>`,无 Co-Authored-By。
- 文档(SRS/SDD/README/CHANGELOG)+ demo 同步留到实现后。
