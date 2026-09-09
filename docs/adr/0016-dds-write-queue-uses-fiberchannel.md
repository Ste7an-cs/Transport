# ADR-0016：DDS 写侧队列改用 `FiberChannel`，四个介质的队列语义统一

**状态：** Accepted
**日期：** 2026-09-08
**关联：** **改变 ADR-0013 D3 的队列选型**（其"专属 OS 线程"的结论不变）；ADR-0007 **D1**（读写双队列样板——本 ADR 使 DDS 真正回到该样板）；ADR-0011 **D6** / SDD **DD-15**（有界 1024 + 静默丢最旧——本 ADR 使 DDS 写侧复用该实现而非手写）。

## 背景（Context）

ADR-0013 **D3** 为 DDS 写侧选了 `std::mutex` + `std::condition_variable` + `std::deque`，理由是「`FiberChannel` 的文档明载『非协程线程上 `pop` 会 crash』」。

**该理由已于 2026-09-08 实测证伪**（见 ADR-0013 D3 的补正，commit `b0fe4fd`）：那句 crash 警告的主语是被 `FiberChannel` 取代的 `boost::fibers::unbuffered_channel`。照 DDS 写侧的形态实测（fiber 里 `push`、普通 `std::thread` 上 `pop`）：不崩、1001/1001 全收到、顺序严格保持、单条唤醒时延 131µs、`close()` 1ms 内唤醒阻塞中的普通线程、空等 3 秒该线程自身 CPU 仅 70µs（不空转）。

理由塌掉之后，维持现状的依据只剩"专属写线程是普通 OS 线程，不必为它引入 fiber 运行时上下文"——**是"没必要"，不再是"不能用"**。本 ADR 重新裁决这个选型。

### 现状的实质代价：DDS 是四个介质里唯一手写队列策略的

`DdsTransport.cpp` 手写了容量与丢弃策略：

```cpp
write_queue_.push_back(std::move(datagram));
while (write_queue_.size() > kWriteQueueCapacity) {
  write_queue_.pop_front();          // 丢最旧,静默
}
```

**这正是 `FiberChannel::push` 的内建语义**（默认容量 1024，满时丢队首最旧，不阻塞生产者也不返回失败）。UDP / TCP / 串口三个介质的读写队列全部直接复用该实现，只有 DDS 写侧是一份手抄件——ADR-0007 D1 的双队列样板在这一处并未真正落到底。

## 决策（Decision）

- **D1（`write_queue_` 改为 `Coro::FiberChannel<Datagram>`）：** 容量经 `setCapacity(1024)` 设定，与三介质**逐字相同**。随之删除：手写的容量+丢最旧循环、`write_mutex_`、`write_cv_`、`write_stop_`、`std::deque<Datagram> write_queue_`。

  `AsyncWrite` 变为一次 `push`：返回 `closed` 即映射为 `kClosed`。**这比现状更严密**——现状是先查 `write_stop_` 再入队，两步在同一把锁内；`FiberChannel::push` 在锁内自判关闭状态，本就是原子的。

- **D2（关闭必须 `close()` + `discard_pending()`，不得只 `close()`）：** 这是本 ADR **唯一必须写对**的地方。

  `FiberChannel::pop` 在 `close()` 之后**仍会把队列排干**才返回 `closed`（其 `pop` 先判 `!queue_.empty()`，队列非空即照常取值）。若照搬，写线程会在关闭时把残留的至多 1024 条**逐条 `Publish` 出去**，而 `Publish` 的阻塞**没有上界**（ADR-0013 背景：同进程订阅方的交付回调在发布线程上同步执行）。

  **实测**（500 条残留、每条模拟 2ms 发布）：

  | 关闭方式 | 关闭后又消费 | 退出耗时 |
  |---|---|---|
  | 只 `close()` | **486 条** | **995 ms** |
  | `close()` + `discard_pending()` | **1 条** | **0 ms** |

  后者与现状**语义等价**：现状的写线程在 `write_stop_` 置位后**先判停止再取值**，故残留全丢；那"1 条"是已经 `pop` 出来、正在 `Publish` 的那条——**现状同样要等它跑完**（ADR-0013「明确接受的代价」⑦ 记的那个无上界等待，就是它）。

  > 顺序取 **先 `close()` 后 `discard_pending()`**，与 `ProtocolNode::DoClose()` 对读侧的既有写法逐字一致。两步之间存在一个极窄的窗口，写线程可能多取走一条——**最坏比现状多一次 `Publish`**，与那个本就无上界的在途 `Publish` 相比可忽略，不为它加锁。

- **D3（`ITransport` 与 `DdsTransport` 的公开面一个字不动）：** 本 ADR 是**纯内部实现替换**。`AsyncWrite` 的签名与 fire-and-forget 语义、`Close()` / `WaitClosed()` 的语义、`LastError()` 的归因，**全部不变**。

- **D4（专属 OS 写线程不变）：** ADR-0013 **D3** 的结论——写侧必须有一条专属 OS 线程——**继续成立且不受本 ADR 影响**。它的依据是 `DataWriter::write()` park 调用线程（该实测始终有效），与队列用什么原语无关。

## 明确接受的代价

1. **fiber 运行时上下文被引入专属写线程。** `FiberChannel::pop` 用的是 `boost::fibers::mutex` / `condition_variable`；boost.fiber 把每条线程的主上下文也当作一条 fiber，故写线程上挂起的是它自己的主 fiber。**行为即"阻塞该线程"，正合所需**，且实测不空转（空等 3 秒仅 70µs CPU）。代价是这条本来纯粹的 OS 线程从此依赖 fiber 运行时。

2. **业务 fiber 的 `push` 可能被写线程持锁挡一下。** `boost::fibers::mutex` 被普通线程持有期间，业务 fiber 调 `push` 会**挂起该 fiber**（不阻塞线程，其它 fiber 照跑）。临界区是 `push_back` + `notify_all`，**极小且无等待路径**；写线程在 `Publish` 期间**不持锁**（`pop` 返回时已解锁）。故这是有界的微小延迟，不是阻塞风险。

3. **`discard_pending()` 这一步一旦漏写，后果严重且不易发现。** 漏了它，关闭路径会从"丢弃残留"变成"逐条刷出至多 1024 条"，而每条的阻塞无上界。功能测试**不会失败**（消息反而都发出去了），只有关闭时延会爆炸。**须有专门用例守住"关闭不刷残留"这条**。

## 影响（Consequences）

- **正面：** ① 四个介质的队列语义**真正统一**，DDS 写侧不再是手抄件；② 删掉一份手写的容量策略与三个同步成员（`write_mutex_` / `write_cv_` / `write_stop_`）；③ `AsyncWrite` 的"判关闭 + 入队"由 `push` 一步原子完成。
- **负面（明确接受）：** 见上三条。
- **对 ADR-0013：** **D3 的队列选型被本 ADR 改变**；其"专属 OS 线程"的结论、`Publish` 阻塞是线程级的实测依据、以及「明确接受的代价」⑦（关闭时的无上界等待）**全部不变**。
- **对 SDD：** `CSU_DDSTRANSPORT` 的写侧设计与 `MS_DDS_DUAL_QUEUE` 图改写；「两处最容易做错的」一节相应更新。
- **对 SRS：** §3.1.9 中 DDS 写队列的实现描述更新。
- **实施次序：** **须在 #236（ADR-0015 的动态注册）合并之后**——两者改同一批文件。

## 备选方案（Alternatives considered）

- **维持 `std::mutex` + `condition_variable` + `std::deque`。** **否决理由：** 它现在唯一的依据是"不必为写线程引入 fiber 运行时上下文"，而代价是**一份手抄的容量策略**与三个额外的同步成员，并使 DDS 成为四个介质里唯一偏离双队列样板的一处。手抄件与正本一旦漂移（例如三介质将来改容量或改丢弃策略），DDS 会静默地不跟随。
- **改用 `Coro::Awaitable<Datagram>`（与读侧对称）。** **否决理由：** `Awaitable` 是 await 层的封装，面向 fiber 消费者；写侧的消费者是一条普通 OS 线程，用不上也不该用 await 语义。`FiberChannel` 才是这一层的正确原语。
- **保留 `close()` 的排干语义，把关闭时的残留刷出去。** **否决理由：** 与三介质不一致（它们关闭时同样不等刷出），且把一个已经无上界的等待（在途 `Publish`）放大至多 1024 倍。ADR-0007 D3 的写契约本就是 fire-and-forget——"已受理"从不等于"已发出"，关闭时丢弃残留与该契约一致。
