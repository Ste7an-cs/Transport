#pragma once

#include <memory>
#include <system_error>

#include "await/awaitable.hpp"
#include "detail/result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 传输层统一接口——纯字节管道。
 *
 * **队列式**（ADR-0007 D1）：传输内部持两条队列——`read_queue`（传输作**生产者**，把
 * I/O 收到的数据投入）与 `write_queue`（传输作**消费者**，取出待写 I/O 的数据）。
 * socket/设备的创建、重建与关闭由传输内部的管理泵负责，对调用方**完全透明**。
 *
 * 接口共七个，分三组：
 *
 * | 组 | 方法 | 语义 |
 * |---|---|---|
 * | 任务 | `Start()` / `Close()` / `WaitClosed()` | 开启 / 关闭（只发信号） / 等待完全结束 |
 * | 数据 | `AsyncRead()` / `AsyncWrite()` | 交出读队列的**等待器句柄** / 送入一份数据与其目的地 |
 * | 观测 | `LastError()` / `CurrentLinkState()` | 每种介质都能给出的最小公分母 I/O 事实 |
 *
 * **读写两侧不对称,这是刻意的**：读是"数据什么时候来"，只能交出等待器由调用方自行决定
 * 超时 / 取消 / 是否扇出；写是"把这份数据发到那里去"，调用方给完就完事了。因此
 * `write_queue` 是**纯内部**的——调用方不感知它的存在。两侧的载荷都是 `Datagram`：
 * 读到的 `peer` 是发送方，写出的 `peer` 是目的地。
 */
class ITransport {
 public:
  virtual ~ITransport() = default;

  // ── 任务 ──────────────────────────────────────────────────────────────

  /// @brief 开启传输任务：起内部管理泵后即返回。
  ///
  /// **首次链路就绪失败不算启动失败**——具重试能力的介质由泵在内部无限重试
  /// （UDP 见 ADR-0007 D2）。仅"生命周期非法"（已 `Start()` 过、已关闭）返错。
  virtual Coro::Result<void> Start() = 0;

  /// @brief 关闭传输任务：**只发信号，不等待收敛**。幂等。
  ///
  /// 与 `WaitClosed()` 的分工：本方法受理即返回，内部工作单元的实际退出由
  /// `WaitClosed()` 观察。
  virtual Coro::Result<void> Close() = 0;

  /// @brief 等待传输**完全结束**：join 全部内部工作单元，返回即可安全释放。
  ///
  /// **无 deadline，也不返回 Coro::Result<void>**：`Awaitable::close()` 只保证等待者被**唤醒**，不保证
  /// 被唤醒的 fiber 已跑完并不再触碰传输的成员；而"可安全释放"要的恰是后者，只有
  /// `Coro::FiberTask::get()` 给得了，它没有超时。deadline 与"可安全释放"二选一，且超时
  /// 返回后调用方什么也做不了（仍不能析构传输），故取后者。
  ///
  /// 与 `NodeBase::WaitClosed()` 同形。未 `Start()` 或已汇合时立即返回。
  virtual void WaitClosed() = 0;

  // ── 数据 ──────────────────────────────────────────────────────────────

  /// @brief 交出 `read_queue` 的等待器句柄——**不返回一份数据**。
  ///
  /// deadline 与取消由调用方自行在句柄上 `await_for` / 接令牌；**是否共享由调用方
  /// 决定**：需要多消费者扇出时自行 `shared()`，不共享则多个消费者天然抢占——socket
  /// 的读取本就是抢占式的。传输层因此**不设单读守卫**。
  ///
  /// **终止语义**：传输终结表现为队列被关闭并携带终止原因，调用方在等待器上
  /// 得到该终止错误后应停止读取；瞬时错误由传输内部消化，不出现在本句柄上。
  /// **"仅我方 `Close()` 才终止"** ——具重连能力的介质在内部透明重建，不向调用方
  /// 暴露链路中断。未 `Start()` 即取句柄：得到一个已以 `kInvalidState` 关闭的句柄。
  ///
  /// **载荷是 `Datagram`（字节 + 发送方 `Endpoint`）**：`source` 是不可省的——UDP 服务端
  /// 要回帧给请求方、DDS 要区分 sample 来自哪个 topic，除了这条报文自带的来源信息之外
  /// 没有别的出处。报文式介质一个元素恰好一条完整报文，流式介质一个元素是一段字节
  /// （边界由 `ICodec` 还原）。
  ///
  /// @return 恒非空。
  [[nodiscard]] virtual std::shared_ptr<Coro::Awaitable<Datagram>>
  AsyncRead() = 0;

  /// @brief 把一份数据送入内部写队列——**异步写：入队即返**。
  ///
  /// **fire-and-forget**（ADR-0007 D3）：返回成功仅表示"已受理并入队"，**不表示已发出**。
  /// 实际写出的一切结果——目的地能不能解析、socket 写成没写成——都**不回传**，只落
  /// `LastError()`。链路不可用时数据留在内部队列等待恢复，不拒绝、不丢弃；恢复后按序
  /// 全部发出（**接受对端可能收到过期数据**）。
  ///
  /// 因此本方法只判两件事：生命周期是否允许写、以及有没有真的入队。
  ///
  /// @param datagram 待发数据；其 `peer` 读作**目的地**。`Endpoint::Default()` 表示
  ///                 "发往本传输配置的默认对端"，由实现自行解析——传输无关的调用方恒可
  ///                 传它，不必知道对端是 ip:port 还是 topic。
  /// @return 已入队；未 `Start()` 返 `kInvalidState`；关闭中 / 已关闭返 `kClosed`。
  [[nodiscard]] virtual Coro::Result<void> AsyncWrite(Datagram datagram) = 0;

  // ── 观测 ──────────────────────────────────────────────────────────────

  /// @brief 最近一次操作错误（无则默认构造的 `error_code`）。
  [[nodiscard]] virtual std::error_code LastError() const = 0;

  /// @brief 当前链路可用性——所有介质同形作答的当前 I/O 事实。
  ///
  /// 具连接管理的介质如实反映其连接状态（连接中/重连中 → `kEstablishing`）；无连接
  /// 或单设备介质以"链路是否可用"作答（已绑定 / 设备已打开 → `kUp`）。未 `Start()`、
  /// 关闭中与已关闭一律 `kDown`。**不暴露连接管理策略**（重试间隔、重连决策留在
  /// 具体实现内）。
  [[nodiscard]] virtual LinkState CurrentLinkState() const = 0;
};

// ── 队列小件（各实现共用）────────────────────────────────────────────────

/// @brief 以终止原因关闭队列，并丢弃尚未被取走的残留数据。
///
/// **我方 `Close()` 路径专用**：句柄式读没有"逐次判生命周期"的位置，须显式丢弃残留
/// 才与"关闭即停止交付"等价。传输内部的泵因**链路终结**而关队列时**不用**本函数——
/// 那条路径上残留数据应先被取尽、再由消费者观察到终止原因。
template <typename T>
inline void CloseQueue(const std::shared_ptr<Coro::Awaitable<T>>& queue,
                       std::error_code error) {
  queue->close(error);  // 先关：此后生产者投不进来，残留集合就此定格。
  queue->channel()->discard_pending();
}

/// @brief 造一个**已关闭**的等待器句柄：调用方 `await` 立即得到 `error`。
///
/// 供各传输在生命周期非法（未 `Start()`）时给出**可等待**的答复——读侧是句柄式的，
/// 没有返回错误码的位置，故把该错误作为队列的终止原因交出。（写侧不需要它：`Write()`
/// 直接返回 `Coro::Result<void>`。）
template <typename T>
[[nodiscard]] inline std::shared_ptr<Coro::Awaitable<T>> ClosedQueue(
    std::error_code error) {
  auto queue = std::make_shared<Coro::Awaitable<T>>();
  queue->close(error);
  return queue;
}

}  // namespace transport
