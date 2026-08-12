#pragma once

#include <memory>
#include <optional>
#include <system_error>

#include "await/awaitable.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 传输层统一接口——纯字节管道:Start/Read/Write/RequestClose/WaitClosed
 *        + 跨介质强制的 I/O 观测面(RT_NODE_006「所有介质如实报」,ADR-0003 D13)。
 *
 * **队列式**(ADR-0007 D1,SDD §4.3.5):传输内部持 `read_queue`(传输作生产者,把 I/O
 * 收到的数据投入)与写侧通路;socket/设备的创建、重建与关闭由传输内部的管理泵负责,
 * 对两端**完全透明**。
 *
 * `LastSendTime`/`LastReceiveTime`/`LastError`/`CurrentLinkState` 是每种介质都能给出
 * 的最小公分母 I/O 事实,由具体实现类各自记账并如实报告——非"连接健康"裁决(判活留给
 * 协议层)。链路可用性上移基类见 ADR-0004 D2(连接**管理**仍不下沉纯字节管道)。
 * `SendWaiterDepth` 等背压类观测非普适(UDP/DDS 无背压概念),故不进本接口,留各
 * 实现类自己的方法。
 */
class ITransport {
 public:
  using Clock = OperationOptions::Clock;

  virtual ~ITransport() = default;
  virtual Status Start() = 0;

  /// @brief 交出 `read_queue` 的等待器句柄(ADR-0007 D4,SDD §4.3.5)——**不返回一份
  ///        数据**,也**不接受 `OperationOptions`**。
  ///
  /// deadline 与取消由调用方自行在句柄上 `await_for` / 接令牌;**是否共享由调用方
  /// 决定**:需要多消费者扇出时自行 `shared()`,不共享则多个消费者天然抢占——socket
  /// 的读取本就是抢占式的。传输层因此**不设单读守卫**(RT_TRANSPORT_004 的该约束已删)。
  /// 扇出策略(谁该收到、是否复制)属调用方语义,传输层无从判断,故不下沉。
  ///
  /// **读取终止语义(DD-11,表达经 ADR-0007 D4 改写)**:传输终结表现为 `read_queue`
  /// 被 `close()` 并携带终止原因,调用方在等待器上得到该终止错误后应停止读取;其余
  /// 读取失败为可继续的瞬时错误,由传输内部消化、不出现在本句柄上。**"仅我方 `Close`
  /// 才终止"的语义不变**——具备重连能力的传输在内部透明重建,不向调用方暴露链路中断。
  /// 未 `Start()` 即取句柄:得到一个已以 `kInvalidState` 关闭的句柄(生命周期非法)。
  ///
  /// @return `read_queue` 的等待器句柄(恒非空)。
  [[nodiscard]] virtual std::shared_ptr<Coro::Awaitable<Datagram>> Read() = 0;

  virtual Status Write(SendUnit unit) = 0;
  virtual Status RequestClose() = 0;
  virtual Status WaitClosed(OperationOptions options = {}) = 0;

  /// @brief 最近一次发送完成的时刻(尚无则空)。
  [[nodiscard]] virtual std::optional<Clock::time_point> LastSendTime()
      const = 0;
  /// @brief 最近一次收到数据的时刻(尚无则空)。
  [[nodiscard]] virtual std::optional<Clock::time_point> LastReceiveTime()
      const = 0;
  /// @brief 最近一次操作错误(无则默认构造的 error_code)。
  [[nodiscard]] virtual std::error_code LastError() const = 0;

  /// @brief 当前链路可用性(RT_TRANSPORT_009,ADR-0004 D2)。
  ///
  /// 所有介质同形作答的当前 I/O 事实:具连接管理的传输如实反映其连接状态
  /// (连接中/重连中 → `kEstablishing`);无连接或单设备介质以"链路是否可用"作答
  /// (已绑定 / 设备已打开 → `kUp`)。未 Start、关闭中与已关闭一律 `kDown`。
  /// 本查询**不暴露连接管理策略**(退避参数、重连决策留在具体实现内)。
  [[nodiscard]] virtual LinkState CurrentLinkState() const = 0;
};

/// @brief 以终止原因关闭 `read_queue`,并丢弃尚未被取走的残留数据。
///
/// **我方 Close 路径专用**。改造前 `Read()` 先判生命周期,故关闭后发起的读一律得
/// `kClosed`、取不到残留;句柄式读没有这个判定点——须显式丢弃残留才与之等价
/// (ADR-0007 D4 改的是终止**表达**,不是"关闭即停止交付"这一行为)。
///
/// 传输内部的泵因**链路终结**而关队列时**不用**本函数:那条路径上残留数据本就应先被
/// 取尽、再由消费者观察到终止原因(改造前消费者直接 await 底层流,channel 亦是先取尽
/// 值再报错)。
inline void CloseDatagramQueue(
    const std::shared_ptr<Coro::Awaitable<Datagram>>& queue,
    std::error_code error) {
  queue->close(error);  // 先关:此后生产者投不进来,残留集合就此定格。
  queue->channel()->discard_pending();
}

/// @brief 造一个**已关闭**的读等待器句柄:调用方 `await` 立即得到 `error`。
///
/// 供各传输在生命周期非法(未 `Start()`)时给出**可等待**的答复——句柄式 `Read()` 没有
/// 返回错误码的位置,故把该错误作为队列的终止原因交出(ADR-0007 D4)。
[[nodiscard]] inline std::shared_ptr<Coro::Awaitable<Datagram>>
ClosedDatagramQueue(std::error_code error) {
  auto queue = std::make_shared<Coro::Awaitable<Datagram>>();
  queue->close(error);
  return queue;
}

}  // namespace transport
