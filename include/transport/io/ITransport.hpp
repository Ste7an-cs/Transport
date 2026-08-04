#pragma once

#include <optional>
#include <system_error>

#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 传输层统一接口——纯字节管道:Start/Read/Write/RequestClose/WaitClosed
 *        + 跨介质强制的 I/O 观测面(RT_NODE_006「所有介质如实报」,ADR-0003 D13)。
 *
 * `LastSendTime`/`LastReceiveTime`/`LastError` 是每种介质都能给出的最小公分母
 * I/O 事实,由具体实现类各自记账并如实报告——非"连接健康"裁决(判活留给协议层)。
 * `SendWaiterDepth` 等背压类观测非普适(UDP/DDS 无背压概念),故不进本接口,留各
 * 实现类自己的方法。
 */
class ITransport {
 public:
  using Clock = OperationOptions::Clock;

  virtual ~ITransport() = default;
  virtual Status Start() = 0;
  virtual Result<Datagram> Read(OperationOptions options = {}) = 0;
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
};

}  // namespace transport
