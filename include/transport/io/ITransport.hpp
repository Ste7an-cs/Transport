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

  /// @brief 当前链路可用性(RT_TRANSPORT_009,ADR-0004 D2)。
  ///
  /// 所有介质同形作答的当前 I/O 事实:具连接管理的传输如实反映其连接状态
  /// (连接中/重连中 → `kEstablishing`);无连接或单设备介质以"链路是否可用"作答
  /// (已绑定 / 设备已打开 → `kUp`)。未 Start、关闭中与已关闭一律 `kDown`。
  /// 本查询**不暴露连接管理策略**(退避参数、重连决策留在具体实现内)。
  [[nodiscard]] virtual LinkState CurrentLinkState() const = 0;
};

}  // namespace transport
