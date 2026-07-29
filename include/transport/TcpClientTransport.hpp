#pragma once

/**
 * @file TcpClientTransport.hpp
 * @brief 协程原生 TCP 客户端连接管理传输——连接/超时abort/退避重连 + 代际隔离。
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/IConnectionObservable.hpp"
#include "transport/ITransport.hpp"
#include "transport/tcp/TcpClientConfig.hpp"

namespace transport {

/**
 * @brief 外层 TCP 客户端连接管理传输(ADR-0003 D11,seed #20 分层)。
 *
 * owns `QTcpSocket`,组合 P1 内层 `TcpTransport`(一代际一实例),实现 `ITransport`
 * + 可选 `IConnectionObservable`。base `ITransport` 保持纯字节管道(连接概念不下沉,
 * D3′)。
 *
 * `Start()` 立即返回并 spawn 一条 connect-loop fiber(节点执行域线程)运行状态机
 * `Connecting/Connected/Reconnecting`:socket 在该 fiber 内创建(亲和纪律);连接
 * 超时后显式 `abort()+deleteLater()`(corosocket 摩擦 1);断连后按 1s×2 上限 30s
 * ±20% jitter 无限退避重试,稳定 ≥stable_reset_after 后下次断开重置退避级别。每次
 * 成功物理连接 `Generation()` +1。
 *
 * ITransport 委托:`Read`/`Write` 委托当前代际内层;`Write` 非 Connected 态立即返
 * `kConnection`(不缓存,RT_TCP_RECONNECT_003)。运行时 `ApplyConfig` 属 P3-3;node
 * 侧透明续命端到端属 P3-2。
 */
class TcpClientTransport final : public ITransport, public IConnectionObservable {
 public:
  using Clock = OperationOptions::Clock;

  /// @brief 用连接管理配置构造(尚未发起连接;须 Start)。
  explicit TcpClientTransport(TcpClientConfig config);
  ~TcpClientTransport() override;

  TcpClientTransport(const TcpClientTransport&) = delete;
  TcpClientTransport& operator=(const TcpClientTransport&) = delete;

  // -- ITransport --

  /// @brief 进入 Running 并 spawn connect-loop fiber;立即返回不等首次 Connected
  ///        (RT_LIFECYCLE 3.1.6.3),状态进 Connecting。重复调用幂等。
  Status Start() override;

  /// @brief 读一片字节:**透明跨重连**(RT_TCP_RECONNECT / ADR-0003 D11 Q1①)。
  ///        Connected 期委托当前代际内层返字节;断连/重连期阻塞等待下一代际连上,再委托
  ///        新代际内层——读循环永不因 TCP 客户端断连而退出。跨代际切换时重新取当前内层。
  ///        仅 Close/RequestClose 时返 `kClosed`;调用方 deadline/取消只结束本次等待并透传
  ///        `kTimeout`/`kCancelled`(后台重连继续)。
  Result<Datagram> Read(OperationOptions options = {}) override;

  /// @brief 发送一帧:委托当前代际内层;非 Connected 态立即返 `kConnection`
  ///        (不缓存,RT_TCP_RECONNECT_003)。
  Status Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等):停 connect-loop、掐断当前尝试、关闭当前内层。
  Status RequestClose() override;

  /// @brief 等待完全关闭(connect-loop 退出;支持多等待者)。
  Status WaitClosed(OperationOptions options = {}) override;

  // -- IConnectionObservable --

  [[nodiscard]] ConnectionState State() const override;
  [[nodiscard]] Status WaitForState(ConnectionState target,
                                    OperationOptions options = {}) override;
  [[nodiscard]] Result<ConnectionState> WaitStateChange(
      OperationOptions options = {}) override;
  [[nodiscard]] std::uint64_t Generation() const override;
  [[nodiscard]] std::uint64_t ConfigVersion() const override;
  [[nodiscard]] std::error_code LastFailure() const override;
  [[nodiscard]] std::size_t AttemptCount() const override;
  [[nodiscard]] std::optional<Clock::time_point> NextAttemptTime()
      const override;

  // -- I/O 事实 getter(委托当前代际内层;无内层则空)--

  /// @brief 当前处于 Write 中的 fiber 数(委托内层)。
  [[nodiscard]] std::size_t SendWaiterDepth() const;
  /// @brief 最近一次发送完成时刻(委托内层)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime() const;
  /// @brief 最近一次收到字节时刻(委托内层)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime() const;
  /// @brief 最近一次内层 I/O 错误(委托内层)。
  [[nodiscard]] std::error_code LastError() const;

  struct Impl;  // 不透明:定义在 .cpp,connect-loop fiber 与本类共享。

 private:
  std::shared_ptr<Impl> state_;
};

}  // namespace transport
