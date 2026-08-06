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

#include "transport/io/IConnectionObservable.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/io/tcp/TcpClientConfig.hpp"

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

  // -- 运行时重配置(客户端扩展方法,宿主直接调,不经 node;ADR-0003 D11 Q5)--

  /// @brief 提交一份完整配置快照并附单调版本(RT_TCP_RECONFIG)。
  ///
  /// 先完整校验后原子应用(RT_TCP_RECONFIG_003):任一字段非法 → 整个失败、旧配置旧
  /// 连接不变、返 `kConfiguration`。单调版本(RT_TCP_RECONFIG_004):version < 当前 或
  /// version == 当前但内容不同 → 拒绝(过期/乱序)、返 `kInvalidArgument`、不覆盖;
  /// version == 当前且内容相同 → 成功 no-op;version > 当前 → 应用。生命周期非法
  /// (关闭中/已关闭)→ 返 `kInvalidState`。失败类别用不同 `TransportErrc` 区分
  /// (参数非法 kConfiguration / 版本过期 kInvalidArgument / 生命周期非法 kInvalidState /
  /// 内部错误 kInternal)。
  ///
  /// 端点(host/port)变化(RT_TCP_RECONFIG_005):原子应用后立即掐断当前尝试/连接、
  /// 切新连接代际、立即尝试新端点(不先等退避);旧连接的在途请求由 node 观察断连驱动
  /// 以 `kConnection` 终结。仅策略参数(超时/退避)变化:不打断正在进行的连接尝试或已
  /// 开始的退避等待(用旧快照),下一次连接动作用新参数。热更新范围仅 host/port/超时/
  /// 退避(RT_TCP_RECONFIG_002)。
  ///
  /// @param config 完整配置快照。
  /// @param version 单调版本(须严格大于当前才应用;等于且同容为 no-op)。
  Status ApplyConfig(TcpClientConfig config, std::uint64_t version);

  // -- IConnectionObservable --

  [[nodiscard]] ConnectionState State() const override;
  [[nodiscard]] Status WaitForState(ConnectionState target,
                                    OperationOptions options = {}) override;
  [[nodiscard]] Result<ConnectionState> WaitStateChange(
      OperationOptions options = {}) override;
  [[nodiscard]] std::uint64_t Generation() const override;
  [[nodiscard]] std::uint64_t ConfigVersion() const override;

  /// @brief 已发生的规范化配置变更次数(每次非空成功 `ApplyConfig` +1;同版同容 no-op
  ///        不计,RT_TCP_RECONFIG_006)。与连接代际两轴独立(RT_DATA_STATE)。
  [[nodiscard]] std::uint64_t ConfigChangeCount() const;

  [[nodiscard]] std::error_code LastFailure() const override;
  [[nodiscard]] std::size_t AttemptCount() const override;
  [[nodiscard]] std::optional<Clock::time_point> NextAttemptTime()
      const override;

  /// @brief 观测:最近一次 Close 发起到 Closed 完成的时延(P5-4,RT_DATA_BUFFER)。尚未
  ///        关闭完成时为 0。
  [[nodiscard]] Clock::duration LastCloseLatency() const;

  // -- I/O 事实 getter(委托当前代际内层;无内层则空)--

  /// @brief 当前处于 Write 中的 fiber 数(委托内层)。
  [[nodiscard]] std::size_t SendWaiterDepth() const;
  /// @brief 最近一次发送完成时刻(委托内层)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime()
      const override;
  /// @brief 最近一次收到字节时刻(委托内层)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime()
      const override;
  /// @brief 最近一次内层 I/O 错误(委托内层)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):映射内部连接状态机——`Connected` →
  ///        `kUp`,`Connecting`/`Reconnecting` → `kEstablishing`,`Disconnected`
  ///        与未 Start / 关闭中 / 已关闭 → `kDown`。**只报事实,不报策略**:退避参数、
  ///        重连决策仍是本类内部事(诊断面见 `NextAttemptTime`/`AttemptCount`)。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  struct Impl;  // 不透明:定义在 .cpp,connect-loop fiber 与本类共享。

 private:
  std::shared_ptr<Impl> state_;
};

}  // namespace transport
