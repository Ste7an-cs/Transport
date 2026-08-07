#pragma once

/**
 * @file TcpClientTransport.hpp
 * @brief 协程原生 TCP 客户端连接管理传输——连接泵 + 对外通道,断链对调用方完全透明。
 */

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <system_error>

#include "transport/io/ITransport.hpp"
#include "transport/io/tcp/TcpClientConfig.hpp"

namespace transport {

/**
 * @brief 连接管理状态机可观察态(RT_LIFECYCLE_002,TCP 客户端 Running 内子状态)。
 *
 * 与 base 生命周期 `LifecycleState` 正交:连接代际 churn 在 Running 内进行,`Close`
 * 走 `Closing→Closed` 终态,不在此枚举内。
 */
enum class ConnectionState {
  kDisconnected,  ///< 未连接(初始 / 关闭后)。
  kConnecting,    ///< 正在发起一次连接尝试。
  kConnected,     ///< 已建立物理连接,内层传输就绪。
  kReconnecting,  ///< 断连后等待下一次尝试。
};

/**
 * @brief 外层 TCP 客户端连接管理传输(ADR-0003 D11 分层;内部结构见 ADR-0004 D6)。
 *
 * owns `QTcpSocket`,组合 P1 内层 `TcpTransport`(一代际一实例)。**只实现 `ITransport`**
 * ——独立的连接观察面接口已随 ADR-0004 D2/D7 取消(链路可用性并入 `ITransport`),其诊断项
 * 降级为本类的具体方法,不进基类、不构成多态缝(交互层不再按介质探测能力)。
 *
 * **内部结构:连接泵 + 对外通道(ADR-0004 D6)**
 *
 * `Start()` 立即返回并 spawn 一条 connect-loop fiber(节点执行域线程),该 fiber 是本类
 * 唯一的内部工作单元:
 * 1. 连接(socket 在该 fiber 内创建,亲和纪律;连接超时后显式 `abort()+deleteLater()`);
 * 2. 连上后建内层 `TcpTransport`,以**读泵**反复取字节片投入**对外通道**;
 * 3. 内层读取终结(断链)即退出读泵、拆掉本代际,**隔固定间隔**(ADR-0005 D4)重连;
 * 4. 仅 `RequestClose` 使循环退出并收敛。
 *
 * `Read()` 只从对外通道取,与连接状态机完全解耦——**断链对调用方完全透明**(ADR-0004
 * D1):断链期间通道无数据故自然挂起,重连后新链路字节到达即被唤醒,`Read` 期间**不返回
 * 任何断链错误**;唯一的失败终止是我方 `RequestClose` 的 `kClosed`。断链前残留在通道里
 * 的字节保留并继续交付(其与新链路首字节可能拼成错帧,由编解码器重同步处置,ADR-0004 D4)。
 *
 * `Write()` **直操当前代际内层**、不经通道:链路不可用时立即返 `kConnection`,不缓存等待
 * 重连(RT_TCP_RECONNECT_003)。
 */
class TcpClientTransport final : public ITransport {
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

  /// @brief 从对外通道取一片字节:**断链完全透明**(ADR-0004 D1/D6,RT_TRANSPORT_008)。
  ///
  /// 通道无数据则挂起(断链期间即如此),重连后新链路字节到达即被唤醒——**不返回任何
  /// 断链错误**。唯一的终止失败是我方 `RequestClose` 后的 `kClosed`;调用方 deadline
  /// 只结束本次等待并返 `kTimeout`(后台重连继续)。逐读取消令牌为 out-of-scope(同
  /// `TcpTransport`:持久单通道被逐读取消关闭会永久终止交付),循环级中断用 `RequestClose`。
  /// 同一时刻至多一个有效读(RT_TRANSPORT_004),并发读返 `kInvalidState`。
  Result<Datagram> Read(OperationOptions options = {}) override;

  /// @brief 发送一帧:直操当前代际内层;链路不可用(未连上/重连中)立即返 `kConnection`
  ///        (不缓存等待重连,RT_TCP_RECONNECT_003)。我方已关闭则返 `kClosed`。
  Status Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等):停 connect-loop、掐断当前尝试、关闭当前内层与对外通道
  ///        (使在途 `Read` 返 `kClosed`)。
  Status RequestClose() override;

  /// @brief 等待完全关闭(connect-loop 退出;支持多等待者)。
  Status WaitClosed(OperationOptions options = {}) override;

  // -- 运行时重配置(客户端具体方法,宿主直接调,不经 node;ADR-0004 D7)--

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
  /// 切新连接代际、立即尝试新端点(不先等重连间隔)。仅策略参数(连接超时/重连间隔)
  /// 变化:不打断正在进行的连接尝试或已开始的间隔等待(用旧快照),下一次连接动作用新
  /// 参数。热更新范围仅端点/连接超时/重连间隔(RT_TCP_RECONFIG_002,ADR-0005 D4)。
  ///
  /// @param config 完整配置快照。
  /// @param version 单调版本(须严格大于当前才应用;等于且同容为 no-op)。
  Status ApplyConfig(TcpClientConfig config, std::uint64_t version);

  // -- 连接诊断面(具体方法,ADR-0004 D7:不进基类、不构成多态缝)--

  /// @brief 当前连接状态快照(Running 内子状态,RT_LIFECYCLE_002)。
  [[nodiscard]] ConnectionState State() const;

  /// @brief 等待进入目标状态(多等待者);已满足即刻返回。
  /// @param options deadline 只结束本次等待(返 kTimeout),后台重连继续;取消返
  ///        kCancelled;目标在关闭前无法达成返 kClosed。
  /// @note 非通用要求(RT_TCP_RECONNECT_005),仅本类以自身方法提供,供宿主与测试同步。
  [[nodiscard]] Status WaitForState(ConnectionState target,
                                    OperationOptions options = {});

  /// @brief 等待下一次状态跃迁,返回跃迁后的新状态(多等待者)。
  [[nodiscard]] Result<ConnectionState> WaitStateChange(
      OperationOptions options = {});

  /// @brief 连接代际:单调 uint64,每次成功物理连接 +1。
  [[nodiscard]] std::uint64_t Generation() const;

  /// @brief 当前生效的配置版本(与连接代际两轴独立,RT_DATA_STATE)。
  [[nodiscard]] std::uint64_t ConfigVersion() const;

  /// @brief 已发生的规范化配置变更次数(每次非空成功 `ApplyConfig` +1;同版同容 no-op
  ///        不计,RT_TCP_RECONFIG_006)。与连接代际两轴独立(RT_DATA_STATE)。
  [[nodiscard]] std::uint64_t ConfigChangeCount() const;

  /// @brief 最近一次连接失败的错误类别(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastFailure() const;

  /// @brief 迄今累计的连接尝试次数。
  [[nodiscard]] std::size_t AttemptCount() const;

  /// @brief 重连等待中下次尝试的预定时刻(非等待期为空)。
  [[nodiscard]] std::optional<Clock::time_point> NextAttemptTime() const;

  /// @brief 观测:最近一次 Close 发起到 Closed 完成的时延(P5-4,RT_DATA_BUFFER)。尚未
  ///        关闭完成时为 0。
  [[nodiscard]] Clock::duration LastCloseLatency() const;

  // -- I/O 事实 getter(委托当前代际内层;无内层则空)--

  /// @brief 当前处于 Write 中的 fiber 数(委托内层)。
  [[nodiscard]] std::size_t SendWaiterDepth() const;
  /// @brief 最近一次发送完成时刻(委托内层)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime()
      const override;
  /// @brief 最近一次收到字节时刻(读泵投入对外通道的时刻)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime()
      const override;
  /// @brief 最近一次内层 I/O 错误(委托内层)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):映射内部连接状态机——`Connected` →
  ///        `kUp`,`Connecting`/`Reconnecting` → `kEstablishing`,`Disconnected`
  ///        与未 Start / 关闭中 / 已关闭 → `kDown`。**只报事实,不报策略**:重连间隔、
  ///        重连决策仍是本类内部事(诊断面见 `NextAttemptTime`/`AttemptCount`)。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  struct Impl;  // 不透明:定义在 .cpp,connect-loop fiber 与本类共享。

 private:
  std::shared_ptr<Impl> state_;
};

}  // namespace transport
