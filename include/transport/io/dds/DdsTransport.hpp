#pragma once

/**
 * @file DdsTransport.hpp
 * @brief 协程原生 DDS 传输——组合 IDdsProvider 与跨线程有界交接边界(ADR-0003 D12 Q1)。
 */

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include "transport/io/ITransport.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/IDdsProvider.hpp"
#include "transport/core/ITraceSink.hpp"

namespace transport {

/**
 * @brief DDS 交接元素:一条到达样本的不透明字节 + 来源 topic。
 *
 * provider `Subscribe` 回调在 listener 线程(非 fiber)构造 Sample 并非阻塞入队交接
 * 边界;`Read` 侧 fiber 出队后据 topic 填 `Datagram.source = Endpoint::Topic(topic)`。
 */
struct Sample {
  std::vector<std::uint8_t> bytes;
  std::string topic;
};

/**
 * @brief 协程原生 DDS 传输:实现 ITransport,组合底层 IDdsProvider 与**跨线程有界
 *        交接边界**(复用 `BoundedQueue<Sample>`,ADR-0003 D12 Q1 / RT_NODE_004/005/007)。
 *
 * 收侧:构造期给定订阅 topic 集,`Start` 对每个 topic `Subscribe`;provider 在 listener
 * 线程触发回调 → **非阻塞 `Push`** 交接边界(满 tail-drop 丢最新 + 命名计数
 * `dds_handoff_overflow`,不阻塞 listener,RT_NODE_007)。`Read` = 出队交接边界(空则
 * fiber 协作 await)→ `Datagram{bytes, source=Endpoint::Topic(topic)}`,同 topic 保接受
 * 顺序(FIFO,RT_NODE_005)。**跨线程唤醒**靠 AsyncTask `Awaitable` 的 boost.fiber
 * channel 跨线程安全性(闭合 ADR-0001「裸 fiber channel 外线程 push」未决项 = 安全)。
 *
 * 发侧:`Write(SendUnit)` = `provider.Publish(destination.topic, bytes)`(destination 须
 * 为 `kTopic`;`kDefault`/其他 kind → `kInvalidArgument`),可发往不同 topic(统一寻址)。
 *
 * 生命周期:**非重连**(D3′,无连接状态机/重连);`RequestClose`/析构先 `Unsubscribe`
 * 停投递 → 交接边界 `Close` 唤醒在途 `Read` → provider `Shutdown`;迟到回调样本被丢弃且
 * 不触碰已销毁对象(回调只捕获交接边界的共享句柄,不强引本对象);本地已丢样本不宣称由
 * DDS Reliable 自动恢复(仅计数,不重取)。
 */
class DdsTransport final : public ITransport {
 public:
  using Clock = OperationOptions::Clock;

  /// @brief 交接边界默认样本数上限(ADR-0002 D4,与 BoundedQueue 默认一致)。
  static constexpr std::size_t kDefaultMaxSamples = 1024;
  /// @brief 交接边界默认字节上限:16 MiB(ADR-0002 D4)。
  static constexpr std::size_t kDefaultMaxBytes = 16u * 1024u * 1024u;

  /**
   * @brief 构造 DDS 传输(尚未 Init/Subscribe,须 `Start` 后方可收发)。
   *
   * @param provider     底层 DDS provider(接管所有权;不得为空)。
   * @param config       provider `Init` 配置(domain/qos/provider 名等)。
   * @param topics       订阅 topic 集(P4 静态给定;`Start` 时逐一 Subscribe)。
   * @param max_samples  交接边界样本数上限(越界由 BoundedQueue 钳制,默认 1024)。
   * @param max_bytes    交接边界字节上限(越界由 BoundedQueue 钳制,默认 16 MiB)。
   * @param trace_sink   可选 Trace 出口(P5-3,ADR-0003 D13);非拥有,可为 nullptr
   *                     (RT_TRACE_002:未配置不改变任何控制流/计数)。传给交接边界,
   *                     归因 `dds_handoff_overflow`。
   */
  DdsTransport(std::unique_ptr<IDdsProvider> provider, DdsConfig config,
               std::vector<std::string> topics,
               std::size_t max_samples = kDefaultMaxSamples,
               std::size_t max_bytes = kDefaultMaxBytes,
               ITraceSink* trace_sink = nullptr);
  ~DdsTransport() override;

  DdsTransport(const DdsTransport&) = delete;
  DdsTransport& operator=(const DdsTransport&) = delete;

  /// @brief 进入 Running:`provider.Init` + 对每个订阅 topic `Subscribe`(回调入队交接边界)。
  /// @return 成功;非法生命周期返回 kInvalidState;provider Init/Subscribe 失败返其错误。
  Status Start() override;

  /// @brief 出队一条交接样本(空则协作 await),填 `Datagram.source = Endpoint::Topic(topic)`。
  /// @param options 截止时间与取消令牌。
  /// @return 一条 Datagram;超时 kTimeout、取消 kCancelled、关闭 kClosed、未 Start kInvalidState。
  Result<Datagram> Read(OperationOptions options = {}) override;

  /// @brief 发送一条样本:`provider.Publish(destination.topic, bytes)`。
  /// @param unit 待发送样本;`destination` 须为 `Endpoint::Topic`,否则 kInvalidArgument。
  /// @return 成功;非 topic 目的地 kInvalidArgument;未 Start kInvalidState;关闭 kClosed;
  ///         provider 发布失败返其错误。
  Status Write(SendUnit unit) override;

  /// @brief 请求关闭(幂等):Unsubscribe 停投递 → 交接边界 Close 唤醒在途 Read → provider Shutdown。
  Status RequestClose() override;

  /// @brief 等待完全关闭(支持多等待者)。
  Status WaitClosed(OperationOptions options = {}) override;

  /// @brief 交接边界累计 tail-drop 丢弃样本数(命名归因 `dds_handoff_overflow`,RT_NODE_007)。
  [[nodiscard]] std::size_t DdsHandoffOverflowCount() const;

  // I/O 事实(ADR-0003 D13,RT_NODE_006「所有介质如实报」)——非"连接健康"裁决。

  /// @brief 最近一次 `provider.Publish` 成功完成的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastSendTime()
      const override;
  /// @brief 最近一次从交接边界出队到样本的时刻(尚无则空)。
  [[nodiscard]] std::optional<Clock::time_point> LastReceiveTime()
      const override;
  /// @brief 最近一次 Publish/Read 操作错误(无则默认构造的 error_code)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性(RT_TRANSPORT_009):provider 已 `Init` 且订阅全部完成 →
  ///        `kUp`;未 Start、Init/Subscribe 失败或已关闭 → `kDown`。DDS 无连接建立
  ///        相位,故永不出现 `kEstablishing`。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  struct State;  // 不透明:定义在 .cpp,仅供实现内部命名。

 private:
  std::shared_ptr<State> state_;
};

}  // namespace transport
