#pragma once

/**
 * @file IDdsProvider.hpp
 * @brief 底层 DDS 库抽象——只管「按 topic 收发不透明字节」。
 *
 * 实现:`FakeDdsProvider`(进程内总线)、`FastDdsProvider`(真实,可选)。
 */

#include "detail/result.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/core/Error.hpp"
#include "transport/io/dds/DdsConfig.hpp"

namespace transport {

/**
 * @brief provider 全部端点上的「匹配 / 判活」计数(ADR-0013 **D9/D13**)。
 *
 * 供 `CurrentLinkState()` 判活:`kUp` = `matched > 0` **且** `alive > 0`;
 * 二者任一为 0 即 `kDown`。
 */
struct DdsMatchedCount {
  /// 当前匹配上的对端端点数(writer 侧的 reader + reader 侧的 writer)。
  int32_t matched = 0;
  /// 上述之中被判为**存活**的数目。
  int32_t alive = 0;
};

/// @brief 「按 topic 收发不透明字节」的抽象;实现方负责全部 DDS 细节。
class IDdsProvider {
 public:
  virtual ~IDdsProvider() = default;

  virtual Coro::Result<void> Init(const DdsConfig& config) = 0;
  virtual void         Shutdown() = 0;

  /// @brief 向 topic 发一份字节。
  ///
  /// **可阻塞**(ADR-0013 **D13**,语义自"必成功"改判):`RELIABLE` 下 writer 准入满时
  /// 实现方会 **park 调用线程**至多一个 `DdsQos::max_blocking_time`,超时返 `kIo`。
  /// 这是**线程级**阻塞、不是 fiber 级——调用方须在**专属 OS 线程**上调用(**D3**),
  /// 否则会卡住整条 fiber 调度线程。
  ///
  /// @return 成功空 `Result`;写超时 / 端点建不出返 `kIo`;未 `Init` 返 `kInvalidState`。
  virtual Coro::Result<void> Publish(const std::string& topic,
                               const std::vector<uint8_t>& bytes) = 0;
  // cb 在样本到达时被 provider 调用(FastDDS=该 topic 的 listener 线程;Fake=发布线程)。
  virtual Coro::Result<void> Subscribe(
      const std::string& topic,
      std::function<void(const std::vector<uint8_t>&)> cb) = 0;
  virtual Coro::Result<void> Unsubscribe(const std::string& topic) = 0;

  /// @brief 取全部端点的匹配 / 判活计数(**D13**);未 `Init` 或已 `Shutdown` 返 `{0, 0}`。
  [[nodiscard]] virtual DdsMatchedCount MatchedCount() const = 0;

  virtual std::string Name() const = 0;
};

}  // namespace transport
