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

  /// @brief 声明写侧端点——**当场建出该 topic 的 `DataWriter`**(**D13**)。**幂等**。
  ///
  /// 与 `Subscribe` 对称的写侧钩子。有它,发现窗口(约 240ms)才能在**启动时**付掉,
  /// 而不是压在第一帧上——否则**该服务的第一次应答会丢**(**D15/D16**)。
  ///
  /// **不设 `UndeclareWriter`**:端点集合启动即定型、运行期恒定(**D16**),只在
  /// `Shutdown()` 时整体拆除。
  ///
  /// @return 成功空 `Result`;端点建不出返 `kIo`;未 `Init` / 关闭中返 `kInvalidState`。
  virtual Coro::Result<void> DeclareWriter(const std::string& topic) = 0;

  /// @brief 向 topic 发一份字节。**该 topic 须已 `DeclareWriter`**。
  ///
  /// **可阻塞**(ADR-0013 **D13**):`RELIABLE` 下 writer 准入满时
  /// 实现方会 **park 调用线程**至多一个 `DdsQos::max_blocking_time`,超时返 `kIo`。
  /// 这是**线程级**阻塞、不是 fiber 级——调用方须在**专属 OS 线程**上调用(**D3**),
  /// 否则会卡住整条 fiber 调度线程。
  ///
  /// **不惰性建 writer**(**D13**):未声明的 topic 返 `kConfiguration`——惰性建会让
  /// `DeclareWriter` 形同虚设,首帧照样吃发现窗口。
  ///
  /// @return 成功空 `Result`;写超时返 `kIo`;topic 未声明返 `kConfiguration`;
  ///         未 `Init` 返 `kInvalidState`。
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
