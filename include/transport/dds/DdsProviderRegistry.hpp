#pragma once

// -----------------------------------------------------------------------------
// DdsProviderRegistry.hpp — provider 注册表（name → 工厂）。FastDDS 在
// TRANSPORT_HAS_FASTDDS 时自动注册；接入其它 DDS 实现调 RegisterProvider 即可。
// -----------------------------------------------------------------------------

#include <functional>
#include <memory>
#include <string>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class DdsProviderRegistry {
 public:
  using Factory = std::function<std::unique_ptr<IDdsProvider>()>;

  static void RegisterProvider(const std::string& name, Factory factory);
  // 未注册返回 nullptr（调用方报 config: 错误）
  static std::unique_ptr<IDdsProvider> Create(const std::string& name);
};

}  // namespace transport
