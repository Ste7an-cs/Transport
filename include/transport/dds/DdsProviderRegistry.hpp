#pragma once

// DdsProviderRegistry.hpp — provider 注册表(name → 工厂)。

#include <functional>
#include <memory>
#include <string>

#include "transport/dds/IDdsProvider.hpp"

namespace transport {

class DdsProviderRegistry {
 public:
  using Factory = std::function<std::unique_ptr<IDdsProvider>()>;
  static void RegisterProvider(const std::string& name, Factory factory);
  static std::unique_ptr<IDdsProvider> Create(const std::string& name);  // 未注册→nullptr
};

}  // namespace transport
