#include "transport/io/dds/DdsProviderRegistry.hpp"

#include <map>
#include <mutex>
#include <utility>

// DdsProviderRegistry.cpp — 静态注册表(函数局部 static 保证初始化顺序安全)。

namespace transport {

namespace {
std::mutex& RegistryMutex() { static std::mutex m; return m; }
std::map<std::string, DdsProviderRegistry::Factory>& RegistryMap() {
  static std::map<std::string, DdsProviderRegistry::Factory> m;
  return m;
}
}  // namespace

void DdsProviderRegistry::RegisterProvider(const std::string& name, Factory factory) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  RegistryMap()[name] = std::move(factory);
}

std::unique_ptr<IDdsProvider> DdsProviderRegistry::Create(const std::string& name) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  auto it = RegistryMap().find(name);
  if (it == RegistryMap().end()) return nullptr;
  return it->second();
}

}  // namespace transport
