#include "transport/io/dds/DdsProviderRegistry.hpp"

#include <map>
#include <mutex>
#include <utility>

#include "transport/io/dds/FakeDdsProvider.hpp"
#ifdef TRANSPORT_HAS_FASTDDS
#include "FastDdsProvider.hpp"
#endif

// DdsProviderRegistry.cpp — 静态注册表(函数局部 static 保证初始化顺序安全)。
//
// `DdsTransport::Start()` 按 `DdsConfig::provider` 这个名字来这里取实现(ADR-0013 D12 的
// "非空且已注册")。两个内建名恒可用,与 `DdsConfig` 注释里写的一致:
//   "fake"    —— 进程内总线,零 FastDDS 依赖;
//   "fastdds" —— 真实互通,**仅在装了 Fast DDS 时存在**(缺席时按未注册处理,返
//                kConfiguration,而不是给一个跑不动的实现)。

namespace transport {

namespace {
std::mutex& RegistryMutex() { static std::mutex m; return m; }
std::map<std::string, DdsProviderRegistry::Factory>& RegistryMap() {
  static std::map<std::string, DdsProviderRegistry::Factory> m;
  return m;
}

// 内建项**惰性补齐**,不用静态初始化器:后者在静态库里会因整个编译单元无人引用而被
// 链接器丢掉。用 `emplace` 而非 `[]=`:调用方若先注册了同名实现,以**调用方的**为准。
void EnsureBuiltins() {
  auto& map = RegistryMap();
  map.emplace("fake", [] { return std::unique_ptr<IDdsProvider>(new FakeDdsProvider()); });
#ifdef TRANSPORT_HAS_FASTDDS
  map.emplace("fastdds", [] { return std::unique_ptr<IDdsProvider>(new FastDdsProvider()); });
#endif
}
}  // namespace

void DdsProviderRegistry::RegisterProvider(const std::string& name, Factory factory) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  RegistryMap()[name] = std::move(factory);
}

std::unique_ptr<IDdsProvider> DdsProviderRegistry::Create(const std::string& name) {
  std::lock_guard<std::mutex> lk(RegistryMutex());
  EnsureBuiltins();
  auto it = RegistryMap().find(name);
  if (it == RegistryMap().end()) return nullptr;
  return it->second();
}

}  // namespace transport
