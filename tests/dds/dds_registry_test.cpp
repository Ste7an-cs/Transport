// dds_registry_test.cpp — provider 注册表(name → 工厂)。
//
// `DdsTransport::Start()` 就是按 `DdsConfig::provider` 这个名字来这里取实现的,故
// ADR-0013 **D12** 的"非空且**已注册**"整个落在本表上:查不到即 kConfiguration。
#include "transport/io/dds/DdsProviderRegistry.hpp"

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsMatchedCount;
using transport::DdsProviderRegistry;
using transport::IDdsProvider;

namespace {
class StubProvider : public IDdsProvider {
 public:
  Coro::Result<void> Init(const DdsConfig&) override { return Coro::Result<void>{}; }
  void   Shutdown() override {}
  Coro::Result<void> DeclareWriter(const std::string&) override {
    return Coro::Result<void>{};
  }
  Coro::Result<void> Publish(const std::string&, const std::vector<uint8_t>&) override {
    return Coro::Result<void>{};
  }
  Coro::Result<void> Subscribe(const std::string&,
                   std::function<void(const std::vector<uint8_t>&)>) override {
    return Coro::Result<void>{};
  }
  Coro::Result<void> Unsubscribe(const std::string&) override { return Coro::Result<void>{}; }
  [[nodiscard]] DdsMatchedCount MatchedCount() const override { return {}; }
  std::string Name() const override { return "stub"; }
};
}  // namespace

TEST(DdsProviderRegistry, RegisterAndCreate) {
  DdsProviderRegistry::RegisterProvider(
      "stub-x", [] { return std::unique_ptr<IDdsProvider>(new StubProvider()); });
  auto p = DdsProviderRegistry::Create("stub-x");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->Name(), "stub");
}

TEST(DdsProviderRegistry, UnknownReturnsNull) {
  EXPECT_EQ(DdsProviderRegistry::Create("no-such-provider"), nullptr);
}

// 内建项:`"fake"` 恒可用(零 FastDDS 依赖),这正是 `DdsConfig::provider` 的缺省值——
// 缺省配置必须能直接 `Start()` 起来,否则 D12 的"已注册"就成了每个调用方的必做样板。
TEST(DdsProviderRegistry, BuiltinFakeIsAlwaysAvailable) {
  auto p = DdsProviderRegistry::Create("fake");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->Name(), "fake");
  EXPECT_EQ(DdsConfig{}.provider, "fake");  // 缺省值与内建名一致。
}

// `"fastdds"` 只在装了 Fast DDS 时存在;缺席时按**未注册**处理(返 nullptr →
// `Start()` 给 kConfiguration),而不是给一个跑不动的实现。
TEST(DdsProviderRegistry, BuiltinFastDdsFollowsBuildConfiguration) {
  auto p = DdsProviderRegistry::Create("fastdds");
#ifdef TRANSPORT_HAS_FASTDDS
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->Name(), "fastdds");
#else
  EXPECT_EQ(p, nullptr);
#endif
}
