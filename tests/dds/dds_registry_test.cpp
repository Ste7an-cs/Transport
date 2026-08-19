#include "transport/io/dds/DdsProviderRegistry.hpp"

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsProviderRegistry;
using transport::IDdsProvider;

namespace {
class StubProvider : public IDdsProvider {
 public:
  Coro::Result<void> Init(const DdsConfig&) override { return Coro::Result<void>{}; }
  void   Shutdown() override {}
  Coro::Result<void> Publish(const std::string&, const std::vector<uint8_t>&) override {
    return Coro::Result<void>{};
  }
  Coro::Result<void> Subscribe(const std::string&,
                   std::function<void(const std::vector<uint8_t>&)>) override {
    return Coro::Result<void>{};
  }
  Coro::Result<void> Unsubscribe(const std::string&) override { return Coro::Result<void>{}; }
  std::string Name() const override { return "stub"; }
};
}  // namespace

TEST(DdsProviderRegistry, RegisterAndCreate) {
  DdsProviderRegistry::RegisterProvider(
      "stub-x", [] { return std::make_unique<StubProvider>(); });
  auto p = DdsProviderRegistry::Create("stub-x");
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->Name(), "stub");
}

TEST(DdsProviderRegistry, UnknownReturnsNull) {
  EXPECT_EQ(DdsProviderRegistry::Create("no-such-provider"), nullptr);
}
