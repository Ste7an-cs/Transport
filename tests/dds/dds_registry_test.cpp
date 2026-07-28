#include "transport/dds/DdsProviderRegistry.hpp"

#include <gtest/gtest.h>

using transport::DdsConfig;
using transport::DdsProviderRegistry;
using transport::IDdsProvider;
using transport::Status;

namespace {
class StubProvider : public IDdsProvider {
 public:
  Status Init(const DdsConfig&) override { return Status{}; }
  void   Shutdown() override {}
  Status Publish(const std::string&, const std::vector<uint8_t>&) override {
    return Status{};
  }
  Status Subscribe(const std::string&,
                   std::function<void(const std::vector<uint8_t>&)>) override {
    return Status{};
  }
  Status Unsubscribe(const std::string&) override { return Status{}; }
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
