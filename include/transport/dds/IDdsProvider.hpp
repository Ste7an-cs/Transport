#pragma once

// IDdsProvider.hpp — 底层 DDS 库抽象(只管「按 topic 收发不透明字节」)。
// 实现:FakeDdsProvider(进程内总线)、FastDdsProvider(真实,可选)。

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/Result.hpp"
#include "transport/dds/DdsConfig.hpp"

namespace transport {

class IDdsProvider {
 public:
  virtual ~IDdsProvider() = default;

  virtual Status Init(const DdsConfig& config) = 0;
  virtual void   Shutdown() = 0;

  virtual Status Publish(const std::string& topic,
                         const std::vector<uint8_t>& bytes) = 0;
  // cb 在样本到达时被 provider 调用(FastDDS=该 topic 的 listener 线程;Fake=发布线程)。
  virtual Status Subscribe(const std::string& topic,
                           std::function<void(const std::vector<uint8_t>&)> cb) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;

  virtual std::string Name() const = 0;
};

}  // namespace transport
