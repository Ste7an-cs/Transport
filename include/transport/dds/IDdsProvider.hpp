#pragma once

// -----------------------------------------------------------------------------
// IDdsProvider.hpp — 底层 DDS 库抽象（provider 只管「按 RawMessage 在 topic 上
// 收发字节」；关联/超时/codec 全在 DdsImpl 层）。实现：FastDdsProvider（真实）、
// FakeDdsProvider（测试，进程内总线）。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/dds/DdsConfig.hpp"

namespace transport {

class IDdsProvider {
 public:
  virtual ~IDdsProvider() = default;

  // 建立 participant、注册 RawMessage 类型
  virtual Status Init(const DdsConfig& config) = 0;

  // ---- pub-sub ----
  virtual Status Publish(const std::string& topic,
                         const std::vector<uint8_t>& data) = 0;
  virtual Status Subscribe(const std::string& topic,
                           ITransport::ReceiveCallback cb) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;

  // ---- req-resp 客户端 ----
  virtual Status SendRequest(const std::string& request_topic,
                             const std::string& request_id,
                             const std::string& reply_topic,
                             const std::vector<uint8_t>& data) = 0;
  using ReplySink = std::function<void(const std::string& request_id,
                                       const std::vector<uint8_t>& payload)>;
  virtual Status SubscribeReplies(const std::string& reply_topic,
                                  ReplySink sink) = 0;

  // ---- req-resp 响应端 ----
  using RequestSink = std::function<void(const std::vector<uint8_t>& payload,
                                         const std::string& request_id,
                                         const std::string& reply_topic)>;
  virtual Status ServeRequests(const std::string& request_topic,
                               RequestSink sink) = 0;
  virtual Status Reply(const std::string& reply_topic,
                       const std::string& request_id,
                       const std::vector<uint8_t>& data) = 0;

  virtual void Shutdown() = 0;
  virtual std::string ProviderName() const = 0;
};

}  // namespace transport
