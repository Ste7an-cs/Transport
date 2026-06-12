#pragma once

// -----------------------------------------------------------------------------
// IDdsTransport.hpp — DDS 扩展接口（ITransport + 多 topic pub-sub + req-resp）
// 一个实例对应一个 DomainParticipant，内部懒加载维护多个 topic。实现见 DdsImpl。
// 按 topic 发送统一走 `ITransport::Send(data, Endpoint::Topic(...))`。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/dds/DdsConfig.hpp"

namespace transport {

class IDdsTransport : public ITransport {
 public:
  // ---- pub-sub ----
  virtual Status Subscribe(const std::string& topic) = 0;
  virtual Status Unsubscribe(const std::string& topic) = 0;

  // ---- req-resp 客户端 ----
  virtual Status SendRequest(const std::vector<uint8_t>& data,
                             const std::string& topic,
                             std::function<void(Result<Message>)> on_reply,
                             uint32_t timeout_ms = 5000) = 0;

  // ---- req-resp 响应端 ----
  using ReplyFn = std::function<Status(const std::vector<uint8_t>&)>;
  using RequestHandler =
      std::function<void(const Message& request, ReplyFn reply)>;
  virtual Status OnRequest(const std::string& topic,
                           RequestHandler handler) = 0;

  virtual DdsMode Mode() const = 0;
  virtual std::string Provider() const = 0;
};

}  // namespace transport
