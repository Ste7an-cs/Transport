#pragma once

// -----------------------------------------------------------------------------
// Endpoint.hpp — 统一寻址发送的中立目的地值类型
// 三种 kind:kDefault(用 config 默认目的地)/ kNet(UDP ip:port)/ kTopic(DDS)。
// 命名工厂 Default()/Net()/Topic() 让调用点自解释;与接收侧 Message.source/topic
// 对称——收到消息可据其构造 Endpoint 直接回发。零第三方依赖。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <utility>

namespace transport {

struct Endpoint {
  enum class Kind { kDefault, kNet, kTopic };

  Kind kind = Kind::kDefault;
  std::string host;     // kNet: ip
  uint16_t port{0};     // kNet
  std::string topic;    // kTopic

  static Endpoint Default() { return {}; }
  static Endpoint Net(std::string ip, uint16_t p) {
    Endpoint e;
    e.kind = Kind::kNet;
    e.host = std::move(ip);
    e.port = p;
    return e;
  }
  static Endpoint Topic(std::string name) {
    Endpoint e;
    e.kind = Kind::kTopic;
    e.topic = std::move(name);
    return e;
  }
};

}  // namespace transport
