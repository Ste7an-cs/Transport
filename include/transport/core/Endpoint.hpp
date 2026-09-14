#pragma once

// -----------------------------------------------------------------------------
// Endpoint.hpp — 统一寻址的中立地址值类型
// 四种 kind:kDefault(用 config 默认目的地)/ kNet(UDP ip:port)/ kTopic(DDS)/
// kService(请求-响应的服务名,ADR-0020 D5)。
// 命名工厂 Default()/Net()/Topic()/Service() 让调用点自解释;与 `Message::endpoint`
// 和 `Datagram::peer` 同一套语义——**发送时是目的地、接收时是来源**,故收到消息可据其
// 直接回发。零第三方依赖。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <utility>

namespace transport {

struct Endpoint {
  /// @brief 地址的种类——决定下面哪些字段有意义。
  ///
  /// `kService` 与 `kTopic` **刻意分开**(ADR-0020 **D5** / ADR-0013 **D6**):服务名不是
  /// topic,它派生出 `cfg.<名>.request` 与 `cfg.<名>.response` 两个 topic。两者共用下面的
  /// `topic` 字段存名字,由本枚举区分——**不为此加第五个字段**。
  enum class Kind { kDefault, kNet, kTopic, kService };

  Kind kind = Kind::kDefault;
  std::string host;     // kNet: ip
  uint16_t port{0};     // kNet
  std::string topic;    // kTopic: topic 名;kService: **服务名**(名字复用本字段,D5)

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
  /// @brief 请求-响应的**服务名**地址(D5):名字存进既有的 `topic` 字段,由 `kind` 区分。
  static Endpoint Service(std::string name) {
    Endpoint e;
    e.kind = Kind::kService;
    e.topic = std::move(name);
    return e;
  }
};

}  // namespace transport
