#pragma once

// DatagramCodec.hpp — 报文直通 codec(header-only)。报文式传输(UDP)无分帧:
// Encode 透传 payload;Decode 把整段字节当作一条 kOneway 消息。

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/ICodec.hpp"

namespace transport {

class DatagramCodec : public ICodec {
 public:
  coro::Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    return msg.payload;
  }
  coro::Result<std::vector<Message>> Decode(const uint8_t* data,
                                            std::size_t len) override {
    std::vector<Message> out;
    if (len > 0) {
      Message m;
      m.payload.assign(data, data + len);
      out.push_back(std::move(m));
    }
    return out;
  }
};

}  // namespace transport
