#pragma once

// DatagramCodec.hpp — 报文直通 codec(header-only)。报文式传输(UDP)无分帧:
// Encode 透传 payload(忽略 frame,ADR-0020 D7);Decode 把整段字节当作一条 kOneway 消息
// ——**整个报文即一帧**,故 frame 是整段字节、payload 是它【全长】的视图(D2)。

#include <cstddef>
#include <cstdint>
#include <vector>

#include <QByteArray>

#include "transport/codec/ICodec.hpp"

namespace transport {

class DatagramCodec : public ICodec {
 public:
  Coro::Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    const auto* p = reinterpret_cast<const uint8_t*>(msg.payload.constData());
    return std::vector<uint8_t>(p, p + msg.payload.size());
  }
  Coro::Result<std::vector<Message>> Decode(const uint8_t* data,
                                            std::size_t len) override {
    std::vector<Message> out;
    if (len > 0) {
      Message m;
      // 本 codec 无帧头:整帧 = 整段报文,payload 偏移 0、长度即全长。
      // ★ 顺序:先落 frame,再在【它】上面建视图(D2)。
      m.frame = QByteArray(reinterpret_cast<const char*>(data),
                           static_cast<int>(len));
      m.payload = QByteArray::fromRawData(m.frame.constData(),
                                          static_cast<int>(len));
      out.push_back(std::move(m));
    }
    return out;
  }
};

}  // namespace transport
