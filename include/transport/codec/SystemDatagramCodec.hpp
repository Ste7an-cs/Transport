#pragma once

// SystemDatagramCodec.hpp — 外部协议帧的【无状态报文版】codec(header-only)。
// 与流式 SystemCodec 共用 EncodeSystemFrame/ScanSystemFrames;每次 Decode 只解这一个
// datagram、吐出其中整帧,残留直接丢弃、零跨报文保留 → 适配 UDP(多对端安全)。

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "transport/codec/ICodec.hpp"
#include "transport/core/Message.hpp"
#include "transport/codec/SystemCodec.hpp"   // CrcFn / DefaultCrc16 / EncodeSystemFrame / ScanSystemFrames

namespace transport {

class SystemDatagramCodec : public ICodec {
 public:
  explicit SystemDatagramCodec(CrcFn crc = DefaultCrc16) : crc_(std::move(crc)) {}

  Result<std::vector<uint8_t>> Encode(const Message& msg) override {
    return EncodeSystemFrame(msg, crc_);
  }
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override {
    std::vector<Message> out;
    (void)ScanSystemFrames(data, len, crc_, out);   // 残留丢弃,不跨报文
    return out;
  }

 private:
  CrcFn crc_;
};

}  // namespace transport
