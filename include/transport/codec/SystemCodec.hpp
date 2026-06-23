#pragma once

// SystemCodec.hpp — 外部协议帧 codec(有状态流式)。
// [head_flag:4=AA BB CC DD][frm_type:1][protocol_id:1][session_id:1][reserve:4=0]
// [crc:2 LE][frm_len:2 LE][frm_body: message_id:2 LE | payload]
// CRC 经 CrcFn 注入,校验整个 frm_body;坏帧 resync(前移 1 字节重扫)。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/Result.hpp"

namespace transport {

using CrcFn = std::function<uint16_t(const uint8_t* body, std::size_t len)>;

// 默认占位 CRC16-CCITT(poly 0x1021, init 0xFFFF);真实算法实现前经构造注入替换。
uint16_t DefaultCrc16(const uint8_t* body, std::size_t len);

class SystemCodec : public ICodec {
 public:
  explicit SystemCodec(CrcFn crc = DefaultCrc16);

  Result<std::vector<uint8_t>> Encode(const Message& msg) override;
  Result<std::vector<Message>> Decode(const uint8_t* data, std::size_t len) override;

 private:
  static constexpr std::size_t kHeaderLen = 15;
  static constexpr std::size_t kMaxBody = 65535;
  CrcFn crc_;
  std::vector<uint8_t> buffer_;
};

}  // namespace transport
