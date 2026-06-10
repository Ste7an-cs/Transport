#pragma once

// -----------------------------------------------------------------------------
// Message.hpp — 交付给应用层的一条消息
// 经 ICodec.Decode 后由框架组装：payload（字节）+ topic / source / timestamp。
// 各传输统一以 Result<Message> 经 ReceiveQueue 投递给应用层。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>

namespace transport {

// 经 ICodec.Decode 处理后交付给应用层的一条消息。
struct Message {
  std::vector<uint8_t> payload;       // 解码后的字节流（含用户 header + body）
  std::string topic;                  // DDS topic / 逻辑通道名；TCP/UDP/串口为空
  std::string source;                 // 发送方标识："ip:port"、topic 名、设备路径等
  int64_t timestamp = 0;              // 接收时间戳（微秒，由框架填充）
};

}  // namespace transport
