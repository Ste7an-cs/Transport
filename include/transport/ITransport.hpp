#pragma once

// -----------------------------------------------------------------------------
// ITransport.hpp — 纯字节管道接口
// 只收发裸字节,不知道 Message/ICodec/分帧/交互模式。各实现自有 io 线程 + strand。
// 收侧经 OnBytes 回调(io 线程,串行)交付本次读到的字节切片(流式)或整 datagram(报文式)。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "transport/Endpoint.hpp"
#include "transport/Result.hpp"

namespace transport {

class ITransport {
 public:
  virtual ~ITransport() = default;

  // bytes = 收到的字节(失败时为错误);from = 来源标识("ip:port"/设备路径),失败时空。
  using BytesCallback =
      std::function<void(Result<std::vector<uint8_t>> bytes, const std::string& from)>;

  virtual Status Open() = 0;
  virtual void   Close() = 0;
  virtual bool   IsOpen() const = 0;

  virtual Status Send(const std::vector<uint8_t>& bytes) = 0;
  virtual Status Send(const std::vector<uint8_t>& bytes, const Endpoint& to) = 0;

  virtual void OnBytes(BytesCallback cb) = 0;
  virtual void OnConnect(std::function<void()> cb) = 0;
  virtual void OnDisconnect(std::function<void(const std::string& reason)> cb) = 0;
};

}  // namespace transport
