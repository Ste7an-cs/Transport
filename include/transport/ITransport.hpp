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

  // 注册收数据回调(单消费者,后注册覆盖先注册;应在 Open() 之前注册以免漏早到数据)。
  // 谁调/何时调:由本 transport 的 io 线程在异步读完成时调用,绑定在 strand 上 ——
  //   多次回调严格串行、绝不并发;回调在 io 线程执行,必须非阻塞,且不可在其中 Close() 本对象。
  // 每次回调对应:
  //   报文式(UDP)= 一个完整 datagram;
  //   流式(TCP/串口)= 本次 read 到的字节切片(可能是半条/一条/多条粘连)——
  //     「一次回调 ≠ 一条消息」,切帧由上层 ICodec 负责。
  // 参数 from:成功时为来源标识(UDP=发送方 "ip:port" 逐包可变;TCP=对端 "ip:port";
  //   串口=设备路径),失败时为空。
  // 错误分流:UDP 的收/发 I/O 错误经此回调投 Result::Fail(单包出错不致命,继续监听);
  //   TCP/串口的连接级读写错误改走 OnDisconnect(此回调只会收到成功字节)。
  virtual void OnBytes(BytesCallback cb) = 0;
  // 连接建立(含重连成功)时调;UDP/串口在 Open() 成功后调一次。io 线程,非阻塞。
  virtual void OnConnect(std::function<void()> cb) = 0;
  // TCP/串口连接断开时调(reason 带前缀,如 "conn: ...")。io 线程,非阻塞。
  virtual void OnDisconnect(std::function<void(const std::string& reason)> cb) = 0;
};

}  // namespace transport
