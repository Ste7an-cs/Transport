#pragma once
#include <chrono>
#include <functional>
#include <string>
#include <variant>
#include <vector>

#include <boost/fiber/operations.hpp>  // boost::this_fiber::sleep_for

#include "transport/ITransport.hpp"

namespace testutil {

// 可编程假传输:记录 Send 的字节;inject() 在当前(fiber)线程把一帧交给引擎 demux。
class FakeTransport : public transport::ITransport {
 public:
  std::vector<std::vector<uint8_t>> sent;

  transport::Status Open() override { open_ = true; return transport::Status::Success(std::monostate{}); }
  void Close() override { open_ = false; }
  bool IsOpen() const override { return open_; }
  transport::Status Send(const std::vector<uint8_t>& b) override {
    sent.push_back(b); return transport::Status::Success(std::monostate{});
  }
  transport::Status Send(const std::vector<uint8_t>& b, const transport::Endpoint&) override { return Send(b); }
  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override { disconnect_cb_ = std::move(cb); }

  // 注入一帧字节(触发引擎 demux)。
  void inject(const std::vector<uint8_t>& bytes, const std::string& from = "test") {
    if (bytes_cb_) bytes_cb_(transport::Result<std::vector<uint8_t>>::Success(bytes), from);
  }
  // 模拟对端断开。
  void dropPeer(const std::string& reason = "conn: peer reset") {
    if (disconnect_cb_) disconnect_cb_(reason);
  }

 private:
  bool open_ = false;
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};

// 在 fiber 里让出并推进时间,直到 pred() 为真或超过 budget_ms。sleep_for 既让出其他 fiber、
// 又推进 fiber 调度器时钟(供 await_for 超时),避免忙等。
inline bool pumpFiberUntil(std::function<bool()> pred, int budget_ms = 3000) {
  for (int i = 0; i < budget_ms && !pred(); ++i)
    boost::this_fiber::sleep_for(std::chrono::milliseconds(1));
  return pred();
}

}  // namespace testutil
