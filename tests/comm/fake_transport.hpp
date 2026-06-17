#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "transport/ITransport.hpp"
#include "transport/Result.hpp"

namespace testutil {
// 两个 FakeTransport 用 Link 互联:一端 Send → 另一端 OnBytes(同步,投递方线程)。
class FakeTransport : public transport::ITransport,
                      public std::enable_shared_from_this<FakeTransport> {
 public:
  static void Link(const std::shared_ptr<FakeTransport>& a,
                   const std::shared_ptr<FakeTransport>& b) {
    a->peer_ = b; b->peer_ = a;
  }

  transport::Status Open() override {
    open_.store(true);
    if (connect_cb_) connect_cb_();
    return transport::Status::Success(std::monostate{});
  }
  void Close() override {
    if (!open_.exchange(false)) return;
    if (auto p = peer_.lock())
      if (p->open_.load() && p->disconnect_cb_) p->disconnect_cb_("conn: peer closed");
  }
  bool IsOpen() const override { return open_.load(); }

  transport::Status Send(const std::vector<uint8_t>& bytes) override {
    auto p = peer_.lock();
    if (!p || !p->open_.load()) return transport::Status::Fail("conn: peer not open");
    if (p->bytes_cb_)
      p->bytes_cb_(transport::Result<std::vector<uint8_t>>::Success(bytes), "fake");
    return transport::Status::Success(std::monostate{});
  }
  transport::Status Send(const std::vector<uint8_t>& bytes,
                         const transport::Endpoint&) override {
    return Send(bytes);
  }

  void OnBytes(BytesCallback cb) override { bytes_cb_ = std::move(cb); }
  void OnConnect(std::function<void()> cb) override { connect_cb_ = std::move(cb); }
  void OnDisconnect(std::function<void(const std::string&)> cb) override {
    disconnect_cb_ = std::move(cb);
  }

 private:
  std::weak_ptr<FakeTransport> peer_;
  std::atomic<bool> open_{false};
  BytesCallback bytes_cb_;
  std::function<void()> connect_cb_;
  std::function<void(const std::string&)> disconnect_cb_;
};
}  // namespace testutil
