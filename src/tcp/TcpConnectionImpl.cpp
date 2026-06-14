#include "transport/tcp/TcpConnectionImpl.hpp"

#include <utility>

#include "transport/core/StreamSend.hpp"
#include "transport/core/TopicEnvelope.hpp"

// TcpConnectionImpl.cpp — 已连接 socket 的收发实现（见 TcpConnectionImpl.hpp）。
//
// 并发模型：
//  - 所有 socket 读写与共享状态(write_queue_/writing_)访问都经 strand_ 串行化，
//    因此无需额外加锁；
//  - 每个 async 处理器都捕获 self = shared_from_this()，保证对象在挂起的异步
//    操作完成前不被销毁（故本类须以 shared_ptr 持有）；
//  - 断连只处理一次（disconnected_ 闩），accepted 连接断连即终态关队列。

namespace transport {

namespace {
// 取对端地址 "ip:port" 作为 Message.source / 服务端 client_id；取不到则空串。
std::string EndpointId(const asio::ip::tcp::socket& s) {
  asio::error_code ec;
  auto ep = s.remote_endpoint(ec);
  if (ec) return "";
  return ep.address().to_string() + ":" + std::to_string(ep.port());
}
}  // namespace

TcpConnectionImpl::TcpConnectionImpl(asio::ip::tcp::socket socket,
                             std::shared_ptr<IFramer> framer,
                             bool enable_topic_routing)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())),
      assembler_(std::move(framer)),
      peer_id_(EndpointId(socket_)),
      enable_topic_routing_(enable_topic_routing) {}

Status TcpConnectionImpl::Open() {
  bool expected = false;
  if (!open_.compare_exchange_strong(expected, true)) {
    return Status::Success(std::monostate{});  // 已打开
  }
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartRead(); });
  return Status::Success(std::monostate{});
}

bool TcpConnectionImpl::IsOpen() const { return open_.load(); }

// 投递一次 async_read_some；完成回调里把字节喂给 FrameAssembler 切帧、逐帧
// DeliverFrame，然后再次 StartRead 形成接收循环。读到错误/eof → 断连。
void TcpConnectionImpl::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              HandleDisconnect(std::string("conn: ") + ec.message());
              return;
            }
            if (enable_topic_routing_) {
              auto tfs = topic_assembler_.Feed(read_buf_.data(), n);
              if (!tfs) {
                HandleDisconnect(tfs.error);
                return;
              }
              for (auto& tf : tfs.value) {
                core_.DeliverFrame(std::move(tf.body), peer_id_, tf.topic);
              }
            } else {
              auto frames = assembler_.Feed(read_buf_.data(), n);
              if (!frames) {
                HandleDisconnect(frames.error);  // frame: 错误
                return;
              }
              for (auto& f : frames.value) {
                core_.DeliverFrame(std::move(f), peer_id_, "");
              }
            }
            StartRead();
          }));
}

Status TcpConnectionImpl::Send(const std::vector<uint8_t>& data) {
  auto bytes = BuildStreamFrame(core_, enable_topic_routing_, data, "");
  if (!bytes) return Status::Fail(bytes.error);
  if (!open_.load()) return Status::Fail("conn: not connected");
  EnqueueWrite(std::move(bytes.value));
  return Status::Success(std::monostate{});
}

Status TcpConnectionImpl::Send(const Message& msg, const Endpoint& to) {
  (void)to;  // TCP 地址即连接
  auto bytes =
      BuildStreamFrame(core_, enable_topic_routing_, msg.payload, msg.topic);
  if (!bytes) return Status::Fail(bytes.error);
  if (!open_.load()) return Status::Fail("conn: not connected");
  EnqueueWrite(std::move(bytes.value));
  return Status::Success(std::monostate{});
}

void TcpConnectionImpl::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

// 串行化写：一次只 async_write 队首 buffer，完成后弹出并发下一个，直到队空。
// writing_ 标志确保同一时刻只有一个在途写，避免交叠。
void TcpConnectionImpl::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              HandleDisconnect(std::string("conn: ") + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) {
              DoWrite();
            } else {
              writing_ = false;
            }
          }));
}

void TcpConnectionImpl::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;  // 每周期一次
  open_.store(false);
  asio::error_code ec;
  socket_.close(ec);
  core_.DeliverError(reason);   // 唤醒同步等待者 / 投递错误
  core_.Close();           // accepted 连接终态：永久关闭接收队列
  core_.NotifyDisconnect(reason);
}

void TcpConnectionImpl::Close() {
  open_.store(false);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    asio::error_code ec;
    socket_.close(ec);
  });
  core_.Close();
}

}  // namespace transport
