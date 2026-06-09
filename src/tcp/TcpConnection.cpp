#include "transport/tcp/TcpConnection.hpp"

#include <utility>

namespace transport {

namespace {
std::string EndpointId(const asio::ip::tcp::socket& s) {
  asio::error_code ec;
  auto ep = s.remote_endpoint(ec);
  if (ec) return "";
  return ep.address().to_string() + ":" + std::to_string(ep.port());
}
}  // namespace

TcpConnection::TcpConnection(asio::ip::tcp::socket socket,
                             std::shared_ptr<IFramer> framer)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())),
      assembler_(std::move(framer)),
      peer_id_(EndpointId(socket_)) {}

Status TcpConnection::Open() {
  bool expected = false;
  if (!open_.compare_exchange_strong(expected, true)) {
    return Status::Success(std::monostate{});  // 已打开
  }
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartRead(); });
  return Status::Success(std::monostate{});
}

bool TcpConnection::IsOpen() const { return open_.load(); }

void TcpConnection::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              HandleDisconnect(std::string("conn: ") + ec.message());
              return;
            }
            auto frames = assembler_.Feed(read_buf_.data(), n);
            if (!frames) {
              HandleDisconnect(frames.error);  // frame: 错误
              return;
            }
            for (auto& f : frames.value) {
              DeliverFrame(std::move(f), peer_id_, "");
            }
            StartRead();
          }));
}

Status TcpConnection::Send(const std::vector<uint8_t>& data) {
  if (!open_.load()) return Status::Fail("conn: not connected");
  auto enc = EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  auto self = shared_from_this();
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(enc.value));
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
  return Status::Success(std::monostate{});
}

void TcpConnection::DoWrite() {
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

void TcpConnection::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;  // 每周期一次
  open_.store(false);
  asio::error_code ec;
  socket_.close(ec);
  DeliverError(reason);   // 唤醒同步等待者 / 投递错误
  CloseQueue();           // accepted 连接终态：永久关闭接收队列
  NotifyDisconnect(reason);
}

void TcpConnection::Close() {
  open_.store(false);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    asio::error_code ec;
    socket_.close(ec);
  });
  CloseQueue();
}

}  // namespace transport
