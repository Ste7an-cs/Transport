#include "transport/tcp/TcpConnection.hpp"

#include <utility>
#include <variant>

// TcpConnection.cpp — 一条【已连接】的 TCP 字节管道(ITransport 实现)。
// client 端(TcpClientTransport 内部)与服务端 accept 来的连接共用本类。
// TCP 是字节流:一次 read 交付的是本次读到的切片(可能含半条/一条/多条消息),
// 切帧交上层 ICodec。所有 socket 操作绑 strand_ 串行;写有队列(背靠背 async_write)。
//
// 断开语义:operation_aborted(本端主动 Close 引起)在读写 handler 里被跳过 → 故主动
// Close 不触发 OnDisconnect;只有真实对端断开(eof/reset)才经 HandleDisconnect 上报【一次】
// (disconnected_ 一次性闸)。

namespace transport {

// 接管一个已连接 socket(由 client connect 或 server accept 得来)。strand 绑到 socket 的 executor。
TcpConnection::TcpConnection(asio::ip::tcp::socket socket)
    : socket_(std::move(socket)),
      strand_(asio::make_strand(socket_.get_executor())) {}

TcpConnection::~TcpConnection() {
  asio::error_code ig;
  socket_.close(ig);
}

// Open:记下对端 "ip:port" 作 from 标识,post 到 strand 回 OnConnect 并启动读循环。
Status TcpConnection::Open() {
  asio::error_code ec;
  auto ep = socket_.remote_endpoint(ec);
  if (!ec) peer_id_ = ep.address().to_string() + ":" + std::to_string(ep.port());
  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartRead();
  });
  return Status::Success(std::monostate{});
}

// StartRead:发起一次 async_read_some;成功交付切片后再次 StartRead 形成读循环。
// 连接级错误(非 aborted)→ HandleDisconnect(一次性上报并关 socket)。
void TcpConnection::StartRead() {
  auto self = shared_from_this();
  socket_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              if (ec == asio::error::operation_aborted) return;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            if (bytes_cb_) {
              std::vector<uint8_t> chunk(read_buf_.begin(), read_buf_.begin() + n);
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(chunk)), peer_id_);
            }
            StartRead();
          }));
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: connection not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status TcpConnection::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

// EnqueueWrite:把一段字节入写队列(在 strand 上),若当前没在写就启动写。
// 写队列 + writing_ 标志保证同一时刻只有一个 async_write 在飞(TCP 写不能并发交叠)。
void TcpConnection::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

// DoWrite:写队首一帧;完成后弹出,队列非空则继续写下一帧(背靠背),否则置 writing_=false。
void TcpConnection::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      socket_, asio::buffer(write_queue_.front()),  // 全写完才回调(asio::async_write 语义)
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              if (ec == asio::error::operation_aborted) return;  // 主动关,正常退出
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

// HandleDisconnect:真实断开(eof/reset/写错)时上报【一次】(disconnected_ 一次性闸),
// 关 socket、回 OnDisconnect。
void TcpConnection::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;     // 已上报过 → 不重复
  open_.store(false);
  asio::error_code ig;
  socket_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
}

// Close:本端主动关。先置 disconnected_(抑制 OnDisconnect)再 post 关 socket
// (令在途读写以 operation_aborted 退出)。幂等。
void TcpConnection::Close() {
  if (closing_.exchange(true)) return;
  disconnected_.store(true);                    // 主动关不算"断线",不回 OnDisconnect
  open_.store(false);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    asio::error_code ig;
    socket_.close(ig);
  });
}

}  // namespace transport
