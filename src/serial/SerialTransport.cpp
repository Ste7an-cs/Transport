#include "transport/serial/SerialTransport.hpp"

#include <utility>
#include <variant>

// SerialTransport.cpp — 见 .hpp。读到的字节切片经 OnBytes 直接交付(无分帧)。

namespace transport {

SerialTransport::SerialTransport(SerialConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      port_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

SerialTransport::~SerialTransport() { Close(); }

Status SerialTransport::Open() {
  asio::error_code ec;
  port_.open(config_.device, ec);
  if (ec) return Status::Fail("config: open " + config_.device + ": " + ec.message());

  auto fail = [&](const std::string& msg) {
    asio::error_code ig; port_.close(ig);
    return Status::Fail(msg);
  };
  using sb = asio::serial_port_base;
  port_.set_option(sb::baud_rate(config_.baud_rate), ec);
  if (ec) return fail("config: baud_rate: " + ec.message());
  port_.set_option(sb::character_size(config_.data_bits), ec);
  if (ec) return fail("config: data_bits: " + ec.message());

  sb::stop_bits::type sbits;
  if (config_.stop_bits == 1) sbits = sb::stop_bits::one;
  else if (config_.stop_bits == 2) sbits = sb::stop_bits::two;
  else return fail("config: stop_bits must be 1 or 2");
  port_.set_option(sb::stop_bits(sbits), ec);
  if (ec) return fail("config: stop_bits: " + ec.message());

  sb::parity::type par;
  switch (config_.parity) {
    case 'N': par = sb::parity::none; break;
    case 'E': par = sb::parity::even; break;
    case 'O': par = sb::parity::odd; break;
    default: return fail("config: parity must be N/E/O");
  }
  port_.set_option(sb::parity(par), ec);
  if (ec) return fail("config: parity: " + ec.message());

  asio::error_code fc_ec;
  port_.set_option(sb::flow_control(sb::flow_control::none), fc_ec);  // best-effort

  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();
    StartRead();
  });
  return Status::Success(std::monostate{});
}

void SerialTransport::StartRead() {
  auto self = shared_from_this();
  port_.async_read_some(
      asio::buffer(read_buf_),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (ec) {
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            if (bytes_cb_) {
              std::vector<uint8_t> chunk(read_buf_.begin(), read_buf_.begin() + n);
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(chunk)),
                        config_.device);
            }
            StartRead();
          }));
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_.load()) return Status::Fail("config: serial not open");
  EnqueueWrite(bytes);
  return Status::Success(std::monostate{});
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void SerialTransport::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void SerialTransport::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      port_, asio::buffer(write_queue_.front()),
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t) {
            if (ec) {
              writing_ = false;
              HandleDisconnect("conn: " + ec.message());
              return;
            }
            write_queue_.pop_front();
            if (!write_queue_.empty()) DoWrite();
            else writing_ = false;
          }));
}

void SerialTransport::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;
  open_.store(false);
  asio::error_code ig; port_.close(ig);
  if (disconnect_cb_) disconnect_cb_(reason);
}

void SerialTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig; port_.close(ig);
  });
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
