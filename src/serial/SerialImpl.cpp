#include "transport/serial/SerialImpl.hpp"

#include <utility>
#include <variant>

#include "transport/core/TopicEnvelope.hpp"
#include "transport/framing/LengthFieldFramer.hpp"

// SerialImpl.cpp — 串口传输实现（见 SerialImpl.hpp）。
// 并发：自有 io_context + 1 io 线程；读写经 strand_ 串行化；handler 用
// shared_from_this 保活。流式 → assembler_ 切帧后经 core_.DeliverFrame 交付。
// 无重连：read 出错即 HandleDisconnect 终态（disconnected_ 闩一次）。

namespace transport {

SerialImpl::SerialImpl(SerialConfig config)
    : config_(std::move(config)),
      assembler_(config_.framer
                     ? std::make_shared<LengthFieldFramer>(*config_.framer)
                     : nullptr),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      port_(ctx_),
      enable_topic_routing_(config_.enable_topic_routing) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

SerialImpl::~SerialImpl() { Close(); }

bool SerialImpl::IsOpen() const { return open_.load(); }

Status SerialImpl::Open() {
  if (config_.framer) {  // framer 配置校验
    auto v = LengthFieldFramer::ValidateConfig(*config_.framer);
    if (!v) return Status::Fail(v.error);
  }

  asio::error_code ec;
  port_.open(config_.device, ec);
  if (ec) return Status::Fail("config: open " + config_.device + ": " + ec.message());

  auto fail = [&](const std::string& msg) {
    asio::error_code ig;
    port_.close(ig);
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

  // 流控固定 none；best-effort（个别环境/pty 可能不支持，不致命）
  asio::error_code fc_ec;
  port_.set_option(sb::flow_control(sb::flow_control::none), fc_ec);

  open_.store(true);
  auto self = shared_from_this();
  asio::post(strand_, [this, self]() { StartRead(); });
  return Status::Success(std::monostate{});
}

void SerialImpl::StartRead() {
  auto self = shared_from_this();
  port_.async_read_some(
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
                core_.DeliverFrame(std::move(tf.body), config_.device, tf.topic);
              }
            } else {
              auto frames = assembler_.Feed(read_buf_.data(), n);
              if (!frames) {
                HandleDisconnect(frames.error);  // frame: 错误
                return;
              }
              for (auto& f : frames.value) {
                core_.DeliverFrame(std::move(f), config_.device, "");
              }
            }
            StartRead();
          }));
}

Status SerialImpl::Send(const std::vector<uint8_t>& data) {
  if (enable_topic_routing_) {
    Message m;
    m.payload = data;
    return Send(m, Endpoint::Default());
  }
  if (!open_.load()) return Status::Fail("config: serial not open");
  auto enc = core_.EncodeForSend(data);
  if (!enc) return Status::Fail(enc.error);
  EnqueueWrite(std::move(enc.value));
  return Status::Success(std::monostate{});
}

Status SerialImpl::Send(const Message& msg, const Endpoint& to) {
  if (!enable_topic_routing_) {
    if (!msg.topic.empty())
      return Status::Fail("config: topic routing not enabled");
    return Send(msg.payload);
  }
  if (!TopicFitsEnvelope(msg.topic))
    return Status::Fail("frame: topic too long");
  (void)to;
  if (!open_.load()) return Status::Fail("config: serial not open");
  auto enc = core_.EncodeForSend(msg.payload, msg.topic);
  if (!enc) return Status::Fail(enc.error);
  EnqueueWrite(FrameStream(msg.topic, enc.value));
  return Status::Success(std::monostate{});
}

void SerialImpl::EnqueueWrite(std::vector<uint8_t> bytes) {
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf]() {
    write_queue_.push_back(std::move(*buf));
    if (!writing_) DoWrite();
  });
}

void SerialImpl::DoWrite() {
  writing_ = true;
  auto self = shared_from_this();
  asio::async_write(
      port_, asio::buffer(write_queue_.front()),
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

void SerialImpl::HandleDisconnect(const std::string& reason) {
  if (disconnected_.exchange(true)) return;  // 每个打开周期一次
  open_.store(false);
  asio::error_code ig;
  port_.close(ig);
  core_.DeliverError(reason);
  core_.Close();
  core_.NotifyDisconnect(reason);
}

void SerialImpl::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    port_.close(ig);
  });
  core_.Close();
  guard_.reset();
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();
}

}  // namespace transport
