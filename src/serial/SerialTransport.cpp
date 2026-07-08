#include "transport/serial/SerialTransport.hpp"

#include <variant>

#include <QSerialPort>

namespace transport {

SerialTransport::SerialTransport(SerialConfig config) : config_(std::move(config)) {}
SerialTransport::~SerialTransport() { Close(); }

Status SerialTransport::Open() {
  port_ = std::make_unique<QSerialPort>();
  port_->setPortName(QString::fromStdString(config_.device));
  if (!port_->open(QIODevice::ReadWrite))
    return Status::Fail("config: open " + config_.device + ": " + port_->errorString().toStdString());

  auto fail = [this](const std::string& m) { port_->close(); return Status::Fail(m); };
  if (!port_->setBaudRate(int(config_.baud_rate))) return fail("config: baud_rate");
  if (!port_->setDataBits(static_cast<QSerialPort::DataBits>(config_.data_bits)))
    return fail("config: data_bits");
  QSerialPort::StopBits sb = (config_.stop_bits == 2) ? QSerialPort::TwoStop
                           : (config_.stop_bits == 1) ? QSerialPort::OneStop
                           : QSerialPort::UnknownStopBits;
  if (sb == QSerialPort::UnknownStopBits) return fail("config: stop_bits must be 1 or 2");
  if (!port_->setStopBits(sb)) return fail("config: stop_bits");
  QSerialPort::Parity par = (config_.parity == 'N') ? QSerialPort::NoParity
                          : (config_.parity == 'E') ? QSerialPort::EvenParity
                          : (config_.parity == 'O') ? QSerialPort::OddParity
                          : QSerialPort::UnknownParity;
  if (par == QSerialPort::UnknownParity) return fail("config: parity must be N/E/O");
  if (!port_->setParity(par)) return fail("config: parity");
  port_->setFlowControl(QSerialPort::NoFlowControl);

  QObject::connect(port_.get(), &QSerialPort::readyRead, [this] { onReadyRead(); });
  QObject::connect(port_.get(),
                   QOverload<QSerialPort::SerialPortError>::of(&QSerialPort::errorOccurred),
                   [this](QSerialPort::SerialPortError e) {
                     if (e == QSerialPort::NoError || disconnected_) return;
                     if (e == QSerialPort::ResourceError || e == QSerialPort::PermissionError) {
                       disconnected_ = true; open_ = false;
                       if (disconnect_cb_) disconnect_cb_("conn: " + port_->errorString().toStdString());
                     }
                   });
  open_ = true;
  if (connect_cb_) connect_cb_();
  return Status::Success(std::monostate{});
}

void SerialTransport::onReadyRead() {
  if (!port_) return;
  QByteArray d = port_->readAll();
  if (d.isEmpty()) return;
  std::vector<uint8_t> bytes(d.begin(), d.end());
  if (bytes_cb_) bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(bytes)), config_.device);
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes) {
  if (!open_ || !port_) return Status::Fail("config: serial not open");
  if (port_->write(reinterpret_cast<const char*>(bytes.data()), qint64(bytes.size())) < 0)
    return Status::Fail("io: write: " + port_->errorString().toStdString());
  return Status::Success(std::monostate{});
}

Status SerialTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {
  if (to.kind != Endpoint::Kind::kDefault)
    return Status::Fail("io: addressed send not supported");
  return Send(bytes);
}

void SerialTransport::Close() {
  disconnected_ = true;
  open_ = false;
  if (port_) { port_->close(); port_.reset(); }
}

}  // namespace transport
