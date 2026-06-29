#include "transport/udp/UdpTransport.hpp"

#include <utility>
#include <variant>

// UdpTransport.cpp — UDP 纯字节管道(ITransport 实现)。
// 自有 io_context + 1 条 io 线程跑 ctx_.run();所有 socket 操作绑到 strand_ 上串行
// (收回调与发送都 post 到 strand → 无需自己加锁)。UDP 报文保边界:每个 datagram
// 原样经 OnBytes 交付一段字节 + from="ip:port"(切帧?不需要,报文即一条)。
// 单类支持单播/组播/广播(UdpMode)。

namespace transport {

// 构造即起 io 线程。guard_(work guard)让 ctx_.run() 在没有未决 io 时也不退出,直到
// Close() 里 guard_.reset()+ctx_.stop()。socket_ 此时仅构造、未 open(Open() 才 open/bind)。
UdpTransport::UdpTransport(UdpConfig config)
    : config_(std::move(config)),
      guard_(ctx_.get_executor()),
      strand_(ctx_.get_executor()),
      socket_(ctx_) {
  io_thread_ = std::thread([this] { ctx_.run(); });
}

UdpTransport::~UdpTransport() { Close(); }

bool UdpTransport::IsOpen() const { return open_.load(); }

// Open:open socket → 按模式设选项 → bind 本地 → (组播)join group / (单播广播)解析默认目的地。
// 任一步失败返回 config: 错误。成功后 post 到 strand:回 OnConnect 并启动收循环。
Status UdpTransport::Open() {
  asio::error_code ec;
  socket_.open(asio::ip::udp::v4(), ec);
  if (ec) return Status::Fail("config: udp open: " + ec.message());

  if (config_.mode == UdpMode::kBroadcast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
    socket_.set_option(asio::socket_base::broadcast(true), ec);
    if (ec) return Status::Fail("config: broadcast option: " + ec.message());
  } else if (config_.mode == UdpMode::kMulticast) {
    socket_.set_option(asio::socket_base::reuse_address(true), ec);
  }

  asio::ip::address local;
  if (config_.mode == UdpMode::kMulticast) {
    local = asio::ip::address_v4::any();
  } else {
    local = asio::ip::make_address(config_.local_addr, ec);
    if (ec) return Status::Fail("config: invalid local_addr");
  }
  socket_.bind(asio::ip::udp::endpoint(local, config_.local_port), ec);
  if (ec) return Status::Fail("config: bind: " + ec.message());

  if (config_.mode == UdpMode::kMulticast) {
    auto group = asio::ip::make_address(config_.multicast_group, ec);
    if (ec) return Status::Fail("config: invalid multicast_group");
    socket_.set_option(asio::ip::multicast::join_group(group), ec);
    if (ec) return Status::Fail("config: join_group: " + ec.message());
    asio::error_code ig;
    socket_.set_option(asio::ip::multicast::hops(config_.ttl), ig);
    socket_.set_option(asio::ip::multicast::enable_loopback(true), ig);
    default_dest_ = asio::ip::udp::endpoint(group, config_.remote_port);
  } else if (!config_.remote_addr.empty()) {
    auto raddr = asio::ip::make_address(config_.remote_addr, ec);
    if (ec) return Status::Fail("config: invalid remote_addr");
    default_dest_ = asio::ip::udp::endpoint(raddr, config_.remote_port);
  }

  asio::error_code lec;
  auto le = socket_.local_endpoint(lec);
  if (lec) return Status::Fail("config: local_endpoint: " + lec.message());
  local_port_ = le.port();                       // OS 分配的端口(local_port=0 时取回)
  open_.store(true);
  auto self = shared_from_this();                // 保活:确保异步链运行期间对象不析构
  asio::post(strand_, [this, self]() {
    if (connect_cb_) connect_cb_();              // UDP 无连接,Open 成功即视为"已连接"
    StartReceive();
  });
  return Status::Success(std::monostate{});
}

// StartReceive:发起一次异步收;回调里交付 datagram 后【再次】StartReceive 形成收循环。
// 全在 strand 上 → 串行,无并发。
void UdpTransport::StartReceive() {
  auto self = shared_from_this();
  socket_.async_receive_from(
      asio::buffer(recv_buf_), recv_from_,       // recv_from_ 收完即对端地址
      asio::bind_executor(
          strand_, [this, self](asio::error_code ec, std::size_t n) {
            if (!open_.load()) return;           // 已 Close,停收
            if (ec) {
              if (ec == asio::error::operation_aborted) return;  // socket.close 引发,正常退出
              // 单包 I/O 错误:经 OnBytes 投 Fail(不致命),继续收(UDP 无"连接"可断)。
              if (bytes_cb_)
                bytes_cb_(Result<std::vector<uint8_t>>::Fail("io: receive: " + ec.message()), "");
              StartReceive();
              return;
            }
            std::string from = recv_from_.address().to_string() + ":" +  // "ip:port"
                               std::to_string(recv_from_.port());
            std::vector<uint8_t> dg(recv_buf_.begin(), recv_buf_.begin() + n);
            if (bytes_cb_)
              bytes_cb_(Result<std::vector<uint8_t>>::Success(std::move(dg)), from);  // 一 datagram 一交付
            StartReceive();                      // 收下一个
          }));
}

// ResolveDest:把中立 Endpoint 解析成 asio UDP 目的地。kDefault→配置默认目的地;
// kNet→解析 ip:port;kTopic→UDP 不支持(报 config:)。
Result<asio::ip::udp::endpoint> UdpTransport::ResolveDest(const Endpoint& to) {
  using R = Result<asio::ip::udp::endpoint>;
  switch (to.kind) {
    case Endpoint::Kind::kDefault:
      return R::Success(default_dest_);
    case Endpoint::Kind::kNet: {
      asio::error_code ec;
      auto addr = asio::ip::make_address(to.host, ec);
      if (ec) return R::Fail("config: invalid address");
      return R::Success(asio::ip::udp::endpoint(addr, to.port));
    }
    case Endpoint::Kind::kTopic:
      return R::Fail("config: udp expects net endpoint");
  }
  return R::Fail("config: unknown endpoint kind");
}

// SendRaw:异步发。把字节拷进 shared_ptr 缓冲(异步期间需存活),post 到 strand 上 send_to。
// 返回的 Status 表示"已入队",不代表对端已收。发失败经 OnBytes 投 Fail(不致命)。
Status UdpTransport::SendRaw(std::vector<uint8_t> bytes,
                             const asio::ip::udp::endpoint& dest) {
  if (!open_.load()) return Status::Fail("config: socket not open");
  auto buf = std::make_shared<std::vector<uint8_t>>(std::move(bytes));  // 异步期间持有字节
  auto self = shared_from_this();
  asio::post(strand_, [this, self, buf, dest]() {
    asio::error_code ec;
    socket_.send_to(asio::buffer(*buf), dest, 0, ec);
    if (ec && bytes_cb_)
      bytes_cb_(Result<std::vector<uint8_t>>::Fail("io: send: " + ec.message()), "");
  });
  return Status::Success(std::monostate{});
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes) {           // 发往默认目的地
  return SendRaw(bytes, default_dest_);
}

Status UdpTransport::Send(const std::vector<uint8_t>& bytes, const Endpoint& to) {  // 运行期寻址
  auto dest = ResolveDest(to);
  if (!dest) return Status::Fail(dest.error);
  return SendRaw(bytes, dest.value);
}

// Close:幂等。停收发(open_=false)→ post 关 socket(会令在途 async_receive_from 以
// operation_aborted 退出)→ 放 work guard、stop ctx → join io 线程。join 保证回调不再跑。
void UdpTransport::Close() {
  if (closing_.exchange(true)) return;
  open_.store(false);
  asio::post(strand_, [this]() {
    asio::error_code ig;
    socket_.close(ig);
  });
  guard_.reset();                                // 撤销 work guard,允许 run() 退出
  ctx_.stop();
  if (io_thread_.joinable()) io_thread_.join();  // 等 io 线程退出 → 之后无回调
}

}  // namespace transport
