#include "transport/tcp/TcpConnectionImpl.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <gtest/gtest.h>

#include "transport/ICodec.hpp"
#include "transport/framing/LengthFieldFramer.hpp"

using transport::ICodec;
using transport::LengthFieldFramer;
using transport::LengthFieldFramerConfig;
using transport::Result;
using transport::TcpConnectionImpl;

namespace {

// 在 127.0.0.1 建一对已连接 socket：返回 {server_side, client_side}
struct Pair {
  asio::ip::tcp::socket server;
  asio::ip::tcp::socket client;
};

Pair MakeConnectedPair(asio::io_context& ctx) {
  asio::ip::tcp::acceptor acc(ctx, asio::ip::tcp::endpoint(
                                       asio::ip::make_address("127.0.0.1"), 0));
  auto port = acc.local_endpoint().port();
  asio::ip::tcp::socket client(ctx);
  client.connect(asio::ip::tcp::endpoint(asio::ip::make_address("127.0.0.1"), port));
  asio::ip::tcp::socket server = acc.accept();
  return Pair{std::move(server), std::move(client)};
}

LengthFieldFramerConfig BeConfig() {
  LengthFieldFramerConfig c;
  c.header_size = 8;
  c.length_offset = 4;
  c.length_size = 4;
  c.big_endian = true;
  c.max_frame_size = 1024;
  return c;
}

std::vector<uint8_t> BuildFrame(uint32_t body_len, uint8_t fill) {
  std::vector<uint8_t> buf(8, 0x00);
  buf[4] = static_cast<uint8_t>((body_len >> 24) & 0xFF);
  buf[5] = static_cast<uint8_t>((body_len >> 16) & 0xFF);
  buf[6] = static_cast<uint8_t>((body_len >> 8) & 0xFF);
  buf[7] = static_cast<uint8_t>(body_len & 0xFF);
  buf.insert(buf.end(), body_len, fill);
  return buf;
}

void BlockingWriteAll(asio::ip::tcp::socket& s, const std::vector<uint8_t>& data) {
  asio::write(s, asio::buffer(data));
}

// 在每个字节上 +1/-1 的可逆 codec
class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto out = d;
    for (auto& b : out) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto out = d;
    for (auto& b : out) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(out));
  }
};

// 运行 io_context 的后台线程封装
struct IoRunner {
  asio::io_context ctx;
  asio::executor_work_guard<asio::io_context::executor_type> guard{ctx.get_executor()};
  std::thread th{[this] { ctx.run(); }};
  ~IoRunner() {
    guard.reset();
    ctx.stop();
    if (th.joinable()) th.join();
  }
};

}  // namespace

TEST(TcpConnectionImpl, PassthroughReceivesRawBytes) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  conn->Open();

  BlockingWriteAll(pair.client, {10, 20, 30});
  auto r = conn->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_FALSE(r.value.source.empty());  // "127.0.0.1:port"
  conn->Close();
}

TEST(TcpConnectionImpl, FramerAssemblesAcrossWrites) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto framer = std::make_shared<LengthFieldFramer>(BeConfig());
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), framer);
  conn->Open();

  auto frame = BuildFrame(5, 0xAB);  // 13 字节
  BlockingWriteAll(pair.client, std::vector<uint8_t>(frame.begin(), frame.begin() + 6));
  BlockingWriteAll(pair.client, std::vector<uint8_t>(frame.begin() + 6, frame.end()));

  auto r = conn->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, frame);
  conn->Close();
}

TEST(TcpConnectionImpl, SendWritesToPeer) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  conn->Open();

  auto st = conn->Send({1, 2, 3, 4});
  ASSERT_TRUE(static_cast<bool>(st));

  std::vector<uint8_t> got(4);
  asio::read(pair.client, asio::buffer(got));
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3, 4}));
  conn->Close();
}

TEST(TcpConnectionImpl, CodecAppliedBothDirections) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  conn->SetCodec(std::make_shared<ShiftCodec>());
  conn->Open();

  // 发送：{1,2,3} 经 Encode(+1) → 对端应收 {2,3,4}
  ASSERT_TRUE(static_cast<bool>(conn->Send({1, 2, 3})));
  std::vector<uint8_t> got(3);
  asio::read(pair.client, asio::buffer(got));
  EXPECT_EQ(got, (std::vector<uint8_t>{2, 3, 4}));

  // 接收：对端发 {2,3,4} 经 Decode(-1) → payload {1,2,3}
  BlockingWriteAll(pair.client, {2, 3, 4});
  auto r = conn->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  conn->Close();
}

TEST(TcpConnectionImpl, PeerCloseTriggersDisconnectAndConnError) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), nullptr);
  std::string reason;
  conn->OnDisconnect([&](const std::string& r) { reason = r; });
  conn->Open();

  pair.client.close();  // 对端关闭
  auto r = conn->Receive(1000);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
  // OnDisconnect 应已触发（io 线程异步，给出充分超时）
  for (int i = 0; i < 100 && reason.empty(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_EQ(reason.rfind("conn:", 0), 0u);
}

TEST(TcpConnectionImpl, FrameErrorDeliversFrameError) {
  IoRunner io;
  auto pair = MakeConnectedPair(io.ctx);
  auto c = BeConfig();
  c.max_frame_size = 4;  // 任何正常帧都会越界
  auto framer = std::make_shared<LengthFieldFramer>(c);
  auto conn = std::make_shared<TcpConnectionImpl>(std::move(pair.server), framer);
  conn->Open();

  BlockingWriteAll(pair.client, BuildFrame(100, 0x44));  // 触发 frame: 错误
  auto r = conn->Receive(1000);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("frame:", 0), 0u);
  conn->Close();
}
