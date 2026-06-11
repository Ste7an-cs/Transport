#include "transport/serial/SerialImpl.hpp"

#include <pty.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"
#include "transport/framing/LengthFieldFramer.hpp"

using transport::ICodec;
using transport::LengthFieldFramerConfig;
using transport::Result;
using transport::SerialConfig;
using transport::SerialImpl;

namespace {

// 主从伪终端：SerialImpl 打开从端(by name)，测试代码直接读写主端 fd。
struct Pty {
  int master = -1;
  std::string slave_name;
  Pty() {
    int slave_fd = -1;
    char name[256] = {0};
    if (openpty(&master, &slave_fd, name, nullptr, nullptr) == 0) {
      slave_name = name;
      ::close(slave_fd);  // 让 SerialImpl 成为从端唯一打开者
    }
  }
  ~Pty() { if (master >= 0) ::close(master); }
  bool ok() const { return master >= 0 && !slave_name.empty(); }
  void WriteMaster(const std::vector<uint8_t>& d) {
    ASSERT_GE(::write(master, d.data(), d.size()), 0);
  }
  std::vector<uint8_t> ReadMaster(size_t n) {
    std::vector<uint8_t> buf(n);
    ssize_t r = ::read(master, buf.data(), n);
    buf.resize(r > 0 ? static_cast<size_t>(r) : 0);
    return buf;
  }
};

SerialConfig Cfg(const std::string& dev) {
  SerialConfig c;
  c.device = dev;
  c.baud_rate = 115200;
  return c;
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

class ShiftCodec : public ICodec {
 public:
  Result<std::vector<uint8_t>> Encode(const std::vector<uint8_t>& d) override {
    auto o = d;
    for (auto& b : o) b = static_cast<uint8_t>(b + 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
  Result<std::vector<uint8_t>> Decode(const std::vector<uint8_t>& d) override {
    auto o = d;
    for (auto& b : o) b = static_cast<uint8_t>(b - 1);
    return Result<std::vector<uint8_t>>::Success(std::move(o));
  }
};

}  // namespace

TEST(SerialTransport, PassthroughReceive) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  pty.WriteMaster({10, 20, 30});
  auto r = s->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{10, 20, 30}));
  EXPECT_EQ(r.value.source, pty.slave_name);
  s->Close();
}

TEST(SerialTransport, FramerAssemblesAcrossReads) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  SerialConfig cfg = Cfg(pty.slave_name);
  cfg.framer = BeConfig();
  auto s = std::make_shared<SerialImpl>(cfg);
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  auto frame = BuildFrame(5, 0xAB);  // 13 字节
  pty.WriteMaster(std::vector<uint8_t>(frame.begin(), frame.begin() + 6));
  pty.WriteMaster(std::vector<uint8_t>(frame.begin() + 6, frame.end()));

  auto r = s->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, frame);
  s->Close();
}

TEST(SerialTransport, SendWritesToPeer) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  ASSERT_TRUE(static_cast<bool>(s->Send({1, 2, 3, 4})));
  auto got = pty.ReadMaster(4);
  EXPECT_EQ(got, (std::vector<uint8_t>{1, 2, 3, 4}));
  s->Close();
}

TEST(SerialTransport, CodecBothDirections) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  s->SetCodec(std::make_shared<ShiftCodec>());
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  // 发：{1,2,3} 经 Encode(+1) → 主端读到 {2,3,4}
  ASSERT_TRUE(static_cast<bool>(s->Send({1, 2, 3})));
  auto got = pty.ReadMaster(3);
  EXPECT_EQ(got, (std::vector<uint8_t>{2, 3, 4}));

  // 收：主端写 {2,3,4} 经 Decode(-1) → payload {1,2,3}
  pty.WriteMaster({2, 3, 4});
  auto r = s->Receive(1000);
  ASSERT_TRUE(static_cast<bool>(r));
  EXPECT_EQ(r.value.payload, (std::vector<uint8_t>{1, 2, 3}));
  s->Close();
}

TEST(SerialTransport, PeerCloseTriggersDisconnect) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  auto s = std::make_shared<SerialImpl>(Cfg(pty.slave_name));
  std::string reason;
  s->OnDisconnect([&](const std::string& r) { reason = r; });
  ASSERT_TRUE(static_cast<bool>(s->Open()));

  ::close(pty.master);  // 关主端 → 从端 read 出错
  pty.master = -1;
  auto r = s->Receive(1000);
  EXPECT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("conn:", 0), 0u);
  for (int i = 0; i < 100 && reason.empty(); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_EQ(reason.rfind("conn:", 0), 0u);
  s->Close();
}

TEST(SerialTransport, InvalidParityRejected) {
  Pty pty;
  ASSERT_TRUE(pty.ok());
  SerialConfig cfg = Cfg(pty.slave_name);
  cfg.parity = 'X';
  auto s = std::make_shared<SerialImpl>(cfg);
  auto st = s->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_EQ(st.error.rfind("config:", 0), 0u);
  s->Close();
}
