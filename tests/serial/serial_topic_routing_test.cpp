#include "transport/serial/SerialImpl.hpp"

#include <pty.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "transport/ICodec.hpp"
#include "transport/Message.hpp"
#include "transport/core/TopicEnvelope.hpp"

using transport::FrameStream;
using transport::ICodec;
using transport::Message;
using transport::Result;
using transport::SerialConfig;
using transport::SerialImpl;
using Bytes = std::vector<uint8_t>;

namespace {
struct Pty {
  int master = -1;
  std::string slave_name;
  Pty() {
    int slave_fd = -1;
    char name[256] = {0};
    if (openpty(&master, &slave_fd, name, nullptr, nullptr) == 0) {
      slave_name = name;
      ::close(slave_fd);
    }
  }
  ~Pty() {
    if (master >= 0) ::close(master);
  }
  bool ok() const { return master >= 0 && !slave_name.empty(); }
  void WriteMaster(const Bytes& d) { ASSERT_GE(::write(master, d.data(), d.size()), 0); }
};

class TagCodec : public ICodec {
 public:
  explicit TagCodec(uint8_t tag) : tag_(tag) {}
  Result<Bytes> Encode(const Bytes& d) override {
    Bytes o{tag_};
    o.insert(o.end(), d.begin(), d.end());
    return Result<Bytes>::Success(std::move(o));
  }
  Result<Bytes> Decode(const Bytes& d) override {
    if (d.empty() || d[0] != tag_) return Result<Bytes>::Fail("codec: bad tag");
    return Result<Bytes>::Success(Bytes(d.begin() + 1, d.end()));
  }

 private:
  uint8_t tag_;
};

SerialConfig RoutingCfg(const std::string& dev) {
  SerialConfig c;
  c.device = dev;
  c.baud_rate = 115200;
  c.enable_topic_routing = true;
  return c;
}
}  // namespace

TEST(SerialTopicRoutingPty, ReceiveDecodesByTopic) {
  Pty pty;
  if (!pty.ok()) GTEST_SKIP() << "openpty unavailable";
  auto s = std::make_shared<SerialImpl>(RoutingCfg(pty.slave_name));
  ASSERT_TRUE(s->Open().ok);
  s->SetCodec("b", std::make_shared<TagCodec>(0xBB));

  Bytes encoded{0xBB, 7, 8};  // = CodecB.Encode({7,8})
  pty.WriteMaster(FrameStream("b", encoded));

  auto r = s->Receive(2000);
  ASSERT_TRUE(r.ok);
  EXPECT_EQ(r.value.topic, "b");
  EXPECT_EQ(r.value.payload, (Bytes{7, 8}));
  s->Close();
}
