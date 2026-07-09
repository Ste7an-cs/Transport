#include "transport/serial/SerialTransport.hpp"
#include "qt_test_util.hpp"
#include <array>
#include <cstdio>
#include <memory>
#include <regex>
#include <string>
#include <QProcess>
#include <gtest/gtest.h>

using transport::Result;
using transport::SerialConfig;
using transport::SerialTransport;
using qtutil::pumpUntil; using qtutil::B;

namespace {
// 起 socat 造一对虚拟串口,解析出两个 /dev/pts/N。socat 不可用返回 false。
bool StartSocat(QProcess& p, std::string& dev_a, std::string& dev_b) {
  p.start("socat", {"-d", "-d", "pty,raw,echo=0", "pty,raw,echo=0"});
  if (!p.waitForStarted(1000)) return false;
  std::string err; std::regex re("(/dev/pts/[0-9]+)");
  for (int i = 0; i < 50 && dev_b.empty(); ++i) {
    p.waitForReadyRead(100);
    err += p.readAllStandardError().toStdString();
    std::smatch m; auto begin = err.cbegin();
    std::vector<std::string> found;
    for (std::sregex_iterator it(err.begin(), err.end(), re), e; it != e; ++it) found.push_back((*it)[1]);
    if (found.size() >= 2) { dev_a = found[0]; dev_b = found[1]; }
  }
  return !dev_a.empty() && !dev_b.empty();
}
}  // namespace

TEST(SerialTransport, SocatLoopbackRoundtrip) {
  QProcess socat; std::string da, db;
  if (!StartSocat(socat, da, db)) { GTEST_SKIP() << "socat unavailable"; }

  SerialConfig ca; ca.device = da; ca.baud_rate = 115200;
  SerialConfig cb; cb.device = db; cb.baud_rate = 115200;
  auto a = std::make_shared<SerialTransport>(ca);
  auto b = std::make_shared<SerialTransport>(cb);
  std::vector<uint8_t> got;
  b->OnBytes([&](Result<std::vector<uint8_t>> r, const std::string&){ if(r) got = r.value; });
  if (!a->Open() || !b->Open()) { GTEST_SKIP() << "pty open/config failed in env"; }

  ASSERT_TRUE(static_cast<bool>(a->Send(B({0xC0, 0xDE}))));
  EXPECT_TRUE(pumpUntil([&]{ return got.size() == 2; }, 3000));
  EXPECT_EQ(got, B({0xC0, 0xDE}));
  a->Close(); b->Close();
  socat.kill(); socat.waitForFinished(1000);
}

TEST(SerialTransport, OpenNonexistentFails) {
  SerialConfig c; c.device = "/dev/nonexistent_tty_zzz";
  auto s = std::make_shared<SerialTransport>(c);
  auto st = s->Open();
  EXPECT_FALSE(static_cast<bool>(st));
  EXPECT_NE(st.error.find("config:"), std::string::npos);
}
