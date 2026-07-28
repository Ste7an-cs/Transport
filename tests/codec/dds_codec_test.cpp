#include "transport/codec/DdsCodec.hpp"

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/Error.hpp"

using transport::DdsCodec;
using transport::Message;
using transport::MessageKind;
using transport::TransportErrc;
using transport::make_error_code;

namespace {
Message Make(MessageKind k, std::string corr, std::string reply, std::vector<uint8_t> p) {
  Message m; m.kind = k; m.correlation_id = std::move(corr);
  m.reply_to = std::move(reply); m.payload = std::move(p); return m;
}
}  // namespace

TEST(DdsCodec, EncodeDecodeRoundtripCarriesMetadata) {
  DdsCodec c;
  auto enc = c.Encode(Make(MessageKind::kRequest, "corr-7", "inbox-A", {1, 2, 3}));
  ASSERT_TRUE(static_cast<bool>(enc));
  auto dec = c.Decode(enc.value().data(), enc.value().size());
  ASSERT_TRUE(static_cast<bool>(dec));
  ASSERT_EQ(dec.value().size(), 1u);                 // 整段 = 恰好一条
  const Message& m = dec.value()[0];
  EXPECT_EQ(m.kind, MessageKind::kRequest);
  EXPECT_EQ(m.correlation_id, "corr-7");
  EXPECT_EQ(m.reply_to, "inbox-A");
  EXPECT_EQ(m.payload, (std::vector<uint8_t>{1, 2, 3}));
}

TEST(DdsCodec, EmptyAndNoMetadata) {
  DdsCodec c;
  // 空输入 → 无消息
  auto none = c.Decode(nullptr, 0);
  ASSERT_TRUE(static_cast<bool>(none));
  EXPECT_TRUE(none.value().empty());
  // kOneway、空 corr/reply、空 payload 往返
  auto enc = c.Encode(Make(MessageKind::kOneway, "", "", {}));
  ASSERT_TRUE(static_cast<bool>(enc));
  auto dec = c.Decode(enc.value().data(), enc.value().size());
  ASSERT_TRUE(static_cast<bool>(dec));
  ASSERT_EQ(dec.value().size(), 1u);
  EXPECT_EQ(dec.value()[0].kind, MessageKind::kOneway);
  EXPECT_TRUE(dec.value()[0].correlation_id.empty());
  EXPECT_TRUE(dec.value()[0].reply_to.empty());
  EXPECT_TRUE(dec.value()[0].payload.empty());
}

TEST(DdsCodec, TruncatedFailsWithCodecError) {
  DdsCodec c;
  // 只有 kind 字节,缺 corr 长度 → codec 越界
  std::vector<uint8_t> bad = {static_cast<uint8_t>(MessageKind::kReply)};
  auto dec = c.Decode(bad.data(), bad.size());
  ASSERT_FALSE(static_cast<bool>(dec));
  EXPECT_EQ(dec.error(), make_error_code(TransportErrc::kCodec));
  // corr_len 声称 100 但无数据 → codec 越界
  std::vector<uint8_t> bad2 = {0, 0, 100};
  auto dec2 = c.Decode(bad2.data(), bad2.size());
  ASSERT_FALSE(static_cast<bool>(dec2));
  EXPECT_EQ(dec2.error(), make_error_code(TransportErrc::kCodec));
}

TEST(DdsCodec, BadKindRejected) {
  DdsCodec c;
  std::vector<uint8_t> bad = {99, 0, 0, 0, 0};  // kind=99 越界
  auto dec = c.Decode(bad.data(), bad.size());
  ASSERT_FALSE(static_cast<bool>(dec));
  EXPECT_EQ(dec.error(), make_error_code(TransportErrc::kCodec));
}

TEST(DdsCodec, EncodeRejectsOverlongField) {
  DdsCodec c;
  // correlation_id 超 uint16 长度前缀上限 → Encode 拒绝(避免长度前缀截断 → 静默坏帧)。
  std::string overlong(70000, 'x');
  auto enc = c.Encode(Make(MessageKind::kRequest, overlong, "", {1}));
  ASSERT_FALSE(static_cast<bool>(enc));
  EXPECT_EQ(enc.error(), make_error_code(TransportErrc::kCodec));
}

TEST(DdsCodec, StatelessConcurrentDecodeSafe) {
  DdsCodec c;
  // 预先编码 N 条不同消息;多线程在同一 codec 实例上并发 Decode,各自结果正确。
  constexpr int N = 64;
  std::vector<std::vector<uint8_t>> frames;
  for (int i = 0; i < N; ++i) {
    auto enc = c.Encode(Make(MessageKind::kNotify, "c" + std::to_string(i), "",
                             {static_cast<uint8_t>(i)}));
    ASSERT_TRUE(static_cast<bool>(enc));
    frames.push_back(std::move(enc.value()));
  }
  std::atomic<int> ok{0};
  std::vector<std::thread> ts;
  for (int i = 0; i < N; ++i) {
    ts.emplace_back([&, i] {
      auto dec = c.Decode(frames[i].data(), frames[i].size());
      if (dec && dec.value().size() == 1u &&
          dec.value()[0].correlation_id == "c" + std::to_string(i) &&
          dec.value()[0].payload == std::vector<uint8_t>{static_cast<uint8_t>(i)})
        ok.fetch_add(1);
    });
  }
  for (auto& t : ts) t.join();
  EXPECT_EQ(ok.load(), N);  // 无共享状态 → 全部正确
}
