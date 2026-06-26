#include "transport/ITraceSink.hpp"

#include <sstream>
#include <gtest/gtest.h>

using transport::CapturingTraceSink;
using transport::OstreamTraceSink;
using transport::TraceEvent;
using transport::TraceLevel;
using transport::kNoNum;
using transport::kNoTag;

TEST(OstreamTraceSink, FormatsOnlyNonEmptyFields) {
  std::ostringstream os;
  OstreamTraceSink sink(os, TraceLevel::kTrace);
  TraceEvent ev{TraceLevel::kDebug, "dispatch", "match-terminal", "01.0005", "", "", 3, kNoNum, -1};
  sink.OnTrace(ev);
  const std::string line = os.str();
  EXPECT_NE(line.find("[D]"), std::string::npos);
  EXPECT_NE(line.find("dispatch"), std::string::npos);
  EXPECT_NE(line.find("match-terminal"), std::string::npos);
  EXPECT_NE(line.find("key=01.0005"), std::string::npos);
  EXPECT_NE(line.find("tag=3"), std::string::npos);
  EXPECT_EQ(line.find("size="), std::string::npos);     // kNoNum 不打印
  EXPECT_EQ(line.find("attempt="), std::string::npos);  // -1 不打印
  EXPECT_EQ(line.find("ep="), std::string::npos);       // 空不打印
}

TEST(OstreamTraceSink, FiltersBelowMinLevel) {
  std::ostringstream os;
  OstreamTraceSink sink(os, TraceLevel::kWarn);
  sink.OnTrace(TraceEvent{TraceLevel::kDebug, "send", "", "", "", "", 1, 7, -1});
  EXPECT_TRUE(os.str().empty());                         // Debug < Warn,被过滤
  sink.OnTrace(TraceEvent{TraceLevel::kError, "error", "", "", "", "io: boom", kNoTag, kNoNum, -1});
  EXPECT_NE(os.str().find("[E]"), std::string::npos);
  EXPECT_NE(os.str().find("err=io: boom"), std::string::npos);
}

TEST(CapturingTraceSink, DeepCopiesEvents) {
  CapturingTraceSink cap;
  {
    std::string cat = "retransmit";
    std::string key = "k1";
    cap.OnTrace(TraceEvent{TraceLevel::kDebug, cat, "", key, "", "", kNoTag, kNoNum, 2});
  }  // cat/key 已析构 — 深拷贝必须仍有效
  ASSERT_EQ(cap.Records().size(), 1u);
  EXPECT_EQ(cap.Records()[0].category, "retransmit");
  EXPECT_EQ(cap.Records()[0].key, "k1");
  EXPECT_EQ(cap.Records()[0].attempt, 2);
  EXPECT_EQ(cap.Records()[0].level, TraceLevel::kDebug);
  cap.Clear();
  EXPECT_TRUE(cap.Records().empty());
}
