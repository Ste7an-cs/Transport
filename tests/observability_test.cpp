// -----------------------------------------------------------------------------
// observability_test.cpp — DropReason + RecordDrop/RecordEvent 共享观测原语单测
// (ADR-0003 D13 Q2/Q3、RT_TRACE_001/002、RT_DATA_BUFFER)
//
// 覆盖:
//  - DropReason 四项枚举值完整可用,DropReasonName 稳定可辨识、互不相同。
//  - RecordDrop:计数器恰好 +1;配 sink 收到一条可辨识出 reason 的 TraceEvent;
//    sink == nullptr 时无副作用(仅计数)。
//  - RecordEvent:配 sink 收到对应事件;sink == nullptr 时是空操作。
//  - 协议无关自审:读取 DropReason.hpp / Observability.hpp 源码文本,核验零 include
//    Message.hpp/Endpoint.hpp、零协议字段引用(session_id/frm_type/correlation_id)。
//
// 协议无关证据:本文件驱动的两个头不 include transport/Message.hpp,
// key/endpoint 全程为裸 std::string_view 字面量。
// -----------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <cstddef>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <string_view>

#include "transport/core/DropReason.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/core/Observability.hpp"

using transport::CapturingTraceSink;
using transport::DropReason;
using transport::DropReasonName;
using transport::ITraceSink;
using transport::RecordDrop;
using transport::RecordEvent;
using transport::TraceLevel;

namespace {

// 四项 DropReason,与 ADR-0003 D13 Q2 taxonomy 一一对应(「连接代际隔离丢弃」随
// ADR-0004 D3 撤销代际隔离而移除;`kNoHandlerConfigured` 随 ADR-0009 废止内建
// handler 通道而移除——入站业务改由订阅承载,"未设 handler"的产生时刻已不存在;
// `kDdsHandoffOverflow` 随 ADR-0013 D11 移除——DDS 的接收队列与三介质的同性质,
// 三介质都不为它单设归因项)。
constexpr DropReason kAllReasons[] = {
    DropReason::kBusinessQueueOverflow,
    DropReason::kBadFrame,
    DropReason::kUnmatchedOrLateResponse,
    DropReason::kCloseDrop,
};

std::string ReadFile(const std::string& path) {
  std::ifstream in(path);
  EXPECT_TRUE(in.good()) << "无法打开: " << path;
  std::ostringstream buf;
  buf << in.rdbuf();
  return buf.str();
}

}  // namespace

TEST(DropReasonTest, FourValuesAreDistinctAndUsable) {
  std::set<std::string_view> names;
  for (DropReason reason : kAllReasons) {
    // 每项都可构造、可比较、可转成稳定短名。
    const std::string_view name = DropReasonName(reason);
    EXPECT_FALSE(name.empty());
    EXPECT_NE(name, "unknown");
    names.insert(name);
  }
  EXPECT_EQ(names.size(), 4u) << "四项 DropReasonName 应互不相同";
}

TEST(RecordDropTest, IncrementsCounterExactlyOnceWithoutSink) {
  std::size_t counter = 0;
  RecordDrop(DropReason::kBusinessQueueOverflow, counter, /*sink=*/nullptr);
  EXPECT_EQ(counter, 1u);
  RecordDrop(DropReason::kBusinessQueueOverflow, counter, /*sink=*/nullptr);
  EXPECT_EQ(counter, 2u);
}

TEST(RecordDropTest, NullSinkHasNoOtherObservableSideEffect) {
  // sink == nullptr 时唯一副作用是计数 +1;不崩溃、不抛异常、无其它可观察状态变化
  // (RT_TRACE_002)。
  std::size_t counter = 0;
  EXPECT_NO_THROW(
      RecordDrop(DropReason::kCloseDrop, counter, nullptr, "key-1", "ep-1"));
  EXPECT_EQ(counter, 1u);
}

TEST(RecordDropTest, EmitsCapturableTraceEventIdentifyingReason) {
  CapturingTraceSink sink;
  std::size_t counter = 0;

  RecordDrop(DropReason::kBadFrame, counter, &sink, "sess-42", "tcp://peer");

  EXPECT_EQ(counter, 1u);
  const auto records = sink.Records();
  ASSERT_EQ(records.size(), 1u);
  const auto& rec = records.front();
  EXPECT_EQ(rec.category, "drop");
  EXPECT_EQ(rec.message, DropReasonName(DropReason::kBadFrame));
  EXPECT_EQ(rec.key, "sess-42");
  EXPECT_EQ(rec.endpoint, "tcp://peer");
  EXPECT_EQ(rec.level, TraceLevel::kWarn) << "丢弃默认按 kWarn 级别上报";
}

TEST(RecordDropTest, EachReasonProducesDistinctIdentifiableMessage) {
  for (DropReason reason : kAllReasons) {
    CapturingTraceSink sink;
    std::size_t counter = 0;
    RecordDrop(reason, counter, &sink);
    const auto records = sink.Records();
    ASSERT_EQ(records.size(), 1u);
    EXPECT_EQ(records.front().category, "drop");
    EXPECT_EQ(records.front().message, DropReasonName(reason));
  }
}

TEST(RecordEventTest, NullSinkIsNoOp) {
  // sink == nullptr 应是纯粹的空操作:不崩溃、无可观察副作用。
  EXPECT_NO_THROW(RecordEvent("connect", nullptr, "connected"));
}

TEST(RecordEventTest, EmitsCapturableTraceEventForCategory) {
  CapturingTraceSink sink;

  RecordEvent("reconnect", &sink, "attempt-succeeded", "node-1", "tcp://host:9000",
              /*error=*/"", /*size=*/-1, /*attempt=*/3);

  const auto records = sink.Records();
  ASSERT_EQ(records.size(), 1u);
  const auto& rec = records.front();
  EXPECT_EQ(rec.category, "reconnect");
  EXPECT_EQ(rec.message, "attempt-succeeded");
  EXPECT_EQ(rec.key, "node-1");
  EXPECT_EQ(rec.endpoint, "tcp://host:9000");
  EXPECT_EQ(rec.attempt, 3);
  EXPECT_EQ(rec.level, TraceLevel::kInfo) << "非丢弃事件默认按 kInfo 级别上报";
}

TEST(RecordEventTest, DoesNotTouchAnyCounter) {
  // RecordEvent 不关联计数器:这里只验证签名里没有 counter 形参可传——
  // 编译期已保证;运行期额外验证多次调用不影响外部状态(用一个哨兵变量佐证
  // "调用方若想计数得自己做",RecordEvent 本身不做)。
  std::size_t sentinel = 7;
  CapturingTraceSink sink;
  RecordEvent("send", &sink, "ok");
  RecordEvent("recv", &sink, "ok");
  EXPECT_EQ(sentinel, 7u);
  EXPECT_EQ(sink.Records().size(), 2u);
}

// ---------------------------------------------------------------------------
// 协议无关自审(D13):两头文件不得 include Message.hpp/Endpoint.hpp,
// 不得引用任何具体协议字段。
// ---------------------------------------------------------------------------
TEST(ProtocolAgnosticSelfAudit, HeadersDoNotIncludeOrReferenceProtocolTypes) {
#ifndef TRANSPORT_INCLUDE_DIR
  GTEST_SKIP() << "TRANSPORT_INCLUDE_DIR 未由构建系统定义,跳过源码文本自审";
#else
  const std::string base = TRANSPORT_INCLUDE_DIR;
  const std::string drop_reason_src = ReadFile(base + "/transport/core/DropReason.hpp");
  const std::string observability_src = ReadFile(base + "/transport/core/Observability.hpp");

  const std::string forbidden_includes[] = {"Message.hpp", "Endpoint.hpp"};
  const std::string forbidden_protocol_fields[] = {
      "session_id", "frm_type", "correlation_id", "SessionId", "FrmType", "CorrelationId"};

  for (const std::string& needle : forbidden_includes) {
    EXPECT_EQ(drop_reason_src.find(needle), std::string::npos)
        << "DropReason.hpp 不应出现 " << needle;
    EXPECT_EQ(observability_src.find(needle), std::string::npos)
        << "Observability.hpp 不应出现 " << needle;
  }
  for (const std::string& needle : forbidden_protocol_fields) {
    EXPECT_EQ(drop_reason_src.find(needle), std::string::npos)
        << "DropReason.hpp 不应引用协议字段 " << needle;
    EXPECT_EQ(observability_src.find(needle), std::string::npos)
        << "Observability.hpp 不应引用协议字段 " << needle;
  }
#endif
}
