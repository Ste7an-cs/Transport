#include "transport/core/Message.hpp"

#include <string>
#include <vector>

#include <QByteArray>
#include <gtest/gtest.h>

#include "message_test_util.hpp"

using testutil::Pay;
using testutil::ToVec;
using transport::Endpoint;
using transport::Message;
using transport::MessageKind;

TEST(Message, DefaultsToOnewayEmptyCorrelation) {
  Message m;
  EXPECT_EQ(m.kind, MessageKind::kOneway);
  EXPECT_TRUE(m.correlation_id.empty());
  EXPECT_TRUE(m.payload.isEmpty());
  // 出站消息的 `frame` 恒为空——`Encode` 完全忽略它(ADR-0020 D7)。
  EXPECT_TRUE(m.frame.isEmpty());
  // `endpoint` 缺省是 `kDefault`:交给传输解析成它自己配置的默认对端。
  EXPECT_EQ(m.endpoint.kind, Endpoint::Kind::kDefault);
  EXPECT_TRUE(m.endpoint.topic.empty());
}

TEST(Message, HoldsKindCorrelationAndEndpoint) {
  Message m;
  m.kind = MessageKind::kRequest;
  m.correlation_id = "req-1";
  m.payload = Pay({1, 2, 3});
  m.endpoint = Endpoint::Topic("calc");
  EXPECT_EQ(m.kind, MessageKind::kRequest);
  EXPECT_EQ(m.correlation_id, "req-1");
  EXPECT_EQ(ToVec(m.payload), (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(m.endpoint.kind, Endpoint::Kind::kTopic);
  EXPECT_EQ(m.endpoint.topic, "calc");
}

// `Endpoint::Service` 与 `Endpoint::Topic` **刻意分开**(ADR-0020 D5):服务名不是 topic,
// 名字复用同一个 `topic` 字段、由 `kind` 区分,**不加第五个字段**。
TEST(Message, ServiceEndpointReusesTheTopicFieldButADifferentKind) {
  const Endpoint svc = Endpoint::Service("get");
  EXPECT_EQ(svc.kind, Endpoint::Kind::kService);
  EXPECT_EQ(svc.topic, "get");
  EXPECT_TRUE(svc.host.empty());
  EXPECT_EQ(svc.port, 0);

  const Endpoint topic = Endpoint::Topic("get");
  EXPECT_EQ(topic.topic, svc.topic);        // 同名……
  EXPECT_NE(topic.kind, svc.kind);          // ……但类型系统守住了区分。
}

// ⭐ ADR-0020 **D8**:`OwnedPayload()` **恒为深拷贝**——其数据块**不在** `frame` 区间内。
//
// 不做"已是拥有型就直接返回"的优化:Qt5 没有公开 API 可判别 `fromRawData`,拿
// `frame.isEmpty()` 当判据则依赖一条可被违反的不变量。
TEST(Message, OwnedPayloadIsAlwaysADeepCopyOutsideTheFrame) {
  Message m;
  m.frame = Pay({0x10, 0x11, 0x12, 0x13, 0x14});
  // 手工建一条与 codec 同形的视图(payload 偏移 2、长度 3)。
  m.payload = QByteArray::fromRawData(m.frame.constData() + 2, 3);
  ASSERT_TRUE(testutil::PayloadIsViewOfFrame(m));

  const QByteArray owned = m.OwnedPayload();
  EXPECT_EQ(ToVec(owned), (std::vector<uint8_t>{0x12, 0x13, 0x14}));  // 内容相同……
  const char* begin = m.frame.constData();
  const char* end = begin + m.frame.size();
  EXPECT_TRUE(owned.constData() < begin || owned.constData() >= end)
      << "OwnedPayload() 必须落在 frame 之外——它是深拷贝,不是视图";

  // 拷贝出来之后原件即可作废,`owned` 照样可读(这正是它存在的理由)。
  m = Message{};
  EXPECT_EQ(ToVec(owned), (std::vector<uint8_t>{0x12, 0x13, 0x14}));
}

// 拥有型 payload(出站消息,`frame` 为空)上调 `OwnedPayload()` 同样是深拷贝——**不是**
// "已是拥有型就原样返回"。本条正是 D8 里那句"不做该优化"的可执行形式。
TEST(Message, OwnedPayloadCopiesEvenWhenPayloadAlreadyOwnsItsBytes) {
  Message m;
  m.payload = Pay({0xAA, 0xBB});
  ASSERT_TRUE(m.frame.isEmpty());

  const QByteArray owned = m.OwnedPayload();
  EXPECT_EQ(ToVec(owned), (std::vector<uint8_t>{0xAA, 0xBB}));
  EXPECT_NE(owned.constData(), m.payload.constData()) << "恒为深拷贝(D8)";
}
