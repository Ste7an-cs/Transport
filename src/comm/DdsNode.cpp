#include "transport/comm/DdsNode.hpp"

#include <utility>

#include "transport/codec/DdsCodec.hpp"

// DdsNode.cpp — 见 .hpp。基类拿 transport 的副本(上转 ITransport);dds_ 保留 IDdsTransport
// 类型用于订阅。Open 后订阅 inbox 并把应答地址设为 inbox topic,使继承的 Request 出站自动带 reply_to。

namespace transport {

DdsNode::DdsNode(std::shared_ptr<IDdsTransport> transport, std::string inbox_topic,
                 std::unique_ptr<ICodec> codec, std::unique_ptr<IExecutor> executor,
                 std::size_t queue_capacity)
    : CommNode(transport,
               codec ? std::move(codec) : std::unique_ptr<ICodec>(new DdsCodec()),
               std::move(executor), queue_capacity),
      dds_(std::move(transport)),
      inbox_topic_(std::move(inbox_topic)) {}

Status DdsNode::Open() {
  auto st = CommNode::Open();
  if (!st) return st;
  reply_address_ = inbox_topic_;     // 继承的 Request 出站填 msg.reply_to
  return Subscribe(inbox_topic_);    // 自动订阅自己的 inbox(收应答/回送)
}

Status DdsNode::Subscribe(const std::string& topic)   { return dds_->Subscribe(topic); }
Status DdsNode::Unsubscribe(const std::string& topic) { return dds_->Unsubscribe(topic); }

}  // namespace transport
