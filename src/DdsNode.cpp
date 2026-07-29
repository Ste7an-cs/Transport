#include "transport/DdsNode.hpp"

#include <string>
#include <utility>

// DdsNode.cpp — 见 .hpp。DDS 特有语义内联于此(D10 红线):correlation_id 生成、kReply
// 终结判别、topic 寻址、reply_to=inbox。协议无关机制(生命周期三方汇合、handler 消费者、
// 业务队列、读循环骨架)组合并驱动 NodeRuntime;关联复用 PendingTable<std::string,Message>。

namespace transport {

DdsNode::DdsNode(std::unique_ptr<ITransport> transport,
                 std::unique_ptr<ICodec> codec, DdsNodeConfig config)
    : transport_(std::move(transport)),
      codec_(std::move(codec)),
      config_(std::move(config)),
      // 关联表:correlation_id 无天然容量语义(≠ session_id 的 uint8 硬顶)→ 纯计数无限
      // (max_pending=0)。D10:仅把 Key 实例化为 std::string,PendingTable 一行不改。
      pending_(),
      // 运行时组合业务队列:字节计量注入 payload.size()(D10:runtime 不读 Message 其它字段);
      // transport 裸指针供读循环 Read + 收敛 RequestClose(node 持 unique_ptr)。
      runtime_(transport_.get(),
               [](const Message& msg) { return msg.payload.size(); },
               config_.business_queue_max_events,
               config_.business_queue_max_bytes) {}

DdsNode::~DdsNode() { Close(); }

Status DdsNode::ValidateConfig() const {
  // inbox_topic 是 reply_to 的来源、node_id 是 correlation_id 的前缀——二者为空则请求-应答
  // 无从成立(收不到应答 / 键无归属),停 Created 允许改配重试。
  if (config_.inbox_topic.empty() || config_.node_id.empty()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  using Q = BoundedQueue<Message>;
  if (config_.business_queue_max_events < Q::kMinEvents ||
      config_.business_queue_max_events > Q::kMaxEvents) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  if (config_.business_queue_max_bytes < Q::kMinBytes ||
      config_.business_queue_max_bytes > Q::kMaxBytes) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  return Status{};
}

Status DdsNode::Start() {
  // 组合并驱动 NodeRuntime 幂等 Start:runtime 管状态机 / 共享结果 / 三方汇合骨架;node
  // 提供 DDS 特有的配置校验与首次 bring-up。无连接(D3′):不检测 IConnectionObservable、
  // 不 spawn reactor——DdsTransport 断开即致命,由读循环收敛。
  return runtime_.Start(
      [this] { return ValidateConfig(); },
      [this]() -> Status {
        Status started = transport_->Start();  // DdsTransport:Init + 订阅 topic 集。
        if (!started) {
          return started;  // 传输启动失败:runtime 退回 Created 允许重试。
        }
        runtime_.MarkRunning();
        // 读-分发循环:runtime 跑 Read 骨架,node 内联 decode + DDS 特有分类/寻址。
        runtime_.SpawnReadLoop(
            [this](Datagram datagram) { DecodeAndDispatch(std::move(datagram)); });
        // 设了 handler → runtime spawn 单消费者 handler fiber(串行消费业务队列)。
        if (config_.handler) {
          runtime_.SpawnHandlerLoop([this](Message&& msg) {
            DdsHandlerContext ctx(this, runtime_.HandlerCancellationToken());
            // 返回 Status 仅记录:框架不据此自动应答(避 TBD-001)。逃逸异常由 runtime
            // 边界兜住并归因 handler_exception(RT_HANDLER_006)。
            (void)config_.handler(msg, ctx);
          });
        }
        return Status{};
      });
}

Status DdsNode::Close() {
  // 驱动 runtime 收敛;node 侧 DDS 特有收敛信号:PendingTable.FailAll(kClosed) 令在途
  // Request 恰好一次收敛。无 reactor(无连接),故无额外取消源。
  return runtime_.Close(
      [this] { pending_.FailAll(make_error_code(TransportErrc::kClosed)); });
}

Status DdsNode::WaitClosed(OperationOptions options) {
  return runtime_.WaitClosed(std::move(options));
}

std::string DdsNode::NextCorrelationId() {
  std::lock_guard<std::mutex> lock(mutex_);
  // 确定性可测:node_id 前缀 + 单调序号(不用随机数,RT_REQUEST)。node_id 集群内唯一
  // 保证跨节点键不碰撞。
  return config_.node_id + ":" + std::to_string(++correlation_counter_);
}

Status DdsNode::WriteFramed(Message msg, MessageKind kind, Endpoint dest) {
  msg.kind = kind;
  auto encoded = codec_->Encode(msg);
  if (!encoded) {
    return encoded.error();
  }
  SendUnit unit;
  unit.bytes = std::move(encoded).value();
  unit.destination = std::move(dest);
  return transport_->Write(std::move(unit));
}

Result<Message> DdsNode::Request(Message req, Endpoint target,
                                 OperationOptions options) {
  if (!runtime_.IsRunning()) {
    // 未启动 / 关闭中 / 已关闭:一律 kClosed(PendingTable closed latch 亦兜底)。
    return make_error_code(TransportErrc::kClosed);
  }

  // DDS 关联语义内联:生成 correlation_id,盖 reply_to=inbox(对端据此回送)。
  const std::string correlation_id = NextCorrelationId();
  req.correlation_id = correlation_id;
  req.reply_to = config_.inbox_topic;

  auto registration = pending_.Register(correlation_id);
  if (!registration) {
    return registration.error();  // 键重复 kInvalidState(几无) / closed kClosed 透传。
  }
  auto handle = std::move(registration).value();

  // 盖 kind=kRequest、Encode、发往目标 topic。任一失败 handle 析构兜底摘除未终结 entry。
  if (auto written = WriteFramed(std::move(req), MessageKind::kRequest,
                                 std::move(target));
      !written) {
    return written.error();
  }

  // 等 inbox 上匹配 correlation_id 的唯一 kReply;终结(值/超时/取消/FailAll)后 handle
  // 析构摘除 entry(关联清理)。
  return handle.Wait(std::move(options));
}

Status DdsNode::Publish(Message msg, Endpoint topic) {
  if (!runtime_.IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // 单向 kNotify fire-and-forget:不登记 PendingTable。
  return WriteFramed(std::move(msg), MessageKind::kNotify, std::move(topic));
}

void DdsNode::DecodeAndDispatch(Datagram datagram) {
  const auto& bytes = datagram.bytes;
  auto decoded = codec_->Decode(bytes.data(), bytes.size());
  if (!decoded) {
    return;  // 坏 sample / codec 错误:丢弃。
  }
  for (auto& msg : decoded.value()) {
    // 引擎按来源 topic 填 source/topic(Message.hpp 约定:DDS 的 source 即来源 topic 名)。
    msg.source = datagram.source.topic;
    msg.topic = datagram.source.topic;
    Dispatch(std::move(msg));
  }
}

void DdsNode::Dispatch(Message msg) {
  // IsTerminal 内联锁死:kReply = 终结应答(请求-应答的应答)。
  if (msg.kind == MessageKind::kReply) {
    const std::string key = msg.correlation_id;
    if (!pending_.Resolve(key, std::move(msg))) {
      // RouteUnmatched 内联锁死:无匹配在途 Request(迟到 / 无匹配 correlation_id)→
      // 归因丢弃,不误配。
      std::lock_guard<std::mutex> lock(mutex_);
      ++unmatched_reply_count_;
    }
    return;
  }
  // 非 kReply 业务消息(kRequest/kNotify/kOneway/kFeedback 延后):设了 handler → 入运行时
  // 有界队列交单消费者串行处理;满则 tail-drop(business_queue_overflow),读循环不阻塞、
  // 应答匹配照常。未设 handler → 归因 dropped_no_handler。
  if (config_.handler) {
    (void)runtime_.Enqueue(std::move(msg));  // 满 / 已 Close 均丢弃,不阻塞。
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  ++dropped_no_handler_count_;
}

std::size_t DdsNode::UnmatchedReplyCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return unmatched_reply_count_;
}

std::size_t DdsNode::DroppedNoHandlerCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return dropped_no_handler_count_;
}

std::size_t DdsNode::BusinessQueueOverflowCount() const {
  return runtime_.BusinessQueueOverflowCount();
}

std::size_t DdsNode::HandlerExceptionCount() const {
  return runtime_.HandlerExceptionCount();
}

std::size_t DdsNode::PendingCount() const { return pending_.Size(); }

std::size_t DdsNode::CloseDropCount() const { return runtime_.CloseDropCount(); }

// —— DdsHandlerContext ————————————————————————————————————————————————————————

Status DdsHandlerContext::Reply(const Message& request, Message reply) {
  // DDS 应答寻址内联:非请求 / 无回送 topic → 无从回送。
  if (request.kind != MessageKind::kRequest || request.reply_to.empty()) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  reply.correlation_id = request.correlation_id;
  return node_->WriteFramed(std::move(reply), MessageKind::kReply,
                            Endpoint::Topic(request.reply_to));
}

Status DdsHandlerContext::Publish(Message msg, Endpoint topic) {
  return node_->Publish(std::move(msg), std::move(topic));
}

Status DdsHandlerContext::RequestClose() {
  // 发起完整关闭拆卸;因当前即 handler 消费者 fiber,runtime.Close 内重入自锁防护只发起、
  // 不自等,立即返回(RT_LIFECYCLE_005)。节点由 finalizer fiber 在三方汇合后收敛到 Closed。
  return node_->Close();
}

}  // namespace transport
