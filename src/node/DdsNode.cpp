#include "transport/node/DdsNode.hpp"

#include <string>
#include <utility>

#include "task/fibertask.h"  // Coro::makeTask —— 读-分发循环 fiber。

#include "transport/core/DropReason.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/TraceCategories.hpp"

// DdsNode.cpp — 见 .hpp。DDS 特有语义内联于此(D10 红线):correlation_id 生成、kReply
// 终结判别、topic 寻址、reply_to=inbox。生命周期(幂等 Start / 关闭仲裁 / 收敛)由基类
// NodeBase 承载,本类只填 DDS 特有钩子(ADR-0006 D1);读-分发循环骨架与 handler 观测计数
// 归本类(ADR-0006 D5,#140),handler 消费者在可选小件 HandlerLoop(D4);关联复用
// PendingTable<std::string,Message>。

namespace transport {

DdsNode::DdsNode(std::unique_ptr<ITransport> transport,
                 std::unique_ptr<ICodec> codec, DdsNodeConfig config)
    // 生命周期基类:共用同一可选 trace_sink(生命周期跃迁 + close_drop 归因)。
    : NodeBase(config.trace_sink),
      transport_(std::move(transport)),
      codec_(std::move(codec)),
      config_(std::move(config)),
      // 关联表:correlation_id 无天然容量语义(≠ session_id 的 uint8 硬顶)→ 纯计数无限
      // (max_pending=0)。D10:仅把 Key 实例化为 std::string,PendingTable 一行不改。
      // P5-4:与 handler_loop_ 共用同一可选 trace_sink(未配则两处 RecordEvent 均一次判空)。
      pending_(/*max_pending=*/0, config_.trace_sink),
      // handler 消费者小件持有业务队列:字节计量注入 payload.size()(D10:小件不读 Message
      // 其它字段)。trace_sink 透传(P5-3):业务队列满(business_queue_overflow)与 handler
      // 调用起止点经它可选上报。**未设 handler 时不 Spawn**,它只是个空队列。
      handler_loop_([](const Message& msg) { return msg.payload.size(); },
                    config_.business_queue_max_events,
                    config_.business_queue_max_bytes, config_.trace_sink) {}

// 析构即关闭:必须在**本类**析构体内做——基类析构时虚钩子已退回纯虚(见 NodeBase 文档)。
DdsNode::~DdsNode() { Close(); }

Coro::Result<void> DdsNode::ValidateConfig() const {
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
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::DoStart() {
  // 基类已做完幂等仲裁与配置校验,这里只做 DDS 特有实事。三介质(含 DDS)共用同一段无
  // 分支读循环(ADR-0004 D1/D2)——DdsTransport 断开即致命(Read 返 kClosed),由读循环收敛。
  Coro::Result<void> started = transport_->Start();  // DdsTransport:Init + 订阅 topic 集。
  if (!started) {
    return started;  // 传输启动失败:基类退回 Created 允许重试(此时未 MarkRunning)。
  }
  MarkRunning();
  // 读-分发循环(本类自持,ADR-0006 D5):Read 骨架 + 内联 decode + DDS 特有分类/寻址;
  // 循环退出后本 fiber 兼任收敛者,走基类内部路径 ConvergeAfterReadLoop(ADR-0005 D1)。
  SpawnReadLoop();
  // 设了 handler → spawn 单消费者 handler fiber(串行消费业务队列)。
  if (config_.handler) {
    handler_loop_.Spawn([this](Message&& msg) {
      DdsHandlerContext ctx(this, handler_loop_.Token());
      // 返回 Coro::Result<void> 仅记录:框架不据此自动应答(避 TBD-001)。逃逸异常由 HandlerLoop
      // 边界兜住并归因 handler_exception(RT_HANDLER_006)。
      (void)config_.handler(msg, ctx);
    });
  }
  return Coro::Result<void>{};
}

Coro::Result<void> DdsNode::DoClose() {
  // 关闭汇合信号,**顺序即契约**(见 NodeBase::DoClose 文档):transport.RequestClose 一
  // 执行读循环就可能被唤醒退出,余下信号必须在本段内发完,基类随后才放行收敛。
  transport_->RequestClose();
  handler_loop_.CancelAndClose();  // 业务队列 Close + handler 协作取消(同一顺序)。
  // DDS 特有收敛信号(ADR-0005 D5):PendingTable.FailAll(kClosed) 令在途 Request 恰好一次
  // 收敛。**外部 Close 与读循环致命错误自终共用本段**——故它是钩子而非 Close 的入参。
  // 无 reactor(无连接),故无额外取消源。
  pending_.FailAll(make_error_code(TransportErrc::kClosed));
  return Coro::Result<void>{};
}

void DdsNode::JoinHandler() { handler_loop_.Join(); }

std::size_t DdsNode::DrainUnstartedBusiness() {
  return handler_loop_.DrainForClose();
}

void DdsNode::SpawnReadLoop() {
  Coro::makeTask([this] {
    // 取一次 read_queue 句柄,循环 await(ADR-0007 D4):不设 deadline、不接令牌
    // ——循环级中断靠传输的 RequestClose 关队列。
    auto rx = transport_->Read();
    while (true) {
      Coro::Result<Datagram, std::error_code> datagram = Coro::await(rx);
      if (!datagram) {
        // 等待器给出终止错误 = read_queue 被 close 并携带终止原因 = 传输终结(唯一
        // 终止语义,ADR-0004 D1 经 ADR-0007 D4 改写表达)→ 退出读循环。可继续的瞬时
        // 错误由传输内部的泵就地消化,不出现在本句柄上。
        break;
      }
      DecodeAndDispatch(std::move(datagram).value());  // 内联 decode + 分发。
    }
    // 读循环兼任收敛者(ADR-0005 D1):走基类内部路径,不得调公开的 Close(会自等)。
    ConvergeAfterReadLoop();
  });
}

std::string DdsNode::NextCorrelationId() {
  std::lock_guard<std::mutex> lock(mutex_);
  // 确定性可测:node_id 前缀 + 单调序号(不用随机数,RT_REQUEST)。node_id 集群内唯一
  // 保证跨节点键不碰撞。
  return config_.node_id + ":" + std::to_string(++correlation_counter_);
}

Coro::Result<void> DdsNode::WriteFramed(Message msg, MessageKind kind, Endpoint dest) {
  msg.kind = kind;
  auto encoded = codec_->Encode(msg);
  if (!encoded) {
    return encoded.error();
  }
  SendUnit unit;
  unit.bytes = std::move(encoded).value();
  unit.destination = std::move(dest);
  const std::size_t sent_bytes = unit.bytes.size();
  auto written = transport_->Write(std::move(unit));
  if (written) {
    // Write 完成边界(P5-4):Request/Publish/Reply 共用本收口,不逐字节。
    RecordEvent(kTraceCategorySend, config_.trace_sink, {}, {}, {}, {},
                static_cast<long>(sent_bytes));
  }
  return written;
}

Coro::Result<Message> DdsNode::Request(Message req, Endpoint target,
                                 OperationOptions options) {
  if (!IsRunning()) {
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

Coro::Result<void> DdsNode::Publish(Message msg, Endpoint topic) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // 单向 kNotify fire-and-forget:不登记 PendingTable。
  return WriteFramed(std::move(msg), MessageKind::kNotify, std::move(topic));
}

void DdsNode::DecodeAndDispatch(Datagram datagram) {
  const auto& bytes = datagram.bytes;
  auto decoded = codec_->Decode(bytes.data(), bytes.size());
  if (!decoded) {
    // 坏 sample / codec 错误:丢弃。归因 kBadFrame(P5-3)。
    std::lock_guard<std::mutex> lock(mutex_);
    RecordDrop(DropReason::kBadFrame, bad_frame_count_, config_.trace_sink);
    return;
  }
  // Decode 成功边界(P5-4):一次 Decode 调用一条事件,不逐条消息重复。
  RecordEvent(kTraceCategoryDecode, config_.trace_sink, {}, {}, {}, {},
              static_cast<long>(bytes.size()));
  for (auto& msg : decoded.value()) {
    // 引擎按来源 topic 填 source/topic(Message.hpp 约定:DDS 的 source 即来源 topic 名)。
    msg.source = datagram.peer.topic;
    msg.topic = datagram.peer.topic;
    // Read 解出消息边界(P5-4):按解出的消息计,不逐字节。
    RecordEvent(kTraceCategoryRecv, config_.trace_sink, {}, {}, msg.source, {},
                static_cast<long>(msg.payload.size()));
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
      RecordDrop(DropReason::kUnmatchedOrLateResponse, unmatched_reply_count_,
                config_.trace_sink);
    }
    return;
  }
  // 非 kReply 业务消息(kRequest/kNotify/kOneway/kFeedback 延后):设了 handler → 入有界
  // 业务队列交单消费者串行处理;满则 tail-drop(business_queue_overflow),读循环不阻塞、
  // 应答匹配照常。未设 handler → 归因 dropped_no_handler。
  if (config_.handler) {
    (void)handler_loop_.Enqueue(std::move(msg));  // 满 / 已 Close 均丢弃,不阻塞。
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  RecordDrop(DropReason::kNoHandlerConfigured, dropped_no_handler_count_,
            config_.trace_sink);
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
  return handler_loop_.BusinessQueueOverflowCount();  // HandlerLoop/队列自守其锁。
}

std::size_t DdsNode::HandlerExceptionCount() const {
  return handler_loop_.HandlerExceptionCount();  // HandlerLoop 自守其锁。
}

std::size_t DdsNode::PendingCount() const { return pending_.Size(); }

std::size_t DdsNode::BadFrameCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return bad_frame_count_;
}

DdsNode::Clock::duration DdsNode::LastRequestLatency() const {
  return pending_.LastRequestLatency();
}

DdsNode::Clock::duration DdsNode::LastHandlerDuration() const {
  return handler_loop_.LastHandlerDuration();  // HandlerLoop 自守其锁。
}

// —— DdsHandlerContext ————————————————————————————————————————————————————————

Coro::Result<void> DdsHandlerContext::Reply(const Message& request, Message reply) {
  // DDS 应答寻址内联:非请求 / 无回送 topic → 无从回送。
  if (request.kind != MessageKind::kRequest || request.reply_to.empty()) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  reply.correlation_id = request.correlation_id;
  return node_->WriteFramed(std::move(reply), MessageKind::kReply,
                            Endpoint::Topic(request.reply_to));
}

Coro::Result<void> DdsHandlerContext::Publish(Message msg, Endpoint topic) {
  return node_->Publish(std::move(msg), std::move(topic));
}

Coro::Result<void> DdsHandlerContext::RequestClose() {
  // 只发汇合信号、不等待(ADR-0006 D8):当前即 handler 消费者 fiber,任何等待收敛的入口
  // 都等于等自己退出。返回仅表示已受理;节点由读循环在汇合完成后收敛到 Closed(ADR-0005 D1)。
  return node_->SignalClose();  // NodeBase 的受保护入口(本类是 DdsNode 的友元)。
}

}  // namespace transport
