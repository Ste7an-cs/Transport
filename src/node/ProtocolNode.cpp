#include "transport/node/ProtocolNode.hpp"

#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"

#include "task/fibertask.h"  // Coro::makeTask —— 读-分发循环 fiber。

#include "transport/core/DropReason.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Observability.hpp"
#include "transport/core/TraceCategories.hpp"

// ProtocolNode.cpp — 见 .hpp。协议特有语义内联于此(D9/D10 红线):key 派生、frm_type
// 盖章、session_id 分配、Dispatch 分类、终结判别、寻址。生命周期(幂等 Start / 关闭仲裁 /
// join)由基类 NodeBase 承载,本类只填三个钩子。
//
// 入站只有一条通路——`Dispatcher` 按键投递(ADR-0009 D1)。本类不再持有业务队列与
// handler 消费者 fiber:入站业务由宿主 `Subscribe` 后在自己的 fiber 上消费。
//
// 本类**不触碰 transport 的生命周期**:不 Start、不 Close、不 WaitClosed,只借它的两条
// 队列句柄。链路的绑定/超时/重连/退避全在传输内部,对本类不可见。

namespace transport {
namespace {

// 丢弃归因:只上报,不计数(本类无观测面)。级别 kWarn——丢弃是需要关注的信号。
void TraceDrop(ITraceSink* sink, DropReason reason) {
  RecordEvent(kTraceCategoryDrop, sink, DropReasonName(reason), {}, {}, {},
              kNoNum, -1, kNoTag, TraceLevel::kWarn);
}

}  // namespace

MessageDispatcher::Key ResponseTo(const Message& request) {
  return {request.session_id, request.message_id, FrameType::kResponse};
}

MessageDispatcher::Key FrameOf(std::uint8_t session_id,
                               std::uint16_t message_id, FrameType type) {
  return {session_id, message_id, type};
}

MessageDispatcher::Key AnyOfType(FrameType type) { return {kAny, kAny, type}; }

ProtocolNode::ProtocolNode(ITransport& transport, std::unique_ptr<ICodec> codec,
                           ProtocolNodeConfig config)
    : transport_(transport),
      codec_(std::move(codec)),
      config_(std::move(config)),
      // 键提取函数:给出一条消息各匹配字段的具体值。部分匹配由 Dispatcher 实现,本类
      // 不再需要把字段压成单一关联键,也不再需要为"响应命令码"做归一化。
      dispatcher_([](const Message& msg) {
        return std::make_tuple(msg.session_id, msg.message_id, msg.frm_type);
      }) {}

// 析构即关闭并汇合:必须在**本类**析构体内做——基类析构时虚钩子已退回纯虚。
// Close 只发信号,WaitClosed 才 join,故两句缺一不可(旧形态的 Close 兼做了后者)。
ProtocolNode::~ProtocolNode() {
  (void)Close();
  WaitClosed();
}

Coro::Result<void> ProtocolNode::ValidateConfig() const {
  // 默认请求超时须为正(SRS §3.1.4.4):写出为 fire-and-forget、断链不终结在途请求,
  // 时限是唯一的兜底终结源;零或负值等价于"永不超时",将使请求挂至节点关闭。
  if (config_.default_request_timeout <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kConfiguration);
  }
  return Coro::Result<void>{};
}

Coro::Result<void> ProtocolNode::DoStart() {
  if (auto valid = ValidateConfig(); !valid) {
    return valid;  // 停 Created,未取句柄、未 spawn,允许改配重试。
  }
  // **不启动 transport**:宿主已经启过。读侧取自己的一路订阅——关它只终止本节点,
  // 源队列与其它订阅者不受影响;写侧无句柄可取,直接调 transport_.Write()。
  rx_ = transport_.AsyncRead()->shared();

  // 本节点只 spawn 这一条 fiber。入站业务的消费 fiber 属宿主,由其自行 spawn 与 join。
  SpawnReadLoop();
  return Coro::Result<void>{};
}

Coro::Result<void> ProtocolNode::DoClose() {
  // 关的是**本节点**,不是传输。close 自己那一路读订阅 → 读循环的 await 立即得到终止
  // 错误而退出;源 read_queue 与其它订阅者不受影响(AsyncTask shared() 语义)。
  // 残留数据一并丢弃:关闭即停止交付。
  if (rx_) {
    rx_->close(make_error_code(TransportErrc::kClosed));
    rx_->channel()->discard_pending();
  }
  // 协议特有的收敛信号:关闭全部订阅信箱并置终止标记。一举两得——令在途请求恰好终结
  // 一次,同时**即入站业务订阅者的协作取消信号**(ADR-0009 D4):其在途 `await` 恰好终结
  // 一次,消费 fiber 据此自然退出。断链不是收敛信号——在途请求仅由时限或节点关闭终结。
  dispatcher_.CloseAll(make_error_code(TransportErrc::kClosed));
  return Coro::Result<void>{};
}

void ProtocolNode::DoJoin() {
  // 让出式 join(FiberTask::get()):返回即意味着读循环已不再运行、不再触碰本对象。
  // 由 WaitClosed() 在**调用方** fiber 内调用,故不构成自等待。
  //
  // 这里 join 的是本节点**全部**的内部工作单元——只此一条。订阅者的消费 fiber 属宿主,
  // 本节点无从 join;`WaitClosed()` 返回后它们可能仍在退出途中(ADR-0009 D4)。
  if (read_task_) {
    (void)read_task_->get();
  }
}

void ProtocolNode::SpawnReadLoop() {
  read_task_ = std::make_shared<Coro::FiberTask<void>>(Coro::makeTask([this] {
    // 循环 await 本节点的读订阅:不设 deadline、不接令牌——循环级中断靠 DoClose 关订阅。
    while (true) {
      Coro::Result<Datagram, std::error_code> datagram = Coro::await(rx_);
      if (!datagram) {
        // 两种成因:我方 Close 关了订阅,或传输终结关了源队列。二者都该让节点关闭。
        // 可继续的瞬时错误由传输内部的泵就地消化,不出现在本句柄上。
        break;
      }
      DecodeAndDispatch(std::move(datagram).value());
    }
    // 无条件调**公开的** Close():我方 Close 所致时是幂等空操作,传输终结所致时即自终。
    // Close 不含等待点,故在本 fiber 内调用安全(旧形态为此另设了 SignalClose)。
    (void)Close();
  }));
}

Coro::Result<void> ProtocolNode::EncodeAndWrite(const Message& msg) {
  auto encoded = codec_->Encode(msg);
  if (!encoded) {
    return encoded.error();
  }
  std::vector<std::uint8_t> bytes = std::move(encoded).value();
  const std::size_t sent_bytes = bytes.size();
  // 目的地恒填 `Endpoint::Default()`,交给传输解析成它自己配置的默认对端:本类传输无关,
  // 不知道也不该知道对端是 ip:port 还是 topic。
  //
  // fire-and-forget:返回成功只表示"已入队",不表示已发出;写出的一切结果不回传,只落
  // 传输的 `LastError()`。这里能拿到的错误只有生命周期非法一种。
  if (auto written = transport_.AsyncWrite({std::move(bytes), Endpoint::Default()});
      !written) {
    return written;
  }
  RecordEvent(kTraceCategorySend, config_.trace_sink, {}, {}, {}, {},
              static_cast<long>(sent_bytes));
  return Coro::Result<void>{};
}

Coro::Result<Message> ProtocolNode::Request(Message req,
                                            std::chrono::milliseconds timeout) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);  // 未启动 / 关闭中 / 已关闭。
  }
  // 总超时自本函数入口起算,涵盖分配 session、登记订阅、编码与入队的全部时间;零值套用
  // 配置默认值。以下各步均无挂起点,故实际耗时可忽略,但仍按契约扣减。
  const auto began = std::chrono::steady_clock::now();
  const auto total = timeout > std::chrono::milliseconds::zero()
                         ? timeout
                         : config_.default_request_timeout;

  req.frm_type = FrameType::kCommand;
  req.protocol_id = config_.protocol_id;
  req.session_id = NextSession();

  // **先登记订阅、再发出请求**:反之则回应可能先于订阅到达而被丢弃。凭据析构自动注销。
  auto ticket = dispatcher_.Subscribe(ResponseTo(req));
  if (auto queued = EncodeAndWrite(req); !queued) {
    return queued.error();
  }

  const auto spent = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - began);
  return ticket.Wait(spent < total ? total - spent : std::chrono::milliseconds{1});
}

Coro::Result<void> ProtocolNode::Send(Message msg) {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);
  }
  // 盖章:调用方给出的业务类型优先,否则取命令帧;默认协议 id;下一个 session_id。
  // 本调用不期待应答,不登记订阅。
  if (msg.frm_type == FrameType::kUnknown) {
    msg.frm_type = FrameType::kCommand;
  }
  msg.protocol_id = config_.protocol_id;
  msg.session_id = NextSession();
  return EncodeAndWrite(msg);
}

std::uint8_t ProtocolNode::NextSession() { return next_session_++; }

void ProtocolNode::DecodeAndDispatch(Datagram datagram) {
  const auto& bytes = datagram.bytes;
  auto decoded = codec_->Decode(bytes.data(), bytes.size());
  if (!decoded) {
    // 坏帧 / codec 错误:丢弃(codec 内部 resync)。归因 kBadFrame。
    TraceDrop(config_.trace_sink, DropReason::kBadFrame);
    return;
  }
  // Decode 成功边界:一次 Decode 调用一条事件,不逐条消息重复。
  RecordEvent(kTraceCategoryDecode, config_.trace_sink, {}, {}, {}, {},
              static_cast<long>(bytes.size()));
  for (const auto& msg : decoded.value()) {
    // Read 解出消息边界:按解出的消息计,不逐字节。
    RecordEvent(kTraceCategoryRecv, config_.trace_sink, {}, {}, {}, {},
                static_cast<long>(msg.payload.size()));
    Dispatch(msg);
  }
}

void ProtocolNode::Dispatch(const Message& msg) {
  // **唯一投递路径**:交由 Dispatcher 按键投递,命中的订阅者各得一份副本(ADR-0009 D1)。
  if (dispatcher_.Dispatch(msg) > 0) {
    return;
  }
  // 无人认领。终结帧(kResponse / kResult)此时属于迟到、乱序或无对应请求——这是请求-响应
  // 侧的异常,仍须归因。
  if (msg.frm_type == FrameType::kResponse || msg.frm_type == FrameType::kResult) {
    TraceDrop(config_.trace_sink, DropReason::kUnmatchedOrLateResponse);
    return;
  }
  // 其余为业务帧:**静默丢弃,不归因**(ADR-0009 D5)。订阅模型下"没人订阅"是宿主的正常
  // 选择(只订阅自己关心的帧)而非异常,记为丢弃会把常态噪声混进丢弃归因。代价是这类帧
  // 成为不可见丢弃,完整性归因的覆盖面随之变窄——已明确接受。
}

MessageDispatcher::Ticket ProtocolNode::Subscribe(MessageDispatcher::Key key) {
  return dispatcher_.Subscribe(std::move(key));
}

}  // namespace transport
