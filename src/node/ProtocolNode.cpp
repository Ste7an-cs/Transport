#include "transport/node/ProtocolNode.hpp"

#include <chrono>
#include <cstdint>
#include <utility>
#include <vector>

#include "await/awaitable.hpp"

#include "task/fibertask.h"  // Coro::makeTask —— 读-分发循环 fiber。

#include "transport/core/Error.hpp"

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

Coro::Result<void> ProtocolNode::DoStart() {
  // 本节点的配置面已无需校验项:时限逐次传参(SRS §3.1.4.4 / ADR-0010 D6),"不得永不
  // 超时"由 `ValidateInteraction` 的参数校验直接拒绝非正值,不再依赖启动期的配置校验。
  //
  // **不启动 transport**:宿主已经启过。读侧取自己的一路订阅——关它只终止本节点,
  // 各订阅者各得全量副本;写侧无句柄可取,直接调 transport_.Write()。
  rx_ = transport_.AsyncRead()->shared();

  // 本节点只 spawn 这一条 fiber。入站业务的消费 fiber 属宿主,由其自行 spawn 与 join。
  SpawnReadLoop();
  return Coro::Result<void>{};
}

Coro::Result<void> ProtocolNode::DoClose() {
  // close 本节点这一路读订阅 → 读循环的 await 立即得到终止错误而退出。
  // 残留数据一并丢弃:关闭即停止交付。
  //
  // **close 是整流传播的**(AsyncTask 417790c 起):它关闭 hub 表里全部消费者队列,
  // 源 read_queue 与同一条传输上的其它订阅者**一并终结**。这是**有意为之**——
  // 节点关闭即读侧终结,宿主随后关传输,两者一起关。
  // 不用"析构句柄只退订自己"那条路径:它唤不醒此刻正阻塞在 await(rx_) 里的读循环。
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
  // 目的地恒填 `Endpoint::Default()`,交给传输解析成它自己配置的默认对端:本类传输无关,
  // 不知道也不该知道对端是 ip:port 还是 topic。
  //
  // fire-and-forget:返回成功只表示"已入队",不表示已发出;写出的一切结果不回传,只落
  // 传输的 `LastError()`。这里能拿到的错误只有生命周期非法一种。
  if (auto written = transport_.AsyncWrite({std::move(bytes), Endpoint::Default()});
      !written) {
    return written;
  }
  return Coro::Result<void>{};
}

Coro::Result<void> ProtocolNode::ValidateInteraction(
    const RetryPolicy& retry) const {
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);  // 未启动 / 关闭中 / 已关闭。
  }
  // 时限是在途交互唯一的兜底终结源(写出是 fire-and-forget、断链不终结在途交互),故不
  // 接受"零即永不超时";次数含首发,少于一次意味着一帧都不发,无意义。
  if (retry.max_attempts < 1 ||
      retry.timeout <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  return Coro::Result<void>{};
}

Coro::Result<Message> ProtocolNode::AwaitAccept(
    const Message& req, const RetryPolicy& retry,
    MessageDispatcher::Ticket& ack_ticket) {
  // 前置:ack_ticket 已由调用者登记(D4)——登记必须先于第一次发出。
  for (int attempt = 0; attempt < retry.max_attempts; ++attempt) {
    // 重发的是**字节完全相同**的原帧(D3):req 全程不改,session_id 不变,故原订阅横跨
    // 全部重发继续有效。
    if (auto queued = EncodeAndWrite(req); !queued) {
      return queued.error();  // 编码失败 / 生命周期非法——不属超时,不重试。
    }
    auto ack = ack_ticket.Wait(retry.timeout);
    if (ack) {
      return ack;  // D3:首个到达者即终结本阶段,不问它对应第几次尝试。
    }
    if (ack.error() != make_error_code(TransportErrc::kTimeout)) {
      return ack.error();  // kClosed 等终止原因直接透出,重试无意义。
    }
    // 超时 → 重发。
  }
  // D12:本质不是"超时"而是"对端始终没有受理",与"已受理但执行慢"是两类事实。
  return make_error_code(TransportErrc::kNotAccepted);
}

Coro::Result<Message> ProtocolNode::RequestForResponse(Message req,
                                                       RetryPolicy retry) {
  if (auto valid = ValidateInteraction(retry); !valid) {
    return valid.error();
  }
  req.frm_type = FrameType::kCommand;
  req.protocol_id = config_.protocol_id;
  req.session_id = NextSession();

  // **先登记订阅、再发出**(D4):反之则回应可能先于订阅到达而被丢弃。
  auto ack = dispatcher_.Subscribe(ResponseTo(req));
  return AwaitAccept(req, retry, ack);
}

Coro::Result<Message> ProtocolNode::RequestForResult(
    Message req, RetryPolicy retry, std::uint16_t result_message_id,
    std::chrono::milliseconds result_timeout) {
  if (auto valid = ValidateInteraction(retry); !valid) {
    return valid.error();
  }
  if (result_timeout <= std::chrono::milliseconds::zero()) {
    return make_error_code(TransportErrc::kInvalidArgument);
  }
  req.frm_type = FrameType::kCommand;
  req.protocol_id = config_.protocol_id;
  req.session_id = NextSession();

  // D4:两个订阅**一起**在发命令之前登记——kResult 可能先于 kResponse 到达(对端足够快),
  // 若等收到受理再登记结果订阅,该帧会因无匹配而被丢弃。结果帧的命令码与请求帧不同,是
  // 协议知识,由调用方给出(D7)。
  auto ack = dispatcher_.Subscribe(ResponseTo(req));
  auto result = dispatcher_.Subscribe(
      FrameOf(req.session_id, result_message_id, FrameType::kResult));

  if (auto accepted = AwaitAccept(req, retry, ack); !accepted) {
    return accepted.error();
  }
  // D5:受理阶段一完成立即注销 ack 订阅——重发会引出重复 kResponse,不注销则它们继续落入
  // 信箱;注销后它们成为无匹配终结帧,按 kUnmatchedOrLateResponse 归因丢弃。
  ack.Reset();

  // D2:本阶段**不重发**——kResult 未达意味着对端正在执行,重发有使其重复执行的风险。
  auto result_msg = result.Wait(result_timeout);
  if (!result_msg) {
    return result_msg;  // 超时即 kTimeout(区别于受理耗尽的 kNotAccepted)。
  }

  // D8:回应结果是本模型**固有的最后一步**,不是可选项。该帧完全由收到的 kResult 派生——
  // payload 原样回显(整块拷贝,框架不解读)、session_id 与 message_id 沿用,**仅**改帧类型;
  // CRC 由 ICodec::Encode 重算。不走 Send():它会强制盖新 session_id 与 kCommand。
  Message reply = result_msg.value();
  reply.frm_type = FrameType::kResponse;
  if (auto sent = EncodeAndWrite(reply); !sent) {
    return sent.error();  // 回应结果发送失败 ⇒ 整次交互返错。
  }
  return result_msg;
}

Coro::Result<Message> ProtocolNode::RequestForResultDirect(
    Message req, RetryPolicy retry, std::uint16_t result_message_id) {
  // 本交互只有一个等待阶段,其时限即 retry.timeout,故无独立的 result_timeout 需校验。
  if (auto valid = ValidateInteraction(retry); !valid) {
    return valid.error();
  }
  req.frm_type = FrameType::kCommand;
  req.protocol_id = config_.protocol_id;
  req.session_id = NextSession();

  // **只有一个订阅**——本交互不存在受理帧(D13),故没有 ack 订阅可登记,也无 D5 的注销。
  // 仍是"先登记、再发出":反之则结果帧可能先于订阅到达而被丢弃。
  auto result = dispatcher_.Subscribe(
      FrameOf(req.session_id, result_message_id, FrameType::kResult));

  // **独立的重发循环,不走 AwaitAccept()**:那个骨架等 kResponse、耗尽返 kNotAccepted,
  // 两处语义都不适用于本交互。
  for (int attempt = 0; attempt < retry.max_attempts; ++attempt) {
    // 重发的是**字节完全相同**的原帧(D3):req 全程不改,session_id 不变,故订阅横跨全部
    // 重发继续有效,最先到达的那一帧即终结本次交互。
    if (auto queued = EncodeAndWrite(req); !queued) {
      return queued.error();  // 编码失败 / 生命周期非法——不属超时,不重试。
    }
    auto got = result.Wait(retry.timeout);
    if (got) {
      return got;  // 收到即成功,**不回应任何帧**(与 RequestForResult 的 D8 末步相反)。
    }
    if (got.error() != make_error_code(TransportErrc::kTimeout)) {
      return got.error();  // kClosed 等终止原因直接透出,重试无意义。
    }
    // 超时 → 重发。本交互**在等结果阶段重发**(D13 / RT_NODE_002_g):它没有受理阶段,
    // 唯一的等待就是等结果,不重发则命令帧一旦丢失即彻底失败、无任何补救。
    // RT_NODE_002_c 的"等 kResult 不得重发"只约束外部系统协议,与本条并存不矛盾。
  }
  // D12:返 kTimeout 而**非** kNotAccepted——后者的语义是"对端没有受理",而本交互根本
  // 不存在受理这一步,"未受理"这一事实不存在。
  return make_error_code(TransportErrc::kTimeout);
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
    // 坏帧 / codec 错误:**丢弃**(codec 内部 resync)。观测面撤销后不再归因、不再记录,
    // 丢弃动作本身不变(ADR-0014 D1/D4)。
    return;
  }
  for (const auto& msg : decoded.value()) {
    Dispatch(msg);
  }
}

void ProtocolNode::Dispatch(const Message& msg) {
  // **唯一投递路径**:交由 Dispatcher 按键投递,命中的订阅者各得一份副本(ADR-0009 D1)。
  if (dispatcher_.Dispatch(msg) > 0) {
    return;
  }
  // 无人认领。终结帧(kResponse / kResult)此时属于迟到、乱序或无对应请求——这是请求-响应
  // 侧的异常;业务帧则是宿主"只订阅自己关心的帧"的正常选择(ADR-0009 D5)。**两者的处置
  // 相同:丢弃。** 观测面随 ADR-0014 D1 撤销后框架已无处记录二者之别,故此处不再分支。
  // 代价(ADR-0014 D4)是丢弃完全不可见——已明确接受。
}

Coro::Result<MessageDispatcher::Ticket> ProtocolNode::Subscribe(
    MessageDispatcher::Key key) {
  // **相位判定**,判据与三个交互方法**同一个** `IsRunning()`——`kClosed` 一并覆盖"未启动 /
  // 关闭中 / 已关闭",这是本类各 `@return` 早已写明的既有约定,本方法不单开一份。写法与
  // `DdsNode::Subscribe` 逐字一致(ADR-0009 D1′ / ADR-0013 D8)。
  //
  // - **`Created` 也返 `kClosed`**:`Subscribe` **只在 `Running` 受理**,还没 `Start()` 就
  //   订阅是**禁用法**,不是"早一点也行"。放行它有一处真实危害:`NodeBase::Close()` 从
  //   `Created` 走时**不调 `DoClose()`**,`dispatcher_.CloseAll` 因此从不执行——而"信箱被
  //   关"是订阅者**唯一**的协作取消信号(D4)。宿主若在此相位订阅并 spawn 了消费 fiber、
  //   随后放弃启动,那条 fiber 的信箱**永远等不到关闭信号**,join 时挂住。
  //   不是悬垂(`Ticket` 持 `weak_ptr`),是唤醒信号永远不发——静默挂起。
  // - **`Closing` / `Closed` 同样 `kClosed`**:此时 `DoClose()` 已 `CloseAll`,再登记只能
  //   得到一张信箱已关闭的凭据。让它**在返回处**就说清楚,而不是推迟到第一次 `Wait`——
  //   D1′ 把本方法从裸 `Ticket` 改成 `Coro::Result<Ticket>` 的理由原样适用于相位。
  if (!IsRunning()) {
    return make_error_code(TransportErrc::kClosed);  // 未启动 / 关闭中 / 已关闭。
  }
  return Coro::Result<MessageDispatcher::Ticket>{
      dispatcher_.Subscribe(std::move(key))};
}

}  // namespace transport
