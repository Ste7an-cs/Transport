#pragma once

// =============================================================================
// InteractionEngine.hpp — 通用交互引擎(整个 Comm 层的心脏)
//
// 一句话:把所有【交互机制】写成唯一一份,协议【差异】外包给 InteractionPolicy,
// 于是 DdsNode / ProtocolNode 退化成几十行的薄壳。
//
// 引擎提供 3 个原语,覆盖本库所有交互模式:
//   · Fire          —— 单向发一帧,不追踪(发布、单向命令、心跳/STATE 的每一拍)。
//   · RequestAwait  —— 发一帧并等回应:登记挂起、排超时、按 RequestSpec 驱动
//                       "等中间帧→等终结帧→超时重发"的状态机,恰好一次回调。
//   · StartPeriodic —— 周期发:立即一帧 + 自重排定时器;支持每拍取最新消息(工厂)。
//
// 引擎一处承载:挂起表、超时、重发、periodic 定时器、io→Decode→Post→单 worker
// Dispatch 的收包路径,以及全部并发纪律(见下)。协议怎么生成相关号、怎么判别帧、
// 应答发哪里,全问 InteractionPolicy —— 引擎只比较抽象的 Key/FrameTag,从不解释。
//
// 【并发模型】(为什么这份代码值得细看):
//   · 数据来自两类线程:① io/listener 线程(transport 的 OnBytes 回调);
//     ② executor 的单 worker 线程(所有业务回调、超时、periodic 在此串行跑);
//     ③ 调用方线程(用户调 Fire/RequestAwait/Send… 当场执行)。
//   · 共享状态(pending_/periodics_/计数器)由 mu_ 保护。
//   · 铁律:Encode + transport->Send + 任何【用户回调/工厂】一律在 mu_ 之外执行,
//     绝不持锁回调用户代码 —— 否则慢回调阻塞全引擎、重入回调死锁(mu_ 非递归)。
//     做法:持锁时把要发的东西/要调的回调拷贝或移出到局部,放锁后再发/再调。
//   · 终结回调【移出】调用、中间回调【拷贝】调用 → 保证终结恰好一次(防 Close/断连
//     与正常路径双触发)。
//   · 生命周期:posted 任务/定时器都捕获 weak_ptr,引擎没了就安全跳过。
//
// 须以 std::shared_ptr 持有(enable_shared_from_this:posted lambda 里 weak_from_this)。
// 用户一般不直接用引擎,而是用 DdsNode/ProtocolNode(它们各持一个引擎)。
// =============================================================================

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "transport/ICodec.hpp"
#include "transport/ITransport.hpp"
#include "transport/Message.hpp"
#include "transport/Result.hpp"
#include "transport/comm/IExecutor.hpp"
#include "transport/comm/InteractionPolicy.hpp"
#include "transport/ITraceSink.hpp"

namespace transport {

class InteractionEngine : public std::enable_shared_from_this<InteractionEngine> {
 public:
  // 注入三个缝 + 一个策略:
  //   transport —— 纯字节管道(TCP/UDP/串口/DDS);引擎注册其 OnBytes/OnConnect/OnDisconnect。
  //   codec     —— 线缆格式(Encode/Decode);引擎在收发边界 Message↔字节。
  //   policy    —— 协议语义(见 InteractionPolicy)。
  //   executor  —— 线程模型;null → 自建 ThreadExecutor(1 worker + 有界队列 + 定时器)。
  //   queue_capacity —— 仅在自建 ThreadExecutor 时用,任务队列容量(满则 Post 阻塞=背压)。
  InteractionEngine(std::shared_ptr<ITransport> transport,
                    std::unique_ptr<ICodec> codec,
                    std::unique_ptr<InteractionPolicy> policy,
                    std::unique_ptr<IExecutor> executor = nullptr,
                    std::size_t queue_capacity = 1024);
  ~InteractionEngine();  // = Close()(幂等),保证析构前停掉线程/定时器。

  // Open:启动 executor、注册 transport 回调、打开 transport。失败则回滚(不留半开状态)。
  Status Open();
  // Close:幂等。先在锁内终结所有挂起请求(回 conn:)+ 取消全部定时器,再 executor->Stop()
  //   (drain+join,阻塞到 worker 退出),最后关 transport。Stop 的 join 是生命周期屏障:
  //   在途的 Dispatch/FirePeriodic 必定先跑完,之后才可能析构节点状态。
  void   Close();
  bool   IsOpen() const { return open_.load(); }

  // ---- 入站回调(节点在 Open 前接好;在 worker 线程被调)----
  // 无主入站帧经 policy.RouteUnmatched 分流:kInboundRequest→on_request_、kDeliver→on_deliver_。
  void OnInboundRequest(std::function<void(const Message&)> cb) { on_request_ = std::move(cb); }
  void OnInboundDeliver(std::function<void(const Message&)> cb) { on_deliver_ = std::move(cb); }
  // 传输读错误/解码错误经此上报(纯观测,不改控制流)。
  void OnError(std::function<void(const std::string&)> cb) { on_error_ = std::move(cb); }

  // 可插拔结构化 trace。须在 Open() 前调用:Open 后埋点只【读】trace_,而设置发生在
  // 单线程的装配期,故无数据竞争(指针在 worker/io 线程起来前已固定)。
  void SetTrace(std::shared_ptr<ITraceSink> t) { trace_ = std::move(t); }

  // ---- 3 原语 ----
  // Fire:单向发 out(先 SetTag(out,tag) 再 Encode+Send)。不登记、不等待。
  Status   Fire(Message out, FrameTag tag, const Endpoint& to = Endpoint::Default());
  // RequestAwait:发 out 并按 spec 等回应。锁内 NewCorrelation+登记挂起+排超时,锁外发送;
  //   发送失败则回滚挂起并以错误回调 on_terminal。回应/超时由 Dispatch/OnTimeout 推进。
  Status   RequestAwait(Message out, RequestSpec spec, const Endpoint& to = Endpoint::Default());
  // StartPeriodic(工厂版):周期发。立即发一帧,然后每 interval_ms 一帧。
  //   make() 在执行器线程上、每次发送前(含立即首拍)被调用,直到 StopPeriodic/Close。
  //   ⇒ 每拍发的是【最新】状态(发送前现取)。
  //   契约:make 须线程安全、非阻塞、快、不抛。null make 或 interval_ms==0 → 返回 0(不启动)。
  //   返回值是 periodic handle(给 StopPeriodic/UpdatePeriodic 用);0 表示未启动。
  uint32_t StartPeriodic(std::function<Message()> make, FrameTag tag, uint32_t interval_ms,
                         const Endpoint& to = Endpoint::Default());
  // StartPeriodic(固定版):每拍发同一份 out。内部包装成工厂 [m]{return m;},行为与历史一致。
  uint32_t StartPeriodic(Message out, FrameTag tag, uint32_t interval_ms,
                         const Endpoint& to = Endpoint::Default());
  // UpdatePeriodic:推送更新某 periodic 的消息(事件驱动模型)。下一拍起生效(已在途的一拍
  //   可能再发一帧旧的——最多一帧陈旧,最终一致)。注意:它把 make 换成"返回固定 out",所以
  //   对一个【工厂启动】的 periodic 调用此法,会把它永久转成固定消息。handle 未知 → false。
  bool     UpdatePeriodic(uint32_t handle, Message out);
  // StopPeriodic:取消该 periodic 的定时器并移除。handle 未知则无操作。
  void     StopPeriodic(uint32_t handle);

  // SendReply:服务端应答 / 客户端自动 ack 用。按 request 经 policy.EchoCorrelation 回填相关号、
  //   policy.ReplyTo 算目的地,SetTag(reply,tag) 后发出。节点的 Responder 薄包此法。
  Status   SendReply(const Message& request, FrameTag tag, std::vector<uint8_t> payload);

 private:
  // 一个在途的 request-await。out/to 留着以便超时重发;retries 计已重发次数;timer 是当前
  // 超时定时器;advanced=true 表示已收到中间或终结帧(「首帧停重发」——此后超时只失败不重发)。
  struct Pending {
    RequestSpec spec; Message out; Endpoint to;
    uint32_t retries = 0; IExecutor::TimerId timer = 0; bool advanced = false;
  };
  // 一个周期任务。make 是消息工厂(固定版也包成工厂);timer 是下一拍定时器。
  struct Periodic { std::function<Message()> make; FrameTag tag; Endpoint to; uint32_t interval_ms; IExecutor::TimerId timer = 0; };

  // SendMessage:Encode(m) → transport->Send(bytes,to)。所有出站的唯一收口(锁外调用)。
  Status SendMessage(Message& m, const Endpoint& to);
  // Dispatch:单 worker 上对每条解出的入站 Message 做分发决策(命中挂起 / 无主路由)。
  void Dispatch(Message msg);
  // OnTimeout:某挂起请求超时——未推进且未达上限则重发,否则以 timeout: 终结。
  void OnTimeout(Key key);
  // HandleDisconnect:transport 断连——终结全部挂起请求(回 reason)。
  void HandleDisconnect(const std::string& reason);
  // FirePeriodic:某 periodic 到点——锁内取出工厂,锁外 make()+Fire,再排下一拍。
  void FirePeriodic(uint32_t handle);
  // Trace:trace_ 非空才构造/投递事件(无 sink 时单分支、近零开销)。
  void Trace(const TraceEvent& ev) const { if (trace_) trace_->OnTrace(ev); }

  std::shared_ptr<ITransport> transport_;          // 纯字节管道(可与他人共享,如 TCP 连接)
  std::unique_ptr<ICodec> codec_;                  // 线缆格式(引擎独占)
  std::unique_ptr<InteractionPolicy> policy_;      // 协议语义(引擎独占)
  std::unique_ptr<IExecutor> executor_;            // 线程模型(引擎独占)
  std::atomic<bool> open_{false};                  // 是否已 Open(发送前提)
  std::atomic<bool> closing_{false};               // Close 重入守卫(exchange 保证只跑一次)
  std::mutex mu_;                                  // 保护 pending_/periodics_/periodic_next_
  std::map<Key, Pending> pending_;                 // 在途请求:Key → 挂起项
  std::map<uint32_t, Periodic> periodics_;         // 在途周期任务:handle → 周期项
  uint32_t periodic_next_ = 1;                     // periodic handle 自增源(0 留作"未启动")
  std::function<void(const Message&)> on_request_; // 无主入站→请求 钩子
  std::function<void(const Message&)> on_deliver_; // 无主入站→投递 钩子
  std::function<void(const std::string&)> on_error_; // 错误上报钩子
  std::shared_ptr<ITraceSink> trace_;              // 可选 trace sink(Open 前固定)
};

}  // namespace transport
