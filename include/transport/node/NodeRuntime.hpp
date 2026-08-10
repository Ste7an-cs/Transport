#pragma once

/**
 * @file NodeRuntime.hpp
 * @brief 读-分发循环骨架 + handler 消费者持有件 NodeRuntime<Event>(**过渡形态**,
 *        ADR-0006 D1/D5 / RT_DESIGN_008 / RT_NODE_003)。
 *
 * **过渡说明(#139 → #140)**:生命周期(状态机、并发幂等 Start、关闭仲裁、收敛、致命错误
 * 自终、多等待者 WaitClosed)已整体上移到非模板基类 `NodeBase`(ADR-0006 D1/D6,#139);
 * 本件只剩 ADR-0006 **D5** 划归各 node 的两样东西——
 *
 *   1. **读-分发循环骨架** `SpawnReadLoop(fn, on_loop_exit)`:跑
 *      `Read → 错误分类(仅 kClosed 退出、其余继续)→ 调 node 的 decode+dispatch`
 *      (ADR-0004 D1:读取终止语义单值化,三介质同一段读循环、无介质分支);
 *   2. **可选 handler 消费者小件 `HandlerLoop<Event>` 的持有与驱动**(ADR-0006 D4):
 *      node 经本件 Spawn / Enqueue / 取消令牌 / 汇合(CancelAndClose、Join、DrainForClose)
 *      与三个 handler 观测计数。
 *
 * 二者按 ADR-0006 D5 应下放各 node、本件随之删除,由 **#140** 单独实施(ADR-0006 D7
 * "每票只改一件事":#139 只搬生命周期)。**本件不再持有任何生命周期状态、不再有锁。**
 *
 * **纪律(RT_NODE_003 / RT_DESIGN_008 / D10)**:Event 全程不透明——本件不读其任何字段
 * (字节计量靠构造时注入 byte_size_of 回调),不含 frm_type/session_id/correlation/连接概念;
 * 协议特有语义全内联在 node 的回调体里。
 */

#include <cstddef>
#include <chrono>
#include <functional>
#include <utility>

#include "task/fibertask.h"  // Coro::makeTask —— 读循环 fiber。

#include "transport/node/HandlerLoop.hpp"
#include "transport/core/Cancellation.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/ITraceSink.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/core/Result.hpp"
#include "transport/core/TransportTypes.hpp"

namespace transport {

/**
 * @brief 读-分发循环骨架 + 可选 handler 消费者小件的持有者(过渡件,#140 下放各 node)。
 *
 * @tparam Event 入站业务事件载荷类型(node 传 Message);本件对其不透明,仅经构造时注入的
 *               byte_size_of 回调计量字节,不读任何字段(RT_DESIGN_008 / D10)。
 *
 * 不可拷贝、不可移动(读循环 fiber 捕获 this;HandlerLoop 持 std::mutex)。
 */
template <typename Event>
class NodeRuntime {
 public:
  using Clock = OperationOptions::Clock;

  /**
   * @brief 构造。
   *
   * @param transport     非拥有的字节管道指针(node 持 unique_ptr,本件只用于读循环 Read);
   *                      须在本件之上存活(node 成员序保证)。关闭汇合的
   *                      `transport->RequestClose()` 由 node 的 `DoClose()` 发出。
   * @param byte_size_of  业务队列的字节计量回调(node 传 payload.size());本件不读字段。
   * @param max_events    业务队列事件数上界(越界由 BoundedQueue 钳制)。
   * @param max_bytes     业务队列字节数上界(越界由 BoundedQueue 钳制)。
   * @param trace_sink    可选 Trace 出口(P5-3/P5-4,ADR-0003 D13);非拥有,可为 nullptr。
   *                      转交 HandlerLoop(业务队列 kBusinessQueueOverflow 归因 + handler
   *                      调用起止点)。RT_TRACE_002:为空时不改变任何控制流/计数。
   */
  NodeRuntime(ITransport* transport,
              typename HandlerLoop<Event>::ByteSizeOf byte_size_of,
              std::size_t max_events, std::size_t max_bytes,
              ITraceSink* trace_sink = nullptr)
      : transport_(transport),
        handler_loop_(std::move(byte_size_of), max_events, max_bytes, trace_sink) {}

  NodeRuntime(const NodeRuntime&) = delete;
  NodeRuntime& operator=(const NodeRuntime&) = delete;

  // —— 读-分发循环骨架 ——————————————————————————————————————————————————————

  /**
   * @brief spawn 读-分发循环 fiber(骨架):Read → 错误分类 → 调 node 的 decode+dispatch,
   *        退出后调 @p on_loop_exit(node 传 `NodeBase::ConvergeAfterReadLoop`)。
   *
   * **三介质同一段读循环、无介质分支、无能力探测**(ADR-0004 D1 / RT_TRANSPORT_008):
   *
   * ```
   * Read() → 成功    → 解码分发
   *        → kClosed → 退出读循环
   *        → 其它     → 瞬时错误,继续
   * ```
   *
   * `kClosed` 是**唯一**的传输终结信号(我方关闭,或不具重连能力的传输发生底层致命错误);
   * 其余失败一律视为可继续的瞬时错误。具备自动重连的传输在内部透明处理链路中断——`Read`
   * 在重连期间挂起、重连后于新链路继续交付,读循环**看不到任何链路中断事件**,故此处不再
   * 有 `kConnection` 分支。
   *
   * **退出后本 fiber 兼任收敛者**(ADR-0005 D1):两条内部工作单元中读循环恒是第一个退出
   * 的,故它天然是收敛的正确位置——无需独立 finalizer fiber,也无人再等"读循环已退出"这
   * 一事件。收敛本身在 `NodeBase` 内(ADR-0006 D6),本件只负责在循环出口把控制权交回去。
   *
   * @param decode_and_dispatch 读到一个 Datagram 后的协议特有处理(node 内联 decode + 分发)。
   * @param on_loop_exit        读循环退出后的收敛入口(node 传基类的
   *                            `ConvergeAfterReadLoop`);**不得**为公开的 `Close()`(自等待)。
   */
  void SpawnReadLoop(std::function<void(Datagram)> decode_and_dispatch,
                     std::function<void()> on_loop_exit) {
    Coro::makeTask([this, fn = std::move(decode_and_dispatch),
                    on_exit = std::move(on_loop_exit)]() mutable {
      while (true) {
        auto datagram = transport_->Read();  // 裸读,无 deadline。
        if (!datagram) {
          if (datagram.error() == make_error_code(TransportErrc::kClosed)) {
            break;  // 传输终结(唯一终止语义)→ 退出读循环。
          }
          continue;  // 其它(瞬时错误):丢弃继续。
        }
        fn(std::move(datagram).value());  // node 内联 decode + 分发(协议特有)。
      }
      on_exit();  // 读循环兼任收敛者(D1);走基类内部路径,不得调公开的 Close(会自等)。
    });
  }

  // —— handler 消费者 + 业务队列(转发至可选小件 HandlerLoop,ADR-0006 D4)——————

  /**
   * @brief spawn 单消费者 handler fiber(串行消费业务队列):出队一条 → 跑 @p consume 到
   *        完成(含其 await)→ 再出下一条(严格串行,RT_HANDLER_003)。
   *
   * 机制全部在 `HandlerLoop<Event>::Spawn` 内(异常隔离、时长计量、fiber 句柄登记);
   * 本方法只是 bring-up 期的驱动点。**未调本方法即"未设 handler"**:HandlerLoop 保持无
   * 消费者状态,收敛时无可汇合者。
   */
  void SpawnHandlerLoop(std::function<void(Event&&)> consume) {
    handler_loop_.Spawn(std::move(consume));
  }

  /// @brief 入站业务事件入有界队列(满/已 Close 均丢弃、不阻塞);读循环分发业务帧时调。
  Status Enqueue(Event event) { return handler_loop_.Enqueue(std::move(event)); }

  /// @brief handler 协作取消令牌:Close 时被触发,node 经 HandlerContext 暴露给 handler。
  [[nodiscard]] CancellationToken HandlerCancellationToken() const {
    return handler_loop_.Token();
  }

  /// @brief 关闭汇合信号(node 的 `DoClose()` 内调):关业务队列 + 触发 handler 协作取消。
  void CancelAndCloseHandler() { handler_loop_.CancelAndClose(); }

  /// @brief 收敛汇合(node 的 `JoinHandler()` 内调):让出式 join 消费者 fiber,等其实际
  ///        退出(ADR-0005 D2 的 `FiberTask::get()`);未 Spawn 时立即返回。
  void JoinHandlerLoop() { handler_loop_.Join(); }

  /// @brief 收敛 Drain(node 的 `DrainUnstartedBusiness()` 内调):取尽队列内未启动的排队
  ///        业务并返回条数;归因 close_drop 由基类做(见 HandlerLoop 的"归因分工")。
  [[nodiscard]] std::size_t DrainHandlerForClose() {
    return handler_loop_.DrainForClose();
  }

  // —— 观测计数(handler 侧机制归因,协议无关;#140 随本件一并下放各 node)——————

  /// @brief 业务队列满而 tail-drop 的累计次数(命名归因 business_queue_overflow)。
  [[nodiscard]] std::size_t BusinessQueueOverflowCount() const {
    return handler_loop_.BusinessQueueOverflowCount();  // HandlerLoop/队列自守其锁。
  }

  /// @brief handler consume 逃逸异常被边界兜住、转 kInternal 隔离的累计次数(RT_HANDLER_006)。
  [[nodiscard]] std::size_t HandlerExceptionCount() const {
    return handler_loop_.HandlerExceptionCount();  // HandlerLoop 自守其锁。
  }

  /// @brief 观测:最近一次 handler 单次调用的处理时长(P5-4,RT_DATA_BUFFER)。尚无已
  ///        完成调用时为 0。简单存最近值(非直方图,分布分析留 P6)。
  [[nodiscard]] Clock::duration LastHandlerDuration() const {
    return handler_loop_.LastHandlerDuration();  // HandlerLoop 自守其锁。
  }

 private:
  ITransport* transport_;  ///< 非拥有字节管道(仅读循环 Read;RequestClose 由 node 发出)。
  /// handler 消费者小件(ADR-0006 D4):业务队列 + 消费者 fiber 句柄 + 协作取消 + 异常隔离
  /// + 时长计量。**可选**——未 SpawnHandlerLoop 时它只是个没人消费的空队列。自守其锁。
  HandlerLoop<Event> handler_loop_;
};

}  // namespace transport
