#pragma once

/**
 * @file DdsTransport.hpp
 * @brief 协程原生 DDS 传输——listener 直推读队列 + 一条专属写线程(ADR-0013)。
 */

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <system_error>
#include <thread>

#include "await/awaitable.hpp"  // Coro::Awaitable —— 读队列。

#include "transport/core/TransportTypes.hpp"
#include "transport/io/ITransport.hpp"
#include "transport/io/dds/DdsConfig.hpp"
#include "transport/io/dds/IDdsProvider.hpp"

namespace transport {

/**
 * @brief 协程原生 DDS 传输——**双队列样板的第四次跟进**(ADR-0013 D1)。
 *
 * ```
 *         read_queue_ (Coro::Awaitable, 有界 1024, 静默丢最旧)
 *              ▲
 *              │ push(【外来线程】上直推,无泵 fiber)
 *         ┌────┴─────┐        write_queue_ (mutex + condition_variable + deque)
 *         │ listener │              │
 *         └────▲─────┘              ▼
 *              │             ┌──────────────┐
 *              │             │ 专属 OS 线程  │ → provider_->Publish()
 *              │             └──────────────┘
 * ```
 *
 * `ITransport` 七方法**签名不变、语义不变、不分叉**(**D1**);公开面在七方法之外另有
 * `DeclareWriter` / `DeclareReader` 两个 DDS 专有的端点声明方法(**D15**)——它们是 DDS
 * 端点模型的必需品,三介质没有对应物,但**不改动 `ITransport` 本身**。
 *
 * **与三介质的两处实质差异**:
 *
 * | | UDP / TCP / 串口 | **DDS** |
 * |---|---|---|
 * | 谁推读队列 | **泵 fiber** 从 socket/device 读流取数后推 | **provider 的 listener 在外来线程**上推(**D2**,故**读侧没有泵 fiber**) |
 * | 谁消费写队列 | **写泵 fiber** | **一条专属 OS 线程**(**D3**) |
 *
 * ### 写侧为什么是线程而不是 fiber(**D3**)
 *
 * `DataWriter::write()` 是纯同步接口,直接 **park 调用线程**;用 fiber 会卡死整条线程上的
 * 所有 fiber。且这段阻塞**没有上界**——Fast DDS 默认 `INTRAPROCESS_FULL`,**同进程订阅方的
 * `on_data_available` 就跑在发布线程上**。
 *
 * ### 由此:listener 必须快且不阻塞,这是硬约束而非建议
 *
 * 承上——这条专属写线程**兼跑同进程内所有对端的交付回调**。故本类的 listener 只做一件事:
 * 把字节 `push` 进 `read_queue_`(`lock` + `push_back` + `notify_all`,**无等待路径**,满时
 * 丢最旧也不阻塞)。**不解码、不加锁等待、不打日志**。同进程内**非本框架**的慢订阅方不受
 * 我方约束,那是部署面的事,框架无法强制。
 *
 * ### 关闭路径的最坏等待【无上界】
 *
 * `Close()` 只发信号;`WaitClosed()` join 专属写线程,而**在途的 `Publish` 打不断**
 * (Fast DDS 3.6.1 的 `DataWriter` 上没有任何中止 `write()` 的入口)。
 * 故最坏等待 **= 那一次在途 `Publish` 自己跑完所需的时间**,它由**同进程内最慢的那个订阅
 * 回调**决定,**不是**一个 `max_blocking_time`(ADR-0013「明确接受的代价」7)。
 *
 * ### 线程模型
 *
 * - 七方法 + 两个 `Declare*` 只在**调用方的执行域**(起本对象的那个 fiber 线程)内调用;
 * - `read_queue_` 由 provider 的 listener 线程写、由调用方 fiber 读(跨线程 `push` 安全);
 * - `write_queue_` 由调用方 fiber 写、由专属线程读,以 `std::mutex` + `std::condition_variable`
 *   保护——**不能用 `Coro::Awaitable`**,它的 `pop` 在非协程线程上会 crash(**D3**);
 * - `last_error_` 两侧都写,故单设一把小锁。
 *
 * 析构 `Close()` + `WaitClosed()`,故写线程不可能活过本对象。
 */
class DdsTransport final : public ITransport {
 public:
  /// @brief `write_queue_` 的容量上限:与三介质的队列**逐字相同**(1024,满时静默丢最旧)。
  ///
  /// `read_queue_` 用的是 `Coro::Awaitable` 的默认容量,恰好也是 1024(**D11**、SDD DD-15);
  /// 本常量只管写侧那条自建的 deque,使两侧口径一致。**写侧尤其需要它**:在途 `Publish`
  /// 的阻塞无上界,不设界则积压无上界。
  static constexpr std::size_t kWriteQueueCapacity = 1024;

  /// @brief 以 `DdsConfig` 构造——**尚未建 provider**,它在 `Start()` 里按名从
  ///        `DdsProviderRegistry` 取(**D12**)。
  explicit DdsTransport(DdsConfig config);

  /// @brief 析构:请求关闭、join 写线程、`Shutdown()` provider。
  ~DdsTransport() override;

  DdsTransport(const DdsTransport&) = delete;
  DdsTransport& operator=(const DdsTransport&) = delete;

  // ── ITransport 七方法 ────────────────────────────────────────────────

  /// @brief 校验配置、按名建 provider 并 `Init`、起专属写线程,进入 Running。
  ///
  /// **不声明任何端点**——topic 不在配置里(**D16**),端点由调用方在启动时逐项
  /// `DeclareWriter` / `DeclareReader`(**D15**)。
  ///
  /// @return 成功;配置非法(`domain_id` 越界 / `provider` 空或未注册 /
  ///         `max_blocking_time`、`liveliness_lease` 非正)返 `kConfiguration` 并
  ///         **停在 `Created`**(可改配后重试,**D12**);provider `Init` 失败原样返其
  ///         错误、同样停在 `Created`;关闭中 / 已关闭返 `kInvalidState`。已 Running
  ///         时重复调用为成功 no-op。
  Coro::Result<void> Start() override;

  /// @brief 交出 `read_queue_` 的等待器句柄(ADR-0007 D4),与三介质完全一致。
  ///
  /// 每个元素是**一条完整样本**的字节,`peer` 是 `Endpoint::Topic(来源 topic)`——topic
  /// **不上线缆**(**D5**),入站靠 listener 闭包捕获的 topic 带出。
  ///
  /// **有界 1024 + 满时静默丢最旧**(**D11**),与 UDP/TCP/串口逐字相同:**不加计数器、
  /// 不加归因**。由此 `RELIABLE` QoS 被本地队列架空——listener 一搬走样本 DDS 即认为已
  /// 交付、背压解除。
  ///
  /// @return `read_queue_` 句柄;未 `Start()` 时给出以 `kInvalidState` 关闭的句柄,
  ///         我方 `Close()` 后为以 `kClosed` 关闭的句柄。
  [[nodiscard]] std::shared_ptr<Coro::Awaitable<Datagram>> AsyncRead() override;

  /// @brief 送入写队列即返(ADR-0007 D3):**只判生命周期与入队**,不等实际发出。
  ///
  /// `datagram.peer` 须是 `Endpoint::Topic`——但**不在此处判**:契约只允许本方法判两件
  /// 事,故非 topic 目的地由写线程丢弃并落 `LastError()`(`kInvalidArgument`),与 UDP
  /// 解析不出目的地时的处置同形。
  ///
  /// @return 成功仅表示**已入队**;未 `Start()` 返 `kInvalidState`,关闭中 / 已关闭返
  ///         `kClosed`。写出的一切结果(含 `RETCODE_TIMEOUT`)只落 `LastError()`、不回传。
  [[nodiscard]] Coro::Result<void> AsyncWrite(Datagram datagram) override;

  /// @brief 请求关闭(幂等,**只发信号不等收敛**):关读队列停止交付、置写线程停止位并
  ///        清空写队列残留、唤醒写线程。
  ///
  /// **不在这里 `Shutdown()` provider**:那会等在途 `Publish` 跑完(无上界),而本方法
  /// 契约是受理即返。收敛落在 `WaitClosed()`。
  Coro::Result<void> Close() override;

  /// @brief join 专属写线程,再 `Shutdown()` provider——返回即可安全释放。
  ///
  /// **最坏等待无上界**:在途的 `Publish` 打不断(3.6.1 的 `DataWriter` 上没有中止入口),
  /// 界由**同进程内最慢的那个订阅回调**决定。先 join 再 `Shutdown()` 也正是为了不绕开
  /// provider 的在途计数——writer 只在无在途写时才被删。
  ///
  /// **本方法阻塞的是调用线程,不只是调用 fiber**(`std::thread::join`),这是"写侧多一条
  /// OS 线程"的连带代价。未 `Start()` 或已汇合时立即返回。
  void WaitClosed() override;

  /// @brief 最近一次操作错误(无则默认构造的 `error_code`)。
  [[nodiscard]] std::error_code LastError() const override;

  /// @brief 当前链路可用性——`matched` + `Liveliness` 三态(**D9**)。
  ///
  /// | 状态 | 判据 |
  /// |---|---|
  /// | `kUp` | `matched > 0` **且** `alive > 0` |
  /// | `kEstablishing` | 已声明过端点但 `matched` 尚未 `> 0`(约 240ms 发现窗口,**无 DDS 原生事件**,由我方状态推出) |
  /// | `kDown` | 未 `Start()` / 关闭中 / 已关闭 / 一个端点都没声明 / `matched > 0` 但 `alive == 0` |
  ///
  /// **三介质里只有 TCP 也给 `kEstablishing`**,但成因不同:TCP 是连接窗口,DDS 是发现窗口。
  [[nodiscard]] LinkState CurrentLinkState() const override;

  // ── DDS 专有:端点声明(D15,在 ITransport 七方法之外)────────────────

  /// @brief 声明本节点在该 topic 上**发**:落到 provider 就是 `DeclareWriter(topic)`,
  ///        **当场建出 `DataWriter`**(**D13**)。**幂等**。
  ///
  /// **只由外部在启动时调用**(`DdsNode::DoStart()`,**D15**):运行期不再有建端点的路径。
  /// **必须幂等**,因为注册里可能重复(同一 topic 既是订阅项、又是某条 client 的应答 topic)。
  ///
  /// 约 240ms 的发现窗口在这里付掉,首帧(尤其服务端的第一次应答)不会赶在 `DataWriter`
  /// 与对端 reader 尚未 match 时发出而丢掉。
  /// 未经本方法声明的 topic,写线程上的 `Publish` 会返 `kConfiguration` 并落到 `LastError()`。
  ///
  /// @return 成功;topic 为空返 `kConfiguration`;未 `Start()` / 关闭中 / 已关闭返
  ///         `kInvalidState`;provider 建 writer 失败原样返其错误。
  [[nodiscard]] Coro::Result<void> DeclareWriter(const std::string& topic);

  /// @brief 声明本节点在该 topic 上**收**:落到 provider 就是 `Subscribe(topic, cb)`。**幂等**。
  ///
  /// 回调**闭包捕获 `topic`**,用来填 `Datagram.peer`(**D5**:topic 不上线缆,入站由
  /// `peer` 带出)。回调只捕获读队列的分发端、**不捕获 `this`**,故迟到的样本不会触碰已
  /// 销毁的对象。
  ///
  /// **两个方法而非一个**(**D15**):一个 topic 上通常只需要一侧——客户端在请求 topic
  /// 上只发不收、在应答 topic 上只收不发,服务端反之。**建成对是浪费,还会招来自收**。
  ///
  /// @return 成功;topic 为空返 `kConfiguration`;未 `Start()` / 关闭中 / 已关闭返
  ///         `kInvalidState`;provider 建 reader 失败原样返其错误。
  [[nodiscard]] Coro::Result<void> DeclareReader(const std::string& topic);

  /// @brief 是否处于 Running(写线程在跑;链路是否可用另见 `CurrentLinkState()`)。
  [[nodiscard]] bool IsRunning() const;

 private:
  /// @brief `Start()` 的一次性配置校验(**D12**):非法返 `kConfiguration`。
  [[nodiscard]] Coro::Result<void> ValidateConfig() const;
  /// @brief 专属写线程主体:取一条 → `provider_->Publish()`。**唯一允许阻塞的地方**。
  void RunWriteThread();
  /// @brief 记一次操作错误(写线程与调用方两侧都会写,故加锁)。
  void SetLastError(std::error_code error);

  DdsConfig config_;
  std::unique_ptr<IDdsProvider> provider_;  ///< `Start()` 按名建,`WaitClosed()` 后失效。

  LifecycleState lifecycle_{LifecycleState::kCreated};
  bool joined_{false};  ///< `WaitClosed()` 已汇合:join 与 `Shutdown()` 都只做一次。

  mutable std::mutex error_mutex_;
  std::error_code last_error_;

  /// 已声明的端点(**D15** 的幂等闩)。只在调用方执行域内访问,不加锁。
  std::set<std::string> declared_writers_;
  std::set<std::string> declared_readers_;

  /// 对外读队列:listener 在**外来线程**上直推(**D2**)。有界 1024、满时静默丢最旧
  /// (`Coro::Awaitable` 的默认容量,**D11**)。
  std::shared_ptr<Coro::Awaitable<Datagram>> read_queue_{
      std::make_shared<Coro::Awaitable<Datagram>>()};

  /// 内部写队列——**普通线程件**,不是 `Coro::Awaitable`(**D3**:消费方是普通线程,而
  /// `Coro::Awaitable` 的 `pop` 在非协程线程上会 crash)。
  mutable std::mutex write_mutex_;
  std::condition_variable write_cv_;
  std::deque<Datagram> write_queue_;
  bool write_stop_{false};  ///< `Close()` 置位:写线程排空判据 + 退出判据。
  std::thread write_thread_;
};

}  // namespace transport
