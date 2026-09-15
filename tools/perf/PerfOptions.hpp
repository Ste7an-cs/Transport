#pragma once

/**
 * @file PerfOptions.hpp
 * @brief `transport_perf` 的命令行面(ADR-0018 **D1** / **D5** / **D8** / **D9**)。
 *
 * 同一个可执行两个角色、三种介质、两套 suite:
 *
 * ```
 * transport_perf --role client|server --medium tcp|udp|dds --suite latency|throughput [...]
 * ```
 *
 * **`--medium serial` 不实现,传入即报错退出**(**D5**)——不留一个跑不通的分支冒充已
 * 支持。介质维度做成可扩展的形状:将来接串口只需在 `MediumKind` 补一档、在
 * `PerfLink` 补一个装配分支与其寻址参数,方法学与报表不变。
 */

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace perf {

/// 角色:客户端(发起端,**打印最终表格**)/ 服务端(对端真节点)。
enum class Role { kClient, kServer };

/// 介质。`serial` **不在此枚举内**——它在解析期即被拒(**D5**)。
enum class MediumKind { kTcp, kUdp, kDds };

/// 两套 suite:时延(**D2**)与吞吐(**D3**)。
enum class Suite { kLatency, kThroughput };

/// 被测面(**D5** 覆盖矩阵的行)。前四档属 `ProtocolNode`(TCP / UDP),后两档属 `DdsNode`。
enum class Mode {
  kSend,          ///< `ProtocolNode::Send`——需回显对端,时延口径 RTT/2。
  kResponse,      ///< `ProtocolNode::RequestForResponse`——天然 RTT。
  kResult,        ///< `ProtocolNode::RequestForResult`——**两个量**:到受理、到结果。
  kResultDirect,  ///< `ProtocolNode::RequestForResultDirect`——单阶段 RTT。
  kPubSub,        ///< `DdsNode` 发布-订阅——需回显对端,RTT/2。
  kReqResp,       ///< `DdsNode` 请求-响应——单阶段 RTT。
};

[[nodiscard]] const char* ToString(Role role);
[[nodiscard]] const char* ToString(MediumKind medium);
[[nodiscard]] const char* ToString(Suite suite);
[[nodiscard]] const char* ToString(Mode mode);

/// @brief 该模式的时延口径是否为 **RTT/2**(需回显对端),否则为**天然/单阶段 RTT**。
///
/// **D2** 给的是公式 `延迟 = RTT / 2 − 时钟开销`,**D5** 的矩阵则逐行给出各被测面**测的
/// 是哪个量**:回显型(`Send` / 发布-订阅)测单向、故取半;请求型的一次交互本身就是一个
/// 往返,取半没有意义,故报全程。表头逐行标出所用口径,列名仍与 Fast DDS 逐一同名。
[[nodiscard]] bool IsHalfRoundTrip(Mode mode);

/// @brief 该介质支持的模式清单(`--mode` 缺省即全跑)。
[[nodiscard]] std::vector<Mode> DefaultModes(MediumKind medium);

/// @brief 模式是否属于该介质的被测面。
[[nodiscard]] bool ModeAppliesTo(MediumKind medium, Mode mode);

/// @brief payload 档位的缺省值(**D8**):`SystemCodec` 的 `frm_len` 是 `uint16`,body 上限
///        65535,故 Fast DDS 矩阵里那行 1 MB 做不了;三个介质**统一**封顶 63488。
[[nodiscard]] std::vector<std::size_t> DefaultPayloads();

/// 全部命令行选项。缺省值即"本机自测跑得起来"的一组。
struct PerfOptions {
  Role role = Role::kClient;
  MediumKind medium = MediumKind::kTcp;
  Suite suite = Suite::kLatency;

  /// 要跑的模式;空表示按 `DefaultModes(medium)` 全跑。
  std::vector<Mode> modes;
  /// payload 档位(字节),逐档一行矩阵。
  std::vector<std::size_t> payloads = DefaultPayloads();

  // ── 时延 suite ───────────────────────────────────────────────────────
  int samples = 1000;  ///< 每档计入统计的样本数。

  // ── 吞吐 suite ───────────────────────────────────────────────────────
  int demand = 1000;       ///< 每个 burst 的条数。
  int recovery_ms = 5;     ///< burst 之间的歇息时长(毫秒)。
  int test_time_ms = 5000; ///< 每档矩阵的测试时长(毫秒)。

  // ── 两套共用 ─────────────────────────────────────────────────────────
  int warmup = 100;      ///< **D9**:每档开测前的预热条数,不计入。
  int timeout_ms = 2000; ///< 单次交互的等待时限(`RetryPolicy::timeout`)。
  int attempts = 5;      ///< 单次交互的总发送次数(含首发)。

  // ── 寻址 ─────────────────────────────────────────────────────────────
  std::string host = "127.0.0.1";  ///< TCP/UDP 客户端的对端地址。
  std::string bind_host = "0.0.0.0";  ///< TCP/UDP 服务端的本地绑定地址。
  std::uint16_t port = 45678;      ///< TCP:服务端监听端口;UDP:服务端本地端口。
  std::uint16_t local_port = 0;    ///< UDP 客户端本地端口(0 = 由 OS 分配)。
  int domain = 0;                  ///< DDS domain 编号。
  int silence_ms = 5000;           ///< TCP/UDP 的 `silence_timeout`。
};

/// 解析结果。三态:成功 / 只需打印帮助 / 出错(`error` 非空)。
struct ParseOutcome {
  bool ok = false;
  bool help = false;
  std::string error;
};

/// @brief 解析命令行。支持 `--key value` 与 `--key=value` 两种写法。
///
/// **不抛异常**——与库的既有约定一致,错误以 `ParseOutcome::error` 回传。
[[nodiscard]] ParseOutcome ParseOptions(int argc, char** argv, PerfOptions* out);

/// @brief 打印用法到 stdout。
void PrintUsage();

}  // namespace perf
