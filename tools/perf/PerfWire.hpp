#pragma once

/**
 * @file PerfWire.hpp
 * @brief `transport_perf` 的 **in-band 控制协议**与数据 payload 布局(ADR-0018 **D4** /
 *        **D3** / **D10**)。
 *
 * ## 控制走被测传输本身(**D4**)
 *
 * 不另开控制通道:握手、开始 / 结束、结果回传一律 in-band,用**保留的 `message_id`**
 * (`ProtocolNode`)与**保留服务名**(`DdsNode`)与数据流区分。
 *
 * **控制消息一律走带重发的交互**(`RequestForResultDirect`)而不是 `Send`——UDP 会丢包,
 * 控制流不能丢。这等于用框架自己的重发能力兜住控制可靠性,不手搓一套。
 *
 * **控制往返落在测量窗口之外**,不计入任何统计。
 *
 * ## 序号与时间戳都在 payload 里(**D10**)
 *
 * **不为测量给库加任何计数、钩子或观测接口**:接收侧的实收与丢失,由本工具按 payload
 * 前四字节的自增序号自行算出(**D3**)。
 *
 * 数据 payload 布局(小端):
 * ```
 * [seq:4 LE][filler ...]      总长 = 该档矩阵的 payload 字节数(**≥ 4**)
 * ```
 */

#include <cstddef>
#include <cstdint>
#include <cstring>

#include <QByteArray>

namespace perf {

// ── ProtocolNode 路径的保留 message_id(**D4**)───────────────────────────────

/// 控制命令帧的命令码。数据帧一律用 `kDataMessageId`,二者不撞。
constexpr std::uint16_t kCtrlMessageId = 0xFF00;
/// 控制应答(`kResult`)的命令码——`RequestForResultDirect` 的 `result_message_id`。
constexpr std::uint16_t kCtrlResultMessageId = 0xFF01;
/// 数据帧的命令码。
constexpr std::uint16_t kDataMessageId = 0x0001;
/// 数据交互的结果帧命令码(`RequestForResult` / `RequestForResultDirect` 两模式用)。
constexpr std::uint16_t kDataResultMessageId = 0x0002;

// ── DdsNode 路径的保留服务名 / topic(**D4**)────────────────────────────────

/// 控制用服务名(与数据面分开,故"保留服务名"这一条落在这里)。
constexpr char kCtrlService[] = "perf.ctrl";
/// 数据用服务名(请求-响应模式)。
constexpr char kDataService[] = "perf.svc";
/// 数据用 topic(发布-订阅模式):客户端 → 服务端。
constexpr char kDataTopic[] = "perf.data";
/// 回显 topic(发布-订阅模式):服务端 → 客户端。
constexpr char kEchoTopic[] = "perf.echo";

// ── 控制协议 ─────────────────────────────────────────────────────────────────

/// 控制命令。
enum class CtrlCommand : std::uint8_t {
  kHello = 1,  ///< 握手:确认对端在、且已就位;顺带把本轮配置告诉它。
  kBegin = 2,  ///< 开始一行矩阵:对端据此设定回显模式并**清零**其接收侧计数。
  kEnd = 3,    ///< 结束一行矩阵:对端回传其接收侧统计(实收 / 丢失 / 首末样本间隔)。
  kBye = 4,    ///< 全部跑完:对端收工退出。
};

/// 控制请求的 payload(小端定长,共 16 字节)。
struct CtrlRequest {
  CtrlCommand command = CtrlCommand::kHello;
  std::uint8_t suite = 0;          ///< 0 = latency,1 = throughput。
  std::uint8_t mode = 0;           ///< `Mode` 的序号。
  std::uint8_t reserved = 0;
  std::uint32_t payload_bytes = 0; ///< 该行矩阵的 payload 档位。
  std::uint32_t expected = 0;      ///< 预期条数(仅供对端预留,统计不依赖它)。
  std::uint32_t run_index = 0;     ///< 第几行矩阵,便于排障时对齐两侧日志。
};

/// 控制应答的 payload(小端定长,共 32 字节)。
struct CtrlReply {
  CtrlCommand command = CtrlCommand::kHello;  ///< 原样回带,便于交叉校验。
  std::uint8_t status = 0;                    ///< 0 = ok。
  std::uint16_t reserved = 0;
  std::uint32_t reserved2 = 0;
  std::uint64_t received = 0;      ///< 接收侧实收条数(由序号数出,**D3**)。
  std::uint64_t lost = 0;          ///< 接收侧丢失条数 = 最大序号 + 1 − 实收。
  std::uint64_t rec_time_us = 0;   ///< 接收侧首末样本间隔(微秒)。
};

constexpr int kCtrlRequestBytes = 16;
constexpr int kCtrlReplyBytes = 32;

// ── 小端读写(不依赖任何库内部件)──────────────────────────────────────────

inline void PutU32(char* p, std::uint32_t v) {
  p[0] = static_cast<char>(v & 0xFF);
  p[1] = static_cast<char>((v >> 8) & 0xFF);
  p[2] = static_cast<char>((v >> 16) & 0xFF);
  p[3] = static_cast<char>((v >> 24) & 0xFF);
}

[[nodiscard]] inline std::uint32_t GetU32(const char* p) {
  return static_cast<std::uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<std::uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

inline void PutU64(char* p, std::uint64_t v) {
  PutU32(p, static_cast<std::uint32_t>(v & 0xFFFFFFFFu));
  PutU32(p + 4, static_cast<std::uint32_t>((v >> 32) & 0xFFFFFFFFu));
}

[[nodiscard]] inline std::uint64_t GetU64(const char* p) {
  return static_cast<std::uint64_t>(GetU32(p)) |
         (static_cast<std::uint64_t>(GetU32(p + 4)) << 32);
}

// ── 数据 payload ─────────────────────────────────────────────────────────────

/// @brief 造一条数据 payload:前四字节是自增序号,其余填 `'x'`。
[[nodiscard]] inline QByteArray MakeDataPayload(std::uint32_t seq,
                                                std::size_t bytes) {
  QByteArray payload(static_cast<int>(bytes < 4 ? 4 : bytes), 'x');
  PutU32(payload.data(), seq);
  return payload;
}

/// @brief 取出 payload 内嵌的序号;长度不足四字节时返回 `false`。
[[nodiscard]] inline bool SeqOf(const QByteArray& payload, std::uint32_t* seq) {
  if (payload.size() < 4) {
    return false;
  }
  *seq = GetU32(payload.constData());
  return true;
}

// ── 控制 payload 编解码 ──────────────────────────────────────────────────────

[[nodiscard]] inline QByteArray EncodeCtrlRequest(const CtrlRequest& req) {
  QByteArray out(kCtrlRequestBytes, '\0');
  char* p = out.data();
  p[0] = static_cast<char>(req.command);
  p[1] = static_cast<char>(req.suite);
  p[2] = static_cast<char>(req.mode);
  p[3] = static_cast<char>(req.reserved);
  PutU32(p + 4, req.payload_bytes);
  PutU32(p + 8, req.expected);
  PutU32(p + 12, req.run_index);
  return out;
}

[[nodiscard]] inline bool DecodeCtrlRequest(const QByteArray& bytes,
                                            CtrlRequest* req) {
  if (bytes.size() < kCtrlRequestBytes) {
    return false;
  }
  const char* p = bytes.constData();
  req->command = static_cast<CtrlCommand>(static_cast<unsigned char>(p[0]));
  req->suite = static_cast<std::uint8_t>(p[1]);
  req->mode = static_cast<std::uint8_t>(p[2]);
  req->reserved = static_cast<std::uint8_t>(p[3]);
  req->payload_bytes = GetU32(p + 4);
  req->expected = GetU32(p + 8);
  req->run_index = GetU32(p + 12);
  return true;
}

[[nodiscard]] inline QByteArray EncodeCtrlReply(const CtrlReply& rsp) {
  QByteArray out(kCtrlReplyBytes, '\0');
  char* p = out.data();
  p[0] = static_cast<char>(rsp.command);
  p[1] = static_cast<char>(rsp.status);
  PutU64(p + 8, rsp.received);
  PutU64(p + 16, rsp.lost);
  PutU64(p + 24, rsp.rec_time_us);
  return out;
}

[[nodiscard]] inline bool DecodeCtrlReply(const QByteArray& bytes,
                                          CtrlReply* rsp) {
  if (bytes.size() < kCtrlReplyBytes) {
    return false;
  }
  const char* p = bytes.constData();
  rsp->command = static_cast<CtrlCommand>(static_cast<unsigned char>(p[0]));
  rsp->status = static_cast<std::uint8_t>(p[1]);
  rsp->received = GetU64(p + 8);
  rsp->lost = GetU64(p + 16);
  rsp->rec_time_us = GetU64(p + 24);
  return true;
}

}  // namespace perf
