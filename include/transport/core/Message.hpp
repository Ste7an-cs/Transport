#pragma once

// -----------------------------------------------------------------------------
// Message.hpp — 一条消息的数据模型(贯穿三层的载体)
//
// 一条 Message 既携带 payload(应用字节),又携带【交互元数据】。元数据分两套,
// 互不干扰、按路径取用:
//   · 通用交互元数据(kind/correlation_id/reply_to):DDS 路径用,由 DdsCodec 上线缆、
//     DdsPolicy 解读。
//   · 外部协议字段(frm_type/protocol_id/session_id/message_id):外部协议路径用,由
//     SystemCodec 上线缆、ProtocolPolicy 解读。
// 引擎只在边界传递 Message,从不解读这些字段——解读全在对应的 policy/codec 里。
// endpoint 由引擎在收到时按来源(ip:port / topic)填,发送时由调用方填目的地。
// -----------------------------------------------------------------------------

#include <cstdint>
#include <string>

#include <QByteArray>  // ADR-0020 D1:frame/payload 的载体(隐式共享 + fromRawData)。

#include "transport/core/Endpoint.hpp"

namespace transport {

// DDS 路径的交互种类(DdsPolicy 把它当判别符 FrameTag;DdsCodec 上线缆首字节)。
enum class MessageKind {
  kOneway,    // 单向(无需应答)
  kRequest,   // 请求(期待应答/反馈)
  kReply,     // 终结应答(请求-应答的应答,或请求-结果反馈的最终结果)
  kFeedback,  // 中间结果反馈(可多次,非终结)
  kNotify,    // 订阅通知(主动推送/发布)
};

// 外部协议帧类型(ProtocolPolicy 把它当判别符;SystemCodec 上线缆 frm_type 字节)。
// ⚠️ 这里是【占位值】:真实对接外部系统时,改成外部协议规定的真实字节值。
enum class FrameType : uint8_t {
  kUnknown   = 0,
  kCommand   = 1,  // 命令(请求)
  kResponse  = 2,  // 即时回应(中间或终结)
  kResult    = 3,  // 最终结果(终结)
  kState     = 4,  // 状态(周期 STATE)
  kHeartbeat = 5,  // 心跳
};

/**
 * @brief 一条逻辑消息:应用字节 + 交互元数据 + 一端地址(ADR-0020)。
 *
 * ## 三个字段的所有权形状(**D1/D2/D3**)
 *
 * | 字段 | 发送(出站) | 接收(入站) |
 * |---|---|---|
 * | `frame` | **空**,`Encode` 完全忽略(**D7**) | **完整一帧**(帧头 → payload 末),**拥有** |
 * | `payload` | 用户数据,**拥有** | **指进 `frame` 的视图**,**不拥有** |
 * | `endpoint` | **目的地** | **来源** |
 *
 * `endpoint` 与传输层 `Datagram::peer` 逐字同义(ADR-0008 D8):同一个字段,写时是目的地、
 * 读时是来源。
 */
struct Message {
  /**
   * @brief 接收时:线缆上那一整帧的**拥有型**副本(帧头 → payload 末,含 CRC 等全部细节)。
   *
   * 供原样透传转发、按原字节重发与排障(框架无观测面,ADR-0014)。**五个内置 codec 的
   * `Decode` 一律必填**(**D2**;自定义 codec 同此纪律)。
   *
   * **发送时为空,`Encode` 完全忽略它**(**D7**):转发一条收到的 `Message` 时 `frame` 非空
   * 但不参与编码,出站帧由当前各字段重新生成。**没有"`frame` 非空则原样重发"这回事。**
   */
  QByteArray frame;

  /**
   * @brief 应用字节(框架不解读其语义)。
   *
   * **发送时是拥有型**的用户数据;**接收时是指进 `frame` 的视图**(`QByteArray::fromRawData`,
   * 零拷贝,**D2**)。
   *
   * @warning **视图只在其所属 `Message` 存活、且 `frame` 未被改写期间有效。悬垂是静默的
   *          内存错误,框架不校验、也无从校验。** 以下都会让它失效且不会报错:
   *          - `frame.clear()`,或对 `frame` 写入(触发 COW 分离、原块可能被释放);
   *          - 把 `payload` 拷出来单独保存,而让 `Message` 先析构;
   *          - 跨线程 / 跨 fiber 只传 `payload` 而不带上 `Message`。
   *
   *          **拷贝与移动 `Message` 是安全的**:`QByteArray` 的数据块在堆上,拷贝只是引用
   *          计数加一、移动只转移 d 指针,**块地址都不变**,故 `Dispatcher` 给每个订阅者各发
   *          一份副本、`std::vector<Message>` 扩容、入队出队之后,视图仍指向同一块仍被引用
   *          的内存。
   *
   *          需要让 payload 活得比本 `Message` 久时,用 `OwnedPayload()`。
   *
   * @note 对 `payload` **写入**是安全的——`fromRawData` 出来的 `QByteArray` 在非 const 访问
   *       时会自行深拷贝转为拥有型。危险只在读侧。
   */
  QByteArray payload;

  /**
   * @brief 本消息的一端地址:**发送时是目的地,接收时是来源**(**D3**)。
   *
   * 与 `Datagram::peer` 同一套语义。`ProtocolNode` 入站填 `datagram.peer`(UDP 即发送方
   * `ip:port`),`DdsNode` 入站填 `Endpoint::Topic(来源 topic)`;出站由调用方填,
   * `DdsNode::Publish` 要 `kTopic`、`RequestForResultDirect` 要 `kService`(**D6**)。
   */
  Endpoint endpoint;

  int64_t timestamp = 0;                        // 预留(本库未用)
  // ---- 通用交互元数据(DDS 路径)----
  MessageKind kind = MessageKind::kOneway;      // 交互种类(DdsPolicy 判别符)
  std::string correlation_id;                   // 配对请求↔应答(DdsPolicy 匹配键);非请求为空
  std::string reply_to;                         // 应答回送目的 topic(DDS 多路 req-resp);否则空
  // ---- 外部协议字段(SystemCodec / ProtocolNode 路径)----
  FrameType frm_type    = FrameType::kUnknown;  // 帧类型(ProtocolPolicy 判别符)
  uint8_t   protocol_id = 0;                    // 外部系统 id
  uint8_t   session_id  = 0;                    // 会话 id(滚动 0–255,匹配键一半)
  uint16_t  message_id  = 0;                    // 命令码(匹配键另一半)

  /**
   * @brief 返回 `payload` 的【拥有型】深拷贝——**唯一的逃生口**(**D8**)。
   *
   * **判据只有一句:只在需要让 payload 活得比本 `Message` 久时才调**——**不是"不确定就调"**。
   *
   * | 场景 | 用法 |
   * |---|---|
   * | 在 `Message` 存活的作用域内用完 | **直接用 `payload`**,零拷贝 |
   * | 存进容器 / 成员变量,待会儿再处理 | `OwnedPayload()` |
   * | 交给别的线程或 fiber | `OwnedPayload()` |
   * | 从函数里把 payload 返回出去 | `OwnedPayload()` |
   *
   * @warning 若调用方"保险起见"处处调本方法,零拷贝收益**全部消失**,而帧头与 CRC 的额外
   *          拷贝还在——那比不做本 ADR 更差。
   *
   * @note **恒为深拷贝,不做"已是拥有型就直接返回"的优化**:Qt5 没有公开 API 可判别一个
   *       `QByteArray` 是不是 `fromRawData` 出来的,而拿 `frame.isEmpty()` 当判据则依赖一条
   *       可被违反的不变量(调用方自行构造的 `Message` 可以两者都填)。正确性优先——本方法
   *       只在调用方显式索要所有权时才被调用,那一次拷贝是它自己要的。
   */
  [[nodiscard]] QByteArray OwnedPayload() const {
    return QByteArray(payload.constData(), payload.size());
  }
};

}  // namespace transport
