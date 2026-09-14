#pragma once

// -----------------------------------------------------------------------------
// ICodec.hpp — 线缆格式扩展点(分帧 + 序列化 + 承载交互元数据)
// Encode: 一条 Message → 一段线缆字节(一对一)。
// Decode: 喂入字节切片 → 切出 0..N 条完整 Message。
// 实现可有状态(内部维护滚动缓冲,单线程喂,如 SystemCodec),
// 或无状态且支持并发 Decode(如 DdsCodec,被多个 DDS 监听线程并发喂)。
// 不依赖 Transport。
// -----------------------------------------------------------------------------

#include "detail/result.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "transport/core/Message.hpp"
#include "transport/core/Error.hpp"

namespace transport {

class ICodec {
 public:
  virtual ~ICodec() = default;

  /// @brief 发:一条消息 → 一段线缆字节。
  ///
  /// **完全忽略 `msg.frame`**(ADR-0020 **D7**):出站帧只由当前各字段生成。转发一条收到的
  /// `Message` 时 `frame` 非空,但它不参与编码——**没有"`frame` 非空则原样重发"这回事**,
  /// 那会让同一个发送路径有两种行为,且与"改了字段却不生效"的直觉冲突。
  virtual Coro::Result<std::vector<uint8_t>> Encode(const Message& msg) = 0;

  /**
   * @brief 收:喂入字节切片 → 切出 0..N 条完整消息(半包返回空、粘包返回多条)。
   *
   * 解析错误按类别报 TransportErrc:分帧/长度/最大帧长 → `kFrame`;
   * codec 语义(坏判别符/字段越界)→ `kCodec`。
   *
   * ## ⚠ 实现纪律:`frame` **必填**,`payload` 是它的视图(ADR-0020 **D2**)
   *
   * 每条切出来的 `Message` 都**必须**填 `frame`(该帧的完整字节:帧头 → payload 末),
   * 并把 `payload` 建成**指进 `frame`** 的视图。**这是必填不是选填**——选填会让"接收方
   * 能拿到原始帧"这条承诺时有时无,调用方无法依赖它,比不做更坏。
   *
   * ```cpp
   * msg.frame   = QByteArray(reinterpret_cast<const char*>(frame_begin), frame_len);
   * msg.payload = QByteArray::fromRawData(msg.frame.constData() + payload_offset,
   *                                       payload_len);
   * ```
   *
   * @warning **顺序是硬要求**:`payload` 必须在 `frame` **落到它最终的那个 `QByteArray`
   *          之后**才建立。若先在局部变量上建视图、再把 frame 拷进 `Message`,视图会指向
   *          局部变量的数据块——**这是静默的内存错误,功能用例照样会绿**。
   */
  virtual Coro::Result<std::vector<Message>> Decode(const uint8_t* data,
                                                    std::size_t len) = 0;
};

}  // namespace transport
