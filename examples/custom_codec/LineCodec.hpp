#pragma once

/**
 * @file LineCodec.hpp
 * @brief 示例 ⑥ 自带的自定义 `ICodec`:一行文本一帧的**无状态报文式** codec。
 *
 * 线缆格式(**可读**,故示例能把字节直接打给你看):
 *
 * ```
 * TXT|<frm_type>|<protocol_id>|<session_id>|<message_id>|<payload>\n
 *  └4┘                                                   └ payload 从这里开始
 * ```
 *
 * ## 为什么它是**无状态**的
 *
 * 本 codec 要装在 **UDP** 上,而**报文式介质的 codec 不得跨报文保留状态**(README
 * 「扩展:自定义 codec」纪律 3):多对端场景下,上一个对端的残留会污染下一个对端的解码。
 * 故 `Decode` 只解**本报文内**的整行,末尾那截没有 `\n` 的残留**直接丢弃**,不留缓冲。
 *
 * 要把同一套格式装到 **TCP / 串口**(字节流)上,只需加一个成员滚动缓冲:把本次喂入的
 * 字节追加进去、扫出整行、**残留留在缓冲里**等下次。那时它就是有状态流式 codec
 * ——**两者不可互换**,装错介质是 README 明说的错误用法。
 *
 * ## ★ 最容易写错的一条:`frame` 必填 + 视图的建立**顺序**(ADR-0020 **D2**)
 *
 * 每条切出来的 `Message` 都**必须**填 `frame`(该帧的完整字节),并把 `payload` 建成
 * **指进 `frame`** 的视图。**顺序是硬要求**:
 *
 * ```cpp
 * // ✔ 对:先让 frame 落到它【最终的】那个 QByteArray 上,再在【它】上面建视图
 * msg.frame   = QByteArray(begin, frame_len);
 * msg.payload = QByteArray::fromRawData(msg.frame.constData() + offset, payload_len);
 *
 * // ✘ 错:视图建在局部变量上,再把 frame 拷进 Message —— 视图指向局部变量的数据块。
 * //    **这是静默的内存错误,功能用例照样会绿。**
 * QByteArray frame(begin, frame_len);
 * msg.payload = QByteArray::fromRawData(frame.constData() + offset, payload_len);
 * msg.frame   = frame;                     // ← 拷贝共享数据块,但局部 frame 一析构……
 * ```
 *
 * @note 把解好的 `Message` `push_back` 进 `std::vector` 是**安全**的:`QByteArray` 的数据
 *       块在堆上,拷贝只是引用计数加一、移动只转移 d 指针,**块地址都不变**,故扩容、
 *       入队出队、`Dispatcher` 发副本之后视图仍然有效(见 `Message::payload` 的说明)。
 */

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include <QByteArray>

#include "transport/codec/ICodec.hpp"
#include "transport/core/Error.hpp"
#include "transport/core/Message.hpp"

namespace example {

/// @brief 一行文本一帧的自定义 codec —— **无状态**,适配 UDP(见文件头)。
class LineCodec final : public transport::ICodec {
 public:
  /// 帧头标签,兼作重同步的锚点。
  static constexpr char kTag[] = "TXT|";
  static constexpr std::size_t kTagLen = 4;

  /// @brief 一条 `Message` → 一帧线缆字节。
  ///
  /// **完全忽略 `msg.frame`**(ADR-0020 **D7**):出站帧只由当前各字段生成,
  /// **没有"`frame` 非空则原样重发"这回事**。
  Coro::Result<std::vector<std::uint8_t>> Encode(
      const transport::Message& msg) override {
    // payload 里出现 '\n' 本格式就承载不了 —— 这是**本 codec 的**格式限制,报 kCodec。
    if (msg.payload.contains('\n')) {
      return transport::make_error_code(transport::TransportErrc::kCodec);
    }
    std::string line;
    line.reserve(kTagLen + 24 + static_cast<std::size_t>(msg.payload.size()));
    line += kTag;
    line += std::to_string(static_cast<unsigned>(msg.frm_type));
    line += '|';
    line += std::to_string(static_cast<unsigned>(msg.protocol_id));
    line += '|';
    line += std::to_string(static_cast<unsigned>(msg.session_id));
    line += '|';
    line += std::to_string(static_cast<unsigned>(msg.message_id));
    line += '|';
    line.append(msg.payload.constData(), static_cast<std::size_t>(msg.payload.size()));
    line += '\n';
    // 两层用不同的字节容器:`Message` 用 QByteArray,`Datagram` 用 std::vector<uint8_t>,
    // 故 `Encode` 的返回类型与 `Datagram::bytes` 对齐。转换只发生在 codec 这一层。
    return std::vector<std::uint8_t>(line.begin(), line.end());
  }

  /// @brief 一段收到的字节 → 0..N 条完整 `Message`。
  ///
  /// 纪律:
  ///  1. **扫不出完整帧时返回空成功,不是错误**(本 codec 上表现为"这条报文里没有整行")。
  ///  2. **返回错误意味着"这段字节坏了"**,节点会静默丢弃并继续读;重同步由 codec 自己负责
  ///     ——本 codec 的重同步就是**跳到下一个 `\n` 之后**继续扫。
  ///  3. **不得跨报文保留状态**:残留直接丢,不进任何成员。
  Coro::Result<std::vector<transport::Message>> Decode(const std::uint8_t* data,
                                                       std::size_t len) override {
    std::vector<transport::Message> out;
    std::size_t begin = 0;
    while (begin < len) {
      const void* found = std::memchr(data + begin, '\n', len - begin);
      if (found == nullptr) {
        break;  // ★ 纪律 1:半行 → 空成功。★ 纪律 3:残留**丢掉**,不留跨报文状态。
      }
      const auto newline = static_cast<std::size_t>(
          static_cast<const std::uint8_t*>(found) - data);
      const std::size_t frame_len = newline - begin + 1;  // 含末尾的 '\n'

      transport::Message msg;
      std::size_t payload_offset = 0;
      if (ParseHeader(data + begin, frame_len, &msg, &payload_offset)) {
        // ★★★ 顺序是硬要求(ADR-0020 D2):
        //   ① 先把整帧落到 `msg.frame` 这个【最终的】QByteArray 上;
        msg.frame = QByteArray(reinterpret_cast<const char*>(data + begin),
                               static_cast<int>(frame_len));
        //   ② 再在【msg.frame】上面建 payload 视图。**绝不能**先在局部变量上建。
        msg.payload = QByteArray::fromRawData(
            msg.frame.constData() + payload_offset,
            static_cast<int>(frame_len - payload_offset - 1));  // -1:去掉末尾 '\n'
        out.push_back(std::move(msg));  // 移动不改数据块地址,视图照样有效
      }
      // 头坏了就跳过这一行接着扫(重同步);本报文内的其余整行照常解出来。
      begin = newline + 1;
    }
    return out;
  }

 private:
  /// 解头部并给出 payload 在本帧内的偏移;格式不符返回 false(由调用方跳过该行)。
  static bool ParseHeader(const std::uint8_t* frame, std::size_t frame_len,
                          transport::Message* msg, std::size_t* payload_offset) {
    if (frame_len <= kTagLen || std::memcmp(frame, kTag, kTagLen) != 0) {
      return false;
    }
    std::size_t pos = kTagLen;
    unsigned fields[4] = {0, 0, 0, 0};
    for (unsigned& field : fields) {
      if (!ReadUnsignedUntilBar(frame, frame_len, &pos, &field)) {
        return false;
      }
    }
    if (fields[0] > static_cast<unsigned>(transport::FrameType::kHeartbeat) ||
        fields[1] > 0xFF || fields[2] > 0xFF || fields[3] > 0xFFFF) {
      return false;  // 字段越界:codec 语义损坏
    }
    msg->frm_type = static_cast<transport::FrameType>(fields[0]);
    msg->protocol_id = static_cast<std::uint8_t>(fields[1]);
    msg->session_id = static_cast<std::uint8_t>(fields[2]);
    msg->message_id = static_cast<std::uint16_t>(fields[3]);
    *payload_offset = pos;
    return true;
  }

  /// 读一个十进制无符号数,须以 `|` 收尾;`pos` 停在 `|` 之后。
  static bool ReadUnsignedUntilBar(const std::uint8_t* frame, std::size_t frame_len,
                                   std::size_t* pos, unsigned* value) {
    unsigned acc = 0;
    std::size_t digits = 0;
    while (*pos < frame_len && frame[*pos] >= '0' && frame[*pos] <= '9') {
      acc = acc * 10 + static_cast<unsigned>(frame[*pos] - '0');
      if (++digits > 9) {
        return false;  // 明显越界,别让它溢出
      }
      ++(*pos);
    }
    if (digits == 0 || *pos >= frame_len || frame[*pos] != '|') {
      return false;
    }
    ++(*pos);  // 跳过 '|'
    *value = acc;
    return true;
  }
};

}  // namespace example
