/**
 * @file examples/custom_codec/main.cpp
 * @brief 示例 ⑥:自己实现一个 `ICodec`(`LineCodec.hpp`),装到节点上**端到端跑通**。
 *
 * **本例只要一个终端**:一个进程里起两个 `ProtocolNode`,各借一条真实的 UDP 回环传输
 * ——codec 是主角,不必再为对端开第二个进程。
 *
 * 本例覆盖的易错点:
 *  - **`Decode` 必须填 `frame`,并把 `payload` 建成指进 `frame` 的视图**(ADR-0020 D2),
 *    且**视图必须建在 `msg.frame` 这个最终的 `QByteArray` 上**;先在局部变量上建再拷进
 *    `Message` 是**静默的内存错误**。本例把"payload 到底指在哪儿"**当场算出来打印**,
 *    让这条不可见的纪律变成可见的事实。
 *  - **扫不出完整帧时返回空成功,不是错误**;**返回错误意味着这段字节坏了**,节点静默
 *    丢弃并继续读,重同步由 codec 自己负责。本例把这两条各跑一次给你看。
 *  - **codec 与介质匹配**:本 codec 是**无状态报文式**的,故可装 UDP;要装 TCP / 串口
 *    须自行加滚动缓冲(见 `LineCodec.hpp` 文件头)。
 *  - **`Encode` 完全忽略 `msg.frame`**(ADR-0020 D7):转发一条收到的 `Message` 时
 *    `frame` 非空但不参与编码,出站帧由当前各字段重新生成。
 *  - 装配时 `std::make_unique<LineCodec>()` 传给节点构造函数,**节点取得所有权**。
 */

#include <chrono>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <QByteArray>
#include <QCoreApplication>

#include "detail/asyncdefine.h"     // Coro::msleep
#include "task/fiberapplication.h"  // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"         // Coro::makeTask

#include "transport/core/Endpoint.hpp"
#include "transport/core/Message.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

#include "ExampleCommon.hpp"
#include "LineCodec.hpp"

using namespace std::chrono_literals;
using example::Err;
using example::Hex16;
using example::LineCodec;
using example::Log;
using example::Pay;
using example::Printable;
using example::Text;
using transport::AnyOfType;
using transport::FrameType;
using transport::Message;
using transport::ProtocolNode;
using transport::ProtocolNodeConfig;
using transport::RetryPolicy;
using transport::UdpConfig;
using transport::UdpMode;
using transport::UdpTransport;

namespace {

constexpr std::uint16_t kMidEcho = 0x0010;

/// ★ **把"视图建对了没有"变成可见的事实**:payload 的数据指针必须落在 frame 的数据块里。
///
/// 顺序写反(先在局部变量上建视图、再把 frame 拷进 `Message`)时,这里算出来的偏移就是
/// 一个毫无意义的巨大数字,甚至直接崩 —— 而**功能上收发照样是通的**,这正是它可怕的地方。
std::string ViewReport(const Message& msg) {
  if (msg.payload.isEmpty()) {
    return "payload 为空(空 payload 是边界:fromRawData(p, 0) 的块不在 frame 内)";
  }
  const char* frame_begin = msg.frame.constData();
  const char* frame_end = frame_begin + msg.frame.size();
  const char* view_begin = msg.payload.constData();
  const char* view_end = view_begin + msg.payload.size();
  const bool inside = view_begin >= frame_begin && view_end <= frame_end;
  if (!inside) {
    return "✘ payload **不在** frame 内 —— 视图建立顺序写反了(静默的内存错误)";
  }
  return "✔ payload 指进 frame:偏移 " +
         std::to_string(static_cast<long long>(view_begin - frame_begin)) + " / 共 " +
         std::to_string(msg.frame.size()) + " 字节,长度 " +
         std::to_string(msg.payload.size()) + " 字节(零拷贝)";
}

/// —— 第一段:不接传输,直接把 codec 单独拿来跑,把格式与两条纪律打给你看 ——
void ShowCodecItself() {
  LineCodec codec;

  Log("—— ① 线缆格式 ——————————————————————————————————————————————");
  Message sample;
  sample.frm_type = FrameType::kCommand;
  sample.protocol_id = 0x09;
  sample.session_id = 7;
  sample.message_id = kMidEcho;
  sample.payload = Pay("hello");
  auto encoded = codec.Encode(sample);
  if (!encoded) {
    Log("Encode 失败:" + Err(encoded.error()));
    return;
  }
  const QByteArray wire(reinterpret_cast<const char*>(encoded.value().data()),
                        static_cast<int>(encoded.value().size()));
  Log("   Encode 出来的字节(" + std::to_string(wire.size()) + "):\"" +
      Printable(wire) + "\"");
  Log("   十六进制:" + example::Hex(wire));

  Log("\n—— ② Decode:切出整帧,并验证 payload 视图指进 frame ——————————");
  auto decoded = codec.Decode(encoded.value().data(), encoded.value().size());
  if (decoded && decoded.value().size() == 1) {
    const Message& m = decoded.value().front();
    Log("   frm_type=" + std::to_string(static_cast<unsigned>(m.frm_type)) +
        " protocol_id=" + std::to_string(m.protocol_id) +
        " session_id=" + std::to_string(m.session_id) +
        " message_id=" + Hex16(m.message_id) +
        " payload=\"" + Printable(m.payload) + "\"");
    Log("   " + ViewReport(m));
  } else {
    Log("   Decode 没切出恰一帧");
  }

  Log("\n—— ③ 纪律:扫不出完整帧 → **空成功**,不是错误 ——————————————");
  const std::string half = "TXT|1|9|7|16|no-newline-yet";  // 没有 '\n'
  auto partial = codec.Decode(reinterpret_cast<const std::uint8_t*>(half.data()),
                              half.size());
  Log(partial ? "   半帧 → 成功,切出 " + std::to_string(partial.value().size()) +
                    " 条(报文式 codec 把残留**丢掉**,不留跨报文状态)"
              : "   半帧 → 错误(**不该**):" + Err(partial.error()));

  Log("\n—— ④ 纪律:坏帧由 codec 自己重同步,本报文内的好帧照样解出来 ——");
  const std::string mixed = "garbage-line\nTXT|1|9|7|16|good\n";
  auto resynced = codec.Decode(reinterpret_cast<const std::uint8_t*>(mixed.data()),
                               mixed.size());
  if (resynced) {
    Log("   一条坏行 + 一条好行 → 切出 " + std::to_string(resynced.value().size()) +
        " 条");
    for (const Message& m : resynced.value()) {
      Log("   payload=\"" + Printable(m.payload) + "\"," + ViewReport(m));
    }
  }
  Log("");
}

/// —— 第二段:把这个 codec 装到两个真节点上,走**真实 UDP 回环**端到端跑一次 ——
void RunEndToEnd() {
  Log("—— ⑤ 端到端:两个 ProtocolNode + 两条真实 UDP 回环传输,codec 换成 LineCodec ——\n");

  // 服务端:绑一个由 OS 分配的临时端口,**不配默认对端**(应答只能靠 endpoint 回)。
  UdpConfig server_cfg;
  server_cfg.mode = UdpMode::kUnicast;
  server_cfg.local_addr = "127.0.0.1";
  server_cfg.local_port = 0;
  server_cfg.silence_timeout = 5000ms;
  UdpTransport server_io(server_cfg);
  if (auto started = server_io.Start(); !started) {
    Log("服务端 UDP 启动失败:" + Err(started.error()));
    return;
  }

  ProtocolNodeConfig ncfg;
  ncfg.protocol_id = 0x09;
  // ★ 装配:`std::make_unique<LineCodec>()` 传给节点构造函数,**节点取得所有权**。
  ProtocolNode server(server_io, std::make_unique<LineCodec>(), ncfg);
  if (auto started = server.Start(); !started) {
    Log("服务端节点启动失败:" + Err(started.error()));
    server_io.Close();
    server_io.WaitClosed();
    return;
  }

  // 客户端:默认对端就是服务端,故请求不必填 endpoint(一对一照旧)。
  UdpConfig client_cfg = server_cfg;
  client_cfg.remote_addr = "127.0.0.1";
  client_cfg.remote_port = server_io.LocalPort();
  UdpTransport client_io(client_cfg);
  if (auto started = client_io.Start(); !started) {
    Log("客户端 UDP 启动失败:" + Err(started.error()));
    server.Close();
    server.WaitClosed();
    server_io.Close();
    server_io.WaitClosed();
    return;
  }
  ProtocolNode client(client_io, std::make_unique<LineCodec>(), ncfg);
  if (auto started = client.Start(); !started) {
    Log("客户端节点启动失败:" + Err(started.error()));
    client_io.Close();
    client_io.WaitClosed();
    server.Close();
    server.WaitClosed();
    server_io.Close();
    server_io.WaitClosed();
    return;
  }
  Log("[装配] 客户端 :" + std::to_string(client_io.LocalPort()) + " → 服务端 :" +
      std::to_string(server_io.LocalPort()) + ",两端都用 LineCodec");

  // —— 服务端:订阅 + 在**自己的 fiber** 里回应答(三项必须回带)——————
  auto sub = server.Subscribe(AnyOfType(FrameType::kCommand));
  if (!sub) {
    Log("订阅失败:" + Err(sub.error()));
  } else {
    auto ticket = std::move(sub).value();
    auto serving = Coro::makeTask([&] {
      auto req = ticket.Wait(5000ms);
      if (!req) {
        Log("[服务端] 没等到请求:" + Err(req.error()));
        return;
      }
      Log("[服务端] 收到整帧:\"" + Printable(req.value().frame) + "\"");
      Log("[服务端] " + ViewReport(req.value()));
      Message rsp;
      rsp.frm_type = FrameType::kResponse;
      rsp.session_id = req.value().session_id;  // ★ ADR-0019
      rsp.message_id = req.value().message_id;  // ★
      rsp.endpoint = req.value().endpoint;      // ★ ADR-0021:回到请求方
      // 作用域内直接用视图(零拷贝);要活得比 req 久才需要 OwnedPayload()。
      rsp.payload = Pay("echo:" + Text(req.value().payload));
      auto sent = server.Send(std::move(rsp));
      if (!sent) {
        Log("[服务端] 应答入队失败:" + Err(sent.error()));
      }
    });

    Message req;
    req.message_id = kMidEcho;
    req.payload = Pay("custom-codec-works");
    auto rsp = client.RequestForResponse(std::move(req), RetryPolicy{2000ms, 3});
    (void)serving.get();  // ★ 宿主自己 join 自己的 fiber

    if (rsp) {
      Log("[客户端] 收到整帧:\"" + Printable(rsp.value().frame) + "\"");
      Log("[客户端] " + ViewReport(rsp.value()));
      Log("[客户端] payload=\"" + Printable(rsp.value().payload) + "\"");
    } else {
      Log("[客户端] 失败:" + Err(rsp.error()));
    }
  }

  // —— 收尾:两套各自先节点、后传输 ————————————————————————————
  Log("\n[收尾] 两端都是 node.Close() → node.WaitClosed() → transport.Close() → transport.WaitClosed()");
  client.Close();
  client.WaitClosed();
  client_io.Close();
  client_io.WaitClosed();
  server.Close();
  server.WaitClosed();
  server_io.Close();
  server_io.WaitClosed();
  Log("[收尾] 完成");
}

}  // namespace

int main(int argc, char** argv) {
  example::PrintBanner(
      "示例 ⑥ custom_codec —— 自己实现一个 ICodec 并装到节点上端到端跑通",
      {"演示:LineCodec(一行文本一帧,无状态报文式)的 Encode / Decode;",
       "      **frame 必填 + payload 视图必须建在 msg.frame 这个最终的 QByteArray 上**",
       "      (ADR-0020 D2),本例把 payload 到底指在 frame 的哪个偏移**当场算出来打印**;",
       "      再把它装到两个 ProtocolNode 上,走真实 UDP 回环端到端跑一次请求-响应。",
       "",
       "怎么跑(**只要一个终端**,两个节点都在本进程里):",
       "  ./transport_example_custom_codec",
       "",
       "codec 源码在 examples/custom_codec/LineCodec.hpp,注释里写清了装 TCP 该怎么改。"});

  // ── fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)──────────────
  QCoreApplication app(argc, argv);  // ① Qt 事件循环
  Coro::installFiberApplication();   // ② 装 fiber 调度器
  auto task = Coro::makeTask([&] {   // ③ 业务代码写在 fiber 里
    ShowCodecItself();               //    这一段其实不需要 fiber,放进来只为版面统一
    RunEndToEnd();                   //    这一段**必须**在 fiber 内:有等待语义
    Coro::quit();                    // ④ 干完让 exec() 返回
  });
  Coro::exec();                      // ⑤ 跑起来
  (void)task;
  return 0;
}
