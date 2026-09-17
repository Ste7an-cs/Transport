/**
 * @file examples/downstream_qmake/main.cpp
 * @brief ADR-0022 **D5** —— 最小的 qmake 下游样例:建一个节点、启动、关闭。
 *
 * 这个程序本身不做收发,它的职责只有一个:**持续验证根上的 `Transport.pri`**。
 *
 * `Transport.pri` 是给下游用的,**本仓库自己的构建不会经过它**——内部走
 * `qmake/lib/lib.pro` 编出的 `libtransport.a`(ADR-0022 D3)。没有这个样例,
 * `Transport.pri` 一旦写错(漏一个 `.cpp`、`INCLUDEPATH` 少一条、AsyncTask 的
 * include 顺序放错)**不会有任何信号**。本仓库反复吃过这类亏(README 的
 * `TopicKey` 曾编译不过、服务端示例曾缺 `endpoint`,都是"没人真的编过")。
 *
 * 与 `examples/` 下另外六个示例的区别:**那六个链 `libtransport.a`**(见
 * `qmake/examples/example.pri`),本例**不链**——它走的是源码级并入这条下游路径,
 * 链接命令里**不应出现 `-ltransport`**。
 *
 * 怎么构建(它挂在与六个示例同一个开关下):
 * @code
 *   mkdir build-qmake && cd build-qmake
 *   qmake CONFIG+=examples ../transport.pro && make -j$(nproc)
 *   ./examples/downstream_qmake/transport_downstream_qmake
 * @endcode
 *
 * 下游工程真正要抄的只有 `downstream_qmake.pro` 里那一行 `include(...)`。
 */

#include <memory>

#include <QCoreApplication>

#include "detail/asyncdefine.h"     // Coro::msleep
#include "task/fiberapplication.h"  // Coro::installFiberApplication / exec / quit
#include "task/fibertask.h"         // Coro::makeTask

#include "transport/codec/SystemDatagramCodec.hpp"
#include "transport/core/version.hpp"
#include "transport/io/udp/UdpTransport.hpp"
#include "transport/node/ProtocolNode.hpp"

namespace {

/// 建一个 UDP 上的 `ProtocolNode`,启动,再按顺序收尾。返回进程退出码。
int RunOnce() {
  transport::UdpConfig cfg;
  cfg.mode = transport::UdpMode::kUnicast;
  cfg.local_addr = "127.0.0.1";
  cfg.local_port = 0;  // 0 = 由 OS 分配临时端口,避免与任何东西撞车

  transport::UdpTransport udp(cfg);
  if (auto started = udp.Start(); !started) {
    qWarning("UdpTransport::Start 失败");
    return 1;
  }

  transport::ProtocolNodeConfig ncfg;
  ncfg.protocol_id = 0x01;
  // UDP 是报文介质 ⇒ 配**无状态报文式** codec(装有状态的 SystemCodec 是错的)。
  transport::ProtocolNode node(udp, std::make_unique<transport::SystemDatagramCodec>(), ncfg);
  if (auto started = node.Start(); !started) {
    qWarning("ProtocolNode::Start 失败");
    udp.Close();
    udp.WaitClosed();
    return 1;
  }

  qInfo("transport %s:节点已在 127.0.0.1:%u 上跑起来了",
        transport::LibraryVersion().c_str(), static_cast<unsigned>(udp.LocalPort()));

  // 收尾顺序:先节点后传输,各自 Close() → WaitClosed()。
  node.Close();
  node.WaitClosed();
  udp.Close();
  udp.WaitClosed();

  qInfo("收尾完成 —— Transport.pri 这条下游路径可用");
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  // fiber 运行时装配(README「运行时:一切都在 fiber 里跑」)。
  QCoreApplication app(argc, argv);
  Coro::installFiberApplication();

  int rc = 0;
  auto task = Coro::makeTask([&] {
    rc = RunOnce();
    Coro::quit();
  });
  Coro::exec();
  (void)task;
  return rc;
}
