# ---------------------------------------------------------------------------
# Transport.pri —— qmake 下游的**一行接入**(ADR-0022 D1)。
#
# 形状照 third_party/AsyncTask/AsyncTask.pri:把 SOURCES / INCLUDEPATH / QT /
# DEFINES / LIBS **直接并进消费者的 target**(源码级并入),不建中间库、不需要
# 构建顺序、不需要 PRE_TARGETDEPS —— qmake 没有 CMake 那套 target 依赖传递。
#
# 下游用法(只此一行):
#   include($$PWD/path/to/transport/Transport.pri)
#
# 开关(ADR-0022 D4,对下游照常可用):
#   qmake CONFIG+=no_fastdds ...          # 强制不编 FastDdsProvider
#   qmake FASTDDS_ROOT=/opt/fastdds ...   # 换 Fast DDS 探测前缀(默认 /usr/local)
#
# 两条路径(ADR-0022 D3):
#   * 仓库内部 —— qmake/lib/lib.pro 用本文件编出 libtransport.a,tests / perf /
#     examples 链它,多个可执行共用一次编译结果;
#   * 下游     —— 直接 include 本文件,源码并入自己的 target。
#   两条路径的**源清单是同一份**(ADR-0022 D2),不会漂移。
#
# 明确接受的代价(ADR-0022):① 下游每个 target 都重编一遍库的源码;② src/ 会进
# 下游的 INCLUDEPATH(FastDdsProvider.hpp 等私有头在 src/ 下),故公共/私有的边界
# 在 qmake 这条路径上**只剩约定**;③ CMake 与 qmake 仍是两份源清单。
#
# 注:qmake 不支持在 `\` 续行的赋值中间插注释(会把后半截一并注掉),故源清单按
# 注释切成数段 `SOURCES +=`,内容与顺序仍与 CMakeLists.txt 逐字对应。
#
# ⚠ 本文件内的 include 顺序是硬要求:AsyncTask.pri 内部有 contains(QT, core)
#   判断,故必须**在设好 QT 之后**才 include 它(见文件末尾)。
# ---------------------------------------------------------------------------

# 本文件位于仓库根,故 $$PWD 即仓库根。qmake 为每个被 include 的文件各自设 PWD,
# 下游 include 时它仍指向本仓库,不会跟着消费者的工程目录跑。
TRANSPORT_PRI_ROOT = $$clean_path($$PWD)

# 库要求 C++17(对应 CMakeLists.txt 的 target_compile_features(transport PUBLIC
# cxx_std_17));下游不必自己知道这件事。
CONFIG += c++17

QT += core network serialport
QT -= gui

INCLUDEPATH += $$TRANSPORT_PRI_ROOT/include $$TRANSPORT_PRI_ROOT/src

SOURCES += \
    $$TRANSPORT_PRI_ROOT/src/core/version.cpp \
    $$TRANSPORT_PRI_ROOT/src/core/Error.cpp \
    $$TRANSPORT_PRI_ROOT/src/node/NodeBase.cpp \
    $$TRANSPORT_PRI_ROOT/src/node/ProtocolNode.cpp \
    $$TRANSPORT_PRI_ROOT/src/io/udp/UdpTransport.cpp \
    $$TRANSPORT_PRI_ROOT/src/io/tcp/TcpTransport.cpp \
    $$TRANSPORT_PRI_ROOT/src/io/serial/SerialTransport.cpp \
    $$TRANSPORT_PRI_ROOT/src/codec/SystemCodec.cpp

# FakeDdsProvider 零 FastDDS 依赖,恒在编译面内。DdsTransport 与 provider 注册表随
# #203 的双队列重写回到编译面;DdsNode 随 #204 的整体重写(注册接口 / Dispatcher 键 /
# 两段式 corr / 四个交互方法)一并回来。DDS 至此整条链路都在面内。
SOURCES += \
    $$TRANSPORT_PRI_ROOT/src/io/dds/FakeDdsProvider.cpp \
    $$TRANSPORT_PRI_ROOT/src/io/dds/DdsProviderRegistry.cpp \
    $$TRANSPORT_PRI_ROOT/src/io/dds/DdsTransport.cpp \
    $$TRANSPORT_PRI_ROOT/src/node/DdsNode.cpp

# Fast DDS 可选依赖(ADR-0022 D4):命中才把真实 provider 编进来。fastdds.pri 同时
# 追加 DEFINES += TRANSPORT_HAS_FASTDDS 与 -lfastdds -lfastcdr,下游因此自动获得
# 「装了就编、没装就跳过」的降级,以及 FASTDDS_ROOT= / CONFIG+=no_fastdds 两个开关。
include($$TRANSPORT_PRI_ROOT/qmake/fastdds.pri)
!isEmpty(TRANSPORT_HAS_FASTDDS) {
    SOURCES += \
        $$TRANSPORT_PRI_ROOT/src/io/dds/FastDdsRawType.cpp \
        $$TRANSPORT_PRI_ROOT/src/io/dds/FastDdsProvider.cpp
}

# AsyncTask 协程运行时(强制依赖 —— RT_DESIGN_002)。子模块须先
# `git submodule update --init third_party/AsyncTask`。
#
# 与 CMake 的一处结构差异:CMake 把 AsyncTask 单独编成 asynctask 静态库再 PUBLIC
# 链给 transport;qmake 这边直接 include 上游的 AsyncTask.pri,让 AsyncTask 的
# 目标文件一并进消费者的 target(仓库内部即 libtransport.a),链接面等价。
# AsyncTask.pri 已列全它自己的 HEADERS/SOURCES、INCLUDEPATH 与 ASYNC_HAS_QTCORE,
# 清单由上游维护,此处不重抄。下游因此不必知道 AsyncTask 的存在(ADR-0022 D1)。
#
# ⚠ 必须放在 `QT += core network serialport` **之后**:AsyncTask.pri 内部有
#   contains(QT, core) 判断,提前 include 会漏掉它的 Qt 那半边源文件与宏。
!exists($$TRANSPORT_PRI_ROOT/third_party/AsyncTask/coro/all.hpp) {
    error("AsyncTask 子模块未初始化:请运行 `git submodule update --init third_party/AsyncTask`。")
}
include($$TRANSPORT_PRI_ROOT/third_party/AsyncTask/AsyncTask.pri)
