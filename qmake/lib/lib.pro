# ---------------------------------------------------------------------------
# libtransport.a —— 对应 CMakeLists.txt 里的 add_library(transport STATIC ...)。
#
# 与 CMake 的一处结构差异:CMake 把 AsyncTask 单独编成 asynctask 静态库再 PUBLIC
# 链给 transport;qmake 这边直接 include 上游的 AsyncTask.pri,让 AsyncTask 的
# 目标文件一并进 libtransport.a,链接面等价。AsyncTask.pri 已列全它自己的
# HEADERS/SOURCES、INCLUDEPATH 与 ASYNC_HAS_QTCORE,清单由上游维护,此处不重抄。
# 注意 AsyncTask.pri 内部有 contains(QT, core) 判断,故必须在设好 QT 之后 include。
#
# 注:qmake 不支持在 `\` 续行的赋值中间插注释(会把后半截一并注掉),故源清单按
# 注释切成数段 `SOURCES +=`,内容与顺序仍与 CMakeLists.txt 逐字对应。
# ---------------------------------------------------------------------------

include(../common.pri)

TEMPLATE = lib
CONFIG += staticlib
TARGET = transport
DESTDIR = $$TRANSPORT_LIB_DIR

QT += core network serialport
QT -= gui

INCLUDEPATH += $$TRANSPORT_ROOT/include $$TRANSPORT_ROOT/src

SOURCES += \
    $$TRANSPORT_ROOT/src/core/version.cpp \
    $$TRANSPORT_ROOT/src/core/Error.cpp \
    $$TRANSPORT_ROOT/src/node/NodeBase.cpp \
    $$TRANSPORT_ROOT/src/node/ProtocolNode.cpp \
    $$TRANSPORT_ROOT/src/io/udp/UdpTransport.cpp \
    $$TRANSPORT_ROOT/src/io/tcp/TcpTransport.cpp \
    $$TRANSPORT_ROOT/src/io/serial/SerialTransport.cpp \
    $$TRANSPORT_ROOT/src/codec/LengthFieldCodec.cpp \
    $$TRANSPORT_ROOT/src/codec/SystemCodec.cpp

# FakeDdsProvider 零 FastDDS 依赖,恒在编译面内。DdsTransport 与 provider 注册表随
# #203 的双队列重写回到编译面;DdsNode 随 #204 的整体重写(注册接口 / Dispatcher 键 /
# 两段式 corr / 四个交互方法)一并回来。DDS 至此整条链路都在面内。
SOURCES += \
    $$TRANSPORT_ROOT/src/io/dds/FakeDdsProvider.cpp \
    $$TRANSPORT_ROOT/src/io/dds/DdsProviderRegistry.cpp \
    $$TRANSPORT_ROOT/src/io/dds/DdsTransport.cpp \
    $$TRANSPORT_ROOT/src/node/DdsNode.cpp

# Fast DDS 可选依赖:命中才把真实 provider 编进来(见 qmake/fastdds.pri)。
include(../fastdds.pri)
!isEmpty(TRANSPORT_HAS_FASTDDS) {
    SOURCES += \
        $$TRANSPORT_ROOT/src/io/dds/FastDdsRawType.cpp \
        $$TRANSPORT_ROOT/src/io/dds/FastDdsProvider.cpp
}

# AsyncTask 协程运行时(强制依赖 —— RT_DESIGN_002)。子模块须先
# `git submodule update --init third_party/AsyncTask`。
!exists($$TRANSPORT_ROOT/third_party/AsyncTask/coro/all.hpp) {
    error("AsyncTask 子模块未初始化:请运行 `git submodule update --init third_party/AsyncTask`。")
}
include($$TRANSPORT_ROOT/third_party/AsyncTask/AsyncTask.pri)
