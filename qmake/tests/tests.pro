# ---------------------------------------------------------------------------
# transport_tests —— 对应 CMakeLists.txt 里的 add_executable(transport_tests ...)。
# 单一测试目标:全部 tests/* 纳入一个可执行文件,在 AsyncTask fiber 调度器内跑。
#
# 静态库链接顺序(务必保持):
#   -ltransport -lgtest  →  boost(fiber/context/thread/chrono)  →  -lutil -lpthread
# libtransport.a 里含 AsyncTask 的目标文件,而 AsyncTask 引用 boost 符号;静态归档
# 是左到右单遍解析,boost 必须排在 -ltransport **之后**。这也是本 .pro **不**
# include AsyncTask.pri 的原因 —— 那个 .pri 会把 boost 的 -l 塞到前面去,必然一堆
# undefined reference。AsyncTask 的头路径与 ASYNC_HAS_QTCORE 在此手工补上。
# -lutil 提供 openpty()(tests/serial_transport_test.cpp、tests/link_state_test.cpp
# 用到),对应 CMake 里链的 util。
#
# 注:qmake 不支持在 `\` 续行的赋值中间插注释,故源清单按注释切成数段
# `SOURCES +=`,内容与顺序仍与 CMakeLists.txt 逐字对应。
# ---------------------------------------------------------------------------

include(../common.pri)

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
TARGET = transport_tests
DESTDIR = $$TRANSPORT_BIN_DIR

# serialport 是 libtransport.a 的传递依赖(CMake 里 Qt5::SerialPort 是 PUBLIC),
# 可执行文件这一端也得链上,否则 SerialTransport.o 里的 QSerialPort 符号无解。
QT += core network serialport
QT -= gui

GTEST_DIR = $$TRANSPORT_ROOT/third_party/googletest/googletest

INCLUDEPATH += \
    $$TRANSPORT_ROOT/include \
    $$TRANSPORT_ROOT/tests \
    $$GTEST_DIR/include

# AsyncTask 的头与宏:手工给,不 include AsyncTask.pri(见文件头的链接顺序说明)。
INCLUDEPATH += $$TRANSPORT_ROOT/third_party/AsyncTask/coro
DEFINES += ASYNC_HAS_QTCORE

SOURCES += \
    $$TRANSPORT_ROOT/tests/coro_test_main.cpp

# —— 重设计(redesign 分支)后仍成立的用例 ——
SOURCES += \
    $$TRANSPORT_ROOT/tests/version_test.cpp \
    $$TRANSPORT_ROOT/tests/error_test.cpp \
    $$TRANSPORT_ROOT/tests/dispatcher_test.cpp \
    $$TRANSPORT_ROOT/tests/coro_harness_test.cpp \
    $$TRANSPORT_ROOT/tests/udp_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/tcp_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/tcp_transport_read_test.cpp \
    $$TRANSPORT_ROOT/tests/tcp_transport_reconnect_test.cpp \
    $$TRANSPORT_ROOT/tests/serial_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/node_base_lifecycle_test.cpp \
    $$TRANSPORT_ROOT/tests/protocol_node_test.cpp \
    $$TRANSPORT_ROOT/tests/protocol_node_tcp_e2e_test.cpp

# —— 以下两个 TCP 用例继续停摆，各有明确的待决依据（#181 的判定）——
#   tcp_client_reconfig_test（`ApplyConfig` 热更新的去留待 ADR-0011 D11 裁决）
#   tcp_server_accept_test（`TcpServer` 本轮不做，ADR-0011 D10）
# —— 以下用例仍在旧 ITransport / NodeBase 面上，随重设计逐个迁移后再放回 ——
#   protocol_node_handler_test / protocol_node_lifecycle_test
#   protocol_node_capacity_test / protocol_node_udp_test / transport_contract_test
#   fake_coro_transport_test / send_semantics_fake_test
#   shared_completion_test（SharedCompletion 已随手搓同步件一并删除）
#   handler_loop_test（HandlerLoop 已随 ADR-0009 废止内建 handler 通道而删除）
SOURCES += \
    $$TRANSPORT_ROOT/tests/codec/message_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/length_field_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/system_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/datagram_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/system_datagram_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/fake_dds_provider_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_registry_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_node_test.cpp

# 1) 先本项目的静态库。
LIBS += -L$$TRANSPORT_LIB_DIR -ltransport -L$$TRANSPORT_LIB_DIR -lgtest

# 2) 再 Fast DDS(libtransport.a 引用它的符号)。命中时把真实 provider 的用例编入。
include(../fastdds.pri)
!isEmpty(TRANSPORT_HAS_FASTDDS) {
    SOURCES += \
        $$TRANSPORT_ROOT/tests/dds/fast_dds_provider_test.cpp \
        $$TRANSPORT_ROOT/tests/dds/dds_node_fastdds_e2e_test.cpp
    # FastDdsProvider.hpp 不是公共头(在 src/ 下),用例须直接见到它。
    INCLUDEPATH += $$TRANSPORT_ROOT/src
}

# 3) 最后 boost 与系统库。boost 只有静态归档,必须排在 -ltransport 之后。
LIBS += -L/usr/local/lib -lboost_fiber -lboost_context -lboost_thread -lboost_chrono
LIBS += -lutil -lpthread

# 库变更触发重链。
PRE_TARGETDEPS += $$TRANSPORT_LIB_DIR/libtransport.a $$TRANSPORT_LIB_DIR/libgtest.a
