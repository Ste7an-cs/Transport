# ---------------------------------------------------------------------------
# transport_tests —— 对应 CMakeLists.txt 里的 add_executable(transport_tests ...)。
# 单一测试目标:全部 tests/* 纳入一个可执行文件,在 AsyncTask fiber 调度器内跑。
#
# 依赖本库的那一整段(QT / include 的 INCLUDEPATH / AsyncTask 的头与
# ASYNC_HAS_QTCORE / Fast DDS 探测 / -ltransport / boost 的链接顺序 /
# PRE_TARGETDEPS)全在根上的 Transport.pri 里,由 TRANSPORT_LINK_STATIC = 1 选到
# 「链预编静态库」那一支(ADR-0022 D3)。本文件只留 tests 独有的东西。
#
# tests 独有:gtest(第三方静态库,本仓库自己编,见 qmake/gtest/gtest.pro)、
# -lutil(提供 openpty(),tests/serial_transport_test.cpp 与 tests/link_state_test.cpp
# 用到,对应 CMake 里链的 util)、测试源清单。
#
# 注:qmake 不支持在 `\` 续行的赋值中间插注释,故源清单按注释切成数段
# `SOURCES +=`,内容与顺序仍与 CMakeLists.txt 逐字对应。
# ---------------------------------------------------------------------------

include(../build_layout.pri)

TRANSPORT_LINK_STATIC = 1
include($$TRANSPORT_ROOT/Transport.pri)

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
TARGET = transport_tests
DESTDIR = $$TRANSPORT_BIN_DIR

GTEST_DIR = $$TRANSPORT_ROOT/third_party/googletest/googletest

INCLUDEPATH += \
    $$TRANSPORT_ROOT/tests \
    $$GTEST_DIR/include

SOURCES += \
    $$TRANSPORT_ROOT/tests/coro_test_main.cpp

# —— 重设计(redesign 分支)后仍成立的用例 ——
SOURCES += \
    $$TRANSPORT_ROOT/tests/version_test.cpp \
    $$TRANSPORT_ROOT/tests/error_test.cpp \
    $$TRANSPORT_ROOT/tests/dispatcher_test.cpp \
    $$TRANSPORT_ROOT/tests/dispatcher_concurrency_test.cpp \
    $$TRANSPORT_ROOT/tests/coro_harness_test.cpp \
    $$TRANSPORT_ROOT/tests/udp_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/tcp_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/tcp_transport_read_test.cpp \
    $$TRANSPORT_ROOT/tests/tcp_transport_reconnect_test.cpp \
    $$TRANSPORT_ROOT/tests/serial_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/node_base_lifecycle_test.cpp \
    $$TRANSPORT_ROOT/tests/protocol_node_test.cpp \
    $$TRANSPORT_ROOT/tests/protocol_node_tcp_e2e_test.cpp \
    $$TRANSPORT_ROOT/tests/protocol_node_endpoint_test.cpp \
    $$TRANSPORT_ROOT/tests/protocol_node_outbound_endpoint_test.cpp

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
    $$TRANSPORT_ROOT/tests/codec/system_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/datagram_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/system_datagram_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/codec/dds_codec_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/fake_dds_provider_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_registry_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_transport_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_node_test.cpp \
    $$TRANSPORT_ROOT/tests/dds/dds_node_dynamic_registration_test.cpp

# Fast DDS 命中时(TRANSPORT_HAS_FASTDDS 由 Transport.pri 的探测段输出)把真实
# provider 的用例编入。
!isEmpty(TRANSPORT_HAS_FASTDDS) {
    SOURCES += \
        $$TRANSPORT_ROOT/tests/dds/fast_dds_provider_test.cpp \
        $$TRANSPORT_ROOT/tests/dds/dds_node_fastdds_e2e_test.cpp \
        $$TRANSPORT_ROOT/tests/dds/fast_dds_payload_length_test.cpp
    # FastDdsProvider.hpp 不是公共头(在 src/ 下),用例须直接见到它。链静态库这条
    # 路径上 Transport.pri 不给 src/,故在此补。
    INCLUDEPATH += $$TRANSPORT_ROOT/src
}

# gtest 与 -lutil 排在 Transport.pri 给的那串之后:测试的目标文件在链接行最前面,
# 静态归档 libgtest.a 在其后即可解析。
LIBS += -L$$TRANSPORT_LIB_DIR -lgtest
LIBS += -lutil

# gtest 变更触发重链(libtransport.a 的那条由 Transport.pri 给)。
PRE_TARGETDEPS += $$TRANSPORT_LIB_DIR/libgtest.a
