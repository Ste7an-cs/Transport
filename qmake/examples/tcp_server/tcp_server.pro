# 示例 ② tcp_server —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(tcp_server tcp_server/main.cpp tools/perf/AcceptedTcpTransport.cpp)`。
#
# 多编一个 tools/perf/AcceptedTcpTransport.cpp:库里的 TcpTransport 是**客户端**传输,
# TcpServer 本轮不做(ADR-0011 D10),服务端角色缺一个"监听 + 接受"的 ITransport 实现;
# tools/perf 早已为同一原因写了一个,直接复用而不再抄一份。

include(../example.pri)

TARGET = transport_example_tcp_server

INCLUDEPATH += $$TRANSPORT_ROOT/tools/perf

SOURCES += \
    $$TRANSPORT_ROOT/examples/tcp_server/main.cpp \
    $$TRANSPORT_ROOT/tools/perf/AcceptedTcpTransport.cpp

HEADERS += \
    $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp \
    $$TRANSPORT_ROOT/tools/perf/AcceptedTcpTransport.hpp
