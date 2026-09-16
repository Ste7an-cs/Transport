# 示例 ① tcp_client —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(tcp_client tcp_client/main.cpp)`。

include(../example.pri)

TARGET = transport_example_tcp_client

SOURCES += $$TRANSPORT_ROOT/examples/tcp_client/main.cpp

HEADERS += $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp
