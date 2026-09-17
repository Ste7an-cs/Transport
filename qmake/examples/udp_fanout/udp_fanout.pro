# 示例 ③ udp_fanout —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(udp_fanout udp_fanout/main.cpp)`。

include(../example.pri)

TARGET = transport_example_udp_fanout

SOURCES += $$TRANSPORT_ROOT/examples/udp_fanout/main.cpp

HEADERS += $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp
