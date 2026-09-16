# 示例 ④ dds_pubsub —— 对应 examples/CMakeLists.txt 的
# `transport_add_example(dds_pubsub dds_pubsub/main.cpp)`。

include(../example.pri)

TARGET = transport_example_dds_pubsub

SOURCES += $$TRANSPORT_ROOT/examples/dds_pubsub/main.cpp

HEADERS += $$TRANSPORT_ROOT/examples/common/ExampleCommon.hpp
