# ---------------------------------------------------------------------------
# libgtest.a —— 对应 CMake 里 add_subdirectory(third_party/googletest) 提供的
# GTest::gtest 目标。
#
# 只编 gtest-all.cc(它把 googletest 的其余 .cc 全 #include 进来);**不编**
# gtest_main.cc —— tests/coro_test_main.cpp 自带 main(CMake 也是链 GTest::gtest
# 而非 gtest_main)。
#
# gtest 不需要 Qt,故 QT 全清空。第三方代码不参与本项目的告警策略。
# ---------------------------------------------------------------------------

include(../common.pri)

TEMPLATE = lib
CONFIG += staticlib
CONFIG -= qt
CONFIG += warn_off
TARGET = gtest
DESTDIR = $$TRANSPORT_LIB_DIR

QT -= core gui

GTEST_DIR = $$TRANSPORT_ROOT/third_party/googletest/googletest

INCLUDEPATH += $$GTEST_DIR $$GTEST_DIR/include

SOURCES += $$GTEST_DIR/src/gtest-all.cc

LIBS += -lpthread
