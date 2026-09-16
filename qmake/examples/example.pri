# ---------------------------------------------------------------------------
# 六个示例 .pro 的公共部分 —— 对应 examples/CMakeLists.txt 里的
# `transport_add_example()` 函数。
#
# 静态库链接顺序(务必保持,理由同 qmake/perf/perf.pro):
#   -ltransport  →  Fast DDS  →  boost(fiber/context/thread/chrono)  →  -lpthread
# libtransport.a 里含 AsyncTask 的目标文件,而 AsyncTask 引用 boost 符号;静态归档是
# 左到右单遍解析,boost 必须排在 -ltransport **之后**。这也是本文件**不** include
# AsyncTask.pri 的原因 —— AsyncTask 的头路径与 ASYNC_HAS_QTCORE 在此手工补上。
# ---------------------------------------------------------------------------

include($$PWD/../common.pri)

# common.pri 按「子工程在 qmake/<sub>/」算 TRANSPORT_BUILD_ROOT(OUT_PWD/../..);
# 示例多嵌了一层(qmake/examples/<name>/),故在此多退一级,产物仍汇到 build 根的
# lib/ 与 bin/,与 libtransport.a 对得上。
TRANSPORT_BUILD_ROOT = $$clean_path($$OUT_PWD/../../..)
TRANSPORT_LIB_DIR = $$TRANSPORT_BUILD_ROOT/lib
TRANSPORT_BIN_DIR = $$TRANSPORT_BUILD_ROOT/bin

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
DESTDIR = $$TRANSPORT_BIN_DIR

# serialport 是 libtransport.a 的传递依赖(CMake 里 Qt5::SerialPort 是 PUBLIC),
# 可执行文件这一端也得链上,否则 SerialTransport.o 里的 QSerialPort 符号无解。
QT += core network serialport
QT -= gui

INCLUDEPATH += \
    $$TRANSPORT_ROOT/include \
    $$TRANSPORT_ROOT/examples/common

# AsyncTask 的头与宏:手工给,不 include AsyncTask.pri(见文件头的链接顺序说明)。
INCLUDEPATH += $$TRANSPORT_ROOT/third_party/AsyncTask/coro
DEFINES += ASYNC_HAS_QTCORE

# 1) 先本项目的静态库。
LIBS += -L$$TRANSPORT_LIB_DIR -ltransport

# 2) 再 Fast DDS(libtransport.a 引用它的符号)。
include($$PWD/../fastdds.pri)

# 3) 最后 boost 与系统库。boost 只有静态归档,必须排在 -ltransport 之后。
LIBS += -L/usr/local/lib -lboost_fiber -lboost_context -lboost_thread -lboost_chrono
LIBS += -lpthread

# 库变更触发重链。
PRE_TARGETDEPS += $$TRANSPORT_LIB_DIR/libtransport.a
