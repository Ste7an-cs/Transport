# ---------------------------------------------------------------------------
# transport_perf —— 对应 CMakeLists.txt 里的 add_executable(transport_perf ...)。
# 性能基准测试工具(ADR-0018 **D7**:独立可执行,不进 transport_tests),须**同时**登记
# CMake 与 qmake 两套清单——这是仓库既有的双清单义务。
#
# 静态库链接顺序(务必保持,理由同 qmake/tests/tests.pro):
#   -ltransport  →  boost(fiber/context/thread/chrono)  →  -lpthread
# libtransport.a 里含 AsyncTask 的目标文件,而 AsyncTask 引用 boost 符号;静态归档是
# 左到右单遍解析,boost 必须排在 -ltransport **之后**。这也是本 .pro **不** include
# AsyncTask.pri 的原因。AsyncTask 的头路径与 ASYNC_HAS_QTCORE 在此手工补上。
#
# 与 tests.pro 的两处差异:
#   * 不链 gtest、不链 -lutil(本工具不用 openpty);
#   * SOURCES 全部在 tools/perf/ 下,且 INCLUDEPATH 要含该目录(工具内部头用引号包含)。
# ---------------------------------------------------------------------------

include(../common.pri)

TEMPLATE = app
CONFIG += console
CONFIG -= app_bundle
TARGET = transport_perf
DESTDIR = $$TRANSPORT_BIN_DIR

# serialport 是 libtransport.a 的传递依赖(CMake 里 Qt5::SerialPort 是 PUBLIC),
# 可执行文件这一端也得链上,否则 SerialTransport.o 里的 QSerialPort 符号无解。
QT += core network serialport
QT -= gui

INCLUDEPATH += \
    $$TRANSPORT_ROOT/include \
    $$TRANSPORT_ROOT/tools/perf

# AsyncTask 的头与宏:手工给,不 include AsyncTask.pri(见文件头的链接顺序说明)。
INCLUDEPATH += $$TRANSPORT_ROOT/third_party/AsyncTask/coro
DEFINES += ASYNC_HAS_QTCORE

SOURCES += \
    $$TRANSPORT_ROOT/tools/perf/main.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfOptions.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfStats.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfLink.cpp \
    $$TRANSPORT_ROOT/tools/perf/AcceptedTcpTransport.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfRun.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfServer.cpp \
    $$TRANSPORT_ROOT/tools/perf/PerfClient.cpp

HEADERS += \
    $$TRANSPORT_ROOT/tools/perf/PerfOptions.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfStats.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfWire.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfLink.hpp \
    $$TRANSPORT_ROOT/tools/perf/PerfSuites.hpp \
    $$TRANSPORT_ROOT/tools/perf/AcceptedTcpTransport.hpp

# 1) 先本项目的静态库。
LIBS += -L$$TRANSPORT_LIB_DIR -ltransport

# 2) 再 Fast DDS(libtransport.a 引用它的符号)。
include(../fastdds.pri)

# 3) 最后 boost 与系统库。boost 只有静态归档,必须排在 -ltransport 之后。
LIBS += -L/usr/local/lib -lboost_fiber -lboost_context -lboost_thread -lboost_chrono
LIBS += -lpthread

# 库变更触发重链。
PRE_TARGETDEPS += $$TRANSPORT_LIB_DIR/libtransport.a
