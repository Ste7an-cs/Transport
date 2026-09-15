# ---------------------------------------------------------------------------
# transport —— qmake 构建入口(与根 CMakeLists.txt 并存)。
#
# 本文件留在仓库根而非 qmake/ 下,是因为 Qt Creator 只认根上的工程入口;
# 真正的 .pro 都收在 qmake/ 里,不散进 src/、tests/。
#
# 构建(shadow build):
#   mkdir build-qmake && cd build-qmake
#   qmake ../transport.pro && make -j$(nproc)
#   ./bin/transport_tests
#
# 可选开关:
#   qmake CONFIG+=no_fastdds ../transport.pro   # 强制不编 FastDdsProvider
#   qmake FASTDDS_ROOT=/opt/fastdds ../transport.pro
# ---------------------------------------------------------------------------

TEMPLATE = subdirs
CONFIG += ordered

SUBDIRS = lib gtest tests perf

lib.subdir   = qmake/lib
gtest.subdir = qmake/gtest
tests.subdir = qmake/tests
# transport_perf —— 性能基准测试工具(ADR-0018 D7:独立可执行,不进 transport_tests)。
perf.subdir  = qmake/perf

tests.depends = lib gtest
perf.depends  = lib
