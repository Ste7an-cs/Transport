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
#   qmake CONFIG+=examples ../transport.pro     # 连六个示例程序一起编(默认不编)
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

# ---------------------------------------------------------------------------
# examples —— 六个完整可运行的示例程序(#255)。
#
# **默认不构建**,对应 CMake 的 `option(TRANSPORT_BUILD_EXAMPLES "Build examples" OFF)`;
# 这里的等价开关是 `qmake CONFIG+=examples ../transport.pro`。
# 不加开关时本段整段不生效,构建与从前逐字相同。
# ---------------------------------------------------------------------------
contains(CONFIG, examples) {
    SUBDIRS += examples
    examples.subdir  = qmake/examples
    examples.depends = lib

    # ADR-0022 D5 —— 最小的 qmake **下游**样例,与上面六个分开列:
    #   * 六个示例链 libtransport.a(故 examples.depends = lib);
    #   * 本例**不链**,它 include 根上的 Transport.pri 走源码级并入 —— 那正是
    #     下游的接入路径,也是本例存在的唯一理由(Transport.pri 写错了要有信号)。
    # 因此它**不依赖 lib**,.pro 也留在 examples/downstream_qmake/ 与 main.cpp 同级,
    # 形如一个真实的外部工程。
    SUBDIRS += downstream_qmake
    downstream_qmake.subdir = examples/downstream_qmake
}
