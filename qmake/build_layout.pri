# ---------------------------------------------------------------------------
# qmake 构建布局 —— **仅本仓库内部**的构建设施(原 common.pri)。
#
# 与根上的 Transport.pri 分工明确:
#   * Transport.pri   —— 「用这个库需要什么」(源清单 / INCLUDEPATH / QT /
#                          DEFINES / LIBS / Fast DDS 探测),**下游也 include 它**;
#   * build_layout.pri —— 「本仓库的产物摆在哪、告警口径怎么定」,下游无关,
#                          下游有自己的构建布局。
#
# 职责:
#   * 定义 TRANSPORT_ROOT(仓库根),供各 .pro 拼绝对路径;
#   * 定义 TRANSPORT_LIB_DIR / TRANSPORT_BIN_DIR(Transport.pri 的
#     TRANSPORT_LINK_STATIC 模式要用前者);
#   * 把中间产物与最终产物赶出源码树(qmake 默认 in-source,会把 .o/Makefile
#     撒进 src/、tests/);
#   * 对齐两套构建的告警口径。
#
# 推荐用法为 shadow build:
#   mkdir build-qmake && cd build-qmake
#   qmake ../transport.pro && make -j$(nproc)
# 产物落在 build-qmake/lib/(静态库)与 build-qmake/bin/(可执行)。
# ---------------------------------------------------------------------------

# C++17:Transport.pri 里也有一份(下游要用),此处保留是因为 qmake/gtest/gtest.pro
# 只 include 本文件、不 include Transport.pri,而 googletest 也要 C++17。
CONFIG += c++17

# 告警口径与 CMake 对齐:根 CMakeLists.txt 不追加任何 -W 开关,用编译器默认口径。
# qmake 的 g++ mkspec 默认塞 -Wall -Wextra,会在**不归本仓库管**的第三方头
# (gtest 的比较模板)上刷出噪声。这里把它清空,两套构建的编译面才真正一致;
# 若要临时开告警:make QMAKE_CXXFLAGS_WARN_ON="-Wall -Wextra"。
#
# ⚠ 这两行**只能在这里**,绝不能进 Transport.pri —— 否则每个 qmake 下游一 include
#   就被无声关掉了全部编译告警。
QMAKE_CXXFLAGS_WARN_ON =
QMAKE_CFLAGS_WARN_ON =

# 本文件位于 <root>/qmake/,故上一级即仓库根。
TRANSPORT_ROOT = $$clean_path($$PWD/..)

# 各子工程的构建根:shadow build 时是 build-qmake/qmake/<sub>,in-source 时是
# 源码目录本身。统一把三类产物汇到同一处,lib 与 tests 才能互相找到。
TRANSPORT_BUILD_ROOT = $$clean_path($$OUT_PWD/../..)
TRANSPORT_LIB_DIR = $$TRANSPORT_BUILD_ROOT/lib
TRANSPORT_BIN_DIR = $$TRANSPORT_BUILD_ROOT/bin

# 中间产物按子工程隔离,落在各自的 OUT_PWD 下。
OBJECTS_DIR = $$OUT_PWD/.obj
MOC_DIR     = $$OUT_PWD/.moc
RCC_DIR     = $$OUT_PWD/.rcc
UI_DIR      = $$OUT_PWD/.ui
