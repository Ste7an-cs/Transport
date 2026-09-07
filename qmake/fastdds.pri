# ---------------------------------------------------------------------------
# Fast DDS(可选依赖 —— SRS RT_IF_DDS)的 qmake 探测,对应 CMakeLists.txt 里的
# find_package(fastcdr/fastdds QUIET) 那一段:找不到就**只**丢掉 FastDdsProvider,
# 框架其余能力照常构建可用。3.x 起包名由 fastrtps 改为 fastdds(ADR-0013 D14)。
#
# 这是优雅降级,不是报错 —— 缺席时仅 message() 提示。
#
# 开关:
#   qmake FASTDDS_ROOT=/opt/fastdds ...   # 换探测前缀(默认 /usr/local)
#   qmake CONFIG+=no_fastdds ...          # 强制关闭,即便装了也不编
#
# 输出:
#   TRANSPORT_HAS_FASTDDS = 1 / 空   —— 供 lib.pro、tests.pro 决定要不要加源文件
#   命中时追加 DEFINES += TRANSPORT_HAS_FASTDDS 与 -lfastdds -lfastcdr
#
# CMake 里 TRANSPORT_HAS_FASTDDS 是 PUBLIC 的编译定义,故 lib.pro 与 tests.pro
# **都要** include 本文件,两边的编译面才一致。
# ---------------------------------------------------------------------------

isEmpty(FASTDDS_ROOT): FASTDDS_ROOT = /usr/local

TRANSPORT_HAS_FASTDDS =

contains(CONFIG, no_fastdds) {
    message("Fast DDS disabled by CONFIG+=no_fastdds: building without FastDdsProvider")
} else {
    exists($$FASTDDS_ROOT/include/fastdds):exists($$FASTDDS_ROOT/include/fastcdr) {
        TRANSPORT_HAS_FASTDDS = 1
        message("Fast DDS found at $$FASTDDS_ROOT: enabling FastDdsProvider")
        DEFINES += TRANSPORT_HAS_FASTDDS
        INCLUDEPATH += $$FASTDDS_ROOT/include
        LIBS += -L$$FASTDDS_ROOT/lib -lfastdds -lfastcdr
    } else {
        message("Fast DDS NOT found under $$FASTDDS_ROOT: 未找到 Fast DDS,不编 FastDdsProvider")
    }
}
