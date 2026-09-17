# ADR-0022：提供 `Transport.pri`，qmake 下游按 AsyncTask 的方式一行接入

**状态：** Accepted
**日期：** 2026-09-15
**关联：** 补齐 **#232**（qmake 与 CMake 并存）留下的消费者侧缺口；形态参照本仓库已依赖的 **`third_party/AsyncTask/AsyncTask.pri`**；ADR-0018 **D7**（`transport_perf` 独立可执行——本 ADR 使其与 tests 共用同一份源清单）。

## 背景（Context）

#232 给仓库加了 qmake 构建，但**只解决了"我们自己怎么构建"，没解决"下游怎么用"**。

现状是一个 subdirs 工程：`transport.pro` → `qmake/lib/lib.pro`（编出 `libtransport.a`）+ gtest + tests + perf。一个想用本库的 qmake 工程，**没有任何受支持的接入方式**——它得自己摸清楚：`INCLUDEPATH` 要加哪两个目录、要不要 `include` AsyncTask 的 `.pri`、boost 四个库的链接顺序、Fast DDS 在不在、`TRANSPORT_HAS_FASTDDS` 该不该定义、`libtransport.a` 从哪来。

README 的「链接到你的工程」**只写了 CMake**（`add_subdirectory` + `target_link_libraries`），qmake 那栏是空的。

### 我们依赖的 AsyncTask 给出了现成的样板

```pro
# AsyncTask/example/basic/basic.pro —— 消费者只需一行
include($$PWD/../../AsyncTask.pri)
```

`AsyncTask.pri` 把 `HEADERS` / `SOURCES` / `INCLUDEPATH` / `DEFINES` / `LIBS` 直接并进**消费者的 target**——不建中间库、不需要构建顺序、不需要 `PRE_TARGETDEPS`。本仓库的 `qmake/lib/lib.pro` 正是这么用它的。

**我们对下游，应当提供同一形状。**

## 决策（Decision）

- **D1（根上提供 `Transport.pri`，源码级并入消费者 target）：** 形状与 `AsyncTask.pri` 一致：

  ```pro
  # 下游工程
  include($$PWD/path/to/transport/Transport.pri)
  ```

  它负责：库的全部 `SOURCES`、`INCLUDEPATH`（`include/` 与 `src/`）、`QT += core network serialport`、Fast DDS 的可选探测与 `TRANSPORT_HAS_FASTDDS`、以及**递归 include `AsyncTask.pri`**（下游因此不必知道 AsyncTask 的存在）。

  **不采用"链接预编静态库"的形态**：那要求下游先构建本库、处理 `PRE_TARGETDEPS` 与产物路径，而 qmake 没有 CMake 那套 target 依赖传递。源码级并入是 qmake 生态里的常规做法，也是我们已经在用的做法。

- **D2（`Transport.pri` 是唯一的源清单，`lib.pro` 改为 include 它）：** 现在 `qmake/lib/lib.pro` 自带一份源清单。本 ADR 之后它只剩：

  ```pro
  include(../common.pri)
  include($$TRANSPORT_ROOT/Transport.pri)
  TEMPLATE = lib
  CONFIG  += staticlib
  TARGET   = transport
  DESTDIR  = $$TRANSPORT_LIB_DIR
  ```

  **这是本 ADR 的实质收益**：qmake 侧的源清单由两份（若再加下游文档里的示例则三份）收敛为**一份**。仓库既有的"CMake 与 qmake 两份清单须手工同步"那条代价**不变**（仍是两份），但**不再增加第三份**。

- **D3（保留 `libtransport.a` 这个目标）：** 尽管下游走源码级并入，本仓库自己的 `tests` / `perf` / `examples` 仍链静态库——多个可执行共用一次编译结果，比每个都重编一遍库快得多。

  **由此形成两条路径**：仓库内部走 `lib.pro` 编出的静态库，下游走 `Transport.pri` 源码并入。**两者的源清单是同一份**（**D2**），不会漂移。

- **D4（Fast DDS 探测对下游同样生效，且可被下游覆盖）：** `Transport.pri` 内部 include `qmake/fastdds.pri`，故下游同样享有"装了就编、没装就跳过"的降级；`FASTDDS_ROOT=` 与 `CONFIG+=no_fastdds` 两个开关对下游照常可用。

- **D5（提供一个最小可跑的下游样例并纳入 CI 口径）：** 在 `examples/` 下放一个**纯 qmake** 的最小工程，其 `.pro` 只有 `include(../../Transport.pri)` 与一个 `main.cpp`。

  > **理由**：`Transport.pri` 是给别人用的，**本仓库自己的构建不会经过它**（内部走静态库，**D3**）。没有这个样例，它一旦写错**没有任何信号**——这正是本仓库反复吃过亏的那类问题（README 的 `TopicKey` 曾编译不过、服务端示例曾缺 `endpoint`）。

## 明确接受的代价

1. **下游每个 target 都会重编一遍库的源码。** 源码级并入的固有代价：下游若有多个可执行，库会被编多次。`AsyncTask.pri` 同理，是 qmake 生态的常态；要避免，下游可自行把它包成一个 staticlib 子工程（我们的 `lib.pro` 就是范例）。

2. **`src/` 会进下游的 `INCLUDEPATH`。** 源码级并入必须如此（`FastDdsProvider.hpp` 等私有头在 `src/` 下）。**后果是下游能 `#include` 到本库的私有头**——公共/私有的边界在 qmake 这条路径上**只剩约定，没有强制**。CMake 那条路径不受影响（`src/` 是 `PRIVATE`）。

3. **仍是两份源清单（CMake 与 qmake），本 ADR 不解决。** 它只保证 qmake 这一侧不再分裂成多份。

## 影响（Consequences）

- **正面：** ① qmake 下游首次有受支持的接入方式，且是一行；② qmake 侧的源清单由两份收敛为一份；③ 下游自动获得 Fast DDS 的可选降级与两个开关；④ `Transport.pri` 有了持续验证的样例（**D5**）。
- **负面（明确接受）：** 见上三条，其中 **代价 2（私有头对下游可见）是这条路径独有的**。
- **对 README：** 「链接到你的工程」补 qmake 一栏；「构建」一节说明 `Transport.pri` 的定位与两条路径的区别。
- **对 SDD：** 构建形态一节补 `Transport.pri`。

## 备选方案（Alternatives considered）

- **让下游链接预编的 `libtransport.a`**（形态对应 CMake 的 `add_subdirectory`）。**否决理由：** qmake 没有 target 依赖传递，下游要自己处理构建顺序、产物路径、`PRE_TARGETDEPS`、以及 boost/Qt/Fast DDS 的传递性链接——把我们内部已经解决过的复杂度原样推给下游。而 AsyncTask 已经示范了更简单的形态，我们又恰好是它的消费者。
- **`Transport.pri` 只导出 `INCLUDEPATH` + `LIBS`，源码仍由下游自行构建。** **否决理由：** 那等于什么都没解决——下游仍要知道编哪些文件、Fast DDS 在不在。
- **不提供 `.pri`，在 README 里写一段"qmake 下游该怎么配"的说明。** **否决理由：** 文档会漂移，而 `.pri` 是可执行的、且有 **D5** 的样例持续验证。本仓库已多次吃过"文档与代码漂移"的亏。
- **`lib.pro` 与 `Transport.pri` 各自维护一份源清单。** **否决理由：** 见 **D2**——那正是本 ADR 要消除的东西。
