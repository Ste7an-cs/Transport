# transport — C++ 通信中间件

C++17 通信中间件库,三层解耦:**Transport(纯字节管道)/ ICodec(线缆格式)/ Comm(交互节点)**。详见 `README.md`、`docs/需求规格说明书-协程原生.md`、`docs/设计说明书-协程原生.md`、`CHANGELOG.md`(as-built 文档存档于 tag `v0.3.0`)。

## 编码规范

C++ 代码遵循 **Google C++ Style Guide** + **Doxygen 风格注释**,并存既有约定(不抛异常/结构化 `Result`、`[[nodiscard]]`、机器可判别错误类别、CONTEXT 术语)。详见 `CODING_STANDARDS.md`。

## Agent skills

### Issue tracker

Issues 记在本仓库的 **GitHub Issues**(`Ste7an-cs/Transport`),用 `gh` CLI 操作。See `docs/agents/issue-tracker.md`.

### Domain docs

**Single-context**:根 `CONTEXT.md`(由 `/domain-modeling` 惰性创建)+ `docs/adr/`。See `docs/agents/domain.md`.
