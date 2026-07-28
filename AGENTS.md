# transport — C++ 通信中间件

C++17 通信中间件库,三层解耦:**Transport(纯字节管道)/ ICodec(线缆格式)/ Comm(交互节点)**。详见 `README.md`、`docs/需求规格说明书-协程原生.md`、`docs/设计说明书-协程原生.md`、`CHANGELOG.md`(as-built 文档存档于 tag `v0.3.0`)。

## Agent skills

### Issue tracker

Issues 记在本仓库的 **GitHub Issues**(`Ste7an-cs/Transport`),用 `gh` CLI 操作。See `docs/agents/issue-tracker.md`.

### Domain docs

**Single-context**:根 `CONTEXT.md`(由 `/domain-modeling` 惰性创建)+ `docs/adr/`。See `docs/agents/domain.md`.
