# transport — C++ 通信中间件

将数据传输与编解码解耦的 C++17 通信中间件库。支持 TCP / UDP / DDS / 串口。

## 构建

```bash
cmake -S . -B build -DTRANSPORT_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## 状态

- [x] Foundation：核心接口、分帧、接收交付、传输基类
- [x] TCP（client / server）
- [x] UDP（单播 / 组播 / 广播）
- [ ] 串口
- [ ] DDS（Fast DDS，pub-sub / req-resp）
- [ ] TransportFactory + JSON 配置

设计文档见 `docs/superpowers/specs/2026-06-09-transport-middleware-design.md`。
