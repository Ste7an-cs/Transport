#include "transport/TransportFactory.hpp"

#include <chrono>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "transport/dds/DdsImpl.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"

using namespace transport;

namespace {

// 写临时 JSON 文件，返回路径
std::string WriteJson(const std::string& name, const std::string& content) {
  std::string path = std::string(::testing::TempDir()) + name;
  std::ofstream out(path, std::ios::trunc);
  out << content;
  return path;
}

// 断言失败且错误串含全部片段
void ExpectConfigError(const Result<std::vector<std::shared_ptr<ITransport>>>& r,
                       std::initializer_list<const char*> fragments) {
  ASSERT_FALSE(static_cast<bool>(r));
  EXPECT_EQ(r.error.rfind("config:", 0), 0u) << r.error;
  for (const char* f : fragments)
    EXPECT_NE(r.error.find(f), std::string::npos) << r.error << " 缺少: " << f;
}

}  // namespace

TEST(FactoryJson, AllFiveTypesParsed) {
  auto path = WriteJson("all5.json", R"({
    "transports": [
      { "type": "tcp_client", "host": "127.0.0.1", "port": 9000, "auto_reconnect": false,
        "framer": { "header_size": 8, "length_offset": 4, "length_size": 4, "big_endian": true } },
      { "type": "tcp_server", "bind_addr": "127.0.0.1", "port": 9001, "max_clients": 5 },
      { "type": "udp", "mode": "multicast", "multicast_group": "239.0.0.1", "local_port": 5000, "remote_port": 5000 },
      { "type": "dds", "mode": "pubsub", "topics": ["sensor"], "domain_id": 0,
        "qos": { "reliability": "best_effort", "durability": "transient_local", "history_depth": 5 } },
      { "type": "serial", "device": "/dev/ttyS0", "baud_rate": 9600, "parity": "E" }
    ]
  })");
  auto r = TransportFactory::CreateFromFile(path);
  ASSERT_TRUE(static_cast<bool>(r)) << r.error;
  ASSERT_EQ(r.value.size(), 5u);
  EXPECT_NE(std::dynamic_pointer_cast<TcpClientImpl>(r.value[0]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<TcpServerImpl>(r.value[1]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<UdpImpl>(r.value[2]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<DdsImpl>(r.value[3]), nullptr);
  EXPECT_NE(std::dynamic_pointer_cast<SerialImpl>(r.value[4]), nullptr);
}

TEST(FactoryJson, MinimalEntriesUseDefaults) {
  auto path = WriteJson("minimal.json", R"({
    "transports": [
      { "type": "udp" },
      { "type": "serial", "device": "/dev/ttyS9" },
      { "type": "tcp_server", "port": 9002 }
    ]
  })");
  auto r = TransportFactory::CreateFromFile(path);
  ASSERT_TRUE(static_cast<bool>(r)) << r.error;
  EXPECT_EQ(r.value.size(), 3u);
}

TEST(FactoryJson, FramerAppliedBehaviorally) {
  // JSON 配置带 framer 的 server；客户端裸发分两段的整帧 → accepted 端应交付一条完整帧
  auto path = WriteJson("framer.json", R"({
    "transports": [
      { "type": "tcp_server", "bind_addr": "127.0.0.1", "port": 0,
        "framer": { "header_size": 8, "length_offset": 4, "length_size": 4,
                    "big_endian": true, "max_frame_size": 1024 } }
    ]
  })");
  auto r = TransportFactory::CreateFromFile(path);
  ASSERT_TRUE(static_cast<bool>(r)) << r.error;
  auto server = std::dynamic_pointer_cast<TcpServerImpl>(r.value[0]);
  ASSERT_NE(server, nullptr);
  std::shared_ptr<ITransport> accepted;
  server->OnNewConnection([&](std::shared_ptr<ITransport> c) { accepted = c; });
  ASSERT_TRUE(static_cast<bool>(server->Open()));

  TcpClientConfig cc;
  cc.host = "127.0.0.1";
  cc.port = server->LocalPort();
  cc.auto_reconnect = false;
  auto client = TransportFactory::Create(cc);
  ASSERT_TRUE(static_cast<bool>(client->Open()));
  for (int i = 0; i < 400 && !accepted; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_NE(accepted, nullptr);

  // 13 字节帧（8 header + 5 body），分两次发
  std::vector<uint8_t> frame(8, 0x00);
  frame[7] = 5;
  frame.insert(frame.end(), 5, 0xAB);
  ASSERT_TRUE(static_cast<bool>(client->Send(
      std::vector<uint8_t>(frame.begin(), frame.begin() + 6))));
  ASSERT_TRUE(static_cast<bool>(client->Send(
      std::vector<uint8_t>(frame.begin() + 6, frame.end()))));

  auto m = accepted->Receive(2000);
  ASSERT_TRUE(static_cast<bool>(m));
  EXPECT_EQ(m.value.payload, frame);  // 一条完整帧（透传则是两段）
  client->Close();
  server->Close();
}

// ---------- 错误矩阵 ----------

TEST(FactoryJson, FileNotFound) {
  auto r = TransportFactory::CreateFromFile("/nonexistent/x.json");
  ExpectConfigError(r, {"cannot open"});
}

TEST(FactoryJson, InvalidJsonSyntax) {
  auto path = WriteJson("bad.json", "{ not json !!!");
  ExpectConfigError(TransportFactory::CreateFromFile(path), {"invalid JSON"});
}

TEST(FactoryJson, MissingTransportsArray) {
  auto path = WriteJson("notr.json", R"({"foo": 1})");
  ExpectConfigError(TransportFactory::CreateFromFile(path), {"transports"});
}

TEST(FactoryJson, UnknownType) {
  auto path = WriteJson("unktype.json",
                        R"({"transports":[{"type":"carrier_pigeon"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "unknown type"});
}

TEST(FactoryJson, UnknownFieldStrict) {
  auto path = WriteJson("unkfield.json",
      R"({"transports":[{"type":"tcp_client","host":"h","port":1,"prot":2}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "prot", "unknown field"});
}

TEST(FactoryJson, UnknownFieldInFramer) {
  auto path = WriteJson("unkframer.json", R"({"transports":[
    {"type":"tcp_server","port":1,"framer":{"header_size":8,"big_endia":true}}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"framer", "big_endia", "unknown field"});
}

TEST(FactoryJson, WrongFieldType) {
  auto path = WriteJson("wrongtype.json",
      R"({"transports":[{"type":"tcp_client","host":"h","port":"9000"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "port"});
}

TEST(FactoryJson, InvalidEnum) {
  auto path = WriteJson("badenum.json",
                        R"({"transports":[{"type":"udp","mode":"anycast"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "mode"});
}

TEST(FactoryJson, MissingRequiredField) {
  auto path = WriteJson("noreq.json",
                        R"({"transports":[{"type":"tcp_client","port":1}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "host", "required"});
}

TEST(FactoryJson, OutOfRangeValue) {
  auto path = WriteJson("range.json",
      R"({"transports":[{"type":"tcp_client","host":"h","port":70000}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[0]", "port", "range"});
}

TEST(FactoryJson, SecondEntryBadFailsWhole) {
  auto path = WriteJson("partial.json", R"({"transports":[
    {"type":"tcp_server","port":9003},
    {"type":"nope"}]})");
  ExpectConfigError(TransportFactory::CreateFromFile(path),
                    {"transports[1]", "unknown type"});
}
