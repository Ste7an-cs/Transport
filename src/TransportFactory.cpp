#include "transport/TransportFactory.hpp"

#include <cstdint>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

#include "transport/dds/DdsImpl.hpp"
#include "transport/serial/SerialImpl.hpp"
#include "transport/tcp/TcpClientImpl.hpp"
#include "transport/tcp/TcpServerImpl.hpp"
#include "transport/udp/UdpImpl.hpp"
#ifdef TRANSPORT_HAS_FASTDDS
#include "dds/FastDdsProvider.hpp"  // RegisterFastDdsProvider（src 内部头）
#endif

// TransportFactory.cpp — 统一创建入口（见 TransportFactory.hpp）。
// 类型化 Create = make_shared<对应 *Impl>；DDS 路径显式注册 FastDDS provider
//（静态库下匿名静态注册器可能被链接器裁剪，工厂是确定被引用的符号，在此根治）。

namespace transport {

std::shared_ptr<ITransport> TransportFactory::Create(
    const TcpClientConfig& config) {
  return std::make_shared<TcpClientImpl>(config);
}

std::shared_ptr<ITcpServer> TransportFactory::Create(
    const TcpServerConfig& config) {
  return std::make_shared<TcpServerImpl>(config);
}

std::shared_ptr<ITransport> TransportFactory::Create(
    const UdpConfig& config) {
  return std::make_shared<UdpImpl>(config);
}

std::shared_ptr<IDdsTransport> TransportFactory::Create(
    const DdsConfig& config) {
#ifdef TRANSPORT_HAS_FASTDDS
  RegisterFastDdsProvider();  // 幂等
#endif
  return std::make_shared<DdsImpl>(config);
}

std::shared_ptr<ITransport> TransportFactory::Create(
    const SerialConfig& config) {
  return std::make_shared<SerialImpl>(config);
}

namespace {

using nlohmann::json;

// 解析上下文：记录首个错误（config: + 条目/字段定位）
struct Ctx {
  std::string where;  // 如 "transports[2]" 或 "transports[2].framer"
  std::string err;
  bool ok() const { return err.empty(); }
  void Fail(const std::string& field, const std::string& msg) {
    if (!err.empty()) return;
    err = "config: " + where + (field.empty() ? "" : "." + field) + ": " + msg;
  }
};

// 单个 JSON 对象的字段提取器：类型检查 + 已知键登记 + 未知键报错（严格模式）
class Fields {
 public:
  Fields(const json& obj, Ctx& ctx) : obj_(obj), ctx_(ctx) {}

  void Str(const char* key, std::string* out, bool required = false) {
    auto it = Find(key, required);
    if (it == obj_.end()) return;
    if (!it->is_string()) return ctx_.Fail(key, "expected string");
    *out = it->get<std::string>();
  }

  void StrArray(const char* key, std::vector<std::string>* out,
                bool required = false) {
    auto it = Find(key, required);
    if (it == obj_.end()) return;
    if (!it->is_array()) return ctx_.Fail(key, "expected array of strings");
    out->clear();
    for (const auto& e : *it) {
      if (!e.is_string()) return ctx_.Fail(key, "expected array of strings");
      out->push_back(e.get<std::string>());
    }
  }

  void Bool(const char* key, bool* out) {
    auto it = Find(key, false);
    if (it == obj_.end()) return;
    if (!it->is_boolean()) return ctx_.Fail(key, "expected boolean");
    *out = it->get<bool>();
  }

  template <typename T>
  void Uint(const char* key, T* out, uint64_t max, bool required = false) {
    auto it = Find(key, required);
    if (it == obj_.end()) return;
    if (!it->is_number_unsigned())
      return ctx_.Fail(key, "expected unsigned integer");
    uint64_t v = it->get<uint64_t>();
    if (v > max) return ctx_.Fail(key, "value out of range");
    *out = static_cast<T>(v);
  }

  void Int(const char* key, int* out) {
    auto it = Find(key, false);
    if (it == obj_.end()) return;
    if (!it->is_number_integer()) return ctx_.Fail(key, "expected integer");
    *out = it->get<int>();
  }

  // 子对象；不存在返回 nullptr（已登记为已知键）
  const json* Sub(const char* key) {
    auto it = Find(key, false);
    if (it == obj_.end()) return nullptr;
    if (!it->is_object()) {
      ctx_.Fail(key, "expected object");
      return nullptr;
    }
    return &*it;
  }

  // 全部字段读取完后调用：未知键 → 报错（严格校验）
  void CheckUnknown() {
    for (auto it = obj_.begin(); it != obj_.end(); ++it)
      if (!known_.count(it.key()))
        return ctx_.Fail(it.key(), "unknown field");
  }

 private:
  json::const_iterator Find(const char* key, bool required) {
    known_.insert(key);
    auto it = obj_.find(key);
    if (it == obj_.end() && required)
      ctx_.Fail(key, "required field missing");
    return it;
  }

  const json& obj_;
  Ctx& ctx_;
  std::set<std::string> known_;
};

void ParseFramer(const json& j, Ctx& ctx,
                 std::optional<LengthFieldFramerConfig>* out) {
  LengthFieldFramerConfig f;
  Fields p(j, ctx);
  p.Uint("header_size", &f.header_size, UINT64_MAX);
  p.Uint("length_offset", &f.length_offset, UINT64_MAX);
  p.Uint("length_size", &f.length_size, UINT64_MAX);
  p.Bool("big_endian", &f.big_endian);
  p.Bool("length_includes_header", &f.length_includes_header);
  p.Uint("max_frame_size", &f.max_frame_size, UINT64_MAX);
  p.CheckUnknown();
  if (ctx.ok()) *out = f;
}

void ParseQos(const json& j, Ctx& ctx, DdsQos* out) {
  Fields p(j, ctx);
  std::string rel, dur;
  p.Str("reliability", &rel);
  p.Str("durability", &dur);
  p.Uint("history_depth", &out->history_depth, 0xFFFFFFFFull);
  p.CheckUnknown();
  if (!ctx.ok()) return;
  if (!rel.empty()) {
    if (rel == "reliable") out->reliability = DdsQos::Reliability::kReliable;
    else if (rel == "best_effort") out->reliability = DdsQos::Reliability::kBestEffort;
    else return ctx.Fail("reliability", "invalid enum value: " + rel);
  }
  if (!dur.empty()) {
    if (dur == "volatile") out->durability = DdsQos::Durability::kVolatile;
    else if (dur == "transient_local") out->durability = DdsQos::Durability::kTransientLocal;
    else return ctx.Fail("durability", "invalid enum value: " + dur);
  }
}

// 解析一个 transports[i] 条目；失败时 ctx.err 已置
std::shared_ptr<ITransport> ParseEntry(const json& e, Ctx& ctx) {
  if (!e.is_object()) {
    ctx.Fail("", "expected object");
    return nullptr;
  }
  Fields p(e, ctx);
  std::string type;
  p.Str("type", &type, /*required=*/true);
  if (!ctx.ok()) return nullptr;

  if (type == "tcp_client") {
    TcpClientConfig c;
    p.Str("host", &c.host, true);
    p.Uint("port", &c.port, 0xFFFF, true);
    p.Uint("connect_timeout_ms", &c.connect_timeout_ms, 0xFFFFFFFFull);
    p.Bool("auto_reconnect", &c.auto_reconnect);
    if (const json* f = p.Sub("framer")) {
      Ctx sub{ctx.where + ".framer", ""};
      ParseFramer(*f, sub, &c.framer);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    return TransportFactory::Create(c);
  }

  if (type == "tcp_server") {
    TcpServerConfig c;
    p.Str("bind_addr", &c.bind_addr);
    p.Uint("port", &c.port, 0xFFFF, true);
    p.Int("max_clients", &c.max_clients);
    if (const json* f = p.Sub("framer")) {
      Ctx sub{ctx.where + ".framer", ""};
      ParseFramer(*f, sub, &c.framer);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    return TransportFactory::Create(c);
  }

  if (type == "udp") {
    UdpConfig c;
    std::string mode;
    p.Str("mode", &mode);
    p.Str("local_addr", &c.local_addr);
    p.Uint("local_port", &c.local_port, 0xFFFF);
    p.Str("remote_addr", &c.remote_addr);
    p.Uint("remote_port", &c.remote_port, 0xFFFF);
    p.Str("multicast_group", &c.multicast_group);
    p.Uint("ttl", &c.ttl, 0xFF);
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    if (!mode.empty()) {
      if (mode == "unicast") c.mode = UdpMode::kUnicast;
      else if (mode == "multicast") c.mode = UdpMode::kMulticast;
      else if (mode == "broadcast") c.mode = UdpMode::kBroadcast;
      else { ctx.Fail("mode", "invalid enum value: " + mode); return nullptr; }
    }
    return TransportFactory::Create(c);
  }

  if (type == "dds") {
    DdsConfig c;
    std::string mode;
    p.Str("mode", &mode);
    p.StrArray("topics", &c.topics, true);
    p.Int("domain_id", &c.domain_id);
    p.Str("provider", &c.provider);
    if (const json* q = p.Sub("qos")) {
      Ctx sub{ctx.where + ".qos", ""};
      ParseQos(*q, sub, &c.qos);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    if (!mode.empty()) {
      if (mode == "pubsub") c.mode = DdsMode::kPubSub;
      else if (mode == "reqresp") c.mode = DdsMode::kReqResp;
      else { ctx.Fail("mode", "invalid enum value: " + mode); return nullptr; }
    }
    return TransportFactory::Create(c);
  }

  if (type == "serial") {
    SerialConfig c;
    std::string parity;
    p.Str("device", &c.device, true);
    p.Uint("baud_rate", &c.baud_rate, 0xFFFFFFFFull);
    p.Uint("data_bits", &c.data_bits, 0xFF);
    p.Uint("stop_bits", &c.stop_bits, 0xFF);
    p.Str("parity", &parity);
    if (const json* f = p.Sub("framer")) {
      Ctx sub{ctx.where + ".framer", ""};
      ParseFramer(*f, sub, &c.framer);
      if (!sub.ok()) { ctx.err = sub.err; return nullptr; }
    }
    p.CheckUnknown();
    if (!ctx.ok()) return nullptr;
    if (!parity.empty()) {
      if (parity.size() != 1 ||
          (parity[0] != 'N' && parity[0] != 'E' && parity[0] != 'O')) {
        ctx.Fail("parity", "invalid enum value (expect \"N\"/\"E\"/\"O\"): " + parity);
        return nullptr;
      }
      c.parity = parity[0];
    }
    return TransportFactory::Create(c);
  }

  ctx.Fail("type", "unknown type: " + type);
  return nullptr;
}

}  // namespace

Result<std::vector<std::shared_ptr<ITransport>>>
TransportFactory::CreateFromFile(const std::string& path) {
  using R = Result<std::vector<std::shared_ptr<ITransport>>>;

  std::ifstream in(path);
  if (!in) return R::Fail("config: cannot open file: " + path);

  // 非抛异常解析（框架不抛异常约定）
  json root = json::parse(in, /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (root.is_discarded()) return R::Fail("config: invalid JSON: " + path);
  if (!root.is_object() || !root.contains("transports") ||
      !root["transports"].is_array()) {
    return R::Fail("config: missing top-level \"transports\" array");
  }

  std::vector<std::shared_ptr<ITransport>> out;
  const json& arr = root["transports"];
  for (size_t i = 0; i < arr.size(); ++i) {
    Ctx ctx{"transports[" + std::to_string(i) + "]", ""};
    auto t = ParseEntry(arr[i], ctx);
    if (!ctx.ok()) return R::Fail(ctx.err);  // 任一条目失败 → 整体失败
    out.push_back(std::move(t));
  }
  return R::Success(std::move(out));
}

}  // namespace transport
