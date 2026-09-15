#include "PerfOptions.hpp"

#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace perf {
namespace {

/// 把 `a,b,c` 切成若干段;空段直接丢弃。
std::vector<std::string> Split(const std::string& text, char sep) {
  std::vector<std::string> parts;
  std::string item;
  std::istringstream stream(text);
  while (std::getline(stream, item, sep)) {
    if (!item.empty()) {
      parts.push_back(item);
    }
  }
  return parts;
}

bool ParseInt(const std::string& text, int* out) {
  char* end = nullptr;
  const long value = std::strtol(text.c_str(), &end, 10);
  if (end == text.c_str() || (end != nullptr && *end != '\0')) {
    return false;
  }
  *out = static_cast<int>(value);
  return true;
}

bool ParseMode(const std::string& text, Mode* out) {
  if (text == "send") { *out = Mode::kSend; return true; }
  if (text == "response") { *out = Mode::kResponse; return true; }
  if (text == "result") { *out = Mode::kResult; return true; }
  if (text == "resultdirect") { *out = Mode::kResultDirect; return true; }
  if (text == "pubsub") { *out = Mode::kPubSub; return true; }
  if (text == "reqresp") { *out = Mode::kReqResp; return true; }
  return false;
}

}  // namespace

const char* ToString(Role role) {
  return role == Role::kClient ? "client" : "server";
}

const char* ToString(MediumKind medium) {
  switch (medium) {
    case MediumKind::kTcp: return "tcp";
    case MediumKind::kUdp: return "udp";
    case MediumKind::kDds: return "dds";
  }
  return "?";
}

const char* ToString(Suite suite) {
  return suite == Suite::kLatency ? "latency" : "throughput";
}

const char* ToString(Mode mode) {
  switch (mode) {
    case Mode::kSend: return "send";
    case Mode::kResponse: return "response";
    case Mode::kResult: return "result";
    case Mode::kResultDirect: return "resultdirect";
    case Mode::kPubSub: return "pubsub";
    case Mode::kReqResp: return "reqresp";
  }
  return "?";
}

bool IsHalfRoundTrip(Mode mode) {
  return mode == Mode::kSend || mode == Mode::kPubSub;
}

std::vector<Mode> DefaultModes(MediumKind medium) {
  if (medium == MediumKind::kDds) {
    return {Mode::kPubSub, Mode::kReqResp};
  }
  return {Mode::kSend, Mode::kResponse, Mode::kResult, Mode::kResultDirect};
}

bool ModeAppliesTo(MediumKind medium, Mode mode) {
  for (Mode candidate : DefaultModes(medium)) {
    if (candidate == mode) {
      return true;
    }
  }
  return false;
}

std::vector<std::size_t> DefaultPayloads() {
  return {16, 256, 1024, 16384, 63488};
}

void PrintUsage() {
  std::printf(
      "transport_perf —— 性能基准测试工具(ADR-0018)。只报数,不做 pass/fail(D6)。\n"
      "\n"
      "用法:\n"
      "  transport_perf --role client|server --medium tcp|udp|dds"
      " --suite latency|throughput [选项...]\n"
      "\n"
      "通用:\n"
      "  --role <client|server>     角色。client 是发起端,打印最终表格。\n"
      "  --medium <tcp|udp|dds>     介质。**serial 本轮不实现,传入即报错退出(D5)**。\n"
      "  --suite <latency|throughput>  测哪一套。\n"
      "  --mode <m[,m...]>          被测面;缺省即该介质的全部模式。\n"
      "                             tcp/udp: send,response,result,resultdirect\n"
      "                             dds    : pubsub,reqresp\n"
      "  --payloads <n[,n...]>      payload 档位(字节)。缺省 16,256,1024,16384,63488(D8)。\n"
      "  --warmup <n>               每档预热条数,不计入统计。缺省 100(D9)。\n"
      "  --timeout <ms>             单次交互的等待时限。缺省 2000。\n"
      "  --attempts <n>             单次交互的总发送次数(含首发)。缺省 5。\n"
      "\n"
      "时延 suite:\n"
      "  --samples <n>              每档计入统计的样本数。缺省 1000。\n"
      "\n"
      "吞吐 suite:\n"
      "  --demand <n>               每个 burst 的条数。缺省 1000。\n"
      "  --recovery <ms>            burst 之间的歇息时长。缺省 5。\n"
      "  --time <ms>                每档矩阵的测试时长。缺省 5000。\n"
      "\n"
      "寻址:\n"
      "  --host <ip>                TCP/UDP 客户端的对端地址。缺省 127.0.0.1。\n"
      "  --bind <ip>                TCP/UDP 服务端的本地绑定地址。缺省 0.0.0.0。\n"
      "  --port <n>                 TCP 服务端监听端口 / UDP 服务端本地端口。缺省 45678。\n"
      "  --local-port <n>           UDP 客户端本地端口。缺省 0(由 OS 分配)。\n"
      "  --domain <n>               DDS domain 编号。缺省 0。\n"
      "  --silence <ms>             TCP/UDP 的 silence_timeout。缺省 5000。\n"
      "  --help                     打印本帮助。\n"
      "\n"
      "本机自测(同机两个进程):\n"
      "  transport_perf --role server --medium tcp --suite latency &\n"
      "  transport_perf --role client --medium tcp --suite latency --host 127.0.0.1\n");
}

ParseOutcome ParseOptions(int argc, char** argv, PerfOptions* out) {
  ParseOutcome outcome;
  bool role_seen = false;
  bool medium_seen = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      outcome.help = true;
      outcome.ok = true;
      return outcome;
    }
    if (arg.rfind("--", 0) != 0) {
      outcome.error = "无法识别的参数:" + arg;
      return outcome;
    }

    std::string key = arg;
    std::string value;
    bool has_value = false;
    const std::size_t eq = arg.find('=');
    if (eq != std::string::npos) {
      key = arg.substr(0, eq);
      value = arg.substr(eq + 1);
      has_value = true;
    } else if (i + 1 < argc) {
      value = argv[i + 1];
      has_value = true;
    }

    auto need_value = [&](const char* name) {
      if (!has_value) {
        outcome.error = std::string(name) + " 缺少取值";
        return false;
      }
      if (eq == std::string::npos) {
        ++i;  // 消费掉下一个 argv。
      }
      return true;
    };
    auto need_int = [&](const char* name, int* slot) {
      if (!need_value(name)) {
        return false;
      }
      if (!ParseInt(value, slot)) {
        outcome.error = std::string(name) + " 取值不是整数:" + value;
        return false;
      }
      return true;
    };

    if (key == "--role") {
      if (!need_value("--role")) { return outcome; }
      if (value == "client") { out->role = Role::kClient; }
      else if (value == "server") { out->role = Role::kServer; }
      else { outcome.error = "--role 只能是 client 或 server:" + value; return outcome; }
      role_seen = true;
    } else if (key == "--medium") {
      if (!need_value("--medium")) { return outcome; }
      if (value == "tcp") { out->medium = MediumKind::kTcp; }
      else if (value == "udp") { out->medium = MediumKind::kUdp; }
      else if (value == "dds") { out->medium = MediumKind::kDds; }
      else if (value == "serial") {
        // **D5**:串口本轮不测,传入即报错退出——不留一个跑不通的分支冒充已支持。
        outcome.error =
            "--medium serial 本轮不实现(ADR-0018 D5:串口需实机,本轮不测)。"
            "介质维度已做成可扩展形状,将来接入只需补一个传输装配分支与其寻址参数。";
        return outcome;
      } else {
        outcome.error = "--medium 只能是 tcp / udp / dds:" + value;
        return outcome;
      }
      medium_seen = true;
    } else if (key == "--suite") {
      if (!need_value("--suite")) { return outcome; }
      if (value == "latency") { out->suite = Suite::kLatency; }
      else if (value == "throughput") { out->suite = Suite::kThroughput; }
      else { outcome.error = "--suite 只能是 latency 或 throughput:" + value; return outcome; }
    } else if (key == "--mode") {
      if (!need_value("--mode")) { return outcome; }
      out->modes.clear();
      for (const std::string& item : Split(value, ',')) {
        Mode mode = Mode::kSend;
        if (!ParseMode(item, &mode)) {
          outcome.error = "无法识别的 --mode:" + item;
          return outcome;
        }
        out->modes.push_back(mode);
      }
      if (out->modes.empty()) {
        outcome.error = "--mode 取值为空";
        return outcome;
      }
    } else if (key == "--payloads") {
      if (!need_value("--payloads")) { return outcome; }
      out->payloads.clear();
      for (const std::string& item : Split(value, ',')) {
        int bytes = 0;
        if (!ParseInt(item, &bytes) || bytes < 4 || bytes > 63488) {
          outcome.error =
              "--payloads 档位须在 [4, 63488] 内(D8:frm_len 是 uint16):" + item;
          return outcome;
        }
        out->payloads.push_back(static_cast<std::size_t>(bytes));
      }
      if (out->payloads.empty()) {
        outcome.error = "--payloads 取值为空";
        return outcome;
      }
    } else if (key == "--samples") {
      if (!need_int("--samples", &out->samples)) { return outcome; }
    } else if (key == "--demand") {
      if (!need_int("--demand", &out->demand)) { return outcome; }
    } else if (key == "--recovery") {
      if (!need_int("--recovery", &out->recovery_ms)) { return outcome; }
    } else if (key == "--time") {
      if (!need_int("--time", &out->test_time_ms)) { return outcome; }
    } else if (key == "--warmup") {
      if (!need_int("--warmup", &out->warmup)) { return outcome; }
    } else if (key == "--timeout") {
      if (!need_int("--timeout", &out->timeout_ms)) { return outcome; }
    } else if (key == "--attempts") {
      if (!need_int("--attempts", &out->attempts)) { return outcome; }
    } else if (key == "--domain") {
      if (!need_int("--domain", &out->domain)) { return outcome; }
    } else if (key == "--silence") {
      if (!need_int("--silence", &out->silence_ms)) { return outcome; }
    } else if (key == "--port" || key == "--local-port") {
      int port = 0;
      if (!need_int(key.c_str(), &port)) { return outcome; }
      if (port < 0 || port > 65535) {
        outcome.error = key + " 超出端口范围:" + value;
        return outcome;
      }
      if (key == "--port") {
        out->port = static_cast<std::uint16_t>(port);
      } else {
        out->local_port = static_cast<std::uint16_t>(port);
      }
    } else if (key == "--host") {
      if (!need_value("--host")) { return outcome; }
      out->host = value;
    } else if (key == "--bind") {
      if (!need_value("--bind")) { return outcome; }
      out->bind_host = value;
    } else {
      outcome.error = "无法识别的选项:" + key;
      return outcome;
    }
  }

  if (!role_seen || !medium_seen) {
    outcome.error = "--role 与 --medium 都是必填项(--help 看用法)";
    return outcome;
  }

  if (out->modes.empty()) {
    out->modes = DefaultModes(out->medium);
  }
  for (Mode mode : out->modes) {
    if (!ModeAppliesTo(out->medium, mode)) {
      outcome.error = std::string("模式 ") + ToString(mode) + " 不属于介质 " +
                      ToString(out->medium) + " 的被测面(D5 覆盖矩阵)";
      return outcome;
    }
  }
  if (out->samples <= 0 || out->demand <= 0 || out->test_time_ms <= 0 ||
      out->timeout_ms <= 0 || out->attempts < 1 || out->warmup < 0 ||
      out->recovery_ms < 0 || out->silence_ms <= 0) {
    outcome.error = "数值选项取值非法(样本/条数/时长须为正,重发次数须 ≥ 1)";
    return outcome;
  }

  outcome.ok = true;
  return outcome;
}

}  // namespace perf
