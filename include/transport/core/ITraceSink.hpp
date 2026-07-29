#pragma once

// ITraceSink.hpp — 可插拔结构化 trace。层中立:放 transport 根,供任意层(当前仅
// InteractionEngine)注入。事件为零分配视图结构;sink 实现须线程安全。

#include <iostream>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

namespace transport {

enum class TraceLevel { kTrace, kDebug, kInfo, kWarn, kError };

inline constexpr int  kNoTag = -9999;  // TraceEvent.tag 哨兵:无判别符
inline constexpr long kNoNum = -1;     // size/attempt 哨兵:无该数值

// 轻量"视图"事件:string_view 指向调用点已存在的数据,构造零分配。
// sink 若需在 OnTrace 返回后留存,须自行拷贝(见 CapturingTraceSink)。
struct TraceEvent {
  TraceLevel       level;
  std::string_view category;   // 静态字面量:"send"/"recv"/"dispatch"/"timeout"/...
  std::string_view message;    // 短静态子原因(可空),如 "match-terminal"
  std::string_view key;        // 相关键(可空)
  std::string_view endpoint;   // endpoint / from / route 名(可空)
  std::string_view error;      // 错误串(可空)
  int  tag     = kNoTag;       // FrameTag
  long size    = kNoNum;       // 字节数 / 计数(按 category 解释)
  int  attempt = -1;           // 重发第几次(-1=无)
};

// 可能被 io / worker / 调用方线程并发调用 → 实现必须线程安全。
class ITraceSink {
 public:
  virtual ~ITraceSink() = default;
  virtual void OnTrace(const TraceEvent& ev) = 0;
};

// ---- 内置 sink:格式化到 ostream(默认 std::cerr) ----
class OstreamTraceSink : public ITraceSink {
 public:
  explicit OstreamTraceSink(std::ostream& os = std::cerr, TraceLevel min = TraceLevel::kDebug)
      : os_(os), min_(min) {}

  OstreamTraceSink(const OstreamTraceSink&) = delete;
  OstreamTraceSink& operator=(const OstreamTraceSink&) = delete;

  void OnTrace(const TraceEvent& ev) override {
    if (static_cast<int>(ev.level) < static_cast<int>(min_)) return;
    std::lock_guard<std::mutex> lk(mu_);
    os_ << '[' << Letter(ev.level) << "] " << ev.category;
    if (!ev.message.empty())  os_ << ' ' << ev.message;
    if (!ev.key.empty())      os_ << " key=" << ev.key;
    if (!ev.endpoint.empty()) os_ << " ep=" << ev.endpoint;
    if (ev.tag != kNoTag)     os_ << " tag=" << ev.tag;
    if (ev.size != kNoNum)    os_ << " size=" << ev.size;
    if (ev.attempt >= 0)      os_ << " attempt=" << ev.attempt;
    if (!ev.error.empty())    os_ << " err=" << ev.error;
    os_ << '\n';
  }

 private:
  static char Letter(TraceLevel l) {
    switch (l) {
      case TraceLevel::kTrace: return 'T';
      case TraceLevel::kDebug: return 'D';
      case TraceLevel::kInfo:  return 'I';
      case TraceLevel::kWarn:  return 'W';
      case TraceLevel::kError: return 'E';
    }
    return '?';
  }
  std::ostream& os_;
  TraceLevel min_;
  std::mutex mu_;
};

// ---- 内置 sink:深拷贝留存,供测试/内省 ----
class CapturingTraceSink : public ITraceSink {
 public:
  struct Record {
    TraceLevel level;
    std::string category, message, key, endpoint, error;
    int tag; long size; int attempt;
  };

  CapturingTraceSink() = default;
  CapturingTraceSink(const CapturingTraceSink&) = delete;
  CapturingTraceSink& operator=(const CapturingTraceSink&) = delete;

  void OnTrace(const TraceEvent& ev) override {
    std::lock_guard<std::mutex> lk(mu_);
    records_.push_back(Record{ev.level, std::string(ev.category), std::string(ev.message),
                              std::string(ev.key), std::string(ev.endpoint), std::string(ev.error),
                              ev.tag, ev.size, ev.attempt});
  }

  std::vector<Record> Records() const {
    std::lock_guard<std::mutex> lk(mu_);
    return records_;
  }
  void Clear() {
    std::lock_guard<std::mutex> lk(mu_);
    records_.clear();
  }

 private:
  mutable std::mutex mu_;
  std::vector<Record> records_;
};

}  // namespace transport
