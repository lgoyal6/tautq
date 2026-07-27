#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace tautq {

// Hand-rolled Prometheus primitives (D8: no client library). Only what the dashboard
// needs: monotonically increasing counters, point-in-time gauges (rendered by the caller
// at scrape time), and a fixed log-scale histogram whose quantiles Grafana computes via
// histogram_quantile().

// Log-scale latency histogram: buckets at 1ms * 2^i up to ~65s, plus +Inf.
class Histogram {
  public:
    static constexpr int kBuckets = 17;

    void observe(double seconds);
    // Renders name_bucket/name_sum/name_count in Prometheus text format.
    void render(const std::string& name, std::string& out) const;

  private:
    std::uint64_t counts_[kBuckets + 1] = {}; // [kBuckets] = +Inf
    double sum_ = 0;
    std::uint64_t total_ = 0;
};

// Rendering helpers for scrape-time values.
void render_counter(const std::string& name, const std::string& labels, std::uint64_t v,
                    std::string& out);
void render_gauge(const std::string& name, const std::string& labels, double v, std::string& out);

} // namespace tautq
