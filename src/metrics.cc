#include "metrics.h"

#include <cstdarg>
#include <cstdio>

namespace tautq {

namespace {

double bucket_bound(int i) {
    return 0.001 * static_cast<double>(1u << i); // 1ms * 2^i
}

void append(std::string& out, const char* fmt, ...) {
    char buf[256];
    va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(buf, sizeof buf, fmt, ap);
    va_end(ap);
    out += buf;
}

} // namespace

void Histogram::observe(double seconds) {
    for (int i = 0; i < kBuckets; ++i) {
        if (seconds <= bucket_bound(i)) {
            counts_[i]++;
            sum_ += seconds;
            total_++;
            return;
        }
    }
    counts_[kBuckets]++;
    sum_ += seconds;
    total_++;
}

void Histogram::render(const std::string& name, std::string& out) const {
    std::uint64_t cum = 0;
    for (int i = 0; i < kBuckets; ++i) {
        cum += counts_[i];
        append(out, "%s_bucket{le=\"%.3f\"} %llu\n", name.c_str(), bucket_bound(i),
               static_cast<unsigned long long>(cum));
    }
    cum += counts_[kBuckets];
    append(out, "%s_bucket{le=\"+Inf\"} %llu\n", name.c_str(),
           static_cast<unsigned long long>(cum));
    append(out, "%s_sum %.6f\n", name.c_str(), sum_);
    append(out, "%s_count %llu\n", name.c_str(), static_cast<unsigned long long>(total_));
}

void render_counter(const std::string& name, const std::string& labels, std::uint64_t v,
                    std::string& out) {
    append(out, "%s%s %llu\n", name.c_str(), labels.c_str(), static_cast<unsigned long long>(v));
}

void render_gauge(const std::string& name, const std::string& labels, double v, std::string& out) {
    append(out, "%s%s %.6f\n", name.c_str(), labels.c_str(), v);
}

} // namespace tautq
