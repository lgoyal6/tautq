// tautq-loadgen: open-loop submit generator + cluster-side latency reader. Submits at a
// fixed target rate for a fixed duration (open loop: a slow cluster does NOT slow the
// offered load — that is what makes the p99-vs-rate knee honest), then scrapes every
// node's /metrics before and after and derives submit->DONE p50/p95/p99 from the
// histogram DELTAS, so only jobs of this run count.
//
//   tautq-loadgen --gateways http://ip:8080,... --rate 200 --duration-s 30 \
//                 --sink-url http://ip:8090/hook [--threads 8] [--prefix lg]
//
// Output (one CSV line to stdout):
//   rate,offered,accepted,errors,completed,p50_ms,p95_ms,p99_ms

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "http_client.h"

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

struct Hist {
    std::map<double, std::uint64_t> buckets; // le -> cumulative count
    std::uint64_t count = 0;
    std::uint64_t completions = 0;
};

// Parse the tautq histogram + completion counter out of one /metrics payload.
Hist scrape(const std::string& base) {
    Hist h;
    const auto r = tautq::http_fetch("GET", base + "/metrics", {}, "", 3000ms);
    if (r.code != 200) {
        return h;
    }
    std::size_t pos = 0;
    while (pos < r.body.size()) {
        std::size_t eol = r.body.find('\n', pos);
        if (eol == std::string::npos) {
            eol = r.body.size();
        }
        const std::string line = r.body.substr(pos, eol - pos);
        pos = eol + 1;
        double le = 0;
        std::uint64_t v = 0;
        char lebuf[32];
        if (std::sscanf(line.c_str(), "tautq_job_latency_seconds_bucket{le=\"%31[^\"]\"} %" PRIu64,
                        lebuf, &v) == 2) {
            le = std::strcmp(lebuf, "+Inf") == 0 ? 1e18 : std::atof(lebuf);
            h.buckets[le] += v;
        } else if (std::sscanf(line.c_str(), "tautq_job_latency_seconds_count %" PRIu64, &v) == 1) {
            h.count += v;
        } else if (std::sscanf(line.c_str(), "tautq_completions_total{result=\"ok\"} %" PRIu64,
                               &v) == 1) {
            h.completions += v;
        }
    }
    return h;
}

Hist sum_scrapes(const std::vector<std::string>& gateways) {
    Hist total;
    for (const auto& g : gateways) {
        const Hist h = scrape(g);
        for (const auto& [le, c] : h.buckets) {
            total.buckets[le] += c;
        }
        total.count += h.count;
        total.completions += h.completions;
    }
    return total;
}

// Quantile from a cumulative-bucket delta, with linear interpolation inside the bucket
// (same estimate Prometheus's histogram_quantile makes).
double quantile_ms(const std::map<double, std::uint64_t>& delta, std::uint64_t total, double q) {
    if (total == 0) {
        return -1;
    }
    const double target = q * static_cast<double>(total);
    double prev_le = 0;
    std::uint64_t prev_cum = 0;
    for (const auto& [le, cum] : delta) {
        if (static_cast<double>(cum) >= target) {
            const auto in_bucket = static_cast<double>(cum - prev_cum);
            const double frac =
                in_bucket <= 0 ? 1.0 : (target - static_cast<double>(prev_cum)) / in_bucket;
            const double hi = le > 1e17 ? prev_le * 2 : le; // +Inf: report beyond-last-bound
            return (prev_le + (hi - prev_le) * frac) * 1000.0;
        }
        prev_le = le;
        prev_cum = cum;
    }
    return prev_le * 1000.0;
}

} // namespace

int main(int argc, char** argv) {
    std::string gateways_s;
    std::string sink_url;
    std::string prefix = "lg";
    int rate = 100;
    int duration_s = 30;
    int threads = 8;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--gateways") {
            gateways_s = next();
        } else if (a == "--sink-url") {
            sink_url = next();
        } else if (a == "--rate") {
            rate = std::atoi(next().c_str());
        } else if (a == "--duration-s") {
            duration_s = std::atoi(next().c_str());
        } else if (a == "--threads") {
            threads = std::atoi(next().c_str());
        } else if (a == "--prefix") {
            prefix = next();
        } else {
            std::fprintf(stderr, "unknown arg %s\n", a.c_str());
            return 2;
        }
    }
    std::vector<std::string> gateways;
    std::size_t pos = 0;
    while (pos < gateways_s.size()) {
        std::size_t comma = gateways_s.find(',', pos);
        if (comma == std::string::npos) {
            comma = gateways_s.size();
        }
        gateways.push_back(gateways_s.substr(pos, comma - pos));
        pos = comma + 1;
    }
    if (gateways.empty() || sink_url.empty()) {
        std::fprintf(stderr, "usage: tautq-loadgen --gateways URL,... --sink-url URL "
                             "--rate N --duration-s S\n");
        return 2;
    }

    const Hist before = sum_scrapes(gateways);

    std::atomic<std::uint64_t> offered{0};
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> errors{0};
    const auto t0 = Clock::now();
    const auto t_end = t0 + std::chrono::seconds(duration_s);
    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t) {
        pool.emplace_back([&, t] {
            // Each thread paces its slice of the target rate on the shared clock, so the
            // offered load is independent of response latency (open loop).
            const double per_thread = static_cast<double>(rate) / threads;
            std::uint64_t n = 0;
            while (Clock::now() < t_end) {
                const auto due =
                    t0 + std::chrono::duration_cast<Clock::duration>(
                             std::chrono::duration<double>(static_cast<double>(n) / per_thread));
                std::this_thread::sleep_until(due);
                if (Clock::now() >= t_end) {
                    break;
                }
                const std::uint64_t seq = offered.fetch_add(1);
                const std::string& gw = gateways[seq % gateways.size()];
                const auto r =
                    tautq::http_fetch("POST",
                                      gw + "/v1/jobs?key=" + prefix + "-" + std::to_string(t) +
                                          "-" + std::to_string(n) + "&url=" + sink_url,
                                      {}, "x", 5000ms);
                if (r.code == 200 || r.code == 201) {
                    accepted.fetch_add(1);
                } else {
                    errors.fetch_add(1);
                }
                ++n;
            }
        });
    }
    for (auto& th : pool) {
        th.join();
    }

    // Let in-flight jobs complete before the closing scrape.
    const auto settle_deadline = Clock::now() + 30s;
    Hist after = sum_scrapes(gateways);
    while (Clock::now() < settle_deadline && after.count - before.count < accepted.load()) {
        std::this_thread::sleep_for(1s);
        after = sum_scrapes(gateways);
    }

    std::map<double, std::uint64_t> delta;
    for (const auto& [le, c] : after.buckets) {
        const auto bit = before.buckets.find(le);
        delta[le] = c - (bit == before.buckets.end() ? 0 : bit->second);
    }
    const std::uint64_t completed = after.count - before.count;
    std::printf("%d,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.1f,%.1f,%.1f\n", rate,
                offered.load(), accepted.load(), errors.load(), completed,
                quantile_ms(delta, completed, 0.50), quantile_ms(delta, completed, 0.95),
                quantile_ms(delta, completed, 0.99));
    return 0;
}
