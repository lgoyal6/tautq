// tautq-sink: the "customer" webhook receiver — the chaos suite's ground-truth oracle.
// Every accepted delivery is appended as one line to the receipt log:
//
//   <mono_ms> <idempotency-key> <attempt> <path> <bytes> <steady_us>
//
// (steady_us is CLOCK_MONOTONIC in microseconds, comparable with tautq-bench timestamps on
// the same kernel.) --fail-rate injects deterministic (seeded) 500s so retry/backoff paths
// run for real. Receiver faults can also be changed live (demo / stall experiments):
//
//   POST /admin/fault?fail_rate=0.5&stall_ms=200   500s and/or delayed responses
//   GET  /admin/stats                               {"received":N,"failed":N,"stalled":N}
//
//   tautq-sink --port 8081 --out /tmp/sink.log [--fail-rate 0.2] [--seed 42]

#include <chrono>
#include <csignal>
#include <cstdio>
#include <deque>
#include <functional>
#include <random>
#include <string>

#include "http.h"
#include "loop.h"

using namespace tautq;

namespace {
volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) {
    g_stop = 1;
}
} // namespace

int main(int argc, char** argv) {
    std::uint16_t port = 8081;
    std::string out_path = "sink.log";
    double fail_rate = 0.0;
    std::uint64_t seed = 1;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--port") {
            port = static_cast<std::uint16_t>(std::stoul(next()));
        } else if (a == "--out") {
            out_path = next();
        } else if (a == "--fail-rate") {
            fail_rate = std::stod(next());
        } else if (a == "--seed") {
            seed = std::stoull(next());
        } else {
            std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
            return 2;
        }
    }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);

    std::FILE* out = std::fopen(out_path.c_str(), "a");
    if (out == nullptr) {
        std::fprintf(stderr, "cannot open %s\n", out_path.c_str());
        return 1;
    }
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> uni(0.0, 1.0);
    const auto t0 = std::chrono::steady_clock::now();

    Loop loop;
    HttpServer http(loop);
    if (!loop.ok() || !http.listen("0.0.0.0", port)) {
        std::fprintf(stderr, "listen failed on %u\n", port);
        return 1;
    }
    http.route("GET", "/healthz", [](const HttpServer::Request&, HttpServer::Respond re) {
        re(200, "text/plain", "ok\n", {});
    });
    std::uint32_t stall_ms = 0;
    std::uint64_t received = 0;
    std::uint64_t failed = 0;
    std::uint64_t stalled = 0;
    // Stalled deliveries: the receiver holds the request and answers after stall_ms.
    std::deque<std::pair<std::chrono::steady_clock::time_point, std::function<void()>>> held;
    http.route("POST", "/admin/fault", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        if (r.query.count("fail_rate") != 0)
            fail_rate = std::stod(r.query.at("fail_rate"));
        if (r.query.count("stall_ms") != 0)
            stall_ms = static_cast<std::uint32_t>(std::stoul(r.query.at("stall_ms")));
        re(200, "application/json",
           "{\"fail_rate\":" + std::to_string(fail_rate) +
               ",\"stall_ms\":" + std::to_string(stall_ms) + "}\n",
           {});
    });
    http.route("GET", "/admin/stats", [&](const HttpServer::Request&, HttpServer::Respond re) {
        re(200, "application/json",
           "{\"received\":" + std::to_string(received) + ",\"failed\":" + std::to_string(failed) +
               ",\"stalled\":" + std::to_string(stalled) + "}\n",
           {});
    });
    http.route("POST", "/", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        if (stall_ms > 0) {
            ++stalled;
        }
        if (fail_rate > 0.0 && uni(rng) < fail_rate) {
            ++failed;
            auto reply = [re] { re(500, "text/plain", "injected failure\n", {}); };
            if (stall_ms > 0) {
                held.emplace_back(
                    std::chrono::steady_clock::now() + std::chrono::milliseconds(stall_ms), reply);
            } else {
                reply();
            }
            return;
        }
        const auto key =
            r.headers.count("idempotency-key") != 0 ? r.headers.at("idempotency-key") : "-";
        const auto attempt =
            r.headers.count("x-tautq-attempt") != 0 ? r.headers.at("x-tautq-attempt") : "-";
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        const auto abs_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now().time_since_epoch())
                                .count();
        ++received;
        std::fprintf(out, "%lld %s %s %s %zu %lld\n", static_cast<long long>(ms), key.c_str(),
                     attempt.c_str(), r.path.c_str(), r.body.size(),
                     static_cast<long long>(abs_us));
        std::fflush(out);
        if (stall_ms > 0) {
            // Logged on arrival (the receiver HAS the webhook); the 200 is what stalls.
            held.emplace_back(std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(stall_ms),
                              [re] { re(200, "text/plain", "ok\n", {}); });
            return;
        }
        re(200, "text/plain", "ok\n", {});
    });

    std::fprintf(stderr, "[sink] port=%u out=%s fail-rate=%.2f\n", port, out_path.c_str(),
                 fail_rate);
    while (g_stop == 0) {
        if (!loop.run_once(std::chrono::milliseconds(held.empty() ? 50 : 5))) {
            break;
        }
        const auto now = std::chrono::steady_clock::now();
        while (!held.empty() && held.front().first <= now) {
            auto fn = std::move(held.front().second);
            held.pop_front();
            fn();
        }
    }
    std::fclose(out);
    return 0;
}
