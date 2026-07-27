// tautq-sink: the "customer" webhook receiver — the chaos suite's ground-truth oracle.
// Every accepted delivery is appended as one line to the receipt log:
//
//   <mono_ms> <idempotency-key> <attempt> <path> <bytes>
//
// --fail-rate injects deterministic (seeded) 500s so retry/backoff paths run for real.
//
//   tautq-sink --port 8081 --out /tmp/sink.log [--fail-rate 0.2] [--seed 42]

#include <chrono>
#include <csignal>
#include <cstdio>
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
    http.route("POST", "/", [&](const HttpServer::Request& r, HttpServer::Respond re) {
        if (fail_rate > 0.0 && uni(rng) < fail_rate) {
            re(500, "text/plain", "injected failure\n", {});
            return;
        }
        const auto key =
            r.headers.count("idempotency-key") != 0 ? r.headers.at("idempotency-key") : "-";
        const auto attempt =
            r.headers.count("x-tautq-attempt") != 0 ? r.headers.at("x-tautq-attempt") : "-";
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        std::fprintf(out, "%lld %s %s %s %zu\n", static_cast<long long>(ms), key.c_str(),
                     attempt.c_str(), r.path.c_str(), r.body.size());
        std::fflush(out);
        re(200, "text/plain", "ok\n", {});
    });

    std::fprintf(stderr, "[sink] port=%u out=%s fail-rate=%.2f\n", port, out_path.c_str(),
                 fail_rate);
    while (g_stop == 0) {
        if (!loop.run_once(std::chrono::milliseconds(50))) {
            break;
        }
    }
    std::fclose(out);
    return 0;
}
