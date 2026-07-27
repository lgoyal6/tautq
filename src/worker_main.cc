// tautq-worker: pulls jobs from its local node over the HTTP lease/ack API and delivers
// them — POST body to the job's URL with the Idempotency-Key header, 2xx = success. A
// separate process on purpose: chaos can SIGKILL it independently, and any external worker
// speaking the same three endpoints would behave identically.
//
//   tautq-worker --node http://127.0.0.1:8080 --worker-id 7 [--poll-ms 100]
//                [--deliver-timeout-ms 5000]

#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "http_client.h"

using namespace std::chrono_literals;

namespace {
volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) {
    g_stop = 1;
}
} // namespace

int main(int argc, char** argv) {
    std::string node = "http://127.0.0.1:8080";
    std::string worker_id = "1";
    int poll_ms = 100;
    int deliver_timeout_ms = 5000;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        const auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
        if (a == "--node") {
            node = next();
        } else if (a == "--worker-id") {
            worker_id = next();
        } else if (a == "--poll-ms") {
            poll_ms = std::atoi(next().c_str());
        } else if (a == "--deliver-timeout-ms") {
            deliver_timeout_ms = std::atoi(next().c_str());
        } else {
            std::fprintf(stderr, "unknown arg: %s\n", a.c_str());
            return 2;
        }
    }
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGPIPE, SIG_IGN);
    std::fprintf(stderr, "[worker %s] pulling from %s\n", worker_id.c_str(), node.c_str());

    while (g_stop == 0) {
        const auto lease =
            tautq::http_fetch("POST", node + "/v1/lease?worker=" + worker_id, {}, "", 2000ms);
        if (lease.code != 200) {
            std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
            continue;
        }
        const auto hdr = [&](const char* k) -> std::string {
            auto it = lease.headers.find(k);
            return it == lease.headers.end() ? "" : it->second;
        };
        const std::string id = hdr("x-tautq-id");
        const std::string url = hdr("x-tautq-url");
        const std::string key = hdr("x-tautq-key");
        const std::string epoch = hdr("x-tautq-epoch");
        const std::string seq = hdr("x-tautq-seq");
        if (id.empty() || url.empty()) {
            continue;
        }

        const auto delivery = tautq::http_fetch(
            "POST", url, {{"Idempotency-Key", key}, {"X-Tautq-Attempt", hdr("x-tautq-attempt")}},
            lease.body, std::chrono::milliseconds(deliver_timeout_ms));
        const bool ok = delivery.code >= 200 && delivery.code < 300;
        std::fprintf(stderr, "[worker %s] job=%s attempt=%s deliver=%d -> %s\n", worker_id.c_str(),
                     id.c_str(), hdr("x-tautq-attempt").c_str(), delivery.code, ok ? "ok" : "fail");

        // Report the outcome; 503 means the completion quorum is still forming — keep
        // retrying, the ack path is idempotent.
        const std::string ack_url = node + "/v1/ack?id=" + id + "&epoch=" + epoch + "&seq=" + seq +
                                    "&result=" + (ok ? "ok" : "fail");
        for (int attempt = 0; attempt < 50 && g_stop == 0; ++attempt) {
            const auto ack = tautq::http_fetch("POST", ack_url, {}, "", 2000ms);
            if (ack.code != 0 && ack.code != 503) {
                break;
            }
            std::this_thread::sleep_for(200ms);
        }
    }
    return 0;
}
