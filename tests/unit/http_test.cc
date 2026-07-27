#include "http.h"

#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "http_client.h"
#include "loop.h"
#include "metrics.h"

using namespace std::chrono_literals;

namespace {

// Server on the main thread's loop; blocking client on a helper thread.
struct HttpFixture : ::testing::Test {
    tautq::Loop loop;
    tautq::HttpServer server{loop};

    void SetUp() override {
        ASSERT_TRUE(loop.ok());
        ASSERT_TRUE(server.listen("127.0.0.1", 0)); // ephemeral port
    }

    tautq::HttpResponse fetch(const std::string& method, const std::string& path,
                              const std::string& body = "",
                              const std::vector<std::pair<std::string, std::string>>& hdrs = {}) {
        tautq::HttpResponse resp;
        std::atomic<bool> done{false};
        std::thread t([&] {
            resp = tautq::http_fetch(method,
                                     "http://127.0.0.1:" + std::to_string(server.port()) + path,
                                     hdrs, body, 5000ms);
            done = true;
        });
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!done && std::chrono::steady_clock::now() < deadline) {
            loop.run_once(5ms);
        }
        t.join();
        return resp;
    }
};

TEST_F(HttpFixture, RoutesQueryHeadersBodyAndExtraHeaders) {
    server.route("POST", "/v1/echo",
                 [](const tautq::HttpServer::Request& r, tautq::HttpServer::Respond re) {
                     EXPECT_EQ(r.query.at("a"), "1");
                     EXPECT_EQ(r.query.at("msg"), "hello world"); // url-decoded
                     EXPECT_EQ(r.headers.at("idempotency-key"), "k-1");
                     re(200, "text/plain", "echo:" + r.body, {{"X-Extra", "yes"}});
                 });
    const auto resp =
        fetch("POST", "/v1/echo?a=1&msg=hello%20world", "payload", {{"Idempotency-Key", "k-1"}});
    ASSERT_EQ(resp.code, 200);
    EXPECT_EQ(resp.body, "echo:payload");
    EXPECT_EQ(resp.headers.at("x-extra"), "yes");
}

TEST_F(HttpFixture, UnknownRouteIs404AndPrefixRoutingWorks) {
    server.route("GET", "/v1/jobs/",
                 [](const tautq::HttpServer::Request& r, tautq::HttpServer::Respond re) {
                     re(200, "text/plain", "job:" + r.path.substr(9), {});
                 });
    EXPECT_EQ(fetch("GET", "/nope").code, 404);
    const auto resp = fetch("GET", "/v1/jobs/abc123");
    ASSERT_EQ(resp.code, 200);
    EXPECT_EQ(resp.body, "job:abc123");
}

TEST_F(HttpFixture, DeferredResponseArrivesWhenHandlerCompletesLater) {
    // Handler parks the Respond; a later loop tick fires it — the quorum-gated pattern.
    tautq::HttpServer::Respond parked;
    server.route("POST", "/v1/slow",
                 [&](const tautq::HttpServer::Request&, tautq::HttpServer::Respond re) {
                     parked = re; // do NOT respond yet
                 });
    tautq::HttpResponse resp;
    std::atomic<bool> done{false};
    std::thread t([&] {
        resp = tautq::http_fetch("POST",
                                 "http://127.0.0.1:" + std::to_string(server.port()) + "/v1/slow",
                                 {}, "", 5000ms);
        done = true;
    });
    // Run until the handler parked the respond, then a few more ticks, then answer.
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!parked && std::chrono::steady_clock::now() < deadline) {
        loop.run_once(5ms);
    }
    ASSERT_TRUE(static_cast<bool>(parked));
    for (int i = 0; i < 10; ++i) {
        loop.run_once(5ms);
    }
    parked(200, "text/plain", "late\n", {});
    while (!done && std::chrono::steady_clock::now() < deadline) {
        loop.run_once(5ms);
    }
    t.join();
    ASSERT_EQ(resp.code, 200);
    EXPECT_EQ(resp.body, "late\n");
}

TEST(Metrics, HistogramRendersCumulativeBuckets) {
    tautq::Histogram h;
    h.observe(0.0005); // below first bound (1ms)
    h.observe(0.003);
    h.observe(0.003);
    h.observe(1000.0); // +Inf
    std::string out;
    h.render("t", out);
    EXPECT_NE(out.find("t_bucket{le=\"0.001\"} 1"), std::string::npos);
    EXPECT_NE(out.find("t_bucket{le=\"0.004\"} 3"), std::string::npos);
    EXPECT_NE(out.find("t_bucket{le=\"+Inf\"} 4"), std::string::npos);
    EXPECT_NE(out.find("t_count 4"), std::string::npos);
}

} // namespace
