#include "rpc.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include "taut/sim_net.h"

using namespace std::chrono_literals;

namespace {

taut::Endpoint ep(std::uint16_t port) {
    taut::Endpoint e{};
    e.addr_be = 1;
    e.port_be = port;
    return e;
}

std::vector<std::byte> bytes(std::initializer_list<int> v) {
    std::vector<std::byte> out;
    for (int x : v) {
        out.push_back(std::byte{static_cast<unsigned char>(x)});
    }
    return out;
}

// Two RpcNodes over one SimNet with a virtual clock; step() is one scheduler round.
struct Pair {
    taut::SimNet net;
    taut::Endpoint ea = ep(9000);
    taut::Endpoint eb = ep(9001);
    std::unique_ptr<tautq::RpcNode> a;
    std::unique_ptr<tautq::RpcNode> b;

    explicit Pair(std::uint64_t seed, taut::Impairments imp) : net(seed, imp) {
        a = std::make_unique<tautq::RpcNode>(net.endpoint(ea), ea, taut::Config{}, 0xA0001);
        b = std::make_unique<tautq::RpcNode>(net.endpoint(eb), eb, taut::Config{}, 0xB0001);
    }

    void step(std::chrono::milliseconds dt = 5ms) {
        net.advance(dt);
        if (a) {
            a->poll();
        }
        if (b) {
            b->poll();
        }
        if (a) {
            a->tick();
        }
        if (b) {
            b->tick();
        }
    }
};

// Echo handler used as the standard test service.
void serve_echo(tautq::RpcNode& n) {
    n.on_request(tautq::Method::Ping, [&n](const tautq::RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        n.respond(ctx, 0, body);
    });
}

TEST(Rpc, ManyCallsCompleteOverLossyLink) {
    Pair p(7, taut::Impairments{.loss = 0.05, .delay = 5ms, .jitter = 2ms});
    serve_echo(*p.b);

    const int kCalls = 40;
    int done = 0;
    int ok = 0;
    for (int i = 0; i < kCalls; ++i) {
        const auto body = bytes({i, i + 1, i + 2});
        p.a->call(p.eb, tautq::Method::Ping, body, 5000ms,
                  [&, body](std::uint32_t st, taut::ByteSpan resp) {
                      ++done;
                      if (st == tautq::status::kOk && resp.size() == body.size() &&
                          std::equal(resp.begin(), resp.end(), body.begin())) {
                          ++ok;
                      }
                  });
    }
    for (int i = 0; i < 2000 && done < kCalls; ++i) {
        p.step();
    }
    EXPECT_EQ(done, kCalls);
    EXPECT_EQ(ok, kCalls) << "every call must return the echoed body with status 0";
    EXPECT_TRUE(p.a->established(p.eb));
    EXPECT_TRUE(p.b->established(p.ea));
}

TEST(Rpc, TimeoutOnSilentPeer) {
    Pair p(8, taut::Impairments{.delay = 5ms});
    std::optional<std::uint32_t> st;
    p.a->call(ep(9999), tautq::Method::Ping, {}, 500ms,
              [&](std::uint32_t s, taut::ByteSpan) { st = s; });
    for (int i = 0; i < 300 && !st; ++i) {
        p.step();
    }
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(*st, tautq::status::kTimeout);
    EXPECT_EQ(p.a->inflight(), 0u);
}

TEST(Rpc, OversizedBodyFailsFast) {
    Pair p(9, taut::Impairments{});
    std::optional<std::uint32_t> st;
    const std::vector<std::byte> big(tautq::kMaxRpcBody + 1);
    p.a->call(p.eb, tautq::Method::Ping, big, 1000ms,
              [&](std::uint32_t s, taut::ByteSpan) { st = s; });
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(*st, tautq::status::kTooLarge);
}

// The tautq restart story at RPC level: B is killed and replaced by a fresh process (new
// boot_id) on the same endpoint. A's in-flight call must fail fast with kPeerDown — the new
// process never saw the request — and a subsequent call must succeed after the automatic
// re-handshake. No manual reset anywhere.
TEST(Rpc, PeerRestartFailsInflightThenRecovers) {
    Pair p(10, taut::Impairments{.delay = 5ms});
    serve_echo(*p.b);

    // Warm the link.
    std::optional<std::uint32_t> warm;
    p.a->call(p.eb, tautq::Method::Ping, bytes({1}), 2000ms,
              [&](std::uint32_t s, taut::ByteSpan) { warm = s; });
    for (int i = 0; i < 400 && !warm; ++i) {
        p.step();
    }
    ASSERT_EQ(warm.value_or(999), tautq::status::kOk);

    // In-flight call, then B dies and is replaced before it can respond.
    std::optional<std::uint32_t> inflight;
    p.a->call(p.eb, tautq::Method::Ping, bytes({2}), 5000ms,
              [&](std::uint32_t s, taut::ByteSpan) { inflight = s; });
    p.b = std::make_unique<tautq::RpcNode>(p.net.endpoint(p.eb), p.eb, taut::Config{}, 0xB0002);
    serve_echo(*p.b);

    for (int i = 0; i < 600 && !inflight; ++i) {
        p.step();
    }
    ASSERT_TRUE(inflight.has_value()) << "in-flight call must not hang after a peer restart";
    EXPECT_EQ(*inflight, tautq::status::kPeerDown)
        << "the restarted peer's HELLO must fail the stale call, well before the timeout";

    std::optional<std::uint32_t> after;
    p.a->call(p.eb, tautq::Method::Ping, bytes({3}), 2000ms,
              [&](std::uint32_t s, taut::ByteSpan) { after = s; });
    for (int i = 0; i < 400 && !after; ++i) {
        p.step();
    }
    EXPECT_EQ(after.value_or(999), tautq::status::kOk)
        << "calls must work again once the new incarnation handshakes";
}

TEST(Rpc, PeerDeadAbandonsInflightCalls) {
    Pair p(11, taut::Impairments{.delay = 5ms});
    serve_echo(*p.b);

    std::optional<std::uint32_t> st;
    p.a->call(p.eb, tautq::Method::Ping, bytes({1}), 60000ms,
              [&](std::uint32_t s, taut::ByteSpan) { st = s; });
    p.step();
    p.a->peer_dead(p.eb); // SWIM verdict
    ASSERT_TRUE(st.has_value());
    EXPECT_EQ(*st, tautq::status::kPeerDown);
    EXPECT_EQ(p.a->inflight(), 0u);
}

TEST(Rpc, HandshakeAndCallsSurviveHeavyLoss) {
    Pair p(12, taut::Impairments{.loss = 0.20, .delay = 10ms, .jitter = 5ms});
    serve_echo(*p.b);

    int done = 0;
    int ok = 0;
    for (int i = 0; i < 10; ++i) {
        p.a->call(p.eb, tautq::Method::Ping, bytes({i}), 10000ms,
                  [&](std::uint32_t s, taut::ByteSpan) {
                      ++done;
                      ok += (s == tautq::status::kOk) ? 1 : 0;
                  });
    }
    for (int i = 0; i < 4000 && done < 10; ++i) {
        p.step();
    }
    EXPECT_EQ(done, 10);
    EXPECT_EQ(ok, 10);
}

} // namespace
