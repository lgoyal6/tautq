#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "taut/config.h"
#include "taut/session.h"
#include "taut/transport.h"

#include "demux.h"
#include "wire.h"

namespace tautq {

// Local status codes the RPC layer synthesizes (never on the wire — the top bits keep them
// out of any application's status space).
namespace status {
inline constexpr std::uint32_t kOk = 0;
inline constexpr std::uint32_t kTimeout = 0xFFFF0001;  // no response within the deadline
inline constexpr std::uint32_t kPeerDown = 0xFFFF0002; // SWIM Dead / peer restarted
inline constexpr std::uint32_t kTooLarge = 0xFFFF0003; // body exceeds one datagram
inline constexpr std::uint32_t kBusy = 0xFFFF0004;     // session send queue full
} // namespace status

// Every RPC (header + body) must fit one taut datagram: 1200 - 21 (base header) - 8 (SACK
// piggyback) - 14 (RPC envelope) = 1157; kept with a little slack. Larger transfers
// paginate at the protocol layer (e.g. RESYNC).
inline constexpr std::size_t kMaxRpcBody = 1120;

// Request/response RPC between cluster nodes over per-peer taut sessions (class 1:
// reliable, unordered — retransmission and dedup are taut's job; matching responses to
// requests and detecting dead/restarted peers is ours).
//
// Sessions are created only after the boot_id HELLO handshake (wire.h) completes, and are
// torn down — failing in-flight calls with kPeerDown — when the peer restarts (new boot_id
// in a HELLO), or when SWIM declares it Dead (the owner wires peer_dead() to
// Swim::on_state_change). Datagrams from peers without a completed current handshake are
// never fed to a Session; they trigger a rate-limited HELLO instead, which is how a
// counterpart discovers we restarted and re-handshakes.
//
// Driven like every taut component: poll() drains the socket and dispatches, tick() fires
// handshake retries, call timeouts, and session timers, using the transport's clock.
class RpcNode {
  public:
    // Handed to request handlers; respond(ctx, ...) may be called immediately or later
    // (e.g. after a quorum of further RPCs completes). If the requester restarted or died
    // in the meantime, the response is silently dropped — its new incarnation would not
    // recognize the req_id anyway.
    struct ReqCtx {
        taut::Endpoint from{};
        std::uint64_t req_id = 0;
        Method method = Method::Ping;
    };
    using Handler = std::function<void(const ReqCtx& ctx, taut::ByteSpan body)>;
    using ResponseCb = std::function<void(std::uint32_t status, taut::ByteSpan body)>;

    RpcNode(taut::UdpTransport& socket, taut::Endpoint self, taut::Config session_cfg,
            std::uint64_t boot_id);

    void on_request(Method m, Handler h);
    void respond(const ReqCtx& ctx, std::uint32_t st, std::span<const std::byte> body);

    // Issue a request. `cb` fires exactly once: with the peer's response, or with a
    // synthesized kTimeout/kPeerDown/kTooLarge/kBusy. May fire synchronously on immediate
    // local failure.
    void call(const taut::Endpoint& peer, Method m, std::span<const std::byte> body,
              std::chrono::milliseconds timeout, ResponseCb cb);

    void poll();
    void tick();

    // SWIM wiring: the peer was declared Dead — abandon in-flight calls, drop its session.
    void peer_dead(const taut::Endpoint& peer);

    std::uint64_t boot_id() const {
        return boot_;
    }
    // The transport's clock (virtual under SimNet) — the time source for everything above.
    std::chrono::steady_clock::time_point now() const {
        return demux_.inner().now();
    }

    // Introspection for tests.
    bool established(const taut::Endpoint& peer) const;
    std::size_t inflight() const {
        return calls_.size();
    }

  private:
    using TimePoint = std::chrono::steady_clock::time_point;

    struct Peer {
        taut::Endpoint addr{};
        std::unique_ptr<taut::Session> session;     // null until the handshake completes
        std::uint64_t remote_boot = 0;              // 0 = unknown
        bool acked = false;                         // peer has confirmed OUR boot
        TimePoint next_hello{};                     // handshake (re)send pacing
        std::deque<std::vector<std::byte>> pending; // frames awaiting establishment
    };
    struct PendingCall {
        std::uint64_t peer_key = 0;
        TimePoint deadline{};
        ResponseCb cb;
    };

    Peer& peer(const taut::Endpoint& addr);
    bool is_established(const Peer& p) const {
        return p.remote_boot != 0 && p.acked && p.session != nullptr;
    }
    void send_hello(Peer& p, TimePoint now);
    void handle_hello(const taut::Endpoint& from, const Hello& h);
    void handle_stranger(const taut::Endpoint& from);
    void establish(Peer& p);                 // create session, flush pending
    void flush_pending(Peer& p);             // move queued frames into the session
    void teardown(Peer& p, bool fail_calls); // drop session/view, optionally fail calls
    void fail_calls_to(std::uint64_t peer_key, std::uint32_t st);
    void dispatch(const taut::Endpoint& from, taut::ByteSpan payload);
    void finish_call(std::uint64_t req_id, std::uint32_t st, taut::ByteSpan body);

    Demux demux_;
    taut::Endpoint self_;
    taut::Config scfg_;
    std::uint64_t boot_;

    std::unordered_map<std::uint64_t, Peer> peers_;
    std::unordered_map<std::uint64_t, PendingCall> calls_;       // req_id -> call
    std::unordered_map<std::uint8_t, Handler> handlers_;         // method -> handler
    std::unordered_map<std::uint64_t, TimePoint> stranger_next_; // HELLO rate limit
    std::uint64_t next_req_ = 1;

    static constexpr std::chrono::milliseconds kHelloRetry{200};
    static constexpr std::chrono::milliseconds kStrangerInterval{1000};
};

} // namespace tautq
