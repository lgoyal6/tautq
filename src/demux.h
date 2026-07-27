#pragma once

#include <cstddef>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "taut/transport.h"

#include "wire.h"

namespace tautq {

// One node owns ONE data socket, but taut::Session is strictly per-peer and its poll()
// drains whatever transport it is given. Demux sits between them: pump() drains the real
// socket once per loop iteration and routes each datagram by source endpoint into a
// per-peer queue; every Session is constructed over a PeerView facade whose recv() pops
// only its own queue (sends pass straight through to the shared socket).
//
// HELLO datagrams (tautq's session-reset handshake, wire.h) are split off by magic before
// any session sees them and handed to the hello callback — they must work when no session
// exists yet, and must never be fed into one.

class Demux;

class PeerView : public taut::UdpTransport {
  public:
    PeerView(Demux& d, taut::Endpoint peer) : demux_(d), peer_(peer) {}

    std::size_t send(const taut::Endpoint& to, std::span<const std::byte> data) override;
    std::optional<taut::RecvResult> recv(std::span<std::byte> buf) override;
    std::chrono::steady_clock::time_point now() const override;
    int fd() const override;

  private:
    friend class Demux;
    Demux& demux_;
    taut::Endpoint peer_;
    std::deque<std::vector<std::byte>> inbox_;
};

class Demux {
  public:
    // (from, hello) for every HELLO datagram; (from) for every non-HELLO datagram arriving
    // from a peer that has no view yet (the RPC layer answers those with a rate-limited
    // HELLO so a restarted counterpart can discover it must re-handshake).
    using HelloHandler = std::function<void(const taut::Endpoint&, const Hello&)>;
    using StrangerHandler = std::function<void(const taut::Endpoint&)>;

    explicit Demux(taut::UdpTransport& inner) : inner_(inner) {}

    void on_hello(HelloHandler h) {
        on_hello_ = std::move(h);
    }
    void on_stranger(StrangerHandler h) {
        on_stranger_ = std::move(h);
    }

    // Drain every readable datagram from the shared socket into the per-peer inboxes.
    void pump();

    // The facade transport for `peer`, created on first use. Stable address until dropped.
    PeerView& view(const taut::Endpoint& peer);
    bool has_view(const taut::Endpoint& peer) const {
        return views_.count(ekey(peer)) != 0;
    }
    // Destroy a peer's view and any queued datagrams (session teardown). The caller must
    // drop its Session (which holds a reference to the view) FIRST.
    void drop(const taut::Endpoint& peer);

    taut::UdpTransport& inner() {
        return inner_;
    }

  private:
    friend class PeerView;
    // Bound per-peer queue: a stalled/malicious peer must not grow memory without limit.
    // Overflow drops the newest datagram — UDP semantics; taut retransmits what matters.
    static constexpr std::size_t kMaxQueued = 1024;

    taut::UdpTransport& inner_;
    HelloHandler on_hello_;
    StrangerHandler on_stranger_;
    std::unordered_map<std::uint64_t, std::unique_ptr<PeerView>> views_;
};

} // namespace tautq
