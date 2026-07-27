#include "rpc.h"

#include <utility>

namespace tautq {

RpcNode::RpcNode(taut::UdpTransport& socket, taut::Endpoint self, taut::Config session_cfg,
                 std::uint64_t boot_id)
    : demux_(socket), self_(self), scfg_(session_cfg), boot_(boot_id) {
    demux_.on_hello([this](const taut::Endpoint& from, const Hello& h) { handle_hello(from, h); });
    demux_.on_stranger([this](const taut::Endpoint& from) { handle_stranger(from); });
}

void RpcNode::on_request(Method m, Handler h) {
    handlers_[static_cast<std::uint8_t>(m)] = std::move(h);
}

RpcNode::Peer& RpcNode::peer(const taut::Endpoint& addr) {
    Peer& p = peers_[ekey(addr)];
    if (p.addr == taut::Endpoint{}) {
        p.addr = addr;
    }
    return p;
}

bool RpcNode::established(const taut::Endpoint& ep) const {
    auto it = peers_.find(ekey(ep));
    return it != peers_.end() && is_established(it->second);
}

void RpcNode::call(const taut::Endpoint& to, Method m, std::span<const std::byte> body,
                   std::chrono::milliseconds timeout, ResponseCb cb) {
    if (body.size() > kMaxRpcBody) {
        cb(status::kTooLarge, {});
        return;
    }
    const std::uint64_t id = next_req_++;
    auto frame = encode_rpc(MsgKind::Request, m, id, 0, body);

    Peer& p = peer(to);
    const auto now = demux_.inner().now();
    if (is_established(p)) {
        if (!p.session->send(taut::Class::ReliableUnordered, frame)) {
            cb(status::kBusy, {});
            return;
        }
    } else {
        p.pending.push_back(std::move(frame));
        if (now >= p.next_hello) {
            send_hello(p, now);
        }
    }
    calls_[id] = PendingCall{ekey(to), now + timeout, std::move(cb)};
}

void RpcNode::send_hello(Peer& p, TimePoint now) {
    Hello h;
    h.is_ack = false;
    h.sender_boot = boot_;
    const auto d = encode_hello(h);
    demux_.inner().send(p.addr, d);
    p.next_hello = now + kHelloRetry;
}

void RpcNode::handle_hello(const taut::Endpoint& from, const Hello& h) {
    Peer& p = peer(from);
    const auto now = demux_.inner().now();

    if (!h.is_ack) {
        // A HELLO advertises the sender's current boot. A different boot than the one our
        // session was built against means the peer restarted: that session's state (and any
        // request in it) died with the old process.
        if (p.remote_boot != 0 && p.remote_boot != h.sender_boot) {
            teardown(p, /*fail_calls=*/true);
            p.acked = false;
        }
        p.remote_boot = h.sender_boot;
        Hello ack;
        ack.is_ack = true;
        ack.sender_boot = boot_;
        ack.echo_boot = h.sender_boot;
        demux_.inner().send(from, encode_hello(ack));
        // Complete our own half of the handshake in the same round trip.
        if (!p.acked && now >= p.next_hello) {
            send_hello(p, now);
        }
        if (p.remote_boot != 0 && p.acked && p.session == nullptr) {
            establish(p);
        }
        return;
    }

    // An ACK is only valid for our CURRENT boot — a stale ack from before a restart of ours
    // must not complete the new handshake.
    if (h.echo_boot != boot_) {
        return;
    }
    if (p.remote_boot != 0 && p.remote_boot != h.sender_boot) {
        teardown(p, /*fail_calls=*/true);
    }
    p.remote_boot = h.sender_boot;
    p.acked = true;
    if (p.session == nullptr) {
        establish(p);
    }
}

void RpcNode::handle_stranger(const taut::Endpoint& from) {
    // taut traffic from a peer with no completed handshake — most likely we restarted and it
    // is still talking to our previous incarnation. Prompt a re-handshake (rate-limited so a
    // retransmit burst doesn't turn into a HELLO storm); its HELLO/ACK carries our new boot
    // implicitly via our reply.
    const auto now = demux_.inner().now();
    auto& next = stranger_next_[ekey(from)];
    if (now < next) {
        return;
    }
    next = now + kStrangerInterval;
    Peer& p = peer(from);
    send_hello(p, now);
}

void RpcNode::establish(Peer& p) {
    PeerView& v = demux_.view(p.addr);
    p.session = std::make_unique<taut::Session>(v, p.addr, scfg_);
    const taut::Endpoint from = p.addr;
    p.session->on_message(
        [this, from](taut::Class, taut::ByteSpan payload) { dispatch(from, payload); });
    flush_pending(p);
}

void RpcNode::flush_pending(Peer& p) {
    while (!p.pending.empty()) {
        if (!p.session->send(taut::Class::ReliableUnordered, p.pending.front())) {
            break; // backpressure: retried from tick(); the calls may still time out
        }
        p.pending.pop_front();
    }
}

void RpcNode::teardown(Peer& p, bool fail_calls) {
    p.session.reset(); // the Session references the view — destroy it first
    demux_.drop(p.addr);
    p.pending.clear();
    p.next_hello = TimePoint{}; // a re-handshake may start immediately
    if (fail_calls) {
        fail_calls_to(ekey(p.addr), status::kPeerDown);
    }
}

void RpcNode::fail_calls_to(std::uint64_t peer_key, std::uint32_t st) {
    std::vector<std::uint64_t> ids;
    for (const auto& [id, c] : calls_) {
        if (c.peer_key == peer_key) {
            ids.push_back(id);
        }
    }
    for (const auto id : ids) {
        finish_call(id, st, {});
    }
}

void RpcNode::peer_dead(const taut::Endpoint& ep) {
    auto it = peers_.find(ekey(ep));
    if (it != peers_.end()) {
        teardown(it->second, /*fail_calls=*/true);
        peers_.erase(it);
    } else {
        fail_calls_to(ekey(ep), status::kPeerDown);
    }
}

void RpcNode::finish_call(std::uint64_t req_id, std::uint32_t st, taut::ByteSpan body) {
    auto it = calls_.find(req_id);
    if (it == calls_.end()) {
        return; // duplicate/late response, or already timed out
    }
    ResponseCb cb = std::move(it->second.cb);
    calls_.erase(it);
    cb(st, body); // after erase: the callback may re-enter call()
}

void RpcNode::dispatch(const taut::Endpoint& from, taut::ByteSpan payload) {
    const auto msg = decode_rpc(payload);
    if (!msg) {
        return;
    }
    if (msg->kind == MsgKind::Response) {
        finish_call(msg->req_id, msg->status, msg->body);
        return;
    }
    auto hit = handlers_.find(static_cast<std::uint8_t>(msg->method));
    Reply r;
    if (hit == handlers_.end()) {
        r.status = 0xFFFFFFFF; // unknown method
    } else {
        r = hit->second(from, msg->body);
    }
    auto pit = peers_.find(ekey(from));
    if (pit == peers_.end() || !is_established(pit->second)) {
        return; // peer vanished while the handler ran
    }
    const auto frame = encode_rpc(MsgKind::Response, msg->method, msg->req_id, r.status, r.body);
    pit->second.session->send(taut::Class::ReliableUnordered, frame);
    // On send failure (full queue) the requester times out and retries — acceptable.
}

void RpcNode::poll() {
    demux_.pump();
    // Collect first: dispatch callbacks may create/destroy peers while we iterate.
    std::vector<std::uint64_t> keys;
    keys.reserve(peers_.size());
    for (const auto& [k, p] : peers_) {
        (void)p;
        keys.push_back(k);
    }
    for (const auto k : keys) {
        auto it = peers_.find(k);
        if (it != peers_.end() && it->second.session) {
            it->second.session->poll();
        }
    }
}

void RpcNode::tick() {
    const auto now = demux_.inner().now();

    std::vector<std::uint64_t> keys;
    keys.reserve(peers_.size());
    for (const auto& [k, p] : peers_) {
        (void)p;
        keys.push_back(k);
    }
    for (const auto k : keys) {
        auto it = peers_.find(k);
        if (it == peers_.end()) {
            continue;
        }
        Peer& p = it->second;
        if (p.session) {
            p.session->tick();
            flush_pending(p);
        } else if (!p.pending.empty() && now >= p.next_hello) {
            send_hello(p, now);
        }
    }

    // Call timeouts. Collect then finish: callbacks may re-enter call().
    std::vector<std::uint64_t> expired;
    for (const auto& [id, c] : calls_) {
        if (now >= c.deadline) {
            expired.push_back(id);
        }
    }
    for (const auto id : expired) {
        finish_call(id, status::kTimeout, {});
    }
}

} // namespace tautq
