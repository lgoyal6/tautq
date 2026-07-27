#include "demux.h"

#include <array>

#include "taut/codec.h"

namespace tautq {

std::size_t PeerView::send(const taut::Endpoint& to, std::span<const std::byte> data) {
    return demux_.inner_.send(to, data);
}

std::optional<taut::RecvResult> PeerView::recv(std::span<std::byte> buf) {
    if (inbox_.empty()) {
        return std::nullopt;
    }
    const std::vector<std::byte>& d = inbox_.front();
    const std::size_t n = d.size() <= buf.size() ? d.size() : buf.size();
    std::copy_n(d.begin(), n, buf.begin());
    inbox_.pop_front();
    return taut::RecvResult{n, peer_};
}

std::chrono::steady_clock::time_point PeerView::now() const {
    return demux_.inner_.now();
}

int PeerView::fd() const {
    return demux_.inner_.fd();
}

void Demux::pump() {
    std::array<std::byte, taut::kMaxDatagram> buf{};
    while (auto r = inner_.recv(buf)) {
        const std::span<const std::byte> data(buf.data(), r->size);
        if (is_hello(data)) {
            if (const auto h = decode_hello(data); h && on_hello_) {
                on_hello_(r->from, *h);
            }
            continue;
        }
        auto it = views_.find(ekey(r->from));
        if (it == views_.end()) {
            // taut traffic from a peer we have no handshake with — likely we restarted and
            // they are still talking to our previous incarnation. Don't create session
            // state from unauthenticated data; let the RPC layer prompt a re-handshake.
            if (on_stranger_) {
                on_stranger_(r->from);
            }
            continue;
        }
        PeerView& v = *it->second;
        if (v.inbox_.size() < kMaxQueued) {
            v.inbox_.emplace_back(data.begin(), data.end());
        }
    }
}

PeerView& Demux::view(const taut::Endpoint& peer) {
    auto& slot = views_[ekey(peer)];
    if (!slot) {
        slot = std::make_unique<PeerView>(*this, peer);
    }
    return *slot;
}

void Demux::drop(const taut::Endpoint& peer) {
    views_.erase(ekey(peer));
}

} // namespace tautq
