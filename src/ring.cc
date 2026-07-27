#include "ring.h"

#include <algorithm>

#include "wire.h"

namespace tautq {
namespace ring {

namespace {

// splitmix64 — mixes the endpoint key into a ring position (a raw ekey would cluster
// same-host nodes, since they share every bit except the port).
std::uint64_t mix(std::uint64_t x) {
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    return x ^ (x >> 31);
}

struct Pos {
    std::uint64_t pos;
    taut::Endpoint node;
};

std::vector<Pos> positions(std::vector<taut::Endpoint>& alive) {
    std::vector<Pos> out;
    out.reserve(alive.size());
    for (const auto& n : alive) {
        out.push_back({mix(ekey(n)), n});
    }
    std::sort(out.begin(), out.end(), [](const Pos& a, const Pos& b) {
        if (a.pos != b.pos) {
            return a.pos < b.pos;
        }
        return ekey(a.node) < ekey(b.node); // deterministic tiebreak
    });
    return out;
}

} // namespace

std::uint64_t hash_key(const std::string& idem_key) {
    std::uint64_t h = 0xCBF29CE484222325ull;
    for (const char c : idem_key) {
        h ^= static_cast<unsigned char>(c);
        h *= 0x100000001B3ull;
    }
    return h;
}

taut::Endpoint owner_for(const std::string& idem_key, std::vector<taut::Endpoint> alive) {
    const auto set = replica_set(idem_key, std::move(alive));
    return set[0];
}

std::array<taut::Endpoint, 3> replica_set(const std::string& idem_key,
                                          std::vector<taut::Endpoint> alive) {
    std::array<taut::Endpoint, 3> out{};
    if (alive.empty()) {
        return out;
    }
    const auto ps = positions(alive);
    const std::uint64_t kh = hash_key(idem_key);

    // Successor of the key's hash, wrapping.
    std::size_t start = 0;
    while (start < ps.size() && ps[start].pos < kh) {
        ++start;
    }
    if (start == ps.size()) {
        start = 0;
    }
    for (std::size_t i = 0; i < ps.size() && i < 3; ++i) {
        out[i] = ps[(start + i) % ps.size()].node;
    }
    return out;
}

} // namespace ring
} // namespace tautq
