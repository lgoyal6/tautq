#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "taut/transport.h"

namespace tautq {

// Consistent-hash ring over the alive membership (DESIGN-protocol §2, D2). Consulted ONLY
// at submit time: it picks the job's owner (cross-node idempotency dedup under stable
// membership) and the replica set, which is then pinned in the job record forever — no
// range handoff, ever. Node positions come from mixing the endpoint key; the key's hash
// walks to its successor.
namespace ring {

std::uint64_t hash_key(const std::string& idem_key); // FNV-1a 64

// The alive set must include self. Returns Endpoint{} if `alive` is empty.
taut::Endpoint owner_for(const std::string& idem_key, std::vector<taut::Endpoint> alive);

// [0] = owner, [1..2] = the next distinct alive nodes clockwise (Endpoint{} when the
// cluster is smaller than 3 — W degrades with it, disclosed in the README).
std::array<taut::Endpoint, 3> replica_set(const std::string& idem_key,
                                          std::vector<taut::Endpoint> alive);

} // namespace ring

// Liveness view the queue consults (ring routing, replica choice, takeover checks). Real
// nodes wrap taut::Swim (M7); tests script it directly.
class Membership {
  public:
    virtual ~Membership() = default;
    // Every node currently believed Alive, INCLUDING self. Order does not matter.
    virtual std::vector<taut::Endpoint> alive() const = 0;
    virtual bool is_alive(const taut::Endpoint& e) const = 0;
};

class StaticMembership : public Membership {
  public:
    explicit StaticMembership(std::vector<taut::Endpoint> nodes) : nodes_(std::move(nodes)) {}

    std::vector<taut::Endpoint> alive() const override {
        return nodes_;
    }
    bool is_alive(const taut::Endpoint& e) const override {
        for (const auto& n : nodes_) {
            if (n == e) {
                return true;
            }
        }
        return false;
    }
    void set(std::vector<taut::Endpoint> nodes) {
        nodes_ = std::move(nodes);
    }

  private:
    std::vector<taut::Endpoint> nodes_;
};

} // namespace tautq
