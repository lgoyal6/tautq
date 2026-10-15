#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "taut/transport.h"

namespace tautq {

// Consistent-hash ring over the configured membership (DESIGN-protocol §2, D2). Consulted
// at submit time to pick a stable idempotency owner; replica placement uses the currently
// alive set and is then pinned in the job record forever — no
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
    // Stable configured membership used for idempotency ownership. It must not shrink
    // when a node is suspected or declared dead.
    virtual std::vector<taut::Endpoint> members() const {
        return alive();
    }
    virtual bool is_alive(const taut::Endpoint& e) const = 0;
};

class StaticMembership : public Membership {
  public:
    explicit StaticMembership(std::vector<taut::Endpoint> nodes)
        : nodes_(nodes), members_(std::move(nodes)) {}

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
    std::vector<taut::Endpoint> members() const override {
        return members_;
    }
    void set(std::vector<taut::Endpoint> nodes) {
        for (const auto& n : nodes) {
            if (std::find(members_.begin(), members_.end(), n) == members_.end()) {
                members_.push_back(n);
            }
        }
        nodes_ = std::move(nodes);
    }

  private:
    std::vector<taut::Endpoint> nodes_;
    std::vector<taut::Endpoint> members_;
};

} // namespace tautq
