#pragma once

#include <algorithm>
#include <utility>
#include <vector>

#include <arpa/inet.h>

#include "ring.h"
#include "taut/swim.h"

namespace tautq {

// Keep ownership membership bound to the configured peer vector. SWIM remains the
// source of liveness and replica placement, but discovering a peer cannot remap keys.
class SwimMembership : public Membership {
  public:
    SwimMembership(taut::Swim& swim, taut::Endpoint self_data,
                   std::vector<taut::Endpoint> configured_data)
        : swim_(swim), self_(self_data), configured_(std::move(configured_data)) {
        std::vector<taut::Endpoint> unique;
        unique.reserve(configured_.size() + 1);
        unique.push_back(self_);
        for (const auto& endpoint : configured_) {
            if (std::find(unique.begin(), unique.end(), endpoint) == unique.end()) {
                unique.push_back(endpoint);
            }
        }
        configured_ = std::move(unique);
    }

    std::vector<taut::Endpoint> alive() const override {
        std::vector<taut::Endpoint> out{self_};
        for (const auto& [ep, state] : swim_.snapshot()) {
            if (state == taut::MemberState::Alive) {
                const auto data = data_endpoint(ep);
                if (std::find(out.begin(), out.end(), data) == out.end())
                    out.push_back(data);
            }
        }
        return out;
    }

    std::vector<taut::Endpoint> members() const override {
        return configured_;
    }

    bool is_alive(const taut::Endpoint& endpoint) const override {
        return endpoint == self_ ||
               swim_.state_of(swim_endpoint(endpoint)) == taut::MemberState::Alive;
    }

  private:
    static taut::Endpoint swim_endpoint(taut::Endpoint data) {
        data.port_be = htons(static_cast<std::uint16_t>(ntohs(data.port_be) + 1));
        return data;
    }
    static taut::Endpoint data_endpoint(taut::Endpoint swim) {
        swim.port_be = htons(static_cast<std::uint16_t>(ntohs(swim.port_be) - 1));
        return swim;
    }

    taut::Swim& swim_;
    taut::Endpoint self_;
    std::vector<taut::Endpoint> configured_;
};

} // namespace tautq
