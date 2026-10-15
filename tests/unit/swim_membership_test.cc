#include "swim_membership.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "taut/sim_net.h"

namespace {

taut::Endpoint endpoint(std::uint16_t port) {
    taut::Endpoint e{};
    e.addr_be = 1;
    e.port_be = htons(port);
    return e;
}

taut::Endpoint swim_endpoint(taut::Endpoint data) {
    data.port_be = htons(static_cast<std::uint16_t>(ntohs(data.port_be) + 1));
    return data;
}

TEST(SwimMembershipTest, LateDiscoveryDoesNotChangeConfiguredOwners) {
    taut::SimNet net(7);
    const auto self = endpoint(9000);
    const auto configured_peer = endpoint(9002);
    const auto discovered_peer = endpoint(9004);
    taut::Swim swim(net.endpoint(swim_endpoint(self)), swim_endpoint(self), taut::SwimConfig{}, 9);
    swim.add_member(swim_endpoint(configured_peer));
    tautq::SwimMembership membership(swim, self, {self, configured_peer});

    std::string changed_key;
    for (int i = 0; i < 10000; ++i) {
        const auto key = "late-peer-" + std::to_string(i);
        if (tautq::ring::owner_for(key, {self, configured_peer}) !=
            tautq::ring::owner_for(key, {self, configured_peer, discovered_peer})) {
            changed_key = key;
            break;
        }
    }
    ASSERT_FALSE(changed_key.empty())
        << "test needs a key whose owner changes if the peer is added";
    const auto owner_before = tautq::ring::owner_for(changed_key, membership.members());

    swim.add_member(swim_endpoint(discovered_peer));

    EXPECT_EQ(membership.alive().size(), 3);
    EXPECT_EQ(membership.members(), (std::vector<taut::Endpoint>{self, configured_peer}));
    EXPECT_EQ(tautq::ring::owner_for(changed_key, membership.members()), owner_before);
}

} // namespace
