#include <optional>

#include <gtest/gtest.h>

#include "cluster.h"

using namespace std::chrono_literals;
using tautq::test::Cluster;

namespace {

tautq::QueueNode::SubmitParams params(const std::string& key, std::uint32_t visibility_ms = 500) {
    tautq::QueueNode::SubmitParams p;
    p.idem_key = key;
    p.url = "http://sink:8081/hook";
    p.body = {std::byte{'x'}};
    p.visibility_ms = visibility_ms;
    return p;
}

tautq::JobId must_submit(Cluster& c, int node, const tautq::QueueNode::SubmitParams& p) {
    std::optional<std::uint32_t> st;
    tautq::JobId id;
    c.nodes[static_cast<std::size_t>(node)]->submit(p, [&](std::uint32_t s, const tautq::JobId& i) {
        st = s;
        id = i;
    });
    for (int i = 0; i < 800 && !st; ++i) {
        c.step();
    }
    EXPECT_EQ(st.value_or(999), tautq::qstatus::kCreated);
    return id;
}

struct LeaseResult {
    std::optional<std::uint32_t> st;
    tautq::QueueNode::LeaseGrant grant;
};

LeaseResult try_lease(Cluster& c, int node, std::uint64_t worker = 1) {
    LeaseResult r;
    c.nodes[static_cast<std::size_t>(node)]->lease(
        worker, [&](std::uint32_t s, const tautq::QueueNode::LeaseGrant& g) {
            r.st = s;
            r.grant = g;
        });
    for (int i = 0; i < 800 && !r.st; ++i) {
        c.step();
    }
    return r;
}

std::optional<std::uint32_t> try_ack(Cluster& c, int node, const tautq::JobId& id,
                                     std::uint32_t epoch, std::uint32_t seq, bool success) {
    std::optional<std::uint32_t> st;
    c.nodes[static_cast<std::size_t>(node)]->ack(id, epoch, seq, success,
                                                 [&](std::uint32_t s) { st = s; });
    for (int i = 0; i < 800 && !st; ++i) {
        c.step();
    }
    return st;
}

// The replica-set successor for a job owned by node 0 in a 3-node cluster: the first alive
// member of the pinned set after the owner. Find its index.
int successor_index(Cluster& c, const tautq::JobId& id, int owner) {
    const tautq::Job* j = c.nodes[static_cast<std::size_t>(owner)]->store().find(id);
    for (const auto& r : j->replicas) {
        for (std::size_t n = 0; n < c.eps.size(); ++n) {
            if (r == c.eps[n] && static_cast<int>(n) != owner) {
                return static_cast<int>(n);
            }
        }
    }
    return -1;
}

TEST(Failover, OwnerDeathTakeoverJobSurvivesAndCompletes) {
    Cluster c(21, taut::Impairments{.delay = 2ms}, 3, "fo-basic");
    const std::string key = c.key_owned_by(0, "b");
    const auto id = must_submit(c, 0, params(key));
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step(); // full 3/3 copies before we kill anything
    }
    const int suc = successor_index(c, id, 0);
    ASSERT_GE(suc, 0);

    c.crash(0);
    c.set_membership({1, 2});
    c.declare_dead_at({1, 2}, 0);
    // The successor claims, reaches majority (self + the other replica), and the job
    // becomes leasable under epoch 2.
    for (int i = 0; i < 1500; ++i) {
        const tautq::Job* j = c.nodes[static_cast<std::size_t>(suc)]->store().find(id);
        if (j != nullptr && j->owner == c.eps[static_cast<std::size_t>(suc)] && j->epoch == 2) {
            break;
        }
        c.step();
    }
    const tautq::Job* j = c.nodes[static_cast<std::size_t>(suc)]->store().find(id);
    ASSERT_NE(j, nullptr);
    EXPECT_EQ(j->owner, c.eps[static_cast<std::size_t>(suc)]);
    EXPECT_EQ(j->epoch, 2u);
    c.run(200); // local takeover flips owner instantly; the claim majority needs steps

    const auto r = try_lease(c, suc);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(try_ack(c, suc, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[static_cast<std::size_t>(suc)]->store().find(id)->state,
              tautq::JobState::Done);
}

// Crown test 1: the takeover MUST learn about a lease the dead owner committed (the claim
// majority intersects the lease majority), so the successor never double-grants — and the
// original worker's token still completes at the new owner.
TEST(Failover, TakeoverInheritsCommittedLeaseNoDoubleGrant) {
    Cluster c(22, taut::Impairments{.delay = 2ms}, 3, "fo-inherit");
    const std::string key = c.key_owned_by(0, "i");
    const auto id = must_submit(c, 0, params(key, /*visibility_ms=*/2000));
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }

    // Worker 7 obtains the lease from the owner, then the owner dies mid-execution.
    const auto r = try_lease(c, 0, /*worker=*/7);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    c.crash(0);
    c.set_membership({1, 2});
    c.declare_dead_at({1, 2}, 0);

    const int suc = successor_index(c, id, 0);
    for (int i = 0; i < 1500; ++i) {
        const tautq::Job* j = c.nodes[static_cast<std::size_t>(suc)]->store().find(id);
        if (j != nullptr && j->owner == c.eps[static_cast<std::size_t>(suc)]) {
            break;
        }
        c.step();
    }
    const tautq::Job* j = c.nodes[static_cast<std::size_t>(suc)]->store().find(id);
    ASSERT_NE(j, nullptr);
    ASSERT_EQ(j->owner, c.eps[static_cast<std::size_t>(suc)]);
    EXPECT_EQ(j->state, tautq::JobState::Leased)
        << "the successor must have learned the committed lease from the claim responses";
    EXPECT_EQ(j->lease_seq, r.grant.lease_seq);

    // No second grant while the inherited lease runs.
    EXPECT_EQ(try_lease(c, suc, /*worker=*/8).st.value_or(999), tautq::qstatus::kNoJob);

    // The worker (its own node died) acks anywhere with its ORIGINAL token — epoch 1 —
    // and the new owner accepts the inherited lease.
    EXPECT_EQ(try_ack(c, suc, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[static_cast<std::size_t>(suc)]->store().find(id)->state,
              tautq::JobState::Done);
}

// Crown test 2 — the fencing story: a frozen owner thaws after a takeover and still
// believes it owns the job (its own membership view never changed). Every lease it tries
// to commit is rejected by the replicas' epoch check; it cannot grant, and resync demotes
// it. Meanwhile the real owner operates normally.
TEST(Failover, StaleOwnerCannotGrantAfterTakeover) {
    Cluster c(23, taut::Impairments{.delay = 2ms}, 3, "fo-fence");
    const std::string key = c.key_owned_by(0, "f");
    const auto id = must_submit(c, 0, params(key));
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }

    // Node 0 freezes (partition). Only the survivors' views change.
    c.crash(0);
    c.set_node_membership(1, {1, 2});
    c.set_node_membership(2, {1, 2});
    c.declare_dead_at({1, 2}, 0);
    const int suc = successor_index(c, id, 0);
    for (int i = 0; i < 1500; ++i) {
        const tautq::Job* j = c.nodes[static_cast<std::size_t>(suc)]->store().find(id);
        if (j != nullptr && j->owner == c.eps[static_cast<std::size_t>(suc)]) {
            break;
        }
        c.step();
    }
    ASSERT_EQ(c.nodes[static_cast<std::size_t>(suc)]->store().find(id)->owner,
              c.eps[static_cast<std::size_t>(suc)]);

    // The frozen owner thaws, still seeing everyone alive and itself as owner@epoch 1.
    c.down[0] = false;
    const auto stale = try_lease(c, 0, /*worker=*/9);
    EXPECT_EQ(stale.st.value_or(999), tautq::qstatus::kNoQuorum)
        << "every replica must fence the stale owner's lease with kStaleEpoch";

    // The kStaleEpoch responses trigger resync: the old owner adopts the new ownership.
    for (int i = 0; i < 2000; ++i) {
        const tautq::Job* j0 = c.nodes[0]->store().find(id);
        if (j0 != nullptr && j0->owner == c.eps[static_cast<std::size_t>(suc)]) {
            break;
        }
        c.step();
    }
    EXPECT_EQ(c.nodes[0]->store().find(id)->owner, c.eps[static_cast<std::size_t>(suc)])
        << "the stale owner must demote itself after fencing";

    // And the legitimate owner still grants + completes normally.
    const auto r = try_lease(c, suc);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(try_ack(c, suc, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
}

// Stale-log restart (chaos scenario d): the old owner restarts from a log that predates
// the takeover AND the completion. Startup resync adopts the truth; no re-execution.
TEST(Failover, RestartFromStaleLogResyncsAndDemotes) {
    Cluster c(24, taut::Impairments{.delay = 2ms}, 3, "fo-stale");
    const std::string key = c.key_owned_by(0, "s");
    const auto id = must_submit(c, 0, params(key));
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }

    c.crash(0);
    c.set_membership({1, 2});
    c.declare_dead_at({1, 2}, 0);
    const int suc = successor_index(c, id, 0);
    for (int i = 0; i < 1500; ++i) {
        const tautq::Job* j = c.nodes[static_cast<std::size_t>(suc)]->store().find(id);
        if (j != nullptr && j->owner == c.eps[static_cast<std::size_t>(suc)]) {
            break;
        }
        c.step();
    }
    c.run(200); // let the claim majority land before leasing
    const auto r = try_lease(c, suc);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    ASSERT_EQ(try_ack(c, suc, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);

    // Node 0 comes back with its stale log (owner=self@1, job Ready) and full membership.
    c.set_membership({0, 1, 2});
    c.restart(0);
    for (int i = 0; i < 2000; ++i) {
        const tautq::Job* j0 = c.nodes[0]->store().find(id);
        if (j0 != nullptr && j0->state == tautq::JobState::Done) {
            break;
        }
        c.step();
    }
    const tautq::Job* j0 = c.nodes[0]->store().find(id);
    ASSERT_NE(j0, nullptr);
    EXPECT_EQ(j0->state, tautq::JobState::Done) << "startup resync must adopt the completion";
    EXPECT_EQ(j0->owner, c.eps[static_cast<std::size_t>(suc)]);
    EXPECT_EQ(try_lease(c, 0).st.value_or(999), tautq::qstatus::kNoJob)
        << "a demoted node must not grant";
}

TEST(Failover, TakeoverStallsWithoutMajorityThenCompletes) {
    Cluster c(25, taut::Impairments{.delay = 2ms}, 3, "fo-cp");
    const std::string key = c.key_owned_by(0, "c");
    const auto id = must_submit(c, 0, params(key));
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }

    // Owner AND one replica die: the last member cannot form a majority. CP: stall.
    c.crash(0);
    const int suc = successor_index(c, id, 0);
    const int other = 3 - suc; // the remaining index in {1,2}
    c.crash(other);
    c.set_membership({suc});
    c.declare_dead_at({suc}, 0);
    c.declare_dead_at({suc}, other);
    c.run(400);
    // The survivor may have bumped its local epoch, but it must not grant: quorum is
    // unreachable (membership gate) — jobs stall rather than risk double execution.
    EXPECT_NE(try_lease(c, suc).st.value_or(999), tautq::qstatus::kCreated);

    // The second replica returns: the claim completes and the job flows again.
    c.down[static_cast<std::size_t>(other)] = false;
    c.set_membership({suc, other});
    c.run(600);
    const auto r = try_lease(c, suc);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(try_ack(c, suc, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
}

TEST(Failover, DrainHandsOffOwnershipGracefully) {
    Cluster c(26, taut::Impairments{.delay = 2ms}, 3, "fo-drain");
    std::vector<tautq::JobId> ids;
    for (int k = 0; k < 3; ++k) {
        ids.push_back(must_submit(c, 0, params(c.key_owned_by(0, "dr" + std::to_string(k)))));
    }
    for (int i = 0; i < 1500 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }

    bool drained = false;
    c.nodes[0]->drain([&] { drained = true; });
    for (int i = 0; i < 3000 && !drained; ++i) {
        c.step();
    }
    EXPECT_TRUE(drained) << "drain must complete once no owned active jobs remain";
    for (const auto& id : ids) {
        const tautq::Job* j = c.nodes[0]->store().find(id);
        ASSERT_NE(j, nullptr);
        EXPECT_NE(j->owner, c.eps[0]) << "ownership must have moved off the drained node";
        // And the job is fully operable at its new owner.
        int new_owner = -1;
        for (std::size_t n = 0; n < c.eps.size(); ++n) {
            if (j->owner == c.eps[n]) {
                new_owner = static_cast<int>(n);
            }
        }
        ASSERT_GE(new_owner, 0);
        const auto r = try_lease(c, new_owner, 50);
        ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
        EXPECT_EQ(
            try_ack(c, new_owner, r.grant.id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
            tautq::qstatus::kCreated);
    }
}

} // namespace
