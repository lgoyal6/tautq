#include <optional>

#include <gtest/gtest.h>

#include "cluster.h"

using namespace std::chrono_literals;
using tautq::test::Cluster;

namespace {

tautq::QueueNode::SubmitParams params(const std::string& key) {
    tautq::QueueNode::SubmitParams p;
    p.idem_key = key;
    p.url = "http://sink:8081/hook";
    p.body = {std::byte{'x'}};
    return p;
}

struct Result {
    std::optional<std::uint32_t> st;
    tautq::JobId id;
};

tautq::QueueNode::SubmitCb capture(Result& r) {
    return [&r](std::uint32_t st, const tautq::JobId& id) {
        r.st = st;
        r.id = id;
    };
}

TEST(Submit, ReachesQuorumReplicatesEverywhereAndDedups) {
    Cluster c(1, taut::Impairments{.delay = 2ms}, 3, "submit-quorum");
    const std::string key = c.key_owned_by(0, "q");

    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    for (int i = 0; i < 400 && !r.st; ++i) {
        c.step();
    }
    ASSERT_TRUE(r.st.has_value());
    EXPECT_EQ(*r.st, tautq::qstatus::kCreated);

    // Repair drives the job to all three replicas.
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }
    for (int i = 0; i < 3; ++i) {
        const tautq::Job* j = c.nodes[i]->store().find(r.id);
        ASSERT_NE(j, nullptr) << "node " << i << " must hold a copy";
        EXPECT_EQ(j->idem_key, key);
        EXPECT_EQ(j->owner, c.eps[0]);
    }

    // Same key resubmitted — to the owner AND via another gateway — dedups to the same id.
    Result r2;
    c.nodes[0]->submit(params(key), capture(r2));
    for (int i = 0; i < 200 && !r2.st; ++i) {
        c.step();
    }
    ASSERT_TRUE(r2.st.has_value());
    EXPECT_EQ(*r2.st, tautq::qstatus::kDuplicate);
    EXPECT_EQ(r2.id, r.id);

    Result r3;
    c.nodes[2]->submit(params(key), capture(r3));
    for (int i = 0; i < 400 && !r3.st; ++i) {
        c.step();
    }
    ASSERT_TRUE(r3.st.has_value());
    EXPECT_EQ(*r3.st, tautq::qstatus::kDuplicate);
    EXPECT_EQ(r3.id, r.id);
}

TEST(Submit, GatewayForwardsToRingOwner) {
    Cluster c(2, taut::Impairments{.loss = 0.02, .delay = 2ms}, 3, "submit-fwd");
    const std::string key = c.key_owned_by(1, "fwd");

    // Client hits node 0; the ring owner is node 1.
    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    for (int i = 0; i < 800 && !r.st; ++i) {
        c.step();
    }
    ASSERT_TRUE(r.st.has_value());
    EXPECT_EQ(*r.st, tautq::qstatus::kCreated);
    const tautq::Job* j = c.nodes[1]->store().find(r.id);
    ASSERT_NE(j, nullptr);
    EXPECT_EQ(j->owner, c.eps[1]) << "the ring owner must own the job";
    EXPECT_EQ(r.id.origin, tautq::ekey(c.eps[1])) << "job id embeds its origin owner";
}

TEST(Submit, NoQuorumReportedWhenAllReplicasDown) {
    Cluster c(3, taut::Impairments{.delay = 2ms}, 3, "submit-noquorum");
    const std::string key = c.key_owned_by(0, "nq");
    c.crash(1);
    c.crash(2);

    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    for (int i = 0; i < 1000 && !r.st; ++i) {
        c.step();
    }
    ASSERT_TRUE(r.st.has_value());
    EXPECT_EQ(*r.st, tautq::qstatus::kNoQuorum)
        << "with both replicas unreachable, W=2 must not be claimed";
    // The job exists locally (documented: failed submits may still create the job; a retry
    // with the same key dedups) and stays in the repair backlog.
    EXPECT_NE(c.nodes[0]->store().find(r.id), nullptr);
    EXPECT_GT(c.nodes[0]->repair_backlog(), 0u);
}

TEST(Submit, RepairConvergesWhenReplicaReturns) {
    Cluster c(4, taut::Impairments{.delay = 2ms}, 3, "submit-repair");
    const std::string key = c.key_owned_by(0, "rep");
    c.crash(2); // one replica down; quorum still reachable via the other

    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    for (int i = 0; i < 800 && !r.st; ++i) {
        c.step();
    }
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[2]->store().find(r.id), nullptr);
    EXPECT_GT(c.nodes[0]->repair_backlog(), 0u);

    c.down[2] = false; // node 2 returns (same process; no data lost)
    for (int i = 0; i < 2000 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }
    EXPECT_EQ(c.nodes[0]->repair_backlog(), 0u) << "repair must reach 3/3 copies";
    EXPECT_NE(c.nodes[2]->store().find(r.id), nullptr);
}

TEST(Submit, RestartRebuildsStateAndDedupIndexFromLog) {
    Cluster c(5, taut::Impairments{.delay = 2ms}, 3, "submit-restart");
    const std::string key = c.key_owned_by(0, "rs");

    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    for (int i = 0; i < 400 && !r.st; ++i) {
        c.step();
    }
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);

    c.restart(0); // fresh process, same data dir

    const tautq::Job* j = c.nodes[0]->store().find(r.id);
    ASSERT_NE(j, nullptr) << "replay must rebuild the job table";
    EXPECT_EQ(j->idem_key, key);

    Result r2;
    c.nodes[0]->submit(params(key), capture(r2));
    for (int i = 0; i < 400 && !r2.st; ++i) {
        c.step();
    }
    EXPECT_EQ(r2.st.value_or(999), tautq::qstatus::kDuplicate)
        << "the dedup index must survive restart";
    EXPECT_EQ(r2.id, r.id);
}

TEST(Submit, FallsBackToLocalOwnershipWhenRingOwnerUnreachable) {
    Cluster c(6, taut::Impairments{.delay = 2ms}, 3, "submit-fallback");
    const std::string key = c.key_owned_by(1, "fb");

    // Node 1 is down but node 0's membership still lists it (SWIM hasn't converged yet) —
    // the forwarded submit times out and node 0 owns the job itself.
    c.crash(1);
    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    for (int i = 0; i < 1500 && !r.st; ++i) {
        c.step();
    }
    ASSERT_TRUE(r.st.has_value());
    EXPECT_EQ(*r.st, tautq::qstatus::kCreated);
    const tautq::Job* j = c.nodes[0]->store().find(r.id);
    ASSERT_NE(j, nullptr);
    EXPECT_EQ(j->owner, c.eps[0]) << "fallback pins the gateway as owner";
    EXPECT_EQ(j->replicas[0], c.eps[0]);
    EXPECT_NE(j->replicas[1], taut::Endpoint{}) << "fallback still picks replicas";
}

} // namespace
