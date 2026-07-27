#include <optional>

#include <gtest/gtest.h>

#include "cluster.h"

using namespace std::chrono_literals;
using tautq::test::Cluster;

namespace {

tautq::QueueNode::SubmitParams params(const std::string& key, std::uint32_t visibility_ms = 500,
                                      std::uint32_t max_attempts = 5) {
    tautq::QueueNode::SubmitParams p;
    p.idem_key = key;
    p.url = "http://sink:8081/hook";
    p.body = {std::byte{'x'}};
    p.visibility_ms = visibility_ms;
    p.max_attempts = max_attempts;
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

// Drives lease() until it stops returning kNoQuorum-transients — i.e. one call, stepped.
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

TEST(Lease, GrantExecuteAckCompletesAndReplicates) {
    Cluster c(11, taut::Impairments{.delay = 2ms}, 3, "lease-happy");
    const std::string key = c.key_owned_by(0, "h");
    const auto id = must_submit(c, 0, params(key));

    const auto r = try_lease(c, 0);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(r.grant.id, id);
    EXPECT_EQ(r.grant.epoch, 1u);
    EXPECT_EQ(r.grant.lease_seq, 1u);
    EXPECT_EQ(r.grant.attempt, 1u);
    EXPECT_EQ(r.grant.url, "http://sink:8081/hook");

    // While leased, nothing else is grantable.
    EXPECT_EQ(try_lease(c, 0, 2).st.value_or(999), tautq::qstatus::kNoJob);

    const auto ackst = try_ack(c, 0, id, r.grant.epoch, r.grant.lease_seq, true);
    EXPECT_EQ(ackst.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Done);

    // DONE converges to every replica (quorum + repair).
    for (int i = 0; i < 2000 && c.nodes[0]->repair_backlog() > 0; ++i) {
        c.step();
    }
    for (int n = 1; n < 3; ++n) {
        const tautq::Job* j = c.nodes[n]->store().find(id);
        if (j != nullptr) {
            EXPECT_EQ(j->state, tautq::JobState::Done) << "node " << n;
        }
    }

    // Retried ack on a Done job: idempotent OK.
    EXPECT_EQ(try_ack(c, 0, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
}

TEST(Lease, ExpiryRegrantsAndStaleTokenIsFenced) {
    Cluster c(12, taut::Impairments{.delay = 2ms}, 3, "lease-expiry");
    const std::string key = c.key_owned_by(0, "e");
    const auto id = must_submit(c, 0, params(key, /*visibility_ms=*/300));

    const auto r1 = try_lease(c, 0, /*worker=*/1);
    ASSERT_EQ(r1.st.value_or(999), tautq::qstatus::kCreated);
    ASSERT_EQ(r1.grant.lease_seq, 1u);

    // Let the visibility timeout lapse; the job returns to Ready and re-grants with a
    // bumped lease_seq.
    c.run(200); // 1s of virtual time
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Ready);
    const auto r2 = try_lease(c, 0, /*worker=*/2);
    ASSERT_EQ(r2.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(r2.grant.lease_seq, 2u);

    // Worker 1 finally finishes and acks with the EXPIRED token: fenced, because worker 2
    // holds the current lease — this is the no-double-completion property.
    EXPECT_EQ(try_ack(c, 0, id, r1.grant.epoch, r1.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kNotLeased);

    // Worker 2's token completes.
    EXPECT_EQ(try_ack(c, 0, id, r2.grant.epoch, r2.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Done);
}

TEST(Lease, LateAckWithoutRegrantGetsAmnesty) {
    Cluster c(13, taut::Impairments{.delay = 2ms}, 3, "lease-amnesty");
    const std::string key = c.key_owned_by(0, "a");
    const auto id = must_submit(c, 0, params(key, /*visibility_ms=*/300));

    const auto r = try_lease(c, 0);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    c.run(200); // lease expires; nobody re-leases
    ASSERT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Ready);

    // The slow worker's late ack is accepted — discarding finished work would only force a
    // duplicate delivery.
    EXPECT_EQ(try_ack(c, 0, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Done);
}

TEST(Lease, AttemptsExhaustToDeadLetter) {
    Cluster c(14, taut::Impairments{.delay = 2ms}, 3, "lease-dlq");
    const std::string key = c.key_owned_by(0, "d");
    const auto id = must_submit(c, 0, params(key, /*visibility_ms=*/300, /*max_attempts=*/2));

    for (int attempt = 1; attempt <= 2; ++attempt) {
        const auto r = try_lease(c, 0);
        ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
        EXPECT_EQ(r.grant.attempt, static_cast<std::uint32_t>(attempt));
        c.run(200); // never acked; expires
    }
    // Third grant attempt would exceed max_attempts: the job parks in DeadLetter instead.
    EXPECT_EQ(try_lease(c, 0).st.value_or(999), tautq::qstatus::kNoJob);
    for (int i = 0; i < 400 && c.nodes[0]->store().find(id)->state != tautq::JobState::DeadLetter;
         ++i) {
        c.step();
    }
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::DeadLetter);
}

TEST(Lease, NackRequeuesWithBackoff) {
    Cluster c(15, taut::Impairments{.delay = 2ms}, 3, "lease-nack");
    const std::string key = c.key_owned_by(0, "n");
    const auto id = must_submit(c, 0, params(key));

    const auto r = try_lease(c, 0);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    // Destination said 5xx.
    EXPECT_EQ(try_ack(c, 0, id, r.grant.epoch, r.grant.lease_seq, false).value_or(999),
              tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Ready);

    // Immediately after the nack the job is backing off.
    EXPECT_EQ(try_lease(c, 0).st.value_or(999), tautq::qstatus::kNoJob);
    c.run(300); // 1.5s > 1s first-attempt backoff
    const auto r2 = try_lease(c, 0);
    EXPECT_EQ(r2.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(r2.grant.attempt, 2u);
}

TEST(Lease, PartitionedOwnerStallsInsteadOfGranting) {
    Cluster c(16, taut::Impairments{.delay = 2ms}, 3, "lease-cp");
    const std::string key = c.key_owned_by(0, "p");
    const auto id = must_submit(c, 0, params(key));

    // Both replicas unreachable AND known-dead to the owner's membership: the owner must
    // refuse to grant (CP choice) and must NOT burn the job's lease_seq doing so.
    c.crash(1);
    c.crash(2);
    c.set_membership({0});
    const auto r = try_lease(c, 0);
    EXPECT_EQ(r.st.value_or(999), tautq::qstatus::kNoQuorum);
    EXPECT_EQ(c.nodes[0]->store().find(id)->lease_seq, 0u)
        << "the membership gate must fire before any lease is committed";
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Ready);
}

TEST(Lease, OwnerRestartKeepsLeaseAndAcceptsAckAfterReplay) {
    Cluster c(17, taut::Impairments{.delay = 2ms}, 3, "lease-restart");
    const std::string key = c.key_owned_by(0, "rs");
    const auto id = must_submit(c, 0, params(key, /*visibility_ms=*/400));

    const auto r = try_lease(c, 0);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);

    c.restart(0); // owner dies and comes back; worker is still executing

    // The lease survives replay; the conservative re-armed deadline means no premature
    // re-grant...
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Leased);
    EXPECT_EQ(try_lease(c, 0, 2).st.value_or(999), tautq::qstatus::kNoJob);

    // ...and the worker's token, minted by the previous process, still completes the job.
    EXPECT_EQ(try_ack(c, 0, id, r.grant.epoch, r.grant.lease_seq, true).value_or(999),
              tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[0]->store().find(id)->state, tautq::JobState::Done);
}

} // namespace
