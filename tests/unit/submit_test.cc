#include <algorithm>
#include <array>
#include <iostream>
#include <optional>
#include <vector>

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

TEST(Submit, UnavailableStableOwnerRefusesWithoutCreatingDuplicate) {
    for (std::uint64_t seed = 6; seed < 18; ++seed) {
        const int owner = static_cast<int>(seed % 3);
        Cluster c(seed, taut::Impairments{.delay = 2ms}, 3,
                  "submit-stable-owner-" + std::to_string(seed));
        const std::string key = c.key_owned_by(owner, "fb" + std::to_string(seed));
        Result original;
        c.nodes[static_cast<std::size_t>(owner)]->submit(params(key), capture(original));
        for (int i = 0; i < 400 && !original.st; ++i)
            c.step();
        ASSERT_EQ(original.st.value_or(999), tautq::qstatus::kCreated) << "seed=" << seed;

        // Remove the stable owner from the live view. Other gateways must continue to
        // route to that configured owner and fail closed, not remap the key.
        c.crash(owner);
        c.mark_dead(owner);
        for (int gateway = 0; gateway < 3; ++gateway) {
            if (gateway == owner)
                continue;
            Result retry;
            c.nodes[static_cast<std::size_t>(gateway)]->submit(params(key), capture(retry));
            ASSERT_EQ(retry.st.value_or(999), tautq::qstatus::kNoQuorum)
                << "seed=" << seed << " gateway=" << gateway;
            EXPECT_EQ(retry.id, tautq::JobId{});
        }
        std::vector<tautq::JobId> ids;
        for (const auto& node : c.nodes) {
            for (const auto& [id, stored] : node->store().jobs()) {
                if (stored.idem_key == key && std::find(ids.begin(), ids.end(), id) == ids.end())
                    ids.push_back(id);
            }
        }
        ASSERT_EQ(ids.size(), 1u) << "seed=" << seed;
        EXPECT_EQ(ids.front(), original.id) << "seed=" << seed;
    }
}

template <typename ClusterType> struct ChurnRow {
    std::uint64_t seed;
    std::string key;
    int owner;
    std::array<int, 3> replicas;
    std::array<int, 2> survivors;
    std::uint32_t retry_status;
    int distinct_ids;
};

template <typename ClusterType>
ChurnRow<ClusterType> run_churn_comparison_arm(std::uint64_t seed, const std::string& arm) {
    ClusterType c(seed, taut::Impairments{.delay = 2ms}, 5,
                  "m4-owner-churn-" + arm + "-" + std::to_string(seed));
    const int owner = 0;
    const std::string key = c.key_owned_by(owner, "m4-" + std::to_string(seed));
    Result original;
    c.nodes[static_cast<std::size_t>(owner)]->submit(params(key), capture(original));
    for (int i = 0; i < 600 && !original.st; ++i)
        c.step();
    EXPECT_EQ(original.st.value_or(999), tautq::qstatus::kCreated) << "seed=" << seed;
    for (int i = 0; i < 1200 && c.nodes[static_cast<std::size_t>(owner)]->repair_backlog() > 0;
         ++i) {
        c.step();
    }

    const auto* original_job = c.nodes[static_cast<std::size_t>(owner)]->store().find(original.id);
    EXPECT_NE(original_job, nullptr) << "seed=" << seed;
    std::array<int, 3> replicas{-1, -1, -1};
    std::vector<bool> old_group(c.eps.size(), false);
    if (original_job != nullptr) {
        for (std::size_t slot = 0; slot < original_job->replicas.size(); ++slot) {
            for (std::size_t node = 0; node < c.eps.size(); ++node) {
                if (original_job->replicas[slot] == c.eps[node]) {
                    replicas[slot] = static_cast<int>(node);
                    old_group[node] = true;
                }
            }
        }
    }
    std::array<int, 2> survivors{-1, -1};
    std::size_t survivor_count = 0;
    for (std::size_t node = 0; node < c.eps.size(); ++node) {
        if (!old_group[node] && survivor_count < survivors.size()) {
            survivors[survivor_count++] = static_cast<int>(node);
        }
    }
    EXPECT_EQ(survivor_count, 2u) << "seed=" << seed;
    for (int node : replicas) {
        if (node >= 0)
            c.crash(node);
    }
    c.set_membership({survivors[0], survivors[1]});

    Result retry;
    c.nodes[static_cast<std::size_t>(survivors[0])]->submit(params(key), capture(retry));
    for (int i = 0; i < 800 && !retry.st; ++i)
        c.step();

    std::vector<tautq::JobId> ids;
    for (const auto& node : c.nodes) {
        for (const auto& [id, stored] : node->store().jobs()) {
            if (stored.idem_key == key && std::find(ids.begin(), ids.end(), id) == ids.end()) {
                ids.push_back(id);
            }
        }
    }
    return {seed,
            key,
            owner,
            replicas,
            survivors,
            retry.st.value_or(999),
            static_cast<int>(ids.size())};
}

TEST(Study, StableOwnerChurnAvailabilityAndDuplicateTradeoffAcrossFrozenSeeds) {
    for (std::uint64_t seed = 6; seed < 18; ++seed) {
        const auto baseline =
            run_churn_comparison_arm<tautq::test::DynamicCluster>(seed, "dynamic");
        const auto treatment = run_churn_comparison_arm<Cluster>(seed, "stable");
        std::cout << "M4," << seed << ',' << baseline.key << ',' << baseline.owner << ','
                  << baseline.replicas[0] << ':' << baseline.replicas[1] << ':'
                  << baseline.replicas[2] << ',' << baseline.survivors[0] << ':'
                  << baseline.survivors[1] << ',' << baseline.retry_status << ','
                  << baseline.distinct_ids << ',' << treatment.retry_status << ','
                  << treatment.distinct_ids << '\n';
        EXPECT_EQ(baseline.retry_status, tautq::qstatus::kCreated) << "seed=" << seed;
        EXPECT_EQ(baseline.distinct_ids, 2) << "seed=" << seed;
        EXPECT_EQ(treatment.retry_status, tautq::qstatus::kNoQuorum) << "seed=" << seed;
        EXPECT_EQ(treatment.distinct_ids, 1) << "seed=" << seed;
    }
}

} // namespace
