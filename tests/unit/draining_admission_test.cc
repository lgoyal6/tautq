// Exercises the drain admission boundary through local and forwarded submissions.
// Stable key ownership survives execution handoff; duplicate retries keep one ID.
// SimNet bounds are functional guards, not performance measurements.
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "cluster.h"
#include <gtest/gtest.h>

namespace {
using namespace std::chrono_literals;
using tautq::test::Cluster;

struct Result {
    std::optional<std::uint32_t> status;
    tautq::JobId id{};
};

tautq::QueueNode::SubmitParams params(const std::string& key) {
    tautq::QueueNode::SubmitParams p;
    p.idem_key = key;
    p.url = "http://sink:8081/hook";
    p.body = {std::byte{'x'}};
    return p;
}

tautq::QueueNode::SubmitCb capture(Result& result) {
    return [&result](std::uint32_t status, const tautq::JobId& id) {
        result.status = status;
        result.id = id;
    };
}

void await_reply(Cluster& c, Result& result) {
    for (int i = 0; i < 400 && !result.status; ++i)
        c.step();
}

std::vector<tautq::JobId> distinct_ids(Cluster& c, const std::string& key) {
    std::vector<tautq::JobId> ids;
    for (const auto& node : c.nodes) {
        for (const auto& [id, job] : node->store().jobs()) {
            if (job.idem_key == key && std::find(ids.begin(), ids.end(), id) == ids.end()) {
                ids.push_back(id);
            }
        }
    }
    return ids;
}

void expect_configured_owner(Cluster& c, const std::string& key, int owner) {
    EXPECT_EQ(c.ring_owner_index(key), owner);
    for (const auto& membership : c.mems) {
        EXPECT_EQ(tautq::ring::owner_for(key, membership->members()),
                  c.eps[static_cast<std::size_t>(owner)]);
    }
}

class DrainingAdmission : public testing::TestWithParam<int> {};

TEST_P(DrainingAdmission, FreshOwnedKeyIsRefusedWithoutAllocation) {
    Cluster c(6001, taut::Impairments{.delay = 2ms}, 3,
              "drain-fresh-" + std::to_string(GetParam()));
    const auto key = c.key_owned_by(0, "drain-fresh");
    const auto initial_lsn = c.nodes[0]->store().next_lsn();
    const auto initial_submits = c.nodes[0]->counters().submits;
    bool drained = false;
    c.nodes[0]->drain([&] { drained = true; });
    ASSERT_TRUE(c.nodes[0]->draining());
    Result reply;
    c.nodes[static_cast<std::size_t>(GetParam())]->submit(params(key), capture(reply));
    // Inspect before any tick can hand an accidentally admitted job to a successor.
    EXPECT_TRUE(distinct_ids(c, key).empty());
    EXPECT_EQ(c.nodes[0]->store().next_lsn(), initial_lsn);
    await_reply(c, reply);
    ASSERT_TRUE(reply.status.has_value());
    EXPECT_EQ(*reply.status, tautq::qstatus::kNoQuorum);
    EXPECT_EQ(reply.id, tautq::JobId{});
    c.run(200); // bounded negative-observation window, not a latency measurement
    ASSERT_TRUE(drained);
    Result after_completion;
    c.nodes[static_cast<std::size_t>(GetParam())]->submit(params(key), capture(after_completion));
    await_reply(c, after_completion);
    ASSERT_EQ(after_completion.status.value_or(999), tautq::qstatus::kNoQuorum);
    EXPECT_EQ(after_completion.id, tautq::JobId{});
    EXPECT_TRUE(distinct_ids(c, key).empty());
    EXPECT_EQ(c.nodes[0]->counters().submits, initial_submits);
    expect_configured_owner(c, key, 0);
}

TEST_P(DrainingAdmission, ExistingKeyRetryStartedDuringDrainKeepsOriginalId) {
    Cluster c(6002, taut::Impairments{.delay = 2ms}, 3,
              "drain-duplicate-" + std::to_string(GetParam()));
    const auto key = c.key_owned_by(0, "drain-duplicate");
    Result original;
    c.nodes[0]->submit(params(key), capture(original));
    await_reply(c, original);
    ASSERT_EQ(original.status.value_or(999), tautq::qstatus::kCreated);
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i)
        c.step();
    ASSERT_EQ(c.nodes[0]->repair_backlog(), 0u);
    const auto initial_submits = c.nodes[0]->counters().submits;
    bool drained = false;
    c.nodes[0]->drain([&] { drained = true; });
    ASSERT_FALSE(drained);
    Result retry;
    c.nodes[static_cast<std::size_t>(GetParam())]->submit(params(key), capture(retry));
    // The request starts before the first drain tick. A forwarded response may race
    // handoff completion; no assertion assumes its callback precedes completion.
    await_reply(c, retry);
    ASSERT_EQ(retry.status.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(retry.id, original.id);
    EXPECT_EQ(distinct_ids(c, key), std::vector<tautq::JobId>{original.id});
    EXPECT_EQ(c.nodes[0]->counters().submits, initial_submits);
    expect_configured_owner(c, key, 0);
}

TEST_P(DrainingAdmission, ExistingKeyRetryAfterHandoffKeepsOriginalId) {
    Cluster c(6003, taut::Impairments{.delay = 2ms}, 3,
              "drain-handoff-retry-" + std::to_string(GetParam()));
    const auto key = c.key_owned_by(0, "drain-handoff-retry");
    Result original;
    c.nodes[0]->submit(params(key), capture(original));
    await_reply(c, original);
    ASSERT_EQ(original.status.value_or(999), tautq::qstatus::kCreated);
    for (int i = 0; i < 1200 && c.nodes[0]->repair_backlog() > 0; ++i)
        c.step();
    ASSERT_EQ(c.nodes[0]->repair_backlog(), 0u);
    bool drained = false;
    c.nodes[0]->drain([&] { drained = true; });
    for (int i = 0; i < 3000 && !drained; ++i)
        c.step();
    ASSERT_TRUE(drained);
    ASSERT_TRUE(c.nodes[0]->draining());
    const auto* moved = c.nodes[0]->store().find(original.id);
    ASSERT_NE(moved, nullptr);
    ASSERT_NE(moved->owner, c.eps[0]);
    Result retry;
    c.nodes[static_cast<std::size_t>(GetParam())]->submit(params(key), capture(retry));
    await_reply(c, retry);
    ASSERT_EQ(retry.status.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(retry.id, original.id);
    EXPECT_EQ(distinct_ids(c, key), std::vector<tautq::JobId>{original.id});
    expect_configured_owner(c, key, 0);
}

INSTANTIATE_TEST_SUITE_P(Paths, DrainingAdmission, testing::Values(0, 2),
                         [](const testing::TestParamInfo<int>& info) {
                             return info.param == 0 ? "DirectOwner" : "ForwardedGateway";
                         });

TEST(DrainingGateway, FreshOtherOwnerKeyKeepsStableRoute) {
    Cluster c(6004, taut::Impairments{.delay = 2ms}, 3, "drain-other-owner");
    const auto key = c.key_owned_by(1, "drain-other-owner");
    c.nodes[0]->drain([] {});
    Result reply;
    c.nodes[0]->submit(params(key), capture(reply));
    await_reply(c, reply);
    ASSERT_EQ(reply.status.value_or(999), tautq::qstatus::kCreated);
    const auto* created = c.nodes[1]->store().find(reply.id);
    ASSERT_NE(created, nullptr);
    EXPECT_EQ(created->owner, c.eps[1]);
    EXPECT_EQ(reply.id.origin, tautq::ekey(c.eps[1]));
    EXPECT_EQ(c.nodes[0]->counters().submits, 0u);
    EXPECT_EQ(distinct_ids(c, key), std::vector<tautq::JobId>{reply.id});
    expect_configured_owner(c, key, 1);
}

TEST(DrainingAdmissionControl, StopsGrantingLeasesBeforeHandoff) {
    Cluster c(6005, taut::Impairments{.delay = 2ms}, 3, "drain-lease-stop");
    Result original;
    c.nodes[0]->submit(params(c.key_owned_by(0, "drain-lease-stop")), capture(original));
    await_reply(c, original);
    ASSERT_EQ(original.status.value_or(999), tautq::qstatus::kCreated);
    c.nodes[0]->drain([] {});
    std::optional<std::uint32_t> status;
    c.nodes[0]->lease(50,
                      [&](std::uint32_t st, const tautq::QueueNode::LeaseGrant&) { status = st; });
    ASSERT_EQ(status.value_or(999), tautq::qstatus::kNoJob);
    EXPECT_EQ(c.nodes[0]->counters().leases_granted, 0u);
}
} // namespace
