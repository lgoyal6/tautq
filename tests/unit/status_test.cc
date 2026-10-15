#include <algorithm>
#include <optional>

#include <gtest/gtest.h>

#include "cluster.h"

using namespace std::chrono_literals;
using tautq::test::Cluster;

namespace {

struct StatusResult {
    std::uint32_t status = tautq::qstatus::kUnknownJob;
    std::optional<tautq::Job> job;
};

tautq::QueueNode::SubmitParams params(const std::string& key) {
    tautq::QueueNode::SubmitParams p;
    p.idem_key = key;
    p.url = "http://sink:8081/hook";
    p.body = {std::byte{'x'}};
    return p;
}

tautq::JobId submit(Cluster& c, int gateway, const std::string& key) {
    std::optional<tautq::JobId> id;
    c.nodes[static_cast<std::size_t>(gateway)]->submit(
        params(key), [&id](std::uint32_t status, const tautq::JobId& result) {
            if (status == tautq::qstatus::kCreated || status == tautq::qstatus::kDuplicate) {
                id = result;
            }
        });
    for (int i = 0; i < 800 && !id; ++i) {
        c.step();
    }
    EXPECT_TRUE(id.has_value());
    return id.value_or(tautq::JobId{});
}

void query(Cluster& c, int gateway, const tautq::JobId& id, StatusResult& result) {
    c.nodes[static_cast<std::size_t>(gateway)]->status(
        id, [&result](std::uint32_t status, const std::optional<tautq::Job>& job) {
            result.status = status;
            result.job = job;
        });
    for (int i = 0; i < 1200 && !result.job && result.status == tautq::qstatus::kUnknownJob; ++i) {
        c.step();
    }
}

TEST(Status, GatewayRoutesToEmbeddedOwnerWhenItHasNoReplica) {
    Cluster c(11, taut::Impairments{.delay = 2ms}, 5, "status-owner");
    const std::string key = c.key_owned_by(1, "status-owner");
    const auto id = submit(c, 4, key);
    ASSERT_EQ(id.origin, tautq::ekey(c.eps[1]));
    const tautq::Job* owner_copy = c.nodes[1]->store().find(id);
    ASSERT_NE(owner_copy, nullptr);
    int gateway = -1;
    for (std::size_t i = 0; i < c.nodes.size(); ++i) {
        const bool replica = std::find(owner_copy->replicas.begin(), owner_copy->replicas.end(),
                                       c.eps[i]) != owner_copy->replicas.end();
        if (!replica) {
            gateway = static_cast<int>(i);
            break;
        }
    }
    ASSERT_GE(gateway, 0);
    EXPECT_EQ(c.nodes[static_cast<std::size_t>(gateway)]->store().find(id), nullptr)
        << "gateway is outside the job's three-node replica set";

    StatusResult result;
    query(c, gateway, id, result);
    ASSERT_EQ(result.status, tautq::qstatus::kCreated);
    ASSERT_TRUE(result.job.has_value());
    EXPECT_EQ(result.job->id, id);
    EXPECT_EQ(result.job->owner, c.eps[1]);
}

TEST(Status, FallsBackToReplicaWhenEmbeddedOwnerIsUnavailable) {
    Cluster c(12, taut::Impairments{.delay = 2ms}, 3, "status-fallback");
    const std::string key = c.key_owned_by(0, "status-fallback");
    const auto id = submit(c, 2, key);
    ASSERT_NE(c.nodes[2]->store().find(id), nullptr);
    c.crash(0);

    StatusResult result;
    query(c, 2, id, result);
    ASSERT_EQ(result.status, tautq::qstatus::kCreated);
    ASSERT_TRUE(result.job.has_value());
    EXPECT_EQ(result.job->id, id);
}

} // namespace
