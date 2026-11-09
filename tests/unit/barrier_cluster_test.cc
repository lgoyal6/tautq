// Cluster-level durability barrier, quorum, admission, drain and compaction behavior over
// SimNet. The durability oracle `durable_copy_has` answers "if every node lost power right
// now, which nodes would still have this state?" by copying a node's data dir, cutting every
// segment back to its last successful data sync, and replaying the copy.

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "cluster.h"

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using tautq::JobState;
using tautq::test::Cluster;

namespace tautq {
class QueueNodeTestAccess {
  public:
    static void deliver_replica_ack(QueueNode& node, const JobId& id, std::size_t slot,
                                    const Job& confirmed) {
        node.mark_replica_ok(id, slot, confirmed);
    }
};
} // namespace tautq

namespace {

tautq::QueueNode::SubmitParams params(const std::string& key, std::size_t body = 1) {
    tautq::QueueNode::SubmitParams p;
    p.idem_key = key;
    p.url = "http://sink:8081/hook";
    p.body = std::vector<std::byte>(body, std::byte{'x'});
    return p;
}

struct Result {
    std::optional<std::uint32_t> st;
    tautq::JobId id{};
};

tautq::QueueNode::SubmitCb capture(Result& r) {
    return [&r](std::uint32_t st, const tautq::JobId& id) {
        r.st = st;
        r.id = id;
    };
}

void await(Cluster& c, const std::optional<std::uint32_t>& st, int steps = 600) {
    for (int i = 0; i < steps && !st; ++i) {
        c.step();
    }
}

// The client contract: a retryable 503 (kNoQuorum: e.g. the first forward to a just-restarted
// owner timing out while sessions re-establish) is retried with the same key.
Result submit_with_retries(Cluster& c, int gateway, const std::string& key, int attempts = 5) {
    Result r;
    for (int a = 0; a < attempts; ++a) {
        r = Result{};
        c.nodes[static_cast<std::size_t>(gateway)]->submit(params(key), capture(r));
        await(c, r.st);
        if (r.st != tautq::qstatus::kNoQuorum)
            break;
    }
    return r;
}

// Power-loss oracle for one node (see file comment).
std::optional<tautq::Job> durable_copy(Cluster& c, int node, const tautq::JobId& id) {
    const fs::path src = c.base_dir + "/node" + std::to_string(node);
    const fs::path dst = c.base_dir + "/oracle";
    fs::remove_all(dst);
    fs::copy(src, dst, fs::copy_options::recursive);
    for (const auto& [path, size] :
         c.nodes[static_cast<std::size_t>(node)]->store().wal().synced_sizes()) {
        const auto rel = fs::relative(path, src);
        const fs::path target = dst / rel;
        if (fs::exists(target) && fs::file_size(target) > size) {
            fs::resize_file(target, size);
        }
    }
    std::optional<tautq::Job> out;
    {
        tautq::JobStore s;
        EXPECT_TRUE(s.open(dst.string()));
        if (const auto* j = s.find(id)) {
            out = *j;
        }
    }
    fs::remove_all(dst);
    return out;
}

int durable_copies(Cluster& c, const tautq::JobId& id, JobState at_least_state = JobState::Ready) {
    int n = 0;
    for (int i = 0; i < static_cast<int>(c.nodes.size()); ++i) {
        // Crashed nodes count too: their files (cut to the last sync) are what a restart sees.
        const auto j = durable_copy(c, i, id);
        if (j && (at_least_state != JobState::Done || j->state == JobState::Done)) {
            ++n;
        }
    }
    return n;
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

std::function<tautq::faults::WalFault(const char*)> fail_syncs() {
    return [](const char* op) {
        return std::strcmp(op, "sync") == 0 ? tautq::faults::WalFault::FailSync
                                            : tautq::faults::WalFault::None;
    };
}

struct Grant {
    std::optional<std::uint32_t> st;
    tautq::QueueNode::LeaseGrant g;
};

Grant lease(Cluster& c, int node, std::uint64_t worker = 1) {
    Grant r;
    c.nodes[static_cast<std::size_t>(node)]->lease(
        worker, [&](std::uint32_t s, const tautq::QueueNode::LeaseGrant& g) {
            r.st = s;
            r.g = g;
        });
    await(c, r.st);
    return r;
}

std::optional<std::uint32_t> ack(Cluster& c, int node, const tautq::QueueNode::LeaseGrant& g,
                                 bool ok = true) {
    std::optional<std::uint32_t> st;
    c.nodes[static_cast<std::size_t>(node)]->ack(g.id, g.epoch, g.lease_seq, ok,
                                                 [&](std::uint32_t s) { st = s; });
    await(c, st);
    return st;
}

} // namespace

// ---- quorum-recovery: no client success before local + replica barrier ------------------

TEST(Barrier, SubmitAckImpliesPowerLossDurableOnOwnerAndAReplica) {
    Cluster c(101, taut::Impairments{.delay = 2ms, .jitter = 6ms}, 3, "bar-submit");
    std::map<std::string, Result> results;
    int checked = 0;
    for (int k = 0; k < 24; ++k) {
        const std::string key = "s-" + std::to_string(k);
        const int gw = k % 3;
        c.nodes[static_cast<std::size_t>(gw)]->submit(
            params(key), [&, key](std::uint32_t st, const tautq::JobId& id) {
                results[key] = Result{st, id};
                if (st == tautq::qstatus::kCreated) {
                    // At the instant of the client's success, a power loss everywhere must
                    // still leave >= 2 copies (W=2 of the pinned set, counting the owner).
                    EXPECT_GE(durable_copies(c, id), 2) << key;
                    ++checked;
                }
            });
        c.step();
    }
    for (int i = 0; i < 600 && results.size() < 24; ++i) {
        c.step();
    }
    EXPECT_EQ(checked, 24);
}

TEST(Barrier, DoneAckImpliesPowerLossDurableDoneOnAMajority) {
    Cluster c(102, taut::Impairments{.delay = 2ms, .jitter = 4ms}, 3, "bar-done");
    const std::string key = c.key_owned_by(0, "d");
    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    await(c, r.st);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    const auto g = lease(c, 0);
    ASSERT_EQ(g.st.value_or(999), tautq::qstatus::kCreated);
    // The lease grant itself was quorum-durable before the worker heard about it.
    int leased_copies = 0;
    for (int i = 0; i < 3; ++i) {
        const auto j = durable_copy(c, i, r.id);
        leased_copies += j && j->lease_seq == g.g.lease_seq ? 1 : 0;
    }
    EXPECT_GE(leased_copies, 2);
    std::optional<std::uint32_t> st;
    c.nodes[0]->ack(g.g.id, g.g.epoch, g.g.lease_seq, true, [&](std::uint32_t s) {
        st = s;
        if (s == tautq::qstatus::kCreated) {
            EXPECT_GE(durable_copies(c, r.id, JobState::Done), 2);
        }
    });
    await(c, st);
    EXPECT_EQ(st.value_or(999), tautq::qstatus::kCreated);
}

TEST(Barrier, DelayedOldAckCannotClearNewerDoneRepair) {
    tautq::NodeConfig cfg;
    cfg.repair_interval = 10s;
    Cluster c(701, taut::Impairments{.delay = 2ms}, 3, "bar-stale-repair-ack", cfg);
    const std::string key = c.key_owned_by(0, "stale-repair");
    Result submitted;
    c.nodes[0]->submit(params(key), capture(submitted));
    await(c, submitted.st);
    ASSERT_EQ(submitted.st.value_or(999), tautq::qstatus::kCreated);

    const Grant grant = lease(c, 0);
    ASSERT_EQ(grant.st.value_or(999), tautq::qstatus::kCreated);
    c.run(20); // settle the lease acknowledgments before simulating a delayed response
    const tautq::Job stale_lease = *c.nodes[0]->store().find(submitted.id);
    ASSERT_EQ(stale_lease.state, JobState::Leased);

    std::size_t missing_slot = 0;
    int missing_peer = -1;
    for (std::size_t slot = 1; slot < stale_lease.replicas.size(); ++slot) {
        if (stale_lease.replicas[slot] == taut::Endpoint{}) {
            continue;
        }
        for (std::size_t i = 0; i < c.eps.size(); ++i) {
            if (c.eps[i] == stale_lease.replicas[slot]) {
                missing_slot = slot;
                missing_peer = static_cast<int>(i);
            }
        }
    }
    ASSERT_GE(missing_peer, 0);
    c.crash(missing_peer);

    EXPECT_EQ(ack(c, 0, grant.g).value_or(999), tautq::qstatus::kCreated);
    ASSERT_EQ(c.nodes[0]->repair_backlog(), 1u);
    EXPECT_EQ(durable_copies(c, submitted.id, JobState::Done), 2);

    // Model a replica response sent for the older LEASE but delivered after the newer DONE
    // transition. It must not satisfy the current repair slot.
    tautq::QueueNodeTestAccess::deliver_replica_ack(*c.nodes[0], submitted.id, missing_slot,
                                                    stale_lease);
    EXPECT_EQ(c.nodes[0]->repair_backlog(), 1u);
    EXPECT_EQ(ack(c, 0, grant.g).value_or(999), tautq::qstatus::kNoQuorum);
    EXPECT_EQ(durable_copies(c, submitted.id, JobState::Done), 2);
}

TEST(Barrier, ReplicaFsyncFailureNeverYieldsAFalseSubmitAck) {
    Cluster c(103, taut::Impairments{.delay = 2ms}, 3, "bar-replica-fail");
    const std::string key = c.key_owned_by(0, "rf");
    c.nodes[1]->store().wal().set_io_fault_hook_for_test(fail_syncs());
    c.nodes[2]->store().wal().set_io_fault_hook_for_test(fail_syncs());
    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    await(c, r.st);
    ASSERT_TRUE(r.st.has_value());
    EXPECT_NE(*r.st, tautq::qstatus::kCreated) << "no replica passed its barrier";
    EXPECT_TRUE(c.nodes[1]->store().poisoned());
    EXPECT_TRUE(c.nodes[2]->store().poisoned());

    // Crash-only recovery for the replicas, then the same key resolves to the same job.
    c.restart(1);
    c.restart(2);
    c.run(1000); // repair interval
    Result again;
    c.nodes[1]->submit(params(key), capture(again));
    await(c, again.st);
    EXPECT_EQ(again.st.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(again.id, r.id);
    EXPECT_EQ(distinct_ids(c, key).size(), 1u);
    EXPECT_GE(durable_copies(c, r.id), 2) << "repair re-replicated after restart";
}

TEST(Barrier, OwnerFsyncFailureIsUnknownAndRetryNeverMintsATwin) {
    for (const bool power_loss : {false, true}) {
        Cluster c(104, taut::Impairments{.delay = 2ms}, 3,
                  power_loss ? "bar-owner-fail-pl" : "bar-owner-fail");
        const std::string key = c.key_owned_by(0, "of");
        Result warm;
        c.nodes[0]->submit(params(key + "-warm"), capture(warm));
        await(c, warm.st);
        ASSERT_EQ(warm.st.value_or(999), tautq::qstatus::kCreated);

        c.nodes[0]->store().wal().set_io_fault_hook_for_test(fail_syncs());
        Result r;
        c.nodes[1]->submit(params(key), capture(r)); // through a gateway
        await(c, r.st);
        ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kUnknown);
        // The barrier failed locally, so nothing was ever sent to replicas.
        EXPECT_EQ(c.nodes[1]->store().find_by_idem(key), nullptr);
        EXPECT_EQ(c.nodes[2]->store().find_by_idem(key), nullptr);

        const auto synced = c.nodes[0]->store().wal().synced_sizes();
        c.crash(0);
        if (power_loss) {
            for (const auto& [path, size] : synced) {
                if (fs::exists(path) && fs::file_size(path) > size)
                    fs::resize_file(path, size);
            }
        }
        c.restart(0);
        c.run(200); // peers notice the new boot id and re-establish sessions
        const Result retry = submit_with_retries(c, 2, key);
        if (power_loss) {
            EXPECT_EQ(retry.st.value_or(999), tautq::qstatus::kCreated) << "record was lost";
        } else {
            EXPECT_EQ(retry.st.value_or(999), tautq::qstatus::kDuplicate)
                << "process crash keeps page cache: the unknown submit survived";
            EXPECT_EQ(retry.id, r.id);
        }
        c.run(400);
        EXPECT_EQ(distinct_ids(c, key).size(), 1u) << (power_loss ? "power loss" : "crash");
        EXPECT_GE(durable_copies(c, retry.id), 2);
    }
}

TEST(Barrier, RetryRacingTheBarrierJoinsTheOriginal) {
    Cluster c(105, taut::Impairments{.delay = 2ms}, 3, "bar-race");
    const std::string key = c.key_owned_by(0, "race");
    Result a;
    Result b;
    Result d;
    c.nodes[0]->submit(params(key), capture(a));
    c.nodes[0]->submit(params(key), capture(b)); // same pass: SUBMIT still behind the barrier
    c.nodes[2]->submit(params(key), capture(d)); // forwarded retry
    EXPECT_FALSE(a.st.has_value());
    EXPECT_FALSE(b.st.has_value()) << "a racing retry must wait for the original's barrier";
    for (int i = 0; i < 600 && !(a.st && b.st && d.st); ++i) {
        c.step();
    }
    EXPECT_EQ(a.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(b.st.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(d.st.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(b.id, a.id);
    EXPECT_EQ(d.id, a.id);
    EXPECT_EQ(distinct_ids(c, key).size(), 1u);
}

TEST(Barrier, OwnerCrashAfterLocalFlushBeforeQuorumKeepsOneJob) {
    Cluster c(106, taut::Impairments{.delay = 20ms}, 3, "bar-owner-crash");
    const std::string key = c.key_owned_by(0, "oc");
    Result r;
    c.nodes[1]->submit(params(key), capture(r));
    // Step until the owner's SUBMIT is durable+applied, but before any replica has it.
    for (int i = 0; i < 3000 && c.nodes[0]->store().find_by_idem(key) == nullptr && !r.st; ++i) {
        c.step(1ms);
    }
    ASSERT_FALSE(r.st.has_value()) << "status " << r.st.value_or(999);
    ASSERT_NE(c.nodes[0]->store().find_by_idem(key), nullptr);
    ASSERT_EQ(c.nodes[2]->store().find_by_idem(key), nullptr);
    c.crash(0);
    await(c, r.st, 2000);
    ASSERT_TRUE(r.st.has_value());
    EXPECT_NE(*r.st, tautq::qstatus::kCreated) << "no quorum was ever reached";
    c.restart(0);
    c.run(200);
    const Result retry = submit_with_retries(c, 1, key);
    EXPECT_EQ(retry.st.value_or(999), tautq::qstatus::kDuplicate);
    c.run(600);
    EXPECT_EQ(distinct_ids(c, key).size(), 1u);
    EXPECT_GE(durable_copies(c, retry.id), 2);
}

TEST(Barrier, QuorumLossStallsSubmitAndLeaseInsteadOfAcknowledging) {
    Cluster c(107, taut::Impairments{.delay = 2ms}, 3, "bar-quorum-loss");
    const std::string key = c.key_owned_by(0, "ql");
    Result first;
    c.nodes[0]->submit(params(key), capture(first));
    await(c, first.st);
    ASSERT_EQ(first.st.value_or(999), tautq::qstatus::kCreated);
    // Replicas die silently (membership has not noticed): nothing can reach quorum.
    c.crash(1);
    c.crash(2);
    Result r;
    c.nodes[0]->submit(params(c.key_owned_by(0, "ql2")), capture(r));
    await(c, r.st, 3000);
    EXPECT_EQ(r.st.value_or(999), tautq::qstatus::kNoQuorum);
    const auto g = lease(c, 0);
    EXPECT_NE(g.st.value_or(999), tautq::qstatus::kCreated);
}

// Campaign finding (fault profile): after a failed submit quorum, a same-key retry got
// "duplicate" (200) for a job no replica held, and the job was leasable while replicas
// answered unknown-job, burning its attempts into dead-letter. Both are W=1 promises.
TEST(Barrier, UnreplicatedSubmitIsNeitherADuplicateSuccessNorLeasable) {
    Cluster c(110, taut::Impairments{.delay = 2ms}, 3, "bar-unconfirmed");
    const std::string key = c.key_owned_by(0, "uc");
    c.crash(1);
    c.crash(2); // silently: membership still says alive, so W=2 is still required
    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    await(c, r.st, 2000);
    ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kNoQuorum);
    ASSERT_NE(c.nodes[0]->store().find_by_idem(key), nullptr) << "locally durable";
    Result retry;
    c.nodes[0]->submit(params(key), capture(retry));
    await(c, retry.st, 2000);
    EXPECT_EQ(retry.st.value_or(999), tautq::qstatus::kNoQuorum)
        << "a retry must not be told 'accepted' while only the owner has the job";
    const auto g = lease(c, 0);
    EXPECT_EQ(g.st.value_or(999), tautq::qstatus::kNoJob) << "unreplicated job was leasable";
    c.restart(1);
    c.restart(2);
    c.run(800); // repair interval + session re-establishment
    Result again;
    c.nodes[0]->submit(params(key), capture(again));
    await(c, again.st);
    EXPECT_EQ(again.st.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_GE(durable_copies(c, again.id), 2);
    const auto g2 = lease(c, 0);
    EXPECT_EQ(g2.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(g2.g.attempt, 1u) << "no attempts were burned while unreplicated";
}

// Validation must see committed state. A lease() racing the EXPIRE that requeued a nacked
// job must not drop the queue entry on the pre-EXPIRE (still Leased) view.
TEST(Barrier, LeaseRacingAPendingExpireKeepsTheJobQueued) {
    Cluster c(108, taut::Impairments{.delay = 2ms}, 3, "bar-lease-race");
    const std::string key = c.key_owned_by(0, "lr");
    Result r;
    c.nodes[0]->submit(params(key), capture(r));
    await(c, r.st);
    const auto g = lease(c, 0);
    ASSERT_EQ(g.st.value_or(999), tautq::qstatus::kCreated);
    std::optional<std::uint32_t> nack;
    std::optional<std::uint32_t> racing;
    c.nodes[0]->ack(g.g.id, g.g.epoch, g.g.lease_seq, false, [&](std::uint32_t s) { nack = s; });
    c.nodes[0]->lease(2, [&](std::uint32_t s, const tautq::QueueNode::LeaseGrant&) { racing = s; });
    await(c, nack);
    EXPECT_EQ(nack.value_or(999), tautq::qstatus::kCreated);
    EXPECT_NE(racing.value_or(999), tautq::qstatus::kCreated) << "job is backing off";
    c.run(300); // > first-attempt backoff
    const auto again = lease(c, 0, 3);
    EXPECT_EQ(again.st.value_or(999), tautq::qstatus::kCreated) << "queue entry was dropped";
}

// A late ack for an expired lease racing a new LEASE that is still behind the barrier must be
// judged against the committed new lease (fenced), not the stale Ready view (amnesty).
TEST(Barrier, LateAckRacingAPendingLeaseIsFencedNotAmnestied) {
    Cluster c(109, taut::Impairments{.delay = 2ms}, 3, "bar-ack-race");
    const std::string key = c.key_owned_by(0, "ar");
    tautq::QueueNode::SubmitParams p = params(key);
    p.visibility_ms = 300;
    Result r;
    c.nodes[0]->submit(p, capture(r));
    await(c, r.st);
    const auto first = lease(c, 0, 1);
    ASSERT_EQ(first.st.value_or(999), tautq::qstatus::kCreated);
    c.run(200); // visibility timeout: back to Ready, nobody re-leased yet
    ASSERT_EQ(c.nodes[0]->store().find(r.id)->state, JobState::Ready);
    Grant second;
    std::optional<std::uint32_t> late;
    c.nodes[0]->lease(2, [&](std::uint32_t s, const tautq::QueueNode::LeaseGrant& g) {
        second.st = s;
        second.g = g;
    });
    // Same pass: the LEASE{seq 2} record is appended but not yet committed.
    c.nodes[0]->ack(first.g.id, first.g.epoch, first.g.lease_seq, true,
                    [&](std::uint32_t s) { late = s; });
    for (int i = 0; i < 600 && !(second.st && late); ++i)
        c.step();
    ASSERT_EQ(second.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(late.value_or(999), tautq::qstatus::kNotLeased)
        << "the stale token was validated against speculative pre-lease state";
    EXPECT_EQ(ack(c, 0, second.g).value_or(999), tautq::qstatus::kCreated);
}

// Randomized delayed/reordered/duplicated/lost datagrams with an owner crash mid-stream.
// Invariants: every submit success is power-loss durable on >= 2 nodes at ack time; one job
// per key; at most one DONE lease_seq per job; every acked job completes after healing.
TEST(Barrier, ReorderedDuplicatedLossyTrafficWithCrashKeepsInvariants) {
    for (std::uint64_t seed : {201u, 202u, 203u}) {
        Cluster c(seed, taut::Impairments{.loss = 0.03, .dup = 0.1, .delay = 2ms, .jitter = 15ms},
                  3, "bar-chaos-" + std::to_string(seed));
        std::mt19937 rng(static_cast<std::uint32_t>(seed));
        std::map<std::string, Result> res;
        std::set<std::string> created;
        int crash_at = 40 + static_cast<int>(rng() % 30);
        int crashed = -1;
        int false_acks = 0;
        for (int step = 0; step < 900; ++step) {
            if (step < 120 && step % 2 == 0) {
                const std::string key = "c-" + std::to_string(step);
                int gw = static_cast<int>(rng() % 3);
                if (gw == crashed)
                    gw = (gw + 1) % 3;
                c.nodes[static_cast<std::size_t>(gw)]->submit(
                    params(key), [&, key](std::uint32_t st, const tautq::JobId& id) {
                        res[key] = Result{st, id};
                        if (st == tautq::qstatus::kCreated) {
                            created.insert(key);
                            if (durable_copies(c, id) < 2)
                                ++false_acks;
                        }
                    });
            }
            if (step == crash_at) {
                crashed = static_cast<int>(rng() % 3);
                c.crash(crashed);
            }
            if (step == crash_at + 60) {
                c.restart(crashed);
                crashed = -1;
            }
            c.step();
        }
        EXPECT_EQ(false_acks, 0) << "seed " << seed;
        // Retry everything not created with the same key (the client contract), then drain
        // the queue through a worker on every node.
        for (auto& [key, r] : res) {
            if (r.st == tautq::qstatus::kCreated || r.st == tautq::qstatus::kDuplicate)
                continue;
            Result retry;
            c.nodes[0]->submit(params(key), capture(retry));
            await(c, retry.st, 2000);
            if (retry.st == tautq::qstatus::kCreated || retry.st == tautq::qstatus::kDuplicate)
                created.insert(key);
        }
        for (int round = 0; round < 400; ++round) {
            bool any = false;
            for (int n = 0; n < 3; ++n) {
                const auto g = lease(c, n, 10 + static_cast<std::uint64_t>(n));
                if (g.st == tautq::qstatus::kCreated) {
                    any = true;
                    ack(c, n, g.g);
                }
            }
            if (!any)
                c.run(20);
            bool all_done = true;
            for (const auto& key : created) {
                const auto ids = distinct_ids(c, key);
                const tautq::Job* j = nullptr;
                for (const auto& node : c.nodes)
                    if (!ids.empty() && (j = node->store().find(ids[0])) != nullptr &&
                        j->state == JobState::Done)
                        break;
                all_done = all_done && j != nullptr && j->state == JobState::Done;
            }
            if (all_done)
                break;
        }
        for (const auto& key : created) {
            const auto ids = distinct_ids(c, key);
            EXPECT_EQ(ids.size(), 1u) << "seed " << seed << " key " << key;
            bool done = false;
            for (const auto& node : c.nodes) {
                const auto* j = ids.empty() ? nullptr : node->store().find(ids[0]);
                done = done || (j != nullptr && j->state == JobState::Done);
            }
            EXPECT_TRUE(done) << "seed " << seed << " acknowledged " << key << " never completed";
        }
        EXPECT_GT(created.size(), 40u) << "seed " << seed;
    }
}

// ---- bounded admission --------------------------------------------------------------------

namespace {
tautq::NodeConfig admission_cfg(std::size_t inflight, std::size_t backlog, double resume = 0.5) {
    tautq::NodeConfig cfg;
    cfg.admission.enabled = true;
    cfg.admission.max_inflight_submits = inflight;
    cfg.admission.max_backlog_jobs = backlog;
    cfg.admission.resume_ratio = resume;
    return cfg;
}
} // namespace

TEST(Admission, RefusesUniqueWorkPromptlyBeforeAllocatingAndAdmitsAgainWithHysteresis) {
    Cluster c(301, taut::Impairments{.delay = 30ms}, 3, "adm-inflight", admission_cfg(4, 1000));
    std::vector<Result> res(10);
    const auto lsn_before = c.nodes[0]->store().next_lsn();
    for (int k = 0; k < 10; ++k) {
        c.nodes[0]->submit(params(c.key_owned_by(0, "i" + std::to_string(k))), capture(res[k]));
    }
    // Refusals are synchronous: decided before any id, log append or network round trip.
    int refused_now = 0;
    for (int k = 4; k < 10; ++k) {
        refused_now += res[k].st == tautq::qstatus::kOverloaded ? 1 : 0;
    }
    EXPECT_EQ(refused_now, 6);
    EXPECT_TRUE(c.nodes[0]->shedding());
    EXPECT_EQ(c.nodes[0]->inflight_submits(), 4u);
    EXPECT_EQ(c.nodes[0]->counters().refused[static_cast<int>(tautq::RefuseReason::Inflight)], 6u);
    c.step();
    EXPECT_EQ(c.nodes[0]->store().next_lsn(), lsn_before + 4) << "refused work never logged";

    // Hysteresis: one answered (3 in flight > 0.5*4) still sheds; all answered admits.
    while (c.nodes[0]->inflight_submits() > 3)
        c.step(1ms);
    if (c.nodes[0]->inflight_submits() == 3) {
        Result mid;
        c.nodes[0]->submit(params(c.key_owned_by(0, "mid")), capture(mid));
        EXPECT_EQ(mid.st.value_or(999), tautq::qstatus::kOverloaded);
    }
    for (int i = 0; i < 400 && c.nodes[0]->inflight_submits() > 0; ++i)
        c.step();
    for (int k = 0; k < 4; ++k) {
        EXPECT_EQ(res[k].st.value_or(999), tautq::qstatus::kCreated);
    }
    Result after;
    c.nodes[0]->submit(params(c.key_owned_by(0, "after")), capture(after));
    EXPECT_FALSE(after.st.has_value()) << "admitted (answered later), not refused";
    await(c, after.st);
    EXPECT_EQ(after.st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_FALSE(c.nodes[0]->shedding());
}

TEST(Admission, IdempotentRetriesAreResolvedNotRefusedWhileShedding) {
    Cluster c(302, taut::Impairments{.delay = 30ms}, 3, "adm-dup", admission_cfg(2, 1000));
    const std::string committed = c.key_owned_by(0, "dc");
    Result first;
    c.nodes[0]->submit(params(committed), capture(first));
    await(c, first.st);
    ASSERT_EQ(first.st.value_or(999), tautq::qstatus::kCreated);
    // Fill the in-flight budget and trip shedding.
    Result a;
    Result b;
    Result refused;
    const std::string pending_key = c.key_owned_by(0, "pa");
    c.nodes[0]->submit(params(pending_key), capture(a));
    c.nodes[0]->submit(params(c.key_owned_by(0, "pb")), capture(b));
    c.nodes[0]->submit(params(c.key_owned_by(0, "pc")), capture(refused));
    ASSERT_EQ(refused.st.value_or(999), tautq::qstatus::kOverloaded);
    ASSERT_TRUE(c.nodes[0]->shedding());
    // A retry of a committed key: answered with the original id, not refused.
    Result dup;
    c.nodes[0]->submit(params(committed), capture(dup));
    EXPECT_EQ(dup.st.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(dup.id, first.id);
    // A retry of a key still behind the barrier: joins it.
    Result join;
    c.nodes[0]->submit(params(pending_key), capture(join));
    EXPECT_FALSE(join.st.has_value());
    for (int i = 0; i < 400 && !(a.st && join.st); ++i)
        c.step();
    EXPECT_EQ(join.st.value_or(999), tautq::qstatus::kDuplicate);
    EXPECT_EQ(join.id, a.id);
    EXPECT_EQ(distinct_ids(c, pending_key).size(), 1u);
}

TEST(Admission, BacklogLimitShedsUntilWorkersDrainBelowTheResumeLevel) {
    Cluster c(303, taut::Impairments{.delay = 2ms}, 3, "adm-backlog", admission_cfg(100, 5, 0.6));
    for (int k = 0; k < 5; ++k) {
        Result r;
        c.nodes[0]->submit(params(c.key_owned_by(0, "b" + std::to_string(k))), capture(r));
        await(c, r.st);
        ASSERT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    }
    EXPECT_EQ(c.nodes[0]->store().owned_active(), 5u);
    Result over;
    c.nodes[0]->submit(params(c.key_owned_by(0, "b-over")), capture(over));
    EXPECT_EQ(over.st.value_or(999), tautq::qstatus::kOverloaded);
    EXPECT_EQ(c.nodes[0]->counters().refused[static_cast<int>(tautq::RefuseReason::Backlog)], 1u);
    // Workers keep making progress while new work is refused.
    for (int k = 0; k < 3; ++k) {
        const auto g = lease(c, 0);
        ASSERT_EQ(g.st.value_or(999), tautq::qstatus::kCreated);
        ASSERT_EQ(ack(c, 0, g.g).value_or(999), tautq::qstatus::kCreated);
    }
    EXPECT_EQ(c.nodes[0]->store().owned_active(), 2u);
    Result again;
    c.nodes[0]->submit(params(c.key_owned_by(0, "b-again")), capture(again));
    await(c, again.st);
    EXPECT_EQ(again.st.value_or(999), tautq::qstatus::kCreated);
}

TEST(Admission, GatewayForwardBoundRefusesWithoutAllocating) {
    auto cfg = admission_cfg(1000, 100000);
    cfg.admission.max_gateway_forwards = 2;
    Cluster c(304, taut::Impairments{.delay = 30ms}, 3, "adm-gw", cfg);
    std::vector<Result> res(4);
    for (int k = 0; k < 4; ++k) {
        c.nodes[1]->submit(params(c.key_owned_by(0, "g" + std::to_string(k))), capture(res[k]));
    }
    EXPECT_EQ(res[2].st.value_or(999), tautq::qstatus::kOverloaded);
    EXPECT_EQ(res[3].st.value_or(999), tautq::qstatus::kOverloaded);
    EXPECT_EQ(c.nodes[1]->counters().refused[static_cast<int>(tautq::RefuseReason::Gateway)], 2u);
    for (int i = 0; i < 400 && !(res[0].st && res[1].st); ++i)
        c.step();
    EXPECT_EQ(res[0].st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(res[1].st.value_or(999), tautq::qstatus::kCreated);
    EXPECT_EQ(c.nodes[1]->gateway_forwards(), 0u);
}

// A submit storm against an admission-bounded node: memory gauges stay within their limits,
// refusals happen, and leases/acks/repair keep completing work the whole time. The same
// storm without admission grows the backlog without bound (the control arm).
TEST(Admission, SubmitStormCannotStarveCompletionAndStaysBounded) {
    struct Outcome {
        std::size_t max_inflight = 0;
        std::size_t max_backlog = 0;
        std::size_t max_pending = 0;
        std::uint64_t completed = 0;
        std::uint64_t refused = 0;
        std::uint64_t created = 0;
    };
    const auto run = [](bool admission) {
        auto cfg = admission ? admission_cfg(64, 200, 0.8) : tautq::NodeConfig{};
        Cluster c(305, taut::Impairments{.delay = 2ms, .jitter = 2ms}, 3,
                  admission ? "adm-storm-on" : "adm-storm-off", cfg);
        Outcome o;
        int next = 0;
        for (int step = 0; step < 600; ++step) {
            for (int k = 0; k < 25; ++k) { // 25 unique submits per 5ms step = 5000/s offered
                const int gw = next % 3;
                c.nodes[static_cast<std::size_t>(gw)]->submit(
                    params("st-" + std::to_string(next++), 64),
                    [&o](std::uint32_t st, const tautq::JobId&) {
                        o.refused += st == tautq::qstatus::kOverloaded ? 1 : 0;
                        o.created += st == tautq::qstatus::kCreated ? 1 : 0;
                    });
            }
            // One worker per node, one lease per step each, acked as soon as granted.
            for (std::size_t n = 0; n < 3; ++n) {
                c.nodes[n]->lease(
                    7, [&c, &o, n](std::uint32_t s, const tautq::QueueNode::LeaseGrant& g) {
                        if (s != tautq::qstatus::kCreated)
                            return;
                        c.nodes[n]->ack(g.id, g.epoch, g.lease_seq, true, [&o](std::uint32_t a) {
                            o.completed += a == tautq::qstatus::kCreated ? 1 : 0;
                        });
                    });
            }
            c.step();
            for (std::size_t n = 0; n < 3; ++n) {
                o.max_inflight = std::max(o.max_inflight, c.nodes[n]->inflight_submits());
                o.max_backlog = std::max(o.max_backlog, c.nodes[n]->store().owned_active());
                o.max_pending = std::max(o.max_pending, c.nodes[n]->store().pending_records());
            }
        }
        return o;
    };
    const Outcome on = run(true);
    const Outcome off = run(false);
    EXPECT_LE(on.max_inflight, 64u);
    EXPECT_LE(on.max_backlog, 200u);
    EXPECT_GT(on.refused, 1000u);
    EXPECT_GT(on.completed, 300u) << "completion must progress during the storm";
    EXPECT_EQ(off.refused, 0u);
    EXPECT_GT(off.max_backlog, 5u * on.max_backlog) << "control arm: unbounded backlog";
    EXPECT_GT(on.completed * 10, off.completed * 9) << "admission must not cost completions";
    RecordProperty("on_completed", static_cast<int>(on.completed));
    RecordProperty("off_completed", static_cast<int>(off.completed));
    RecordProperty("on_max_backlog", static_cast<int>(on.max_backlog));
    RecordProperty("off_max_backlog", static_cast<int>(off.max_backlog));
}

// ---- compaction / drain / restart with pending writes ------------------------------------

TEST(DrainBarrier, DrainDuringAnOpenBatchAnswersPendingSubmitsAndPreservesThem) {
    Cluster c(401, taut::Impairments{.delay = 2ms}, 3, "drain-batch");
    std::vector<Result> res(5);
    std::vector<std::string> keys;
    for (int k = 0; k < 5; ++k) {
        keys.push_back(c.key_owned_by(0, "dr" + std::to_string(k)));
        c.nodes[0]->submit(params(keys.back()), capture(res[static_cast<std::size_t>(k)]));
    }
    ASSERT_GT(c.nodes[0]->store().pending_records(), 0u) << "batch still open";
    bool drained = false;
    c.nodes[0]->drain([&] { drained = true; });
    // Fresh unique work is refused; the open batch is not abandoned.
    Result fresh;
    c.nodes[0]->submit(params(c.key_owned_by(0, "dr-fresh")), capture(fresh));
    EXPECT_EQ(fresh.st.value_or(999), tautq::qstatus::kNoQuorum);
    for (int i = 0; i < 3000 && !drained; ++i)
        c.step();
    EXPECT_TRUE(drained);
    for (const auto& r : res) {
        EXPECT_EQ(r.st.value_or(999), tautq::qstatus::kCreated);
    }
    // Restart the drained node: every acknowledged job is still in the cluster and moved.
    c.restart(0);
    c.run(200);
    for (std::size_t k = 0; k < res.size(); ++k) {
        EXPECT_EQ(distinct_ids(c, keys[k]).size(), 1u);
        EXPECT_GE(durable_copies(c, res[k].id), 2);
        const auto* j = c.nodes[0]->store().find(res[k].id);
        ASSERT_NE(j, nullptr);
        EXPECT_NE(j->owner, c.eps[0]) << "execution ownership was handed off";
    }
}

TEST(CompactionBarrier, CompactingMidStreamThenRestartingKeepsEveryAcknowledgedJob) {
    Cluster c(402, taut::Impairments{.delay = 2ms, .jitter = 3ms}, 3, "compact-stream");
    std::map<std::string, Result> res;
    int compactions = 0;
    for (int step = 0; step < 300; ++step) {
        if (step < 200) {
            const std::string key = "cs-" + std::to_string(step);
            c.nodes[static_cast<std::size_t>(step % 3)]->submit(
                params(key), [&res, key](std::uint32_t st, const tautq::JobId& id) {
                    res[key] = Result{st, id};
                });
        }
        if (step % 37 == 5) {
            // Put a record behind the barrier on its owner, then compact everyone: the cut
            // must include it (flushed first) and never snapshot unflushed state.
            const std::string extra = "cs-x-" + std::to_string(step);
            c.nodes[static_cast<std::size_t>(c.ring_owner_index(extra))]->submit(
                params(extra), [&res, extra](std::uint32_t st, const tautq::JobId& id) {
                    res[extra] = Result{st, id};
                });
            for (auto& node : c.nodes) {
                const bool had_pending = node->store().pending_records() > 0;
                ASSERT_TRUE(node->store().compact());
                compactions += had_pending ? 1 : 0;
            }
        }
        if (step % 3 == 0) {
            for (int n = 0; n < 3; ++n) {
                const auto g = lease(c, n);
                if (g.st == tautq::qstatus::kCreated)
                    ack(c, n, g.g);
            }
        }
        c.step();
    }
    EXPECT_GE(compactions, 3);
    for (int n = 0; n < 3; ++n) {
        c.crash(n);
        c.restart(n);
    }
    int acked = 0;
    for (const auto& [key, r] : res) {
        if (r.st != tautq::qstatus::kCreated)
            continue;
        ++acked;
        const int owner = c.ring_owner_index(key);
        const auto* j = c.nodes[static_cast<std::size_t>(owner)]->store().find(r.id);
        ASSERT_NE(j, nullptr) << key;
        EXPECT_EQ(c.nodes[static_cast<std::size_t>(owner)]->store().find_by_idem(key), j);
        EXPECT_GE(durable_copies(c, r.id), 2) << key;
    }
    EXPECT_GT(acked, 150);
}
