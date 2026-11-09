// Durability barrier (group commit) at the JobStore/WAL level: ordering, watermark,
// multi-segment coverage, IO-fault poisoning, power-loss truncation, compaction cut, and
// real SIGKILLs at every barrier crashpoint in forked children.

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "faults.h"
#include "store.h"

namespace fs = std::filesystem;
using tautq::JobState;
using tautq::JobStore;

namespace {

struct TempDir {
    fs::path dir;
    explicit TempDir(const std::string& name)
        : dir(fs::temp_directory_path() / ("tautq-gc-" + name + "-" + std::to_string(::getpid()))) {
        fs::remove_all(dir);
        tautq::faults::reset_for_test();
    }
    ~TempDir() {
        fs::remove_all(dir);
        tautq::faults::reset_for_test();
    }
};

tautq::Job job(std::uint64_t n) {
    tautq::Job j;
    j.id = {7, n};
    j.idem_key = "key-" + std::to_string(n);
    j.url = "http://sink/hook";
    j.body = std::vector<std::byte>(40, std::byte{'p'});
    j.replicas[0].port_be = 1;
    j.owner = j.replicas[0];
    return j;
}

tautq::Record submit(std::uint64_t n) {
    return tautq::Record{tautq::SubmitRec{job(n)}};
}

// Power loss: everything after each segment's last successful data sync disappears.
void drop_unsynced(const std::map<std::string, std::size_t>& synced) {
    for (const auto& [path, size] : synced) {
        if (fs::exists(path) && fs::file_size(path) > size) {
            fs::resize_file(path, size);
        }
    }
}

std::set<std::uint64_t> replayed_jobs(const fs::path& dir, std::size_t segment_bytes = 64u << 20) {
    JobStore s;
    s.set_segment_bytes(segment_bytes);
    EXPECT_TRUE(s.open(dir.string()));
    std::set<std::uint64_t> out;
    for (const auto& [id, j] : s.jobs()) {
        out.insert(id.nonce);
    }
    return out;
}

} // namespace

TEST(GroupCommit, NothingVisibleOrAcknowledgedBeforeTheBarrier) {
    TempDir t("order");
    JobStore s;
    s.set_group_commit({.max_batch_records = 64});
    ASSERT_TRUE(s.open(t.dir.string()));
    std::vector<std::uint64_t> released;
    for (std::uint64_t n = 1; n <= 10; ++n) {
        ASSERT_TRUE(s.commit_async(submit(n), [&, n](bool ok) {
            EXPECT_TRUE(ok);
            // Every waiter sees its own record (and all earlier ones) applied.
            for (std::uint64_t k = 1; k <= n; ++k) {
                EXPECT_NE(s.find({7, k}), nullptr);
            }
            released.push_back(n);
        }));
    }
    // Appended, not durable, not applied, nobody told.
    EXPECT_EQ(s.next_lsn(), 10u);
    EXPECT_EQ(s.durable_lsn(), 0u);
    EXPECT_EQ(s.find({7, 1}), nullptr);
    EXPECT_EQ(s.find_by_idem("key-1"), nullptr);
    EXPECT_TRUE(released.empty());
    EXPECT_EQ(s.pending_records(), 10u);

    s.pump();
    EXPECT_EQ(s.durable_lsn(), 10u);
    EXPECT_EQ(released, (std::vector<std::uint64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
    EXPECT_EQ(s.stats().flushes, 1u) << "one fsync for the whole batch";
    EXPECT_EQ(s.stats().records_committed, 10u);
}

TEST(GroupCommit, BatchOfOneIsOneFsyncPerRecordAndStillNeverReentrant) {
    TempDir t("batch1");
    JobStore s;
    s.set_group_commit({.max_batch_records = 1});
    ASSERT_TRUE(s.open(t.dir.string()));
    bool inside = false;
    int fired = 0;
    for (std::uint64_t n = 1; n <= 5; ++n) {
        inside = true;
        ASSERT_TRUE(s.commit_async(submit(n), [&](bool ok) {
            EXPECT_FALSE(inside) << "waiter ran inside commit_async";
            EXPECT_TRUE(ok);
            ++fired;
        }));
        inside = false;
        EXPECT_EQ(s.durable_lsn(), n) << "batch cap 1 syncs inline";
        EXPECT_EQ(s.find({7, n}), nullptr) << "apply waits for pump even when synced";
    }
    s.pump();
    EXPECT_EQ(fired, 5);
    EXPECT_EQ(s.stats().flushes, 5u);
}

TEST(GroupCommit, FlushCertifiesEverySegmentTheBatchTouched) {
    TempDir t("segments");
    JobStore s;
    s.set_segment_bytes(300); // ~3 submit frames per segment
    s.set_group_commit({.max_batch_records = 1000});
    ASSERT_TRUE(s.open(t.dir.string()));
    int ok_count = 0;
    for (std::uint64_t n = 1; n <= 20; ++n) {
        ASSERT_TRUE(s.commit_async(submit(n), [&](bool ok) { ok_count += ok ? 1 : 0; }));
    }
    std::size_t segments = 0;
    for (const auto& e : fs::directory_iterator(t.dir)) {
        segments += e.path().extension() == ".log" ? 1 : 0;
    }
    ASSERT_GE(segments, 4u) << "the batch must straddle several rotations";
    // Rotation flushed each outgoing segment; the watermark covers exactly those records.
    EXPECT_GT(s.durable_lsn(), 0u);
    EXPECT_LT(s.durable_lsn(), 20u);
    s.pump();
    EXPECT_EQ(ok_count, 20);
    EXPECT_EQ(s.durable_lsn(), 20u);
    // Every segment's synced size equals its file size: nothing acknowledged is unsynced.
    for (const auto& [path, size] : s.wal().synced_sizes()) {
        EXPECT_EQ(fs::file_size(path), size) << path;
    }
    const auto synced = s.wal().synced_sizes();
    drop_unsynced(synced);
    EXPECT_EQ(replayed_jobs(t.dir, 300).size(), 20u);
}

TEST(GroupCommit, PowerLossBeforeFlushLosesOnlyUnacknowledgedRecords) {
    TempDir t("powerloss");
    std::map<std::string, std::size_t> synced;
    std::set<std::uint64_t> acked;
    {
        JobStore s;
        s.set_segment_bytes(300);
        s.set_group_commit({.max_batch_records = 1000});
        ASSERT_TRUE(s.open(t.dir.string()));
        for (std::uint64_t n = 1; n <= 6; ++n) {
            ASSERT_TRUE(s.commit_async(submit(n), [&, n](bool ok) {
                if (ok)
                    acked.insert(n);
            }));
        }
        s.pump();
        for (std::uint64_t n = 7; n <= 15; ++n) { // second batch never flushed
            ASSERT_TRUE(s.commit_async(submit(n), [&, n](bool ok) {
                if (ok)
                    acked.insert(n);
            }));
        }
        synced = s.wal().synced_sizes();
        // "power loss": process gone, page cache gone.
    }
    drop_unsynced(synced);
    const auto replayed = replayed_jobs(t.dir, 300);
    EXPECT_EQ(acked, (std::set<std::uint64_t>{1, 2, 3, 4, 5, 6}));
    for (auto n : acked) {
        EXPECT_TRUE(replayed.count(n)) << "acknowledged record " << n << " lost";
    }
    // Rotations inside the unflushed batch may have certified some of it; whatever
    // survives must be a prefix (no holes): replay never skips a record.
    std::uint64_t expect = 1;
    for (auto n : replayed) {
        EXPECT_EQ(n, expect++);
    }
}

TEST(GroupCommit, FsyncFailurePoisonsTheBatchAndEverythingAfter) {
    TempDir t("fsyncfail");
    JobStore s;
    s.set_group_commit({.max_batch_records = 64});
    ASSERT_TRUE(s.open(t.dir.string()));
    std::vector<bool> results;
    ASSERT_TRUE(s.commit_async(submit(1), [&](bool ok) { results.push_back(ok); }));
    s.pump();
    ASSERT_EQ(results, std::vector<bool>{true});

    s.wal().set_io_fault_hook_for_test([](const char* op) {
        return std::strcmp(op, "sync") == 0 ? tautq::faults::WalFault::FailSync
                                            : tautq::faults::WalFault::None;
    });
    for (std::uint64_t n = 2; n <= 4; ++n) {
        ASSERT_TRUE(s.commit_async(submit(n), [&](bool ok) { results.push_back(ok); }));
    }
    s.pump();
    EXPECT_EQ(results, (std::vector<bool>{true, false, false, false}));
    EXPECT_TRUE(s.poisoned());
    EXPECT_EQ(s.find({7, 2}), nullptr) << "a failed batch is never applied";
    EXPECT_FALSE(s.commit_async(submit(5), [](bool) { ADD_FAILURE() << "must not be called"; }));
    EXPECT_FALSE(s.commit(submit(6)));
    s.wal().set_io_fault_hook_for_test({});

    // Process restart: the replay alone decides. The failed batch's bytes were written (page
    // cache survives a process restart), so they may come back; their clients were told
    // "unknown", never "failed" or "ok".
    const auto replayed = replayed_jobs(t.dir);
    EXPECT_TRUE(replayed.count(1));
    EXPECT_FALSE(replayed.count(5));
    EXPECT_FALSE(replayed.count(6));
}

TEST(GroupCommit, ShortWriteLeavesTornTailThatReplayTruncates) {
    TempDir t("shortwrite");
    std::vector<bool> results;
    {
        JobStore s;
        s.set_group_commit({.max_batch_records = 64});
        ASSERT_TRUE(s.open(t.dir.string()));
        ASSERT_TRUE(s.commit_async(submit(1), [&](bool ok) { results.push_back(ok); }));
        s.pump();
        ASSERT_TRUE(s.commit_async(submit(2), [&](bool ok) { results.push_back(ok); }));
        int writes = 0;
        s.wal().set_io_fault_hook_for_test([&](const char* op) {
            if (std::strcmp(op, "write") == 0 && ++writes == 1)
                return tautq::faults::WalFault::ShortWrite; // disk full mid-frame
            return tautq::faults::WalFault::None;
        });
        EXPECT_FALSE(s.commit_async(submit(3), [](bool) { ADD_FAILURE(); }));
        s.pump();
        EXPECT_TRUE(s.poisoned());
    }
    EXPECT_EQ(results, (std::vector<bool>{true, false}));
    const auto replayed = replayed_jobs(t.dir);
    EXPECT_TRUE(replayed.count(1));
    EXPECT_FALSE(replayed.count(3)) << "half a frame must never replay";
    // Record 2 was fully written before the torn frame; it may replay (outcome unknown).
    // The store is writable again after the torn tail is truncated.
    JobStore again;
    ASSERT_TRUE(again.open(t.dir.string()));
    EXPECT_TRUE(again.commit(submit(9)));
}

TEST(GroupCommit, RotationSyncFailurePoisons) {
    TempDir t("rotfail");
    JobStore s;
    s.set_segment_bytes(300);
    s.set_group_commit({.max_batch_records = 1000});
    ASSERT_TRUE(s.open(t.dir.string()));
    s.wal().set_io_fault_hook_for_test([](const char* op) {
        return std::strcmp(op, "sync") == 0 ? tautq::faults::WalFault::FailSync
                                            : tautq::faults::WalFault::None;
    });
    std::vector<bool> results;
    bool refused = false;
    for (std::uint64_t n = 1; n <= 10 && !refused; ++n) {
        refused = !s.commit_async(submit(n), [&](bool ok) { results.push_back(ok); });
    }
    EXPECT_TRUE(refused) << "the rotation's sync failure must stop further appends";
    s.pump();
    for (bool ok : results) {
        EXPECT_FALSE(ok);
    }
    EXPECT_TRUE(s.poisoned());
}

TEST(GroupCommit, DirectorySyncFailureAtRotationPoisons) {
    TempDir t("dirfail");
    JobStore s;
    s.set_segment_bytes(300);
    s.set_group_commit({.max_batch_records = 1000});
    int dir_syncs = 0;
    s.wal().set_directory_sync_hook_for_test([&](const std::string&) { return ++dir_syncs < 2; });
    ASSERT_TRUE(s.open(t.dir.string())); // first directory sync (segment create) succeeds
    std::vector<bool> results;
    bool refused = false;
    for (std::uint64_t n = 1; n <= 10 && !refused; ++n) {
        refused = !s.commit_async(submit(n), [&](bool ok) { results.push_back(ok); });
    }
    EXPECT_TRUE(refused);
    s.pump();
    for (bool ok : results) {
        EXPECT_FALSE(ok) << "a record in a segment whose dir entry is not durable was acked";
    }
}

TEST(GroupCommit, PendingBoundRefusesBeforeAppending) {
    TempDir t("bound");
    JobStore s;
    s.set_group_commit({.max_batch_records = 1000, .max_pending_records = 3});
    ASSERT_TRUE(s.open(t.dir.string()));
    for (std::uint64_t n = 1; n <= 3; ++n) {
        ASSERT_TRUE(s.commit_async(submit(n), [](bool) {}));
    }
    const auto lsn = s.next_lsn();
    EXPECT_FALSE(s.commit_async(submit(4), [](bool) { ADD_FAILURE(); }));
    EXPECT_EQ(s.next_lsn(), lsn) << "refused record must not touch the log";
    EXPECT_EQ(s.stats().refused_full, 1u);
    EXPECT_FALSE(s.poisoned());
    s.pump();
    EXPECT_TRUE(s.commit_async(submit(4), [](bool) {}));
}

TEST(GroupCommit, CompactionCutsAtTheWatermarkAndWaitersAppendToTheNewGeneration) {
    TempDir t("compact");
    std::vector<std::uint64_t> acked;
    {
        JobStore s;
        s.set_group_commit({.max_batch_records = 1000});
        ASSERT_TRUE(s.open(t.dir.string()));
        for (std::uint64_t n = 1; n <= 5; ++n) {
            ASSERT_TRUE(s.commit_async(submit(n), [&, n](bool ok) {
                ASSERT_TRUE(ok);
                acked.push_back(n);
                if (n == 5) {
                    // A waiter appending during compaction must land after the cut.
                    EXPECT_EQ(s.wal().generation(), 1u);
                    ASSERT_TRUE(s.commit_async(submit(100), [&](bool ok2) {
                        if (ok2)
                            acked.push_back(100);
                    }));
                }
            }));
        }
        ASSERT_TRUE(s.compact());
        EXPECT_EQ(s.wal().generation(), 1u);
        EXPECT_EQ(acked.size(), 5u) << "waiters run after the new generation is installed";
        s.pump();
        EXPECT_EQ(acked.size(), 6u);
    }
    const auto replayed = replayed_jobs(t.dir);
    for (auto n : acked) {
        EXPECT_TRUE(replayed.count(n)) << n;
    }
}

TEST(GroupCommit, CompactionFailureAfterPointerSwapPoisonsTheStore) {
    TempDir t("compactfail");
    JobStore s;
    s.set_group_commit({.max_batch_records = 1000});
    ASSERT_TRUE(s.open(t.dir.string()));
    ASSERT_TRUE(s.commit(submit(1)));
    ASSERT_TRUE(s.commit_async(submit(2), [](bool) {}));
    // Fail at "installed": CURRENT points at the new generation, old one not yet deleted.
    EXPECT_FALSE(s.compact(
        [](tautq::Wal::CompactStage st) { return st != tautq::Wal::CompactStage::Installed; }));
    EXPECT_TRUE(s.poisoned());
    const auto replayed = replayed_jobs(t.dir);
    EXPECT_TRUE(replayed.count(1));
    EXPECT_TRUE(replayed.count(2)) << "flushed before the cut, so it is in the snapshot";
}

TEST(GroupCommit, DoneSurvivesAHigherEpochNonDoneCopy) {
    // Campaign finding (fault profile): a successor's unconfirmed TAKEOVER pushed a
    // Leased epoch-2 copy that erased a committed DONE on a replica, the claim grant then
    // reported a live lease, and the job completed twice. Done must be absolute.
    TempDir t("done-absolute");
    JobStore s;
    ASSERT_TRUE(s.open(t.dir.string()));
    tautq::Job j = job(1);
    ASSERT_TRUE(s.commit(tautq::SubmitRec{j}));
    ASSERT_TRUE(s.commit(tautq::LeaseRec{j.id, 1, 1, 9}));
    ASSERT_TRUE(s.commit(tautq::DoneRec{j.id, 1, 1}));
    tautq::Job pushed = j;
    pushed.state = JobState::Leased;
    pushed.epoch = 2;
    pushed.lease_seq = 2;
    pushed.attempts = 2;
    pushed.owner.port_be = 2;
    ASSERT_TRUE(s.commit(tautq::ReplicateRec{pushed}));
    const auto* now = s.find(j.id);
    ASSERT_NE(now, nullptr);
    EXPECT_EQ(now->state, JobState::Done);
    EXPECT_EQ(now->epoch, 2u) << "ownership still moves forward";
    EXPECT_EQ(now->lease_seq, 1u) << "the committed completion fence survives repair";
    EXPECT_EQ(now->attempts, 1u) << "the terminal job keeps its completed attempt count";
    EXPECT_EQ(now->owner, pushed.owner);
    JobStore replay;
    ASSERT_TRUE(replay.open(t.dir.string()));
    EXPECT_EQ(replay.find(j.id)->state, JobState::Done);
    EXPECT_EQ(replay.find(j.id)->lease_seq, 1u);
    EXPECT_EQ(replay.find(j.id)->attempts, 1u);
}

TEST(GroupCommit, DoneRecordCarriesItsLeaseSequenceIntoTheSnapshot) {
    TempDir t("done-lease-seq");
    const tautq::Job j = job(2); // stale local copy, still at lease_seq 0
    tautq::Job snapshot;
    {
        JobStore s;
        ASSERT_TRUE(s.open(t.dir.string()));
        ASSERT_TRUE(s.commit(tautq::SubmitRec{j}));
        ASSERT_TRUE(s.commit(tautq::TakeoverRec{j.id, 2, j.owner}));

        // A successor learned DONE(seq=1) from a claim majority and committed that fact at
        // its new epoch. Its own local pre-takeover copy had not seen the lease record.
        ASSERT_TRUE(s.commit(tautq::DoneRec{j.id, 2, 1}));
        const auto* done = s.find(j.id);
        ASSERT_NE(done, nullptr);
        EXPECT_EQ(done->state, JobState::Done);
        EXPECT_EQ(done->epoch, 2u);
        EXPECT_EQ(done->lease_seq, 1u)
            << "replicating this terminal snapshot must preserve the committed DONE token";
        EXPECT_EQ(done->attempts, 1u)
            << "the stale owner had not seen the grant, but DONE proves it was issued";
        snapshot = *done;
    }

    JobStore replay;
    ASSERT_TRUE(replay.open(t.dir.string()));
    ASSERT_NE(replay.find(j.id), nullptr);
    EXPECT_EQ(replay.find(j.id)->state, JobState::Done);
    EXPECT_EQ(replay.find(j.id)->epoch, 2u);
    EXPECT_EQ(replay.find(j.id)->lease_seq, 1u);

    // A full-state REPLICATE export to a stale peer must carry the same winning token.
    JobStore replica;
    const fs::path replica_dir = t.dir / "replica";
    ASSERT_TRUE(replica.open(replica_dir.string()));
    ASSERT_TRUE(replica.commit(tautq::ReplicateRec{snapshot}));
    ASSERT_NE(replica.find(j.id), nullptr);
    EXPECT_EQ(replica.find(j.id)->state, JobState::Done);
    EXPECT_EQ(replica.find(j.id)->epoch, 2u);
    EXPECT_EQ(replica.find(j.id)->lease_seq, 1u);
}

TEST(GroupCommit, StaleDoneRecordCannotCompleteANewerLease) {
    TempDir t("done-seq-monotone");
    JobStore s;
    ASSERT_TRUE(s.open(t.dir.string()));
    const tautq::Job j = job(3);
    ASSERT_TRUE(s.commit(tautq::SubmitRec{j}));
    ASSERT_TRUE(s.commit(tautq::LeaseRec{j.id, 1, 2, 9}));
    ASSERT_TRUE(s.commit(tautq::DoneRec{j.id, 1, 1}));

    const auto* same_epoch = s.find(j.id);
    ASSERT_NE(same_epoch, nullptr);
    EXPECT_EQ(same_epoch->state, JobState::Leased)
        << "an older token cannot complete the current lease";
    EXPECT_EQ(same_epoch->epoch, 1u);
    EXPECT_EQ(same_epoch->lease_seq, 2u);
    EXPECT_EQ(same_epoch->attempts, 2u);

    ASSERT_TRUE(s.commit(tautq::TakeoverRec{j.id, 2, j.owner}));
    ASSERT_TRUE(s.commit(tautq::DoneRec{j.id, 2, 1}));
    const auto* higher_epoch = s.find(j.id);
    ASSERT_NE(higher_epoch, nullptr);
    EXPECT_EQ(higher_epoch->state, JobState::Leased)
        << "a newer epoch does not make an older lease token current";
    EXPECT_EQ(higher_epoch->epoch, 2u);
    EXPECT_EQ(higher_epoch->lease_seq, 2u);
    EXPECT_EQ(higher_epoch->attempts, 2u);
}

// ---- real SIGKILL crash matrix (forked children, real filesystem) -------------------------

namespace {

// Child: append batches of submits with random flush points, compact now and then, and
// write every ACKNOWLEDGED job number to `ack_fd` (an O_APPEND file, written only from the
// barrier waiter). The configured crashpoint SIGKILLs it mid-stream.
[[noreturn]] void crash_child(const std::string& dir, int ack_fd, const std::string& point,
                              std::uint64_t nth, std::uint32_t seed) {
    tautq::faults::reset_for_test();
    tautq::faults::set_crashpoint_for_test(point, nth);
    JobStore s;
    s.set_segment_bytes(700);
    s.set_group_commit({.max_batch_records = 8});
    if (!s.open(dir)) {
        ::_exit(3);
    }
    std::mt19937 rng(seed);
    for (std::uint64_t n = 1; n <= 400; ++n) {
        const bool ok = s.commit_async(submit(n), [ack_fd, n](bool committed) {
            if (committed) {
                const std::string line = std::to_string(n) + "\n";
                if (::write(ack_fd, line.data(), line.size()) < 0) {
                    ::_exit(4);
                }
            }
        });
        if (!ok) {
            ::_exit(5);
        }
        if (rng() % 5 == 0) {
            s.pump();
        }
        if (rng() % 97 == 0) {
            if (!s.compact()) {
                ::_exit(6);
            }
        }
    }
    s.pump();
    ::_exit(0); // crashpoint never hit (nth beyond the run)
}

} // namespace

TEST(GroupCommitCrash, SigkillAtEveryBarrierPointNeverLosesAnAcknowledgedRecord) {
    const std::vector<std::string> points = {"wal.before_write",
                                             "wal.after_write_before_sync",
                                             "wal.after_sync_before_apply",
                                             "compact.snapshot_durable",
                                             "compact.installed",
                                             "compact.before_old_log_deletion"};
    int killed = 0;
    int trials = 0;
    std::mt19937 rng(20261104);
    for (const auto& point : points) {
        for (int trial = 0; trial < 8; ++trial) {
            TempDir t("crash-" + std::to_string(trials));
            const fs::path ack_path = t.dir.string() + ".acks";
            fs::remove(ack_path);
            const int ack_fd = ::open(ack_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            ASSERT_GE(ack_fd, 0);
            // Hit counts per run: ~400 writes, ~70-90 flushes, ~4 compactions.
            const bool compact_point = point.rfind("compact.", 0) == 0;
            const bool sync_point = point == "wal.after_sync_before_apply";
            const std::uint64_t nth =
                compact_point ? 1 + rng() % 2 : (sync_point ? 1 + rng() % 50 : 1 + rng() % 300);
            const auto seed = static_cast<std::uint32_t>(rng());
            const pid_t pid = ::fork();
            ASSERT_GE(pid, 0);
            if (pid == 0) {
                crash_child(t.dir.string(), ack_fd, point, nth, seed);
            }
            int status = 0;
            ASSERT_EQ(::waitpid(pid, &status, 0), pid);
            ::close(ack_fd);
            ++trials;
            if (WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) {
                ++killed;
            } else {
                ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0)
                    << point << " child status " << status;
            }
            std::set<std::uint64_t> acked;
            {
                std::ifstream in(ack_path);
                std::uint64_t n = 0;
                while (in >> n) {
                    acked.insert(n);
                }
            }
            JobStore s;
            s.set_segment_bytes(700);
            ASSERT_TRUE(s.open(t.dir.string())) << point << " nth=" << nth;
            std::set<std::uint64_t> replayed;
            for (const auto& [id, j] : s.jobs()) {
                replayed.insert(id.nonce);
                EXPECT_EQ(j.state, JobState::Ready);
            }
            for (auto n : acked) {
                EXPECT_TRUE(replayed.count(n))
                    << point << " nth=" << nth << ": acknowledged job " << n << " lost";
            }
            std::uint64_t expect = 1;
            for (auto n : replayed) {
                EXPECT_EQ(n, expect++) << point << ": replay has a hole";
            }
            // And the recovered store accepts new work.
            EXPECT_TRUE(s.commit(submit(10000)));
            fs::remove(ack_path);
        }
    }
    EXPECT_EQ(trials, 48);
    EXPECT_GE(killed, 40) << "most trials must actually die at their crashpoint";
    RecordProperty("trials", trials);
    RecordProperty("killed", killed);
}
