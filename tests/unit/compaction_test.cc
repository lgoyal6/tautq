#include <filesystem>
#include <string>

#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "store.h"

namespace fs = std::filesystem;

namespace {

struct TempStore {
    fs::path dir;
    explicit TempStore(const std::string& name)
        : dir(fs::temp_directory_path() / ("tautq-compaction-" + name)) {
        fs::remove_all(dir);
    }
    ~TempStore() {
        fs::remove_all(dir);
    }
};

tautq::Job job(std::uint64_t id, std::string key) {
    tautq::Job j;
    j.id = {7, id};
    j.idem_key = std::move(key);
    j.url = "http://sink/hook";
    j.body = {std::byte{'p'}};
    j.replicas[0].port_be = 1;
    j.owner = j.replicas[0];
    return j;
}

std::uintmax_t log_bytes(const fs::path& dir) {
    std::uintmax_t total = 0;
    if (!fs::exists(dir))
        return total;
    for (const auto& e : fs::recursive_directory_iterator(dir)) {
        if (e.is_regular_file() && e.path().extension() == ".log")
            total += e.file_size();
    }
    return total;
}

TEST(WalCompaction, SnapshotRetainsDedupAndFenceStateAndReducesHistory) {
    TempStore temp("state");
    tautq::Job expected_lease = job(1, "key-live");
    tautq::Job expected_done = job(2, "key-done");
    {
        tautq::JobStore store;
        store.set_segment_bytes(160);
        ASSERT_TRUE(store.open(temp.dir.string()));
        ASSERT_TRUE(store.commit(tautq::SubmitRec{expected_lease}));
        for (std::uint32_t seq = 1; seq <= 8; ++seq) {
            ASSERT_TRUE(store.commit(tautq::LeaseRec{expected_lease.id, 3, seq, 99}));
        }
        ASSERT_TRUE(store.commit(tautq::SubmitRec{expected_done}));
        ASSERT_TRUE(store.commit(tautq::DoneRec{expected_done.id, 2, 4}));
        expected_lease.state = tautq::JobState::Leased;
        expected_lease.epoch = 3;
        expected_lease.lease_seq = 8;
        expected_lease.attempts = 8;
        expected_done.state = tautq::JobState::Done;
        expected_done.epoch = 2;
        expected_done.lease_seq = 4;
        expected_done.attempts = 0;
        expected_done.body.clear();
        const auto before = log_bytes(temp.dir);
        ASSERT_TRUE(store.compact());
        EXPECT_LT(log_bytes(temp.dir / "wal-gen-00000000000000000001"), before);
    }
    tautq::JobStore replayed;
    replayed.set_segment_bytes(160);
    ASSERT_TRUE(replayed.open(temp.dir.string()));
    const auto* live = replayed.find_by_idem("key-live");
    ASSERT_NE(live, nullptr);
    EXPECT_EQ(live->state, expected_lease.state);
    EXPECT_EQ(live->epoch, expected_lease.epoch);
    EXPECT_EQ(live->lease_seq, expected_lease.lease_seq);
    EXPECT_EQ(live->attempts, expected_lease.attempts);
    EXPECT_EQ(live->id, expected_lease.id);
    const auto* done = replayed.find_by_idem("key-done");
    ASSERT_NE(done, nullptr);
    EXPECT_EQ(done->state, tautq::JobState::Done);
    EXPECT_EQ(done->body.size(), 0u);
    ASSERT_TRUE(replayed.commit(tautq::LeaseRec{expected_lease.id, 3, 9, 100}));
    EXPECT_EQ(replayed.find(expected_lease.id)->lease_seq, 9u);
    {
        tautq::JobStore after_append;
        after_append.set_segment_bytes(160);
        ASSERT_TRUE(after_append.open(temp.dir.string()));
        ASSERT_NE(after_append.find_by_idem("key-live"), nullptr);
        ASSERT_NE(after_append.find_by_idem("key-done"), nullptr);
        EXPECT_EQ(after_append.find_by_idem("key-live")->lease_seq, 9u);
        EXPECT_EQ(after_append.find_by_idem("key-done")->state, tautq::JobState::Done);
    }
}

TEST(WalCompaction, DuplicateIdempotencyKeyKeepsThePreCompactionWinner) {
    TempStore temp("duplicate-key-canonical-winner");
    const auto earlier_id = job(2, "duplicate-key");
    const auto selected_id = job(1, "duplicate-key");
    {
        tautq::JobStore store;
        ASSERT_TRUE(store.open(temp.dir.string()));
        ASSERT_TRUE(store.commit(tautq::SubmitRec{earlier_id}));
        ASSERT_TRUE(store.commit(tautq::SubmitRec{selected_id}));
        ASSERT_EQ(store.find_by_idem("duplicate-key")->id, selected_id.id);
        ASSERT_EQ(store.jobs().size(), 2u);
        ASSERT_TRUE(store.compact());
    }

    tautq::JobStore replayed;
    ASSERT_TRUE(replayed.open(temp.dir.string()));
    ASSERT_EQ(replayed.jobs().size(), 2u);
    const auto* selected = replayed.find_by_idem("duplicate-key");
    ASSERT_NE(selected, nullptr);
    EXPECT_EQ(selected->id, selected_id.id);
}

TEST(WalCompaction, CrashBeforeInstallReplaysOldGeneration) {
    TempStore temp("before-install");
    {
        tautq::JobStore store;
        ASSERT_TRUE(store.open(temp.dir.string()));
        ASSERT_TRUE(store.commit(tautq::SubmitRec{job(1, "stable-key")}));
        const bool ok = store.compact([](tautq::Wal::CompactStage stage) {
            return stage != tautq::Wal::CompactStage::SnapshotDurable;
        });
        EXPECT_FALSE(ok);
    }
    tautq::JobStore replayed;
    ASSERT_TRUE(replayed.open(temp.dir.string()));
    ASSERT_NE(replayed.find_by_idem("stable-key"), nullptr);
    EXPECT_EQ(replayed.find_by_idem("stable-key")->id.nonce, 1u);
}

TEST(WalCompaction, CrashAfterInstallBeforeCleanupReplaysNewGeneration) {
    TempStore temp("after-install");
    {
        tautq::JobStore store;
        ASSERT_TRUE(store.open(temp.dir.string()));
        ASSERT_TRUE(store.commit(tautq::SubmitRec{job(1, "stable-key")}));
        const bool ok = store.compact([](tautq::Wal::CompactStage stage) {
            return stage != tautq::Wal::CompactStage::BeforeOldLogDeletion;
        });
        EXPECT_FALSE(ok);
    }
    tautq::JobStore replayed;
    ASSERT_TRUE(replayed.open(temp.dir.string()));
    ASSERT_NE(replayed.find_by_idem("stable-key"), nullptr);
    EXPECT_EQ(replayed.find_by_idem("stable-key")->id.nonce, 1u);
    EXPECT_TRUE(fs::exists(temp.dir / "CURRENT"));
}

TEST(WalCompaction, ProcessCrashesAtDurableBoundariesReplayACompleteGeneration) {
    for (const auto stage :
         {tautq::Wal::CompactStage::SnapshotDurable, tautq::Wal::CompactStage::Installed,
          tautq::Wal::CompactStage::BeforeOldLogDeletion}) {
        const std::string name =
            stage == tautq::Wal::CompactStage::SnapshotDurable ? "process-before-install"
            : stage == tautq::Wal::CompactStage::Installed     ? "process-after-install"
                                                               : "process-before-delete";
        TempStore temp(name);
        {
            tautq::JobStore initial;
            ASSERT_TRUE(initial.open(temp.dir.string()));
            ASSERT_TRUE(initial.commit(tautq::SubmitRec{job(7, "crash-key")}));
            ASSERT_TRUE(initial.commit(tautq::LeaseRec{{7, 7}, 4, 3, 91}));
        }
        const pid_t child = ::fork();
        ASSERT_GE(child, 0);
        if (child == 0) {
            tautq::JobStore compactor;
            if (!compactor.open(temp.dir.string()))
                ::_exit(71);
            compactor.compact([stage](tautq::Wal::CompactStage reached) {
                if (reached == stage)
                    ::_exit(70);
                return true;
            });
            ::_exit(72);
        }
        int status = 0;
        ASSERT_EQ(::waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 70);
        tautq::JobStore replayed;
        ASSERT_TRUE(replayed.open(temp.dir.string()));
        const auto* recovered = replayed.find_by_idem("crash-key");
        ASSERT_NE(recovered, nullptr);
        EXPECT_EQ(recovered->state, tautq::JobState::Leased);
        EXPECT_EQ(recovered->epoch, 4u);
        EXPECT_EQ(recovered->lease_seq, 3u);
    }
}

} // namespace
