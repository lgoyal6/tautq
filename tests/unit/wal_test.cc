#include "wal.h"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace fs = std::filesystem;

namespace {

std::vector<std::byte> body(const std::string& s) {
    std::vector<std::byte> out;
    for (char c : s) {
        out.push_back(std::byte{static_cast<unsigned char>(c)});
    }
    return out;
}

std::string str(std::span<const std::byte> b) {
    std::string s;
    for (auto x : b) {
        s.push_back(static_cast<char>(std::to_integer<unsigned char>(x)));
    }
    return s;
}

struct WalTest : ::testing::Test {
    std::string dir;

    void SetUp() override {
        dir = (fs::temp_directory_path() /
               ("tautq-wal-" +
                std::string(::testing::UnitTest::GetInstance()->current_test_info()->name())))
                  .string();
        fs::remove_all(dir);
    }
    void TearDown() override {
        fs::remove_all(dir);
    }

    std::vector<std::string> replay_all(tautq::Wal& w) {
        std::vector<std::string> out;
        EXPECT_TRUE(w.open(dir, [&](std::span<const std::byte> b) { out.push_back(str(b)); }));
        return out;
    }
};

TEST_F(WalTest, EmptyDirStartsClean) {
    tautq::Wal w;
    EXPECT_TRUE(replay_all(w).empty());
    EXPECT_EQ(w.next_lsn(), 0u);
}

TEST_F(WalTest, AppendThenReplayAfterReopen) {
    {
        tautq::Wal w;
        ASSERT_TRUE(replay_all(w).empty());
        ASSERT_TRUE(w.append(body("alpha")));
        ASSERT_TRUE(w.append(body("beta")));
        ASSERT_TRUE(w.append(body("gamma")));
        EXPECT_EQ(w.next_lsn(), 3u);
    }
    tautq::Wal w2;
    const auto got = replay_all(w2);
    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], "alpha");
    EXPECT_EQ(got[1], "beta");
    EXPECT_EQ(got[2], "gamma");
    EXPECT_EQ(w2.next_lsn(), 3u);

    // And the reopened log keeps appending where it left off.
    ASSERT_TRUE(w2.append(body("delta")));
    tautq::Wal w3;
    EXPECT_EQ(replay_all(w3).size(), 4u);
}

TEST_F(WalTest, TornTailIsTruncatedAndLogStaysUsable) {
    std::string seg;
    {
        tautq::Wal w;
        ASSERT_TRUE(replay_all(w).empty());
        ASSERT_TRUE(w.append(body("alpha")));
        ASSERT_TRUE(w.append(body("beta-longer-record")));
        seg = (fs::path(dir) / "wal-00000000000000000000.log").string();
    }
    // Simulate a crash mid-write: chop the last record in half.
    const auto full = fs::file_size(seg);
    fs::resize_file(seg, full - 9);

    tautq::Wal w2;
    auto got = replay_all(w2);
    ASSERT_EQ(got.size(), 1u) << "the torn record must be dropped";
    EXPECT_EQ(got[0], "alpha");
    EXPECT_EQ(w2.next_lsn(), 1u);

    // New appends land where the torn record was, and replay cleanly.
    ASSERT_TRUE(w2.append(body("gamma")));
    tautq::Wal w3;
    got = replay_all(w3);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[1], "gamma");
}

TEST_F(WalTest, CorruptTailCrcIsDropped) {
    std::string seg;
    {
        tautq::Wal w;
        ASSERT_TRUE(replay_all(w).empty());
        ASSERT_TRUE(w.append(body("alpha")));
        ASSERT_TRUE(w.append(body("beta")));
        seg = (fs::path(dir) / "wal-00000000000000000000.log").string();
    }
    // Flip one byte inside the LAST record's body.
    std::fstream f(seg, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(-1, std::ios::end);
    f.put('X');
    f.close();

    tautq::Wal w2;
    const auto got = replay_all(w2);
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0], "alpha");
}

TEST_F(WalTest, RotatesSegmentsAndReplaysAcrossThem) {
    {
        tautq::Wal w;
        w.set_segment_bytes(64); // force rotation every couple of records
        ASSERT_TRUE(replay_all(w).empty());
        for (int i = 0; i < 20; ++i) {
            ASSERT_TRUE(w.append(body("record-" + std::to_string(i))));
        }
    }
    std::size_t segments = 0;
    for (const auto& e : fs::directory_iterator(dir)) {
        (void)e;
        ++segments;
    }
    EXPECT_GT(segments, 3u) << "64-byte segments must have rotated many times";

    tautq::Wal w2;
    const auto got = replay_all(w2);
    ASSERT_EQ(got.size(), 20u);
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(got[static_cast<std::size_t>(i)], "record-" + std::to_string(i))
            << "replay must preserve append order across segments";
    }
}

TEST_F(WalTest, RotationAfterCompactionSyncsTheActiveGenerationDirectory) {
    tautq::Wal w;
    w.set_segment_bytes(32);
    std::vector<std::string> synced;
    w.set_directory_sync_hook_for_test([&](const std::string& path) {
        synced.push_back(path);
        return true;
    });
    ASSERT_TRUE(replay_all(w).empty());
    ASSERT_TRUE(w.append(body("before")));
    ASSERT_TRUE(w.compact({body("snapshot")}));
    synced.clear();

    ASSERT_TRUE(w.append(body(std::string(20, 'x')))); // forces rotation in wal-gen-1
    const std::string generation = dir + "/wal-gen-00000000000000000001";
    ASSERT_EQ(synced.size(), 1u);
    EXPECT_EQ(synced[0], generation);
}

TEST_F(WalTest, FailedCurrentParentSyncFailStopsUntilReopen) {
    tautq::Wal w;
    ASSERT_TRUE(replay_all(w).empty());
    ASSERT_TRUE(w.append(body("before")));
    int root_syncs = 0;
    w.set_directory_sync_hook_for_test([&](const std::string& path) {
        if (path == dir && ++root_syncs == 2)
            return false;
        return true;
    });

    EXPECT_FALSE(w.compact({body("snapshot")}));
    EXPECT_FALSE(w.append(body("must-not-be-acknowledged")));

    tautq::Wal reopened;
    std::vector<std::string> recovered;
    ASSERT_TRUE(
        reopened.open(dir, [&](std::span<const std::byte> b) { recovered.push_back(str(b)); }));
    ASSERT_EQ(recovered.size(), 1u);
    EXPECT_EQ(recovered[0], "snapshot");
}

TEST_F(WalTest, RejectsOversizedAndEmptyBodies) {
    tautq::Wal w;
    ASSERT_TRUE(replay_all(w).empty());
    EXPECT_FALSE(w.append({}));
    const std::vector<std::byte> big((64u << 10) + 1);
    EXPECT_FALSE(w.append(big));
}

} // namespace
