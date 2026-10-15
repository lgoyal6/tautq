#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <vector>

namespace tautq {

// Append-only write-ahead log, written by hand per the spec (no embedded DB). Frame:
//
//   u32 body_len | u32 crc32c(body) | body
//
// data-sync-on-commit: append(sync=true) returns only after fdatasync (fsync on Darwin), a commit
// point per DESIGN-protocol §3 is durable when append returns. append(sync=false) + sync() is the
// group-commit path (M10's sanctioned optimization).
//
// Segments rotate at segment_bytes as wal-<start_lsn>.log; the directory is fsynced when a
// segment is created so the file itself survives a crash. On open, all segments replay in
// order; a short/corrupt frame in the FINAL segment is a torn tail from a mid-write crash
// and is truncated away; corruption anywhere earlier is real damage and open() fails —
// recovery for that is replication (RESYNC), not heroics here.
class Wal {
  public:
    using ReplayFn = std::function<void(std::span<const std::byte> body)>;
    enum class CompactStage { SnapshotDurable, Installed, BeforeOldLogDeletion };
    using CompactHook =
        std::function<bool(CompactStage)>; // false simulates a crash at a durable boundary

    Wal() = default;
    ~Wal();
    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    // Test knob; call before open(). Default 64 MiB.
    void set_segment_bytes(std::size_t n) {
        segment_bytes_ = n;
    }

    // Test seam for verifying which directory entries are synced and injecting sync failures.
    void set_directory_sync_hook_for_test(std::function<bool(const std::string&)> hook) {
        directory_sync_hook_ = hook;
    }

    // Open (creating if needed) and replay. Returns false on IO error / non-tail corruption.
    bool open(const std::string& dir, const ReplayFn& fn);

    bool append(std::span<const std::byte> body, bool sync = true);
    bool sync(); // data-sync the active segment
    bool compact(const std::vector<std::vector<std::byte>>& snapshot, const CompactHook& hook = {});

    // Records replayed + appended so far == the lsn the caller stamps into the next record.
    std::uint64_t next_lsn() const {
        return next_lsn_;
    }

  private:
    bool open_active(const std::string& path, bool create);
    bool rotate();
    bool sync_directory(const std::string& path);
    static std::string segment_name(std::uint64_t start_lsn);

    std::string dir_;
    std::string active_dir_;
    std::uint64_t generation_ = 0;
    int fd_ = -1;
    bool failed_ = false;
    std::size_t active_size_ = 0;
    std::size_t segment_bytes_ = 64u << 20;
    std::uint64_t next_lsn_ = 0;
    std::function<bool(const std::string&)> directory_sync_hook_;
};

} // namespace tautq
