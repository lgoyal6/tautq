#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <span>
#include <string>
#include <vector>

#include "faults.h"

namespace tautq {

// Append-only write-ahead log, written by hand per the spec (no embedded DB). Frame:
//
//   u32 body_len | u32 crc32c(body) | body
//
// data-sync-on-commit: append(sync=true) returns only after fdatasync (fsync on Darwin), a commit
// point per DESIGN-protocol §3 is durable when append returns. append(sync=false) + sync() is the
// group-commit path: JobStore's coordinator appends a batch, calls sync() once, and only then
// treats the batch as committed. durable_lsn() is the watermark: every record with an lsn below
// it is on stable storage. Rotation data-syncs the outgoing segment before opening the next one
// (and fsyncs the directory for the new file), so a sync() of the active descriptor certifies
// every earlier segment too; a failed sync, write or rotation poisons the log (failed()) and
// nothing appended after the watermark may ever be reported durable by this process.
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

    // Per-instance fault seam (in-process cluster tests give each node its own disk faults).
    // op is "write" or "sync"; consulted in addition to the process-global faults:: config.
    void set_io_fault_hook_for_test(std::function<faults::WalFault(const char* op)> hook) {
        io_fault_hook_ = std::move(hook);
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
    // Every record with lsn < durable_lsn() has passed a successful data sync (this generation).
    std::uint64_t durable_lsn() const {
        return durable_lsn_;
    }
    bool failed() const {
        return failed_;
    }
    std::uint64_t generation() const {
        return generation_;
    }
    // Test/verifier aid: segment path -> byte size covered by its last successful data sync.
    // Truncating each segment to this size models losing all unsynced page-cache data.
    const std::map<std::string, std::size_t>& synced_sizes() const {
        return synced_sizes_;
    }

  private:
    bool open_active(const std::string& path, bool create);
    bool rotate();
    bool sync_directory(const std::string& path);
    bool sync_fd(int fd);
    faults::WalFault fault(const char* op);
    static std::string segment_name(std::uint64_t start_lsn);

    std::string dir_;
    std::string active_dir_;
    std::string active_path_;
    std::uint64_t generation_ = 0;
    int fd_ = -1;
    bool failed_ = false;
    std::size_t active_size_ = 0;
    std::size_t segment_bytes_ = 64u << 20;
    std::uint64_t next_lsn_ = 0;
    std::uint64_t durable_lsn_ = 0;
    std::map<std::string, std::size_t> synced_sizes_;
    std::function<bool(const std::string&)> directory_sync_hook_;
    std::function<faults::WalFault(const char*)> io_fault_hook_;
};

} // namespace tautq
