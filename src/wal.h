#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string>

namespace tautq {

// Append-only write-ahead log, written by hand per the spec (no embedded DB). Frame:
//
//   u32 body_len | u32 crc32c(body) | body
//
// fsync-on-commit: append(sync=true) returns only after fdatasync — a commit point per
// DESIGN-protocol §3 is durable when append returns. append(sync=false) + sync() is the
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

    Wal() = default;
    ~Wal();
    Wal(const Wal&) = delete;
    Wal& operator=(const Wal&) = delete;

    // Test knob; call before open(). Default 64 MiB.
    void set_segment_bytes(std::size_t n) {
        segment_bytes_ = n;
    }

    // Open (creating if needed) and replay. Returns false on IO error / non-tail corruption.
    bool open(const std::string& dir, const ReplayFn& fn);

    bool append(std::span<const std::byte> body, bool sync = true);
    bool sync(); // fdatasync the active segment

    // Records replayed + appended so far == the lsn the caller stamps into the next record.
    std::uint64_t next_lsn() const {
        return next_lsn_;
    }

  private:
    bool open_active(const std::string& path, bool create);
    bool rotate();
    static std::string segment_name(std::uint64_t start_lsn);

    std::string dir_;
    int fd_ = -1;
    std::size_t active_size_ = 0;
    std::size_t segment_bytes_ = 64u << 20;
    std::uint64_t next_lsn_ = 0;
};

} // namespace tautq
