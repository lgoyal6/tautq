#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "records.h"
#include "wal.h"

namespace tautq {

// The node's job table: an in-memory view rebuilt deterministically by folding log records,
// in order, through apply() — replay at open() and live commits go through the exact same
// function, so "what restart rebuilds" and "what the running node believes" cannot drift.
//
// commit() is the ONLY write path: encode record -> WAL append (fsync when sync) -> apply.
// A record is applied only after it is durable, matching DESIGN-protocol §3's commit rule.
class JobStore {
  public:
    // Test knob; call before open().
    void set_segment_bytes(std::size_t n) {
        wal_.set_segment_bytes(n);
    }

    bool open(const std::string& dir);

    // Append (durable if sync) then apply. Returns false only on IO failure.
    bool commit(const Record& r, bool sync = true);

    Job* find(const JobId& id);
    const Job* find(const JobId& id) const;
    Job* find_by_idem(const std::string& key);

    std::unordered_map<JobId, Job, JobIdHash>& jobs() {
        return jobs_;
    }
    std::uint64_t next_lsn() const {
        return wal_.next_lsn();
    }
    Wal& wal() {
        return wal_;
    }

  private:
    void apply(const Record& r);

    Wal wal_;
    std::unordered_map<JobId, Job, JobIdHash> jobs_;
    std::unordered_map<std::string, JobId> by_idem_;
};

} // namespace tautq
