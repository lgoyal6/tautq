#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "records.h"
#include "wal.h"

namespace tautq {

// Group-commit policy for the node-local durability barrier. max_batch_records == 1 is the
// unbatched baseline: every append is data-synced immediately (inline), exactly one fsync
// per record. Larger batches flush when the cap is hit or when the owner pumps the store at
// the end of an event-loop pass (flush-on-drain: at low load a pass holds one record, so no
// delay is added; under load a pass accumulates many). linger optionally holds a partial
// batch open for more arrivals (0 = never wait).
struct GroupCommitConfig {
    std::size_t max_batch_records = 256;
    std::size_t max_batch_bytes = 1u << 20;
    std::chrono::microseconds linger{0};
    // Hard bound on appended-but-uncommitted records (memory bound for waiters). Appends
    // beyond it are refused before they touch the log.
    std::size_t max_pending_records = 8192;
};

struct GroupCommitStats {
    std::uint64_t flushes = 0;
    std::uint64_t records_committed = 0;
    std::uint64_t bytes_committed = 0;
    std::uint64_t flush_failures = 0;
    std::uint64_t poisoned_waiters = 0; // waiters released with failure by a poisoned batch
    std::uint64_t refused_full = 0;     // appends refused at max_pending_records
    std::uint64_t flush_ns_total = 0;
    std::uint64_t flush_ns_max = 0;
    std::uint64_t last_batch = 0;
    std::uint64_t max_batch = 0;
    // batch size histogram: [1], [2-3], [4-7], ... [>=512]
    std::uint64_t batch_hist[11] = {};
};

// The node's job table: an in-memory view rebuilt deterministically by folding log records,
// in order, through apply() — replay at open() and live commits go through the exact same
// function, so "what restart rebuilds" and "what the running node believes" cannot drift.
//
// Durability barrier (group commit): commit_async() encodes + appends a record and parks its
// waiter; NOTHING is applied yet. flush() data-syncs once for the whole pending batch, and
// only after that sync succeeds are the records applied, in LSN order, and their waiters
// released, in the same order. So the visible job table never contains a record that is not
// on stable storage, and every caller-visible success (RPC reply, HTTP response) is issued
// from a waiter, i.e. after its barrier. A failed write/sync poisons the store: every pending
// waiter is released with ok=false (outcome UNKNOWN to its client: the bytes may or may not
// survive), nothing is applied, and every later append fails. Recovery is a process restart,
// whose replay is the only thing allowed to decide which unsynced records survived.
class JobStore {
  public:
    using Clock = std::chrono::steady_clock;
    using CommitCb = std::function<void(bool ok)>;

    // Test knob; call before open().
    void set_segment_bytes(std::size_t n) {
        wal_.set_segment_bytes(n);
    }
    void set_group_commit(const GroupCommitConfig& c) {
        gc_ = c;
    }
    const GroupCommitConfig& group_commit() const {
        return gc_;
    }
    // Owned non-terminal jobs (the admission backlog) are counted for this endpoint.
    void set_self(const taut::Endpoint& self) {
        self_ = self;
        has_self_ = true;
    }

    bool open(const std::string& dir);

    // Synchronous commit: append, then flush everything pending (this record included).
    // Pending waiters released by that flush run before this returns. Returns false on IO
    // failure (poisoned); the record may or may not survive a restart.
    bool commit(const Record& r, bool sync = true);

    // Barrier commit. Returns false (cb NOT called, nothing appended) when the store is
    // poisoned or the pending bound is reached; the caller reports a refusal/unavailable.
    // Otherwise cb(ok) is called later from flush()/pump(), never re-entrantly from here.
    bool commit_async(const Record& r, CommitCb cb);

    // Flush if policy says so (batch full, pass ended, linger elapsed). Call once per event
    // loop pass. end_of_pass=true is the flush-on-drain trigger.
    void pump(bool end_of_pass = true);
    // Unconditional flush of the pending batch (no-op when empty). Returns false if poisoned.
    bool flush();
    // When the next linger deadline falls (for the loop timeout); max() when none.
    Clock::time_point next_deadline() const;

    // Replace transition history with one full-state record per retained job. All
    // idempotency keys and live lease fences are retained; disk remains O(unique jobs).
    // The snapshot is cut at the durable watermark: pending records are flushed and applied
    // first (their waiters run only after the new generation is installed, so any record a
    // waiter appends lands in the new generation), and a snapshot is never built from
    // unflushed state.
    bool compact(const Wal::CompactHook& hook = {});

    Job* find(const JobId& id);
    const Job* find(const JobId& id) const;
    Job* find_by_idem(const std::string& key);

    std::unordered_map<JobId, Job, JobIdHash>& jobs() {
        return jobs_;
    }
    std::uint64_t next_lsn() const {
        return wal_.next_lsn();
    }
    std::uint64_t durable_lsn() const {
        return wal_.durable_lsn();
    }
    std::size_t pending_records() const {
        return pending_.size() + committed_.size();
    }
    std::size_t pending_bytes() const {
        return pending_bytes_;
    }
    bool poisoned() const {
        return poisoned_;
    }
    std::size_t owned_active() const {
        return owned_active_;
    }
    const GroupCommitStats& stats() const {
        return stats_;
    }
    Wal& wal() {
        return wal_;
    }

  private:
    struct Pending {
        Record rec;
        CommitCb cb;
        std::size_t bytes = 0;
    };

    void apply(const Record& r);
    bool counts_active(const Job* j) const;
    // Data-sync the pending batch and move it to committed_ (nothing applied yet).
    bool sync_pending();
    // Apply committed_ in LSN order and move its waiters into `released` (not run).
    void apply_committed(std::vector<std::pair<CommitCb, bool>>& released);
    // Fail every unapplied waiter (pending and inline-synced alike: once poisoned, this
    // process reports nothing durable past the last applied record).
    void poison();
    void run(std::vector<std::pair<CommitCb, bool>>& released);

    Wal wal_;
    std::unordered_map<JobId, Job, JobIdHash> jobs_;
    std::unordered_map<std::string, JobId> by_idem_;

    GroupCommitConfig gc_;
    GroupCommitStats stats_;
    std::deque<Pending> pending_;   // appended, not yet synced
    std::deque<Pending> committed_; // synced inline (batch cap hit), applied at next pump
    std::size_t pending_bytes_ = 0;
    Clock::time_point oldest_pending_{};
    bool poisoned_ = false;
    bool flushing_ = false;
    std::vector<std::pair<CommitCb, bool>> deferred_failures_;

    taut::Endpoint self_{};
    bool has_self_ = false;
    std::size_t owned_active_ = 0;
};

} // namespace tautq
