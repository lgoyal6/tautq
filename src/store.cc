#include "store.h"

#include <algorithm>

#include "faults.h"

namespace tautq {

bool JobStore::open(const std::string& dir) {
    jobs_.clear();
    by_idem_.clear();
    owned_active_ = 0;
    return wal_.open(dir, [this](std::span<const std::byte> body) {
        if (const auto rec = decode_record(body)) {
            apply(*rec);
        }
        // An undecodable-but-CRC-valid record would mean a version skew bug; replay keeps
        // going — the record is durable in the log for a human to look at.
    });
}

bool JobStore::commit(const Record& r, bool sync) {
    if (poisoned_) {
        return false;
    }
    if (!sync) {
        // Legacy unsynced append (tests/tools only): visible immediately, durable later.
        const auto body = encode_record(r, wal_.next_lsn());
        if (!pending_.empty() || !committed_.empty() || !wal_.append(body, false)) {
            return false;
        }
        apply(r);
        return true;
    }
    bool ok = false;
    bool called = false;
    if (!commit_async(r, [&](bool v) {
            ok = v;
            called = true;
        })) {
        return false;
    }
    flush();
    return called && ok;
}

bool JobStore::commit_async(const Record& r, CommitCb cb) {
    if (poisoned_ || wal_.failed()) {
        return false;
    }
    if (pending_records() >= gc_.max_pending_records) {
        stats_.refused_full++;
        return false;
    }
    const auto body = encode_record(r, wal_.next_lsn());
    if (!wal_.append(body, /*sync=*/false)) {
        // A write error (ENOSPC, EIO, short write). The partial frame is a torn tail for the
        // next open(); the earlier pending records may or may not survive. Poison: their
        // waiters fail at the next pump, and this record's caller gets false now.
        poison();
        return false;
    }
    if (pending_.empty()) {
        oldest_pending_ = Clock::now();
    }
    faults::crashpoint("wal.after_write_before_sync");
    pending_.push_back(Pending{r, std::move(cb), body.size()});
    pending_bytes_ += body.size();
    if (pending_.size() >= gc_.max_batch_records || pending_bytes_ >= gc_.max_batch_bytes) {
        // Batch cap: sync inline (with max_batch_records == 1 this is one fsync per record,
        // the unbatched baseline). Applying and releasing still wait for pump() so waiters
        // never run re-entrantly inside the caller's commit_async.
        if (!sync_pending()) {
            poison();
        }
    }
    return true;
}

bool JobStore::sync_pending() {
    if (pending_.empty()) {
        return true;
    }
    const auto t0 = Clock::now();
    if (!wal_.sync()) {
        stats_.flush_failures++;
        return false;
    }
    faults::crashpoint("wal.after_sync_before_apply");
    const auto ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
    stats_.flushes++;
    stats_.flush_ns_total += ns;
    stats_.flush_ns_max = std::max(stats_.flush_ns_max, ns);
    const std::uint64_t n = pending_.size();
    stats_.last_batch = n;
    stats_.max_batch = std::max(stats_.max_batch, n);
    std::size_t bucket = 0;
    for (std::uint64_t v = n; v > 1 && bucket < 10; v >>= 1) {
        ++bucket;
    }
    stats_.batch_hist[bucket]++;
    stats_.records_committed += n;
    stats_.bytes_committed += pending_bytes_;
    while (!pending_.empty()) {
        committed_.push_back(std::move(pending_.front()));
        pending_.pop_front();
    }
    pending_bytes_ = 0;
    return true;
}

void JobStore::apply_committed(std::vector<std::pair<CommitCb, bool>>& released) {
    // Order: apply the whole synced batch, then release waiters in LSN order. A waiter
    // therefore sees every durable record, never a non-durable one.
    while (!committed_.empty()) {
        Pending p = std::move(committed_.front());
        committed_.pop_front();
        apply(p.rec);
        if (p.cb) {
            released.emplace_back(std::move(p.cb), true);
        }
    }
}

void JobStore::poison() {
    if (!poisoned_) {
        poisoned_ = true;
    }
    for (auto* q : {&committed_, &pending_}) {
        // committed_ records were synced, but once a later operation failed we do not apply
        // them in this process either: the restart replay decides. Their clients hear
        // "unknown", which is truthful whichever way replay goes.
        while (!q->empty()) {
            if (q->front().cb) {
                deferred_failures_.emplace_back(std::move(q->front().cb), false);
                stats_.poisoned_waiters++;
            }
            q->pop_front();
        }
    }
    pending_bytes_ = 0;
}

void JobStore::run(std::vector<std::pair<CommitCb, bool>>& released) {
    for (auto& [cb, ok] : released) {
        cb(ok);
    }
    released.clear();
}

bool JobStore::flush() {
    if (flushing_) {
        return !poisoned_; // a waiter re-entered; the outer flush finishes the work
    }
    flushing_ = true;
    std::vector<std::pair<CommitCb, bool>> released;
    // A few rounds: waiters commonly append follow-up records (e.g. an expire after a failed
    // lease quorum); flushing those here keeps them out of the next pass's latency.
    for (int round = 0; round < 4; ++round) {
        if (!sync_pending()) {
            poison();
        }
        if (!poisoned_) {
            apply_committed(released);
        }
        released.insert(released.end(), std::make_move_iterator(deferred_failures_.begin()),
                        std::make_move_iterator(deferred_failures_.end()));
        deferred_failures_.clear();
        if (released.empty()) {
            break;
        }
        run(released);
        if (pending_.empty() && committed_.empty() && deferred_failures_.empty()) {
            break;
        }
    }
    flushing_ = false;
    return !poisoned_;
}

void JobStore::pump(bool end_of_pass) {
    if (pending_.empty() && committed_.empty() && deferred_failures_.empty()) {
        return;
    }
    if (!pending_.empty() && gc_.linger.count() > 0 && pending_.size() < gc_.max_batch_records &&
        Clock::now() - oldest_pending_ < gc_.linger) {
        // Hold the open batch; still release anything already synced or failed.
        if (committed_.empty() && deferred_failures_.empty()) {
            return;
        }
        std::vector<std::pair<CommitCb, bool>> released;
        if (!poisoned_) {
            apply_committed(released);
        }
        released.insert(released.end(), std::make_move_iterator(deferred_failures_.begin()),
                        std::make_move_iterator(deferred_failures_.end()));
        deferred_failures_.clear();
        run(released);
        return;
    }
    if (end_of_pass || !committed_.empty() || !deferred_failures_.empty()) {
        flush();
    }
}

JobStore::Clock::time_point JobStore::next_deadline() const {
    if (pending_.empty() || gc_.linger.count() <= 0) {
        return Clock::time_point::max();
    }
    return oldest_pending_ + gc_.linger;
}

bool JobStore::compact(const Wal::CompactHook& hook) {
    if (poisoned_) {
        return false;
    }
    // Committed cut: sync and apply everything pending, but hold the waiters until the new
    // generation is installed (they may append, and those appends must not land in the old
    // generation that the snapshot replaces).
    std::vector<std::pair<CommitCb, bool>> held;
    if (!sync_pending()) {
        poison();
    } else {
        apply_committed(held);
    }
    if (poisoned_) {
        flush(); // releases failures
        return false;
    }
    std::vector<Job> retained;
    retained.reserve(jobs_.size());
    for (const auto& [id, job] : jobs_) {
        (void)id;
        retained.push_back(job);
    }
    std::sort(retained.begin(), retained.end(), [this](const Job& a, const Job& b) {
        if (a.idem_key != b.idem_key)
            return a.idem_key < b.idem_key;
        const auto selected = by_idem_.find(a.idem_key);
        const bool a_selected = selected != by_idem_.end() && a.id == selected->second;
        const bool b_selected = selected != by_idem_.end() && b.id == selected->second;
        if (a_selected != b_selected)
            return !a_selected;
        return a.id.origin < b.id.origin || (a.id.origin == b.id.origin && a.id.nonce < b.id.nonce);
    });
    std::vector<std::vector<std::byte>> snapshot;
    snapshot.reserve(retained.size());
    for (std::size_t i = 0; i < retained.size(); ++i) {
        snapshot.push_back(encode_record(Record{ReplicateRec{retained[i]}}, i));
    }
    const bool ok = wal_.compact(snapshot, hook);
    if (!ok && wal_.failed()) {
        // The old generation may be unreachable from CURRENT or in an unknown state. The
        // already-applied records were synced before the cut, so their waiters' successes
        // are true; but nothing else may commit in this process.
        poisoned_ = true;
    }
    run(held);
    return ok;
}

Job* JobStore::find(const JobId& id) {
    auto it = jobs_.find(id);
    return it == jobs_.end() ? nullptr : &it->second;
}

const Job* JobStore::find(const JobId& id) const {
    auto it = jobs_.find(id);
    return it == jobs_.end() ? nullptr : &it->second;
}

Job* JobStore::find_by_idem(const std::string& key) {
    auto it = by_idem_.find(key);
    return it == by_idem_.end() ? nullptr : find(it->second);
}

bool JobStore::counts_active(const Job* j) const {
    return has_self_ && j != nullptr && j->owner == self_ &&
           (j->state == JobState::Ready || j->state == JobState::Leased);
}

void JobStore::apply(const Record& r) {
    JobId touched{};
    if (const auto* s = std::get_if<SubmitRec>(&r)) {
        touched = s->job.id;
    } else if (const auto* rep = std::get_if<ReplicateRec>(&r)) {
        touched = rep->job.id;
    } else if (const auto* l = std::get_if<LeaseRec>(&r)) {
        touched = l->id;
    } else if (const auto* d = std::get_if<DoneRec>(&r)) {
        touched = d->id;
    } else if (const auto* e = std::get_if<ExpireRec>(&r)) {
        touched = e->id;
    } else if (const auto* dl = std::get_if<DeadLetterRec>(&r)) {
        touched = dl->id;
    } else if (const auto* t = std::get_if<TakeoverRec>(&r)) {
        touched = t->id;
    }
    const bool was_active = counts_active(find(touched));

    if (const auto* s = std::get_if<SubmitRec>(&r)) {
        jobs_[s->job.id] = s->job;
        by_idem_[s->job.idem_key] = s->job.id;
    } else if (const auto* rep = std::get_if<ReplicateRec>(&r)) {
        // Terminal-state rules: Done is absolute (nothing changes a Done job); DeadLetter
        // yields only to Done (a late completion trumps parking). Necessary because Apply
        // RPCs ride class 1 (reliable but UNORDERED) — a delayed Lease must not resurrect a
        // job the owner already completed.
        auto it = jobs_.find(rep->job.id);
        if (it != jobs_.end() && it->second.state == JobState::Done &&
            rep->job.state != JobState::Done) {
            // Done is absolute even against a higher epoch: a copy pushed by a successor
            // whose takeover has not yet learned of the committed DONE (e.g. repair of an
            // unconfirmed local TAKEOVER) must not erase it, or the claim grant would report
            // a live lease and the job would be completed twice. Adopt only the ownership.
            Job& j = it->second;
            if (rep->job.epoch > j.epoch) {
                j.epoch = rep->job.epoch;
                j.owner = rep->job.owner;
                j.replicas = rep->job.replicas;
            }
            // Ownership may advance before the successor learns about this terminal job,
            // but the incoming lease sequence is not a completion token. Keep the sequence
            // that actually completed the job so a later full-state repair cannot relabel it.
        } else if (it == jobs_.end() || job_advances(it->second, rep->job)) {
            jobs_[rep->job.id] = rep->job;
        }
        by_idem_[rep->job.idem_key] = rep->job.id;
    } else if (const auto* l = std::get_if<LeaseRec>(&r)) {
        if (Job* j = find(l->id); j != nullptr && l->epoch >= j->epoch &&
                                  j->state != JobState::Done && j->state != JobState::DeadLetter) {
            j->state = JobState::Leased;
            j->epoch = l->epoch;
            j->lease_seq = l->lease_seq;
            j->attempts = l->lease_seq; // attempts == grants, by construction
        }
    } else if (const auto* d = std::get_if<DoneRec>(&r)) {
        if (Job* j = find(d->id);
            j != nullptr && d->epoch >= j->epoch && d->lease_seq >= j->lease_seq) {
            if (j->state != JobState::Done) {
                j->state = JobState::Done;
                j->body.clear(); // completed jobs keep metadata, not payloads
            }
            j->epoch = d->epoch;
            j->lease_seq = d->lease_seq;
            j->attempts = std::max(j->attempts, d->lease_seq);
        }
    } else if (const auto* e = std::get_if<ExpireRec>(&r)) {
        if (Job* j = find(e->id); j != nullptr && e->epoch >= j->epoch &&
                                  j->state == JobState::Leased && j->lease_seq == e->lease_seq) {
            j->state = JobState::Ready;
            j->epoch = e->epoch;
        }
    } else if (const auto* dl = std::get_if<DeadLetterRec>(&r)) {
        if (Job* j = find(dl->id);
            j != nullptr && dl->epoch >= j->epoch && j->state != JobState::Done) {
            j->state = JobState::DeadLetter;
            j->epoch = dl->epoch;
        }
    } else if (const auto* t = std::get_if<TakeoverRec>(&r)) {
        if (Job* j = find(t->id); j != nullptr && t->new_epoch > j->epoch) {
            j->epoch = t->new_epoch;
            j->owner = t->new_owner;
            // A Leased job stays Leased: the old owner's lease may still be running
            // somewhere; the new owner re-arms a conservative expiry timer (M6).
        }
    }

    const bool is_active = counts_active(find(touched));
    if (is_active && !was_active) {
        ++owned_active_;
    } else if (!is_active && was_active && owned_active_ > 0) {
        --owned_active_;
    }
}

} // namespace tautq
