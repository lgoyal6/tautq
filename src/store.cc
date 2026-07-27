#include "store.h"

namespace tautq {

bool JobStore::open(const std::string& dir) {
    return wal_.open(dir, [this](std::span<const std::byte> body) {
        if (const auto rec = decode_record(body)) {
            apply(*rec);
        }
        // An undecodable-but-CRC-valid record would mean a version skew bug; replay keeps
        // going — the record is durable in the log for a human to look at.
    });
}

bool JobStore::commit(const Record& r, bool sync) {
    const auto body = encode_record(r, wal_.next_lsn());
    if (!wal_.append(body, sync)) {
        return false;
    }
    apply(r);
    return true;
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

void JobStore::apply(const Record& r) {
    if (const auto* s = std::get_if<SubmitRec>(&r)) {
        jobs_[s->job.id] = s->job;
        by_idem_[s->job.idem_key] = s->job.id;
        return;
    }
    // Terminal-state rules: Done is absolute (nothing changes a Done job); DeadLetter
    // yields only to Done (a late completion trumps parking). Necessary because Apply RPCs
    // ride class 1 (reliable but UNORDERED) — a delayed Lease must not resurrect a job the
    // owner already completed.
    if (const auto* rep = std::get_if<ReplicateRec>(&r)) {
        auto it = jobs_.find(rep->job.id);
        if (it == jobs_.end() || job_advances(it->second, rep->job)) {
            jobs_[rep->job.id] = rep->job;
        }
        by_idem_[rep->job.idem_key] = rep->job.id;
        return;
    }
    if (const auto* l = std::get_if<LeaseRec>(&r)) {
        if (Job* j = find(l->id); j != nullptr && l->epoch >= j->epoch &&
                                  j->state != JobState::Done && j->state != JobState::DeadLetter) {
            j->state = JobState::Leased;
            j->epoch = l->epoch;
            j->lease_seq = l->lease_seq;
            j->attempts = l->lease_seq; // attempts == grants, by construction
        }
        return;
    }
    if (const auto* d = std::get_if<DoneRec>(&r)) {
        if (Job* j = find(d->id);
            j != nullptr && d->epoch >= j->epoch && j->state != JobState::Done) {
            j->state = JobState::Done;
            j->epoch = d->epoch;
            j->body.clear(); // completed jobs keep metadata, not payloads
        }
        return;
    }
    if (const auto* e = std::get_if<ExpireRec>(&r)) {
        if (Job* j = find(e->id); j != nullptr && e->epoch >= j->epoch &&
                                  j->state == JobState::Leased && j->lease_seq == e->lease_seq) {
            j->state = JobState::Ready;
            j->epoch = e->epoch;
        }
        return;
    }
    if (const auto* dl = std::get_if<DeadLetterRec>(&r)) {
        if (Job* j = find(dl->id);
            j != nullptr && dl->epoch >= j->epoch && j->state != JobState::Done) {
            j->state = JobState::DeadLetter;
            j->epoch = dl->epoch;
        }
        return;
    }
    if (const auto* t = std::get_if<TakeoverRec>(&r)) {
        if (Job* j = find(t->id); j != nullptr && t->new_epoch > j->epoch) {
            j->epoch = t->new_epoch;
            j->owner = t->new_owner;
            if (j->state == JobState::Leased) {
                // The old owner's lease may still be running somewhere; the new owner
                // re-arms a conservative expiry timer (M6). State stays Leased until then.
            }
        }
        return;
    }
}

} // namespace tautq
