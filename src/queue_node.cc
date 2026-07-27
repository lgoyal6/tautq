#include "queue_node.h"

#include <utility>

#include "bytes.h"

namespace tautq {

QueueNode::QueueNode(taut::UdpTransport& socket, Membership& membership, NodeConfig cfg,
                     std::uint64_t boot_id)
    : mem_(membership), cfg_(std::move(cfg)), rpc_(socket, cfg_.self, taut::Config{}, boot_id),
      boot_(boot_id) {
    rpc_.on_request(Method::FwdSubmit, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_fwd_submit(ctx, body);
    });
    rpc_.on_request(Method::Replicate, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_replicate(ctx, body);
    });
    rpc_.on_request(Method::Apply, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_apply(ctx, body);
    });
    rpc_.on_request(Method::FwdAck, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_fwd_ack(ctx, body);
    });
    rpc_.on_request(Method::Claim, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_claim(ctx, body);
    });
    rpc_.on_request(Method::Resync, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_resync(ctx, body);
    });
    rpc_.on_request(Method::DrainHandoff, [this](const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
        handle_drain_handoff(ctx, body);
    });
}

// The replica-set members this node must keep in sync for job j — every slot that isn't us
// and isn't empty. After a takeover, self may sit in ANY slot (including none of the first),
// so nothing below assumes slot 0 == self.
namespace {
template <typename Fn> void for_each_peer_slot(const Job& j, const taut::Endpoint& self, Fn&& fn) {
    for (std::size_t s = 0; s < 3; ++s) {
        if (!(j.replicas[s] == taut::Endpoint{}) && !(j.replicas[s] == self)) {
            fn(s, j.replicas[s]);
        }
    }
}
} // namespace

bool QueueNode::open() {
    if (!store_.open(cfg_.data_dir)) {
        return false;
    }
    const auto now = rpc_.now();
    // Pessimistic repair state: every job we own is assumed under-replicated until a
    // replica confirms otherwise (idempotent, epoch-guarded on receipt — includes Done
    // jobs, so a completion that never reached a replica converges after restart).
    // Leased jobs get a conservative fresh deadline: the log carries no wall clock, so the
    // worst case is one extra visibility window, never a premature re-grant.
    for (const auto& [id, j] : store_.jobs()) {
        if (!owned_by_me(j)) {
            continue;
        }
        repair_[id] = 0;
        if (j.state == JobState::Ready) {
            ready_.push_back(id);
        } else if (j.state == JobState::Leased) {
            lease_deadline_[id] = now + std::chrono::milliseconds(j.visibility_ms);
        }
        // Stale-restart reconciliation (§4): our log may predate a takeover that demoted
        // us. Resync every owned active job before trusting ownership — not for safety
        // (grants are quorum-fenced regardless) but to converge without wasted grants.
        if (j.state == JobState::Ready || j.state == JobState::Leased) {
            resync_job(id);
        }
    }
    return true;
}

void QueueNode::poll() {
    rpc_.poll();
}

void QueueNode::tick() {
    rpc_.tick();
    const auto now = rpc_.now();
    expiry_tick(now);
    repair_tick(now);
    claim_tick(now);
    drain_tick(now);
}

void QueueNode::on_peer_dead(const taut::Endpoint& peer) {
    rpc_.peer_dead(peer);
    // Deterministic succession: for each non-terminal job whose owner is (now) dead, the
    // FIRST alive member in the job's pinned replica order claims it. Everyone applies the
    // same rule to the same pinned list, so two live successors racing is confined to
    // asymmetric membership views — and the claim majority arbitrates that. Scanning all
    // dead-owned jobs (not just this peer's) covers cascading deaths, where the job's
    // recorded owner died before an earlier takeover completed.
    std::vector<JobId> to_claim;
    for (const auto& [id, j] : store_.jobs()) {
        if (owned_by_me(j) || mem_.is_alive(j.owner) || j.state == JobState::Done ||
            j.state == JobState::DeadLetter) {
            continue;
        }
        (void)peer;
        taut::Endpoint successor{};
        for (const auto& r : j.replicas) {
            if (!(r == taut::Endpoint{}) && mem_.is_alive(r)) {
                successor = r;
                break;
            }
        }
        if (successor == cfg_.self) {
            to_claim.push_back(id);
        }
    }
    for (const auto& id : to_claim) {
        start_takeover(id);
    }
}

// ---- submit path ---------------------------------------------------------------------------

std::vector<std::byte> QueueNode::encode_params(const SubmitParams& p) {
    std::vector<std::byte> out;
    put_u16(out, static_cast<std::uint16_t>(p.idem_key.size()));
    for (char c : p.idem_key) {
        out.push_back(std::byte{static_cast<unsigned char>(c)});
    }
    put_u16(out, static_cast<std::uint16_t>(p.url.size()));
    for (char c : p.url) {
        out.push_back(std::byte{static_cast<unsigned char>(c)});
    }
    put_u16(out, static_cast<std::uint16_t>(p.body.size()));
    put_bytes(out, p.body);
    put_u32(out, p.visibility_ms);
    put_u32(out, p.max_attempts);
    return out;
}

bool QueueNode::decode_params(taut::ByteSpan in, SubmitParams& p) {
    std::size_t off = 0;
    const auto get_str = [&](std::string& s, std::size_t max) {
        if (off + 2 > in.size()) {
            return false;
        }
        const std::size_t n = get_u16(in, off);
        off += 2;
        if (n > max || off + n > in.size()) {
            return false;
        }
        s.resize(n);
        for (std::size_t i = 0; i < n; ++i) {
            s[i] = static_cast<char>(std::to_integer<unsigned char>(in[off + i]));
        }
        off += n;
        return true;
    };
    if (!get_str(p.idem_key, kMaxIdemKey) || !get_str(p.url, kMaxUrl)) {
        return false;
    }
    if (off + 2 > in.size()) {
        return false;
    }
    const std::size_t blen = get_u16(in, off);
    off += 2;
    if (blen > kMaxJobBody || off + blen > in.size()) {
        return false;
    }
    p.body.assign(in.begin() + static_cast<std::ptrdiff_t>(off),
                  in.begin() + static_cast<std::ptrdiff_t>(off + blen));
    off += blen;
    if (off + 8 > in.size()) {
        return false;
    }
    p.visibility_ms = get_u32(in, off);
    p.max_attempts = get_u32(in, off + 4);
    return true;
}

void QueueNode::submit(SubmitParams p, SubmitCb cb) {
    if (p.idem_key.empty() || p.idem_key.size() > kMaxIdemKey || p.url.empty() ||
        p.url.size() > kMaxUrl || p.body.size() > kMaxJobBody) {
        cb(qstatus::kInvalid, JobId{});
        return;
    }
    auto alive = mem_.alive();
    if (draining_) {
        // A draining node routes new work away from itself.
        std::erase(alive, cfg_.self);
    }
    const taut::Endpoint owner = ring::owner_for(p.idem_key, alive);
    if (owner == cfg_.self || owner == taut::Endpoint{}) {
        owner_submit(std::move(p), std::move(cb));
        return;
    }
    const auto frame = encode_params(p);
    rpc_.call(owner, Method::FwdSubmit, frame, cfg_.rpc_timeout,
              [this, p = std::move(p), cb](std::uint32_t st, taut::ByteSpan body) mutable {
                  if (st < 0xFFFF0000) {
                      JobId id{};
                      if (body.size() >= kJobIdSize) {
                          id = get_job_id(body, 0);
                      }
                      cb(st, id);
                      return;
                  }
                  // Ring owner unreachable: own it here rather than fail the client
                  // (DESIGN-protocol §2). If the owner actually created the job before
                  // dying, both jobs carry the same idem key — the receiver's dedup key
                  // still holds; disclosed as best-effort dedup under failure.
                  owner_submit(std::move(p), std::move(cb));
              });
}

void QueueNode::owner_submit(SubmitParams p, SubmitCb cb) {
    if (Job* existing = store_.find_by_idem(p.idem_key)) {
        counters_.duplicates++;
        cb(qstatus::kDuplicate, existing->id);
        return;
    }
    counters_.submits++;
    Job j;
    j.id.origin = ekey(cfg_.self);
    j.id.nonce = ((boot_ & 0xFFFFFFFFull) << 32) | next_job_seq_++;
    j.idem_key = std::move(p.idem_key);
    j.url = std::move(p.url);
    j.body = std::move(p.body);
    j.visibility_ms = p.visibility_ms != 0 ? p.visibility_ms : cfg_.default_visibility_ms;
    j.max_attempts = p.max_attempts != 0 ? p.max_attempts : cfg_.default_max_attempts;
    j.replicas = ring::replica_set(j.idem_key, mem_.alive());
    if (j.replicas[0] != cfg_.self) {
        // Owning off-ring (fallback path): pin ourselves as owner and fill the remaining
        // slots with the first distinct alive nodes — ring order stops mattering the moment
        // the set is pinned in the record.
        std::array<taut::Endpoint, 3> set{cfg_.self, taut::Endpoint{}, taut::Endpoint{}};
        std::size_t slot = 1;
        for (const auto& n : mem_.alive()) {
            if (slot >= 3) {
                break;
            }
            if (!(n == cfg_.self)) {
                set[slot++] = n;
            }
        }
        j.replicas = set;
    }
    j.owner = cfg_.self;
    j.state = JobState::Ready;
    j.epoch = 1;

    if (!store_.commit(Record{SubmitRec{j}})) {
        cb(qstatus::kNoQuorum, JobId{}); // local durability failed; nothing promised
        return;
    }
    ready_.push_back(j.id); // leasable immediately; workers see it before repair finishes
    submit_time_[j.id] = rpc_.now();
    start_replication(j.id, /*track_quorum=*/true, std::move(cb));
}

void QueueNode::start_replication(const JobId& id, bool track_quorum, SubmitCb cb) {
    const Job* j = store_.find(id);
    int replica_count = 0;
    for_each_peer_slot(*j, cfg_.self, [&](std::size_t, const taut::Endpoint&) { ++replica_count; });
    repair_[id] = 0;
    if (track_quorum) {
        // W=2 counting ourselves: one replica ack suffices. Smaller clusters degrade
        // (replica_count == 0 -> W=1), which the README discloses.
        PendingSubmit ps;
        ps.cb = std::move(cb);
        ps.needed = replica_count > 0 ? 1 : 0;
        ps.outstanding = replica_count;
        pending_submits_[id] = std::move(ps);
        if (replica_count == 0) {
            submit_result(id, qstatus::kCreated);
        }
    }
    for_each_peer_slot(*j, cfg_.self,
                       [&](std::size_t slot, const taut::Endpoint&) { send_replicate(id, slot); });
}

void QueueNode::send_replicate(const JobId& id, std::size_t slot) {
    const Job* j = store_.find(id);
    if (j == nullptr) {
        return;
    }
    std::vector<std::byte> body;
    put_job(body, *j);
    rpc_.call(j->replicas[slot], Method::Replicate, body, cfg_.rpc_timeout,
              [this, id, slot](std::uint32_t st, taut::ByteSpan) {
                  auto pit = pending_submits_.find(id);
                  const bool ok = st == qstatus::kCreated || st == qstatus::kDuplicate;
                  if (ok) {
                      mark_replica_ok(id, slot);
                  } else if (st == qstatus::kStaleEpoch) {
                      resync_job(id); // someone out there has a newer epoch than us
                  }
                  if (pit == pending_submits_.end()) {
                      return;
                  }
                  PendingSubmit& ps = pit->second;
                  ps.outstanding--;
                  if (ok) {
                      ps.needed--;
                  }
                  if (ps.needed <= 0) {
                      submit_result(id, qstatus::kCreated);
                  } else if (ps.outstanding == 0) {
                      // Both replicas failed. The job exists in our log and repair keeps
                      // trying — the client sees a failure but a retry with the same key
                      // dedups. Documented at-least-once behavior.
                      submit_result(id, qstatus::kNoQuorum);
                  }
              });
}

void QueueNode::submit_result(const JobId& id, std::uint32_t st) {
    auto it = pending_submits_.find(id);
    if (it == pending_submits_.end()) {
        return;
    }
    SubmitCb cb = std::move(it->second.cb);
    pending_submits_.erase(it);
    cb(st, id);
}

void QueueNode::handle_fwd_submit(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    SubmitParams p;
    if (!decode_params(body, p)) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    owner_submit(std::move(p), [this, ctx](std::uint32_t st, const JobId& id) {
        std::vector<std::byte> resp;
        put_job_id(resp, id);
        rpc_.respond(ctx, st, resp);
    });
}

void QueueNode::handle_replicate(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    Job j;
    if (get_job(body, 0, j) == 0) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    if (const Job* known = store_.find(j.id); known != nullptr && j.epoch < known->epoch) {
        rpc_.respond(ctx, qstatus::kStaleEpoch, {});
        return;
    }
    if (!store_.commit(Record{ReplicateRec{std::move(j)}})) {
        rpc_.respond(ctx, qstatus::kNoQuorum, {});
        return;
    }
    rpc_.respond(ctx, qstatus::kCreated, {});
}

// ---- lease / ack / expiry (M5, DESIGN-protocol §3) -------------------------------------------

namespace {

struct RecMeta {
    JobId id{};
    std::uint32_t epoch = 0;
    bool valid = false;
};

RecMeta rec_meta(const Record& r) {
    if (const auto* l = std::get_if<LeaseRec>(&r)) {
        return {l->id, l->epoch, true};
    }
    if (const auto* d = std::get_if<DoneRec>(&r)) {
        return {d->id, d->epoch, true};
    }
    if (const auto* e = std::get_if<ExpireRec>(&r)) {
        return {e->id, e->epoch, true};
    }
    if (const auto* dl = std::get_if<DeadLetterRec>(&r)) {
        return {dl->id, dl->epoch, true};
    }
    if (const auto* t = std::get_if<TakeoverRec>(&r)) {
        return {t->id, t->new_epoch, true};
    }
    return {};
}

} // namespace

std::chrono::milliseconds QueueNode::nack_backoff(std::uint32_t attempt) const {
    const std::uint32_t shift = attempt > 6 ? 6 : (attempt > 0 ? attempt - 1 : 0);
    return std::chrono::milliseconds(std::min<std::uint64_t>(1000ull << shift, 60000ull));
}

void QueueNode::requeue_ready(const JobId& id, TimePoint not_before) {
    ready_.push_back(id);
    not_before_[id] = not_before;
}

bool QueueNode::replicas_reachable(const Job& j) const {
    bool has_slot = false;
    bool reachable = false;
    for_each_peer_slot(j, cfg_.self, [&](std::size_t, const taut::Endpoint& m) {
        has_slot = true;
        reachable = reachable || mem_.is_alive(m);
    });
    return reachable || !has_slot; // no peers configured (tiny cluster): W=1, disclosed
}

void QueueNode::mark_replica_ok(const JobId& id, std::size_t slot) {
    auto rit = repair_.find(id);
    if (rit == repair_.end()) {
        return;
    }
    rit->second |= static_cast<std::uint8_t>(1u << slot);
    const Job* j = store_.find(id);
    std::uint8_t want = 0;
    if (j != nullptr) {
        for_each_peer_slot(*j, cfg_.self, [&](std::size_t s, const taut::Endpoint&) {
            want |= static_cast<std::uint8_t>(1u << s);
        });
    }
    if ((rit->second & want) == want) {
        repair_.erase(rit); // all copies confirmed current
    }
}

void QueueNode::quorum_commit(const JobId& id, const Record& rec, std::function<void(bool)> done) {
    if (!store_.commit(rec)) {
        done(false);
        return;
    }
    const Job* j = store_.find(id);
    repair_[id] = 0; // this transition is unconfirmed on every replica until acked
    struct QState {
        std::function<void(bool)> done;
        int needed = 1;
        int outstanding = 0;
        bool fired = false;
    };
    auto st = std::make_shared<QState>();
    st->done = std::move(done);
    for_each_peer_slot(*j, cfg_.self,
                       [&](std::size_t, const taut::Endpoint&) { st->outstanding++; });
    if (st->outstanding == 0) {
        st->done(true);
        return;
    }
    const auto body = encode_record(rec, 0); // replicas re-stamp their own lsn on commit
    for_each_peer_slot(*j, cfg_.self, [&](std::size_t s, const taut::Endpoint& member) {
        rpc_.call(member, Method::Apply, body, cfg_.rpc_timeout,
                  [this, id, s, st](std::uint32_t code, taut::ByteSpan) {
                      st->outstanding--;
                      if (code == qstatus::kCreated) {
                          mark_replica_ok(id, s);
                          st->needed--;
                      } else if (code == qstatus::kStaleEpoch) {
                          resync_job(id); // our authority is gone; adopt the winner
                      }
                      if (!st->fired && st->needed <= 0) {
                          st->fired = true;
                          st->done(true);
                      } else if (!st->fired && st->outstanding == 0) {
                          st->fired = true;
                          st->done(false);
                      }
                  });
    });
}

void QueueNode::handle_apply(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    const auto rec = decode_record(body);
    if (!rec) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    const RecMeta m = rec_meta(*rec);
    if (!m.valid) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    Job* j = store_.find(m.id);
    if (j == nullptr) {
        // Repair lag: we never got the job copy. The owner's quorum uses the other replica;
        // the full-copy repair loop fills us in.
        rpc_.respond(ctx, qstatus::kUnknownJob, {});
        return;
    }
    // Epoch fencing — THE line that stops a partitioned stale owner: its records carry an
    // epoch below what a majority already adopted, so it cannot commit a lease anywhere.
    if (m.epoch < j->epoch) {
        counters_.fenced_stale++;
        rpc_.respond(ctx, qstatus::kStaleEpoch, {});
        return;
    }
    // Equal-epoch records must come from the owner we believe in; a legitimate successor
    // always CLAIMs first (which is how our owner/epoch view moves forward).
    if (!(ctx.from == j->owner) && !std::holds_alternative<TakeoverRec>(*rec)) {
        rpc_.respond(ctx, qstatus::kNotOwner, {});
        return;
    }
    if (!store_.commit(*rec)) {
        rpc_.respond(ctx, qstatus::kNoQuorum, {});
        return;
    }
    rpc_.respond(ctx, qstatus::kCreated, {});
}

void QueueNode::lease(std::uint64_t worker_id, LeaseCb cb) {
    if (draining_) {
        cb(qstatus::kNoJob, {}); // shedding, not granting
        return;
    }
    const auto now = rpc_.now();
    Job* pick = nullptr;
    const std::size_t scan = ready_.size();
    for (std::size_t i = 0; i < scan && pick == nullptr; ++i) {
        const JobId id = ready_.front();
        ready_.pop_front();
        Job* j = store_.find(id);
        if (j == nullptr || j->state != JobState::Ready || !owned_by_me(*j)) {
            continue; // stale queue entry; drop it
        }
        if (auto nb = not_before_.find(id); nb != not_before_.end() && now < nb->second) {
            ready_.push_back(id); // backing off; revisit later
            continue;
        }
        pick = j;
    }
    if (pick == nullptr) {
        cb(qstatus::kNoJob, {});
        return;
    }
    // Membership gate: a partitioned owner stalls instead of burning the job's attempts on
    // lease commits that can never reach quorum.
    if (!replicas_reachable(*pick)) {
        ready_.push_front(pick->id);
        cb(qstatus::kNoQuorum, {});
        return;
    }
    const std::uint32_t new_seq = pick->lease_seq + 1;
    if (new_seq > pick->max_attempts) {
        const JobId dead = pick->id;
        counters_.deadletters++;
        quorum_commit(dead, Record{DeadLetterRec{dead, pick->epoch}}, [](bool) {});
        cb(qstatus::kNoJob, {}); // parked; the worker just polls again
        return;
    }
    const JobId id = pick->id;
    const LeaseRec lrec{id, pick->epoch, new_seq, worker_id};
    not_before_.erase(id);
    quorum_commit(id, Record{lrec}, [this, id, cb](bool ok) {
        Job* j = store_.find(id);
        if (j == nullptr) {
            cb(qstatus::kUnknownJob, {});
            return;
        }
        if (!ok) {
            // The lease is locally committed but unconfirmed — expire it right back to
            // Ready (same fence values). The consumed lease_seq is the conservative price;
            // backoff + the membership gate keep this from recurring fast.
            store_.commit(Record{ExpireRec{id, j->epoch, j->lease_seq}});
            requeue_ready(id, rpc_.now() + nack_backoff(j->attempts));
            cb(qstatus::kNoQuorum, {});
            return;
        }
        counters_.leases_granted++;
        lease_deadline_[id] = rpc_.now() + std::chrono::milliseconds(j->visibility_ms);
        LeaseGrant g;
        g.id = id;
        g.url = j->url;
        g.body = j->body;
        g.idem_key = j->idem_key;
        g.epoch = j->epoch;
        g.lease_seq = j->lease_seq;
        g.visibility_ms = j->visibility_ms;
        g.attempt = j->attempts;
        cb(qstatus::kCreated, g);
    });
}

void QueueNode::expiry_tick(TimePoint now) {
    std::vector<JobId> due;
    for (const auto& [id, deadline] : lease_deadline_) {
        if (now >= deadline) {
            due.push_back(id);
        }
    }
    for (const auto& id : due) {
        lease_deadline_.erase(id);
        Job* j = store_.find(id);
        if (j == nullptr || j->state != JobState::Leased || !owned_by_me(*j)) {
            continue;
        }
        if (j->lease_seq >= j->max_attempts) {
            counters_.deadletters++;
            quorum_commit(id, Record{DeadLetterRec{id, j->epoch}}, [](bool) {});
        } else {
            counters_.expirations++;
            quorum_commit(id, Record{ExpireRec{id, j->epoch, j->lease_seq}}, [](bool) {});
            requeue_ready(id, now); // expired worker is gone; no backoff for the next one
        }
    }
}

void QueueNode::ack(const JobId& id, std::uint32_t epoch, std::uint32_t lease_seq, bool success,
                    AckCb cb) {
    Job* j = store_.find(id);
    if (j == nullptr) {
        cb(qstatus::kUnknownJob);
        return;
    }
    if (owned_by_me(*j)) {
        owner_ack(id, epoch, lease_seq, success, std::move(cb));
        return;
    }
    std::vector<std::byte> body;
    put_job_id(body, id);
    put_u32(body, epoch);
    put_u32(body, lease_seq);
    put_u8(body, success ? 1 : 0);
    rpc_.call(j->owner, Method::FwdAck, body, cfg_.rpc_timeout,
              [cb](std::uint32_t code, taut::ByteSpan) { cb(code); });
}

void QueueNode::handle_fwd_ack(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    if (body.size() < kJobIdSize + 9) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    const JobId id = get_job_id(body, 0);
    const std::uint32_t epoch = get_u32(body, kJobIdSize);
    const std::uint32_t seq = get_u32(body, kJobIdSize + 4);
    const bool success = get_u8(body, kJobIdSize + 8) != 0;
    Job* j = store_.find(id);
    if (j == nullptr) {
        rpc_.respond(ctx, qstatus::kUnknownJob, {});
        return;
    }
    if (!owned_by_me(*j)) {
        rpc_.respond(ctx, qstatus::kNotOwner, {});
        return;
    }
    owner_ack(id, epoch, seq, success,
              [this, ctx](std::uint32_t code) { rpc_.respond(ctx, code, {}); });
}

void QueueNode::owner_ack(const JobId& id, std::uint32_t epoch, std::uint32_t lease_seq,
                          bool success, AckCb cb) {
    Job* j = store_.find(id);
    if (j == nullptr) {
        cb(qstatus::kUnknownJob);
        return;
    }
    if (j->state == JobState::Done) {
        // Idempotent completion: OK once the DONE copies are confirmed on the replicas,
        // kNoQuorum (retry) until then — a worker's success always implies a majority DONE.
        cb(repair_.count(id) != 0 ? qstatus::kNoQuorum : qstatus::kCreated);
        return;
    }
    // The lease identity is its seq (monotone per job across epochs — a successor always
    // inherits the max committed seq); an OLDER token epoch with the current seq is the
    // lease this owner inherited through takeover, and its worker deserves its completion.
    const bool current_lease =
        j->state == JobState::Leased && epoch <= j->epoch && j->lease_seq == lease_seq;
    if (!success) {
        if (!current_lease) {
            cb(qstatus::kNotLeased);
            return;
        }
        // Delivery failed (e.g. destination 5xx): back to Ready with exponential backoff.
        counters_.completions_failed++;
        lease_deadline_.erase(id);
        quorum_commit(id, Record{ExpireRec{id, j->epoch, j->lease_seq}}, [](bool) {});
        requeue_ready(id, rpc_.now() + nack_backoff(j->attempts));
        cb(qstatus::kCreated);
        return;
    }
    // Amnesty (§3): the lease expired (or the owner changed) but nobody re-leased the job —
    // accepting the late completion is always safe and avoids a duplicate delivery.
    const bool amnesty = j->state == JobState::Ready &&
                         (epoch < j->epoch || (epoch == j->epoch && lease_seq <= j->lease_seq));
    const bool late_after_park = j->state == JobState::DeadLetter && epoch <= j->epoch;
    if (!current_lease && !amnesty && !late_after_park) {
        cb(qstatus::kNotLeased);
        return;
    }
    lease_deadline_.erase(id);
    not_before_.erase(id);
    quorum_commit(id, Record{DoneRec{id, j->epoch, j->lease_seq}}, [this, id, cb](bool ok) {
        if (auto ts = submit_time_.find(id); ts != submit_time_.end()) {
            if (on_latency_) {
                on_latency_(std::chrono::duration<double>(rpc_.now() - ts->second).count());
            }
            submit_time_.erase(ts);
        }
        if (ok) {
            counters_.completions_ok++;
        }
        cb(ok ? qstatus::kCreated : qstatus::kNoQuorum);
    });
}

void QueueNode::repair_tick(TimePoint now) {
    if (now < next_repair_) {
        return;
    }
    next_repair_ = now + cfg_.repair_interval;
    for (const auto& [id, mask] : repair_) {
        const Job* j = store_.find(id);
        if (j == nullptr || !owned_by_me(*j)) {
            continue;
        }
        for_each_peer_slot(*j, cfg_.self, [&](std::size_t slot, const taut::Endpoint& m) {
            const bool acked = (mask & (1u << slot)) != 0;
            if (!acked && mem_.is_alive(m)) {
                send_replicate(id, slot);
            }
        });
    }
}

// ---- failover: CLAIM / TAKEOVER / RESYNC / drain (M6, §3-§4) ---------------------------------

namespace {

// Merged-state precedence for claim responses: Done > DeadLetter > (Leased/Ready).
int state_rank(JobState s) {
    switch (s) {
    case JobState::Done:
        return 3;
    case JobState::DeadLetter:
        return 2;
    case JobState::Leased:
        return 1;
    case JobState::Ready:
        return 0;
    }
    return 0;
}

} // namespace

void QueueNode::start_takeover(const JobId& id) {
    if (draining_ || claims_.count(id) != 0) {
        return; // a draining node sheds work, never acquires it
    }
    Job* j = store_.find(id);
    if (j == nullptr || owned_by_me(*j) || j->state == JobState::Done ||
        j->state == JobState::DeadLetter) {
        return;
    }
    // Local TAKEOVER first (fsync), claims second. An unconfirmed local takeover cannot do
    // harm: acting on it means committing leases/completions, and those are quorum-fenced —
    // a rival that reached majority first makes our epoch stale everywhere it matters.
    const std::uint32_t proposed = j->epoch + 1;
    if (!store_.commit(Record{TakeoverRec{id, proposed, cfg_.self}})) {
        return;
    }
    int set_size = 0;
    for (const auto& r : j->replicas) {
        if (!(r == taut::Endpoint{})) {
            ++set_size;
        }
    }
    ClaimState st;
    st.proposed = proposed;
    st.needed = set_size / 2 + 1 - 1; // majority of the pinned set, counting self
    st.best_state = static_cast<std::uint8_t>(j->state);
    st.best_seq = j->lease_seq;
    st.next_retry = rpc_.now() + std::chrono::milliseconds(1000);
    claims_[id] = st;
    if (st.needed <= 0) {
        finalize_takeover(id);
        return;
    }
    for_each_peer_slot(*j, cfg_.self, [&](std::size_t, const taut::Endpoint& m) {
        if (mem_.is_alive(m)) {
            claim_send(id, m);
        }
    });
}

void QueueNode::claim_send(const JobId& id, const taut::Endpoint& to) {
    auto cit = claims_.find(id);
    if (cit == claims_.end()) {
        return;
    }
    std::vector<std::byte> body;
    put_job_id(body, id);
    put_u32(body, cit->second.proposed);
    put_endpoint(body, cfg_.self);
    rpc_.call(
        to, Method::Claim, body, cfg_.rpc_timeout,
        [this, id, to](std::uint32_t code, taut::ByteSpan resp) {
            auto it = claims_.find(id);
            if (it == claims_.end()) {
                return; // finalized or abandoned meanwhile
            }
            ClaimState& st = it->second;
            if (code == qstatus::kCreated) {
                const Job* j = store_.find(id);
                std::uint64_t bit = 0;
                if (j != nullptr) {
                    for (std::size_t s = 0; s < 3; ++s) {
                        if (j->replicas[s] == to) {
                            bit = 1ull << s;
                        }
                    }
                }
                if (bit == 0 || (st.acked_mask & bit) == 0) {
                    st.acked_mask |= bit;
                    st.acks++;
                }
                // Merge the replica's pre-claim knowledge. The claim majority
                // intersects every lease majority, so any committed lease surfaces
                // in at least one of these responses.
                if (resp.size() >= 5) {
                    const auto rstate = static_cast<JobState>(get_u8(resp, 0));
                    const std::uint32_t rseq = get_u32(resp, 1);
                    if (rseq > st.best_seq) {
                        st.best_seq = rseq;
                    }
                    if (state_rank(rstate) > state_rank(static_cast<JobState>(st.best_state))) {
                        st.best_state = static_cast<std::uint8_t>(rstate);
                    }
                }
                if (st.acks >= st.needed) {
                    finalize_takeover(id);
                }
            } else if (code == qstatus::kStaleEpoch) {
                claims_.erase(it); // a higher authority exists; adopt it
                resync_job(id);
            } else if (code == qstatus::kUnknownJob) {
                // The peer never got a copy; give it one so a retried claim can land.
                const Job* j = store_.find(id);
                if (j != nullptr) {
                    for_each_peer_slot(*j, cfg_.self, [&](std::size_t s, const taut::Endpoint& m) {
                        if (m == to) {
                            send_replicate(id, s);
                        }
                    });
                }
            }
            // Timeouts/peer-down: claim_tick retries while the claim stands.
        });
}

void QueueNode::handle_claim(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    if (body.size() < kJobIdSize + 4 + kEndpointSize) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    const JobId id = get_job_id(body, 0);
    const std::uint32_t proposed = get_u32(body, kJobIdSize);
    const taut::Endpoint new_owner = get_endpoint(body, kJobIdSize + 4);
    if (!(ctx.from == new_owner)) {
        rpc_.respond(ctx, qstatus::kInvalid, {}); // claims are strictly first-person
        return;
    }
    Job* j = store_.find(id);
    if (j == nullptr) {
        rpc_.respond(ctx, qstatus::kUnknownJob, {});
        return;
    }
    bool member = false;
    for (const auto& r : j->replicas) {
        member = member || r == new_owner;
    }
    if (!member) {
        rpc_.respond(ctx, qstatus::kNotOwner, {}); // only the pinned set may claim
        return;
    }
    // Pre-claim knowledge rides back in the grant — this is how a successor learns about a
    // committed lease it never saw.
    std::vector<std::byte> resp;
    put_u8(resp, static_cast<std::uint8_t>(j->state));
    put_u32(resp, j->lease_seq);

    if (proposed < j->epoch || (proposed == j->epoch && !(j->owner == new_owner))) {
        rpc_.respond(ctx, qstatus::kStaleEpoch, {});
        return;
    }
    if (proposed == j->epoch) {
        rpc_.respond(ctx, qstatus::kCreated, resp); // idempotent re-claim
        return;
    }
    if (!store_.commit(Record{TakeoverRec{id, proposed, new_owner}})) {
        rpc_.respond(ctx, qstatus::kNoQuorum, {});
        return;
    }
    after_ownership_change(id); // if WE were the owner (drain, false death), stand down
    rpc_.respond(ctx, qstatus::kCreated, resp);
}

void QueueNode::finalize_takeover(const JobId& id) {
    auto it = claims_.find(id);
    if (it == claims_.end()) {
        return;
    }
    const ClaimState st = it->second;
    claims_.erase(it);
    Job* j = store_.find(id);
    if (j == nullptr || !owned_by_me(*j)) {
        return;
    }
    counters_.takeovers++;
    const auto now = rpc_.now();
    const auto merged = static_cast<JobState>(st.best_state);
    if (merged == JobState::Done && j->state != JobState::Done) {
        store_.commit(Record{DoneRec{id, j->epoch, st.best_seq}});
    } else if (merged == JobState::DeadLetter && j->state != JobState::Done &&
               j->state != JobState::DeadLetter) {
        store_.commit(Record{DeadLetterRec{id, j->epoch}});
    } else if (merged == JobState::Leased || st.best_seq > j->lease_seq) {
        // An inherited lease — possibly one this node never saw. Record it at our epoch and
        // give the (unknown) worker one full visibility window before any re-grant.
        if (j->state != JobState::Leased || j->lease_seq < st.best_seq) {
            const std::uint32_t seq = st.best_seq > j->lease_seq ? st.best_seq : j->lease_seq;
            store_.commit(Record{LeaseRec{id, j->epoch, seq, 0}});
        }
        lease_deadline_[id] = now + std::chrono::milliseconds(j->visibility_ms);
    }
    if (j->state == JobState::Ready) {
        ready_.push_back(id);
    } else if (j->state == JobState::Leased && lease_deadline_.count(id) == 0) {
        lease_deadline_[id] = now + std::chrono::milliseconds(j->visibility_ms);
    }
    repair_[id] = 0; // push the new ownership to everyone, incl. a returning old owner
}

void QueueNode::claim_tick(TimePoint now) {
    if (now < next_claim_tick_) {
        return;
    }
    next_claim_tick_ = now + std::chrono::milliseconds(500);
    std::vector<JobId> due;
    for (const auto& [id, st] : claims_) {
        if (now >= st.next_retry) {
            due.push_back(id);
        }
    }
    for (const auto& id : due) {
        auto it = claims_.find(id);
        if (it == claims_.end()) {
            continue;
        }
        it->second.next_retry = now + std::chrono::milliseconds(1000);
        const Job* j = store_.find(id);
        if (j == nullptr) {
            continue;
        }
        for_each_peer_slot(*j, cfg_.self, [&](std::size_t s, const taut::Endpoint& m) {
            if ((it->second.acked_mask & (1ull << s)) == 0 && mem_.is_alive(m)) {
                claim_send(id, m);
            }
        });
    }
}

void QueueNode::resync_job(const JobId& id) {
    if (resync_inflight_.count(id) != 0) {
        return;
    }
    const Job* j = store_.find(id);
    if (j == nullptr) {
        return;
    }
    std::vector<std::byte> body;
    put_job_id(body, id);
    int sent = 0;
    for_each_peer_slot(*j, cfg_.self, [&](std::size_t, const taut::Endpoint& m) {
        if (!mem_.is_alive(m)) {
            return;
        }
        ++sent;
        rpc_.call(m, Method::Resync, body, cfg_.rpc_timeout,
                  [this, id](std::uint32_t code, taut::ByteSpan resp) {
                      auto rit = resync_inflight_.find(id);
                      if (rit != resync_inflight_.end() && --rit->second <= 0) {
                          resync_inflight_.erase(rit);
                      }
                      if (code != qstatus::kCreated) {
                          return;
                      }
                      Job remote;
                      if (get_job(resp, 0, remote) == 0 || !(remote.id == id)) {
                          return;
                      }
                      Job* local = store_.find(id);
                      if (local == nullptr || !job_advances(*local, remote)) {
                          return;
                      }
                      store_.commit(Record{ReplicateRec{remote}});
                      after_ownership_change(id);
                  });
    });
    if (sent > 0) {
        resync_inflight_[id] = sent;
    }
}

void QueueNode::handle_resync(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    if (body.size() < kJobIdSize) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    const Job* j = store_.find(get_job_id(body, 0));
    if (j == nullptr) {
        rpc_.respond(ctx, qstatus::kUnknownJob, {});
        return;
    }
    std::vector<std::byte> out;
    put_job(out, *j);
    rpc_.respond(ctx, qstatus::kCreated, out);
}

void QueueNode::after_ownership_change(const JobId& id) {
    Job* j = store_.find(id);
    if (j == nullptr) {
        return;
    }
    if (!owned_by_me(*j)) {
        // Demoted: this node grants nothing for the job anymore. ready_ entries drop
        // lazily inside lease().
        lease_deadline_.erase(id);
        not_before_.erase(id);
        repair_.erase(id);
        claims_.erase(id);
        return;
    }
    if (j->state == JobState::Ready) {
        ready_.push_back(id); // duplicates are tolerated; lease() drops stale entries
    }
}

void QueueNode::drain(DrainCb done) {
    draining_ = true;
    drain_cb_ = std::move(done);
    next_drain_tick_ = TimePoint{}; // act on the very next tick
}

void QueueNode::drain_tick(TimePoint now) {
    if (!draining_ || now < next_drain_tick_) {
        return;
    }
    next_drain_tick_ = now + std::chrono::milliseconds(500);
    bool remaining = false;
    for (const auto& [id, j] : store_.jobs()) {
        if (!owned_by_me(j) || j.state == JobState::Done || j.state == JobState::DeadLetter) {
            continue;
        }
        remaining = true;
        // Ask the first alive fellow member to claim the job through the ordinary takeover
        // path — the drain handoff IS a takeover, just invited instead of death-triggered.
        for (const auto& r : j.replicas) {
            if (!(r == taut::Endpoint{}) && !(r == cfg_.self) && mem_.is_alive(r)) {
                std::vector<std::byte> b;
                put_job_id(b, id);
                rpc_.call(r, Method::DrainHandoff, b, cfg_.rpc_timeout,
                          [](std::uint32_t, taut::ByteSpan) {});
                break;
            }
        }
    }
    if (!remaining && drain_cb_) {
        DrainCb cb = std::move(drain_cb_);
        drain_cb_ = nullptr;
        cb();
    }
}

void QueueNode::handle_drain_handoff(const RpcNode::ReqCtx& ctx, taut::ByteSpan body) {
    if (body.size() < kJobIdSize) {
        rpc_.respond(ctx, qstatus::kInvalid, {});
        return;
    }
    const JobId id = get_job_id(body, 0);
    const Job* j = store_.find(id);
    if (j == nullptr) {
        rpc_.respond(ctx, qstatus::kUnknownJob, {});
        return;
    }
    if (ctx.from == j->owner && !owned_by_me(*j)) {
        start_takeover(id); // the drainer itself grants our claim, so majority is immediate
    }
    rpc_.respond(ctx, qstatus::kCreated, {});
}

} // namespace tautq
