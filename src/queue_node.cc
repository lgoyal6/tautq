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
}

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
}

void QueueNode::on_peer_dead(const taut::Endpoint& peer) {
    rpc_.peer_dead(peer);
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
    const taut::Endpoint owner = ring::owner_for(p.idem_key, mem_.alive());
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
        cb(qstatus::kDuplicate, existing->id);
        return;
    }
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
    start_replication(j.id, /*track_quorum=*/true, std::move(cb));
}

void QueueNode::start_replication(const JobId& id, bool track_quorum, SubmitCb cb) {
    const Job* j = store_.find(id);
    int replica_count = 0;
    for (std::size_t slot = 1; slot < 3; ++slot) {
        if (j->replicas[slot] != taut::Endpoint{}) {
            ++replica_count;
        }
    }
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
    for (std::size_t slot = 1; slot < 3; ++slot) {
        if (j->replicas[slot] != taut::Endpoint{}) {
            send_replicate(id, slot);
        }
    }
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
                      auto rit = repair_.find(id);
                      if (rit != repair_.end()) {
                          rit->second |= static_cast<std::uint8_t>(1u << (slot - 1));
                          const Job* j2 = store_.find(id);
                          std::uint8_t want = 0;
                          for (std::size_t s = 1; s < 3; ++s) {
                              if (j2 != nullptr && j2->replicas[s] != taut::Endpoint{}) {
                                  want |= static_cast<std::uint8_t>(1u << (s - 1));
                              }
                          }
                          if ((rit->second & want) == want) {
                              repair_.erase(rit); // fully replicated
                          }
                      }
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
    for (std::size_t s = 1; s < 3; ++s) {
        if (j.replicas[s] != taut::Endpoint{}) {
            has_slot = true;
            if (mem_.is_alive(j.replicas[s])) {
                return true;
            }
        }
    }
    return !has_slot; // no replicas configured (tiny cluster): W degrades to 1, disclosed
}

void QueueNode::mark_replica_ok(const JobId& id, std::size_t slot) {
    auto rit = repair_.find(id);
    if (rit == repair_.end()) {
        return;
    }
    rit->second |= static_cast<std::uint8_t>(1u << (slot - 1));
    const Job* j = store_.find(id);
    std::uint8_t want = 0;
    for (std::size_t s = 1; s < 3; ++s) {
        if (j != nullptr && j->replicas[s] != taut::Endpoint{}) {
            want |= static_cast<std::uint8_t>(1u << (s - 1));
        }
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
    for (std::size_t s = 1; s < 3; ++s) {
        if (j->replicas[s] != taut::Endpoint{}) {
            st->outstanding++;
        }
    }
    if (st->outstanding == 0) {
        st->done(true);
        return;
    }
    const auto body = encode_record(rec, 0); // replicas re-stamp their own lsn on commit
    for (std::size_t s = 1; s < 3; ++s) {
        if (j->replicas[s] == taut::Endpoint{}) {
            continue;
        }
        rpc_.call(j->replicas[s], Method::Apply, body, cfg_.rpc_timeout,
                  [this, id, s, st](std::uint32_t code, taut::ByteSpan) {
                      st->outstanding--;
                      if (code == qstatus::kCreated) {
                          mark_replica_ok(id, s);
                          st->needed--;
                      }
                      if (!st->fired && st->needed <= 0) {
                          st->fired = true;
                          st->done(true);
                      } else if (!st->fired && st->outstanding == 0) {
                          st->fired = true;
                          st->done(false);
                      }
                  });
    }
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
            quorum_commit(id, Record{DeadLetterRec{id, j->epoch}}, [](bool) {});
        } else {
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
    const bool current_lease =
        j->state == JobState::Leased && j->epoch == epoch && j->lease_seq == lease_seq;
    if (!success) {
        if (!current_lease) {
            cb(qstatus::kNotLeased);
            return;
        }
        // Delivery failed (e.g. destination 5xx): back to Ready with exponential backoff.
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
    quorum_commit(id, Record{DoneRec{id, j->epoch, j->lease_seq}},
                  [cb](bool ok) { cb(ok ? qstatus::kCreated : qstatus::kNoQuorum); });
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
        for (std::size_t slot = 1; slot < 3; ++slot) {
            const bool acked = (mask & (1u << (slot - 1))) != 0;
            if (!acked && j->replicas[slot] != taut::Endpoint{} &&
                mem_.is_alive(j->replicas[slot])) {
                send_replicate(id, slot);
            }
        }
    }
}

} // namespace tautq
