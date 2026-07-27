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
}

bool QueueNode::open() {
    if (!store_.open(cfg_.data_dir)) {
        return false;
    }
    // Pessimistic repair state: every active job we own is assumed under-replicated until a
    // replica confirms otherwise. Re-replication is idempotent (epoch-guarded on receipt).
    for (const auto& [id, j] : store_.jobs()) {
        if (owned_by_me(j) && j.state != JobState::Done && j.state != JobState::DeadLetter) {
            repair_[id] = 0;
        }
    }
    return true;
}

void QueueNode::poll() {
    rpc_.poll();
}

void QueueNode::tick() {
    rpc_.tick();
    repair_tick(rpc_.now());
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
