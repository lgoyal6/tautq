#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ring.h"
#include "rpc.h"
#include "store.h"

namespace tautq {

// Application-level statuses (RPC status field / HTTP mapping). Transport failures use the
// status:: space from rpc.h.
namespace qstatus {
inline constexpr std::uint32_t kCreated = 0;
inline constexpr std::uint32_t kDuplicate = 1; // idempotency-key hit; body carries the id
inline constexpr std::uint32_t kStaleEpoch = 2;
inline constexpr std::uint32_t kNoQuorum = 3;
inline constexpr std::uint32_t kInvalid = 4;
inline constexpr std::uint32_t kUnknownJob = 5;
inline constexpr std::uint32_t kNotOwner = 6;
inline constexpr std::uint32_t kNotLeased = 7; // ack with no matching lease
inline constexpr std::uint32_t kNoJob = 8;     // lease request: nothing ready
} // namespace qstatus

struct NodeConfig {
    taut::Endpoint self{};
    std::string data_dir;
    std::uint32_t default_visibility_ms = 30000;
    std::uint32_t default_max_attempts = 5;
    std::chrono::milliseconds rpc_timeout{1000};
    std::chrono::milliseconds repair_interval{2000};
};

// The queue protocol brain (DESIGN-protocol §§2-4): ring-routed submit, W=2 replication
// with async repair. Leases/completion land in M5, failover in M6. Single-threaded,
// poll()/tick() driven, all I/O via the RpcNode/WAL it owns.
class QueueNode {
  public:
    struct SubmitParams {
        std::string idem_key;
        std::string url;
        std::vector<std::byte> body;
        std::uint32_t visibility_ms = 0; // 0 = node default
        std::uint32_t max_attempts = 0;  // 0 = node default
    };
    // (status, job id). id is valid for kCreated/kDuplicate.
    using SubmitCb = std::function<void(std::uint32_t status, const JobId& id)>;

    QueueNode(taut::UdpTransport& socket, Membership& membership, NodeConfig cfg,
              std::uint64_t boot_id);

    // Replays the WAL; must be called (and succeed) before anything else.
    bool open();

    void poll();
    void tick();

    // Client-facing submit (the HTTP layer calls this on whichever node the client hit).
    // Ring-routes to the owner; falls back to owning locally if the ring owner is
    // unreachable (dedup degrades to best-effort; duplicates still share the idem key).
    void submit(SubmitParams p, SubmitCb cb);

    // What a worker receives with a granted lease: the job to execute plus the fencing
    // token (epoch, lease_seq) it must present on completion.
    struct LeaseGrant {
        JobId id{};
        std::string url;
        std::vector<std::byte> body;
        std::string idem_key;
        std::uint32_t epoch = 0;
        std::uint32_t lease_seq = 0;
        std::uint32_t visibility_ms = 0;
        std::uint32_t attempt = 0;
    };
    using LeaseCb = std::function<void(std::uint32_t status, const LeaseGrant& grant)>;
    using AckCb = std::function<void(std::uint32_t status)>;

    // Grant one owned Ready job to a worker. The LEASE record is majority-committed (local
    // fsync + one replica ack) BEFORE the grant is handed out — the line that makes
    // double-leasing across epochs impossible (§3). kNoJob when nothing is leasable;
    // kNoQuorum when replicas are unreachable (the job is returned to Ready, backoff
    // applied — a partitioned owner must stall rather than grant, and the membership gate
    // keeps repeated quorum failures from burning a job's attempts).
    void lease(std::uint64_t worker_id, LeaseCb cb);

    // Worker completion (success) or delivery failure (success=false -> immediate expire +
    // exponential backoff). Success commits DONE at W=2 before the worker hears OK; retried
    // acks on an already-Done job answer OK once the DONE copies are confirmed (idempotent
    // completion). Late acks whose lease expired but whose job was never re-leased are
    // accepted (amnesty) — discarding finished work only forces a duplicate delivery.
    void ack(const JobId& id, std::uint32_t epoch, std::uint32_t lease_seq, bool success, AckCb cb);

    // SWIM wiring (M6 expands this into takeover checks).
    void on_peer_dead(const taut::Endpoint& peer);

    // Introspection.
    JobStore& store() {
        return store_;
    }
    RpcNode& rpc() {
        return rpc_;
    }
    const taut::Endpoint& self() const {
        return cfg_.self;
    }
    std::size_t repair_backlog() const {
        return repair_.size();
    }

  private:
    using TimePoint = std::chrono::steady_clock::time_point;

    bool owned_by_me(const Job& j) const {
        return j.owner == cfg_.self;
    }

    void owner_submit(SubmitParams p, SubmitCb cb);
    void handle_fwd_submit(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_replicate(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_apply(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_fwd_ack(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void start_replication(const JobId& id, bool track_quorum, SubmitCb cb);
    void send_replicate(const JobId& id, std::size_t slot);
    void repair_tick(TimePoint now);
    void expiry_tick(TimePoint now);
    void submit_result(const JobId& id, std::uint32_t st);

    void owner_ack(const JobId& id, std::uint32_t epoch, std::uint32_t lease_seq, bool success,
                   AckCb cb);
    // Local fsync'd commit followed by Apply RPCs to both replicas; done(true) after one
    // replica ack (W=2 counting self). Also refreshes the repair mask pessimistically so
    // stragglers converge via the full-copy repair loop.
    void quorum_commit(const JobId& id, const Record& rec, std::function<void(bool)> done);
    void mark_replica_ok(const JobId& id, std::size_t slot);
    bool replicas_reachable(const Job& j) const;
    void requeue_ready(const JobId& id, TimePoint not_before);
    std::chrono::milliseconds nack_backoff(std::uint32_t attempt) const;

    static std::vector<std::byte> encode_params(const SubmitParams& p);
    static bool decode_params(taut::ByteSpan in, SubmitParams& p);

    Membership& mem_;
    NodeConfig cfg_;
    RpcNode rpc_;
    JobStore store_;
    std::uint64_t boot_;
    std::uint32_t next_job_seq_ = 1;

    struct PendingSubmit {
        SubmitCb cb;
        int needed = 0;      // replica acks still required for W=2
        int outstanding = 0; // replicate RPCs still in flight
    };
    std::unordered_map<JobId, PendingSubmit, JobIdHash> pending_submits_;
    // Owned jobs with replica slots not yet confirmed durable: bit i = replicas[i+1] OK.
    // Rebuilt pessimistically (all unconfirmed) after restart; re-replication is idempotent.
    std::unordered_map<JobId, std::uint8_t, JobIdHash> repair_;
    TimePoint next_repair_{};

    // Owner-side lease machinery — all in-memory, never logged (job.h explains why):
    std::deque<JobId> ready_;                                        // leasable, FIFO-ish
    std::unordered_map<JobId, TimePoint, JobIdHash> lease_deadline_; // visibility timers
    std::unordered_map<JobId, TimePoint, JobIdHash> not_before_;     // nack/quorum backoff
};

} // namespace tautq
