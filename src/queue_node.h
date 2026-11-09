#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ring.h"
#include "rpc.h"
#include "store.h"

namespace tautq {

class QueueNodeTestAccess;

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
// Known refusal: rejected by admission control BEFORE an id was allocated or anything was
// appended. Definitely not accepted; retry later (HTTP 429 + Retry-After).
inline constexpr std::uint32_t kOverloaded = 9;
// Outcome unknown: the submit reached the log but its durability barrier failed (write or
// fsync error). It may or may not survive; retry with the SAME idempotency key (HTTP 503).
inline constexpr std::uint32_t kUnknown = 10;
} // namespace qstatus

// Bounded admission for UNIQUE submits (duplicates/idempotent retries are resolved first and
// never refused here). Hard limits with hysteresis: once any limit is reached the node sheds
// new unique work until every gauge falls to resume_ratio of its limit. Lease/ack/replication
// /repair/takeover traffic is never subject to admission, and unique submits may only use
// wal_submit_share of the WAL pending bound, reserving the rest for progress records.
struct AdmissionConfig {
    bool enabled = false;
    std::size_t max_inflight_submits = 512;           // owner: admitted, not yet answered
    std::size_t max_inflight_submit_bytes = 4u << 20; // owner: payload bytes of the above
    std::size_t max_backlog_jobs = 20000;             // owner: owned Ready+Leased jobs
    std::size_t max_gateway_forwards = 512;           // gateway: forwarded submits in flight
    double resume_ratio = 0.8;
    double wal_submit_share = 0.75;
};

enum class RefuseReason : std::uint8_t { Inflight = 0, Bytes, Backlog, Wal, Gateway, kCount };
const char* refuse_reason_name(RefuseReason r);

struct NodeConfig {
    taut::Endpoint self{};
    std::string data_dir;
    std::uint32_t default_visibility_ms = 30000;
    std::uint32_t default_max_attempts = 5;
    std::chrono::milliseconds rpc_timeout{1000};
    std::chrono::milliseconds repair_interval{2000};
    GroupCommitConfig group_commit{};
    AdmissionConfig admission{};
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
    using StatusCb = std::function<void(std::uint32_t status, const std::optional<Job>& job)>;

    QueueNode(taut::UdpTransport& socket, Membership& membership, NodeConfig cfg,
              std::uint64_t boot_id);

    // Replays the WAL; must be called (and succeed) before anything else.
    bool open();

    void poll();
    void tick();

    // Client-facing submit (the HTTP layer calls this on whichever node the client hit).
    // Routes to the stable configured owner. If that owner is unavailable, refuse the
    // submit rather than create a second job under a membership view that excludes it.
    void submit(SubmitParams p, SubmitCb cb);

    // Query the embedded origin owner first, then the remaining alive nodes. The best
    // replica state is returned so a gateway can answer from any node in the cluster.
    void status(const JobId& id, StatusCb cb);

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

    // SWIM wiring: tears down RPC state AND starts takeover claims for every non-terminal
    // job the dead peer owned where this node is the deterministic successor (first alive
    // member in the job's pinned replica-set order).
    void on_peer_dead(const taut::Endpoint& peer);

    // A returning peer may have lost its WAL. Re-send owned records to its pinned slots.
    void on_peer_alive(const taut::Endpoint& peer);

    // Graceful shutdown (§3 drain): refuse fresh owned keys and new leases, preserve
    // duplicate lookups, and hand non-terminal jobs to their pinned successor. Forwarding
    // to another configured owner remains valid. `done` waits for owned non-terminal jobs.
    using DrainCb = std::function<void()>;
    void drain(DrainCb done);
    bool draining() const {
        return draining_;
    }

    // Observability (scraped by the /metrics endpoint; gauges come from store scans).
    struct Counters {
        std::uint64_t submits = 0;
        std::uint64_t duplicates = 0;
        std::uint64_t leases_granted = 0;
        std::uint64_t completions_ok = 0;
        std::uint64_t completions_failed = 0; // worker-reported delivery failures
        std::uint64_t expirations = 0;
        std::uint64_t deadletters = 0;
        std::uint64_t takeovers = 0;
        std::uint64_t fenced_stale = 0;   // Apply records we rejected with kStaleEpoch
        std::uint64_t submit_unknown = 0; // barrier failed after append: outcome unknown
        std::uint64_t refused[static_cast<int>(RefuseReason::kCount)] = {};
        std::uint64_t deferred_ops = 0; // ops parked behind a job's pending transition
        std::uint64_t busy_rejects = 0; // ops refused because a job's park queue was full
    };
    const Counters& counters() const {
        return counters_;
    }
    // Fired with submit->DONE seconds for jobs completed while this node owned them (and
    // whose submit this process saw — the map is in-memory; restarts lose samples, which
    // biases nothing that matters for a latency histogram).
    void on_done_latency(std::function<void(double)> f) {
        on_latency_ = std::move(f);
    }

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
    std::size_t inflight_submits() const {
        return inflight_submits_;
    }
    std::size_t inflight_submit_bytes() const {
        return inflight_submit_bytes_;
    }
    std::size_t gateway_forwards() const {
        return gateway_forwards_;
    }
    bool shedding() const {
        return shedding_;
    }
    std::size_t busy_jobs() const {
        return busy_.size();
    }
    // When the loop must wake for a linger deadline (max() when none).
    std::chrono::steady_clock::time_point next_wakeup() const {
        return store_.next_deadline();
    }

  private:
    friend class QueueNodeTestAccess;

    using TimePoint = std::chrono::steady_clock::time_point;

    bool owned_by_me(const Job& j) const {
        return j.owner == cfg_.self;
    }

    void owner_submit(SubmitParams p, SubmitCb cb);
    void handle_fwd_submit(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_replicate(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_apply(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_fwd_ack(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_fwd_status(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void query_status_target(const JobId& id, std::vector<taut::Endpoint> targets,
                             std::size_t index, std::optional<Job> best, StatusCb cb);
    void start_replication(const JobId& id, bool track_quorum, SubmitCb cb);
    void send_replicate(const JobId& id, std::size_t slot);
    void repair_tick(TimePoint now);
    void expiry_tick(TimePoint now);
    void submit_result(const JobId& id, std::uint32_t st);
    void confirm_then_answer(const JobId& id, SubmitCb cb);

    void owner_ack(const JobId& id, std::uint32_t epoch, std::uint32_t lease_seq, bool success,
                   AckCb cb);

    // Failover (M6, §3). Takeover commits TAKEOVER{e+1, self} locally FIRST, then collects
    // CLAIM acks from the other replica-set members; the job only becomes actively owned
    // (leasable) with a majority of the set (counting self). An unconfirmed local takeover
    // is harmless: every grant/completion is quorum-fenced anyway.
    void start_takeover(const JobId& id);
    void begin_claims(const JobId& id, std::uint32_t proposed);
    void finalize_takeover(const JobId& id);
    void claim_send(const JobId& id, const taut::Endpoint& to);
    void handle_claim(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void handle_claim_committed(const RpcNode::ReqCtx& ctx, const JobId& id, std::uint32_t proposed,
                                const taut::Endpoint& new_owner);
    void handle_drain_handoff(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void claim_tick(TimePoint now);
    // Reconcile one job against its replica set (triggered by kStaleEpoch anywhere, by a
    // lost claim, and for every owned job at startup). Adopts only copies that strictly
    // advance (job_advances) — a stale view can never regress a committed lease.
    void resync_job(const JobId& id);
    void handle_resync(const RpcNode::ReqCtx& ctx, taut::ByteSpan body);
    void after_ownership_change(const JobId& id); // drop lease bookkeeping if demoted
    void drain_tick(TimePoint now);
    // Local fsync'd commit followed by Apply RPCs to both replicas; done(true) after one
    // replica ack (W=2 counting self). Also refreshes the repair mask pessimistically so
    // stragglers converge via the full-copy repair loop.
    void quorum_commit(const JobId& id, const Record& rec, std::function<void(bool)> done,
                       std::function<void(bool)> on_local = {});
    void replicate_record(const JobId& id, const Record& rec, std::function<void(bool)> done);

    // Durability barrier plumbing. commit_local appends `rec` behind the group-commit
    // barrier and marks the job busy; then(ok) runs after the batch's fsync (state already
    // applied when ok) and only then is the job released. Returns false when nothing was
    // appended (store poisoned or WAL pending bound reached); `then` is not called.
    bool commit_local(const JobId& id, const Record& rec, std::function<void(bool)> then);
    // Run fn now if the job has no pending transition, else park it behind that transition
    // (validation must see committed state, never a speculative one). False = park queue
    // full; fn is dropped and the caller answers with a retryable status.
    bool with_job(const JobId& id, std::function<void()> fn);
    bool job_busy(const JobId& id) const {
        return busy_.count(id) != 0;
    }
    void release_job(const JobId& id);
    std::optional<RefuseReason> admission_check(std::size_t bytes);
    void mark_replica_ok(const JobId& id, std::size_t slot, const Job& confirmed);
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
    // Idempotency reservations for submits whose SUBMIT record is behind the barrier: a
    // concurrent retry of the same key waits for the original instead of minting a twin.
    struct PendingIdem {
        JobId id{};
        std::vector<SubmitCb> waiters;
    };
    std::unordered_map<std::string, PendingIdem> pending_idem_;
    std::unordered_map<JobId, int, JobIdHash> busy_;
    // Owned jobs whose SUBMIT no replica has yet acknowledged (pessimistically every owned
    // job after a restart). Such a job is neither leasable nor reported to a retrying client
    // as a duplicate success: both would be W=1 promises.
    std::unordered_set<JobId, JobIdHash> unconfirmed_;
    std::unordered_map<JobId, std::deque<std::function<void()>>, JobIdHash> parked_;
    std::size_t inflight_submits_ = 0;
    std::size_t inflight_submit_bytes_ = 0;
    std::size_t gateway_forwards_ = 0;
    bool shedding_ = false;
    // Owned jobs with replica slots not yet confirmed durable: bit i = replicas[i+1] OK.
    // Rebuilt pessimistically (all unconfirmed) after restart; re-replication is idempotent.
    std::unordered_map<JobId, std::uint8_t, JobIdHash> repair_;
    TimePoint next_repair_{};

    // Owner-side lease machinery — all in-memory, never logged (job.h explains why):
    std::deque<JobId> ready_;                                        // leasable, FIFO-ish
    std::unordered_map<JobId, TimePoint, JobIdHash> lease_deadline_; // visibility timers
    std::unordered_map<JobId, TimePoint, JobIdHash> not_before_;     // nack/quorum backoff

    Counters counters_;
    std::function<void(double)> on_latency_;
    std::unordered_map<JobId, TimePoint, JobIdHash> submit_time_;

    // Failover machinery:
    struct ClaimState {
        std::uint32_t proposed = 0;
        int acks = 0;   // members (excluding self) that granted the claim
        int needed = 1; // majority of the replica set, minus self
        // Merged remote knowledge (the claim-quorum ∩ lease-quorum overlap guarantees any
        // committed lease is visible in at least one grant response):
        std::uint8_t best_state = 0;
        std::uint32_t best_seq = 0;
        std::uint64_t acked_mask = 0; // by replica slot
        TimePoint next_retry{};
    };
    std::unordered_map<JobId, ClaimState, JobIdHash> claims_;
    void finalize_takeover_committed(const JobId& id, const ClaimState& st);
    void after_takeover_settled(const JobId& id, bool arm_lease);
    std::unordered_map<JobId, int, JobIdHash> resync_inflight_;
    bool draining_ = false;
    DrainCb drain_cb_;
    TimePoint next_claim_tick_{};
    TimePoint next_drain_tick_{};
};

} // namespace tautq
