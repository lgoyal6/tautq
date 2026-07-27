# tautq — decision record

Format mirrors taut/docs/DECISIONS.md. All of D1–D8 were proposed in the protocol brief and
approved by Laksh on 2026-07-27 ("Approve as proposed"). Details: docs/DESIGN-protocol.md.

### D1. Workload: **distributed webhook delivery service** (+ download-worker demo workload)
- A job is "POST body to URL with an Idempotency-Key until 2xx". At-least-once + idempotency
  keys + visibility timeouts is literally the industry contract (Stripe/GitHub webhooks), so
  the demo service and the protocol are the same thing, not an approximation.
- Rationale (Laksh): wanted the job tuned to a real-life instance; watcher-style personal
  workloads rejected as not needing tautq — only workloads where the guarantees are
  load-bearing (fan-out delivery, long downloads) are claimed.

### D2. Placement: **ring-routed ownership, replica set of 3 pinned at submit**
- owner = ring_successor(hash(idempotency_key)) over alive members; replica set recorded in
  the job, never rebalanced; ring consulted only at submit. Cross-node dedup under stable
  membership, best-effort under churn. Rejected: submitter-is-owner, full ring w/ handoff, Raft.
- Rationale (Laksh): approved per brief §2.

### D3. Commit rule: **W = 2-of-3 fsync'd majority on SUBMIT / LEASE / DONE / TAKEOVER**
- Ack after owner + 1 replica are durable; third copy repaired async (backlog is a metric).
  W=3 rejected (tail latency hostage to slowest replica), W=1 rejected (loses acked jobs on
  one crash).
- Rationale (Laksh): approved per brief §3.

### D4. Lease authority: **owner-only, token = (owner_epoch, lease_seq), lease committed W=2 before grant**
- The pre-grant majority commit — not a timeout — is what prevents a partitioned stale owner
  from granting; visibility timeout 30 s default, per-job override; successor amnesty-accepts
  older-epoch acks for jobs neither DONE nor re-leased.
- Rationale (Laksh): approved per brief §3.

### D5. Failover: **deterministic successor + majority CLAIM + epoch bump; CP stall on minority**
- First alive node in pinned replica-set order claims with majority (self+1), TAKEOVER{e+1},
  waits one visibility timeout before re-leasing. No majority → stall until heal (stall over
  double-execution).
- Rationale (Laksh): approved per brief §3.

### D6. Persistence: **hand-written append-only log, CRC32C records, fsync-on-commit, torn-tail truncation**
- No embedded DB (spec). Segment rotation now, compaction v1.1 (disclosed). Group commit is
  the sanctioned fix if the load-test knee is fsync-bound.
- Rationale (Laksh): approved per brief §4 / spec item 3.

### D7. Semantics: **at-least-once execution, exactly-once completion**
- Chaos suite asserts: no loss, single committed DONE, every duplicate attempt attributable to
  an expired/orphaned lease. Literal "never runs twice" disclaimed as unachievable — duplicate
  deliveries carry the same Idempotency-Key.
- Rationale (Laksh): approved per brief §5 (honesty correction accepted).

### D8. Infra defaults (brief §6, accepted by silence)
- Sibling repo vendoring taut (FetchContent, tag ≥ v0.1.1; taut must become public or CI needs
  a PAT — OPEN, decide before CI lands). Hand-rolled HTTP/1.1 + Prometheus text exposition.
  Ports 9000/9001/8080. Chaos via netem/iptables in NET_ADMIN containers, userspace UDP-proxy
  fallback for CI determinism. In-process worker pools behind the same HTTP lease/ack API.
  DEAD_LETTER after max_attempts. Metrics: depth, in-flight leases, replication lag, churn,
  submit→DONE latency histogram (hand-rolled buckets, Grafana computes quantiles).
- Rationale (Laksh): approved per brief §6.
