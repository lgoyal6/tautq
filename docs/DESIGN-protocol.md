# tautq — lease & replication protocol (approved 2026-07-27)

tautq is a distributed webhook-delivery service (a work queue whose jobs are "POST this body
to this URL until a 2xx comes back") built on the taut transport + SWIM membership library.
5+ nodes, no central coordinator. This document is the protocol as approved by Laksh on
2026-07-27; deviations discovered during implementation get recorded here with a dated note.

## 1. Job model

- `job = {job_id, idempotency_key, payload ≤ 1 KiB, visibility_timeout (default 30 s),
  attempts, max_attempts, state}`.
- States: `READY → LEASED → DONE`, lease expiry returns `LEASED → READY`, and
  `attempts == max_attempts` moves a job to `DEAD_LETTER` (terminal, queryable, re-drivable
  by admin).
- `job_id = (owner_node_id, local_seq, random16)` — unique without coordination; status
  queries route to the embedded owner first, then fall back to broadcast (N is small).
- Payloads above one taut datagram (~1.1 KiB) are out of scope for v1 (v1.1: chunked over
  class 2).

## 2. Placement & replication — ring-routed ownership, pinned at birth

- Any node accepts a submit. It forwards to `owner = ring_successor(hash(idempotency_key))`
  over the currently-alive membership (sorted node ids; SWIM supplies liveness).
- Owner + next 2 alive ring nodes form the job's **replica set of 3, recorded in the job
  record and never rebalanced**. The ring is consulted only at submit time.
- Cross-node idempotency dedup therefore holds under stable membership and degrades to
  best-effort under churn or when the ring owner is unreachable (gateway falls back to any
  alive node that can reach 2 peers) — documented, not hidden.
- Rejected alternatives: submitter-is-owner (no cross-node dedup), full Dynamo-style ring with
  range handoff (rebalancing machinery unneeded at this scale), single Raft group (a central
  coordinator in disguise).

## 3. Commit rule — majority commit with owner-epoch fencing

Every state transition (`SUBMIT`, `LEASE`, `DONE`, `TAKEOVER`) is a log record, acknowledged
only after it is fsync-durable on a **majority of the replica set (W = 2 of 3, counting the
owner)**. The third copy is repaired asynchronously (repair backlog is a published metric).

Each job carries an **owner epoch `e`** (starts at 1); replicas reject records with a stale
epoch. Consequences:

- **Submit:** owner fsyncs `SUBMIT`, replicates to both replicas (class-1 RPC), HTTP 200 on
  the first replica ack. Tolerates f=1; two simultaneous failures inside one replica set can
  lose an unrepaired job (stated in README).
- **Lease:** `LEASE{job, worker, token=(e, lease_seq), deadline}` must be acked by one replica
  **before** the job is handed to a worker. A partitioned stale owner (epoch e, cluster at
  e+1) cannot get any lease acked, so it cannot grant — this is what makes "two workers never
  validly hold the same job" true, not a timeout heuristic.
- **Complete:** worker acks with its token; owner validates token == current lease, then
  majority-commits `DONE`. Stale tokens → 409. A successor also accepts an older-epoch ack iff
  the job is neither DONE nor currently re-leased (accepting a late ack for a job nobody else
  is running is always safe and avoids a needless re-run).
- **Failover:** on SWIM Dead(owner), the deterministic successor (first alive node in the
  job's pinned replica-set order) sends `CLAIM` and proceeds only with majority (self + 1),
  appending `TAKEOVER{e+1}`. It waits one full visibility timeout before re-leasing non-DONE
  jobs (belt-and-suspenders over the W=2 lease rule; covers clock skew). No majority → those
  jobs stall until heal (CP choice: stall over double-execution).
- **Drain (admin):** stop accepting submits/leases, transfer ownership of owned jobs via the
  TAKEOVER path, then leave the ring.

## 4. Persistence & recovery

- Hand-written append-only log per node (no embedded DB): length-prefixed records with
  **CRC32C (reusing `taut::crc32c`)**, monotone LSN, segment rotation. fsync at every commit
  point above; group commit is the sanctioned optimization if the load-test knee is
  fsync-bound. Replay truncates at the first torn/bad-CRC tail record and rebuilds the job
  table. Compaction is v1.1; v1 retains segments and says so.
- **Stale restart:** rejoin SWIM (taut ≥ v0.1.1), then for every active job in the log run a
  RESYNC round against that job's replica set before acting as owner; higher epoch wins;
  demote where a TAKEOVER superseded us.

## 5. Delivery semantics (the honest contract)

**At-least-once execution, exactly-once completion.** Literal "no job ever runs twice" is
unachievable in any visibility-timeout system — a worker SIGKILLed mid-delivery *must* cause a
re-run. What the chaos suite asserts instead:

1. **No loss:** every job acked at submit is eventually DONE (or DEAD_LETTER) exactly once,
   verified across all node logs.
2. **Exactly-once completion:** at most one DONE ever commits per job; no two valid leases
   overlap (epoch fencing).
3. **Attributable re-execution:** every duplicate delivery attempt maps to a provably expired
   or orphaned lease — zero unexplained duplicates. In chaos scenarios where no leaseholder
   dies mid-delivery, attempts == 1.

Duplicate deliveries carry the same `Idempotency-Key` header, so the receiver can dedup —
which is exactly the contract real webhook providers (Stripe/GitHub) publish.

## 6. Transport mapping (taut integration)

- Per node: UDP 9000 data, UDP 9001 SWIM, TCP 8080 HTTP (admin + Prometheus text).
- One data socket, demuxed by source endpoint into per-peer `taut::Session`s (a facade
  `UdpTransport` per peer over per-peer inbound queues; sends pass through).
- Session reset: random per-process `boot_id` exchanged via HELLO; a changed `boot_id`
  recreates that peer's Session on both sides (taut has no connection handshake by design).
- RPCs (REPLICATE/CLAIM/DONE/RESYNC-digest) ride class 1 (reliable-unordered, no head-of-line
  blocking); RESYNC state transfer rides class 2 framed. SWIM runs unchanged on its own socket.
- On SWIM Dead(peer): abandon in-flight RPCs to it and tear down its Session.
