# tautq - lease & replication protocol (approved 2026-07-27)

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
- `job_id = (owner_node_id, local_seq, random16)` - unique without coordination; status
  queries route to the embedded owner first, then fall back to broadcast (N is small).
- Payloads above one taut datagram (~1.1 KiB) are out of scope for v1 (v1.1: chunked over
  class 2).

## 2. Placement & replication - ring-routed ownership, pinned at birth

- Any node accepts a submit. It forwards to `owner = ring_successor(hash(idempotency_key))`
  over the currently-alive membership (sorted node ids; SWIM supplies liveness).
- Owner + next 2 alive ring nodes form the job's **replica set of 3, recorded in the job
  record and never rebalanced**. The ring is consulted only at submit time.
- Cross-node idempotency dedup therefore holds under stable membership and degrades to
  best-effort under churn or when the ring owner is unreachable (gateway falls back to any
  alive node that can reach 2 peers) - documented, not hidden.
- Rejected alternatives: submitter-is-owner (no cross-node dedup), full Dynamo-style ring with
  range handoff (rebalancing machinery unneeded at this scale), single Raft group (a central
  coordinator in disguise).

## 3. Commit rule - majority commit with owner-epoch fencing

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
  e+1) cannot get any lease acked, so it cannot grant - this is what makes "two workers never
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
unachievable in any visibility-timeout system - a worker SIGKILLed mid-delivery *must* cause a
re-run. What the chaos suite asserts instead:

1. **No loss:** every job acked at submit is eventually DONE (or DEAD_LETTER) exactly once,
   verified across all node logs.
2. **Exactly-once completion:** at most one DONE ever commits per job; no two valid leases
   overlap (epoch fencing).
3. **Attributable re-execution:** every duplicate delivery attempt maps to a provably expired
   or orphaned lease - zero unexplained duplicates. In chaos scenarios where no leaseholder
   dies mid-delivery, attempts == 1.

Duplicate deliveries carry the same `Idempotency-Key` header, so the receiver can dedup  - 
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

## Deviations and refinements discovered during implementation (dated notes)

- **(M5, 2026-07-27) Quorum-failed lease attempts consume `lease_seq`.** A lease is
  committed locally before its replica quorum; on quorum failure it is self-expired with
  the fence values intact. Conservative in the safe direction (can never double-grant),
  but repeated quorum failures could burn a job's attempts - mitigated by the membership
  gate (an owner that can see no replica alive refuses to try) plus per-job backoff.
- **(M5) EXPIRE and DEAD_LETTER replicate asynchronously** (not W=2-blocking like
  SUBMIT/LEASE/DONE): ack fencing is by exact `(epoch, lease_seq)` match, so a replica
  with stale lease knowledge cannot cause an incorrect acceptance, and takeover re-arms
  deadlines conservatively. The full-copy repair loop converges stragglers.
- **(M6) Takeover commits locally BEFORE collecting claim acks.** An unconfirmed local
  takeover is provably harmless - acting on ownership means committing leases/completions,
  and those are quorum-fenced. This avoids a claim-retry livelock without adding an epoch
  reservation protocol.
- **(M6) `current_lease` keys on `lease_seq`, with `token.epoch <= job.epoch`:** the seq is
  monotone per job across epochs (successors inherit the max committed seq via the
  claim/lease majority intersection), so an old-epoch token with the current seq is the
  inherited lease, and its worker's completion is accepted.
- **(M9) Fallback twins are a verified, attributed case:** under loss, a forwarded submit
  whose response is lost triggers the §2 local-ownership fallback and two jobs can share
  one idempotency key. The chaos verifier attributes such duplicates explicitly
  (`fallback_twins`) so "zero unexplained duplicates" stays a tight assertion.

- **(2026-10-15) Stable submission ownership and draining admission supersede the
  alive-member fallback in §2 and the M9 fallback-twin behavior above.** Configured
  membership fixes the idempotency owner. An unavailable owner yields retryable HTTP
  503; gateways do not mint a replacement ID. A draining owner returns the original
  ID for an indexed key but refuses a fresh key with 503 before allocation or WAL
  append. The same rule applies to forwarded submissions and after the drain callback.
  A draining gateway can still forward to a different healthy configured owner.
  TAKEOVER moves execution ownership only. Configured member changes require a
  coordinated migration; drain does not remove a member from the configured ring.

- **(2026-11-04) Group-commit durability barrier.** "fsync at every commit point" is now
  implemented as a node-local barrier instead of one fsync per record. `JobStore::commit_async`
  encodes and appends a record and parks its waiter; nothing is applied. One `Wal::sync()`
  covers the whole pending batch (flush-on-drain at the end of each event-loop pass, or
  inline when `--max-batch` records or `max_batch_bytes` accumulate; `--max-batch 1` is the
  old fsync-per-record behavior). Only after the sync succeeds are the records applied, in
  LSN order, and their waiters released in the same order. Invariants:
  1. The visible job table never contains a record that is not on stable storage, so every
     RPC reply, replica ack and HTTP success is issued after its record's barrier.
  2. `durable_lsn()` advances only on a successful data sync. Rotation data-syncs the
     outgoing segment and fsyncs the directory for the new one before any record lands in
     it, so a sync of the active descriptor certifies every segment the batch touched.
  3. Owner order is local barrier, then replica RPC. A replica acks (`kCreated`) only from
     its own barrier waiter. A submit/lease/done success therefore means "power-loss durable
     on the owner and at least one pinned replica" (W=2 unchanged).
  4. A write or sync failure poisons the store: every unapplied waiter is released with
     failure, nothing more may commit, `/healthz` turns 503 and the node exits 70 for a
     supervisor restart. Replay alone decides which unsynced records survived.
  5. Per-job ordering: while a job has a transition behind the barrier it is "busy";
     validations that read its state (replica apply/replicate, token checks in ack, claims,
     takeover finalization, resync adoption, lease selection, expiry) are parked behind it
     and run against committed state only. A retry of an idempotency key whose SUBMIT is
     still behind the barrier joins the original (same id or same failure), never mints a
     twin.
  6. Compaction cuts at the watermark: pending records are synced and applied first, the
     snapshot is built from applied state, and their waiters run only after the new
     generation is installed (so follow-up appends land in it). A failure after the CURRENT
     swap poisons the store.
  7. Drain completes only when no submit is behind the barrier and nothing is pending.
- **(2026-11-04) Known refusal vs unknown outcome.** Submits now return three failure kinds:
  HTTP 429 `overloaded` (refused by admission before an id or log append: definitely not
  accepted, retry later), HTTP 503 with `durability_unknown` (the local barrier failed after
  the append: the job may exist; retry with the SAME key, which resolves to the original id if
  it survived), and the existing retryable 503s (owner unavailable, draining, quorum not
  reached). Earlier code reported a failed local fsync as "nothing promised", which was not
  true once bytes had reached the page cache.
- **(2026-11-04) Bounded admission.** Optional (`--admission`). Unique submits are refused
  at the owner when in-flight submits, their payload bytes, the owned Ready+Leased backlog or
  the WAL pending share would exceed a limit; once tripped, the node sheds until every gauge
  is back under `resume_ratio` of its limit (hysteresis). Duplicate keys are resolved first
  and never refused by admission. Gateways bound forwarded submits separately (they cannot
  distinguish retries, so that refusal is still pre-allocation and retryable). Lease, ack,
  replication, repair and takeover traffic is never subject to admission, and unique submits
  may use only `wal_submit_share` of the WAL pending bound, reserving the rest for progress
  records.
