# tautq — PROGRESS

State log, newest at top. Same conventions as taut/docs/PROGRESS.md: what shipped, what was
verified and how, slips stated plainly. Operating model unchanged from the taut amendment
(2026-07-20): Claude implements + functionally verifies; Laksh directs design and commits.

---

## M6 — failover: CLAIM/TAKEOVER, RESYNC, drain (2026-07-27)

**Shipped:** the §3/§4 failover machinery, completing the protocol core.
- **Takeover:** on a SWIM death verdict, the first alive member of each dead-owned job's
  pinned replica order claims it: local `TAKEOVER{e+1}` fsync first, then CLAIM RPCs;
  the job becomes leasable only at **majority of the pinned set counting self**. An
  unconfirmed local takeover is provably harmless — every grant/completion is quorum-fenced.
  Cascading deaths covered (each death event rescans all dead-owned jobs); claim retries
  with idempotent re-claims; a peer missing the copy gets a full Replicate then a retry.
- **Lease inheritance:** CLAIM grants return the replica's pre-claim `(state, lease_seq)`.
  Because a claim majority intersects every lease majority, a committed lease ALWAYS
  surfaces — the successor records it at its epoch, waits a full visibility window, and
  accepts the original worker's old-epoch token (`current_lease` now keys on seq, which is
  monotone per job across epochs).
- **RESYNC:** triggered by any kStaleEpoch (fenced stale owner), by lost claims, and for
  every owned active job at startup. Adopts a peer copy only when it strictly advances
  (`job_advances`: epoch > , then Done > DeadLetter > higher seq > Leased>Ready) — a stale
  view can never regress a committed lease. Demotion drops all lease bookkeeping.
- **Drain:** stops grants, routes submits away, invites successors to claim via
  DrainHandoff (the drainer itself grants the claim, so majority is immediate); done-cb
  when no owned active jobs remain.
- **Bug found by this module:** M4/M5 replica-slot iteration assumed self == slot 0, which
  breaks the moment a takeover puts self elsewhere — generalized to peer-slot iteration
  everywhere (quorum, repair, reachability).

**Verified in Lima under ASan/UBSan: 44/44 ctest green** (6 new): owner death → claim
majority → lease+complete under epoch 2; **inherited committed lease → no double grant +
old-epoch token completes** (crown 1); **thawed stale owner fenced by kStaleEpoch
everywhere, then self-demotes via resync** (crown 2); stale-log restart adopts Done and
never re-executes; owner+replica dead → CP stall, recovers when one returns; drain hands
off all jobs, fully operable at new owners. Two test-only races fixed (waits keyed on the
local takeover instead of the claim majority). clang-format clean.

---

## M5 — leases, completion, expiry, dead-letter (2026-07-27)

**Shipped (queue_node + store guards + Method::Apply):**
- **Lease grant (§3's core line):** owner picks an owned Ready job (FIFO deque, lazy stale
  drop, per-job backoff), and the LEASE record is **majority-committed before the worker
  gets the grant** — local fsync + one replica Apply ack. A membership gate refuses to even
  try when both replicas are dead (CP stall without burning `lease_seq`); a quorum failure
  after local commit self-expires the lease (fence values intact) with backoff.
- **Epoch fencing at replicas (`handle_apply`):** reject `epoch < known`, reject equal-epoch
  records from a sender that isn't the job's recorded owner (a legitimate successor CLAIMs
  first, M6). Records from a fenced stale owner cannot commit anywhere.
- **Completion:** ack validates the exact `(epoch, lease_seq)` token; DONE is W=2 before the
  worker hears OK; retried acks on Done answer OK only once replica copies are confirmed
  (idempotent completion); **amnesty** for late acks on expired-but-not-re-leased jobs and
  post-DeadLetter completions (completion trumps parking). Nack = immediate expire +
  exponential backoff (1s·2^n cap 60s, in-memory).
- **Expiry/DLQ:** in-memory visibility timers (re-armed conservatively `now+visibility` on
  restart — log stays wall-clock-free); attempts == lease grants; grant that would exceed
  `max_attempts` parks the job as DeadLetter (W=2).
- **Reorder safety:** class-1 Apply RPCs are unordered, so `store.apply` got terminal-state
  rules — Done is absolute; DeadLetter yields only to Done; Expire requires exact
  `(epoch, lease_seq)` match. Deviation noted: quorum-failed lease attempts DO consume
  `lease_seq` (conservative, never double-grants; gate+backoff keep it slow) — recorded in
  DESIGN-protocol deviations.

**Verified in Lima under ASan/UBSan: 38/38 ctest green** (7 new): grant→ack→Done replicated
3/3 + idempotent re-ack; expiry→regrant with stale-token fencing (the no-double-completion
test); late-ack amnesty; attempts→DeadLetter; nack backoff; **partitioned owner stalls with
lease_seq unburned**; owner restart mid-lease → replayed lease honored, old token still
completes. clang-format clean.

---

## M4 — submit + replication: ring routing, W=2 quorum, dedup, repair (2026-07-27)

**Shipped:**
- `ring.{h,cc}` — consistent-hash ring per D2 (splitmix64 node positions — raw ekeys would
  cluster same-host nodes; FNV-1a key hash; successor walk). Consulted only at submit;
  replica set pinned in the record forever. `Membership` interface + `StaticMembership`
  (tests script liveness; M7 wraps taut::Swim).
- `store.{h,cc}` — JobStore: replay and live commits fold through the SAME `apply()`
  (restart view ≡ running view by construction). `commit()` = encode → WAL fsync → apply;
  epoch-guarded merges everywhere (older-epoch Replicate/Done/etc. can't clobber newer state).
- `queue_node.{h,cc}` — submit path per §§2-3: gateway ring-routes (FwdSubmit RPC), owner
  dedups by idem key, fsyncs SubmitRec, replicates to both replicas, **acks the client on
  W=2** (owner + 1); third copy async via the repair loop (pessimistic after restart —
  re-replication is idempotent). Ring-owner-unreachable fallback: gateway owns locally,
  documented dedup degradation. RPC handlers got deferred replies (`ReqCtx` + `respond()`)
  because a forwarded submit can't answer until quorum.

**Verified in Lima under ASan/UBSan: 31/31 ctest green** (6 new over a reusable SimNet
Cluster harness with per-node tmp WALs, scripted membership, crash/restart): quorum ack +
3/3 repair convergence + cross-gateway dedup; forward-to-owner under 2% loss; **kNoQuorum
when both replicas down** (job kept locally, disclosed); repair after replica returns;
restart rebuilds table + dedup index from log; fallback ownership when ring owner
unreachable. clang-format clean.

---

## M3 — job model, record serde, append-only WAL (2026-07-27)

**Shipped:**
- `job.h` — Job model + JobId (`origin ekey + boot_id/seq nonce`: unique with zero
  coordination, hex form for HTTP). Design point: lease deadlines never hit the log — replay
  is time-independent; a restarted/taking-over node re-arms `now + visibility` conservatively.
- `records.{h,cc}` — the 7 record types (Submit/Replicate carry the full job; Lease/Done/
  Expire carry the `(epoch, lease_seq)` fence; DeadLetter/Takeover) as a `std::variant`,
  strict bounded decode. Same bytes travel in Replicate RPCs and land in replica logs.
- `wal.{h,cc}` — hand-written segmented WAL: `u32 len | u32 crc32c | body` frames (CRC is
  `taut::crc32c`), fsync-on-commit + separate `sync()` for future group commit, segment
  rotation with directory fsync, replay with torn-tail truncation (short OR bad-CRC tail);
  corruption before the tail fails open() — that is replication's job to repair, stated.

**Verified in Lima under ASan/UBSan: 25/25 ctest green** (11 new), incl. torn-tail
(mid-record truncate → clean replay → appends land where the tear was), corrupt-tail CRC,
rotation with 64-byte segments (20 records, order preserved across files), job serde
rejecting every truncated prefix. clang-format clean.

---

## M2 — node skeleton: demux, boot_id HELLO, RPC layer (2026-07-27)

**Shipped:** build skeleton (CMake mirroring taut's presets/warnings/sanitizers; vendored
taut via `TAUTQ_TAUT_DIR` + `add_subdirectory`, sanitizer flags applied top-level so the
library is instrumented consistently) and the three transport-integration pieces:
- `wire.{h,cc}` — HELLO datagrams (non-taut magic `TQH`, boot_id + echoed-boot acks so a
  stale ack can't complete a new handshake) and the 14-byte RPC envelope
  (kind/method/req_id/status); `Method` space reserved for all later modules.
- `demux.{h,cc}` — one data socket per node → per-peer `PeerView` facades feeding per-peer
  `taut::Session`s; HELLOs split off before any session sees them; taut traffic from
  un-handshaked peers → `on_stranger` (never fed to a session); bounded per-peer inboxes.
- `rpc.{h,cc}` — request/response over class-1 sessions: HELLO handshake state machine
  (create-session-on-establish, teardown+fail-calls on boot_id change or SWIM `peer_dead`,
  rate-limited HELLO to strangers so a restarted node's counterpart re-handshakes), req_id
  matching, per-call deadlines, kTimeout/kPeerDown/kTooLarge/kBusy synthesized locally.

**Verified in Lima under ASan/UBSan: 14/14 ctest green**, incl.
`Rpc.PeerRestartFailsInflightThenRecovers` (kill+replace B mid-call: in-flight call fails
kPeerDown via the new incarnation's HELLO, next call succeeds after auto re-handshake) and
`Rpc.HandshakeAndCallsSurviveHeavyLoss` (20% loss). clang-format clean.

**Build snag (fixed):** taut's `include(Warnings)` resolved to tautq's module of the same
name via inherited `CMAKE_MODULE_PATH` — tautq now includes its cmake helpers by full path.

---

## S0 — protocol approved, repo scaffolded, taut v0.1.1 prerequisite shipped (2026-07-27)

- **Protocol approved by Laksh** (lease + replication; see docs/DESIGN-protocol.md and
  DECISIONS.md D1–D8). Workload framing: distributed webhook delivery service.
- **taut v0.1.1 shipped in the taut repo** (prerequisite found during feasibility): SWIM
  rejoin — lexicographic (incarnation, state) precedence so Alive@k+1 resurrects Dead@k, JOIN
  reply marking to kill an infinite JOIN ping-pong. 57/57 tests green in Lima (ASan/UBSan),
  3 new SWIM tests. Recorded there as D26 + DESIGN-swim.md "Rejoin".
- **Open items before CI lands:** taut public vs. PAT for vendoring (D8); Lima mount covers
  ~/tautq (builds of tautq happen in the Linux VM / containers).

**Next (module order):** node skeleton — demux transport, boot_id HELLO session reset,
class-1 RPC layer with request ids + SWIM-driven abandon. Then: log+replay → submit/replicate
→ lease/ack/expiry → failover/TAKEOVER/RESYNC → HTTP admin + metrics → compose + chaos → CI
→ load test + README.
