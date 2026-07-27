# tautq — PROGRESS

State log, newest at top. Same conventions as taut/docs/PROGRESS.md: what shipped, what was
verified and how, slips stated plainly. Operating model unchanged from the taut amendment
(2026-07-20): Claude implements + functionally verifies; Laksh directs design and commits.

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
