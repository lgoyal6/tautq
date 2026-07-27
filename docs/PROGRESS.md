# tautq — PROGRESS

State log, newest at top. Same conventions as taut/docs/PROGRESS.md: what shipped, what was
verified and how, slips stated plainly. Operating model unchanged from the taut amendment
(2026-07-20): Claude implements + functionally verifies; Laksh directs design and commits.

---

## M10 + FINAL STATUS — v0.1.0 complete (2026-07-27)

**All ten modules shipped in one day-long agentic run (M2–M10), every module verified
before the next was built.** 48/48 unit/protocol tests (ASan/UBSan, deterministic SimNet),
chaos matrix 4/4 in both dev and release builds, 3-node smoke, full docker-compose stack
validated end-to-end (submit → SWIM-converged 5 nodes → worker → sink receipt; Prometheus
ready; Grafana healthy).

**Load results (committed CSVs in bench/):**
- **Knee at ~800 jobs/s** on a shared 4-core VM: 16,000/16,000 jobs accepted+completed in
  20 s at p50 30 ms / p99 107 ms; p99 stays ~63 ms through 600/s. Beyond ~1000/s the
  single-threaded fsync-per-commit path saturates (~9 fsyncs/job cluster-wide) and nodes
  wedge until load drops — rows kept in the CSV flagged suspect, NOT published as capacity.
  Group commit is the sanctioned next step (Wal API already supports it), not implemented.
- **Loss matrix at 200/s:** full throughput at every level 0–20%; p99 63 → 1019 ms,
  p50 only 27 → 65 ms (taut's 25 ms RTO floor earning its keep).
- Loadgen hardening from the first bad run: counter-reset clamping + unreachable-node
  detection so a suspect row can never be published silently.

**The acceptance-criteria answers:**
- jobs/sec: **800/s sustained (5-node cluster), knee published with mechanism analysis.**
- p99 under sustained load: **107 ms at the knee; 63 ms in the flat region.**
- Failure the chaos suite caught that was then fixed: **the partition scenario proved
  taut's SWIM never reconverges after a healed partition once Dead verdicts land (8 jobs
  stalled forever); fixed as taut v0.1.2 (post-Dead refutation channel) and now covered by
  a taut regression test.** Runner-ups, all real: SWIM terminal-Dead blocking restarts
  (v0.1.1, caught at feasibility), an ASan-caught HTTP use-after-free, and the verifier
  learning to attribute fallback twins.

**Still open / honest debts:** first GitHub push validates CI runner-side; taut must be
public (or a PAT secret added) for tautq CI checkout; log compaction, group commit,
overload admission control, >640 B payloads all documented as not-yet.

---

## M9 — CI: chaos on every PR (2026-07-27)

**Shipped:** `.github/workflows/ci.yml` — build+ctest matrix {dev (ASan/UBSan), release},
clang-format gate, and the **chaos job**: release build → loopback smoke → the full
4-scenario chaos matrix in netns (iptables-only faults were chosen in M8 precisely so
plain ubuntu runners need no netem/kernel modules). Failure uploads the kept evidence dirs
as artifacts. taut is checked out as a sibling (public repo or `TAUT_READ_TOKEN` secret —
the D8 open item; must be resolved at push time).

**Second catch by the suite (release-mode run under 10% loss):** one "unexplained"
duplicate delivery — diagnosed as the DOCUMENTED §2 fallback twin: a forwarded submit's
response was lost, the gateway fell back to local ownership, two distinct jobs shared one
idempotency key (`jobs_seen = accepted + 1` was the tell). The system behaved as designed
and disclosed; the VERIFIER's attributability rule was too narrow — it now also attributes
duplicates to multi-job keys and reports them as `fallback_twins`, keeping "zero
unexplained duplicates" a real, tight assertion.

**Verified locally (the CI jobs' exact commands):** dev ctest 48/48 + release ctest,
release smoke PASS, release chaos matrix 4/4 PASS (clean re-run: 0 twins, 0 dupes).
Honest caveat: the workflow itself runs first on the first GitHub push — runner-side
behavior is validated only then.

---

## M8 — 5-node netns cluster, chaos matrix, deploy artifacts (2026-07-27)

**Shipped:**
- `chaos/cluster.sh` — 5 network namespaces (tq0..tq4, 10.77.0.10+i) behind a bridge; sink
  + chaos driver in the root ns. Faults are per-node iptables rules — partitions by source
  drop, loss via `-m statistic` — deliberately NO netem/module dependency so plain CI
  runners work. kill-node/start-node for crash-restart.
- `chaos/chaos.sh` — the four-scenario matrix, each with submits streaming through
  rotating gateways WHILE the fault is live: (a) SIGKILL a node mid-stream + restart;
  (b) 2-node minority partition + heal; (c) 10% UDP loss everywhere; (d) stale-log restart
  (WAL tail chopped by 400 bytes after SIGKILL).
- `tautq-verify` — reads every node's WAL + the submitter's accept log + the sink receipts
  and asserts §5: (1) every accepted job's merged cluster state is Done; (2) all DONE
  records per job carry ONE lease_seq (exactly-once completion); (3) every duplicate sink
  delivery is attributable (>1 grant or an epoch change in the logs) — zero unexplained.
- `deploy/` — Dockerfile (multi-stage, sibling-checkout context), docker-compose (5 nodes
  + 3 workers + sink + Prometheus + Grafana, NET_ADMIN for in-container chaos),
  prometheus.yml, provisioned Grafana dashboard JSON (depth, leases, replication backlog,
  p50/95/99 via histogram_quantile, throughput, churn, takeovers/fenced, DLQ).
- `tautq-loadgen` + `bench/ramp.sh` + `bench/loss_matrix.sh` (M10 ammunition): open-loop
  generator; latency quantiles derived from the CLUSTER's histogram deltas.

**THE CATCH (the acceptance-criteria bug):** first full matrix run: kill/loss/stale PASS,
**partition FAIL — 8 accepted jobs permanently stalled Ready** on minority node 4. Root
cause traced to taut SWIM: a partition held past suspicion_timeout leaves both sides
holding Dead verdicts; Dead members are never probed, so after heal no packet ever crosses
the link again, and the accusations' gossip budgets were spent into the partition, so the
accused never refutes. Permanent split; minority-owned jobs gated off quorum forever.
**Fixed in taut v0.1.2** (dead-probing once per period carrying the target's own Dead
rumor + Dead-rumor re-queue on contact; see taut D27). Partition scenario re-run: PASS
(60/60). Full matrix re-run: see below.

**Chaos matrix results (5-node netns cluster, streams live during faults) — 4/4 PASS:**
- kill:      accepted=59 delivered=59 dup=0 dead_letter=0 → PASS
- partition: accepted=60 delivered=60 dup=0 dead_letter=0 → PASS (after the v0.1.2 fix)
- loss(10%): accepted=60 delivered=60 dup=0 dead_letter=0 → PASS
- stale-log: accepted=59 delivered=59 dup=0 dead_letter=0 → PASS (tail chopped 400 B)

**Harness bugs fixed along the way:** pkill patterns matching their own sudo wrapper
(bracket trick); verifier needs sudo (root-owned WALs + tail truncation on replay);
per-stream key ranges (batches were deduping into each other, shrinking coverage).

---

## M7 — the running service: HTTP admin, metrics, node/worker/sink binaries (2026-07-27)

**Shipped:**
- `loop.{h,cc}` — tautq's own level-triggered epoll wrapper (the node multiplexes two UDP
  sockets + a TCP listener + connections; taut's loop is UDP-only).
- `http.{h,cc}` — hand-rolled HTTP/1.1 server (D8, no framework): Content-Length bodies,
  keep-alive, **async responses** (a handler parks its Respond until a quorum completes;
  responding to a gone client is a safe no-op). Lease grants ride in `X-Tautq-*` response
  headers so the worker needs no JSON parser anywhere.
- `http_client.{h,cc}` — tiny blocking client for worker/loadgen processes.
- `metrics.{h,cc}` — hand-rolled Prometheus text: counters, gauges, log-scale histogram
  (Grafana computes p50/95/99 via histogram_quantile). QueueNode instrumented: submits,
  duplicates, leases, completions ok/fail, expirations, deadletters, takeovers,
  fenced_stale, replication backlog, queue-depth gauges, submit→DONE latency histogram.
- `tautq-node` — binary wiring RealUdpTransport×2 + Swim (SWIM port = data+1; JOIN on boot
  for the v0.1.1 rejoin path) + SwimMembership + QueueNode + admin API:
  POST /v1/jobs|lease|ack|drain, GET /v1/jobs/{id}|/v1/nodes|/metrics|/healthz.
- `tautq-worker` — separate process (chaos can SIGKILL it independently): long-polls
  lease, POSTs body to the job URL with `Idempotency-Key`, acks ok/fail; retries acks
  through 503 (completion quorum forming).
- `tautq-sink` — the chaos oracle: logs `mono_ms key attempt path bytes` per accepted
  delivery; seeded `--fail-rate` for injected 500s.

**Bug ASan caught before any human could:** HttpServer::Respond passed the connection
map's own shared_ptr slot BY REFERENCE into send_response; a close inside the response
path erased that slot mid-use (heap-use-after-free). All conn-mutating paths now take the
shared_ptr by value.

**Verified:** 48/48 ctest green in Lima (4 new: routing/query/header/body round-trip,
deferred-response pattern, histogram render). **First real end-to-end run PASSED**
(`scripts/smoke.sh`): 3 nodes + sink + 2 workers on loopback, 10 jobs via 3 different
gateways → 10/10 Done, sink logged exactly 10 unique idempotency keys. clang-format clean.

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
