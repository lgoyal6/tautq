# Resume output — tautq

Draft. Reword in your own voice. Every number traces to a committed CSV in `bench/`, the
chaos results in `docs/PROGRESS.md`, or the CI runs — do not inflate them, and keep the
taut bullet's honesty rules (losing axes disclosed).

## Bullet (full)

> **tautq** — distributed webhook-delivery service in C++20 (5 nodes, no coordinator) on
> my own reliable-UDP transport ([taut](https://github.com/lgoyal6/taut)). Jobs are
> fsync-durable on a majority of a consistent-hash replica set before the submit acks;
> leases carry epoch-fenced tokens majority-committed **before** the grant, so a
> partitioned stale owner provably cannot double-lease; failover inherits committed leases
> through the claim/lease quorum intersection. Hand-written WAL, SWIM-driven takeover,
> epoll HTTP plane, Prometheus/Grafana — zero third-party libraries. A netns chaos suite
> (SIGKILL mid-stream, healed partitions, 10 % loss, corrupted-log restarts) runs on every
> PR and replays all five WALs to prove **no accepted job lost, no job completed twice,
> zero unexplained duplicate deliveries**. Sustained **800 jobs/s at p99 107 ms** (p99
> 63 ms to 600/s); full throughput at 20 % packet loss (p50 65 ms). The chaos suite caught
> a real SWIM protocol gap — healed partitions never reconverge once Dead verdicts land —
> which I fixed in the transport library with a regression test.

## Bullet (tight, ~3 lines)

> Built and operated **tautq**, a coordinator-less distributed webhook-delivery service in
> C++20 on my own reliable-UDP transport: majority-committed WAL replication with
> epoch-fenced leases, SWIM failover that provably inherits committed leases, and a chaos
> suite (kill / partition / loss / corrupt-log, on every PR) asserting no job lost and none
> completed twice. **800 jobs/s sustained at p99 107 ms**; full throughput at 20 % loss.
> The suite caught (and I fixed) a real SWIM partition-heal bug in the transport.

## One-liner pairing with the taut bullet

> **tautq** — 5-node coordinator-less webhook-delivery service on taut: quorum-replicated
> WAL, epoch-fenced leases, chaos-tested on every PR (no loss / no double completion);
> 800 jobs/s at p99 107 ms.

## The numbers (source of truth)

| Claim | Value | Evidence |
|---|---|---|
| Sustained throughput (knee) | 800 jobs/s, 16,000/16,000 completed in 20 s | `bench/ramp.csv` |
| p99 at the knee / flat region | 107 ms / 63 ms | `bench/ramp.csv` |
| Loss tolerance | 100 % completion at 0/1/5/10/20 % UDP loss @ 200/s | `bench/loss_matrix.csv` |
| p50 at 20 % loss | 65 ms (2.4× the clean-link p50) | `bench/loss_matrix.csv` |
| Chaos matrix | 4/4 scenarios, ~60 jobs each, 0 lost / 0 double-completed / 0 unexplained dupes | `docs/PROGRESS.md` M8/M9, CI runs |
| Tests | 48 tautq + 58 taut, ASan/UBSan, deterministic SimNet | `ctest` both repos |
| Size | ~5.0k LOC src + ~2.1k tests/harness (tautq); ~2.5k + ~2.2k (taut) | `git ls-files \| xargs wc -l` |

## The interview stories (know these cold)

1. **The fencing story (why no double-lease):** LEASE is majority-committed before the
   worker gets the grant. A stale owner's epoch is below what the replicas adopted at
   takeover, so every Apply it sends is rejected (`kStaleEpoch`) — it *cannot* grant, then
   demotes itself via resync. Test: `Failover.StaleOwnerCannotGrantAfterTakeover`.
2. **The inheritance story (why failover doesn't re-run jobs):** a claim majority (2 of 3)
   always intersects the lease majority (2 of 3), so at least one claim grant carries the
   committed lease's `(state, lease_seq)`. The successor records it, waits a visibility
   window, and accepts the original worker's old-epoch token by seq. Test:
   `Failover.TakeoverInheritsCommittedLeaseNoDoubleGrant`.
3. **The bug the chaos suite caught:** SWIM post-Dead permanent split (taut v0.1.2) —
   partition held past suspicion timeout ⇒ both sides hold Dead verdicts ⇒ Dead members
   are never probed and the death rumor's gossip budget is spent ⇒ silence forever. Fix:
   probe one Dead member per period carrying its own death rumor so a live accused refutes
   at inc+1. Found as 8 permanently-stalled jobs; regression test
   `SymmetricPartitionHealsAfterDeadVerdicts`.
4. **The semantics story:** at-least-once execution, exactly-once completion. Literal
   exactly-once execution is impossible with visibility timeouts (SIGKILL mid-delivery
   must re-run); duplicates always carry the same `Idempotency-Key`. The verifier's
   "attributable duplicates" assertion is what makes this claim checkable, not hand-waving.
5. **Where it loses (say it first):** knee is fsync-bound at ~800/s (~9 fsyncs/job
   cluster-wide, single-threaded commit path) and overload behavior past ~1000/s is a
   wedge, not graceful degradation — group commit and admission control are the known,
   unimplemented fixes. f=1 durability. 640 B payload cap. No log compaction.

## Honest framing

- "AI-assisted implementation; the protocol design, verification strategy, and analysis
  are what I defend." The defensibility homework is `docs/DESIGN-protocol.md` +
  `docs/PROGRESS.md` + the five stories above — same rule as taut: don't put it on the
  resume until you can whiteboard stories 1–3 from memory.
- Every metric in prose exists in a committed CSV; the overload rows past the knee are IN
  the CSV, flagged suspect, and deliberately not claimed as capacity.
