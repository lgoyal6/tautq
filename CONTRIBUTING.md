# Contributing to tautq

Thanks for looking. tautq is a five-node webhook queue with no coordinator, so almost
every interesting change is a change to a distributed commit rule. The bar near those
rules is high and the bar for everything else is normal.

## The contract you must not break

**At-least-once execution, exactly-once completion.** Every duplicate delivery must be
attributable to an expired lease or an ownership change, and no accepted job may be
lost. `src/verify_main.cc` (`tautq-verify`) is the judge: it replays every node's WAL,
the submitter's accepted-submits log and the sink's receipts, then asserts no loss, one
`lease_seq` per completed job across all logs, and a cause on disk for every duplicate.
If a change of yours makes that harness fail, the change is wrong, not the harness.

Corollaries worth stating, because they are easy to violate by accident:

- Nothing is acknowledged before its WAL record is fsynced locally and accepted by a
  majority of the pinned replica set (W=2 of 3). SUBMIT, LEASE, DONE and TAKEOVER are
  all commit points, not bookkeeping.
- The lease is committed to a majority *before* it is granted. That, not the timeout, is
  what stops a partitioned stale owner from double-granting. Do not "optimise" the grant
  ahead of its commit.
- `JobStore::commit()` in `src/store.cc` is the only write path, and replay runs the same
  `apply()` as live commits. Replayed state is identical to running state by
  construction; keep it that way rather than adding a recovery-only branch.

## Getting oriented

| Path | What lives there |
|---|---|
| `docs/DESIGN-protocol.md` | The approved protocol. Read before touching a commit rule. |
| `docs/DECISIONS.md` | D1 to D8, why each shape was chosen and what was rejected. |
| `src/queue_node.cc` | The protocol itself: submit, lease, ack, takeover, resync. |
| `src/wal.cc`, `src/store.cc` | Append-only log, CRC32C frames, torn-tail truncation. |
| `src/verify_main.cc` | `tautq-verify`, the chaos suite's judge. |
| `tests/unit/cluster.h` | N-node cluster over taut's SimNet with a virtual clock. |
| `chaos/chaos.sh` | The four-scenario matrix: kill, partition, loss, stale-log. |
| `bench/` | The ramp and loss-matrix scripts, and the CSVs the README's tables come from. |

## Building and testing

tautq builds against a sibling checkout of [taut](https://github.com/lgoyal6/taut).
`TAUTQ_TAUT_DIR` defaults to `../taut`; override it if yours lives elsewhere.

```bash
cmake --preset dev              # Debug + ASan/UBSan, clang++ and Ninja
cmake --build --preset dev
ctest --preset dev              # 48 unit and protocol tests over SimNet

./scripts/smoke.sh              # 3-node loopback end-to-end, no sudo
./chaos/chaos.sh all            # the full matrix (sudo, network namespaces)
```

`cmake --preset release` is the same build without sanitizers; measure chaos timing
there, not under ASan. CI runs both presets, a `clang-format --dry-run -Werror` check
over `src/` and `tests/unit/`, and the whole chaos matrix at `JOBS=40` on every pull
request, uploading chaos evidence as an artifact when it fails.

## What makes a good PR here

- One concern per PR, with a test that fails before and passes after.
- Anything touching submit, lease, ack, takeover or recovery needs a SimNet test in
  `tests/unit/` and a local `./chaos/chaos.sh all` run first. SimNet is deterministic, so
  a protocol bug reproduces exactly; use it rather than a bespoke harness.
- Protocol changes need a note in `docs/DESIGN-protocol.md`, and a new decision needs an
  entry in `docs/DECISIONS.md` in the existing D-numbered format including what you
  rejected.
- No third-party runtime dependencies. HTTP server, WAL, metrics, ring and verifier are
  hand-written in `src/` on purpose; test-only deps come in via FetchContent.
- README benchmark numbers come from `bench/ramp.sh` and `bench/loss_matrix.sh`. If your
  change moves them, commit the new CSVs rather than editing the tables.
- Run `clang-format` before pushing. A format-drift failure is a wasted CI cycle.

## Good first areas

- **Group commit.** `Wal::append(sync=false)` and `Wal::sync()` already exist and are
  documented as the sanctioned optimisation, but nothing calls them: `JobStore::commit()`
  defaults `sync` to true and every call site takes the default. The README's knee at
  ~800 jobs/s is roughly 1.4k fsync/s per node, so this is the measurable win.
- **Overload behaviour.** Beyond ~1000 jobs/s nodes go intermittently unresponsive
  instead of shedding load. There is no admission control anywhere in `src/`.
- **Log compaction.** Segments rotate at 64 MiB (`Wal::set_segment_bytes`) but completed
  jobs are never pruned. `docs/DESIGN-protocol.md` marks compaction as v1.1.
- **Job bodies are capped at 640 bytes** (`kMaxJobBody` in `src/job.h`) so a job fits one
  taut datagram. Chunked bodies are unimplemented.
- **Portability.** `src/loop.h` is an epoll wrapper and says so; the runtime is
  Linux-only. A kqueue backend is self-contained work in `src/loop.cc`.

## Conduct

Be kind, assume good faith, argue with WAL traces and verifier output rather than
adjectives.
