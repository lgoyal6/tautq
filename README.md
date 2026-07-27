# tautq

A distributed webhook-delivery service — 5+ nodes, no central coordinator — built on
[taut](../taut), a reliable-UDP transport + SWIM membership library.

A job is "POST this JSON body to this URL with this idempotency key, and retry until a 2xx
comes back." Jobs are submitted to any node, replicated to a majority of a 3-node replica set
before the submit is acknowledged, leased to workers under a visibility timeout with epoch
fencing, and completed exactly once. Delivery semantics: **at-least-once execution,
exactly-once completion** — duplicate deliveries carry the same `Idempotency-Key`, the
contract real webhook providers publish.

Status: protocol approved and recorded (docs/DESIGN-protocol.md, docs/DECISIONS.md);
implementation not started. This README gains the architecture diagram, chaos matrix, and
throughput/p99-vs-loss table as those artifacts are actually produced — no forward-looking
claims.
