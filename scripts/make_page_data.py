"""Build the JSON the results page reads from the two benchmark CSVs.

The ramp CSV contains rows the README flags as suspect and deliberately does
not publish: past the knee the cluster stops degrading gracefully, nodes go
intermittently unresponsive, and a step can report no completions at all. Those
rows are carried through here marked `suspect`, so the page can show that they
exist and why they are excluded, rather than quietly dropping them or plotting
them as if they were measurements.

    python3 scripts/make_page_data.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "bench"
OUT = ROOT / "docs" / "data"

# The harness writes -1.0 for a percentile when nothing completed in the step.
# It is a sentinel, not a latency, and must never reach a chart.
NO_DATA = -1.0

# From the README: the published ramp stops at the knee.
KNEE = 800


def rows(name: str) -> list[dict]:
    with (BENCH / name).open() as fh:
        return [{k: float(v) for k, v in r.items()} for r in csv.DictReader(fh)]


def clean(r: dict) -> dict:
    out = dict(r)
    for k in ("p50_ms", "p95_ms", "p99_ms"):
        out[k] = None if r[k] == NO_DATA else r[k]
    out["completed_all"] = r["completed"] >= r["accepted"] > 0
    return out


def main() -> None:
    ramp = []
    for r in rows("ramp.csv"):
        row = clean(r)
        # Suspect for either reason the README gives: the step lost most of the
        # load it was supposed to offer, or nothing completed inside it.
        row["suspect"] = r["rate"] > KNEE
        row["no_completions"] = r["completed"] == 0
        ramp.append(row)

    payload = {
        "knee_rate": KNEE,
        "ramp": ramp,
        "loss": [clean(r) for r in rows("loss_matrix.csv")],
        "setup": {
            "nodes": 5,
            "host": "5-node netns cluster on a shared 4-core VM",
            "step_seconds": 20,
            "loss_rate": 200,
        },
    }
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "bench.json"
    path.write_text(json.dumps(payload, indent=1) + "\n")
    print(f"{path.relative_to(ROOT)}  {path.stat().st_size / 1024:.1f} kB")
    for r in ramp:
        tag = " SUSPECT" if r["suspect"] else ""
        p99 = "no data" if r["p99_ms"] is None else f"{r['p99_ms']:.1f} ms"
        print(f"  {int(r['rate']):>5}/s  completed {int(r['completed']):>6}  p99 {p99:>10}{tag}")


if __name__ == "__main__":
    main()
