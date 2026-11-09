#!/usr/bin/env python3
"""Four-arm overload/durability study driver (Linux, root, network namespaces).

One *run* = fresh 5-node netns cluster (chaos/cluster.sh) for one arm, one offered-load
profile, one seed: open-loop tautq-bench against all gateways, optional faults on a fixed
schedule, bounded quiesce, SIGKILL teardown, then tautq-verify over every node's WAL plus
per-run metrics computed from raw per-request and per-delivery records.

  study.py run      --arm both --profile overload --seed 1 --out DIR [--protocol P]
  study.py campaign --protocol protocol.json --out DIR [--only-arms a,b] [--dry-run]

Everything a run produced stays in its directory (raw.csv, sink.log, node WALs, metrics
samples, verify.txt, result.json). The protocol file fixes arms, profiles, seeds, SLOs and
the run order; the campaign refuses to run if its sha256 differs from --expect-sha256.
"""
import argparse
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CLUSTER = os.path.join(REPO, "chaos", "cluster.sh")
NODES = 5
GW = ",".join(f"http://10.77.0.{10 + i}:8080" for i in range(NODES))
SINK = "http://10.77.0.1:8090/hook"

DEFAULT_PROTOCOL = {
    "bin": "/build/release",
    "baseline_node_bin": "/build/baseline-release/tautq-node",
    "worker_args": "--quiet --concurrency 8",
    "visibility_ms": 8000,
    "arms": {
        "baseline": {"node_bin": "baseline", "node_args": ""},
        "batch": {"node_args": "--max-batch 256"},
        "admission": {"node_args": "--max-batch 1 --admission"},
        "both": {"node_args": "--max-batch 256 --admission"},
    },
    "profiles": {"smoke": {"phases": "5:200", "faults": []}},
    "seeds": [1],
    "quiesce_s": 30,
    "slo_e2e_ms": 2000,
    "slo_submit_ms": 1000,
}


def sh(cmd, env=None, check=True, timeout=None):
    e = dict(os.environ)
    if env:
        e.update(env)
    r = subprocess.run(cmd, shell=True, env=e, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd}: {r.returncode}\n{r.stdout}\n{r.stderr}")
    return r


def http_get(url, timeout=2.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return None


def http_post(url, timeout=2.0):
    try:
        req = urllib.request.Request(url, data=b"", method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return None


METRIC_RE = re.compile(r"^([a-z_]+)(\{[^}]*\})? ([0-9.eE+-]+)$")


def parse_metrics(text):
    out = {}
    for line in (text or "").splitlines():
        m = METRIC_RE.match(line)
        if m:
            out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
    return out


def pids_of(pattern):
    r = sh(f"pgrep -f '{pattern}'", check=False)
    return [int(p) for p in r.stdout.split()]


def proc_cpu_rss(pid):
    try:
        with open(f"/proc/{pid}/stat") as f:
            parts = f.read().rsplit(")", 1)[1].split()
        ticks = os.sysconf("SC_CLK_TCK")
        cpu = (int(parts[11]) + int(parts[12])) / ticks
        rss = int(parts[21]) * os.sysconf("SC_PAGE_SIZE")
        return cpu, rss
    except Exception:
        return None


class Sampler(threading.Thread):
    """1 Hz: every node's /metrics (queue depth, leases, repair, WAL gauges) + node CPU/RSS."""

    def __init__(self, path):
        super().__init__(daemon=True)
        self.path = path
        self.stop = threading.Event()
        self.rows = []

    def run(self):
        with open(self.path, "w") as f:
            while not self.stop.is_set():
                t = time.monotonic()
                row = {"t": time.time(), "nodes": []}
                for i in range(NODES):
                    m = parse_metrics(http_get(f"http://10.77.0.{10 + i}:8080/metrics", 0.8))
                    row["nodes"].append({k: v for k, v in m.items() if not k.startswith("tautq_job_latency")})
                procs = {}
                for pid in pids_of("tautq[-]node --listen 10.77"):
                    v = proc_cpu_rss(pid)
                    if v:
                        procs[pid] = v
                row["node_procs"] = procs
                f.write(json.dumps(row) + "\n")
                f.flush()
                self.rows.append(row)
                self.stop.wait(max(0.0, 1.0 - (time.monotonic() - t)))


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[k]


def run_one(proto, arm, profile_name, seed, out):
    arm_cfg = proto["arms"][arm]
    prof = proto["profiles"][profile_name]
    os.makedirs(out, exist_ok=True)
    data = os.path.join(out, "cluster")
    shutil.rmtree(data, ignore_errors=True)
    env = {
        "BIN": proto["bin"],
        "NODE_ARGS": arm_cfg.get("node_args", ""),
        "WORKER_ARGS": proto["worker_args"],
        "VISIBILITY_MS": str(proto["visibility_ms"]),
    }
    if arm_cfg.get("node_bin") == "baseline":
        env["NODE_BIN"] = proto["baseline_node_bin"]
    sh(f"bash {CLUSTER} down", env=env, check=False)
    # Background daemons must not inherit our pipes, or capture would wait for them forever.
    sh(f"bash {CLUSTER} up {data} >{out}/cluster_up.log 2>&1", env=env)
    deadline = time.time() + 20
    while time.time() < deadline:
        if all(http_get(f"http://10.77.0.{10 + i}:8080/healthz") for i in range(NODES)):
            break
        time.sleep(0.3)
    time.sleep(3)  # SWIM convergence
    sampler = Sampler(os.path.join(out, "samples.jsonl"))
    sampler.start()
    before = [parse_metrics(http_get(f"http://10.77.0.{10 + i}:8080/metrics")) for i in range(NODES)]

    # Faults on a fixed schedule relative to bench start: [t_s, action, arg...]
    fault_log = []
    bench_start = time.time()

    def fault_thread():
        for f in prof.get("faults", []):
            t, action = f[0], f[1]
            while time.time() - bench_start < t:
                time.sleep(0.05)
            ts = time.time() - bench_start
            if action == "kill":
                sh(f"bash {CLUSTER} kill-node {f[2]}", env=env, check=False)
            elif action == "start":
                sh(f"bash {CLUSTER} start-node {f[2]} {data} >>{out}/cluster_up.log 2>&1", env=env, check=False)
            elif action == "sink":
                http_post(f"http://10.77.0.1:8090/admin/fault?{f[2]}")
            fault_log.append([round(ts, 3), action] + list(f[2:]))

    ft = threading.Thread(target=fault_thread, daemon=True)
    ft.start()
    raw = os.path.join(out, "raw.csv")
    b = sh(
        f"{proto['bin']}/tautq-bench --gateways {GW} --sink-url {SINK} --phases {prof['phases']} "
        f"--seed {seed} --out {raw} --prefix {arm[:2]}{seed}x --timeout-ms {proto.get('client_timeout_ms', 3000)} "
        f"--retries {proto.get('client_retries', 3)} --payload {proto.get('payload', 128)}",
        check=False,
    )
    bench_end = time.time()
    ft.join(timeout=60)
    with open(os.path.join(out, "bench.err"), "w") as f:
        f.write(b.stderr)

    # Outcomes per key.
    keys = {}
    with open(raw) as f:
        next(f)
        for line in f:
            k, att, intended, sent, done, code, jid = line.rstrip("\n").split(",")
            r = keys.setdefault(k, {"attempts": []})
            r["attempts"].append((int(att), int(intended), int(sent), int(done), int(code), jid))
    accepted = {}
    for k, r in keys.items():
        r["attempts"].sort()
        first_intended = r["attempts"][0][1]
        r["first_intended"] = first_intended
        ok = [a for a in r["attempts"] if a[4] in (200, 201)]
        if ok:
            accepted[k] = ok[0][5]
            r["outcome"] = "accepted"
            r["ack_ms"] = (ok[0][3] - first_intended) / 1000
        elif any(a[4] == 429 for a in r["attempts"]):
            r["outcome"] = "refused"
        elif all(a[4] == -1 for a in r["attempts"]):
            r["outcome"] = "client_saturated"
        else:
            r["outcome"] = "unresolved"

    # Quiesce: wait (bounded) for every accepted key to reach the sink.
    sink_log = os.path.join(data, "sink.log")
    q_deadline = time.time() + proto["quiesce_s"]
    delivered = {}
    while True:
        delivered = {}
        try:
            with open(sink_log) as f:
                for line in f:
                    p = line.split()
                    if len(p) >= 6 and p[1] not in delivered:
                        delivered[p[1]] = int(p[5])
        except FileNotFoundError:
            pass
        if all(k in delivered for k in accepted) or time.time() > q_deadline:
            break
        time.sleep(1)
    quiesce_s = time.time() - bench_end
    after = [parse_metrics(http_get(f"http://10.77.0.{10 + i}:8080/metrics")) for i in range(NODES)]
    undelivered_at_quiesce = len([k for k in accepted if k not in delivered])
    delivered_by_quiesce = dict(delivered)
    # Drain phase (no new load): every acknowledged job must EVENTUALLY complete. This is the
    # correctness horizon, separate from the SLO horizon above.
    drain_cap = proto.get("drain_cap_s", 180)
    d_start = time.time()
    while undelivered_at_quiesce and time.time() - d_start < drain_cap:
        try:
            with open(sink_log) as f:
                for line in f:
                    p = line.split()
                    if len(p) >= 6 and p[1] not in delivered:
                        delivered[p[1]] = int(p[5])
        except FileNotFoundError:
            pass
        if all(k in delivered for k in accepted):
            break
        time.sleep(1)
    drain_s = time.time() - d_start if undelivered_at_quiesce else 0.0
    sampler.stop.set()
    sampler.join(timeout=5)
    sh(f"bash {CLUSTER} down", env=env, check=False)

    # verify over all WALs.
    subs = os.path.join(out, "submits.csv")
    with open(subs, "w") as f:
        for k, jid in accepted.items():
            f.write(f"{k},{jid},201\n")
    logs = ",".join(os.path.join(data, f"node{i}") for i in range(NODES))
    v = sh(
        f"{proto['bin']}/tautq-verify --submits {subs} --sink {sink_log} --logs {logs}",
        check=False,
    )
    with open(os.path.join(out, "verify.txt"), "w") as f:
        f.write(v.stdout + v.stderr)

    # Metrics.
    win_s = sum(float(p.split(":")[0]) for p in prof["phases"].split(","))
    slo_e2e = proto["slo_e2e_ms"]
    slo_sub = proto["slo_submit_ms"]
    out_counts = {}
    for r in keys.values():
        out_counts[r["outcome"]] = out_counts.get(r["outcome"], 0) + 1
    ack_ms = [r["ack_ms"] for r in keys.values() if r["outcome"] == "accepted"]
    refusal_ms = []
    refusal_service_ms = []
    refusal_schedule_ms = []
    for r in keys.values():
        for a in r["attempts"]:
            if a[4] == 429:
                refusal_ms.append((a[3] - a[1]) / 1000)
                refusal_service_ms.append((a[3] - a[2]) / 1000)
                refusal_schedule_ms.append((a[2] - a[1]) / 1000)
    e2e = []
    good = 0
    done_in_window = 0
    for k in accepted:
        if k in delivered_by_quiesce:
            ms = (delivered_by_quiesce[k] - keys[k]["first_intended"]) / 1000
            e2e.append(ms)
            done_in_window += 1
            if ms <= slo_e2e and keys[k]["ack_ms"] <= slo_sub:
                good += 1
    depth_series = []
    for row in sampler.rows:
        d = 0
        for n in row["nodes"]:
            d += n.get('tautq_queue_depth{state="ready"}', 0) + n.get("tautq_inflight_leases", 0)
        depth_series.append(d)
    cpu = {}
    rss_max = 0
    for row in sampler.rows:
        for pid, (c, rss) in row["node_procs"].items():
            cpu.setdefault(pid, [c, c])
            cpu[pid][1] = c
            rss_max = max(rss_max, rss)
    node_cpu_s = sum(v[1] - v[0] for v in cpu.values())

    def delta(name):
        tot = 0.0
        for b0, a0 in zip(before, after):
            if name in a0:
                tot += a0[name] - b0.get(name, 0)
        return tot

    vm = re.search(r"accepted=(\d+).*duplicate_deliveries=(\d+).*fallback_twins=(\d+)\) dead_letter=(\d+).*-> (PASS|FAIL)", v.stdout)
    lost = re.search(r"FAIL: (\d+) accepted jobs not completed", v.stderr)
    result = {
        "arm": arm,
        "profile": profile_name,
        "seed": seed,
        "phases": prof["phases"],
        "faults_applied": fault_log,
        "window_s": win_s,
        "offered": len(keys),
        "outcomes": out_counts,
        "accepted": len(accepted),
        "delivered_accepted": done_in_window,
        "goodput_per_s": good / win_s,
        "slo": {"e2e_ms": slo_e2e, "submit_ms": slo_sub},
        "good_jobs": good,
        "ack_ms": {"p50": pct(ack_ms, 0.5), "p99": pct(ack_ms, 0.99), "max": max(ack_ms) if ack_ms else None},
        # refusal_ms is frozen: response minus intended arrival. The components make
        # load-generator scheduling delay and actual request/response time visible too.
        "refusal_ms": {"n": len(refusal_ms), "p50": pct(refusal_ms, 0.5), "p99": pct(refusal_ms, 0.99), "max": max(refusal_ms) if refusal_ms else None},
        "refusal_latency_components_ms": {
            "service_response": {"n": len(refusal_service_ms), "p50": pct(refusal_service_ms, 0.5), "p99": pct(refusal_service_ms, 0.99), "max": max(refusal_service_ms) if refusal_service_ms else None},
            "loadgen_scheduling": {"n": len(refusal_schedule_ms), "p50": pct(refusal_schedule_ms, 0.5), "p99": pct(refusal_schedule_ms, 0.99), "max": max(refusal_schedule_ms) if refusal_schedule_ms else None},
        },
        "e2e_ms": {"p50": pct(e2e, 0.5), "p99": pct(e2e, 0.99), "max": max(e2e) if e2e else None},
        "queue_depth": {"max": max(depth_series) if depth_series else None, "end": depth_series[-1] if depth_series else None},
        "quiesce_s": round(quiesce_s, 2),
        "undelivered_after_quiesce": undelivered_at_quiesce,
        "drain_s": round(drain_s, 2),
        "undelivered_after_drain": len([k for k in accepted if k not in delivered]),
        "wal_flushes": delta("tautq_wal_flushes_total"),
        "wal_records": delta("tautq_wal_records_committed_total"),
        "completions_ok_metric": delta('tautq_completions_total{result="ok"}'),
        "node_cpu_s": round(node_cpu_s, 2),
        "node_rss_max_bytes": rss_max,
        "verify": {
            "exit": v.returncode,
            "pass": v.returncode == 0,
            "line": v.stdout.strip(),
            "lost": int(lost.group(1)) if lost else 0,
            "duplicate_deliveries": int(vm.group(2)) if vm else None,
            "fallback_twins": int(vm.group(3)) if vm else None,
        },
    }
    with open(os.path.join(out, "result.json"), "w") as f:
        json.dump(result, f, indent=1)
    # Keep the evidence compact: WAL dirs stay (verify can be rerun); drop worker stderr.
    return result


def load_protocol(path):
    if not path:
        return DEFAULT_PROTOCOL
    with open(path) as f:
        p = json.load(f)
    return p


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--arm", required=True)
    r.add_argument("--profile", required=True)
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--out", required=True)
    r.add_argument("--protocol")
    c = sub.add_parser("campaign")
    c.add_argument("--protocol", required=True)
    c.add_argument("--expect-sha256")
    c.add_argument("--out", required=True)
    c.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.cmd == "run":
        res = run_one(load_protocol(a.protocol), a.arm, a.profile, a.seed, a.out)
        print(json.dumps(res, indent=1))
        return
    raw = open(a.protocol, "rb").read()
    digest = hashlib.sha256(raw).hexdigest()
    if a.expect_sha256 and digest != a.expect_sha256:
        sys.exit(f"protocol sha256 {digest} != expected {a.expect_sha256}")
    proto = json.loads(raw)
    order = []
    for seed in proto["seeds"]:
        for prof in proto["profile_order"]:
            for arm in proto["arms"]:
                order.append((arm, prof, seed))
    random.Random(proto.get("order_seed", 0)).shuffle(order)  # interleaved, pre-registered order
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "order.json"), "w") as f:
        json.dump({"protocol_sha256": digest, "order": order}, f, indent=1)
    if a.dry_run:
        print(json.dumps(order))
        return
    for n, (arm, prof, seed) in enumerate(order):
        d = os.path.join(a.out, f"{n:03d}-{arm}-{prof}-s{seed}")
        if os.path.exists(os.path.join(d, "result.json")):
            continue  # resumable; never overwrite a finished run
        t = time.time()
        try:
            res = run_one(proto, arm, prof, seed, d)
            print(f"[{n + 1}/{len(order)}] {arm:9s} {prof:12s} s{seed} offered={res['offered']} "
                  f"acc={res['accepted']} good/s={res['goodput_per_s']:.0f} verify={res['verify']['pass']} "
                  f"({time.time() - t:.0f}s)", flush=True)
        except Exception as e:  # keep going; the failure is evidence
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, "error.txt"), "w") as f:
                f.write(repr(e))
            print(f"[{n + 1}/{len(order)}] {arm} {prof} s{seed} ERROR {e!r}", flush=True)


if __name__ == "__main__":
    main()
