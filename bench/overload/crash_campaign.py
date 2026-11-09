#!/usr/bin/env python3
"""Real-process fault campaign (Linux, root, network namespaces, real filesystem).

Each case starts a fresh 5-node cluster (group commit + admission), arms ONE fault on ONE
victim node through its environment (deterministic crashpoint SIGKILL, injected EIO fsync,
short write/ENOSPC, slow disk) or a real tiny ext4 filesystem that fills up, drives open-loop
load with the client retry contract, restarts the victim when it dies, retries every
non-accepted key with the SAME idempotency key, drains, SIGKILLs everything and runs
tautq-verify over all WALs. A case passes when:

  * every key the client saw accepted (2xx) is DONE in the merged logs (nothing lost,
    nothing parked in dead-letter),
  * no job completed twice and every duplicate delivery is attributable (verify),
  * no idempotency key maps to two job ids (stable ownership: zero twins),
  * the configured fault actually fired (otherwise the case is reported NOT_TRIGGERED).

Fault model: SIGKILL keeps the kernel page cache (process crash, not power loss); EIO/
ENOSPC injection is at the WAL syscall boundary; the tiny filesystem produces a genuine
ENOSPC from ext4. Power-loss truncation is covered by the unit tests' oracle.

  crash_campaign.py --out DIR [--cases name,...] [--bin /build/release]
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CLUSTER = os.path.join(REPO, "chaos", "cluster.sh")
NODES = 5
GW = ",".join(f"http://10.77.0.{10 + i}:8080" for i in range(NODES))
SINK = "http://10.77.0.1:8090/hook"
ARGS = "--max-batch 256 --admission --max-backlog 1000"

# name -> (victim node, env for victim, extra)
CASES = {
    "kill_before_append": (1, "TAUTQ_CRASHPOINT=submit.before_append:300", {}),
    "kill_after_append_before_flush": (1, "TAUTQ_CRASHPOINT=wal.after_write_before_sync:900", {}),
    "kill_after_flush_before_apply": (1, "TAUTQ_CRASHPOINT=wal.after_sync_before_apply:200", {}),
    "kill_after_local_flush_before_quorum": (1, "TAUTQ_CRASHPOINT=submit.after_flush_before_quorum:300", {}),
    "kill_replica_after_flush_before_reply": (2, "TAUTQ_CRASHPOINT=replica.after_flush_before_reply:400", {}),
    "kill_after_quorum_before_response": (1, "TAUTQ_CRASHPOINT=submit.after_quorum_before_response:300", {}),
    "kill_after_done_before_response": (1, "TAUTQ_CRASHPOINT=done.after_quorum_before_response:300", {}),
    "kill_compaction_snapshot_durable": (1, "TAUTQ_CRASHPOINT=compact.snapshot_durable:2", {"compact": True}),
    "kill_compaction_installed": (1, "TAUTQ_CRASHPOINT=compact.installed:2", {"compact": True}),
    "kill_compaction_before_old_log_deletion": (1, "TAUTQ_CRASHPOINT=compact.before_old_log_deletion:2", {"compact": True}),
    "fsync_eio": (1, "TAUTQ_FAULT_FSYNC_FAIL_AT=400", {}),
    "short_write_enospc": (1, "TAUTQ_FAULT_WRITE_FAIL_AT=900", {}),
    "slow_replica_disk": (2, "TAUTQ_FAULT_SYNC_DELAY_US=20000", {"no_death": True}),
    "real_disk_full": (1, "", {"tinyfs_kb": 2048}),
    "drain_under_load": (1, "", {"drain": True, "no_death": True}),
}


def sh(cmd, env=None, check=False, timeout=None):
    e = dict(os.environ)
    if env:
        e.update(env)
    return subprocess.run(cmd, shell=True, env=e, capture_output=True, text=True, timeout=timeout)


def get(url, timeout=1.5):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception:
        return 0, ""


def post(url, timeout=2.0):
    try:
        req = urllib.request.Request(url, data=b"x", method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception:
        return 0, ""


def node_pid(i):
    r = sh(f"pgrep -f 'tautq[-]node --listen 10.77.0.{10 + i}:9000'")
    pids = [int(p) for p in r.stdout.split()]
    return pids[-1] if pids else None


def run_case(name, out, binp, phases):
    victim, fault_env, extra = CASES[name]
    os.makedirs(out, exist_ok=True)
    data = os.path.join(out, "cluster")
    shutil.rmtree(data, ignore_errors=True)
    env = {"BIN": binp, "NODE_ARGS": ARGS, "WORKER_ARGS": "--quiet --concurrency 4", "VISIBILITY_MS": "4000"}
    if fault_env:
        env[f"NODE_ENV_{victim}"] = fault_env
    sh(f"bash {CLUSTER} down", env=env)
    tiny = None
    if extra.get("tinyfs_kb"):
        os.makedirs(data, exist_ok=True)
        img = os.path.join(out, "tiny.img")
        tiny = f"{data}/node{victim}"
        os.makedirs(tiny, exist_ok=True)
        sh(f"truncate -s {extra['tinyfs_kb']}K {img} && mkfs.ext4 -q -F -O ^has_journal {img} && "
           f"mount -o loop {img} {tiny}", check=True)
    sh(f"bash {CLUSTER} up {data} >{out}/cluster.log 2>&1", env=env)
    for _ in range(60):
        if all(get(f"http://10.77.0.{10 + i}:8080/healthz")[0] == 200 for i in range(NODES)):
            break
        time.sleep(0.3)
    time.sleep(3)

    events = []
    stop = threading.Event()
    t0 = time.time()

    def watchdog():
        restarted = 0
        while not stop.is_set():
            if node_pid(victim) is None and restarted < 3 and not extra.get("drain"):
                events.append([round(time.time() - t0, 2), "victim_dead"])
                time.sleep(1.0)
                if tiny:
                    # The operator frees space: grow the image and the filesystem online.
                    img = os.path.join(out, "tiny.img")
                    sh(f"truncate -s 64M {img} && losetup -c $(losetup -j {img} | cut -d: -f1) && "
                       f"resize2fs $(losetup -j {img} | cut -d: -f1)")
                    events.append([round(time.time() - t0, 2), "disk_grown"])
                e2 = dict(env)
                e2.pop(f"NODE_ENV_{victim}", None)  # restart healthy
                sh(f"bash {CLUSTER} start-node {victim} {data} >>{out}/cluster.log 2>&1", env=e2)
                restarted += 1
                events.append([round(time.time() - t0, 2), "victim_restarted"])
            if extra.get("compact"):
                st, body = post(f"http://10.77.0.{10 + victim}:8080/v1/compact", timeout=5)
                if st:
                    events.append([round(time.time() - t0, 2), "compact", st])
                stop.wait(1.5)
            else:
                stop.wait(0.25)

    wd = threading.Thread(target=watchdog, daemon=True)
    wd.start()
    if extra.get("drain"):
        def drainer():
            time.sleep(4)
            events.append([round(time.time() - t0, 2), "drain", post(f"http://10.77.0.{10 + victim}:8080/v1/drain")[0]])
        threading.Thread(target=drainer, daemon=True).start()

    raw = os.path.join(out, "raw.csv")
    sh(f"{binp}/tautq-bench --gateways {GW} --sink-url {SINK} --phases {phases} --seed 11 "
       f"--out {raw} --prefix {name[:6]} --retries 3 --timeout-ms 2000", timeout=300)
    time.sleep(3)

    # Client contract: every key whose outcome is not a 2xx is retried with the same key.
    def outcomes():
        keys = {}
        with open(raw) as f:
            next(f)
            for line in f:
                k, _a, _i, _s, _d, code, jid = line.rstrip("\n").split(",")
                if code in ("200", "201"):
                    keys[k] = jid
                else:
                    keys.setdefault(k, None)
        return keys

    keys = outcomes()
    retried = {}
    def retry_one(k):
        gw = 10 + (hash(k) % NODES)
        if extra.get("drain") and gw == 10 + victim:
            gw = 10 + (victim + 1) % NODES
        st, body = post(f"http://10.77.0.{gw}:8080/v1/jobs?key={k}&url={SINK}", timeout=3)
        m = re.search(r'"id":"([0-9a-f]+)"', body)
        return k, st, m.group(1) if m else None

    with ThreadPoolExecutor(max_workers=32) as retries:
        for rnd in range(6):
            pending = [k for k, v in keys.items() if v is None]
            if not pending:
                break
            futures = [retries.submit(retry_one, k) for k in pending]
            for fut in as_completed(futures):
                k, st, job_id = fut.result()
                if st in (200, 201) and job_id:
                    keys[k] = job_id
                    retried[k] = st
            time.sleep(2)
    unresolved_refused = [k for k, v in keys.items() if v is None]

    # Drain: every accepted key must reach the sink.
    accepted = {k: v for k, v in keys.items() if v}
    deadline = time.time() + 90
    delivered = set()
    while time.time() < deadline:
        delivered = set()
        try:
            with open(os.path.join(data, "sink.log")) as f:
                for line in f:
                    p = line.split()
                    if len(p) > 1:
                        delivered.add(p[1])
        except FileNotFoundError:
            pass
        if all(k in delivered for k in accepted):
            break
        time.sleep(1)
    stop.set()
    wd.join(timeout=5)
    victim_err = open(os.path.join(data, f"node{victim}.err")).read()
    sh(f"bash {CLUSTER} down", env=env)
    if tiny:
        sh(f"umount {tiny}")
        # Copy what the victim had back out of the image for verification.
        img = os.path.join(out, "tiny.img")
        mnt = os.path.join(out, "tiny-mnt")
        os.makedirs(mnt, exist_ok=True)
        sh(f"mount -o loop,ro {img} {mnt} && cp -a {mnt}/. {tiny}/ && umount {mnt}")

    subs = os.path.join(out, "submits.csv")
    with open(subs, "w") as f:
        for k, v in accepted.items():
            f.write(f"{k},{v},201\n")
    logs = ",".join(os.path.join(data, f"node{i}") for i in range(NODES))
    v = sh(f"{binp}/tautq-verify --submits {subs} --sink {data}/sink.log --logs {logs}")
    # Twins: any key with more than one job id across all logs.
    tw = sh(f"{binp}/tautq-verify --submits {subs} --sink {data}/sink.log --logs {logs} --report-twins")
    twins = re.search(r"keys_with_multiple_ids=(\d+)", tw.stdout)
    configured_faults = fault_env.upper()
    fired = {
        "crashpoint": "TAUTQ_CRASHPOINT" in configured_faults and "SIGKILL" in victim_err,
        "fsync": "TAUTQ_FAULT_FSYNC_FAIL_AT" in configured_faults and "poisoned" in victim_err,
        "write": "TAUTQ_FAULT_WRITE_FAIL_AT" in configured_faults and "poisoned" in victim_err,
        "disk_full": bool(tiny) and "poisoned" in victim_err,
        "slow": "SYNC_DELAY" in fault_env,
        "drain": bool(extra.get("drain")),
    }
    triggered = any(fired.values())
    res = {
        "case": name,
        "phases": phases,
        "victim": victim,
        "fault_env": fault_env,
        "events": events,
        "offered": len(keys),
        "accepted": len(accepted),
        "accepted_via_retry": len(retried),
        "never_accepted": len(unresolved_refused),
        "undelivered_accepted": len([k for k in accepted if k not in delivered]),
        "victim_stderr_tail": victim_err.strip().splitlines()[-4:],
        "fault_fired": triggered,
        "verify_exit": v.returncode,
        "verify": (v.stdout + v.stderr).strip().splitlines()[-3:],
        "keys_with_multiple_ids": int(twins.group(1)) if twins else None,
    }
    res["status"] = (
        "NOT_TRIGGERED" if not triggered else
        "PASS" if v.returncode == 0 and res["undelivered_accepted"] == 0 and res["keys_with_multiple_ids"] == 0 else
        "FAIL"
    )
    with open(os.path.join(out, "result.json"), "w") as f:
        json.dump(res, f, indent=1)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--bin", default="/build/release")
    ap.add_argument("--cases", default=",".join(CASES))
    ap.add_argument("--phases", default="12:600", help="tautq-bench open-loop phases spec")
    a = ap.parse_args()
    results = []
    for name in a.cases.split(","):
        d = os.path.join(a.out, name)
        if os.path.exists(os.path.join(d, "result.json")):
            results.append(json.load(open(os.path.join(d, "result.json"))))
            continue
        t = time.time()
        r = run_case(name, d, a.bin, a.phases)
        results.append(r)
        print(f"{name:42s} {r['status']:13s} acc={r['accepted']}/{r['offered']} retry_acc={r['accepted_via_retry']} "
              f"undeliv={r['undelivered_accepted']} twins={r['keys_with_multiple_ids']} ({time.time() - t:.0f}s) "
              f"{r['verify'][-1] if r['verify'] else ''}", flush=True)
    with open(os.path.join(a.out, "summary.json"), "w") as f:
        json.dump(results, f, indent=1)


if __name__ == "__main__":
    main()
