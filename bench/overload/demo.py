#!/usr/bin/env python3
"""Bounded local demo: a live 5-node tautq cluster you can overload and break.

Use case: an engineer wants to SEE that a webhook queue either accepts a job durably or
refuses it promptly, keeps delivering accepted jobs through a receiver stall or node kill,
and never loses an acknowledged job.

Runs inside the Linux dev container (root, network namespaces; the node uses epoll):

  python3 bench/overload/demo.py --bin /build/release --port 8707 [--recorded DIR]

then open http://127.0.0.1:8707 (publish the port to host loopback only). Every number on
the LIVE tab comes from the running processes: tautq-bench (open-loop client), each node's
/v1/stats, the sink's /admin/stats, and tautq-verify run over a snapshot copy of the WALs.
The RECORDED tab renders a finished campaign's result files and is labelled as such.

Bounds: one cluster, one load run at a time; rate <= 8000/s; duration <= 20 s; sink stall
<= 2000 ms; verify on a copy, 60 s timeout. No arbitrary commands are accepted.
"""
import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
CLUSTER = os.path.join(REPO, "chaos", "cluster.sh")
NODES = 5
GW = ",".join(f"http://10.77.0.{10 + i}:8080" for i in range(NODES))
SINK = "http://10.77.0.1:8090/hook"
ARMS = {
    "both": "--max-batch 256 --admission --max-backlog 1000",
    "batch": "--max-batch 256",
    "admission": "--max-batch 1 --admission --max-backlog 1000",
    "unbatched": "--max-batch 1",
}
MAX_RATE, MAX_SECONDS, MAX_STALL = 8000, 20, 2000


def http(method, url, timeout=1.5):
    try:
        req = urllib.request.Request(url, data=b"" if method == "POST" else None, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception:
        return 0, ""


class Demo:
    def __init__(self, binp, workdir, recorded):
        self.bin = binp
        self.work = workdir
        self.recorded = recorded
        self.lock = threading.Lock()
        self.arm = None
        self.run_no = 0
        self.data = None
        self.load = None  # dict while a bench runs / after it ran
        self.events = []

    def env(self):
        return {**os.environ, "BIN": self.bin, "NODE_ARGS": ARMS.get(self.arm or "both", ""),
                "WORKER_ARGS": "--quiet --concurrency 8", "VISIBILITY_MS": "8000"}

    def log(self, msg):
        self.events.append([time.strftime("%H:%M:%S"), msg])
        self.events = self.events[-40:]

    def cluster(self, action, arm=None):
        with self.lock:
            if self.load and self.load.get("running"):
                return {"error": "load running"}
            subprocess.run(f"bash {CLUSTER} down", shell=True, env=self.env(), capture_output=True)
            if action == "down":
                self.arm = None
                self.log("cluster stopped")
                return {"ok": True}
            if arm not in ARMS:
                return {"error": "unknown arm"}
            self.arm = arm
            self.run_no += 1
            self.data = os.path.join(self.work, f"cluster-{self.run_no}")
            shutil.rmtree(self.data, ignore_errors=True)
            subprocess.run(f"bash {CLUSTER} up {self.data} >{self.work}/cluster-{self.run_no}.log 2>&1",
                           shell=True, env=self.env())
            for _ in range(50):
                if all(http("GET", f"http://10.77.0.{10 + i}:8080/healthz")[0] == 200 for i in range(NODES)):
                    break
                time.sleep(0.2)
            time.sleep(2)
            self.load = None
            self.log(f"cluster up: arm={arm} ({ARMS[arm]})")
            return {"ok": True}

    def start_load(self, rate, seconds):
        rate = max(1, min(MAX_RATE, int(rate)))
        seconds = max(1, min(MAX_SECONDS, int(seconds)))
        with self.lock:
            if not self.arm:
                return {"error": "start the cluster first"}
            if self.load and self.load.get("running"):
                return {"error": "load already running"}
            raw = os.path.join(self.work, f"raw-{self.run_no}-{int(time.time())}.csv")
            proc = subprocess.Popen(
                [f"{self.bin}/tautq-bench", "--gateways", GW, "--sink-url", SINK, "--phases",
                 f"{seconds}:{rate}", "--seed", str(int(time.time()) % 100000), "--out", raw,
                 "--prefix", f"demo{self.run_no}t{int(time.time()) % 100000}", "--retries", "3",
                 "--timeout-ms", "3000"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.load = {"running": True, "raw": raw, "rate": rate, "seconds": seconds,
                         "started": time.monotonic(), "finished": None, "proc": proc}
            self.log(f"load: {rate}/s for {seconds}s (open loop, Poisson)")
            return {"ok": True}

    def load_counts(self):
        l = self.load
        if not l:
            return None
        if l.get("running") and l["proc"].poll() is not None:
            l["running"] = False
            l["finished"] = time.monotonic()
            self.log("load finished")
        elapsed_at = l["finished"] if l["finished"] is not None else time.monotonic()
        keys = {}
        try:
            with open(l["raw"]) as f:
                next(f, None)
                for line in f:
                    p = line.rstrip("\n").split(",")
                    if len(p) < 7:
                        continue
                    k, code = p[0], int(p[5])
                    prev = keys.get(k)
                    rank = {201: 4, 200: 4, 429: 3, 503: 2, 0: 1, -1: 0}.get(code, 1)
                    if prev is None or rank > prev[0]:
                        keys[k] = (rank, code, int(p[1]), (int(p[4]) - int(p[2])) / 1000, p[6])
        except FileNotFoundError:
            pass
        acc = [v for v in keys.values() if v[0] == 4]
        ref = [v for v in keys.values() if v[0] == 3]
        lat_ref = sorted(v[3] for v in ref)
        lat_acc = sorted(v[3] for v in acc)
        q = lambda xs, p: xs[min(len(xs) - 1, int(p * (len(xs) - 1)))] if xs else None
        return {
            "running": l["running"], "rate": l["rate"], "seconds": l["seconds"],
            "elapsed": round(elapsed_at - l["started"], 1),
            "keys_answered": len(keys), "accepted": len(acc), "refused_429": len(ref),
            "unknown_or_unavailable": len([v for v in keys.values() if v[0] in (1, 2)]),
            "client_saturated": len([v for v in keys.values() if v[0] == 0]),
            "ack_ms_p99": q(lat_acc, 0.99), "refusal_ms_p99": q(lat_ref, 0.99),
        }

    def accepted_ids(self):
        out = {}
        for raw in glob.glob(os.path.join(self.work, f"raw-{self.run_no}-*.csv")):
            with open(raw) as f:
                next(f, None)
                for line in f:
                    p = line.rstrip("\n").split(",")
                    if len(p) >= 7 and p[5] in ("200", "201"):
                        out[p[0]] = p[6]
        return out

    def state(self):
        nodes = []
        for i in range(NODES):
            st, body = http("GET", f"http://10.77.0.{10 + i}:8080/v1/stats", 0.8)
            nodes.append(json.loads(body) if st == 200 else None)
        st, body = http("GET", "http://10.77.0.1:8090/admin/stats", 0.8)
        sink = json.loads(body) if st == 200 else None
        delivered = None
        if self.data and os.path.exists(os.path.join(self.data, "sink.log")):
            acc = self.accepted_ids()
            seen = set()
            with open(os.path.join(self.data, "sink.log")) as f:
                for line in f:
                    p = line.split()
                    if len(p) > 1:
                        seen.add(p[1])
            delivered = {"accepted_total": len(acc), "accepted_delivered": len([k for k in acc if k in seen])}
        return {"arm": self.arm, "arms": ARMS, "nodes": nodes, "sink": sink, "load": self.load_counts(),
                "delivered": delivered, "events": self.events,
                "bounds": {"max_rate": MAX_RATE, "max_seconds": MAX_SECONDS, "max_stall_ms": MAX_STALL}}

    def sink(self, stall_ms, fail_rate):
        stall_ms = max(0, min(MAX_STALL, int(stall_ms)))
        fail_rate = max(0.0, min(0.9, float(fail_rate)))
        st, body = http("POST", f"http://10.77.0.1:8090/admin/fault?stall_ms={stall_ms}&fail_rate={fail_rate}")
        self.log(f"receiver: stall {stall_ms} ms, fail rate {fail_rate:.2f}")
        return {"ok": st == 200}

    def node(self, i, action):
        i = int(i)
        if not (0 <= i < NODES) or not self.arm:
            return {"error": "bad node or no cluster"}
        if action == "kill":
            subprocess.run(f"bash {CLUSTER} kill-node {i}", shell=True, env=self.env(), capture_output=True)
            self.log(f"SIGKILL node {i} (and its worker)")
        elif action == "start":
            subprocess.run(f"bash {CLUSTER} start-node {i} {self.data} >>{self.work}/cluster-{self.run_no}.log 2>&1",
                           shell=True, env=self.env())
            self.log(f"restarted node {i} over its WAL")
        elif action == "drain":
            http("POST", f"http://10.77.0.{10 + i}:8080/v1/drain")
            self.log(f"drain node {i}")
        return {"ok": True}

    def verify(self):
        if not self.data:
            return {"error": "no cluster"}
        snap = os.path.join(self.work, "verify-snapshot")
        shutil.rmtree(snap, ignore_errors=True)
        os.makedirs(snap)
        for i in range(NODES):
            src = os.path.join(self.data, f"node{i}")
            if os.path.exists(src):
                shutil.copytree(src, os.path.join(snap, f"node{i}"))
        shutil.copy(os.path.join(self.data, "sink.log"), os.path.join(snap, "sink.log"))
        acc = self.accepted_ids()
        with open(os.path.join(snap, "submits.csv"), "w") as f:
            for k, v in acc.items():
                f.write(f"{k},{v},201\n")
        logs = ",".join(os.path.join(snap, f"node{i}") for i in range(NODES))
        r = subprocess.run([f"{self.bin}/tautq-verify", "--submits", os.path.join(snap, "submits.csv"),
                            "--sink", os.path.join(snap, "sink.log"), "--logs", logs, "--report-twins"],
                           capture_output=True, text=True, timeout=60)
        self.log("verify on WAL snapshot: exit %d" % r.returncode)
        return {"exit": r.returncode, "stdout": r.stdout[-3000:], "stderr": r.stderr[-3000:],
                "note": "Snapshot of live WALs. Jobs still in flight are reported as not completed; "
                        "re-run after the queue drains for the final verdict."}

    def job(self, key):
        acc = self.accepted_ids()
        jid = acc.get(key)
        if not jid:
            return {"error": "key not accepted in this cluster run"}
        st, body = http("GET", f"http://10.77.0.10:8080/v1/jobs/{jid}")
        return {"key": key, "id": jid, "status_http": st, "job": json.loads(body) if st == 200 else body}

    def recorded_runs(self):
        out = []
        if not self.recorded:
            return {"mode": "recorded run", "runs": out}
        for p in sorted(glob.glob(os.path.join(self.recorded, "*", "result.json"))):
            try:
                d = json.load(open(p))
            except Exception:
                continue
            out.append({k: d.get(k) for k in ("arm", "profile", "seed", "offered", "accepted", "outcomes",
                                              "goodput_per_s", "ack_ms", "refusal_ms", "e2e_ms", "queue_depth",
                                              "undelivered_after_drain", "verify", "wal_flushes", "node_cpu_s",
                                              "faults_applied")})
        return {"mode": "recorded run", "source": self.recorded, "runs": out}


def make_handler(demo):
    page = open(os.path.join(HERE, "demo", "index.html"), "rb").read()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, obj, ctype="application/json"):
            body = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            if u.path == "/":
                return self.send(200, page, "text/html; charset=utf-8")
            if u.path == "/api/state":
                return self.send(200, demo.state())
            if u.path == "/api/job":
                return self.send(200, demo.job(q.get("key", "")[:120]))
            if u.path == "/api/recorded":
                return self.send(200, demo.recorded_runs())
            self.send(404, {"error": "not found"})

        def do_POST(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            try:
                if u.path == "/api/cluster":
                    return self.send(200, demo.cluster(q.get("action", "up"), q.get("arm")))
                if u.path == "/api/load":
                    return self.send(200, demo.start_load(q.get("rate", 500), q.get("seconds", 10)))
                if u.path == "/api/sink":
                    return self.send(200, demo.sink(q.get("stall_ms", 0), q.get("fail_rate", 0)))
                if u.path == "/api/node":
                    return self.send(200, demo.node(q.get("i", -1), q.get("action", "")))
                if u.path == "/api/verify":
                    return self.send(200, demo.verify())
            except (ValueError, subprocess.TimeoutExpired) as e:
                return self.send(400, {"error": str(e)})
            self.send(404, {"error": "not found"})

    return H


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="/build/release")
    ap.add_argument("--port", type=int, default=8707)
    ap.add_argument("--work", default="/data/demo")
    ap.add_argument("--recorded", default=None, help="campaign dir with */result.json (recorded tab)")
    a = ap.parse_args()
    os.makedirs(a.work, exist_ok=True)
    demo = Demo(a.bin, a.work, a.recorded)
    srv = ThreadingHTTPServer(("0.0.0.0", a.port), make_handler(demo))
    print(f"tautq demo on :{a.port} (bin={a.bin}, work={a.work}, recorded={a.recorded})", flush=True)
    try:
        srv.serve_forever()
    finally:
        subprocess.run(f"bash {CLUSTER} down", shell=True, env={**os.environ, "BIN": a.bin}, capture_output=True)


if __name__ == "__main__":
    main()
