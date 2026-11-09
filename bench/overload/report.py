#!/usr/bin/env python3
"""Aggregate a four-arm campaign (study.py campaign) into summary.json + a markdown table and
evaluate the pre-registered win criteria. Per-seed values are kept; cells report the median
and [min, max] across seeds (n=3 is too small for a bootstrap interval, so none is claimed).

  report.py --campaign DIR --protocol protocol.json --out-json summary.json --out-md table.md
"""
import argparse
import glob
import json
import os
import statistics

ARMS = ["baseline", "batch", "admission", "both"]


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def rng(xs):
    xs = [x for x in xs if x is not None]
    return [min(xs), max(xs)] if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign", required=True)
    ap.add_argument("--protocol", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-md", required=True)
    a = ap.parse_args()
    proto = json.load(open(a.protocol))
    runs = []
    errors = []
    for d in sorted(glob.glob(os.path.join(a.campaign, "[0-9]*"))):
        p = os.path.join(d, "result.json")
        if os.path.exists(p):
            r = json.load(open(p))
            r["dir"] = os.path.basename(d)
            runs.append(r)
        else:
            errors.append(os.path.basename(d))
    cells = {}
    for r in runs:
        cells.setdefault((r["profile"], r["arm"]), []).append(r)

    def per_job(r, num):
        return num / r["delivered_accepted"] if r["delivered_accepted"] else None

    summary = {"protocol_sha256_file": a.protocol, "runs": len(runs), "missing_or_error": errors, "cells": {}}
    md = ["| profile | arm | n | offered/s | accepted % | refused % | goodput/s med [min,max] | ack p99 ms | refusal p99 from intended ms | service response p99 ms | loadgen delay p99 ms | e2e p99 ms | max queue | fsync/job | CPU ms/job | undelivered after drain | verify pass |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for prof in proto["profile_order"]:
        for arm in ARMS:
            rs = sorted(cells.get((prof, arm), []), key=lambda r: r["seed"])
            if not rs:
                continue
            c = {
                "n": len(rs),
                "seeds": [r["seed"] for r in rs],
                "offered": [r["offered"] for r in rs],
                "accepted": [r["accepted"] for r in rs],
                "refused": [r["outcomes"].get("refused", 0) for r in rs],
                "unresolved": [r["outcomes"].get("unresolved", 0) + r["outcomes"].get("client_saturated", 0) for r in rs],
                "goodput_per_s": [r["goodput_per_s"] for r in rs],
                "ack_p99_ms": [r["ack_ms"]["p99"] for r in rs],
                "refusal_p99_ms": [r["refusal_ms"]["p99"] for r in rs],
                "refusal_service_p99_ms": [r["refusal_latency_components_ms"]["service_response"]["p99"] for r in rs],
                "refusal_schedule_p99_ms": [r["refusal_latency_components_ms"]["loadgen_scheduling"]["p99"] for r in rs],
                "refusal_n": [r["refusal_ms"]["n"] for r in rs],
                "e2e_p99_ms": [r["e2e_ms"]["p99"] for r in rs],
                "queue_max": [r["queue_depth"]["max"] for r in rs],
                "fsync_per_job": [per_job(r, r["wal_flushes"]) if r["wal_flushes"] else None for r in rs],
                "cpu_ms_per_job": [per_job(r, r["node_cpu_s"] * 1000) for r in rs],
                "undelivered_after_drain": [r["undelivered_after_drain"] for r in rs],
                "verify_pass": [r["verify"]["pass"] for r in rs],
                "verify_line": [r["verify"]["line"] for r in rs],
                "dirs": [r["dir"] for r in rs],
            }
            summary["cells"][f"{prof}/{arm}"] = c
            win = rs[0]["window_s"]
            g = c["goodput_per_s"]
            md.append(
                f"| {prof} | {arm} | {c['n']} | {med(c['offered']) / win:.0f} | "
                f"{100 * med([x / y for x, y in zip(c['accepted'], c['offered'])]):.1f} | "
                f"{100 * med([x / y for x, y in zip(c['refused'], c['offered'])]):.1f} | "
                f"{med(g):.0f} [{min(g):.0f}, {max(g):.0f}] | {fmt(med(c['ack_p99_ms']))} | {fmt(med(c['refusal_p99_ms']))} | {fmt(med(c['refusal_service_p99_ms']))} | {fmt(med(c['refusal_schedule_p99_ms']))} | "
                f"{fmt(med(c['e2e_p99_ms']))} | {fmt(med(c['queue_max']))} | {fmt(med(c['fsync_per_job']), 2)} | "
                f"{fmt(med(c['cpu_ms_per_job']), 2)} | {sum(c['undelivered_after_drain'])} | {sum(c['verify_pass'])}/{c['n']} |")

    # Pre-registered criteria.
    C = summary["cells"]
    crit = {}

    def cell(p, arm):
        return C.get(f"{p}/{arm}")

    b, o = cell("overload", "baseline"), cell("overload", "both")
    if b and o:
        ratio = med(o["goodput_per_s"]) / med(b["goodput_per_s"]) if med(b["goodput_per_s"]) else float("inf")
        crit["W1_overload_goodput"] = {"both_median": med(o["goodput_per_s"]), "baseline_median": med(b["goodput_per_s"]),
                                       "ratio": ratio, "pass": ratio >= 2.0}
    safety = []
    for k, c in C.items():
        if k.split("/")[1] in ("batch", "admission", "both"):
            safety.append({"cell": k, "verify_pass": c["verify_pass"], "undelivered_after_drain": c["undelivered_after_drain"]})
    crit["W2_safety"] = {"cells": safety, "pass": all(all(s["verify_pass"]) for s in safety)}
    w3 = []
    for p in ("overload", "burst"):
        c = cell(p, "both")
        if c:
            for n, v in zip(c["refusal_n"], c["refusal_p99_ms"]):
                if n:
                    w3.append(v)
    w3_service = []
    w3_schedule = []
    for p in ("overload", "burst"):
        c = cell(p, "both")
        if c:
            for n, service, schedule in zip(c["refusal_n"], c["refusal_service_p99_ms"], c["refusal_schedule_p99_ms"]):
                if n:
                    w3_service.append(service)
                    w3_schedule.append(schedule)
    crit["W3_prompt_refusal"] = {
        "original_intended_offer_p99_ms_per_run": w3,
        "service_response_p99_ms_per_run": w3_service,
        "loadgen_scheduling_delay_p99_ms_per_run": w3_schedule,
        "pass": bool(w3) and all(v <= 100 for v in w3),
    }
    w4 = []
    for p in ("overload", "burst"):
        c = cell(p, "both")
        if c:
            w4 += c["queue_max"]
    crit["W4_bounded_queue"] = {"queue_max_per_run": w4, "pass": bool(w4) and all(v <= 6000 for v in w4)}
    lb, lo = cell("low", "baseline"), cell("low", "both")
    if lb and lo:
        crit["W5_no_low_load_regression"] = {"both_ack_p99_median": med(lo["ack_p99_ms"]), "baseline_ack_p99_median": med(lb["ack_p99_ms"]),
                                             "pass": med(lo["ack_p99_ms"]) <= med(lb["ack_p99_ms"]) + 5}
    summary["criteria"] = crit
    json.dump(summary, open(a.out_json, "w"), indent=1)
    with open(a.out_md, "w") as f:
        f.write("\n".join(md) + "\n\n")
        for k, v in crit.items():
            f.write(f"- {k}: {'PASS' if v.get('pass') else 'FAIL'} {json.dumps({x: y for x, y in v.items() if x not in ('cells',)})}\n")
    print("\n".join(md))
    for k, v in crit.items():
        print(k, "PASS" if v.get("pass") else "FAIL")


def fmt(v, d=0):
    return "-" if v is None else f"{v:.{d}f}"


if __name__ == "__main__":
    main()
