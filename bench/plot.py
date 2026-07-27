#!/usr/bin/env python3
"""Render the README benchmark figures from the committed CSVs.

    python3 bench/plot.py

Reads bench/ramp.csv + bench/loss_matrix.csv, writes bench/ramp.png +
bench/loss_matrix.png. Suspect ramp rows (clamped/unreachable, quantiles -1) are
excluded from the plot exactly as they are excluded from the README table.
"""

import csv
import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = pathlib.Path(__file__).parent

# Series palette (validated, dataviz reference palette slots 1-3; p99 gets slot 1
# so the headline series carries the strongest contrast).
INK = "#0b0b0b"
MUTED = "#898781"
GRID = "#e1e0d9"
SURFACE = "#fcfcfb"
SERIES = {"p50": "#008300", "p95": "#e87ba4", "p99": "#2a78d6"}


def style(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.grid(True, axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def plot_series(ax, xs, rows, x_of):
    for name in ("p99", "p95", "p50"):
        ys = [float(r[f"{name}_ms"]) for r in rows]
        ax.plot(xs, ys, color=SERIES[name], linewidth=2, marker="o", markersize=6,
                label=name)
        ax.annotate(name, (xs[-1], ys[-1]), xytext=(8, 0), textcoords="offset points",
                    color=INK, fontsize=9, va="center")


def load(name):
    with open(HERE / name, newline="") as f:
        return [r for r in csv.DictReader(f)]


def clean(r):
    # Publishable = the same bar the README table applies: every offered submit accepted,
    # zero errors, everything completed, quantiles present. The overload rows stay in the
    # CSV as evidence but are not capacity claims.
    return (float(r["p99_ms"]) >= 0 and int(r["errors"]) == 0 and
            int(r["accepted"]) == int(r["offered"]) and
            int(r["completed"]) >= int(r["accepted"]))


def ramp():
    rows = [r for r in load("ramp.csv") if clean(r)]
    xs = [int(r["rate"]) for r in rows]
    fig, ax = plt.subplots(figsize=(6.4, 3.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    style(ax)
    plot_series(ax, xs, rows, "rate")
    ax.set_xlabel("offered rate (jobs/s), 20 s per step", color=MUTED, fontsize=9)
    ax.set_ylabel("submit → DONE latency (ms)", color=MUTED, fontsize=9)
    ax.set_title("End-to-end latency vs. offered rate — knee at ~800 jobs/s",
                 color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, labelcolor=INK, fontsize=9, loc="upper left")
    ax.set_xlim(left=0, right=max(xs) * 1.16)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(HERE / "ramp.png", facecolor=SURFACE)


def loss():
    rows = load("loss_matrix.csv")
    xs = [float(r["loss"]) * 100 for r in rows]
    fig, ax = plt.subplots(figsize=(6.4, 3.6), dpi=150)
    fig.patch.set_facecolor(SURFACE)
    style(ax)
    ax.set_yscale("log")
    plot_series(ax, xs, rows, "loss")
    ax.set_xlabel("UDP packet loss on every node (%), fixed 200 jobs/s", color=MUTED,
                  fontsize=9)
    ax.set_ylabel("submit → DONE latency (ms, log)", color=MUTED, fontsize=9)
    ax.set_title("End-to-end latency vs. packet loss — full throughput at every level",
                 color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, labelcolor=INK, fontsize=9, loc="upper left")
    ax.set_xlim(left=-0.5, right=max(xs) * 1.16)
    fig.tight_layout()
    fig.savefig(HERE / "loss_matrix.png", facecolor=SURFACE)


ramp()
loss()
print("wrote bench/ramp.png bench/loss_matrix.png")
