#!/usr/bin/env bash
# Load ramp to the p99 knee (M10): bring up the 5-node netns cluster, then run the
# open-loop generator at increasing rates, one CSV row per step. The knee is where p99
# departs its flat region while accepted/offered stays ~1 — published in the README from
# the committed CSV, never from prose memory.
#
#   bench/ramp.sh out.csv [rates...]
set -u

cd "$(dirname "$0")/.."
BIN="${BIN:-build/release}"
CL=chaos/cluster.sh
OUT="${1:-bench/ramp.csv}"
shift 2>/dev/null || true
RATES=("${@:-50 100 200 400 800 1200 1600 2000}")
DURATION="${DURATION:-20}"

GWS="http://10.77.0.10:8080,http://10.77.0.11:8080,http://10.77.0.12:8080,http://10.77.0.13:8080,http://10.77.0.14:8080"

run=$(mktemp -d /tmp/tautq-ramp.XXXXXX)
bash $CL down >/dev/null 2>&1
BIN="$BIN" bash $CL up "$run"
sleep 3

echo "rate,offered,accepted,errors,completed,p50_ms,p95_ms,p99_ms" >"$OUT"
for rate in ${RATES[@]}; do
    echo "--- rate $rate/s for ${DURATION}s ---" >&2
    "$BIN/tautq-loadgen" --gateways "$GWS" --sink-url http://10.77.0.1:8090/hook \
        --rate "$rate" --duration-s "$DURATION" --prefix "r$rate" >>"$OUT"
    tail -1 "$OUT" >&2
    sleep 3
done

bash $CL down >/dev/null 2>&1
sudo rm -rf "$run"
echo "wrote $OUT" >&2
