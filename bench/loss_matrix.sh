#!/usr/bin/env bash
# Throughput + p99 at five packet-loss levels (M10 README table). Fixed offered rate,
# fresh cluster per loss level, loss injected symmetrically on every node's UDP ingress
# (iptables statistic — module-free). One CSV row per level.
#
#   bench/loss_matrix.sh out.csv
set -u

cd "$(dirname "$0")/.."
BIN="${BIN:-build/release}"
CL=chaos/cluster.sh
OUT="${1:-bench/loss_matrix.csv}"
RATE="${RATE:-200}"
DURATION="${DURATION:-20}"
LOSSES=(0 0.01 0.05 0.10 0.20)

GWS="http://10.77.0.10:8080,http://10.77.0.11:8080,http://10.77.0.12:8080,http://10.77.0.13:8080,http://10.77.0.14:8080"

echo "loss,rate,offered,accepted,errors,completed,p50_ms,p95_ms,p99_ms" >"$OUT"
for loss in "${LOSSES[@]}"; do
    run=$(mktemp -d /tmp/tautq-loss.XXXXXX)
    bash $CL down >/dev/null 2>&1
    BIN="$BIN" bash $CL up "$run"
    sleep 3
    if [ "$loss" != "0" ]; then
        for i in 0 1 2 3 4; do bash $CL loss "$i" "$loss"; done
    fi
    echo "--- loss $loss @ $RATE/s for ${DURATION}s ---" >&2
    row=$("$BIN/tautq-loadgen" --gateways "$GWS" --sink-url http://10.77.0.1:8090/hook \
        --rate "$RATE" --duration-s "$DURATION" --prefix "l$loss")
    echo "$loss,$row" >>"$OUT"
    tail -1 "$OUT" >&2
    bash $CL down >/dev/null 2>&1
    sudo rm -rf "$run"
done
echo "wrote $OUT" >&2
