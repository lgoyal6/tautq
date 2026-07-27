#!/usr/bin/env bash
# The chaos matrix (DESIGN-protocol §5). Each scenario: fresh 5-node cluster, a stream of
# submits through rotating gateways WHILE the fault is live, quiesce, then tautq-verify
# proves no accepted job was lost, no job completed twice, and every duplicate delivery is
# attributable to an expired lease or an ownership change.
#
#   chaos.sh [kill|partition|loss|stale|all]
set -u

cd "$(dirname "$0")/.."
BIN="${BIN:-build/dev}"
CL=chaos/cluster.sh
JOBS="${JOBS:-60}"

submit_stream() { # <run> <start> <count> [sleep]
    local run="$1" start="$2" count="$3" pause="${4:-0.1}" n gw resp code id
    for n in $(seq "$start" $((start + count - 1))); do
        gw="10.77.0.$((10 + n % 5))"
        resp=$(curl -sS -m 5 -w '\n%{http_code}' -X POST \
            "http://$gw:8080/v1/jobs?key=$SCEN-$n&url=http://10.77.0.1:8090/hook" \
            --data-binary "payload-$n" 2>/dev/null)
        code=$(echo "$resp" | tail -1)
        id=$(echo "$resp" | head -1 | sed -n 's/.*"id":"\([0-9a-f]*\)".*/\1/p')
        echo "$SCEN-$n,$id,$code" >>"$run/submits.csv"
        sleep "$pause"
    done
}

wait_quiesce() { # <run> <seconds>
    local run="$1" deadline=$((SECONDS + $2)) want got
    want=$(awk -F, '$3=="200"||$3=="201"' "$run/submits.csv" | wc -l)
    while [ "$SECONDS" -lt "$deadline" ]; do
        got=$(awk '{print $2}' "$run/sink.log" 2>/dev/null | sort -u | wc -l)
        [ "$got" -ge "$want" ] && break
        sleep 1
    done
}

verify() { # <run> [extra flags]
    local run="$1"
    shift
    local logs=""
    for i in 0 1 2 3 4; do logs="${logs:+$logs,}$run/node$i"; done
    # sudo: node WALs are root-owned (nodes run inside sudo'd namespaces) and replay
    # truncates torn tails, which needs write access.
    sudo "$BIN/tautq-verify" --submits "$run/submits.csv" --sink "$run/sink.log" \
        --logs "$logs" "$@"
}

scenario() { # <name> <body-fn>
    SCEN="$1"
    local run
    run=$(mktemp -d "/tmp/tautq-chaos-$SCEN.XXXXXX")
    echo "=== scenario: $SCEN (run dir $run) ==="
    bash $CL down >/dev/null 2>&1
    bash $CL up "$run"
    sleep 3 # SWIM convergence
    "$2" "$run"
    sleep 1
    bash $CL down >/dev/null 2>&1
    if verify "$run" ${VERIFY_FLAGS:-}; then
        echo "=== $SCEN: PASS ==="
        sudo rm -rf "$run"
        return 0
    fi
    echo "=== $SCEN: FAIL (evidence kept in $run) ==="
    return 1
}

# (a) SIGKILL a node mid-stream: its owned jobs fail over, its worker's in-flight lease is
# inherited, the restarted node resyncs. Nothing lost, nothing completed twice.
body_kill() {
    local run="$1"
    submit_stream "$run" 1 $((JOBS / 3)) 0.05
    submit_stream "$run" 1001 $((JOBS / 3)) 0.05 &
    local bg=$!
    sleep 1
    bash $CL kill-node 2 "$run"
    wait $bg
    sleep 12 # SWIM detects, takeovers land, leases expire and retry
    bash $CL start-node 2 "$run"
    submit_stream "$run" 2001 $((JOBS / 3)) 0.05
    wait_quiesce "$run" 60
}

# (b) partition a 2-node minority: majority side keeps working, minority stalls (CP),
# after heal everything reconciles.
body_partition() {
    local run="$1"
    submit_stream "$run" 1 $((JOBS / 2)) 0.05
    bash $CL partition "3,4" "0,1,2"
    submit_stream "$run" 1001 $((JOBS / 2)) 0.1 || true
    sleep 12
    bash $CL heal
    wait_quiesce "$run" 90
}

# (c) 10% UDP loss on every node while a full job stream runs — taut's job, surfaced.
body_loss() {
    local run="$1"
    for i in 0 1 2 3 4; do bash $CL loss "$i" 0.10; done
    submit_stream "$run" 1 "$JOBS" 0.05
    wait_quiesce "$run" 90
    bash $CL heal
}

# (d) stale-log restart: kill a node, chop the tail off its WAL (records it acked are gone
# — the worst kind of restart), bring it back. Resync must repair it; nothing lost.
body_stale() {
    local run="$1"
    submit_stream "$run" 1 $((JOBS / 2)) 0.05
    bash $CL kill-node 1 "$run"
    local seg
    seg=$(ls -1 "$run/node1/" | sort | tail -1)
    local size
    size=$(stat -c %s "$run/node1/$seg")
    if [ "$size" -gt 400 ]; then
        sudo truncate -s $((size - 400)) "$run/node1/$seg"
        echo "truncated $seg by 400 bytes (was $size)"
    fi
    sleep 10
    bash $CL start-node 1 "$run"
    submit_stream "$run" 1001 $((JOBS / 2)) 0.05
    wait_quiesce "$run" 60
}

fails=0
case "${1:-all}" in
kill) scenario kill body_kill || fails=1 ;;
partition) scenario partition body_partition || fails=1 ;;
loss) scenario loss body_loss || fails=1 ;;
stale) scenario stale body_stale || fails=1 ;;
all)
    scenario kill body_kill || fails=$((fails + 1))
    scenario partition body_partition || fails=$((fails + 1))
    scenario loss body_loss || fails=$((fails + 1))
    scenario stale body_stale || fails=$((fails + 1))
    ;;
*) echo "usage: chaos.sh [kill|partition|loss|stale|all]"; exit 2 ;;
esac
echo "chaos suite: $fails failure(s)"
exit "$fails"
