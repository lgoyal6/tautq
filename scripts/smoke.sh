#!/usr/bin/env bash
# 3-node loopback smoke: start cluster + sink + worker, submit N jobs through different
# gateways, and assert every job reaches Done and every idempotency key hit the sink
# exactly once. The cheapest "it actually runs" check; chaos lives in chaos/.
set -u

BIN="${BIN:-build/dev}"
RUN="$(mktemp -d /tmp/tautq-smoke.XXXXXX)"
JOBS="${JOBS:-10}"
pids=()

cleanup() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
    wait 2>/dev/null
    rm -rf "$RUN"
}
trap cleanup EXIT

"$BIN/tautq-sink" --port 8090 --out "$RUN/sink.log" 2>"$RUN/sink.err" &
pids+=($!)

for i in 0 1 2; do
    data_port=$((9100 + i * 10))
    http_port=$((8080 + i))
    peers=""
    for j in 0 1 2; do
        if [ "$j" != "$i" ]; then
            peers="${peers:+$peers,}127.0.0.1:$((9100 + j * 10))"
        fi
    done
    "$BIN/tautq-node" --listen "127.0.0.1:$data_port" --http-port "$http_port" \
        --data-dir "$RUN/node$i" --peers "$peers" --visibility-ms 5000 \
        2>"$RUN/node$i.err" &
    pids+=($!)
done

sleep 1
for i in 0 1 2; do
    if ! curl -fsS "http://127.0.0.1:$((8080 + i))/healthz" >/dev/null; then
        echo "FAIL: node $i not healthy"
        exit 1
    fi
done

"$BIN/tautq-worker" --node http://127.0.0.1:8080 --worker-id 1 --poll-ms 50 2>"$RUN/w1.err" &
pids+=($!)
"$BIN/tautq-worker" --node http://127.0.0.1:8081 --worker-id 2 --poll-ms 50 2>"$RUN/w2.err" &
pids+=($!)

ids=()
for n in $(seq 1 "$JOBS"); do
    gw=$((8080 + n % 3))
    resp=$(curl -fsS -X POST \
        "http://127.0.0.1:$gw/v1/jobs?key=smoke-$n&url=http://127.0.0.1:8090/hook" \
        --data-binary "payload-$n") || { echo "FAIL: submit $n"; exit 1; }
    id=$(echo "$resp" | sed -n 's/.*"id":"\([0-9a-f]*\)".*/\1/p')
    ids+=("$id")
done
echo "submitted $JOBS jobs"

deadline=$((SECONDS + 30))
done_count=0
while [ "$SECONDS" -lt "$deadline" ]; do
    done_count=0
    for i in "${!ids[@]}"; do
        gw=$((8080 + (i + 1) % 3))
        state=$(curl -fsS "http://127.0.0.1:$gw/v1/jobs/${ids[$i]}" 2>/dev/null |
            sed -n 's/.*"state":"\([a-z_]*\)".*/\1/p')
        if [ "$state" = "done" ]; then
            done_count=$((done_count + 1))
        fi
    done
    if [ "$done_count" -eq "$JOBS" ]; then
        break
    fi
    sleep 0.5
done
echo "done: $done_count/$JOBS"

recv=$(wc -l <"$RUN/sink.log")
uniq_keys=$(awk '{print $2}' "$RUN/sink.log" | sort -u | wc -l)
echo "sink received: $recv lines, $uniq_keys unique keys"
curl -fsS http://127.0.0.1:8080/metrics | grep -E "tautq_(submits|completions)" | head -4

if [ "$done_count" -eq "$JOBS" ] && [ "$uniq_keys" -eq "$JOBS" ]; then
    echo "SMOKE PASS"
    exit 0
fi
echo "SMOKE FAIL"
tail -5 "$RUN"/node*.err "$RUN"/w*.err
exit 1
