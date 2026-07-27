#!/usr/bin/env bash
# 5-node tautq cluster in network namespaces (sudo required). Each node lives in its own ns
# (tq0..tq4, 10.77.0.10+i) behind a bridge; the sink and the chaos driver run in the root
# ns at 10.77.0.1. Per-node namespaces are what make chaos surgical: partitions and loss
# are iptables rules INSIDE one node's ns, and never depend on netem being available.
#
#   cluster.sh up <run-dir>       start bridge, 5 nodes, 5 workers, sink
#   cluster.sh down               tear everything down
#   cluster.sh kill-node <i>      SIGKILL node i and its worker
#   cluster.sh start-node <i> <run-dir>   (re)start node i + worker over its data dir
#   cluster.sh partition <i,j,..> <k,l,..>   drop all UDP between the two groups
#   cluster.sh heal               remove every injected iptables rule
#   cluster.sh loss <i> <prob>    drop UDP into node i with probability <prob>
set -u

BIN="${BIN:-build/dev}"
BR=tqbr0
SUBNET=10.77.0
NODES=5

node_ip() { echo "$SUBNET.$((10 + $1))"; }

up() {
    local run="$1"
    mkdir -p "$run"
    sudo ip link add $BR type bridge 2>/dev/null
    sudo ip addr add $SUBNET.1/24 dev $BR 2>/dev/null
    sudo ip link set $BR up
    for i in $(seq 0 $((NODES - 1))); do
        local ip
        ip=$(node_ip "$i")
        sudo ip netns add "tq$i"
        sudo ip link add "tqv$i" type veth peer name "tqp$i"
        sudo ip link set "tqv$i" netns "tq$i"
        sudo ip link set "tqp$i" master $BR up
        sudo ip netns exec "tq$i" ip addr add "$ip/24" dev "tqv$i"
        sudo ip netns exec "tq$i" ip link set "tqv$i" up
        sudo ip netns exec "tq$i" ip link set lo up
    done
    "$BIN/tautq-sink" --port 8090 --out "$run/sink.log" 2>"$run/sink.err" &
    echo $! >"$run/sink.pid"
    for i in $(seq 0 $((NODES - 1))); do
        start_node "$i" "$run"
    done
}

start_node() {
    local i="$1" run="$2" ip peers=""
    ip=$(node_ip "$i")
    for j in $(seq 0 $((NODES - 1))); do
        [ "$j" != "$i" ] && peers="${peers:+$peers,}$(node_ip "$j"):9000"
    done
    sudo ip netns exec "tq$i" "$PWD/$BIN/tautq-node" \
        --listen "$ip:9000" --http-port 8080 --data-dir "$run/node$i" \
        --peers "$peers" --visibility-ms 8000 \
        >>"$run/node$i.out" 2>>"$run/node$i.err" &
    echo $! >"$run/node$i.pid"
    sudo ip netns exec "tq$i" "$PWD/$BIN/tautq-worker" \
        --node "http://$ip:8080" --worker-id "$((100 + i))" --poll-ms 50 \
        2>>"$run/worker$i.err" &
    echo $! >"$run/worker$i.pid"
}

kill_node() {
    # [b]racket trick: the pattern must not match this script's own sudo/pkill cmdline.
    local i="$1" run="$2"
    sudo pkill -KILL -f "tautq[-]node --listen $(node_ip "$i"):9000" 2>/dev/null
    sudo pkill -KILL -f "tautq[-]worker --node http://$(node_ip "$i"):8080" 2>/dev/null
    true
}

partition() {
    local left="$1" right="$2"
    IFS=',' read -ra L <<<"$left"
    IFS=',' read -ra R <<<"$right"
    for i in "${L[@]}"; do
        for j in "${R[@]}"; do
            sudo ip netns exec "tq$i" iptables -A INPUT -s "$(node_ip "$j")" -j DROP
            sudo ip netns exec "tq$j" iptables -A INPUT -s "$(node_ip "$i")" -j DROP
        done
    done
}

heal() {
    for i in $(seq 0 $((NODES - 1))); do
        sudo ip netns exec "tq$i" iptables -F INPUT 2>/dev/null
    done
}

loss() {
    local i="$1" prob="$2"
    sudo ip netns exec "tq$i" iptables -A INPUT -p udp -m statistic \
        --mode random --probability "$prob" -j DROP
}

down() {
    sudo pkill -KILL -f 'tautq[-]node --listen 10\.77\.' 2>/dev/null
    sudo pkill -KILL -f 'tautq[-]worker --node http://10\.77\.' 2>/dev/null
    pkill -KILL -f 'tautq[-]sink --port 8090' 2>/dev/null
    for i in $(seq 0 $((NODES - 1))); do
        sudo ip netns del "tq$i" 2>/dev/null
    done
    sudo ip link del $BR 2>/dev/null
    true
}

case "${1:-}" in
up) up "$2" ;;
down) down ;;
kill-node) kill_node "$2" "${3:-}" ;;
start-node) start_node "$2" "$3" ;;
partition) partition "$2" "$3" ;;
heal) heal ;;
loss) loss "$2" "$3" ;;
*) echo "usage: cluster.sh up|down|kill-node|start-node|partition|heal|loss"; exit 2 ;;
esac
