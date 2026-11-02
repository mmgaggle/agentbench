#!/bin/bash
# SWIM membership at scale: agentd runs the controller member (--swim),
# swimsim runs N agent members. No HTTP agent traffic, so agentd's CPU here
# is the cost of membership alone.
#
#   S-swim-n<N>  join, steady-state traffic, 3 signal broadcasts, 1% of
#                members killed at once (at least 10)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
AGENTD=$AB/rust/target/release/agentd
SWIMSIM=$AB/rust/target/release/swimsim

for n in ${SIZES:-1000 4000 10000}; do
    hosts=/mon/agents-$n.json
    [ -f "$hosts" ] || python3 "$AB/cluster/gen_hosts.py" --hosts "$n" --out "$hosts"
    name=S-swim-n$n
    out=$AB/results/$name
    rm -rf "$out"
    mkdir -p "$out"
    kill=$(( n / 100 ))
    [ "$kill" -lt 10 ] && kill=10
    pkill -x agentd
    sleep 1
    taskset -c 0-7,16-23 "$AGENTD" --hosts-file "$hosts" --persist none --key ec --swim > "$out/agentd.log" 2>&1 &
    until curl -sf 127.0.0.1:7161/stats > /dev/null; do sleep 0.5; done
    taskset -c 8-15,24-31 python3 "$here/sample.py" --out "$out" &
    sampler=$!
    taskset -c 8-15,24-31 "$SWIMSIM" --agents "$n" --kill "$kill" --out "$out" > "$out/swimsim.log" 2>&1
    curl -s 127.0.0.1:7161/stats > "$out/agentd-stats.json"
    kill "$sampler"
    wait "$sampler" 2>/dev/null
    echo "$name: $(grep -E '^(steady|signal|detection)' "$out/swimsim.log" | tr '\n' ' ')"
done
pkill -x agentd
echo "sweep done"
