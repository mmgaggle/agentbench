#!/bin/bash
# v1 vs v2 agent protocol against agentd, without a cluster: agentd keeps
# state in memory (--persist none, ECDSA cert) so the comparison is the
# ingestion path alone.
#
#   V-v1-n<N>  today's agent: a full ~50 KB report on a new TLS connection
#              every 20 s
#   V-v2-n<N>  persistent connection; deltas on change (0.01 changes per
#              host per period), usage every 15 periods, heartbeats
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
SIM=$AB/rust/target/release/agentsim
AGENTD=$AB/rust/target/release/agentd

for n in ${SIZES:-4000 10000 20000 50000}; do
    hosts=/mon/agents-$n.json
    [ -f "$hosts" ] || python3 "$AB/cluster/gen_hosts.py" --hosts "$n" --out "$hosts"
    for proto in v1 v2; do
        name=V-$proto-n$n
        out=$AB/results/$name
        rm -rf "$out"
        mkdir -p "$out"
        warmup=20
        extra=()
        if [ "$proto" = v2 ]; then
            warmup=40
            extra=(--change-rate 0.01 --usage-every 15)
        fi
        pkill -x agentd
        sleep 1
        taskset -c 0-7,16-23 "$AGENTD" --hosts-file "$hosts" --persist none --key ec > "$out/agentd.log" 2>&1 &
        until curl -sf 127.0.0.1:7161/stats > /dev/null; do sleep 0.5; done
        taskset -c 8-15,24-31 python3 "$here/sample.py" --out "$out" &
        sampler=$!
        ( sleep "$warmup"; curl -s 127.0.0.1:7161/stats > "$out/agentd-start.json" ) &
        taskset -c 8-15,24-31 "$SIM" --proto "$proto" --hosts-file "$hosts" --period 20 --duration 120 \
            --warmup "$warmup" --threads 16 --out "$out" --label "$name" "${extra[@]}" > "$out/agentsim.log" 2>&1
        curl -s 127.0.0.1:7161/stats > "$out/agentd-end.json"
        python3 -c "
import json
s = {'start': json.load(open('$out/agentd-start.json')), 'end': json.load(open('$out/agentd-end.json'))}
json.dump(s, open('$out/agentd-stats.json', 'w'), indent=1)"
        kill "$sampler"
        wait "$sampler" 2>/dev/null
        echo "$name: $(tail -1 "$out/agentsim.log")"
        python3 -c "import json,time; json.dump({'args': {'agents': $n, 'period': 20, 'duration': 120, 'warmup': $warmup, 'proto': '$proto'}, 'end': time.time()}, open('$out/meta.json', 'w'))"
    done
done
pkill -x agentd
echo "sweep done"
