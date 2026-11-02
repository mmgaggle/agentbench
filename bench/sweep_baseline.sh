#!/bin/bash
# Baseline (A): the cephadm mgr module's agent endpoint.
#
#   A-agents-n<N>   N hosts, every agent reporting each 20s (offered N/20 msg/s).
#                   The host cache is seeded with every daemon the agents
#                   report, as after a mgr failover, so no priming is needed.
#   A-sat-n<N>      peak throughput at N hosts, 8 concurrent senders
#   A-conc-n250-c<C>  concurrency sweep at 250 hosts
#   A-idle-n<N>     probes with no agent traffic (responsiveness reference)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
tag=${TAG:-A}
run() { python3 "$here/run.py" "$@" 2>&1 | grep -v -E 'Deprecat|^\+ |experimental' ; }

for n in 250 500 750 1000 2000 4000; do
    dur=180
    [ "$n" -ge 2000 ] && dur=120
    run --name "$tag-agents-n$n" --agents "$n" --duration "$dur" --prime 0
    run --name "$tag-sat-n$n" --agents "$n" --saturate 8 --duration 60 --no-seed --prime 0
    case $n in
        250)
            for c in 1 2 4 16 32 64; do
                run --name "$tag-conc-n250-c$c" --agents 250 --saturate "$c" --duration 40 --no-seed \
                    --prime 0 --no-probes
            done ;;
        1000|4000)
            run --name "$tag-idle-n$n" --agents "$n" --idle --duration 60 --warmup 5 --no-seed ;;
    esac
done
echo "sweep done"
