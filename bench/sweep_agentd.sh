#!/bin/bash
# B: the same scenarios against agentd, the Rust prototype.
#
#   B1  --persist sync --policy always --key rsa
#       same work per report as the mgr: three blocking config-key sets
#       before answering, same RSA-4096 endpoint cert
#   B2  --persist writeback --policy always --key rsa
#       same writes, decoupled from the request and issued in parallel
#   B3  --persist writeback --policy changed --key ec
#       only persist what the orchestrator acts on (+5 min checkpoint),
#       ECDSA P-256 endpoint cert
#   B0  --persist none   processing ceiling (no mon writes)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
run() { python3 "$here/run.py" "$@" 2>&1 | grep -v -E 'Deprecat|^\+ |experimental' ; }

declare -A V=(
    [B1]='--persist sync --policy always --key rsa'
    [B2]='--persist writeback --policy always --key rsa'
    [B3]='--persist writeback --policy changed --key ec'
    [B0]='--persist none --key rsa'
)

for n in 250 1000 2000 4000; do
    python3 "$AB/cluster/seed.py" --hosts "$n" 2>&1 | grep -v Deprecat
    sleep 10
    for v in B1 B2 B3; do
        [ "$v" = B2 ] && [ "$n" != 1000 ] && [ "$n" != 4000 ] && continue
        run --name "$v-agents-n$n" --agents "$n" --duration 180 --no-seed --agentd "${V[$v]}"
    done
    case $n in
        1000)
            for c in 8 64 256; do
                run --name "B1-sat-n$n-c$c" --agents "$n" --saturate "$c" --duration 60 --no-seed \
                    --prime 0 --agentd "${V[B1]}"
            done ;;
        4000)
            for v in B0 B1 B2 B3; do
                run --name "$v-sat-n$n-c256" --agents "$n" --saturate 256 --duration 60 --no-seed \
                    --prime 0 --agentd "${V[$v]}"
            done
            run --name "B3-sat-n$n-c1024" --agents "$n" --saturate 1024 --duration 60 --no-seed \
                --prime 0 --agentd "${V[B3]}"
            # Offered load far beyond the mgr: every agent reporting every 2s / 1s.
            for p in 2 1; do
                run --name "B3-agents-n$n-p$p" --agents "$n" --period "$p" --duration 120 --no-seed \
                    --warmup 10 --prime 0 --agentd "${V[B3]}"
            done ;;
    esac
done
pkill -x agentd
echo "sweep done"
