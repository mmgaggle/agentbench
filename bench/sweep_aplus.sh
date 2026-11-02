#!/bin/bash
# A+: the stock mgr endpoint with cluster/patch_aplus.py applied (per-host
# failed-daemon change detection instead of three cluster-wide rescans).
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
run() { python3 "$here/run.py" "$@" 2>&1 | grep -v -E 'Deprecat|^\+ |experimental' ; }

pkill -x agentd
python3 "$AB/cluster/patch_aplus.py" apply || exit 1
for n in 1000 2000 4000; do
    run --name "Aplus-agents-n$n" --agents "$n" --duration 120 --prime 0 --no-probes
    run --name "Aplus-sat-n$n" --agents "$n" --saturate 8 --duration 45 --no-seed --prime 0 --no-probes
    run --name "AplusP-agents-n$n" --agents "$n" --duration 120 --no-seed --prime 0
done
python3 "$AB/cluster/patch_aplus.py" revert
echo "sweep done"
