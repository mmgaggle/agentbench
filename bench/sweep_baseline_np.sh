#!/bin/bash
# Baseline (A) without probes: agent capacity of the mgr endpoint alone.
# probe.py's `orch ps --hostname` at 1 Hz costs ~0.9 s of GIL per call at
# 1000 hosts, so the probed runs measure agents + operator polling.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
run() { python3 "$here/run.py" "$@" 2>&1 | grep -v -E 'Deprecat|^\+ |experimental' ; }

for n in 500 750 1000 1500 2000; do
    run --name "Anp-agents-n$n" --agents "$n" --duration 120 --prime 0 --no-probes
    run --name "Anp-sat-n$n" --agents "$n" --saturate 8 --duration 45 --no-seed --prime 0 --no-probes
done
echo "sweep done"
