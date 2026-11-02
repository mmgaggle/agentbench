#!/bin/bash
# Attribute ceph-mgr GIL time: py-spy samples only the thread holding the GIL
# (--gil), so each profile is how the interpreter spends its time. Profiles
# 30s with no agent traffic, then 60s while every agent reports each 20s.
#
#   diag_pyspy.sh <hosts> <name> [seed]
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
n=$1
name=$2
out=$AB/results/$name
mkdir -p "$out"
if [ "${3:-}" = seed ]; then
    python3 "$AB/cluster/seed.py" --hosts "$n" 2>&1 | grep -v Deprecat
    sleep 10
fi
spy() {
    taskset -c 8-15,24-31 py-spy record --gil --rate 100 --duration "$1" --format raw \
        --pid "$(pgrep -x ceph-mgr)" --output "$2" > "$2.log" 2>&1 || true
}
spy 30 "$out/pyspy-gil-idle.txt"
taskset -c 8-15,24-31 "$AB/rust/target/release/agentsim" --agents "$n" --period 20 --duration 90 \
    --warmup 20 --listen --out "$out" --label "$name" > "$out/agentsim.log" 2>&1 &
sim=$!
sleep 25
spy 60 "$out/pyspy-gil.txt"
wait $sim
for f in idle ""; do
    in=$out/pyspy-gil${f:+-$f}.txt
    python3 "$here/pyspy_top.py" "$in" > "$out/pyspy-top${f:+-$f}.txt"
done
head -45 "$out/pyspy-top-idle.txt"
echo ======
head -70 "$out/pyspy-top.txt"
