#!/bin/bash
# Shuts down node1..nodeN (or the given node numbers): ACPI powerdown, then
# kill after 60 s.
set -euo pipefail
. "$(dirname "$0")/env.sh"

nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || mapfile -t nodes < <(seq 1 "$NODES")
for i in "${nodes[@]}"; do
    d=$(vm_dir "$i")
    [ -f "$d/pid" ] && kill -0 "$(cat "$d/pid")" 2>/dev/null || continue
    python3 -c '
import socket, sys
s = socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); s.sendall(b"system_powerdown\n")
' "$d/monitor.sock" || true
done
for i in "${nodes[@]}"; do
    d=$(vm_dir "$i")
    [ -f "$d/pid" ] || continue
    pid=$(cat "$d/pid")
    for _ in $(seq 60); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -0 "$pid" 2>/dev/null && kill "$pid"
    rm -f "$d/pid"
    echo "node$i stopped"
done
