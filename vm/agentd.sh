#!/bin/bash
# (Re)starts agentd for the VMs inside agentvm: agent endpoint on the VM
# bridge (10.77.0.1:7160, TLS), admin on 10.77.0.1:7161. No cluster: hosts
# and keyrings come from /vms/cluster/hosts.json, state stays in memory.
set -euo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
if [ -f "$C/agentd.pid" ] && kill -0 "$(cat "$C/agentd.pid")" 2>/dev/null; then
    kill "$(cat "$C/agentd.pid")"
    sleep 0.5
fi
setsid /ab/rust/target/release/agentd --hosts-file "$C/hosts.json" --persist none --key ec \
    --cert-dir "$C/tls" --listen "$GATEWAY:7160" --admin "$GATEWAY:7161" --swim-bind "$GATEWAY:7946" ${AGENTD_ARGS:-} \
    >"$C/agentd.log" 2>&1 </dev/null &
echo $! >"$C/agentd.pid"
for _ in $(seq 50); do
    curl -sf "http://$GATEWAY:7161/stats" >/dev/null && break
    sleep 0.1
done
head -4 "$C/agentd.log"
# the mgr re-teaches agentd which hosts joined
[ -f "$C/mgr.json" ] && /ab/rust/target/release/intentctl members --admin "$GATEWAY:7161" --state "$C/mgr.json"
true
