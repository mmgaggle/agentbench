#!/bin/bash
# Joins node1..N (or the given nodes) with fresh identities (inside agentvm):
# issue a token per host (the mgr), copy the agent binary and unit, run
# `ceph-agent join <token>` on the host while the mgr approves, then start
# the agent. Wipes the host's previous agent identity and state first.
set -euo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
ICTL=/ab/rust/target/release/intentctl
nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || mapfile -t nodes < <(seq 1 "$NODES")
scpo=(-i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
pids=()
for i in "${nodes[@]}"; do
    ip=$(vm_ip "$i")
    token=$($ICTL token --state "$C/mgr.json" --host "node$i" --agentd "$GATEWAY:7160" \
        --server-name agentd.lan --ca "$C/tls/ca.pem")
    vm_ssh "$i" "systemctl stop ceph-agent 2>/dev/null; rm -rf /etc/ceph-agent /var/lib/ceph-agent"
    scp -q "${scpo[@]}" /ab/rust/target/release/ceph-agent "root@$ip:/usr/local/bin/ceph-agent"
    scp -q "${scpo[@]}" /ab/vm/ceph-agent.service "root@$ip:/etc/systemd/system/ceph-agent.service"
    vm_ssh "$i" "ceph-agent join '$token'" &
    pids+=($!)
done
$ICTL approve --admin "$GATEWAY:7161" --state "$C/mgr.json" --ca-cert "$C/tls/ca.pem" \
    --ca-key "$C/tls/ca.key" --intent-key "$C/intent.key" --swim-seed "$GATEWAY:7946" \
    --expect "${#nodes[@]}" --timeout 120
for p in "${pids[@]}"; do wait "$p"; done
for i in "${nodes[@]}"; do
    vm_ssh "$i" "systemctl daemon-reload && systemctl enable -q ceph-agent && systemctl restart ceph-agent && systemctl is-active ceph-agent"
done
