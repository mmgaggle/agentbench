#!/bin/bash
# Copies the current ceph-agent binary to node1..N (or the given nodes) and
# restarts the agent; identity and state are kept (inside agentvm).
set -euo pipefail
. "$(dirname "$0")/env.sh"
nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || mapfile -t nodes < <(seq 1 "$NODES")
scpo=(-i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
for i in "${nodes[@]}"; do
    scp -q "${scpo[@]}" /ab/rust/target/release/ceph-agent "root@$(vm_ip "$i"):/usr/local/bin/ceph-agent.new"
    vm_ssh "$i" "mv /usr/local/bin/ceph-agent.new /usr/local/bin/ceph-agent && systemctl restart ceph-agent && systemctl is-active ceph-agent"
done
