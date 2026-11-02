#!/bin/bash
# Installs (or upgrades) ceph-agent on node1..N or the given nodes, from
# /ab/rust/target/release and /vms/cluster (inside agentvm).
set -euo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || mapfile -t nodes < <(seq 1 "$NODES")
for i in "${nodes[@]}"; do
    ip=$(vm_ip "$i")
    scpo=(-i "$SSH_KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)
    vm_ssh "$i" "mkdir -p /etc/ceph-agent && chmod 700 /etc/ceph-agent"
    scp -q "${scpo[@]}" /ab/rust/target/release/ceph-agent "root@$ip:/usr/local/bin/ceph-agent.new"
    scp -q "${scpo[@]}" "$C/tls/ca.pem" "$C/intent.key.pub" "$C/node$i/agent.json" "$C/node$i/keyring" \
        "root@$ip:/etc/ceph-agent/"
    scp -q "${scpo[@]}" /ab/vm/ceph-agent.service "root@$ip:/etc/systemd/system/ceph-agent.service"
    vm_ssh "$i" "chmod 600 /etc/ceph-agent/keyring && mv /usr/local/bin/ceph-agent.new /usr/local/bin/ceph-agent \
        && systemctl daemon-reload && systemctl enable -q ceph-agent && systemctl restart ceph-agent \
        && systemctl is-active ceph-agent"
    echo "node$i: agent installed"
done
