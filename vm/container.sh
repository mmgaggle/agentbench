#!/bin/bash
# Creates the rootless podman container that hosts the agent test VMs
# (run on bravo, not inside a container). The VMs sit on a bridge inside
# the container's own network namespace; see net.sh.
set -euo pipefail

mkdir -p ~/vms
if ! podman container exists agentvm; then
    podman run -d --name agentvm --init \
        --security-opt label=disable \
        --device /dev/kvm --device /dev/net/tun \
        --cap-add NET_ADMIN,NET_RAW \
        --sysctl net.ipv4.ip_forward=1 \
        -v "$HOME/src/agentbench:/ab" -v "$HOME/vms:/vms" \
        registry.fedoraproject.org/fedora:44 sleep infinity
fi
podman exec agentvm dnf -y -q install \
    qemu-system-x86-core qemu-img xorriso dnsmasq nftables iproute \
    openssh-clients procps-ng python3 jq rsync curl openssl edk2-ovmf librados2 ceph-common ceph-base
