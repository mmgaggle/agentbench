#!/bin/bash
# Creates VM disks and a cloud-init seed for node1..nodeN (inside agentvm).
#   mkvm.sh            all nodes
#   mkvm.sh 2          node2 only (recreates its disks)
set -euo pipefail
. "$(dirname "$0")/env.sh"

mkdir -p "$VMS/base"
if [ ! -f "$VMS/base/$BASE_IMG" ]; then
    curl -fsSL -o "$VMS/base/$BASE_SUM" "$BASE_URL/$BASE_SUM"
    curl -fL -o "$VMS/base/$BASE_IMG.part" "$BASE_URL/$BASE_IMG"
    mv "$VMS/base/$BASE_IMG.part" "$VMS/base/$BASE_IMG"
    (cd "$VMS/base" && grep "$BASE_IMG" "$BASE_SUM" | sha256sum -c -)
fi
[ -f "$SSH_KEY" ] || ssh-keygen -q -t ed25519 -N '' -f "$SSH_KEY"

mk() {
    local i=$1 d
    d=$(vm_dir "$i")
    rm -rf "$d"
    mkdir -p "$d/seed"
    qemu-img create -q -f qcow2 -F qcow2 -b "$VMS/base/$BASE_IMG" "$d/root.qcow2" 20G
    for k in 1 2; do
        qemu-img create -q -f qcow2 "$d/data$k.qcow2" 10G
    done
    cat >"$d/seed/meta-data" <<EOF
instance-id: node$i-$(date +%s)
local-hostname: node$i
EOF
    cat >"$d/seed/user-data" <<EOF
#cloud-config
hostname: node$i
fqdn: node$i.lan
disable_root: false
ssh_pwauth: false
ssh_authorized_keys:
  - $(cat "$SSH_KEY.pub")
packages: [podman, lvm2, chrony, jq, strace]
runcmd:
  - [systemctl, enable, --now, chronyd]
  - [setenforce, "0"]
  - [sed, -i, "s/^SELINUX=enforcing/SELINUX=permissive/", /etc/selinux/config]
EOF
    xorriso -as genisoimage -quiet -output "$d/seed.iso" -volid cidata -joliet -rock \
        "$d/seed/user-data" "$d/seed/meta-data"
    echo "node$i: $(vm_ip "$i") $(vm_mac "$i")"
}

if [ $# -gt 0 ]; then
    mk "$1"
else
    for i in $(seq 1 "$NODES"); do mk "$i"; done
fi
