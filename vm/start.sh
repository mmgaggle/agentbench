#!/bin/bash
# Boots node1..nodeN (or the given node numbers) inside agentvm.
set -euo pipefail
. "$(dirname "$0")/env.sh"

start() {
    local i=$1 d
    d=$(vm_dir "$i")
    if [ -f "$d/pid" ] && kill -0 "$(cat "$d/pid")" 2>/dev/null; then
        echo "node$i already running"
        return
    fi
    # UEFI: SeaBIOS on this qemu build never tries the virtio disks
    [ -f "$d/OVMF_VARS.fd" ] || cp /usr/share/edk2/ovmf/OVMF_VARS.fd "$d/OVMF_VARS.fd"
    taskset -c "$VM_CPUSET" qemu-system-x86_64 \
        -name "node$i" -machine q35,accel=kvm -cpu host \
        -smp "$VM_CPUS" -m "$VM_MEM" \
        -drive if=pflash,format=raw,readonly=on,file=/usr/share/edk2/ovmf/OVMF_CODE.fd \
        -drive "if=pflash,format=raw,file=$d/OVMF_VARS.fd" \
        -drive "file=$d/root.qcow2,if=none,id=root,format=qcow2,cache=unsafe" \
        -device virtio-blk-pci,drive=root,bootindex=0 \
        -drive "file=$d/data1.qcow2,if=none,id=data1,format=qcow2" \
        -device virtio-blk-pci,drive=data1,serial=data1 \
        -drive "file=$d/data2.qcow2,if=none,id=data2,format=qcow2" \
        -device virtio-blk-pci,drive=data2,serial=data2 \
        -drive "file=$d/seed.iso,if=virtio,format=raw,readonly=on" \
        -netdev "tap,id=n0,ifname=tap$i,script=no,downscript=no" \
        -device "virtio-net-pci,netdev=n0,mac=$(vm_mac "$i")" \
        -device virtio-rng-pci \
        -display none -serial "file:$d/console.log" \
        -monitor "unix:$d/monitor.sock,server,nowait" \
        -pidfile "$d/pid" -daemonize
    echo "node$i started ($(vm_ip "$i"))"
}

nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || mapfile -t nodes < <(seq 1 "$NODES")
for i in "${nodes[@]}"; do start "$i"; done
