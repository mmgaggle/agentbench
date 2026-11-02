#!/bin/bash
# Idle cost of the real ceph-agent on the VMs (real systemd, podman, Ceph
# daemons): CPU time, memory and tasks over a window, per node.
#   vm_footprint.sh [seconds]
set -euo pipefail
win=${1:-600}
. /ab/vm/env.sh
snap() {
    for i in 1 2 3; do
        vm_ssh "$i" 'u=ceph-agent.service; pid=$(systemctl show -p MainPID --value $u);
            echo "node$(hostname | tr -dc 0-9) $(date +%s.%N) $(systemctl show -p CPUUsageNSec,MemoryCurrent,TasksCurrent --value $u | tr "\n" " ") rss_kb=$(awk "/VmRSS/{print \$2}" /proc/$pid/status) units=$(ls /etc/containers/systemd | wc -l) threads=$(ls /proc/$pid/task | wc -l)"'
    done
}
snap
sleep "$win"
snap
