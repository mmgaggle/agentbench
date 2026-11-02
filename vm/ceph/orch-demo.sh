#!/bin/bash
# The agentd orchestrator module on the VM cluster (inside agentvm):
# enable it, then orch host ls / ps, a restart through a signed intent, and
# health checks from agent state and SWIM liveness.
set -uo pipefail
. /ab/vm/env.sh
D=/vms/cluster/ceph
ceph() { command ceph -c $D/ceph.conf -k $D/admin.keyring "$@"; }
FSID=$(jq -r .fsid /vms/cluster/hosts.json)

ceph config-key set mgr/agentd/intent_key "$(cat /vms/cluster/intent.key)" >/dev/null
ceph mgr module enable agentd --force
ceph orch set backend agentd
for _ in $(seq 30); do ceph orch status 2>/dev/null | grep -q agentd && break; sleep 1; done
sleep 2
echo "== ceph orch status"; ceph orch status
echo "== ceph orch host ls"; ceph orch host ls
echo "== ceph orch ps"; ceph orch ps

echo "== ceph orch daemon restart osd.4 (a re-signed intent with a new restart key)"
before=$(vm_ssh 1 systemctl show -P MainPID ceph-$FSID-osd.4.service)
t0=$(date +%s%N)
ceph orch daemon restart osd.4
until [ "$(vm_ssh 1 systemctl show -P MainPID ceph-$FSID-osd.4.service)" != "$before" ] && \
      [ "$(vm_ssh 1 systemctl show -P ActiveState ceph-$FSID-osd.4.service)" = active ]; do sleep 0.2; done
echo "osd.4 MainPID $before -> $(vm_ssh 1 systemctl show -P MainPID ceph-$FSID-osd.4.service) after $(( ($(date +%s%N) - t0) / 1000000 )) ms"
sleep 2
ceph orch ps --daemon-type osd --refresh

echo "== freeze node3's agent: SWIM declares it down, the module raises AGENTD_HOST_DOWN"
vm_ssh 3 systemctl kill -s SIGSTOP ceph-agent
t0=$(date +%s%N)
until ceph health detail | grep -q AGENTD_HOST_DOWN; do sleep 0.5; done
echo "warning after $(( ($(date +%s%N) - t0) / 1000000 )) ms:"; ceph health detail | grep -A2 AGENTD_HOST_DOWN
ceph orch host ls
vm_ssh 3 systemctl kill -s SIGCONT ceph-agent
t0=$(date +%s%N)
until ! ceph health detail | grep -q AGENTD_HOST_DOWN; do sleep 0.5; done
echo "cleared $(( ($(date +%s%N) - t0) / 1000000 )) ms after the thaw"

echo "== a daemon that won't stay up: osd.0's container killed 6 times; systemd gives up"
for _ in 1 2 3 4 5 6; do
    vm_ssh 3 "podman kill -s KILL ceph-$FSID-osd.0 >/dev/null 2>&1"
    sleep 11
done
until ceph health detail | grep -q AGENTD_FAILED_DAEMON; do sleep 1; done
ceph health detail | grep -A2 AGENTD_FAILED_DAEMON
echo "   ... and ceph orch daemon restart brings it back (new restart key resets systemd's start limit)"
ceph orch daemon restart osd.0
until ! ceph health detail | grep -q AGENTD_FAILED_DAEMON; do sleep 1; done
ceph orch ps --daemon-type osd | grep -E "NAME|osd.0"
echo "== ceph agentd stats"; ceph agentd stats
ceph -s | grep -E "health|osd:"
