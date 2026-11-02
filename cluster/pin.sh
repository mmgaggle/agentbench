#!/bin/bash
# Pin the cluster daemons (and agentd, if running) to CCX0. The mgr keeps
# its affinity across `mgr fail` because it respawns with execv.
for p in $(pgrep -f 'ceph-(mon|mgr|osd)|agentd'); do
    taskset -a -p -c "${CLUSTER_CPUS:-0-7,16-23}" "$p" >/dev/null
done
