#!/bin/bash
# What melted SWIM down when 5,000 agents upgraded at once
# (results/scale/n5000-freeze-upgrades)? The same phases again, with one
# change each:
#   64k    a 64 KB agent binary instead of 8 MB: no burst of hashing and
#          TLS reads when every agent fetches it at once
#   swim2  two SWIM threads per simulator process instead of one (each
#          runs ~420 members' probes, acks and gossip)
#   keepid agents keep their SWIM identity across the restart, so the
#          upgrade renames no one (the others rename every member, as
#          agents did when the original run was made)
#   rename the original run again (every member renamed), with the
#          thread and rename counters the original lacked
#   keepview  agents keep their identity and restore the membership they
#          had (the others start empty and relearn the whole cluster)
#   lifeguard  the original run's agents (renamed, empty membership), with
#          Lifeguard on every member
#   both   keepview with Lifeguard
# All but keepview and both start members empty, as agents did for the
# original run (SWIM_VIEW=0).
# Each is a full run.sh, one after the other (run.sh stops the previous
# run's processes when it starts).
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd)
L=/ab/results/scale
export PHASES='deploy agent-upgrade-paced-freeze agent-upgrade-all-freeze' UPGRADE_RATE=20 SETTLE_AFTER_UPGRADE=300
for v in ${VARIANTS:-64k swim2}; do
    case $v in
    64k) env SWIM_RENAME=1 SWIM_VIEW=0 AGENT_SIZE=65536 TAG=freeze-upgrades-64k "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-64k.log" 2>&1 ;;
    swim2) env SWIM_RENAME=1 SWIM_VIEW=0 SWIM_THREADS=2 TAG=freeze-upgrades-swim2 "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-swim2.log" 2>&1 ;;
    keepid) env SWIM_RENAME=0 SWIM_VIEW=0 TAG=freeze-upgrades-keepid "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-keepid.log" 2>&1 ;;
    rename) env SWIM_RENAME=1 SWIM_VIEW=0 TAG=freeze-upgrades-rename "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-rename.log" 2>&1 ;;
    keepview) env SWIM_RENAME=0 SWIM_VIEW=1 TAG=freeze-upgrades-keepview "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-keepview.log" 2>&1 ;;
    lifeguard) env SWIM_RENAME=1 SWIM_VIEW=0 SWIM_LIFEGUARD=1 TAG=freeze-upgrades-lifeguard "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-lifeguard.log" 2>&1 ;;
    both) env SWIM_RENAME=0 SWIM_VIEW=1 SWIM_LIFEGUARD=1 TAG=freeze-upgrades-both "$S/run.sh" 5000 >"$L/n5000-freeze-upgrades-both.log" 2>&1 ;;
    esac
    echo "$(date +%T) $v done" >>"$L/meltdown.log"
done
