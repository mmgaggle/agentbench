#!/bin/bash
# Does SWIM recover from a CPU disturbance? A 70 s cargo build on the
# simulator's CPUs during a paced rollout once tipped it into a meltdown it
# never left (results/scale/n5000-freeze-upgrades-rename-disturbed). The
# same on purpose: a paced rollout of 5,000 hosts with the simulator's CPUs
# mostly taken away for HOG_S seconds partway through (run.sh's
# agent-upgrade-paced-hog), for
#   old     the agents as they were: a new identity and an empty membership
#           at every start
#   old-lg  the same, with Lifeguard
#   new     agents that keep their identity and membership
#   new-lg  the same, with Lifeguard
# Waits for meltdown.sh to finish first (both use the whole machine).
set -uo pipefail
S=$(cd "$(dirname "$0")" && pwd)
L=/ab/results/scale
while pgrep -f "[m]eltdown.sh" >/dev/null; do sleep 30; done
export PHASES='deploy agent-upgrade-paced-hog' UPGRADE_RATE=20 SETTLE_AFTER_UPGRADE=300
for v in ${VARIANTS:-old old-lg new new-lg}; do
    case $v in
    old) env SWIM_RENAME=1 SWIM_VIEW=0 TAG=hog-old "$S/run.sh" 5000 >"$L/n5000-hog-old.log" 2>&1 ;;
    old-lg) env SWIM_RENAME=1 SWIM_VIEW=0 SWIM_LIFEGUARD=1 TAG=hog-old-lg "$S/run.sh" 5000 >"$L/n5000-hog-old-lg.log" 2>&1 ;;
    new) env TAG=hog-new "$S/run.sh" 5000 >"$L/n5000-hog-new.log" 2>&1 ;;
    new-lg) env SWIM_LIFEGUARD=1 TAG=hog-new-lg "$S/run.sh" 5000 >"$L/n5000-hog-new-lg.log" 2>&1 ;;
    esac
    echo "$(date +%T) $v done" >>"$L/hogtest.log"
done
