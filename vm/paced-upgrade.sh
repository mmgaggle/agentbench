#!/bin/bash
# paced-upgrade.sh BINARY VERSION [intentctl pace options...] (inside agentvm)
# Rolls BINARY out to every node with intentctl's paced push: each node's
# latest desired state again, now naming BINARY as its agent. Canaries go
# first; a failed canary halts the rollout before the other nodes.
#
#   paced-upgrade.sh /ab/vm/demo/crashing-agent.sh 9.9.9 --canary 1 --rate 0.2 --max-in-flight 1
set -euo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
bin=$1
ver=$2
shift 2
hosts=()
for i in $(seq 1 "$NODES"); do
    h=node$i
    spec=$(ls -t "$C/ceph/render/$h"/spec-*.json | grep -v spec-agent | head -1)
    jq --arg src "$bin" --arg v "$ver" '. + {agent: {src: $src, version: $v}}' "$spec" >"$C/ceph/render/$h/spec-agent.json"
    hosts+=(--host "$h")
done
"${INTENTCTL:-/ab/rust/target/release/intentctl}" push --admin "$GATEWAY:7161" --key "$C/intent.key" --state "$C/mgr.json" \
    --spec "$C/ceph/render/{host}/spec-agent.json" "${hosts[@]}" "$@"
