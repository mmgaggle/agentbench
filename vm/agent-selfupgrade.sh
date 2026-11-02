#!/bin/bash
# agent-selfupgrade.sh BINARY VERSION [node numbers...] (inside agentvm)
# Pushes each node's latest desired state again, now naming BINARY as the
# agent it should run, and waits until the node runs it, or reports why not.
set -euo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
bin=$1
ver=$2
shift 2
nodes=("$@")
[ ${#nodes[@]} -gt 0 ] || mapfile -t nodes < <(seq 1 "$NODES")
want=$(sha256sum "$bin" | cut -d' ' -f1)
t0=$(date +%s%N)
for i in "${nodes[@]}"; do
    h=node$i
    spec=$(ls -t "$C/ceph/render/$h"/spec-*.json | grep -v spec-agent | head -1)
    jq --arg src "$bin" --arg v "$ver" '. + {agent: {src: $src, version: $v}}' "$spec" >"$C/ceph/render/$h/spec-agent.json"
    /ab/rust/target/release/intentctl push --admin "$GATEWAY:7161" --key "$C/intent.key" --state "$C/mgr.json" \
        --spec "$C/ceph/render/$h/spec-agent.json" --host "$h" >/dev/null
done
for i in "${nodes[@]}"; do
    h=node$i
    for _ in $(seq 120); do
        st=$(curl -s "http://$GATEWAY:7161/v1/hosts/$h/status" | jq -c '.status | {agent_version, sha: .agent_sha256[:12], errors}')
        if [ "$(jq -r .sha <<<"$st")" = "${want:0:12}" ] || jq -e '.errors | map(select(test("agent"))) | length > 0' <<<"$st" >/dev/null; then
            break
        fi
        sleep 1
    done
    echo "$h after $(( ($(date +%s%N) - t0) / 1000000 )) ms: $st"
done
