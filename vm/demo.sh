#!/bin/bash
# Exercises ceph-agent on the VMs (inside agentvm, after cluster-init.sh,
# agentd.sh, install-agent.sh). Each step prints what it checks.
#   demo.sh [step...]   steps: deploy config restart crash crashloop remove
#                       stray agent-restart agent-down forged
set -uo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
D=/ab/vm/demo
ICTL=/ab/rust/target/release/intentctl
ADMIN=$GATEWAY:7161
FSID=$(jq -r .fsid "$C/hosts.json")
ALL=(--host node1 --host node2 --host node3)

push() {  # spec [intentctl args...]
    local spec=$1
    shift
    $ICTL push --admin "$ADMIN" --key "$C/intent.key" --state "$C/mgr.json" --fsid "$FSID" --spec "$D/$spec" "$@"
}
unit_prop() {  # node daemon property
    vm_ssh "$1" systemctl show -P "$3" "ceph-$FSID-$2.service"
}
seq_now() { curl -s "http://$ADMIN/v1/events?since=999999999" | jq .seq; }
events_since() { curl -s "http://$ADMIN/v1/events?since=$1" | jq -c '.events[]'; }

step_deploy() {
    echo "== deploy: node-exporter + web on all nodes"
    push base.json --set version=1 --set nonce=1 "${ALL[@]}" --wait
}

step_config() {
    echo "== config: new index.html, same restart key -> file rewritten, no restart"
    local pid0 since0
    pid0=$(unit_prop 1 web.node1 MainPID)
    since0=$(unit_prop 1 web.node1 ActiveEnterTimestampMonotonic)
    push base.json --set version=2 --set nonce=1 "${ALL[@]}" --wait
    echo "web.node1 serves: $(curl -s "$(vm_ip 1):8080/")"
    echo "MainPID $pid0 -> $(unit_prop 1 web.node1 MainPID), ActiveEnter $since0 -> $(unit_prop 1 web.node1 ActiveEnterTimestampMonotonic)"
}

step_restart() {
    echo "== restart: new restart key -> unit restarted"
    local pid0
    pid0=$(unit_prop 1 web.node1 MainPID)
    push base.json --set version=2 --set nonce=2 "${ALL[@]}" --wait
    echo "MainPID $pid0 -> $(unit_prop 1 web.node1 MainPID), NRestarts $(unit_prop 1 web.node1 NRestarts)"
}

step_crash() {
    echo "== crash: SIGKILL web.node2's container; systemd restarts it after RestartSec=10s"
    local s0 kill_ns
    s0=$(seq_now)
    kill_ns=$(vm_ssh 2 "date +%s%N; podman kill -s KILL ceph-$FSID-web.node2 >/dev/null" | head -1)
    sleep 14
    events_since "$s0" | python3 "$D/latency.py" web.node2 "$kill_ns"
}

step_crashloop() {
    echo "== crash loop: crashy.node3 exits 3 every 2 s; Restart=on-failure, StartLimitBurst=5"
    local s0
    s0=$(seq_now)
    push with-crashy.json --set version=2 --set nonce=2 --host node3 --wait
    sleep 75
    events_since "$s0" | jq -r 'select(.daemon == "crashy.node3") | "  \(.from) -> \(.to) result=\(.result) restarts=\(.n_restarts) apply_error=\(.apply_error)"'
    $ICTL status --admin "$ADMIN" --host node3 | jq -c '.status.units["crashy.node3"] | {active_state, sub_state, result, n_restarts, apply_error}'
    echo "  after another drift pass (the agent must not override systemd's start limit):"
    sleep 65
    $ICTL status --admin "$ADMIN" --host node3 | jq -c '.status.units["crashy.node3"] | {active_state, n_restarts}'
}

step_remove() {
    echo "== remove: web and crashy leave node3 via remove (stop, delete unit, archive data)"
    push remove-web.json --host node3 --wait
    vm_ssh 3 "ls /etc/containers/systemd/; ls /var/lib/ceph/$FSID/removed/; systemctl is-active ceph-$FSID-web.node3.service"
}

step_stray() {
    echo "== stray: a Quadlet file the intent doesn't mention is reported, not touched"
    vm_ssh 1 "cp /etc/containers/systemd/ceph-$FSID-web.node1.container /etc/containers/systemd/ceph-$FSID-mystery.node1.container"
    push base.json --set version=2 --set nonce=2 --host node1 --wait
    $ICTL status --admin "$ADMIN" --host node1 | jq -c '.status.strays'
    vm_ssh 1 "rm /etc/containers/systemd/ceph-$FSID-mystery.node1.container && systemctl daemon-reload"
}

step_agent_restart() {
    local d=${AGENT_DEMO_DAEMON:-node-exporter.node1}
    echo "== agent restart: daemons keep running ($d)"
    local pid0
    pid0=$(unit_prop 1 "$d" MainPID)
    vm_ssh 1 systemctl restart ceph-agent
    sleep 2
    echo "$d MainPID $pid0 -> $(unit_prop 1 "$d" MainPID)"
    vm_ssh 1 journalctl -u ceph-agent -n 3 --no-pager -o cat
}

step_agent_down() {
    local d=${AGENT_DEMO_DAEMON:-node-exporter.node1}
    echo "== agent down: systemd still restarts a crashed daemon ($d); the agent reports it when back"
    vm_ssh 1 "systemctl stop ceph-agent; podman kill -s KILL ceph-$FSID-$d >/dev/null"
    sleep 13
    echo "with the agent stopped: $d $(unit_prop 1 "$d" ActiveState), NRestarts $(unit_prop 1 "$d" NRestarts)"
    vm_ssh 1 systemctl start ceph-agent
    sleep 2
    $ICTL status --admin "$ADMIN" --host node1 | jq -c --arg d "$d" '.status.units[$d] | {active_state, n_restarts}'
}

step_forged() {
    echo "== forged: an intent signed by another key is rejected and reported"
    [ -f "$C/rogue.key" ] || $ICTL keygen --out "$C/rogue.key" >/dev/null
    $ICTL push --admin "$ADMIN" --key "$C/rogue.key" --fsid "$FSID" --spec "$D/remove-web.json" --host node2
    sleep 2
    $ICTL status --admin "$ADMIN" --host node2 | jq -c '{intended: .intended_generation, applied: .status.applied_generation, errors: .status.errors}'
    vm_ssh 2 "systemctl is-active ceph-$FSID-web.node2.service"
}

step_secret() {
    echo "== secret: a keyring sealed to node1's key; agentd only ever holds ciphertext"
    push secret.json --set version=2 --set nonce=2 --host node1 --wait
    local f=/var/lib/ceph/$FSID/web.node1/keyring
    vm_ssh 1 "stat -c '%a %U' $f; head -1 $f"
    local sha
    sha=$($ICTL status --admin "$ADMIN" --host node1 >/dev/null; curl -s "http://$ADMIN/v1/hosts/node1/intent" | jq -r .payload | base64 -d | jq -r --arg f "$f" '.files[$f].sha256')
    echo "agentd's blob $sha starts with: $(curl -s "http://$ADMIN/v1/agent/blobs/$sha" >/dev/null; vm_ssh 1 "head -c 6 /var/lib/ceph-agent/blobs/$sha"; echo) (ciphertext)"
    echo "pushed to node2 too: each host gets its own ciphertext"
    push secret.json --set version=2 --set nonce=2 --host node2 --wait >/dev/null
    local sha2
    sha2=$(curl -s "http://$ADMIN/v1/hosts/node2/intent" | jq -r .payload | base64 -d | jq -r --arg f "${f/node1/node2}" '.files[$f].sha256')
    echo "  node1 blob ${sha:0:16}..., node2 blob ${sha2:0:16}...; node2 reads: $(vm_ssh 2 "head -1 ${f/node1/node2}")"
}

step_swim() {
    echo "== swim: liveness from gossip"
    curl -s "http://$ADMIN/v1/intent-status" | jq -c 'to_entries[] | select(.key | test("node[123]")) | {host: .key, liveness: .value.liveness.state, digest: .value.digest}'
    echo "  freeze node3's agent (SIGSTOP): how long until agentd declares it down?"
    local s0 t0
    s0=$(seq_now)
    t0=$(date +%s%N)
    vm_ssh 3 "systemctl kill -s SIGSTOP ceph-agent"
    until events_since "$s0" | jq -e 'select(.kind == "liveness" and .host == "node3" and .state == "down")' >/dev/null; do sleep 0.2; done
    echo "  down after $(( ($(date +%s%N) - t0) / 1000000 )) ms"
    vm_ssh 3 "systemctl kill -s SIGCONT ceph-agent"
    t0=$(date +%s%N)
    until events_since "$s0" | jq -e 'select(.kind == "liveness" and .host == "node3" and .state == "up")' >/dev/null; do sleep 0.2; done
    echo "  thawed: up again after $(( ($(date +%s%N) - t0) / 1000000 )) ms"
}

step_partition() {
    local d=${PARTITION_DAEMON:-node-exporter.node2}
    echo "== partition: node2 can't reach agentd over TCP; $d crashes; the digest (UDP, straight to agentd) tells agentd it is behind"
    local s0
    s0=$(seq_now)
    vm_ssh 2 "nft add table inet blk; nft add chain inet blk out '{ type filter hook output priority 0; }'; nft add rule inet blk out tcp dport 7160 drop"
    vm_ssh 2 "podman kill -s KILL ceph-$FSID-$d >/dev/null"
    sleep 4
    events_since "$s0" | jq -c 'select(.host == "node2") | {kind, reason, to, at_ns, received_ns}'
    vm_ssh 2 "nft delete table inet blk"
    sleep 3
    echo "  after the partition heals:"
    events_since "$s0" | jq -c 'select(.host == "node2" and .kind == "unit") | {daemon, from, to}' | tail -3
}

step_shutdown() {
    echo "== shutdown: node3 powers off; its agent announces leaving before the host goes"
    local s0 t0
    s0=$(seq_now)
    t0=$(date +%s%N)
    vm_ssh 3 "systemctl poweroff" || true
    until events_since "$s0" | jq -e 'select(.host == "node3" and .kind == "liveness" and (.state == "left" or .state == "down"))' >/dev/null; do sleep 0.1; done
    events_since "$s0" | jq -c --argjson t0 "$t0" 'select(.host == "node3") | {kind, state, ms_after_poweroff: (((.at_ns // .received_ns) - $t0) / 1000000 | floor)}'
    echo "  booting node3 again: daemons come back from ceph-<fsid>.target, the agent rejoins"
    while pgrep -f "[q]emu-system-x86_64 -name node3 " >/dev/null; do sleep 0.5; done
    "$(dirname "$0")/start.sh" 3 >/dev/null
    t0=$(date +%s%N)
    until events_since "$s0" | jq -e 'select(.host == "node3" and .kind == "liveness" and .state == "up")' >/dev/null; do sleep 0.5; done
    echo "  node3 up again $(( ($(date +%s%N) - t0) / 1000000000 )) s after qemu started"
    vm_ssh 3 "systemctl is-active ceph-$FSID-node-exporter.node3.service ceph-$FSID-web.node3.service"
}

step_join_negative() {
    echo "== join: a used token, an expired token and a token for another host are all refused"
    local t
    t=$($ICTL token --state "$C/mgr.json" --host node9 --agentd "$GATEWAY:7160" --server-name agentd.lan --ca "$C/tls/ca.pem")
    vm_ssh 3 "ceph-agent join --etc /tmp/j1 --state-dir /tmp/j1s '$t'" 2>&1 | tail -1 &
    $ICTL approve --admin "$ADMIN" --state "$C/mgr.json" --ca-cert "$C/tls/ca.pem" --ca-key "$C/tls/ca.key" --intent-key "$C/intent.key" --expect 1
    wait
    echo "  reuse:"
    vm_ssh 3 "ceph-agent join --etc /tmp/j2 --state-dir /tmp/j2s '$t'" 2>&1 | tail -1 &
    sleep 1; $ICTL approve --admin "$ADMIN" --state "$C/mgr.json" --ca-cert "$C/tls/ca.pem" --ca-key "$C/tls/ca.key" --intent-key "$C/intent.key"
    wait
    echo "  expired:"
    t=$($ICTL token --state "$C/mgr.json" --host node9 --ttl 0 --agentd "$GATEWAY:7160" --server-name agentd.lan --ca "$C/tls/ca.pem")
    sleep 1
    vm_ssh 3 "ceph-agent join --etc /tmp/j3 --state-dir /tmp/j3s '$t'" 2>&1 | tail -1 &
    sleep 1; $ICTL approve --admin "$ADMIN" --state "$C/mgr.json" --ca-cert "$C/tls/ca.pem" --ca-key "$C/tls/ca.key" --intent-key "$C/intent.key"
    wait
    echo "  a CA that doesn't match the token's pin:"
    t=$($ICTL token --state "$C/mgr.json" --host node9 --agentd "$GATEWAY:7160" --server-name agentd.lan --ca /vms/cluster/tls/endpoint-ec.crt)
    vm_ssh 3 "ceph-agent join --etc /tmp/j4 --state-dir /tmp/j4s '$t'" 2>&1 | tail -1
    vm_ssh 3 "rm -rf /tmp/j1 /tmp/j1s /tmp/j2 /tmp/j2s /tmp/j3 /tmp/j3s /tmp/j4 /tmp/j4s"
}

steps=("$@")
[ ${#steps[@]} -gt 0 ] || steps=(deploy config restart crash crashloop remove stray agent-restart agent-down forged)
for s in "${steps[@]}"; do
    "step_${s//-/_}"
    echo
done
