#!/bin/bash
# Agent scale test: one agentd, N simulated hosts running the real agent
# code (agentd client, reconciler, reporter, SWIM member) over in-memory
# systemd, and the mgr side in Python. Runs inside the agentbench container.
#
#   run.sh N [P]      P agent processes (default 12) plus one 'rack'
#
# Phases (timestamps in phases.txt, results in results.jsonl):
#   join      every host joins at once (token, CSR, sealing key); intentctl
#             approves
#   start     every agent starts at once: TLS connect, first status, SWIM
#   deploy    first intents: 12 OSDs + crash + node-exporter per host
#   steady    idle: long-polls, SWIM, drift checks
#   change    a new ceph.conf: every ceph daemon restarts
#   freeze    a rack of hosts stops (SIGSTOP): SWIM detection, then thaw
#   restart   agentd is killed and restarted: reconnect storm, the mgr
#             re-teaches members and republishes
# and, when named in PHASES:
#   ceph-upgrade         a new image in every Quadlet file, then the wait
#                        until agentd has dropped the old files (gc)
#   agent-upgrade-paced  a new agent binary: canaries, then UPGRADE_RATE
#                        hosts/s, at most 100 in flight
#   agent-upgrade-all    a new agent binary to every host at once
#   agent-upgrade-paced-freeze, agent-upgrade-all-freeze
#                        the same, with a rack frozen partway through
#   agent-upgrade-paced-hog
#                        the paced rollout, with the simulator's CPUs
#                        mostly taken away for HOG_S seconds partway
#                        through: does SWIM recover once they're back?
#
#   PHASES="deploy ceph-upgrade agent-upgrade-paced" run.sh 5000
set -uo pipefail
N=$1
P=${2:-12}
RACK=${RACK:-40}
STEADY=${STEADY:-180}
# Agents join at this many hosts/s (0: all at once). SWIM spreads each
# membership change ~4*log10(N) times per member at ~15 packets/s, so a
# mass join leaves minutes of backlog; real hosts absorb that, a simulator
# running thousands of members on a few threads does not.
RAMP_RATE=${RAMP_RATE:-40}
SETTLE=${SETTLE:-30}
# SWIM=0: agents without SWIM members (the HTTP path only; no freeze phase)
SWIM=${SWIM:-1}
PHASES=${PHASES:-deploy steady change freeze restart}
BLOB_GRACE=${BLOB_GRACE:-600}
UPGRADE_RATE=${UPGRADE_RATE:-10}
# how long agent-upgrade-paced-hog takes the simulator's CPUs away
HOG_S=${HOG_S:-70}
# the agent binary a rollout ships (the stripped agent is 8 MB)
AGENT_SIZE=${AGENT_SIZE:-8388608}
# SWIM threads per simulator process: each runs all of that process's
# members (N/P of them), where a real host's SWIM thread runs one
SWIM_THREADS=${SWIM_THREADS:-1}
# 1: agents take a new SWIM identity at every start (the old behaviour,
# where an upgrade renames every member); 0: they keep it
SWIM_RENAME=${SWIM_RENAME:-0}
# 0: members start from an empty membership after a restart, as before
# they saved it (relearning it gossips every member as news); 1: restore it
SWIM_VIEW=${SWIM_VIEW:-1}
# 1: Lifeguard on every member, agentd's included (vendored foca)
SWIM_LIFEGUARD=${SWIM_LIFEGUARD:-0}
UP=$N
SIM_ARGS=
if [ "$SWIM" = 0 ]; then
    UP=0
    SIM_ARGS=--no-swim
else
    [ "$SWIM_RENAME" = 1 ] && SIM_ARGS+=" --swim-rename-on-start"
    [ "$SWIM_VIEW" = 0 ] && SIM_ARGS+=" --swim-forget-view"
    [ "$SWIM_LIFEGUARD" = 1 ] && SIM_ARGS+=" --swim-lifeguard"
fi
AGENTD_ARGS=
[ "$SWIM_LIFEGUARD" = 1 ] && AGENTD_ARGS=--swim-lifeguard
AB=/ab
B=${BIN_DIR:-$AB/rust/target/release}
S=$AB/bench/scale
D=/mon/scale/n$N
T=/mon/scale/tls
OUT=$AB/results/scale/n$N${TAG:+-$TAG}
ADMIN=127.0.0.1:7161
export LD_LIBRARY_PATH=/work/ceph/build/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
# agentd on 2 cores (it has needed well under one), the mgr side on 2, the
# simulated hosts on the other 12 (24 threads)
AGENTD_CPUS=0-1,16-17
MGR_CPUS=2-3,18-19
SIM_CPUS=4-15,20-31

stamp() {
    # CPU ticks so far: agentd, and all simulated agents together
    local a s
    a=$(awk '{print $14+$15}' "/proc/$(cat "$D/agentd.pid" 2>/dev/null)/stat" 2>/dev/null || echo 0)
    s=$(for p in $(pgrep -f "[c]eph-agent sim "); do awk '{print $14+$15}' "/proc/$p/stat"; done | awk '{s+=$1} END {print s+0}')
    echo "$(date +%s.%N) $* agentd_ticks=${a:-0} sim_ticks=$s" >>"$OUT/phases.txt"
    echo "[$(date +%T)] $*"
}

stop_all() {
    pkill -f "[c]eph-agent sim" 2>/dev/null
    pkill -x agentd 2>/dev/null
    pkill -f "[f]ollow.py" 2>/dev/null
    [ -n "${SAMPLER:-}" ] && kill "$SAMPLER" 2>/dev/null
    [ -n "${THREADS:-}" ] && kill "$THREADS" 2>/dev/null
    sleep 1
}

start_agentd() {
    taskset -c $AGENTD_CPUS "$B/agentd" --hosts-file "$D/hosts.json" --persist none --key ec --cert-dir "$T" \
        --listen 127.0.0.1:7160 --admin $ADMIN --swim-bind 127.3.0.1:7946 --swim-cluster-size "$N" --blob-grace-s "$BLOB_GRACE" $AGENTD_ARGS \
        >>"$OUT/agentd.log" 2>&1 &
    echo $! >"$D/agentd.pid"
    until curl -sf "http://$ADMIN/stats" >/dev/null; do
        kill -0 "$(cat "$D/agentd.pid")" 2>/dev/null || { tail -3 "$OUT/agentd.log"; exit 1; }
        sleep 0.1
    done
}

mgr() {
    taskset -c $MGR_CPUS python3 "$S/mgr.py" "$@" --admin $ADMIN --state "$D/mgr.json" --key "$D/intent.key" \
        --cache "$D/mgrcache.json" --result "$OUT/results.jsonl"
}

until_() {
    taskset -c $MGR_CPUS python3 "$S/until.py" "$@" --admin $ADMIN --result "$OUT/results.jsonl"
}

stop_all
rm -rf "$D" "$OUT"
mkdir -p "$D" "$OUT"
if [ ! -f "$T/ca.pem" ]; then
    mkdir -p "$T"
    (
        cd "$T"
        openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 \
            -subj /CN=agentscale-ca -keyout ca.key -out ca.pem 2>/dev/null
        cp ca.pem root_ca.pem
        issue() {
            openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=$2" \
                -keyout "$1.key" -out "$1.csr" 2>/dev/null
            printf 'subjectAltName=%s\n' "$3" >"$1.ext"
            openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
                -extfile "$1.ext" -out "$1.crt" 2>/dev/null
            rm -f "$1.csr" "$1.ext"
        }
        issue endpoint-ec agentd "IP:127.0.0.1,DNS:agentd.lan"
        issue mgr-client mgr "DNS:mgr"
    )
fi
echo '{"fsid": "5c2e6a6e-6f4b-4c43-9a7e-0b5d5c2a7e11", "endpoint": "127.0.0.1:7160", "addr": "127.0.0.1", "hosts": []}' >"$D/hosts.json"
"$B/intentctl" keygen --out "$D/intent.key" >/dev/null
seq -f "h%05g" 0 $((N - 1)) >"$D/hosts.txt"
tail -n "$RACK" "$D/hosts.txt" >"$D/rack.txt"
echo "N=$N P=$P RACK=$RACK STEADY=$STEADY RAMP_RATE=$RAMP_RATE SETTLE=$SETTLE SWIM=$SWIM PHASES='$PHASES' BLOB_GRACE=$BLOB_GRACE UPGRADE_RATE=$UPGRADE_RATE HOG_S=$HOG_S AGENT_SIZE=$AGENT_SIZE SWIM_THREADS=$SWIM_THREADS SWIM_RENAME=$SWIM_RENAME SWIM_VIEW=$SWIM_VIEW SWIM_LIFEGUARD=$SWIM_LIFEGUARD BIN_DIR=$B" >"$OUT/config.txt"

start_agentd
taskset -c $MGR_CPUS python3 "$AB/bench/sample.py" --out "$OUT" &
SAMPLER=$!
taskset -c $MGR_CPUS python3 "$S/threads.py" --out "$OUT" &
THREADS=$!
"$B/intentctl" token --state "$D/mgr.json" --hosts-file "$D/hosts.txt" --agentd 127.0.0.1:7160 \
    --server-name agentd.lan --ca "$T/ca.pem" --ttl 7200 >"$D/tokens.txt"

# host ranges: P processes over the first N-RACK hosts, then the rack
ranges=()
main=$((N - RACK))
for p in $(seq 0 $((P - 1))); do
    a=$((p * main / P))
    b=$(((p + 1) * main / P))
    ranges+=("$a:$((b - a))")
done
ranges+=("$main:$RACK")

stamp join
taskset -c $MGR_CPUS "$B/intentctl" approve --admin $ADMIN --state "$D/mgr.json" --ca-cert "$T/ca.pem" \
    --ca-key "$T/ca.key" --intent-key "$D/intent.key" --swim-seed 127.3.0.1:7946 --swim-cluster-size "$N" \
    --expect "$N" --quiet --timeout 1800 >"$OUT/approve.log" 2>&1 &
APPROVE=$!
jpids=()
for k in "${!ranges[@]}"; do
    IFS=: read -r a c <<<"${ranges[$k]}"
    taskset -c $SIM_CPUS "$B/ceph-agent" sim-join --tokens "$D/tokens.txt" --dir "$D" --from "$a" --count "$c" \
        --threads 2 >"$OUT/join-$k.log" 2>&1 &
    jpids+=($!)
done
for p in "${jpids[@]}"; do wait "$p"; done
wait $APPROVE
stamp join-done
grep -h joined "$OUT"/join-*.log | head -3
tail -1 "$OUT/approve.log"

stamp start
spids=()
for k in "${!ranges[@]}"; do
    IFS=: read -r a c <<<"${ranges[$k]}"
    ramp=0
    [ "$RAMP_RATE" -gt 0 ] && ramp=$((N / RAMP_RATE))
    taskset -c $SIM_CPUS "$B/ceph-agent" sim --dir "$D" --from "$a" --count "$c" --threads 2 --swim-threads "$SWIM_THREADS" --ramp-s "$ramp" $SIM_ARGS \
        >"$OUT/sim-$k.out" 2>"$OUT/sim-$k.log" &
    spids+=($!)
done
RACKPID=${spids[-1]}
until_ counts --reported "$N" --up "$UP" --label start --timeout $((900 + N / (RAMP_RATE > 0 ? RAMP_RATE : 1)))
stamp start-done
sleep "$SETTLE"

# two followers on the same feed: the mgr module's read path as written
# (a request per changed host, the full summary on liveness/applied
# events) and the lean one (batch status, /v1/liveness)
taskset -c $MGR_CPUS python3 "$S/follow.py" --out "$OUT/follow.jsonl" &
FOLLOWER=$!
taskset -c $MGR_CPUS python3 "$S/follow.py" --batch --out "$OUT/follow-batch.jsonl" &
FOLLOWER2=$!
sleep 5

phase_deploy() {
    stamp deploy
    mgr deploy --procs 4 --label deploy --timeout 1200
    stamp deploy-done
}

phase_steady() {
    stamp steady
    sleep "$STEADY"
    stamp steady-done
}

phase_change() {
    stamp change
    mgr change --procs 1 --label change
    stamp change-done
    sleep 20
}

phase_freeze() {
    [ "$SWIM" = 0 ] && return
    stamp freeze
    t0=$(date +%s%N)
    kill -STOP "$RACKPID"
    until_ liveness --hosts-file "$D/rack.txt" --state down --t0-ns "$t0" --label freeze --timeout 120
    sleep 10
    stamp thaw
    t0=$(date +%s%N)
    kill -CONT "$RACKPID"
    until_ liveness --hosts-file "$D/rack.txt" --state up --t0-ns "$t0" --label thaw --timeout 120
    stamp thaw-done
    sleep 20
}

phase_restart() {
    stamp restart
    t0=$(date +%s%N)
    old=$(cat "$D/agentd.pid")
    kill -9 "$old"
    # a process holding 20,000 sockets takes a moment to go
    while kill -0 "$old" 2>/dev/null; do sleep 0.05; done
    start_agentd
    "$B/intentctl" members --admin $ADMIN --state "$D/mgr.json"
    until_ counts --reported "$N" --up "$UP" --t0-ns "$t0" --label reconnect --timeout ${RECONNECT_TIMEOUT:-300}
    mgr republish --procs 4 --label republish --timeout 1200
    stamp restart-done
    sleep 20
}

CEPH_UPGRADES=0
phase_ceph_upgrade() {
    # each one a new image; labels count up when PHASES names it twice
    CEPH_UPGRADES=$((CEPH_UPGRADES + 1))
    local n=""
    [ $CEPH_UPGRADES -gt 1 ] && n=-$CEPH_UPGRADES
    stamp ceph-upgrade$n
    mgr ceph-upgrade --procs 4 --label ceph-upgrade$n --timeout 1200
    stamp ceph-upgrade$n-done
    stamp gc$n
    until_ gc --label gc$n --timeout $((BLOB_GRACE * 2 + 120))
    stamp gc$n-done
    sleep 10
}

phase_agent_upgrade_paced() {
    stamp agent-upgrade-paced
    mgr agent-upgrade --version 0.2.6-sim --size "$AGENT_SIZE" --rate "$UPGRADE_RATE" --canary 10 --soak 10 --max-in-flight 100 \
        --label agent-upgrade-paced --timeout 7200
    stamp agent-upgrade-paced-done
    sleep 30
}

phase_agent_upgrade_all() {
    stamp agent-upgrade-all
    mgr agent-upgrade --version 0.2.7-sim --size "$AGENT_SIZE" --rate 0 --canary 0 --max-in-flight 0 --label agent-upgrade-all --timeout 1800
    stamp agent-upgrade-all-done
    # SWIM's backlog of renamed members drains after the last host is done
    until_ counts --reported "$N" --up "$UP" --label after-agent-upgrade-all --timeout 600
    stamp settled-after-agent-upgrade-all
    sleep 30
}

# A rack fails partway through an agent rollout: is it noticed as fast as
# in a quiet cluster, and without false downs? The rollout keeps going
# (the rack's hosts are last in its queue, or already done).
upgrade_with_freeze() {
    local kind=$1 after=$2 version=$3 label=agent-upgrade-$1-freeze args
    if [ "$kind" = paced ]; then
        args="--rate $UPGRADE_RATE --canary 10 --soak 10 --max-in-flight 100"
    else
        args="--rate 0 --canary 0 --max-in-flight 0"
    fi
    stamp "$label"
    # shellcheck disable=SC2086
    mgr agent-upgrade --version "$version" --size "$AGENT_SIZE" $args --max-failures 100 --host-timeout 300 --label "$label" --timeout 7200 &
    local up=$!
    sleep "$after"
    stamp "freeze-$kind"
    t0=$(date +%s%N)
    kill -STOP "$RACKPID"
    until_ liveness --hosts-file "$D/rack.txt" --state down --t0-ns "$t0" --label "freeze-during-$kind" --timeout 180
    sleep 10
    stamp "thaw-$kind"
    t0=$(date +%s%N)
    kill -CONT "$RACKPID"
    until_ liveness --hosts-file "$D/rack.txt" --state up --t0-ns "$t0" --label "thaw-during-$kind" --timeout 180
    wait "$up"
    stamp "$label-done"
    # how long the gossip takes to quiet down (sim-*.out, every 10 s)
    sleep "${SETTLE_AFTER_UPGRADE:-30}"
    stamp "settled-$label"
}

phase_agent_upgrade_paced_freeze() {
    upgrade_with_freeze paced 60 0.2.8-sim
}

phase_agent_upgrade_all_freeze() {
    upgrade_with_freeze all 5 0.2.9-sim
}

# A 70 s cargo build on the simulator's CPUs during a paced rollout once
# tipped SWIM into a meltdown it never left. The same on purpose: 90 s into
# the rollout, two busy loops on each of the simulator's CPUs for HOG_S
# seconds (the simulator keeps about a third of them), then the rollout's
# end and SETTLE_AFTER_UPGRADE seconds more.
phase_agent_upgrade_paced_hog() {
    local label=agent-upgrade-paced-hog
    stamp "$label"
    mgr agent-upgrade --version 0.2.10-sim --size "$AGENT_SIZE" --rate "$UPGRADE_RATE" --canary 10 --soak 10 \
        --max-in-flight 100 --max-failures 100 --host-timeout 300 --label "$label" --timeout 7200 &
    local up=$!
    sleep 90
    stamp hog
    python3 "$S/hog.py" --cpus "$SIM_CPUS" --per-cpu 2 --seconds "$HOG_S"
    stamp hog-done
    wait "$up"
    stamp "$label-done"
    sleep "${SETTLE_AFTER_UPGRADE:-30}"
    stamp "settled-$label"
}

for ph in $PHASES; do
    "phase_${ph//-/_}"
done

curl -s "http://$ADMIN/stats" >"$OUT/agentd-stats.json"
kill "$FOLLOWER" "$FOLLOWER2"
wait "$FOLLOWER" "$FOLLOWER2" 2>/dev/null
for k in "${!ranges[@]}"; do tail -1 "$OUT/sim-$k.out" >>"$OUT/swim.txt"; done
stop_all
echo "agent log lines: $(cat "$OUT"/sim-*.log | wc -l)"
grep -h "error\|Error\|failed\|retrying" "$OUT"/sim-*.log | sed 's/h[0-9]\{5\}/hN/g; s/[0-9]\+/N/g' | sort | uniq -c | sort -rn | head -15
echo "done: $OUT"
