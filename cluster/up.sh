#!/bin/bash
# Bring up a fresh vstart cluster with the cephadm module configured for
# agent-driven metadata, without SSH-reachable hosts.
#
#   cluster/up.sh                # 1 mon, 1 mgr, 3 memstore OSDs
#
# Fake hosts are added afterwards by cluster/seed.py.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/env.sh"

# vstart refuses to run if it can find daemons from a previous run.
"$here/down.sh" >/dev/null 2>&1 || true

# Writable copy of the mgr python modules so the cephadm module can be
# instrumented (cluster/instrument.py) without touching the build tree.
if [ ! -d "$MGR_PYTHON_PATH" ]; then
    cp -a /work/ceph/src/pybind/mgr "$MGR_PYTHON_PATH"
fi

rm -rf "$VSTART_DEST"
mkdir -p "$VSTART_DEST"
cd "$BUILD"
MON=1 MGR=1 OSD=3 MDS=0 RGW=0 NFS=0 \
    "$AB/vstart/vstart.sh" -n --nolockdep --memstore --without-dashboard -i 127.0.0.1 \
    -o 'mon_config_key_max_entry_size = 4194304' \
    -o 'memstore_device_bytes = 2147483648' \
    > "$VSTART_DEST/vstart.log" 2>&1

# vstart runs the mon with debug mon = 10 / debug ms = 1; use production
# defaults so logging does not skew mon latency.
sed -i -e '/^\s*debug mon = 10/d' -e '/^\s*debug ms = 1$/d' "$CEPH_CONF"
ceph tell mon.a config set debug_mon 1/5
ceph tell mon.a config set debug_ms 0/0

ceph config set mgr mgr/prometheus/x/server_port "$PROM_PORT" --force
ceph config set mgr mgr/prometheus/server_addr 127.0.0.1
ceph mgr module enable prometheus

# cephadm needs an identity even though no host is reachable over SSH. The
# ssh_config points every host at a closed port so any SSH attempt fails
# immediately instead of hanging.
ssh-keygen -q -t ed25519 -N '' -f "$VSTART_DEST/ssh_key"
ceph config-key set mgr/cephadm/ssh_identity_key -i "$VSTART_DEST/ssh_key"
ceph config-key set mgr/cephadm/ssh_identity_pub -i "$VSTART_DEST/ssh_key.pub"
cat > "$VSTART_DEST/ssh_config" <<EOF
Host *
  User root
  Port 9
  ConnectTimeout 2
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
EOF

ceph config set mgr mgr/cephadm/use_agent true
ceph config set mgr mgr/cephadm/host_check_interval 86400
# Never fall back to SSH refreshes when an agent misses reports (idle gaps
# between runs, or overload). agentsim measures report gaps client-side.
ceph config set mgr mgr/cephadm/agent_down_multiplier 100000
ceph config set mgr mgr/cephadm/warn_on_stray_hosts false
ceph config set mgr mgr/cephadm/warn_on_stray_daemons false
ceph config set mgr mgr/cephadm/autotune_memory_target_ratio 0
ceph mgr module enable cephadm
ceph orch set backend cephadm
ceph cephadm set-ssh-config -i "$VSTART_DEST/ssh_config"
ceph orch pause

# Wait for the agent endpoint's root CA to be generated.
for _ in $(seq 60); do
    if ceph config-key ls 2>/dev/null | grep -q cephadm_root_ca_cert; then
        break
    fi
    sleep 1
done
ceph config-key ls | grep -i -E 'root_ca|cert_store' | head
echo "cluster up: $(ceph fsid)"

# Keep the cluster on CCX0 (cpus 0-7,16-23); load generation runs on CCX1.
"$here/pin.sh"
