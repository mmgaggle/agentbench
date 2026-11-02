#!/bin/bash
# Does cephadm's single-key inventory fit the mon's default config-key limit?
# Writes the current inventory to a scratch key with the default 64 KiB limit
# in effect, then restores the benchmark's setting.
. /ab/cluster/env.sh
inv=$(mktemp)
ceph config-key get mgr/cephadm/inventory > "$inv" 2>/dev/null
hosts=$(python3 -c "import json,sys; print(len(json.load(open('$inv'))))")
echo "inventory: $hosts hosts, $(stat -c %s "$inv") bytes ($(( $(stat -c %s "$inv") / hosts )) bytes/host)"
ceph tell mon.a config set mon_config_key_max_entry_size 65536 >/dev/null 2>&1
ceph config-key set agentbench/inventory-limit-test -i "$inv" 2>&1 | tail -1
head -c 60000 "$inv" > "$inv.small"
ceph config-key set agentbench/inventory-limit-test -i "$inv.small" 2>&1 | tail -1
ceph config-key rm agentbench/inventory-limit-test >/dev/null 2>&1
ceph tell mon.a config set mon_config_key_max_entry_size 4194304 >/dev/null 2>&1
rm -f "$inv" "$inv.small"
