#!/bin/bash
# Bridge, tap devices, NAT and DHCP for the VMs. Idempotent; run inside
# the agentvm container (its network namespace is private, so none of this
# touches bravo's own networking).
set -euo pipefail
. "$(dirname "$0")/env.sh"

if ! ip link show "$BRIDGE" >/dev/null 2>&1; then
    ip link add "$BRIDGE" type bridge
    ip addr add "$GATEWAY/24" dev "$BRIDGE"
    ip link set "$BRIDGE" up
fi
for i in $(seq 1 "$NODES"); do
    if ! ip link show "tap$i" >/dev/null 2>&1; then
        ip tuntap add dev "tap$i" mode tap
        ip link set "tap$i" master "$BRIDGE" up
    fi
done
if ! nft list table ip agentvm >/dev/null 2>&1; then
    nft -f - <<EOF
table ip agentvm {
    chain post {
        type nat hook postrouting priority 100;
        ip saddr $NET_PREFIX.0/24 oifname != "$BRIDGE" masquerade
    }
}
EOF
fi

pidfile=$VMS/dnsmasq.pid
if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    kill "$(cat "$pidfile")"
    sleep 0.5
fi
hosts=()
for i in $(seq 1 "$NODES"); do
    hosts+=("--dhcp-host=$(vm_mac "$i"),$(vm_ip "$i"),node$i")
done
dnsmasq --interface="$BRIDGE" --bind-interfaces \
    --dhcp-range="$NET_PREFIX.100,$NET_PREFIX.199,12h" "${hosts[@]}" \
    --address="/agentd.lan/$GATEWAY" \
    --pid-file="$pidfile" --log-facility="$VMS/dnsmasq.log" --log-dhcp
echo "bridge $BRIDGE $GATEWAY/24, taps 1-$NODES, dnsmasq $(cat "$pidfile")"
