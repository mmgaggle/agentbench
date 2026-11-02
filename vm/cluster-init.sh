#!/bin/bash
# Cluster-side material for the agent VMs (inside agentvm), in /vms/cluster:
#   hosts.json      hosts node1..N with agent keyrings (agentd --hosts-file)
#   tls/            cluster CA, agentd's endpoint cert (10.77.0.1, agentd.lan)
#                   and the mgr->agent client cert agentd loads at startup
#   intent.key      Ed25519 key that signs intents (the mgr's), + .pub
#   nodeN/          agent.json and keyring for each node
# Idempotent: existing material is kept.
set -euo pipefail
. "$(dirname "$0")/env.sh"
C=$VMS/cluster
BIN=/ab/rust/target/release
mkdir -p "$C/tls"

if [ ! -f "$C/hosts.json" ]; then
    python3 - "$NODES" "$C/hosts.json" <<'PYEOF'
import base64, json, os, sys, uuid
n, out = int(sys.argv[1]), sys.argv[2]
hosts = []
for i in range(1, n + 1):
    name = f'node{i}'
    secret = base64.b64encode(os.urandom(28)).decode()
    hosts.append({'name': name, 'keyring': f'[client.agent.{name}]\n\tkey = {secret}\n', 'port': 4721})
json.dump({'fsid': str(uuid.uuid4()), 'endpoint': '10.77.0.1:7160', 'addr': '10.77.0.1', 'hosts': hosts},
          open(out, 'w'), indent=1)
PYEOF
fi

cd "$C/tls"
if [ ! -f ca.pem ]; then
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 \
        -subj /CN=agentbench-cluster-ca -keyout ca.key -out ca.pem 2>/dev/null
    cp ca.pem root_ca.pem
    issue() {  # name cn san
        openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=$2" \
            -keyout "$1.key" -out "$1.csr" 2>/dev/null
        printf 'subjectAltName=%s\n' "$3" >"$1.ext"
        openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 3650 \
            -extfile "$1.ext" -out "$1.crt" 2>/dev/null
        rm -f "$1.csr" "$1.ext"
    }
    issue endpoint-ec agentd "IP:$GATEWAY,DNS:agentd.lan"
    issue mgr-client mgr "DNS:mgr"
fi
cd "$C"
[ -f intent.key ] || "$BIN/intentctl" keygen --out "$C/intent.key"

for i in $(seq 1 "$NODES"); do
    mkdir -p "$C/node$i"
    python3 - "$C/hosts.json" "node$i" "$C/node$i" <<'PYEOF'
import json, sys
hf, name, d = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
h = next(h for h in hf['hosts'] if h['name'] == name)
open(f'{d}/keyring', 'w').write(h['keyring'])
json.dump({'host': name, 'agentd': '10.77.0.1:7160', 'server_name': 'agentd.lan',
           'ca': '/etc/ceph-agent/ca.pem', 'keyring': '/etc/ceph-agent/keyring',
           'trusted_keys': ['/etc/ceph-agent/intent.key.pub']}, open(f'{d}/agent.json', 'w'), indent=1)
PYEOF
done
echo "fsid $(python3 -c "import json; print(json.load(open('$C/hosts.json'))['fsid'])")"
