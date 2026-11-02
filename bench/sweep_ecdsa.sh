#!/bin/bash
# A+ with the endpoint's RSA-4096 cert against A+ with an ECDSA P-256 cert,
# back to back. cluster/ecdsa.diff is the mgr part of ceph branch
# wip-mgr-cephadm-agent-ecdsa: when the module loads, the endpoint replaces
# a stored RSA cert with an ECDSA one. seed.py fails the mgr over, so each
# seeded run reloads the module.
#
# The stored endpoint cert and key are saved first and put back at the end,
# so later runs of the stock module use the RSA cert again.
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
. "$here/../cluster/env.sh"
run() { python3 "$here/run.py" "$@" 2>&1 | grep -v -E 'Deprecat|^\+ |experimental' ; }
KV=mgr/cephadm/cert_store
SAVE=$AB/results/ecdsa-saved-endpoint
NS=${NS:-2000 4000}

endpoint_key() {
    python3 - <<'EOF'
import ssl
from cryptography import x509
for port in range(7150, 7160):
    try:
        pem = ssl.get_server_certificate(('127.0.0.1', port), timeout=5)
    except OSError:
        continue
    print(port, type(x509.load_pem_x509_certificate(pem.encode()).public_key()).__name__)
    break
EOF
}

sweep() {
    local tag=$1
    for n in $NS; do
        run --name "Aplus-$tag-agents-n$n" --agents "$n" --duration 120 --prime 0 --no-probes
        echo "$(date +%T) $tag n$n endpoint:$(endpoint_key)"
        run --name "Aplus-$tag-sat-n$n" --agents "$n" --saturate 8 --duration 45 --no-seed --prime 0 --no-probes
    done
}

mkdir -p "$SAVE"
for o in cert.cephadm-signed_agent_cert key.cephadm-signed_agent_key; do
    ceph config-key get "$KV.$o" >"$SAVE/$o.json" 2>/dev/null || exit 1
done

pkill -x agentd
python3 "$AB/cluster/patch_aplus.py" apply || exit 1
sweep rsa

(cd "$MGR_PYTHON_PATH" && patch -p4 --forward <"$AB/cluster/ecdsa.diff") || exit 1
sweep ecdsa

for f in agent.py ssl_certs.py cert_mgr.py; do
    cp "$AB/mgr-modules.orig/cephadm/$f" "$MGR_PYTHON_PATH/cephadm/$f"
done
for o in cert.cephadm-signed_agent_cert key.cephadm-signed_agent_key; do
    ceph config-key set "$KV.$o" -i "$SAVE/$o.json" 2>/dev/null
done
ceph mgr fail 2>/dev/null
echo "sweep done"
