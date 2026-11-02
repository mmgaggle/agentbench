#!/bin/bash
# agentd memory per stored blob (blobmem.py) for one agentd binary:
#   blobmem.sh BIN_DIR [count] [size]
set -u
B=${1:-/ab/rust/target/release}
COUNT=${2:-100000}
SIZE=${3:-1100}
D=/mon/blobmem
mkdir -p $D
export LD_LIBRARY_PATH=/work/ceph/build/lib
echo '{"fsid": "5c2e6a6e-6f4b-4c43-9a7e-0b5d5c2a7e11", "endpoint": "127.0.0.1:7160", "addr": "127.0.0.1", "hosts": []}' >$D/hosts.json
"$B/agentd" --hosts-file $D/hosts.json --persist none --key ec --cert-dir /mon/scale/tls \
    --listen 127.0.0.1:7160 --admin 127.0.0.1:7161 >$D/agentd.log 2>&1 &
pid=$!
until curl -sf http://127.0.0.1:7161/stats >/dev/null; do sleep 0.1; done
python3 /ab/bench/scale/blobmem.py --pid $pid --count "$COUNT" --size "$SIZE"
curl -s http://127.0.0.1:7161/stats | python3 -c 'import json,sys; j=json.load(sys.stdin); print({k: j.get(k) for k in ("blobs", "blob_bytes")})'
kill $pid
wait $pid 2>/dev/null
