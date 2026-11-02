#!/bin/bash
# Stop the agentbench cluster. Runs only inside the agentbench container, so
# stop.sh's pkill cannot reach the mdsbench cluster in the cephdev container.
here=$(cd "$(dirname "$0")" && pwd)
. "$here/env.sh"
cd "$BUILD"
CEPH_CONF_PATH=$VSTART_DEST /work/ceph/src/stop.sh
