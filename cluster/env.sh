# Shared environment for the agentbench vstart cluster. Source inside the
# agentbench container (bravo: `podman exec -it agentbench bash`).
export AB=${AB:-/ab}
export BUILD=${BUILD:-/work/ceph/build}
export VSTART_DEST=${VSTART_DEST:-/mon/vcluster}
export CEPH_CONF=$VSTART_DEST/ceph.conf
export CEPH_PORT=${CEPH_PORT:-45000}
export MGR_PYTHON_PATH=${MGR_PYTHON_PATH:-$AB/mgr-modules}
export PROM_PORT=${PROM_PORT:-19283}
export PYTHONPATH=/work/ceph/src/pybind:$BUILD/lib/cython_modules/lib.3:/work/ceph/src/python-common${PYTHONPATH:+:$PYTHONPATH}
export LD_LIBRARY_PATH=$BUILD/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export PATH=$BUILD/bin:$PATH

ceph() { "$BUILD/bin/ceph" -c "$CEPH_CONF" "$@"; }
