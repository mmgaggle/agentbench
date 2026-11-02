"""
agentbench instrumentation for the cephadm mgr module.

Installed as cephadm/agentbench_instr.py in the benchmark's copy of the mgr
modules and imported at the end of cephadm/module.py (cluster/instrument.sh).
Active only if /ab/run/instrument exists when the module loads.

Wraps the methods on the agent /data path and accumulates, per method:
calls, wall seconds, thread-CPU seconds and max wall. Wall minus CPU is time
spent blocked: on the GIL, or on I/O (set_store waits for the mon). Also times
Python's cyclic GC per generation (gc.genN; wall == cpu, GIL held). Every
two seconds the totals are written to /ab/run/instr.json; the runner diffs
two snapshots.
"""
import functools
import json
import os
import threading
import time

FLAG = '/ab/run/instrument'
OUT = '/ab/run/instr.json'

# Reentrant: the gc callback can fire inside a wrapper's critical section (any
# allocation there may trigger a collection) and takes this lock too.
_lock = threading.RLock()
_stats = {}
_tls = threading.local()


def _wrap(cls, name, label=None):
    orig = getattr(cls, name)
    label = label or f'{cls.__name__}.{name}'

    @functools.wraps(orig)
    def wrapper(*a, **kw):
        w0 = time.perf_counter()
        c0 = time.thread_time()
        try:
            return orig(*a, **kw)
        finally:
            wall = time.perf_counter() - w0
            cpu = time.thread_time() - c0
            with _lock:
                s = _stats.setdefault(label, [0, 0.0, 0.0, 0.0])
                s[0] += 1
                s[1] += wall
                s[2] += cpu
                if wall > s[3]:
                    s[3] = wall
    setattr(cls, name, wrapper)


_gc_start = [0.0]


def _gc_cb(phase, info):
    # Python's cyclic GC runs in whichever thread allocates, holding the GIL.
    if phase == 'start':
        _gc_start[0] = time.perf_counter()
        return
    dt = time.perf_counter() - _gc_start[0]
    label = f'gc.gen{info.get("generation")}'
    with _lock:
        s = _stats.setdefault(label, [0, 0.0, 0.0, 0.0])
        s[0] += 1
        s[1] += dt
        s[2] += dt
        if dt > s[3]:
            s[3] = dt


def _writer():
    while True:
        time.sleep(2)
        with _lock:
            snap = {k: {'calls': v[0], 'wall': v[1], 'cpu': v[2], 'max': v[3]} for k, v in _stats.items()}
        snap['_time'] = time.time()
        tmp = OUT + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(snap, f)
        os.replace(tmp, OUT)


def install():
    from ceph.deployment.inventory import Devices
    from cephadm.agent import HostData
    from cephadm.inventory import AgentCache, HostCache
    from cephadm.module import CephadmOrchestrator

    _wrap(HostData, 'index', 'agent.index')
    _wrap(HostData, 'handle_metadata', 'agent.handle_metadata')
    _wrap(CephadmOrchestrator, '_process_ls_output', 'process_ls_output')
    _wrap(CephadmOrchestrator, 'update_failed_daemon_health_check', 'failed_daemon_health_check')
    _wrap(CephadmOrchestrator, 'set_store', 'set_store')
    _wrap(CephadmOrchestrator, 'set_health_checks', 'set_health_checks')
    _wrap(HostCache, 'save_host', 'save_host')
    _wrap(HostCache, 'save_host_devices', 'save_host_devices')
    _wrap(HostCache, 'get_error_daemons', 'get_error_daemons')
    _wrap(HostCache, 'update_host_facts', 'update_host_facts')
    _wrap(HostCache, 'update_host_devices', 'update_host_devices')
    _wrap(HostCache, 'all_host_metadata_up_to_date', 'all_host_metadata_up_to_date')
    _wrap(AgentCache, 'save_agent', 'save_agent')
    _wrap(Devices, 'from_json', 'Devices.from_json')
    import gc
    gc.callbacks.append(_gc_cb)
    threading.Thread(target=_writer, name='agentbench-instr', daemon=True).start()


if os.path.exists(FLAG):
    install()
