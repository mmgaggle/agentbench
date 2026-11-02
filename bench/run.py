#!/usr/bin/env python3
"""
Run one benchmark scenario inside the agentbench container.

  run.py --name base-n1000 --agents 1000                 # agents at 20s
  run.py --name sat-n1000 --agents 1000 --saturate 8     # peak throughput
  run.py --name idle-n1000 --agents 1000 --idle          # probes, no agents
  run.py --name B-n1000 --agents 1000 --agentd '--persist writeback'

Seeds the cluster with exactly --agents hosts (unless --no-seed), primes
every agent once, then runs agentsim with probe.py and sample.py over the
same window. Results land in /ab/results/<name>/.
"""
import argparse
import json
import os
import shlex
import subprocess
import urllib.request
import sys
import threading
import time

AB = os.environ.get('AB', '/ab')
LOADGEN_CPUS = '8-15,24-31'
CLUSTER_CPUS = '0-7,16-23'
SIM = f'{AB}/rust/target/release/agentsim'
AGENTD = f'{AB}/rust/target/release/agentd'
AGENTD_ADMIN = 'http://127.0.0.1:7161'


def agentd_stats():
    try:
        with urllib.request.urlopen(f'{AGENTD_ADMIN}/stats', timeout=10) as r:
            return json.load(r)
    except Exception:
        return None


def start_agentd(extra, log):
    subprocess.run(['pkill', '-x', 'agentd'])
    time.sleep(0.5)
    proc = subprocess.Popen(['taskset', '-c', CLUSTER_CPUS, AGENTD] + shlex.split(extra),
                            stdout=log, stderr=subprocess.STDOUT)
    for _ in range(600):
        if agentd_stats():
            return proc
        if proc.poll() is not None:
            raise RuntimeError('agentd exited')
        time.sleep(0.5)
    raise RuntimeError('agentd did not come up')


def sh(cmd, **kw):
    print('+', ' '.join(cmd), file=sys.stderr)
    return subprocess.run(cmd, check=True, **kw)


def instr_snapshot():
    try:
        with open(f'{AB}/run/instr.json') as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def instr_delta(a, b):
    if not a or not b:
        return None
    out = {'_seconds': b['_time'] - a['_time']}
    for k, v in b.items():
        if k.startswith('_'):
            continue
        p = a.get(k, {'calls': 0, 'wall': 0.0, 'cpu': 0.0})
        calls = v['calls'] - p['calls']
        if calls <= 0:
            continue
        out[k] = {
            'calls': calls,
            'wall_ms_per_call': 1000 * (v['wall'] - p['wall']) / calls,
            'cpu_ms_per_call': 1000 * (v['cpu'] - p['cpu']) / calls,
            'wall_s': v['wall'] - p['wall'],
            'cpu_s': v['cpu'] - p['cpu'],
        }
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--name', required=True)
    p.add_argument('--agents', type=int, required=True)
    p.add_argument('--period', type=float, default=20)
    p.add_argument('--duration', type=float, default=180)
    p.add_argument('--warmup', type=float, default=None)
    p.add_argument('--saturate', type=int)
    p.add_argument('--target', help='endpoint host:port (default: mgr agent endpoint)')
    p.add_argument('--agentd', metavar='ARGS',
                   help='run agentd with these arguments and send agents to it')
    p.add_argument('--keep-agentd', action='store_true',
                   help='reuse a running agentd (with --no-seed)')
    p.add_argument('--osds', type=int, default=16)
    p.add_argument('--prime', type=int, default=16)
    p.add_argument('--no-seed', action='store_true')
    p.add_argument('--no-probes', action='store_true')
    p.add_argument('--idle', action='store_true', help='probes only, no agent traffic')
    p.add_argument('--probe-interval', type=float, default=1.0)
    p.add_argument('--settle', type=float, default=10)
    p.add_argument('--sim-arg', action='append', default=[])
    args = p.parse_args()

    warmup = args.warmup if args.warmup is not None else (5 if args.saturate else args.period)
    out = f'{AB}/results/{args.name}'
    os.makedirs(out, exist_ok=True)
    meta = {'args': vars(args), 'start': time.time(), 'loadavg_before': os.getloadavg()}

    if not args.no_seed:
        t = time.monotonic()
        sh(['python3', f'{AB}/cluster/seed.py', '--hosts', str(args.agents)])
        meta['seed_secs'] = time.monotonic() - t
        time.sleep(args.settle)

    agentd = None
    if args.agentd is not None:
        args.target = args.target or '127.0.0.1:7160'
        if not (args.keep_agentd and agentd_stats()):
            agentd = start_agentd(args.agentd, open(f'{out}/agentd.log', 'w'))

    sim = ['taskset', '-c', LOADGEN_CPUS, SIM, '--agents', str(args.agents),
           '--period', str(args.period), '--osds', str(args.osds), '--listen'] + args.sim_arg
    if args.target:
        sim += ['--target', args.target]

    if not args.idle and args.prime:
        t = time.monotonic()
        sh(sim + ['--prime', str(args.prime), '--duration', '0', '--warmup', '0',
                  '--out', f'{out}/prime'])
        meta['prime_secs'] = time.monotonic() - t

    sampler = subprocess.Popen(['taskset', '-c', LOADGEN_CPUS, 'python3', f'{AB}/bench/sample.py',
                                '--out', out])
    probe_rc = []

    def probes():
        time.sleep(warmup)
        cmd = ['taskset', '-c', LOADGEN_CPUS, 'python3', f'{AB}/bench/probe.py',
               '--duration', str(args.duration), '--interval', str(args.probe_interval), '--out', out]
        if args.agentd is not None:
            cmd += ['--agentd', AGENTD_ADMIN]
        r = subprocess.run(cmd)
        probe_rc.append(r.returncode)
    pt = None
    if not args.no_probes:
        pt = threading.Thread(target=probes)
        pt.start()

    snaps = {}

    def snap_at(when, key):
        time.sleep(when)
        snaps[key] = instr_snapshot()
        if args.agentd is not None:
            snaps['agentd_' + key] = agentd_stats()
    st = [threading.Thread(target=snap_at, args=(warmup, 'a'), daemon=True),
          threading.Thread(target=snap_at, args=(warmup + args.duration, 'b'), daemon=True)]
    for t in st:
        t.start()

    if args.idle:
        time.sleep(warmup + args.duration)
    else:
        cmd = sim + ['--duration', str(args.duration), '--warmup', str(warmup), '--out', out,
                     '--label', args.name]
        if args.saturate:
            cmd += ['--saturate', str(args.saturate)]
        sh(cmd)
    if pt:
        pt.join()
    for t in st:
        t.join(10)
    sampler.terminate()
    sampler.wait()

    meta['end'] = time.time()
    meta['loadavg_after'] = os.getloadavg()
    if args.agentd is not None:
        with open(f'{out}/agentd-stats.json', 'w') as f:
            json.dump({'start': snaps.get('agentd_a'), 'end': snaps.get('agentd_b')}, f, indent=2)
    if agentd and not args.keep_agentd:
        agentd.terminate()
        agentd.wait()
    delta = instr_delta(snaps.get('a'), snaps.get('b'))
    if delta:
        with open(f'{out}/instr.json', 'w') as f:
            json.dump(delta, f, indent=2)
    with open(f'{out}/meta.json', 'w') as f:
        json.dump(meta, f, indent=2)
    print(f'results in {out}', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
