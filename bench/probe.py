#!/usr/bin/env python3
"""
Measure how responsive the cluster stays to ordinary requests while agents
report. Each probe runs in its own thread on a fixed schedule and records
its latency; a probe that falls behind fires again immediately (so slow
responses show up as fewer samples, recorded in `behind`).

  orch_host_ls     mgr, cephadm via the orchestrator module (Python)
  orch_ps_host     mgr, cephadm daemon cache for one host (Python)
  balancer_status  mgr, an unrelated Python module
  pg_stat          mgr, C++ DaemonServer only (no Python)
  mon_health       mon
  prom_metrics     prometheus module HTTP scrape (Python, CherryPy)
  agentd_host      agentd's per-host daemon query (--agentd), the analogue
                   of orch_ps_host when agentd owns the agent data
"""
import argparse
import json
import os
import sys
import threading
import time
import urllib.request

import rados


def percentile(v, q):
    if not v:
        return None
    v = sorted(v)
    return v[min(len(v) - 1, int(round((len(v) - 1) * q)))]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--conf', default=os.environ.get('CEPH_CONF'))
    p.add_argument('--duration', type=float, required=True)
    p.add_argument('--interval', type=float, default=1.0)
    p.add_argument('--host', default='abhost-00000')
    p.add_argument('--prom', default=f'http://127.0.0.1:{os.environ.get("PROM_PORT", "19283")}/metrics')
    p.add_argument('--timeout', type=float, default=60)
    p.add_argument('--out', required=True)
    p.add_argument('--only', default='', help='comma-separated probe names')
    p.add_argument('--agentd', help='agentd admin URL, e.g. http://127.0.0.1:7161')
    args = p.parse_args()

    cluster = rados.Rados(conffile=args.conf)
    cluster.connect()

    def mgr(cmd):
        def f():
            ret, out, errs = cluster.mgr_command(json.dumps(cmd), b'', timeout=args.timeout)
            if ret != 0:
                raise RuntimeError(f'{ret} {errs}')
            return len(out)
        return f

    def mon(cmd):
        def f():
            ret, out, errs = cluster.mon_command(json.dumps(cmd), b'', timeout=args.timeout)
            if ret != 0:
                raise RuntimeError(f'{ret} {errs}')
            return len(out)
        return f

    def prom():
        with urllib.request.urlopen(args.prom, timeout=args.timeout) as r:
            return len(r.read())

    probes = {
        'orch_host_ls': mgr({'prefix': 'orch host ls', 'format': 'json'}),
        'orch_ps_host': mgr({'prefix': 'orch ps', 'hostname': args.host, 'format': 'json'}),
        'balancer_status': mgr({'prefix': 'balancer status', 'format': 'json'}),
        'pg_stat': mgr({'prefix': 'pg stat', 'format': 'json'}),
        'mon_health': mon({'prefix': 'health', 'format': 'json'}),
        'prom_metrics': prom,
    }
    if args.agentd:
        def agentd_host():
            with urllib.request.urlopen(f'{args.agentd}/host/{args.host}', timeout=args.timeout) as r:
                return len(r.read())
        probes['agentd_host'] = agentd_host
    if args.only:
        probes = {k: v for k, v in probes.items() if k in args.only.split(',')}

    t0 = time.time()
    end = time.monotonic() + args.duration
    samples = []
    lock = threading.Lock()

    def run(name, fn, phase):
        next_t = time.monotonic() + phase
        while True:
            now = time.monotonic()
            if now >= end:
                return
            if next_t > now:
                time.sleep(next_t - now)
            start = time.monotonic()
            behind = max(0.0, start - next_t)
            err = None
            size = 0
            try:
                size = fn()
            except Exception as e:
                err = str(e)[:200]
            lat = time.monotonic() - start
            with lock:
                samples.append({'probe': name, 't': round(time.time() - t0, 3),
                                'ms': round(lat * 1000, 3), 'behind_ms': round(behind * 1000, 1),
                                'bytes': size, 'err': err})
            next_t = max(next_t + args.interval, time.monotonic())

    threads = [threading.Thread(target=run, args=(n, f, i * args.interval / len(probes)), daemon=True)
               for i, (n, f) in enumerate(probes.items())]
    for t in threads:
        t.start()
    for t in threads:
        t.join(args.duration + args.timeout + 10)
    cluster.shutdown()

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, 'probes.jsonl'), 'w') as f:
        for s in samples:
            f.write(json.dumps(s) + '\n')
    summary = {}
    for name in probes:
        ok = [s['ms'] for s in samples if s['probe'] == name and not s['err']]
        summary[name] = {
            'n': len(ok),
            'errors': sum(1 for s in samples if s['probe'] == name and s['err']),
            'p50_ms': percentile(ok, 0.5), 'p90_ms': percentile(ok, 0.9),
            'p99_ms': percentile(ok, 0.99), 'max_ms': max(ok) if ok else None,
            'mean_ms': sum(ok) / len(ok) if ok else None,
        }
    with open(os.path.join(args.out, 'probes.json'), 'w') as f:
        json.dump(summary, f, indent=2)
    for name, s in summary.items():
        print(f'{name:16} n={s["n"]:4} err={s["errors"]} p50={s["p50_ms"]} p99={s["p99_ms"]} max={s["max_ms"]}',
              file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
