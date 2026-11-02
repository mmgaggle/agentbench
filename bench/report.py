#!/usr/bin/env python3
"""
Aggregate result directories into markdown tables and a JSON digest.

  report.py /ab/results 'A-*' > A.md        # also writes digest.json
"""
import csv
import fnmatch
import json
import os
import sys


def load(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def cpu(run_dir, t0, t1):
    """Mean CPU (cores) per process over [t0, t1] of sampler time, and the
    busiest mgr threads."""
    acc, n, threads = {}, 0, {}
    try:
        f = open(os.path.join(run_dir, 'samples.jsonl'))
    except OSError:
        return {}, []
    with f:
        for line in f:
            r = json.loads(line)
            if not (t0 <= r['t'] <= t1):
                continue
            n += 1
            for k, v in r['proc'].items():
                acc[k] = acc.get(k, 0) + v['cpu']
            for t in r.get('mgr_threads', []):
                key = f"{t['comm']}/{t['tid']}"
                threads[key] = threads.get(key, 0) + t['cpu']
    if not n:
        return {}, []
    top = sorted(((v / n, k) for k, v in threads.items()), reverse=True)[:6]
    return {k: v / n for k, v in acc.items()}, [(k, round(v, 2)) for v, k in top]


def stalls(run_dir, warmup, duration):
    """Seconds in the window where nothing completed although requests had
    been started (the endpoint was stalled)."""
    try:
        rows = list(csv.DictReader(open(os.path.join(run_dir, 'timeseries.csv'))))
    except OSError:
        return None
    started = done = 0
    zero = 0
    for r in rows:
        t = int(r['t'])
        started += int(r['started'])
        d = sum(int(r[k]) for k in ('ok', 'app_err', 'http_err', 'timeout', 'conn_err'))
        done += d
        if warmup <= t < warmup + duration and d == 0 and started > done:
            zero += 1
    return zero


def digest(run_dir):
    meta = load(os.path.join(run_dir, 'meta.json'), {})
    a = meta.get('args', {})
    s = load(os.path.join(run_dir, 'summary.json'))
    p = load(os.path.join(run_dir, 'probes.json'), {})
    ins = load(os.path.join(run_dir, 'instr.json'))
    stats = load(os.path.join(run_dir, 'agentd-stats.json'))
    warmup = (s or {}).get('args', {}).get('warmup')
    if warmup is None:
        warmup = a.get('warmup') if a.get('warmup') is not None else (5 if a.get('saturate') else a.get('period', 20))
    dur = a.get('duration', 0)
    # the sampler starts with the measured agentsim run
    c, top = cpu(run_dir, warmup, warmup + dur)
    d = {
        'name': os.path.basename(run_dir),
        'agents': a.get('agents'),
        'period': a.get('period'),
        'saturate': a.get('saturate'),
        'idle': a.get('idle', False),
        'cpu': {k: round(v, 3) for k, v in c.items()},
        'mgr_top_threads': top,
        'probes': p,
    }
    if s:
        d.update({
            'offered': s.get('offered_per_sec'),
            'ok_per_sec': s['ok_per_sec'],
            'counts': s['counts'],
            'latency': s['latency'],
            'tls_handshake': s['tls_handshake'],
            'response': s['response'],
            'gaps': s.get('max_report_gap'),
            'stall_secs': stalls(run_dir, warmup, dur),
            'errors': s.get('errors', [])[:3],
            'body_bytes': s.get('body_bytes'),
        })
    if ins:
        d['instr'] = ins
        secs = ins.get('_seconds') or 1
        d['gc_frac'] = sum(v['wall_s'] for k, v in ins.items() if k.startswith('gc.')) / secs
        d['handler_cpu_frac'] = ins.get('agent.index', {}).get('cpu_s', 0) / secs
    if stats:
        d['agentd'] = stats
    return d


def fmt(v, nd=1):
    if v is None:
        return '-'
    if isinstance(v, float):
        return f'{v:.{nd}f}'
    return str(v)


def main():
    root = sys.argv[1]
    pats = sys.argv[2:] or ['*']
    runs = sorted(d for d in os.listdir(root)
                  if os.path.isdir(os.path.join(root, d)) and any(fnmatch.fnmatch(d, p) for p in pats))
    ds = [digest(os.path.join(root, r)) for r in runs]
    with open(os.path.join(root, 'digest.json'), 'w') as f:
        json.dump(ds, f, indent=1)

    probes = ['orch_host_ls', 'orch_ps_host', 'balancer_status', 'pg_stat', 'mon_health', 'prom_metrics',
              'agentd_host']
    print('| run | hosts | offered/s | ok/s | fail | p50 ms | p99 ms | max ms | tls p50/p99 | stalled s | >3x period | mgr cpu | mon cpu | agentd cpu |')
    print('|---|---|---|---|---|---|---|---|---|---|---|---|---|---|')
    for d in ds:
        if 'ok_per_sec' not in d:
            continue
        fail = sum(v for k, v in d['counts'].items() if k != 'ok')
        print(f"| {d['name']} | {d['agents']} | {fmt(d['offered'])} | {fmt(d['ok_per_sec'])} | {fail} | "
              f"{fmt(d['latency']['p50_ms'])} | {fmt(d['latency']['p99_ms'])} | {fmt(d['latency']['max_ms'], 0)} | "
              f"{fmt(d['tls_handshake']['p50_ms'])}/{fmt(d['tls_handshake']['p99_ms'])} | {fmt(d['stall_secs'])} | "
              f"{(d.get('gaps') or {}).get('agents_over_3x_period', '-')} | "
              f"{fmt(d['cpu'].get('ceph-mgr'), 2)} | {fmt(d['cpu'].get('ceph-mon'), 2)} | "
              f"{fmt(d['cpu'].get('agentd'), 2)} |")
    print()
    print('Probe latency p50 / p99 (ms)')
    print()
    print('| run | ' + ' | '.join(probes) + ' |')
    print('|---|' + '---|' * len(probes))
    for d in ds:
        if not d['probes']:
            continue
        cells = []
        for p in probes:
            x = d['probes'].get(p)
            cells.append(f"{fmt(x['p50_ms'])} / {fmt(x['p99_ms'])}" if x and x['n'] else '-')
        print(f"| {d['name']} | " + ' | '.join(cells) + ' |')
    print()
    keys = ['agent.handle_metadata', 'process_ls_output', 'save_host', 'save_host_devices', 'save_agent',
            'set_store', 'failed_daemon_health_check', 'get_error_daemons', 'Devices.from_json']
    if any('instr' in d for d in ds):
        print('mgr handler breakdown, per call: wall / cpu ms (calls)')
        print()
        print('| run | ' + ' | '.join(keys) + ' | GIL share: handler cpu / gc |')
        print('|---|' + '---|' * (len(keys) + 1))
        for d in ds:
            if 'instr' not in d:
                continue
            cells = []
            for k in keys:
                x = d['instr'].get(k)
                cells.append(f"{x['wall_ms_per_call']:.1f} / {x['cpu_ms_per_call']:.2f} ({x['calls']})" if x else '-')
            print(f"| {d['name']} | " + ' | '.join(cells)
                  + f" | {d.get('handler_cpu_frac', 0):.2f} / {d.get('gc_frac', 0):.2f} |")


if __name__ == '__main__':
    main()
