#!/usr/bin/env python3
"""
Build the A/B report page from report.py's digest.json.

  build_report.py digest.json report_template.html charts.js out.html [proto.json]

The template carries the narrative; this script supplies the numbers the
charts and tables draw (as a JSON blob) and inlines the chart helpers.
"""
import json
import sys

digest, template, charts, out = sys.argv[1:5]
proto = json.load(open(sys.argv[5])) if len(sys.argv) > 5 else {'v': [], 'swim': []}
runs = {r['name']: r for r in json.load(open(digest))}
HOSTS = [250, 500, 750, 1000, 1500, 2000, 4000]


def get(name, *path):
    r = runs.get(name)
    for p in path:
        if not isinstance(r, dict):
            return None
        r = r.get(p)
    return r


def first(*names):
    for n in names:
        if n in runs and runs[n].get('ok_per_sec') is not None:
            return n
    return None


def r1(v, nd=1):
    return None if v is None else round(v, nd)


def agents_run(variant, n):
    if variant == 'A':
        return first(f'Anp-agents-n{n}', f'A-agents-n{n}')
    if variant == 'Aplus':
        return first(f'Aplus-agents-n{n}')
    return first(f'{variant}-agents-n{n}')


def delivered(variant):
    vals, p99, src = [], [], []
    for n in HOSTS:
        name = agents_run(variant, n)
        if not name:
            vals.append(None)
            p99.append(None)
            src.append(None)
            continue
        pct = 100.0 * get(name, 'ok_per_sec') / (n / 20.0)
        vals.append(r1(min(pct, 100.0)))
        # p99 is only meaningful while the endpoint delivers nearly everything
        p99.append(r1(get(name, 'latency', 'p99_ms')) if pct >= 90 else None)
        src.append(name)
    return vals, p99, src


data = {'hosts': HOSTS, 'delivered': {}, 'p99': {}, 'p50': {}, 'src': {}}
for v in ['A', 'Aplus', 'B1', 'B3']:
    d, p, s = delivered(v)
    data['delivered'][v], data['p99'][v], data['src'][v] = d, p, s
    data['p50'][v] = [r1(get(n, 'latency', 'p50_ms')) if n and d[i] and d[i] >= 90 else None
                      for i, n in enumerate(s)]

# Peak capacity of the mgr endpoint vs hosts (8 closed-loop senders).
cap_a = []
for n in HOSTS:
    name = first(f'Anp-sat-n{n}') if n in (500, 750, 1000) else first(f'A-sat-n{n}')
    v = get(name, 'ok_per_sec') if name else None
    cap_a.append(r1(v) if v else None)
data['capacity'] = {
    'A': cap_a,
    'Aplus': [r1(get(f'Aplus-sat-n{n}', 'ok_per_sec')) if f'Aplus-sat-n{n}' in runs else None for n in HOSTS],
    'offered': [n / 20.0 for n in HOSTS],
}

# Peak throughput at 4,000 hosts, every implementation.
data['peak4000'] = [
    {'label': 'A  stock mgr', 'cls': 'a', 'value': r1(get('A-sat-n4000', 'ok_per_sec'))},
    {'label': 'A+  patched mgr', 'cls': 'aplus', 'value': r1(get('Aplus-sat-n4000', 'ok_per_sec'))},
    {'label': 'B1  agentd, sync writes', 'cls': 'b1', 'value': r1(get('B1-sat-n4000-c256', 'ok_per_sec'))},
    {'label': 'B2  agentd, write-back', 'cls': 'b2', 'value': r1(get('B2-sat-n4000-c256', 'ok_per_sec'))},
    {'label': 'agentd, no writes, RSA', 'cls': 'ref', 'value': r1(get('B0-sat-n4000-c256', 'ok_per_sec'))},
    {'label': 'B3  agentd, changed + EC', 'cls': 'b3',
     'value': r1(max(get('B3-sat-n4000-c256', 'ok_per_sec') or 0, get('B3-sat-n4000-c1024', 'ok_per_sec') or 0))},
]

# Concurrency sweep at 250 hosts (stock mgr, no probes).
conc = [1, 2, 4, 16, 32, 64]
data['conc'] = {
    'c': conc,
    'ok': [r1(get(f'A-conc-n250-c{c}', 'ok_per_sec')) for c in conc],
    'p99': [r1(get(f'A-conc-n250-c{c}', 'latency', 'p99_ms')) for c in conc],
}

# Probe latency at 1,000 and 4,000 hosts: mgr with agents (A) vs agents on agentd (B3).
PROBES = ['orch_ps_host', 'orch_host_ls', 'prom_metrics', 'balancer_status', 'pg_stat', 'mon_health', 'agentd_host']
data['probes'] = {}
for n in (1000, 4000):
    for v, name in (('A', f'A-agents-n{n}'), ('B3', f'B3-agents-n{n}')):
        p = get(name, 'probes') or {}
        data['probes'][f'{v}-{n}'] = {k: {'p50': r1((p.get(k) or {}).get('p50_ms')),
                                          'p99': r1((p.get(k) or {}).get('p99_ms')),
                                          'n': (p.get(k) or {}).get('n', 0),
                                          'err': (p.get(k) or {}).get('errors', 0)} for k in PROBES}

# orch ps --hostname p50 vs hosts, with and without agent load on the mgr.
data['orchps'] = {
    'A': [r1(get(f'A-agents-n{n}', 'probes', 'orch_ps_host', 'p50_ms')) for n in HOSTS],
    'B3': [r1(get(f'B3-agents-n{n}', 'probes', 'orch_ps_host', 'p50_ms')) for n in HOSTS],
}


def agentd_rates(name):
    s = get(name, 'agentd') or {}
    a, b = s.get('start') or {}, s.get('end') or {}
    if not b:
        return None
    dt = b['uptime_s'] - a.get('uptime_s', 0)
    pa, pb = a.get('persist', {}), b['persist']
    return {'reports': r1((b['messages'] - a.get('messages', 0)) / dt),
            'writes': r1((pb['writes'] - pa.get('writes', 0)) / dt),
            'kv_p50': pb['kv_set_ms']['p50'], 'handler_p50': b['handler_ms']['p50'],
            'handler_p99': b['handler_ms']['p99']}


# CPU and persistence cost at 4,000 hosts (B) and 1,000 hosts (A, B).
cost = []
for label, name in [('A  stock mgr, 1,000 hosts', 'A-agents-n1000'),
                    ('B1  sync writes, 1,000 hosts', 'B1-agents-n1000'),
                    ('B1  sync writes, 4,000 hosts', 'B1-agents-n4000'),
                    ('B2  write-back, 4,000 hosts', 'B2-agents-n4000'),
                    ('B3  changed only, 4,000 hosts', 'B3-agents-n4000')]:
    cpu = get(name, 'cpu') or {}
    rates = agentd_rates(name)
    cost.append({'label': label, 'reports': r1(get(name, 'ok_per_sec')),
                 'writes': rates['writes'] if rates else r1(3 * (get(name, 'ok_per_sec') or 0)),
                 'endpoint_cpu': r1(cpu.get('agentd') if rates else cpu.get('ceph-mgr'), 2),
                 'mon_cpu': r1(cpu.get('ceph-mon'), 2),
                 'p99': r1(get(name, 'latency', 'p99_ms'))})
data['cost'] = cost

# Full table of the agents-at-20s runs.
table = []
for v in ['A', 'Aplus', 'B1', 'B2', 'B3']:
    for n in HOSTS:
        name = agents_run(v, n)
        if not name:
            continue
        c = get(name, 'cpu') or {}
        table.append({'variant': v, 'run': name, 'hosts': n, 'offered': n / 20.0,
                      'ok': r1(get(name, 'ok_per_sec')), 'p50': r1(get(name, 'latency', 'p50_ms')),
                      'p99': r1(get(name, 'latency', 'p99_ms')), 'max': r1(get(name, 'latency', 'max_ms'), 0),
                      'down': (get(name, 'gaps') or {}).get('agents_over_3x_period'),
                      'mgr': r1(c.get('ceph-mgr'), 2), 'mon': r1(c.get('ceph-mon'), 2),
                      'agentd': r1(c.get('agentd'), 2)})
data['table'] = table
data['proto'] = proto

html = open(template).read()
html = html.replace('/*CHARTS_JS*/', open(charts).read())
html = html.replace('/*DATA*/', json.dumps(data, separators=(',', ':')))
open(out, 'w').write(html)
print(f'wrote {out} ({len(html)} bytes)', file=sys.stderr)
