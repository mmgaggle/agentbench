#!/usr/bin/env python3
"""
Collect the agent-protocol experiments for the report:
  V-v1-n<N> / V-v2-n<N>  (bench/sweep_v2.sh)
  S-swim-n<N>            (bench/sweep_swim.sh)

  collect_proto.py /ab/results > proto.json
"""
import glob
import json
import os
import statistics
import sys

root = sys.argv[1]


def cpu(run, proc, t0=None, t1=None):
    vals = []
    try:
        for line in open(os.path.join(run, 'samples.jsonl')):
            r = json.loads(line)
            if (t0 is None or r['t'] >= t0) and (t1 is None or r['t'] <= t1) and proc in r['proc']:
                vals.append(r['proc'][proc]['cpu'])
    except OSError:
        pass
    return round(statistics.mean(vals), 3) if vals else None


out = {'v': [], 'swim': []}
for run in sorted(glob.glob(os.path.join(root, 'V-v*-n*')), key=lambda p: int(p.rsplit('n', 1)[1])):
    name = os.path.basename(run)
    proto, n = name.split('-')[1], int(name.rsplit('n', 1)[1])
    s = json.load(open(os.path.join(run, 'summary.json')))
    st = json.load(open(os.path.join(run, 'agentd-stats.json')))
    w = s['args']['warmup']
    v2 = s.get('v2') or {}
    lag = st['end']['v2']['change_lag_ms']
    out['v'].append({
        'proto': proto, 'hosts': n, 'requests_per_sec': round(s['ok_per_sec'], 1),
        'failed': sum(v for k, v in s['counts'].items() if k != 'ok'),
        'p50_ms': s['latency']['p50_ms'], 'p99_ms': s['latency']['p99_ms'],
        'agentd_cpu': cpu(run, 'agentd', w, w + s['args']['duration']),
        'bytes_per_agent_per_period': round(v2['bytes_per_agent_per_period']) if v2 else s['body_bytes'],
        'new_connections_per_sec': round((st['end']['connections'] - st['start']['connections'])
                                         / (st['end']['uptime_s'] - st['start']['uptime_s']), 1),
        'change_lag_p50_ms': lag['p50'] if proto == 'v2' else None,
        'change_lag_p99_ms': lag['p99'] if proto == 'v2' else None,
        'kinds': {k: round(v['per_sec'], 1) for k, v in v2.get('kinds', {}).items()} if v2 else None,
    })
for run in sorted(glob.glob(os.path.join(root, 'S-swim-n*')), key=lambda p: int(p.rsplit('n', 1)[1])):
    f = os.path.join(run, 'summary.json')
    if not os.path.exists(f):
        continue
    s = json.load(open(f))
    sig = s['signals']
    out['swim'].append({
        'members': s['agents'], 'converge_secs': round(s['converge_secs']),
        'member_pkts_per_sec': round(s['steady']['member_pkts_sent_per_sec'], 2),
        'member_bytes_per_sec': round(s['steady']['member_bytes_sent_per_sec']),
        'controller_pkts_per_sec': round(s['steady']['controller_pkts_recv_per_sec'] + s['steady']['controller_pkts_sent_per_sec'], 1),
        'controller_cpu': cpu(run, 'agentd'),
        'sim_cpu_per_member_ms': round(1000 * cpu(run, 'swimsim') / s['agents'], 3) if cpu(run, 'swimsim') else None,
        'signal_coverage': min(g['received'] / g['of'] for g in sig),
        'signal_p50_ms': round(statistics.median(g['latency_ms']['p50'] for g in sig)),
        'signal_p99_ms': round(statistics.median(g['latency_ms']['p99'] for g in sig)),
        'signal_max_ms': round(max(g['latency_ms']['max'] for g in sig)),
        'killed': s['detection']['killed'], 'undetected': s['detection']['undetected'],
        'false_down': s['detection']['false_down_during_test'] + s['steady']['controller_down_events'],
        'detect_p50_s': round(s['detection']['detect_secs']['p50'], 1),
        'detect_max_s': round(s['detection']['detect_secs']['max'], 1),
    })
json.dump(out, sys.stdout, indent=1)
