#!/usr/bin/env python3
"""
Summarizes results/scale/n*/ (run.sh) into one JSON document: per size and
phase, the mgr's and the hosts' timings, CPU of agentd / the simulated
agents / the follower, agentd memory, SWIM traffic, follower costs.

  analyze.py results/scale [run ...] > results/scale/summary.json
"""
import glob
import json
import os
import re
import sys

CLK = os.sysconf('SC_CLK_TCK')
PHASES = [('join', 'join-done'), ('start', 'start-done'), ('deploy', 'deploy-done'), ('steady', 'steady-done'),
          ('change', 'change-done'), ('freeze', 'thaw-done'), ('restart', 'restart-done')]


def jsonl(path):
    """The records of a JSON-lines file; a line still being written is skipped."""
    out = []
    for line in open(path):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def load_phases(d):
    ph = {}
    for line in open(os.path.join(d, 'phases.txt')):
        f = line.split()
        kv = dict(x.split('=') for x in f[2:] if '=' in x)
        ph[f[1]] = {'t': float(f[0]), 'agentd': int(kv.get('agentd_ticks', 0)), 'sim': int(kv.get('sim_ticks', 0))}
    return ph


def window(samples, a, b):
    return [s for s in samples if a <= s['ts'] <= b]


def proc_stats(ws, name):
    cpu = [s['proc'].get(name, {}).get('cpu', 0) for s in ws]
    rss = [s['proc'].get(name, {}).get('rss_mb', 0) for s in ws]
    if not cpu:
        return None
    return {'cpu_avg': round(sum(cpu) / len(cpu), 3), 'cpu_max': round(max(cpu), 2), 'rss_max_mb': round(max(rss))}


def swim_counters(d):
    """Per sim process: (wall ms, pkts_sent, pkts_recv, bytes_sent) every 10 s."""
    out = []
    for f in glob.glob(os.path.join(d, 'sim-*.out')):
        rows = []
        for line in open(f):
            m = re.match(r'(\d+) swim pkts_sent (\d+) pkts_recv (\d+) bytes_sent (\d+)', line)
            if m:
                rows.append(tuple(int(x) for x in m.groups()))
        out.append(rows)
    return out


def swim_rate(procs, a, b):
    """Packets/s and bytes/s sent by all simulated members over [a, b]."""
    tot = [0, 0, 0]
    span = 0
    for rows in procs:
        r = [x for x in rows if a * 1000 <= x[0] <= b * 1000]
        if len(r) < 2 or r[-1][0] == r[0][0]:
            continue
        dt = (r[-1][0] - r[0][0]) / 1000
        span = max(span, dt)
        for i in range(3):
            tot[i] += (r[-1][i + 1] - r[0][i + 1]) / dt
    return {'pkts_sent_s': round(tot[0]), 'pkts_recv_s': round(tot[1]), 'bytes_sent_s': round(tot[2])} if span else None


def follower(d, a, b, name='follow.jsonl'):
    recs = []
    if not os.path.exists(os.path.join(d, name)):
        return None
    for line in open(os.path.join(d, name)):
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if 't' in r and a <= r['t'] <= b:
            recs.append(r)
    if not recs:
        return None
    return {
        'polls_with_events': len(recs),
        'cpu_s': round(sum(r['cpu_s'] for r in recs), 2),
        'max_poll_wall_s': max(r['wall_s'] for r in recs),
        'max_lag_s': max((r.get('lag_max_s', 0) for r in recs), default=0),
        'requests': sum(r['requests'] for r in recs),
        'mb': round(sum(r['mb'] for r in recs), 1),
        'max_requests_per_poll': max(r['requests'] for r in recs),
        'gaps': sum(1 for r in recs if 'gap' in r),
        'errors': sum(1 for r in recs if 'error' in r),
        'resyncs': sum(1 for r in recs if 'resync' in r),
    }


def main() -> int:
    root = sys.argv[1]
    only = set(sys.argv[2:])
    out = {}
    dirs = [d for d in glob.glob(os.path.join(root, 'n*')) if os.path.isdir(d) and re.match(r'n\d+', os.path.basename(d))
            and (not only or os.path.basename(d) in only)]
    for d in sorted(dirs, key=lambda p: (int(re.match(r'n(\d+)', os.path.basename(p)).group(1)), p)):
        if not os.path.exists(os.path.join(d, 'phases.txt')):
            continue
        label = os.path.basename(d)
        n = int(re.match(r"n(\d+)", label).group(1))
        ph = load_phases(d)
        results = jsonl(os.path.join(d, 'results.jsonl')) if os.path.exists(os.path.join(d, 'results.jsonl')) else []
        samples = jsonl(os.path.join(d, 'samples.jsonl'))
        samples = [s for s in samples if 'ts' in s]
        swim = swim_counters(d)
        joins = []
        for f in glob.glob(os.path.join(d, 'join-*.log')):
            for line in open(f):
                m = re.match(r'joined (\d+)/(\d+) in ([\d.]+)s; per host p50 ([\d.]+)s p99 ([\d.]+)s max ([\d.]+)s', line)
                if m:
                    joins.append([float(x) for x in m.groups()])
        res = {'hosts': n, 'daemons': n * 14, 'results': {r['label']: r for r in results}}
        if joins:
            res['join'] = {'joined': int(sum(j[0] for j in joins)), 'wall_s': max(j[2] for j in joins),
                           'per_host_p50_s': max(j[3] for j in joins), 'per_host_max_s': max(j[5] for j in joins)}
        phases = {}
        pairs = list(PHASES) + [(k, k + '-done') for k in ph if k + '-done' in ph and k not in dict(PHASES)]
        for a, b in pairs:
            if a not in ph or b not in ph:
                continue
            ta, tb = ph[a]['t'], ph[b]['t']
            ws = window(samples, ta, tb)
            p = {'wall_s': round(tb - ta, 2)}
            # agentd restarts in 'restart': ticks don't span it
            if a != 'restart' and ph[b]['agentd'] >= ph[a]['agentd']:
                p['agentd_cpu_s'] = round((ph[b]['agentd'] - ph[a]['agentd']) / CLK, 2)
            if ph[a]['sim'] and ph[b]['sim'] >= ph[a]['sim']:
                p['agents_cpu_s'] = round((ph[b]['sim'] - ph[a]['sim']) / CLK, 2)
            for name in ('agentd', 'ceph-agent', 'follow.py', 'mgr.py', 'intentctl'):
                st = proc_stats(ws, name)
                if st and (st['cpu_max'] or st['rss_max_mb']):
                    p[name] = st
            sw = swim_rate(swim, ta, tb)
            if sw:
                p['swim'] = sw
            for key, name in (('follower', 'follow.jsonl'), ('follower_batch', 'follow-batch.jsonl')):
                fo = follower(d, ta, tb + (20 if a == 'change' else 0), name)
                if fo:
                    p[key] = fo
            phases[a] = p
        if 'steady' in phases:
            st = phases['steady']
            s = st['wall_s']
            st['per_agent'] = {
                'cpu_pct_of_core': round(st.get('agents_cpu_s', 0) / s / n * 100, 4),
                'swim_pkts_sent_s': round(st['swim']['pkts_sent_s'] / n, 2) if st.get('swim') else None,
                'swim_bytes_sent_s': round(st['swim']['bytes_sent_s'] / n) if st.get('swim') else None,
            }
            st['agentd_cores'] = round(st.get('agentd_cpu_s', 0) / s, 4)
        res['phases'] = phases
        st = os.path.join(d, 'agentd-stats.json')
        j = None
        if os.path.exists(st) and os.path.getsize(st):
            j = json.load(open(st))
        if j:
            res['agentd_stats'] = {k: j.get(k) for k in ('gossip', 'blobs', 'blob_bytes', 'blobs_unreferenced', 'blob_gc', 'rss_mb',
                                                         'connections_open', 'connections', 'tls_errors', 'intent_hosts')}
        out[label] = res
    json.dump(out, sys.stdout, indent=1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
