#!/usr/bin/env python3
"""
One row per run for a phase: hosts down (peak, mean over 60-300 s, at
300 s, from downs.py), rejoins in 300 s, SWIM threads pegged, the frozen
rack's detection and false downs, the rollout's wall time, and the gossip
rate 100 s in.

  meltdown_table.py PHASE RUN_DIR...
"""
import glob
import json
import os
import re
import subprocess
import sys

S = os.path.dirname(os.path.abspath(__file__))


def results(run):
    out = {}
    for line in open(os.path.join(run, 'results.jsonl')):
        try:
            d = json.loads(line)
        except ValueError:
            continue
        out[d.get('label')] = d
    return out


def gossip_at(run, phase, at=100):
    """pkts/s and MB/s sent by all members in the 10 s window ending `at` s
    after the phase started."""
    t0 = None
    for line in open(os.path.join(run, 'phases.txt')):
        f = line.split()
        if f[1] == phase:
            t0 = float(f[0])
    pk = by = 0.0
    for path in glob.glob(os.path.join(run, 'sim-*.out')):
        pts = []
        for line in open(path):
            m = re.match(r'(\d+) swim pkts_sent (\d+) pkts_recv \d+ bytes_sent (\d+)', line)
            if m:
                pts.append((int(m.group(1)) / 1000, int(m.group(2)), int(m.group(3))))
        a = [p for p in pts if p[0] <= t0 + at - 10]
        b = [p for p in pts if p[0] <= t0 + at]
        if a and b and b[-1][0] > a[-1][0]:
            dt = b[-1][0] - a[-1][0]
            pk += (b[-1][1] - a[-1][1]) / dt
            by += (b[-1][2] - a[-1][2]) / dt
    return round(pk), round(by / 1e6, 1)


def main():
    phase, runs = sys.argv[1], sys.argv[2:]
    kind = 'all' if 'all' in phase else 'paced'
    for run in runs:
        rows = json.loads(subprocess.check_output(
            [sys.executable, os.path.join(S, 'downs.py'), run, phase, '--bucket', '5', '--span', '300', '--json']))
        downs = [r['down'] for r in rows]
        mid = [r['down'] for r in rows if 60 <= r['t'] <= 300]
        pegged = [r.get('swim_busy') for r in rows if r['t'] >= 30 and 'swim_busy' in r]
        res = results(run)
        fz = res.get(f'freeze-during-{kind}', {})
        up = res.get(phase, {})
        print(json.dumps({
            'run': os.path.basename(run.rstrip('/')),
            'down_peak': max(downs), 'down_mean_60_300': round(sum(mid) / len(mid)) if mid else None,
            'down_at_300': downs[-1],
            'rejoins_300': rows[-1].get('rejoins'),
            'swim_threads_pegged_min_max': [min(pegged), max(pegged)] if pegged else None,
            'rack_first_p50_max': [fz.get('first_s'), fz.get('p50_s'), fz.get('max_s')],
            'rack_reached': fz.get('reached'), 'false_downs': fz.get('false_positives'),
            'wall_s': up.get('wall_s'),
            'gossip_100s_pkts_mb': gossip_at(run, phase),
        }))


if __name__ == '__main__':
    main()
