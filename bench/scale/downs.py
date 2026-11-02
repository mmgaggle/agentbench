#!/usr/bin/env python3
"""
Hosts reported down after a phase starts, in buckets: the batch follower's
'down' count (follow-batch.jsonl), carried forward to each bucket's end,
next to the simulator's SWIM thread load (threads.jsonl) and its members'
counters (sim-*.out: renames and rejoins since the phase started, members
whose Lifeguard local health is above zero) when recorded.

  downs.py RUN_DIR PHASE [--bucket 5] [--span 300] [--json]
"""
import argparse
import glob
import json
import os
import re

SIM_LINE = re.compile(r'(\d+) swim pkts_sent (\d+) pkts_recv (\d+) bytes_sent (\d+) send_errors (\d+)'
                      r'(?: renames (\d+) rejoins (\d+))?(?: restored (\d+) health (-?\d+) unhealthy (-?\d+))?(?: suspicions (-?\d+))?')


def jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    for line in open(path):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def sim_counters(run):
    """Per sim process: sorted [(ts, {counter: value})]."""
    procs = []
    for path in sorted(glob.glob(os.path.join(run, 'sim-*.out'))):
        recs = []
        for line in open(path):
            m = SIM_LINE.match(line)
            if not m or m.group(6) is None:
                continue
            c = {'renames': int(m.group(6)), 'rejoins': int(m.group(7))}
            if m.group(8) is not None:
                c.update(restored=int(m.group(8)), health=int(m.group(9)), unhealthy=int(m.group(10)))
            if m.group(11) is not None:
                c['suspicions'] = int(m.group(11))
            recs.append((int(m.group(1)) / 1000, c))
        if recs:
            procs.append(recs)
    return procs


def at(procs, t):
    """Counters summed over processes, each at its last line by t."""
    out = {}
    for recs in procs:
        last = [c for ts, c in recs if ts <= t]
        for k, v in (last[-1] if last else {}).items():
            out[k] = out.get(k, 0) + v
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('run')
    ap.add_argument('phase')
    ap.add_argument('--bucket', type=float, default=5)
    ap.add_argument('--span', type=float, default=300)
    ap.add_argument('--json', action='store_true')
    args = ap.parse_args()
    t0 = None
    for line in open(os.path.join(args.run, 'phases.txt')):
        f = line.split()
        if f[1] == args.phase:
            t0 = float(f[0])
    if t0 is None:
        raise SystemExit(f'no phase {args.phase}')
    downs = sorted((r['t'], r['down']) for r in jsonl(os.path.join(args.run, 'follow-batch.jsonl')) if 'down' in r and 't' in r)
    thr = sorted((r['ts'], r) for r in jsonl(os.path.join(args.run, 'threads.jsonl')))
    procs = sim_counters(args.run)
    base = at(procs, t0)
    rows = []
    k = 0
    while k * args.bucket <= args.span:
        te = t0 + k * args.bucket
        d = [v for t, v in downs if t <= te]
        w = [r for t, r in thr if te - args.bucket < t <= te]
        row = {'t': round(k * args.bucket), 'down': d[-1] if d else 0}
        if w:
            row['swim'] = round(sum(r['swim'] for r in w) / len(w), 1)
            row['swim_max'] = max(r['swim_max'] for r in w)
            row['swim_busy'] = max(r['swim_busy'] for r in w)
            row['agent'] = round(sum(r['agent'] for r in w) / len(w), 1)
        c = at(procs, te)
        if c:
            row['renames'] = c['renames'] - base.get('renames', 0)
            row['rejoins'] = c['rejoins'] - base.get('rejoins', 0)
            for name in ('restored', 'unhealthy', 'suspicions'):
                if name in c:
                    row[name] = c[name]
        rows.append(row)
        k += 1
    if args.json:
        print(json.dumps(rows))
        return
    peak = max(rows, key=lambda r: r['down'])
    print(f"{args.run} {args.phase}: peak {peak['down']} down at {peak['t']} s; at {rows[-1]['t']} s: {rows[-1]['down']}")
    for r in rows:
        extra = ''
        if 'swim' in r:
            extra = f"  swim {r['swim']:5.1f} cores, busiest thread {r['swim_max']:.2f}, {r['swim_busy']} threads >= 0.9; agents {r['agent']:.1f}"
        if 'rejoins' in r:
            extra += f"; rejoins {r['rejoins']}"
        if 'unhealthy' in r:
            extra += f", unhealthy {r['unhealthy']}, suspicions {r.get('suspicions', 0)}"
        print(f"{r['t']:5d} s {r['down']:6d} down{extra}")


if __name__ == '__main__':
    main()
