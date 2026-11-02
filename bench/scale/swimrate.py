#!/usr/bin/env python3
"""
SWIM packets/s and KB/s sent by all simulated members, per 10 s window,
relative to a phase stamp in phases.txt (run.sh): how much gossip a phase
caused and how long it took to quiet down.

  swimrate.py results/scale/n5000-upgrades agent-upgrade-all [--before 60] [--after 600]
"""
import argparse
import glob
import os
import re


def main():
    p = argparse.ArgumentParser()
    p.add_argument('run')
    p.add_argument('phase')
    p.add_argument('--before', type=int, default=60)
    p.add_argument('--after', type=int, default=600)
    a = p.parse_args()
    ph = {}
    for line in open(os.path.join(a.run, 'phases.txt')):
        f = line.split()
        ph[f[1]] = float(f[0])
    t0 = ph[a.phase]
    win = {}
    for f in glob.glob(os.path.join(a.run, 'sim-*.out')):
        prev = None
        for line in open(f):
            m = re.match(r'(\d+) swim pkts_sent (\d+) pkts_recv (\d+) bytes_sent (\d+)', line)
            if not m:
                continue
            t, ps, _, bs = (int(x) for x in m.groups())
            if prev and t > prev[0]:
                w = round((t / 1000 - t0) / 10) * 10
                dt = (t - prev[0]) / 1000
                c = win.setdefault(w, [0.0, 0.0])
                c[0] += (ps - prev[1]) / dt
                c[1] += (bs - prev[2]) / dt
            prev = (t, ps, bs)
    marks = {round((v - t0) / 10) * 10: k for k, v in ph.items()}
    for w in sorted(win):
        if -a.before <= w <= a.after:
            print(f't={w:+5d}s  pkts/s {win[w][0]:8.0f}  KB/s {win[w][1] / 1024:7.0f}  {marks.get(w, "")}')


if __name__ == '__main__':
    main()
