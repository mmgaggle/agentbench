#!/usr/bin/env python3
"""Per-thread CPU of a process over an interval: threadcpu.py <comm> [secs]"""
import os
import subprocess
import sys
import time

pid = int(subprocess.check_output(['pgrep', '-x', sys.argv[1]]).split()[0])
secs = float(sys.argv[2]) if len(sys.argv) > 2 else 10
clk = os.sysconf('SC_CLK_TCK')


def snap():
    d = {}
    for t in os.listdir(f'/proc/{pid}/task'):
        try:
            s = open(f'/proc/{pid}/task/{t}/stat').read()
        except OSError:
            continue
        r = s.rindex(')')
        f = s[r + 2:].split()
        d[t] = (s[s.index('(') + 1:r], int(f[11]) + int(f[12]))
    return d


a = snap()
time.sleep(secs)
b = snap()
rows = sorted(((b[t][1] - a[t][1]) / clk / secs, t, b[t][0]) for t in b if t in a)
print(f'pid {pid} total {sum(r[0] for r in rows):.2f} cores over {secs:.0f}s')
for cpu, tid, comm in rows[::-1][:12]:
    print(f'{cpu:6.2f}  {tid:>8}  {comm}')
