#!/usr/bin/env python3
"""
Sample CPU and RSS of the processes under test once per interval from
/proc: ceph-mgr, ceph-mon, the OSDs, agentd and agentsim. For ceph-mgr it
also records the busiest threads so CPU can be attributed (cheroot workers,
finisher, messenger, ...).
"""
import argparse
import json
import os
import signal
import sys
import time

CLK = os.sysconf('SC_CLK_TCK')
WATCH = ('ceph-mgr', 'ceph-mon', 'ceph-osd', 'agentd', 'agentsim', 'swimsim', 'ceph-agent', 'intentctl')
SCRIPTS = ('follow.py', 'mgr.py')


def read_stat(path):
    with open(path) as f:
        s = f.read()
    # comm may contain spaces; it is enclosed in parentheses
    r = s.rindex(')')
    comm = s[s.index('(') + 1:r]
    fields = s[r + 2:].split()
    return comm, int(fields[11]) + int(fields[12]), int(fields[21])  # utime+stime, rss pages


def procs():
    out = {}
    for pid in os.listdir('/proc'):
        if not pid.isdigit():
            continue
        try:
            comm, _, _ = read_stat(f'/proc/{pid}/stat')
            with open(f'/proc/{pid}/cmdline', 'rb') as f:
                cmd = f.read().split(b'\0')
        except (OSError, ValueError):
            continue
        exe = os.path.basename(cmd[0].decode(errors='replace')) if cmd and cmd[0] else comm
        if exe.startswith('python') and len(cmd) > 1 and os.path.basename(cmd[1].decode(errors='replace')) in SCRIPTS:
            out[int(pid)] = os.path.basename(cmd[1].decode())
            continue
        for w in WATCH:
            if exe == w or comm == w:
                ident = w
                if w == 'ceph-osd' and '-i' in [c.decode() for c in cmd]:
                    ident = 'ceph-osd'
                out[int(pid)] = ident
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--interval', type=float, default=1.0)
    p.add_argument('--out', required=True)
    p.add_argument('--top-threads', type=int, default=8)
    args = p.parse_args()

    stop = False

    def handler(*_):
        nonlocal stop
        stop = True
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    os.makedirs(args.out, exist_ok=True)
    f = open(os.path.join(args.out, 'samples.jsonl'), 'w')
    prev = {}
    prev_threads = {}
    t0 = time.time()
    page = os.sysconf('SC_PAGE_SIZE')
    pids = procs()
    last_scan = time.monotonic()
    while not stop:
        now = time.monotonic()
        if now - last_scan > 5:
            pids = procs()
            last_scan = now
        rec = {'t': round(time.time() - t0, 3), 'ts': round(time.time(), 3), 'proc': {}, 'mgr_threads': []}
        for pid, name in pids.items():
            try:
                _, ticks, rss = read_stat(f'/proc/{pid}/stat')
            except (OSError, ValueError):
                continue
            key = (pid, name)
            if key in prev:
                dt = now - prev[key][1]
                cpu = (ticks - prev[key][0]) / CLK / dt if dt > 0 else 0
                agg = rec['proc'].setdefault(name, {'cpu': 0.0, 'rss_mb': 0.0, 'n': 0})
                agg['cpu'] += round(cpu, 3)
                agg['rss_mb'] += round(rss * page / 2**20, 1)
                agg['n'] += 1
            prev[key] = (ticks, now)
            if name == 'ceph-mgr':
                threads = []
                try:
                    tids = os.listdir(f'/proc/{pid}/task')
                except OSError:
                    tids = []
                for tid in tids:
                    try:
                        tcomm, tticks, _ = read_stat(f'/proc/{pid}/task/{tid}/stat')
                    except (OSError, ValueError):
                        continue
                    tk = (pid, int(tid))
                    if tk in prev_threads:
                        dt = now - prev_threads[tk][1]
                        threads.append((round((tticks - prev_threads[tk][0]) / CLK / dt, 3), int(tid), tcomm))
                    prev_threads[tk] = (tticks, now)
                threads.sort(reverse=True)
                rec['mgr_threads'] = [{'tid': t, 'comm': c, 'cpu': u} for u, t, c in threads[:args.top_threads] if u > 0]
                rec['mgr_threads_total'] = len(tids)
        f.write(json.dumps(rec) + '\n')
        f.flush()
        time.sleep(max(0.0, args.interval - (time.monotonic() - now)))
    f.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
