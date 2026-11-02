#!/usr/bin/env python3
"""
Per-thread CPU of the simulated agents (ceph-agent sim) every few seconds.
Each sim process runs all of its hosts' SWIM members on its own 'swim'
thread(s) and the agents on tokio workers, so a SWIM thread that can't keep
up shows here as ~1.0 core even while the machine has CPU to spare.

  threads.py --out DIR [--interval 5]     appends to DIR/threads.jsonl

Each line: ts, swim / agent / other cores summed over every sim process,
swim_max (the busiest SWIM thread), swim_busy (SWIM threads at >= 0.9),
per_proc [[swim, agent], ...] by pid.
"""
import argparse
import json
import os
import time

CLK = os.sysconf('SC_CLK_TCK')


def sim_pids():
    out = []
    for p in os.listdir('/proc'):
        if not p.isdigit():
            continue
        try:
            argv = open(f'/proc/{p}/cmdline', 'rb').read().split(b'\0')
        except OSError:
            continue
        if len(argv) > 1 and argv[0].endswith(b'ceph-agent') and argv[1] == b'sim':
            out.append(int(p))
    return sorted(out)


def thread_ticks(pid):
    """{tid: (comm, utime + stime)} for every thread of pid."""
    out = {}
    try:
        tids = os.listdir(f'/proc/{pid}/task')
    except OSError:
        return out
    for t in tids:
        try:
            comm = open(f'/proc/{pid}/task/{t}/comm').read().strip()
            st = open(f'/proc/{pid}/task/{t}/stat').read()
        except OSError:
            continue
        f = st[st.rindex(')') + 2:].split()
        out[int(t)] = (comm, int(f[11]) + int(f[12]))
    return out


def kind(comm):
    if comm.startswith('swim'):
        return 'swim'
    if comm.startswith('tokio'):
        return 'agent'
    return 'other'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--interval', type=float, default=5)
    args = ap.parse_args()
    prev, prev_t = {}, time.monotonic()
    with open(os.path.join(args.out, 'threads.jsonl'), 'a') as f:
        while True:
            time.sleep(args.interval)
            now = time.monotonic()
            dt = now - prev_t
            cur = {pid: thread_ticks(pid) for pid in sim_pids()}
            tot = {'swim': 0.0, 'agent': 0.0, 'other': 0.0}
            swim_threads, per_proc = [], []
            for pid, threads in cur.items():
                pp = {'swim': 0.0, 'agent': 0.0}
                for tid, (comm, ticks) in threads.items():
                    old = prev.get(pid, {}).get(tid)
                    if old is None:
                        continue
                    c = (ticks - old[1]) / CLK / dt
                    k = kind(comm)
                    tot[k] += c
                    if k in pp:
                        pp[k] += c
                    if k == 'swim':
                        swim_threads.append(c)
                if pid in prev:
                    per_proc.append([round(pp['swim'], 2), round(pp['agent'], 2)])
            prev, prev_t = cur, now
            if not per_proc:
                continue
            rec = {'ts': time.time(), **{k: round(v, 2) for k, v in tot.items()},
                   'swim_max': round(max(swim_threads, default=0), 2),
                   'swim_busy': sum(1 for c in swim_threads if c >= 0.9),
                   'swim_threads': len(swim_threads), 'per_proc': per_proc}
            f.write(json.dumps(rec) + '\n')
            f.flush()


if __name__ == '__main__':
    main()
