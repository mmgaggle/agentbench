#!/usr/bin/env python3
"""
Takes CPUs away for a while: PER_CPU busy loops pinned to each of CPUS, at
the same priority as everything else there, so what already runs on them
keeps about 1 / (PER_CPU + 1) of each. Exits when done.

  hog.py --cpus 4-15,20-31 [--per-cpu 2] --seconds 70
"""
import argparse
import os
import signal
import time


def cpu_list(spec):
    out = []
    for part in spec.split(','):
        a, _, b = part.partition('-')
        out.extend(range(int(a), int(b or a) + 1))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cpus', required=True)
    ap.add_argument('--per-cpu', type=int, default=2)
    ap.add_argument('--seconds', type=float, required=True)
    args = ap.parse_args()
    kids = []
    for cpu in cpu_list(args.cpus):
        for _ in range(args.per_cpu):
            pid = os.fork()
            if pid == 0:
                os.sched_setaffinity(0, {cpu})
                while True:
                    pass
            kids.append(pid)
    try:
        time.sleep(args.seconds)
    finally:
        for pid in kids:
            os.kill(pid, signal.SIGKILL)
        for pid in kids:
            os.waitpid(pid, 0)


if __name__ == '__main__':
    main()
