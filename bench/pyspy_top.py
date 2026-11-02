#!/usr/bin/env python3
"""Summarize a py-spy --format raw profile: inclusive and self share per
function (file:function), as a percentage of all samples."""
import collections
import sys

incl, self_, total = collections.Counter(), collections.Counter(), 0
for line in open(sys.argv[1]):
    stack, _, n = line.rstrip().rpartition(' ')
    if not n.isdigit():
        continue
    n = int(n)
    total += n
    frames = []
    for f in stack.split(';'):
        # "func (path/file.py:123)" -> "func (path/file.py)"
        name, _, loc = f.partition(' (')
        frames.append(f'{name} ({loc.split(":")[0]})' if loc else name)
    self_[frames[-1]] += n
    for f in set(frames):
        incl[f] += n
print(f'total GIL samples {total}')
print('\n== inclusive ==')
for f, n in incl.most_common(50):
    print(f'{100 * n / total:5.1f}%  {f}')
print('\n== self ==')
for f, n in self_.most_common(30):
    print(f'{100 * n / total:5.1f}%  {f}')
