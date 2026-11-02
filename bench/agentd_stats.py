#!/usr/bin/env python3
"""Per-run agentd counters over the measured window (agentd-stats.json):
reports/s, config-key writes/s, writes skipped by the `changed` policy,
write-back queue at the end, kv set and handler latency."""
import glob
import json
import os
import sys

root = sys.argv[1] if len(sys.argv) > 1 else '/ab/results'
for d in sorted(glob.glob(os.path.join(root, 'B*'))):
    try:
        s = json.load(open(os.path.join(d, 'agentd-stats.json')))
    except (OSError, ValueError):
        continue
    a, b = s.get('start') or {}, s.get('end') or {}
    if not b:
        continue
    dt = b['uptime_s'] - a.get('uptime_s', 0)
    pa, pb = a.get('persist', {}), b['persist']
    rate = lambda k, x, y: (y[k] - x.get(k, 0)) / dt
    print(f"{os.path.basename(d):22} reports/s={rate('messages', a, b):8.1f} "
          f"writes/s={rate('writes', pa, pb):8.1f} skipped/s={rate('skipped', pa, pb):8.1f} "
          f"queued={pb['queued']:5} kv_set p50/p99={pb['kv_set_ms']['p50']:.1f}/{pb['kv_set_ms']['p99']:.1f}ms "
          f"handler p50/p99={b['handler_ms']['p50']:.2f}/{b['handler_ms']['p99']:.2f}ms "
          f"errors={pb['errors']} failed={b['failed']} bad={b['bad_metadata']}")
