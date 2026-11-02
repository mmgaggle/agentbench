# applied_latency.py < events: for every applied generation, the time from
# agentd storing the intent to agentd receiving the agent's status that
# reports it applied (both agentd's clock).
#   curl -s 10.77.0.1:7161/v1/events | jq -c '.events[]' | python3 applied_latency.py
import json
import statistics
import sys

stored = {}
rows = []
for line in sys.stdin:
    e = json.loads(line)
    if e['kind'] == 'intent':
        stored[(e['host'], e['generation'])] = e['at_ns']
    elif e['kind'] == 'applied':
        t0 = stored.get((e['host'], e['generation']))
        if t0:
            rows.append((e['host'], e['generation'], (e['received_ns'] - t0) / 1e6))
for h, g, ms in rows:
    print(f'{h} generation {g}: {ms:8.1f} ms')
if rows:
    v = sorted(ms for _, _, ms in rows)
    print(f'n={len(v)} median {statistics.median(v):.1f} ms, max {v[-1]:.1f} ms')
