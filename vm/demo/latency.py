# latency.py DAEMON T0_NS < events: unit transitions of DAEMON after T0
# (wall ns on the host where the event was caused), as the agent observed
# them (same host's clock) and as agentd received them.
import json
import sys

daemon, t0 = sys.argv[1], int(sys.argv[2])
for line in sys.stdin:
    e = json.loads(line)
    if e.get('daemon') != daemon:
        continue
    print(f"  {e['from']} -> {e['to']} result={e['result']} restarts={e['n_restarts']}: "
          f"agent saw it {(e['observed_ns'] - t0) / 1e6:.1f} ms after, agentd {(e['received_ns'] - t0) / 1e6:.1f} ms")
