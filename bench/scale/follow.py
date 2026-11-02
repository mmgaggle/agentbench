#!/usr/bin/env python3
"""
The agentd orchestrator module's read path (vm/ceph/mgr-agentd/module.py,
serve() and _update_health()), outside the mgr so its cost can be measured
at scale: poll agentd's event feed, re-read the status of every host that
changed (one request each, urllib, as the module does), re-read the
cluster summary on liveness/intent/applied events, and recompute the
health checks.

Writes one JSON line per poll that saw events (--out), with the poll's
wall time, CPU time, requests and the lag from agentd receiving an event to
this follower having absorbed it.

  --batch   one request for all changed hosts' statuses (POST
            /v1/hosts-status) and /v1/liveness instead of the full summary
"""
import argparse
import json
import signal
import sys
import time
import urllib.request

stop = False


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--url', default='http://127.0.0.1:7161')
    p.add_argument('--interval', type=float, default=1.0)
    p.add_argument('--out', required=True)
    p.add_argument('--batch', action='store_true')
    args = p.parse_args()

    def handler(*_):
        global stop
        stop = True
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)

    reqs = {'n': 0, 'bytes': 0}

    def http(path, data=None, method='GET'):
        req = urllib.request.Request(args.url + path, data=data, method=method)
        with urllib.request.urlopen(req, timeout=60) as r:
            body = r.read()
        reqs['n'] += 1
        reqs['bytes'] += len(body)
        return json.loads(body) if body else None

    def host_status(h):
        try:
            return (http(f'/v1/hosts/{h}/status') or {}).get('status')
        except Exception:
            return None

    def statuses(hosts):
        if args.batch:
            return http('/v1/hosts-status', json.dumps(sorted(hosts)).encode(), 'POST')
        return {h: host_status(h) for h in hosts}

    def summary():
        if args.batch:
            # only liveness is needed: the per-host statuses carry the rest
            return {h: {'liveness': {'state': v[0]} if v[0] else None} for h, v in http('/v1/liveness').items()}
        return {h: v for h, v in http('/v1/intent-status').items() if not h.startswith('member-')}

    status, summ, seq = {}, {}, 0
    health = (None, None)
    out = open(args.out, 'w')
    total = {'polls': 0, 'events': 0, 'cpu_s': 0.0, 'resyncs': 0, 'gaps': 0, 'errors': 0}
    resync = True
    while not stop:
        t0, c0, r0, b0 = time.time(), time.thread_time(), reqs['n'], reqs['bytes']
        rec = {}
        try:
            if resync:
                seq = http('/v1/events?since=18446744073709551615')['seq']
                summ = summary()
                status = {h: s for h, s in statuses(list(summ)).items() if s}
                total['resyncs'] += 1
                rec['resync'] = len(summ)
                resync = False
            ev = http(f'/v1/events?since={seq}')
            total['polls'] += 1
            if ev['seq'] < seq:
                resync = True
                continue
            events = ev['events']
            if seq + 1 < ev.get('first', 0) or (events and events[0]['seq'] != seq + 1):
                # the ring moved past us: events were lost
                total['gaps'] += 1
                rec['gap'] = (events[0]['seq'] if events else ev['first']) - seq - 1
                resync = True
            seq = ev['seq']
            if events:
                total['events'] += len(events)
                changed = {e['host'] for e in events if e.get('host') and not e['host'].startswith('member-')}
                fresh = statuses(changed)
                for h, s in fresh.items():
                    if s:
                        status[h] = s
                if any(e['kind'] in ('liveness', 'leaving', 'intent', 'applied') for e in events):
                    summ = summary()
                    rec['summary'] = True
                now = time.time_ns()
                lags = [now - e.get('received_ns', e.get('at_ns', now)) for e in events]
                rec.update(events=len(events), hosts=len(changed), lag_max_s=round(max(lags) / 1e9, 3))
            # _update_health
            failed = sum(1 for st in status.values() for u in st.get('units', {}).values()
                         if u['active_state'] == 'failed' or u.get('apply_error'))
            down = sum(1 for row in summ.values() if row.get('liveness') and row['liveness']['state'] != 'up')
            if (failed, down) != health:
                health = (failed, down)
                rec.update(failed=failed, down=down)
        except Exception as e:
            total['errors'] += 1
            rec['error'] = str(e)[:200]
            resync = True
        cpu = time.thread_time() - c0
        total['cpu_s'] += cpu
        if rec:
            rec.update(t=round(t0, 3), wall_s=round(time.time() - t0, 4), cpu_s=round(cpu, 4),
                       requests=reqs['n'] - r0, mb=round((reqs['bytes'] - b0) / 2**20, 2))
            out.write(json.dumps(rec) + '\n')
            out.flush()
        time.sleep(max(0.0, args.interval - (time.time() - t0)))
    total.update(requests=reqs['n'], mb=round(reqs['bytes'] / 2**20, 1), hosts=len(status))
    out.write(json.dumps({'total': total}) + '\n')
    out.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
