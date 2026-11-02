#!/usr/bin/env python3
"""
Waits for a cluster-wide condition at agentd and reports how long it took.

  until.py counts --reported N --up N
      every host has reported status and is up in SWIM (polls /v1/liveness)
  until.py liveness --hosts-file F --state down|up --t0-ns T
      every host in F reached the state (from agentd's liveness events);
      reports each host's latency from T and the other hosts that changed
      liveness meanwhile (false positives)
  until.py gc
      agentd has dropped every unreferenced blob (blob_grace_s after the
      intents stopped naming them); reports its blob store and RSS before
      and after
"""
import argparse
import http.client
import json
import sys
import time


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('mode', choices=['counts', 'liveness', 'gc'])
    p.add_argument('--admin', default='127.0.0.1:7161')
    p.add_argument('--reported', type=int, default=0)
    p.add_argument('--up', type=int, default=0)
    p.add_argument('--hosts-file')
    p.add_argument('--state', choices=['down', 'up'])
    p.add_argument('--t0-ns', type=int, default=0)
    p.add_argument('--timeout', type=float, default=600)
    p.add_argument('--result')
    p.add_argument('--label', default='')
    args = p.parse_args()

    host, port = args.admin.rsplit(':', 1)
    c = http.client.HTTPConnection(host, int(port), timeout=30)

    def get(path):
        for _ in range(100):
            try:
                c.request('GET', path)
                r = c.getresponse()
                return json.loads(r.read())
            except (OSError, http.client.HTTPException):
                c.close()
                time.sleep(0.2)
        raise RuntimeError(f'agentd unreachable for {path}')

    t0 = args.t0_ns / 1e9 if args.t0_ns else time.time()
    res = {'label': args.label, 'mode': args.mode}
    if args.mode == 'gc':
        pick = lambda st: {k: st.get(k) for k in ('blobs', 'blob_bytes', 'blobs_unreferenced', 'rss_mb', 'blob_gc')}
        before = pick(get('/stats'))
        while True:
            st = get('/stats')
            if not st.get('blobs_unreferenced'):
                break
            if time.time() - t0 > args.timeout:
                res['timeout'] = True
                break
            time.sleep(1)
        res.update(elapsed_s=round(time.time() - t0, 1), before=before, after=pick(st))
    elif args.mode == 'counts':
        timeline = []
        while True:
            m = get('/v1/liveness')
            reported = sum(1 for v in m.values() if v[1])
            up = sum(1 for v in m.values() if v[0] == 'up')
            el = time.time() - t0
            if not timeline or timeline[-1][1:] != [reported, up]:
                timeline.append([round(el, 2), reported, up])
            if reported >= args.reported and up >= args.up:
                break
            if el > args.timeout:
                res['timeout'] = True
                break
            time.sleep(0.5)
        res.update(elapsed_s=round(time.time() - t0, 2), reported=reported, up=up, timeline=timeline[-30:])
    else:
        want = set(open(args.hosts_file).read().split())
        seq = get('/v1/events?since=18446744073709551615')['seq']
        # hosts already in the state count as reached at T
        reached = {h: 0.0 for h, v in get('/v1/liveness').items() if h in want and (v[0] == 'up') == (args.state == 'up')}
        others = {}
        while len(reached) < len(want) and time.time() - t0 < args.timeout:
            ev = get(f'/v1/events?since={seq}')
            for e in ev['events']:
                if e['kind'] != 'liveness':
                    continue
                h, st = e['host'], e['state']
                if h in want:
                    if (st == 'up') == (args.state == 'up') and h not in reached:
                        reached[h] = e['at_ns'] / 1e9 - t0
                elif st != 'up':
                    others[h] = st
            seq = ev['seq']
            time.sleep(0.2)
        lat = sorted(reached.values())
        qq = lambda x: round(lat[min(len(lat) - 1, int(len(lat) * x))], 2) if lat else None
        res.update(hosts=len(want), reached=len(reached), first_s=qq(0), p50_s=qq(.5), max_s=qq(1),
                   false_positives=len(others), false_positive_hosts=sorted(others)[:20])
    print(json.dumps(res), flush=True)
    if args.result:
        with open(args.result, 'a') as f:
            f.write(json.dumps(res) + '\n')
    return 1 if res.get('timeout') or res.get('reached', 0) < res.get('hosts', 0) else 0


if __name__ == '__main__':
    sys.exit(main())
