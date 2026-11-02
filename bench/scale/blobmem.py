#!/usr/bin/env python3
"""
agentd's memory per stored blob: PUT --count blobs of --size bytes over one
keep-alive connection (as mgr.py does), then report agentd's RSS growth
against the bytes stored.

  blobmem.py --pid $(pidof agentd) --count 100000 --size 1100
"""
import argparse
import http.client
import os
import time


def rss_mb(pid):
    for line in open(f'/proc/{pid}/status'):
        if line.startswith('VmRSS:'):
            return int(line.split()[1]) / 1024
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--admin', default='127.0.0.1:7161')
    p.add_argument('--pid', type=int, required=True)
    p.add_argument('--count', type=int, default=100000)
    p.add_argument('--size', type=int, default=1100)
    a = p.parse_args()
    host, port = a.admin.rsplit(':', 1)
    c = http.client.HTTPConnection(host, int(port))
    r0 = rss_mb(a.pid)
    t0 = time.time()
    for i in range(a.count):
        body = os.urandom(a.size // 2).hex().encode()[:a.size]
        c.request('PUT', '/v1/blobs', body=body)
        c.getresponse().read()
    time.sleep(1)
    r1 = rss_mb(a.pid)
    stored = a.count * a.size / 2**20
    print(f'{a.count} blobs of {a.size} B ({stored:.0f} MB) in {time.time() - t0:.1f}s: '
          f'agentd RSS {r0:.0f} -> {r1:.0f} MB (+{r1 - r0:.0f} MB, {(r1 - r0) * 2**20 / a.count:.0f} B per blob)')


if __name__ == '__main__':
    main()
