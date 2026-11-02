#!/usr/bin/env python3
"""
The mgr side of the agent scale test, in Python as cephadm would be: renders,
seals and signs per-host intents for simulated storage hosts, publishes them
to agentd, then follows agentd's event feed until every host has applied
them.

Each host runs 12 OSDs, crash and node-exporter (14 units). Every daemon's
keyring is sealed to the host's key, every daemon has its own Quadlet file,
ceph.conf is one blob shared by all hosts, and the ceph daemons restart when
ceph.conf changes (restart_on).

  mgr.py deploy         first intents (seals every keyring)
  mgr.py change         a new ceph.conf: all ceph daemons on all hosts restart
  mgr.py republish      after an agentd restart: blobs and intents again
  mgr.py ceph-upgrade   a new image: every daemon's Quadlet file changes (the
                        old ones become garbage in agentd) and every daemon
                        restarts
  mgr.py agent-upgrade  a new agent binary, rolled out canary first, then at
                        --rate hosts/s with at most --max-in-flight hosts
                        between "given the new binary" and "running it";
                        halts after --max-failures failed hosts

Writes one JSON result line (--result) with the mgr's publish cost and the
hosts' time to applied.
"""
import argparse
import base64
import random
import hashlib
import http.client
import json
import multiprocessing
import os
import sys
import time

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

OSDS_PER_HOST = 12
IMAGE = 'quay.io/ceph/ceph:v20.2.4'


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def seal(recipient: bytes, plaintext: bytes) -> bytes:
    """proto::seal: CSEAL1 || ephemeral X25519 public || ChaCha20-Poly1305."""
    eph = X25519PrivateKey.generate()
    eph_pub = eph.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    shared = eph.exchange(X25519PublicKey.from_public_bytes(recipient))
    key = HKDF(algorithm=hashes.SHA256(), length=32, salt=eph_pub + recipient, info=b'ceph-agent seal v1').derive(shared)
    return b'CSEAL1' + eph_pub + ChaCha20Poly1305(key).encrypt(b'\0' * 12, plaintext, None)


class Agentd:
    """agentd's admin port over one keep-alive connection."""

    def __init__(self, addr: str):
        host, port = addr.rsplit(':', 1)
        self.c = http.client.HTTPConnection(host, int(port), timeout=60)
        self.requests = 0
        self.bytes = 0

    def call(self, method: str, path: str, body: bytes = b'') -> bytes:
        for attempt in range(3):
            try:
                self.c.request(method, path, body=body or None)
                r = self.c.getresponse()
                data = r.read()
                break
            except (http.client.HTTPException, OSError):
                self.c.close()
                if attempt == 2:
                    raise
        self.requests += 1
        self.bytes += len(body) + len(data)
        if r.status != 200:
            raise RuntimeError(f'{method} {path}: {r.status} {data[:200]!r}')
        return data

    def json(self, method: str, path: str, body: bytes = b''):
        return json.loads(self.call(method, path, body))

    def put_blob(self, data: bytes) -> str:
        s = self.json('PUT', '/v1/blobs', data)['sha256']
        assert s == sha(data), 'agentd stored the blob under another hash'
        return s


def conf_text(fsid: str, version: int) -> bytes:
    return (f'# minimal ceph.conf for {fsid}, version {version}\n[global]\n\tfsid = {fsid}\n'
            f'\tmon_host = [v2:10.0.0.1:3300/0,v1:10.0.0.1:6789/0] [v2:10.0.0.2:3300/0,v1:10.0.0.2:6789/0] '
            f'[v2:10.0.0.3:3300/0,v1:10.0.0.3:6789/0]\n\tosd_memory_target = {4 << 30}\n').encode()


def agent_binary(version: str, size: int) -> bytes:
    """The simulated agent binary: the same bytes for the same version."""
    return random.Random(version).randbytes(size)


def quadlet(fsid: str, daemon: str, image: str = IMAGE) -> bytes:
    dtype, _, did = daemon.partition('.')
    data = f'/var/lib/ceph/{fsid}/{daemon}'
    if dtype == 'node-exporter':
        return (f'[Unit]\nDescription=node-exporter for {fsid}\n\n[Container]\nImage=quay.io/prometheus/node-exporter:v1.7.0\n'
                f'ContainerName=ceph-{fsid}-{daemon}\nNetwork=host\nVolume=/proc:/host/proc:ro\nVolume=/sys:/host/sys:ro\n'
                f'Exec=--no-collector.timex --web.listen-address=:9100 --path.procfs=/host/proc --path.sysfs=/host/sys\n\n'
                f'[Service]\nRestart=on-failure\n\n[Install]\nWantedBy=ceph-{fsid}.target\n').encode()
    args = {'osd': f'-n osd.{did} -f --setuser ceph --setgroup ceph --default-log-to-file=false --default-log-to-stderr=true',
            'crash': f'-n client.crash.{did}'}[dtype]
    entry = {'osd': '/usr/bin/ceph-osd', 'crash': '/usr/bin/ceph-crash'}[dtype]
    extra = 'PodmanArgs=--ipc=host --pids-limit=0\nAddDevice=/dev/sdx\n' if dtype == 'osd' else ''
    return (f'[Unit]\nDescription=Ceph {daemon} for {fsid}\nAfter=network-online.target\n\n[Container]\nImage={image}\n'
            f'ContainerName=ceph-{fsid}-{daemon}\nNetwork=host\nRunInit=true\nEntrypoint={entry}\nExec={args}\n'
            f'Volume={data}:/var/lib/ceph/{dtype}/ceph-{did}:z\nVolume=/etc/ceph/ceph.conf:/etc/ceph/ceph.conf:ro\n'
            f'Volume=/dev:/dev\nVolume=/run/udev:/run/udev\nVolume=/var/log/ceph/{fsid}:/var/log/ceph:z\n'
            f'Environment=CONTAINER_IMAGE={image}\nEnvironment=NODE_NAME={daemon}\n{extra}'
            f'\n[Service]\nRestart=on-failure\nRestartSec=10s\nTimeoutStartSec=200\nLimitNOFILE=1048576\n\n'
            f'[Install]\nWantedBy=ceph-{fsid}.target\n').encode()


def daemons(host: str, i: int):
    return [f'osd.{i * OSDS_PER_HOST + j}' for j in range(OSDS_PER_HOST)] + [f'crash.{host}', f'node-exporter.{host}']


def intent(fsid, host, gen, conf_sha, keyrings, quadlets, image=IMAGE, agent=None):
    files = {'/etc/ceph/ceph.conf': {'sha256': conf_sha, 'mode': '0644'}}
    units = {}
    for d, q in quadlets.items():
        if d in keyrings:
            files[f'/var/lib/ceph/{fsid}/{d}/keyring'] = {'sha256': keyrings[d], 'mode': '0600', 'sealed': True}
        key = q + '\n' + (conf_sha + '\n' if not d.startswith('node-exporter') else '')
        units[d] = {'quadlet': q, 'state': 'running', 'restart_key': sha(key.encode())[:16]}
    it = {'host': host, 'fsid': fsid, 'generation': gen, 'mode': 'normal', 'prefetch': [image],
          'files': files, 'units': units, 'remove': []}
    if agent:
        it['agent'] = agent
    return it


class Signer:
    def __init__(self, seed_b64: str):
        self.key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(seed_b64.strip()))
        pub = self.key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        self.key_id = sha(pub)[:16]

    def envelope(self, it) -> bytes:
        payload = json.dumps(it, separators=(',', ':')).encode()
        return json.dumps({'host': it['host'], 'generation': it['generation'], 'key_id': self.key_id,
                           'payload': b64(payload), 'sig': b64(self.key.sign(payload))}).encode()


def publish(job):
    """Worker: publish job['hosts']. mode 'seal': seal and upload every
    per-host blob; 'quadlets': render and upload the Quadlet files again
    (new image), keep the keyrings; 'cached': reuse what was uploaded."""
    a = Agentd(job['admin'])
    signer = Signer(job['key'])
    fsid, conf_sha, mode, image = job['fsid'], job['conf_sha'], job['mode'], job['image']
    out = {}
    c0 = time.process_time()
    for i, host, gen, seal_pub, cached in job['hosts']:
        keyrings, quadlets = (cached or {}).get('keyrings'), (cached or {}).get('quadlets')
        if mode in ('seal', 'quadlets'):
            quadlets = {d: a.put_blob(quadlet(fsid, d, image)) for d in daemons(host, i)}
        if mode == 'seal':
            pub = base64.b64decode(seal_pub)
            keyrings = {}
            for d in daemons(host, i):
                if not d.startswith('node-exporter'):
                    secret = b64(os.urandom(32))
                    name = d if d.startswith('osd.') else f'client.{d}'
                    keyrings[d] = a.put_blob(seal(pub, f'[{name}]\n\tkey = {secret}\n'.encode()))
        agent = (cached or {}).get('agent')
        a.call('PUT', f'/v1/hosts/{host}/intent',
               signer.envelope(intent(fsid, host, gen, conf_sha, keyrings, quadlets, image, agent)))
        out[host] = {'keyrings': keyrings, 'quadlets': quadlets, 'agent': agent}
    return out, time.process_time() - c0, a.requests, a.bytes


def wait_applied(a: Agentd, targets: dict, since: int, t_pub_ns: int, timeout: float):
    """Follows the event feed until every host reports its target generation."""
    pending = dict(targets)
    lat_stored, lat_pub = [], []
    seen = {'events': 0, 'unit': 0, 'applied': 0, 'gaps': 0, 'polls': 0}
    t0 = time.time()
    while pending and time.time() - t0 < timeout:
        ev = a.json('GET', f'/v1/events?since={since}')
        seen['polls'] += 1
        evs = ev['events']
        if evs and evs[0]['seq'] != since + 1:
            seen['gaps'] += 1
        for e in evs:
            seen['events'] += 1
            k = e['kind']
            if k == 'unit':
                seen['unit'] += 1
            elif k == 'applied':
                seen['applied'] += 1
                h = e['host']
                if h in pending and e['generation'] >= pending[h]:
                    del pending[h]
                    lat_stored.append((e['received_ns'] - e['intent_stored_ns']) / 1e9)
                    lat_pub.append((e['received_ns'] - t_pub_ns) / 1e9)
        since = ev['seq']
        if seen['gaps'] and pending:
            # events overran agentd's ring: settle from the summary
            for h, row in a.json('GET', '/v1/intent-status').items():
                if h in pending and (row.get('applied') or 0) >= pending[h]:
                    del pending[h]
        time.sleep(0.1)
    return pending, lat_stored, lat_pub, seen, since


def q(v, x):
    if not v:
        return None
    v = sorted(v)
    return round(v[min(len(v) - 1, int(len(v) * x))], 3)


def statuses(a: Agentd, hosts, chunk=2000):
    out = {}
    for k in range(0, len(hosts), chunk):
        out.update(a.json('POST', '/v1/hosts-status', json.dumps(hosts[k:k + chunk]).encode()))
    return out


def rollout(args, a: Agentd, cache, hosts, conf_sha, image, key):
    """Agent upgrade, the way the mgr would pace it. A host is done when
    agentd hears from an agent process running the new binary; failed
    when an agent process starts on another binary (rolled back), or after
    --host-timeout without the new one. Resumable: hosts already running
    the target count as done."""
    blob = agent_binary(args.version, args.size)
    target = sha(blob)
    a.put_blob(blob)
    cache.setdefault('agent_binaries', {})[target] = {'version': args.version, 'size': args.size}
    spec = {'version': args.version, 'sha256': target}
    signer = Signer(key)
    fsid, gens, cached = cache['fsid'], cache['generations'], cache['hosts']

    since = a.json('GET', '/v1/events?since=18446744073709551615')['seq']
    st = statuses(a, hosts)
    done = {h for h in hosts if (st.get(h) or {}).get('agent_sha256') == target}
    todo = [h for h in hosts if h not in done]
    # canaries spread over the host list (racks, in a real cluster)
    canary = todo[::max(1, len(todo) // args.canary)][:args.canary] if args.canary else []
    queue = canary + [h for h in todo if h not in set(canary)]
    in_flight, failed, lat = {}, {}, []
    down_events, down_hosts, agent_events = 0, set(), 0
    max_in_flight, publish_s = 0, 0.0
    t0 = time.time()
    phase = 'canary' if canary else 'main'
    soak_until = None
    tokens, last = 1.0, time.time()
    n_canary = len(canary)
    halted = None

    def publish_one(h):
        nonlocal publish_s
        tp = time.perf_counter()
        gens[h] += 1
        c = dict(cached[h], agent=spec)
        a.call('PUT', f'/v1/hosts/{h}/intent',
               signer.envelope(intent(fsid, h, gens[h], conf_sha, c['keyrings'], c['quadlets'], image, spec)))
        cached[h] = c
        in_flight[h] = time.time_ns()
        publish_s += time.perf_counter() - tp

    while (queue or in_flight) and time.time() - t0 < args.timeout:
        now = time.time()
        if halted is None:
            if phase == 'canary':
                if not in_flight and not soak_until and queue[:n_canary] == canary:
                    for h in canary:
                        publish_one(h)
                    del queue[:n_canary]
                elif not in_flight and not soak_until:
                    soak_until = now + args.soak
                elif soak_until and now >= soak_until:
                    phase, tokens, last = 'main', 1.0, now
            else:
                if args.rate <= 0:
                    budget = len(queue)
                else:
                    tokens = min(max(1.0, args.rate), tokens + (now - last) * args.rate)
                    budget = int(tokens)
                last = now
                n = 0
                limit = args.max_in_flight or len(hosts)
                while queue and n < budget and len(in_flight) < limit:
                    publish_one(queue.pop(0))
                    n += 1
                if args.rate > 0:
                    tokens -= n
        max_in_flight = max(max_in_flight, len(in_flight))

        ev = a.json('GET', f'/v1/events?since={since}')
        for e in ev['events']:
            k, h = e['kind'], e.get('host')
            if k == 'liveness' and e['state'] == 'down':
                down_events += 1
                down_hosts.add(h)
            elif k == 'agent' and h in in_flight:
                agent_events += 1
                if e['sha256'] == target:
                    lat.append((e['received_ns'] - in_flight.pop(h)) / 1e9)
                    done.add(h)
                elif any('agent upgrade' in x or 'rolled back' in x for x in e.get('errors') or []):
                    # a new process on another binary that says why: the
                    # new one was rolled back (any other restart just
                    # picks the upgrade up)
                    in_flight.pop(h)
                    failed[h] = 'agent restarted on ' + (e['sha256'] or '?')[:12] + ': ' + '; '.join(e['errors'])
        since = ev['seq']
        for h, tp in list(in_flight.items()):
            if time.time_ns() - tp > args.host_timeout * 1e9:
                in_flight.pop(h)
                errs = ((statuses(a, [h]).get(h) or {}).get('errors')) or []
                failed[h] = f'not running the new agent after {args.host_timeout}s: ' + '; '.join(errs)
        if failed and halted is None and (phase == 'canary' or len(failed) > args.max_failures):
            halted = f'{len(failed)} failed host(s) during {phase}'
            print(f'agent-upgrade: halting: {halted}', flush=True)
            for h, why in list(failed.items())[:5]:
                print(f'  {h}: {why}', flush=True)
        if halted and not in_flight:
            break
        time.sleep(0.1)

    wall = time.time() - t0
    # restarted members are suspected only after SWIM's suspicion timeout:
    # keep counting down events for a while after the last host is done
    t_end = time.time()
    while time.time() - t_end < args.watch_after:
        ev = a.json('GET', f'/v1/events?since={since}')
        for e in ev['events']:
            if e['kind'] == 'liveness' and e['state'] == 'down':
                down_events += 1
                down_hosts.add(e['host'])
        since = ev['seq']
        time.sleep(0.5)
    return {
        'version': args.version, 'sha256': target[:16], 'binary_bytes': args.size,
        'rate': args.rate, 'canary': n_canary, 'soak_s': args.soak, 'max_in_flight_limit': args.max_in_flight,
        'wall_s': round(wall, 2), 'upgraded': len(done), 'failed': len(failed), 'halted': halted,
        'not_started': len(queue), 'still_in_flight': len(in_flight), 'max_in_flight': max_in_flight,
        'mgr_publish_s': round(publish_s, 2),
        'host_upgrade_s': {'p50': q(lat, .5), 'p90': q(lat, .9), 'p99': q(lat, .99), 'max': q(lat, 1)},
        'liveness_down_events': down_events, 'hosts_seen_down': len(down_hosts), 'watched_after_s': args.watch_after,
        'agent_events': agent_events,
        'failures': dict(list(failed.items())[:20]),
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('cmd', choices=['deploy', 'change', 'republish', 'ceph-upgrade', 'agent-upgrade'])
    p.add_argument('--admin', default='127.0.0.1:7161')
    p.add_argument('--state', required=True, help="intentctl's mgr.json (hosts' sealing keys)")
    p.add_argument('--key', required=True, help='intent signing key (base64 seed)')
    p.add_argument('--cache', required=True, help="this mgr's own state: blobs, generations")
    p.add_argument('--procs', type=int, default=1)
    p.add_argument('--timeout', type=float, default=600)
    p.add_argument('--result', help='append a JSON result line here')
    p.add_argument('--label', default='')
    p.add_argument('--image', help='ceph-upgrade: the new image (default: the next v20.2.x)')
    g = p.add_argument_group('agent-upgrade')
    g.add_argument('--version', default='0.2.6-sim')
    g.add_argument('--size', type=int, default=8 << 20, help='binary size (the stripped agent: 8 MB)')
    g.add_argument('--rate', type=float, default=10, help='hosts/s after the canaries (0: all at once)')
    g.add_argument('--canary', type=int, default=10)
    g.add_argument('--soak', type=float, default=10, help='s to wait after the canaries')
    g.add_argument('--max-in-flight', type=int, default=100, help='0: no limit')
    g.add_argument('--max-failures', type=int, default=0)
    g.add_argument('--host-timeout', type=float, default=180)
    g.add_argument('--watch-after', type=float, default=60,
                   help='s to keep counting liveness down events after the last host is done')
    args = p.parse_args()

    mgr = json.load(open(args.state))
    hosts = sorted(mgr['hosts'])
    cache = json.load(open(args.cache)) if os.path.exists(args.cache) else {}
    fsid = cache.setdefault('fsid', '5c2e6a6e-6f4b-4c43-9a7e-0b5d5c2a7e11')
    gens = cache.setdefault('generations', {})
    blobs = cache.setdefault('hosts', {})
    if args.cmd == 'change':
        cache['conf_version'] = cache.get('conf_version', 1) + 1
    if args.cmd == 'ceph-upgrade':
        cache['image_patch'] = cache.get('image_patch', 4) + 1
        cache['image'] = args.image or f"quay.io/ceph/ceph:v20.2.{cache['image_patch']}"
    conf_version = cache.setdefault('conf_version', 1)
    image = cache.setdefault('image', IMAGE)
    key = open(args.key).read()

    def save():
        json.dump(cache, open(args.cache + '.tmp', 'w'))
        os.replace(args.cache + '.tmp', args.cache)

    a = Agentd(args.admin)
    if args.cmd == 'agent-upgrade':
        conf_sha = a.put_blob(conf_text(fsid, conf_version))
        res = rollout(args, a, cache, hosts, conf_sha, image, key)
        save()
        res = {'label': args.label, 'cmd': args.cmd, 'hosts': len(hosts), **res}
        print(json.dumps(res), flush=True)
        if args.result:
            with open(args.result, 'a') as f:
                f.write(json.dumps(res) + '\n')
        return 0 if not res['halted'] and res['upgraded'] == len(hosts) else 1

    since = a.json('GET', '/v1/events?since=18446744073709551615')['seq']
    t_pub_ns = time.time_ns()
    t0 = time.perf_counter()
    c0 = time.process_time()
    conf_sha = a.put_blob(conf_text(fsid, conf_version))
    if args.cmd == 'republish':
        # agentd forgot everything: the agent binaries the intents name too
        for _, b in cache.get('agent_binaries', {}).items():
            a.put_blob(agent_binary(b['version'], b['size']))
    mode = {'deploy': 'seal', 'republish': 'seal', 'change': 'cached', 'ceph-upgrade': 'quadlets'}[args.cmd]
    rows = []
    for h in hosts:
        gens[h] = gens.get(h, 0) + 1
        i = int(h.lstrip('h'))
        rows.append((i, h, gens[h], mgr['hosts'][h]['seal_pub'], blobs.get(h)))
    jobs = [{'admin': args.admin, 'key': key, 'fsid': fsid, 'conf_sha': conf_sha, 'mode': mode, 'image': image,
             'hosts': rows[k::args.procs]} for k in range(args.procs)]
    if args.procs == 1:
        results = [publish(jobs[0])]
    else:
        with multiprocessing.Pool(args.procs) as pool:
            results = pool.map(publish, jobs)
    pub_wall = time.perf_counter() - t0
    pub_cpu = time.process_time() - c0 + (sum(r[1] for r in results) if args.procs > 1 else 0)
    requests = sum(r[2] for r in results)
    for r in results:
        blobs.update(r[0])
    save()
    print(f'{args.cmd}: published {len(hosts)} hosts in {pub_wall:.2f}s '
          f'(mgr CPU {pub_cpu:.2f}s, {requests} requests, {pub_cpu / len(hosts) * 1e3:.3f} ms CPU/host)', flush=True)

    pending, lat_stored, lat_pub, seen, _ = wait_applied(a, {h: gens[h] for h in hosts}, since, t_pub_ns, args.timeout)
    conv = max(lat_pub) if lat_pub else None
    res = {
        'label': args.label, 'cmd': args.cmd, 'hosts': len(hosts), 'daemons': len(hosts) * (OSDS_PER_HOST + 2),
        'publish_s': round(pub_wall, 3), 'publish_cpu_s': round(pub_cpu, 3), 'publish_requests': requests,
        'procs': args.procs, 'not_applied': len(pending),
        'applied_after_store_s': {'p50': q(lat_stored, .5), 'p90': q(lat_stored, .9), 'p99': q(lat_stored, .99), 'max': q(lat_stored, 1)},
        'applied_after_publish_start_s': {'p50': q(lat_pub, .5), 'p99': q(lat_pub, .99), 'max': conv},
        'events': seen,
    }
    print(json.dumps(res), flush=True)
    if args.result:
        with open(args.result, 'a') as f:
            f.write(json.dumps(res) + '\n')
    return 0 if not pending else 1


if __name__ == '__main__':
    sys.exit(main())
