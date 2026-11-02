#!/usr/bin/env python3
"""
Deploys a Ceph cluster on the agent VMs through intents alone (no SSH),
playing the cephadm mgr module's part: it renders per-host intents (config,
monmap, keyrings sealed to each host, Quadlet units carrying cephadm's
container arguments, and jobs for `ceph-mon --mkfs` and `ceph-volume`),
pushes them with intentctl, and follows the cluster with the ceph CLI.

  bootstrap.py [--hosts node1,node2,node3] [--mgrs node1,node2] [--image IMG]
  bootstrap.py --reset     remove the Ceph daemons again (data is archived)

Phases: mons (+ mgrs) -> quorum -> OSD prepare jobs -> OSD list jobs ->
OSD units -> a pool and some I/O. Each push is the host's whole desired
state; jobs stay in the intent (they run once per id).
Runs inside the agentvm container; state in /vms/cluster/ceph.
"""
import argparse
import base64
import hashlib
import json
import os
import struct
import subprocess
import sys
import time
import urllib.request

C = '/vms/cluster'
D = f'{C}/ceph'
ICTL = '/ab/rust/target/release/intentctl'
ADMIN = '10.77.0.1:7161'
NODE_EXPORTER = '/ab/vm/demo/node-exporter.container'

p = argparse.ArgumentParser()
p.add_argument('--hosts', default='node1,node2,node3')
p.add_argument('--mgrs', default='node1,node2')
p.add_argument('--image', default='quay.io/ceph/ceph:v20.2.4')
p.add_argument('--disks', default='/dev/vdb,/dev/vdc')
p.add_argument('--reset', action='store_true')
p.add_argument('--no-bench', action='store_true')
a = p.parse_args()
HOSTS = a.hosts.split(',')
MGRS = a.mgrs.split(',')
IMAGE = a.image
T0 = time.time()
MGR = json.load(open(f'{C}/mgr.json'))
FSID = json.load(open(f'{C}/hosts.json'))['fsid']


def log(msg):
    print(f'[{time.time() - T0:7.1f}s] {msg}', flush=True)


def addr(h):
    return MGR['hosts'][h]['addr']


def cephx_key():
    """Tentacle wants AES-256 cephx keys (type 2); ceph-authtool makes them."""
    return subprocess.run(['ceph-authtool', '--gen-print-key'], check=True, capture_output=True, text=True).stdout.strip()


def keyring(entries):
    out = ''
    for name, (key, caps) in entries.items():
        out += f'[{name}]\n\tkey = {key}\n'
        for k, v in caps.items():
            out += f'\tcaps {k} = "{v}"\n'
    return out


def write(path, text, mode=0o644):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb' if isinstance(text, bytes) else 'w') as f:
        f.write(text)
    os.chmod(path, mode)


def setup():
    """Keys, monmap, ceph.conf, admin keyring: what cephadm bootstrap makes."""
    os.makedirs(f'{D}/render', exist_ok=True)
    kf = f'{D}/keys.json'
    keys = json.load(open(kf)) if os.path.exists(kf) else {}
    for name in ['mon.', 'client.admin', 'client.bootstrap-osd'] + [f'mgr.{m}' for m in MGRS]:
        keys.setdefault(name, cephx_key())
    write(kf, json.dumps(keys, indent=1), 0o600)
    mon_addrs = {h: f'[v2:{addr(h)}:3300,v1:{addr(h)}:6789]' for h in HOSTS}
    write(f'{D}/ceph.conf', f"""[global]
\tfsid = {FSID}
\tmon_host = {','.join(mon_addrs.values())}
\tpublic_network = 10.77.0.0/24
\tosd_memory_target = 1073741824
\tosd_pool_default_size = 3
\tauth_allow_insecure_global_id_reclaim = false
\tlog_to_file = false
\tmon_cluster_log_to_file = false
""")
    caps_all = {'mon': 'allow *', 'osd': 'allow *', 'mds': 'allow *', 'mgr': 'allow *'}
    entries = {
        'mon.': (keys['mon.'], {'mon': 'allow *'}),
        'client.admin': (keys['client.admin'], caps_all),
        'client.bootstrap-osd': (keys['client.bootstrap-osd'], {'mon': 'allow profile bootstrap-osd'}),
    }
    for m in MGRS:
        entries[f'mgr.{m}'] = (keys[f'mgr.{m}'], {'mon': 'allow profile mgr', 'osd': 'allow *', 'mds': 'allow *'})
    write(f'{D}/mon.keyring', keyring(entries), 0o600)
    write(f'{D}/admin.keyring', keyring({'client.admin': (keys['client.admin'], caps_all)}), 0o600)
    write(f'{D}/bootstrap-osd.keyring', keyring({'client.bootstrap-osd': entries['client.bootstrap-osd']}), 0o600)
    for m in MGRS:
        write(f'{D}/mgr.{m}.keyring', keyring({f'mgr.{m}': (keys[f'mgr.{m}'], {})}), 0o600)
    cmd = ['monmaptool', '--create', '--clobber', '--fsid', FSID]
    for h, v in mon_addrs.items():
        cmd += ['--addv', h, v]
    subprocess.run(cmd + [f'{D}/monmap'], check=True, capture_output=True)
    log(f'cluster {FSID}: mons {", ".join(HOSTS)}, mgrs {", ".join(MGRS)}, image {IMAGE}')
    return hashlib.sha256(keys['mon.'].encode()).hexdigest()[:6]


# --- rendering: cephadm's container arguments as Quadlet units ------------

UNIT_HEAD = """[Unit]
Description=Ceph {name} for {fsid}
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target local-fs.target time-sync.target
PartOf=ceph-{fsid}.target
Before=ceph-{fsid}.target
StartLimitIntervalSec=30min
StartLimitBurst=5
"""

UNIT_TAIL = """
[Service]
ExecStartPre=/usr/bin/install -d -o 167 -g 167 /var/run/ceph/{fsid} /var/log/ceph/{fsid}
{pre}Restart=on-failure
RestartSec=10s
TimeoutStartSec=300
TimeoutStopSec=120
LimitNOFILE=1048576
LimitNPROC=1048576

[Install]
WantedBy=ceph-{fsid}.target
"""

COMMON_ARGS = ('-f --setuser ceph --setgroup ceph --default-log-to-file=false '
               '--default-log-to-stderr=true --default-log-stderr-prefix=debug')
CONF = f'/var/lib/ceph/{FSID}/config/ceph.conf'
LVM_VOLS = ['/dev:/dev', '/run/udev:/run/udev', '/run/lvm:/run/lvm', '/run/lock/lvm:/run/lock/lvm']


def container(name, host, entrypoint, args, volumes, privileged=False, pre=''):
    vols = volumes + [f'{CONF}:/etc/ceph/ceph.conf:z', f'/var/run/ceph/{FSID}:/var/run/ceph:z',
                      f'/var/log/ceph/{FSID}:/var/log/ceph:z']
    # --ipc=host: LVM waits on udev through SysV semaphores, as cephadm knows
    extra = ' --privileged --group-add=disk --ipc=host' if privileged else ''
    body = f"""
[Container]
ContainerName=ceph-{FSID}-{name}
Image={IMAGE}
Network=host
RunInit=true
PodmanArgs=--entrypoint={entrypoint} --pids-limit=-1{extra}
Exec=-n {name} {COMMON_ARGS}{args}
Environment=CONTAINER_IMAGE={IMAGE} NODE_NAME={host} TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=134217728
""" + ''.join(f'Volume={v}\n' for v in vols)
    text = UNIT_HEAD.format(name=name, fsid=FSID) + body + UNIT_TAIL.format(fsid=FSID, pre=pre)
    path = f'{D}/render/{host}/{name}.container'
    write(path, text)
    return path


def mon_unit(h):
    args = ' --default-mon-cluster-log-to-file=false --default-mon-cluster-log-to-stderr=true'
    return container(f'mon.{h}', h, '/usr/bin/ceph-mon', args, [f'/var/lib/ceph/{FSID}/mon.{h}:/var/lib/ceph/mon/ceph-{h}:z'])


MGR_MODULE = f'/var/lib/ceph/{FSID}/mgr-modules/agentd'
# The orchestrator module only accepts known backends; upstream this is one
# more name in enum_allowed. Here: the image's file with that name added
# (extracted to orchestrator-module.orig.py), mounted over the original.
ORCH_PATCH = f'/var/lib/ceph/{FSID}/mgr-modules/orchestrator-module.py'


def orchestrator_patch():
    src = open(f'{D}/orchestrator-module.orig.py').read()
    old = "enum_allowed=['cephadm', 'rook', 'test_orchestrator'],"
    assert old in src, 'orchestrator module changed: re-extract orchestrator-module.orig.py'
    write(f'{D}/orchestrator-module.py', src.replace(old, "enum_allowed=['cephadm', 'rook', 'test_orchestrator', 'agentd'],"))
    return f'{D}/orchestrator-module.py'


def mgr_unit(h):
    # the agentd orchestrator module ships as intent files, mounted into the
    # image's module directory
    return container(f'mgr.{h}', h, '/usr/bin/ceph-mgr', '', [f'/var/lib/ceph/{FSID}/mgr.{h}:/var/lib/ceph/mgr/ceph-{h}:z',
                                                               f'{MGR_MODULE}:/usr/share/ceph/mgr/agentd:z',
                                                               f'{ORCH_PATCH}:/usr/share/ceph/mgr/orchestrator/module.py:ro,z'])


def osd_unit(h, osd_id, osd_fsid):
    data = f'/var/lib/ceph/{FSID}/osd.{osd_id}'
    # cephadm's prestart container: activate (re)creates the data dir and
    # chowns the LV, which udev on the host doesn't know to do after a reboot
    activate = ('/usr/bin/podman run --rm --privileged --net=host --ipc=host --entrypoint=/usr/sbin/ceph-volume '
                '-e CEPH_VOLUME_SKIP_RESTORECON=yes '
                f'-v {data}:/var/lib/ceph/osd/ceph-{osd_id}:z -v {CONF}:/etc/ceph/ceph.conf:z '
                + ' '.join(f'-v {v}' for v in LVM_VOLS) + ' -v /sys:/sys '
                f'{IMAGE} lvm activate {osd_id} {osd_fsid} --no-systemd --no-tmpfs')
    pre = (f'ExecStartPre=/usr/bin/install -d -m 0755 {data} /run/lvm /run/lock/lvm\n'
           f'ExecStartPre={activate}\n')
    return container(f'osd.{osd_id}', h, '/usr/bin/ceph-osd', '', [f'{data}:/var/lib/ceph/osd/ceph-{osd_id}:z'] + LVM_VOLS,
                     privileged=True, pre=pre)


def ceph_volume_job(args, timeout=900, keyring=False):
    vols = LVM_VOLS + ['/sys:/sys', f'{CONF}:/etc/ceph/ceph.conf:z']
    if keyring:
        vols.append(f'/var/lib/ceph/{FSID}/.keyrings/bootstrap-osd.keyring:/var/lib/ceph/bootstrap-osd/ceph.keyring:z')
    return {'image': IMAGE, 'entrypoint': '/usr/sbin/ceph-volume', 'args': args, 'privileged': True,
            'volumes': vols, 'env': ['CEPH_VOLUME_SKIP_RESTORECON=yes'], 'dirs': ['/run/lvm', '/run/lock/lvm'],
            'podman_args': ['--ipc=host', '--group-add=disk'], 'timeout_s': timeout}


def mkfs_job(h):
    script = (f'set -e; d=/data/mon.{h}; mkdir -p $d; '
              'if [ -e $d/store.db ]; then echo already made; exit 0; fi; chown 167:167 $d; '
              f'ceph-mon --mkfs -i {h} --fsid {FSID} --mon-data $d -c /etc/ceph/ceph.conf '
              '--monmap /tmp/monmap --keyring /tmp/keyring --setuser ceph --setgroup ceph; echo mkfs ok')
    return {'image': IMAGE, 'entrypoint': '/bin/bash', 'args': ['-c', script],
            'volumes': [f'/var/lib/ceph/{FSID}:/data:z', f'{CONF}:/etc/ceph/ceph.conf:z',
                        f'/var/lib/ceph/{FSID}/.keyrings/monmap:/tmp/monmap:z',
                        f'/var/lib/ceph/{FSID}/.keyrings/mon.keyring:/tmp/keyring:z'],
            'timeout_s': 300}


# --- per-host desired state -------------------------------------------------

class Host:
    def __init__(self, h):
        self.h = h
        self.files = {
            CONF: {'src': f'{D}/ceph.conf', 'raw': True},
            f'/var/lib/ceph/{FSID}/.keyrings/monmap': {'src': f'{D}/monmap', 'raw': True, 'owner': '167:167'},
            f'/var/lib/ceph/{FSID}/.keyrings/mon.keyring': {'src': f'{D}/mon.keyring', 'raw': True, 'secret': True,
                                                            'mode': '0600', 'owner': '167:167'},
            f'/var/lib/ceph/{FSID}/.keyrings/bootstrap-osd.keyring': {'src': f'{D}/bootstrap-osd.keyring', 'raw': True,
                                                                      'secret': True, 'mode': '0600'},
        }
        self.remove = [f'web.{h}', f'crashy.{h}']
        self.units = {
            f'node-exporter.{h}': {'quadlet': NODE_EXPORTER},
            f'mon.{h}': {'quadlet': mon_unit(h), 'after_jobs': [f'mkfs-mon-{h}-{KID}']},
        }
        self.jobs = {f'mkfs-mon-{h}-{KID}': mkfs_job(h)}
        if h in MGRS:
            self.files[f'/var/lib/ceph/{FSID}/mgr.{h}/keyring'] = {'src': f'{D}/mgr.{h}.keyring', 'raw': True,
                                                                  'secret': True, 'mode': '0600', 'owner': '167:167'}
            # a new module version restarts the mgr (cephadm's reconfig deps)
            self.units[f'mgr.{h}'] = {'quadlet': mgr_unit(h),
                                      'restart_on': [f'{MGR_MODULE}/__init__.py', f'{MGR_MODULE}/module.py', ORCH_PATCH]}
            self.files[ORCH_PATCH] = {'src': orchestrator_patch(), 'raw': True}
            for f in ('__init__.py', 'module.py'):
                self.files[f'{MGR_MODULE}/{f}'] = {'src': f'/ab/vm/ceph/mgr-agentd/{f}', 'raw': True}

    def push(self, label, wait=True, timeout=900):
        spec = {'fsid': FSID, 'prefetch': [IMAGE], 'files': self.files, 'units': self.units, 'jobs': self.jobs,
                'remove': self.remove}
        path = f'{D}/render/{self.h}/spec-{label}.json'
        write(path, json.dumps(spec, indent=1))
        cmd = [ICTL, 'push', '--admin', ADMIN, '--key', f'{C}/intent.key', '--state', f'{C}/mgr.json',
               '--spec', path, '--host', self.h]
        if wait:
            cmd += ['--wait', '--timeout', str(timeout)]
        out = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout
        for line in out.strip().splitlines():
            log(f'  {line}')


def status(h):
    with urllib.request.urlopen(f'http://{ADMIN}/v1/hosts/{h}/status') as r:
        return json.load(r).get('status') or {}


def wait_job(h, job, timeout=900):
    t = time.time()
    while time.time() - t < timeout:
        j = status(h).get('jobs', {}).get(job)
        if j and j['state'] in ('succeeded', 'failed'):
            return j
        time.sleep(1)
    raise SystemExit(f'{h}: job {job} did not finish in {timeout}s')


def ceph(*args, check=True):
    r = subprocess.run(['ceph', '-c', f'{D}/ceph.conf', '-k', f'{D}/admin.keyring', '--connect-timeout', '10',
                        *args], capture_output=True, text=True)
    if check and r.returncode:
        raise SystemExit(f'ceph {" ".join(args)}: {r.stderr.strip()}')
    return r.stdout


def until(what, fn, timeout):
    t = time.time()
    while time.time() - t < timeout:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit(f'timed out waiting for {what}')


def reset():
    """Every Ceph daemon leaves through `remove`; the agents archive data;
    a zap job per host wipes the OSD disks."""
    stamp = int(time.time())
    for h in HOSTS:
        st = status(h)
        daemons = [d for d in st.get('units', {}) if d.split('.')[0] in ('mon', 'mgr', 'osd')] + st.get('strays', [])
        zap = ceph_volume_job(['lvm', 'zap', '--destroy'] + a.disks.split(','), timeout=300)
        spec = {'fsid': FSID, 'units': {f'node-exporter.{h}': {'quadlet': NODE_EXPORTER}},
                'remove': sorted(set(daemons + [f'mon.{h}', f'mgr.{h}', f'web.{h}', f'crashy.{h}'])),
                'jobs': {f'zap-{h}-{stamp}': zap}}
        path = f'{D}/render/{h}/spec-reset.json'
        write(path, json.dumps(spec, indent=1))
        subprocess.run([ICTL, 'push', '--admin', ADMIN, '--key', f'{C}/intent.key', '--state', f'{C}/mgr.json',
                        '--spec', path, '--host', h, '--wait'], check=True)
    for h in HOSTS:
        j = wait_job(h, f'zap-{h}-{stamp}', 300)
        log(f'  {h}: zap {j["state"]} (exit {j.get("exit_code")})')
    if os.path.exists(f'{D}/keys.json'):
        os.rename(f'{D}/keys.json', f'{D}/keys.json.{int(time.time())}')
    log('reset: Ceph daemons removed, keys retired')


if a.reset:
    reset()
    sys.exit(0)
KID = setup()
hosts = {h: Host(h) for h in HOSTS}

log('phase 1: config, sealed keyrings, monmap, mkfs jobs, mon and mgr units')
for h in HOSTS:
    hosts[h].push('mons')
for h in HOSTS:
    j = wait_job(h, f'mkfs-mon-{h}-{KID}', 60)
    log(f'  {h}: mkfs {j["state"]} ({(j["finished_ns"] - j["started_ns"]) / 1e9:.1f}s): {j["stdout"].strip()}')
q = until('mon quorum', lambda: len(json.loads(ceph('quorum_status', '-f', 'json'))['quorum_names']) == len(HOSTS), 300)
log(f'quorum of {len(HOSTS)}: {json.loads(ceph("quorum_status", "-f", "json"))["quorum_names"]}')
until('an active mgr', lambda: json.loads(ceph('mgr', 'stat', '-f', 'json')).get('available'), 300)
log(f'mgr: {json.loads(ceph("mgr", "stat", "-f", "json"))["active_name"]} active')

log('phase 2: ceph-volume lvm batch, as a job on every host')
for h in HOSTS:
    hosts[h].jobs[f'osd-prepare-{h}-{KID}'] = ceph_volume_job(['lvm', 'batch', '--yes', '--no-systemd'] + a.disks.split(','),
                                                              keyring=True)
    hosts[h].push('osd-prepare', wait=False)
for h in HOSTS:
    j = wait_job(h, f'osd-prepare-{h}-{KID}')
    log(f'  {h}: osd-prepare {j["state"]} ({(j["finished_ns"] - j["started_ns"]) / 1e9:.1f}s, exit {j.get("exit_code")})')
    if j['state'] != 'succeeded':
        print(j['stderr'][-3000:])
        raise SystemExit(1)

log('phase 3: which OSDs did they make (ceph-volume lvm list job), then OSD units')
for h in HOSTS:
    hosts[h].jobs[f'osd-list-{h}-{KID}'] = ceph_volume_job(['lvm', 'list', '--format', 'json'], timeout=120)
    hosts[h].push('osd-list', wait=False)
for h in HOSTS:
    j = wait_job(h, f'osd-list-{h}-{KID}', 300)
    osds = json.loads(j['stdout'])
    for osd_id, devs in sorted(osds.items(), key=lambda kv: int(kv[0])):
        block = next(d for d in devs if d['type'] == 'block')
        hosts[h].units[f'osd.{osd_id}'] = {'quadlet': osd_unit(h, osd_id, block['tags']['ceph.osd_fsid']),
                                           'after_jobs': [f'osd-prepare-{h}-{KID}']}
        log(f'  {h}: osd.{osd_id} on {", ".join(block["devices"])}')
    hosts[h].push('osds')
n_osds = sum(1 for h in HOSTS for u in hosts[h].units if u.startswith('osd.'))
until(f'{n_osds} OSDs up', lambda: json.loads(ceph('osd', 'stat', '-f', 'json'))['num_up_osds'] == n_osds, 300)
log(f'{n_osds} OSDs up and in')

log('phase 4: a pool and some I/O')
ceph('osd', 'pool', 'create', 'agenttest', '32', check=False)
ceph('osd', 'pool', 'application', 'enable', 'agenttest', 'rados', check=False)
until('PGs active+clean', lambda: 'active+clean' in ceph('pg', 'stat') and 'peering' not in ceph('pg', 'stat'), 300)
r = subprocess.run(['rados', '-c', f'{D}/ceph.conf', '-k', f'{D}/admin.keyring', '-p', 'agenttest', 'bench', '10',
                    'write', '-b', '1M', '-t', '8', '--no-cleanup'], capture_output=True, text=True) if not a.no_bench else None
for line in (r.stdout.splitlines() if r else []):
    if line.startswith(('Bandwidth (MB/sec)', 'Average IOPS', 'Total writes made')):
        log(f'  rados bench: {line.strip()}')
print(ceph('-s'))
log('done')
