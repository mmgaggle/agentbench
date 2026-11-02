#!/usr/bin/env python3
"""
Seed the cephadm module's KV store with N fake hosts that each run an agent.

For every host this writes the records cephadm loads at startup:

  mgr/cephadm/inventory          one HostSpec per host, addr 127.0.0.1
  mgr/cephadm/host.<host>        host cache: the agent daemon, fresh
                                 last_host_check so no SSH host check runs
  mgr/cephadm/agent.<host>       agent keyring, listener port, counter=1 and
                                 config deps matching the running endpoint,
                                 so _check_agent never reconfigures over SSH

then fails the mgr over so the module reloads them. It also writes
hosts.json (consumed by agentsim) and a listener cert signed by the cephadm
root CA so simulated agents can accept mgr->agent messages.
"""
import argparse
import base64
import concurrent.futures
import datetime
import ipaddress
import json
import os
import random
import sys
import time

import rados
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

PREFIX = 'mgr/cephadm/'


def now_str() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def mon_command(cluster: rados.Rados, cmd: dict, inbuf: bytes = b'') -> str:
    ret, out, errs = cluster.mon_command(json.dumps(cmd), inbuf, timeout=120)
    if ret != 0:
        raise RuntimeError(f'{cmd["prefix"]} failed: {ret} {errs}')
    return out.decode()


def kv_get(cluster: rados.Rados, key: str) -> str:
    return mon_command(cluster, {'prefix': 'config-key get', 'key': key})


def kv_set(cluster: rados.Rados, key: str, val: str) -> None:
    mon_command(cluster, {'prefix': 'config-key set', 'key': key}, val.encode())


def port_sockets(port: int) -> list:
    """TCP states (hex, from /proc/net/tcp*) of sockets bound to local `port`."""
    states = []
    for path in ('/proc/net/tcp', '/proc/net/tcp6'):
        try:
            lines = open(path).readlines()[1:]
        except OSError:
            continue
        for line in lines:
            f = line.split()
            if int(f[1].rsplit(':', 1)[1], 16) == port:
                states.append(f[3])
    return states


def listener_cert(root_cert_pem: str, root_key_pem: str, ip: str) -> tuple:
    root_cert = x509.load_pem_x509_certificate(root_cert_pem.encode())
    root_key = serialization.load_pem_private_key(root_key_pem.encode(), password=None)
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'agentsim')]))
        .issuer_name(root_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=365))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(ip))]),
                       critical=False)
        .sign(root_key, hashes.SHA256())
    )
    return (cert.public_bytes(serialization.Encoding.PEM).decode(),
            key.private_bytes(serialization.Encoding.PEM,
                              serialization.PrivateFormat.PKCS8,
                              serialization.NoEncryption()).decode())


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument('--conf', default=os.environ.get('CEPH_CONF'))
    p.add_argument('--hosts', type=int, required=True)
    p.add_argument('--prefix', default='abhost-')
    p.add_argument('--addr', default='127.0.0.1')
    p.add_argument('--base-port', type=int, default=20000)
    p.add_argument('--endpoint-port', type=int, default=7150)
    p.add_argument('--out', default='/mon/vcluster/agents')
    p.add_argument('--threads', type=int, default=128)
    p.add_argument('--seed', type=int, default=1)
    p.add_argument('--no-failover', action='store_true')
    p.add_argument('--osds', type=int, default=16,
                   help='OSDs per host; the host cache is seeded with the same daemons '
                        'agentsim reports, as a mgr failover would load them')
    args = p.parse_args()

    rng = random.Random(args.seed)
    cluster = rados.Rados(conffile=args.conf)
    cluster.connect()
    fsid = cluster.get_fsid()
    mgr_ip = args.addr

    root_cert = json.loads(kv_get(cluster, PREFIX + 'cert_store.cert.cephadm_root_ca_cert'))['cert']
    root_key = json.loads(kv_get(cluster, PREFIX + 'cert_store.key.cephadm_root_ca_key'))['key']
    # Mirror CephadmAgent.get_dependencies() so _check_agent sees no change.
    image = mon_command(cluster, {'prefix': 'config get', 'who': 'osd',
                                  'key': 'container_image'}).strip()
    deps = [mgr_ip, str(args.endpoint_port), root_cert, 'False', image]
    try:
        agent_spec = json.loads(kv_get(cluster, PREFIX + 'spec.agent'))['spec'].get('spec', {})
        if agent_spec.get('ssl') and agent_spec.get('certificate_source'):
            deps.append(f'certificate_source: {agent_spec["certificate_source"]}')
    except RuntimeError:
        pass
    deps = sorted(deps)

    stamp = now_str()
    hosts = []
    inventory = {}
    writes = []
    for i in range(args.hosts):
        name = f'{args.prefix}{i:05d}'
        port = args.base_port + i
        secret = base64.b64encode(rng.randbytes(28)).decode()
        keyring = f'[client.agent.{name}]\n\tkey = {secret}\n'
        hosts.append({'name': name, 'keyring': keyring, 'port': port})
        inventory[name] = {'hostname': name, 'addr': args.addr, 'labels': [], 'status': ''}
        # Same daemon names as proto::PayloadGen::daemon_names().
        daemons = {}
        for dtype, did in ([('agent', name), ('crash', name), ('node-exporter', name),
                            ('ceph-exporter', name)]
                           + [('osd', str(i * args.osds + k)) for k in range(args.osds)]):
            daemons[f'{dtype}.{did}'] = {
                'daemon_type': dtype, 'daemon_id': did, 'hostname': name,
                'status': 1, 'status_desc': 'running',
                'ports': [port] if dtype == 'agent' else [],
                'last_refresh': stamp, 'is_active': False,
            }
        writes.append((PREFIX + 'host.' + name, json.dumps({
            'daemons': daemons,
            'devices': [], 'osdspec_previews': [], 'osdspec_last_applied': {},
            'daemon_config_deps': {},
            'last_host_check': stamp, 'last_daemon_update': stamp,
            'last_device_update': stamp, 'last_network_update': stamp,
            'metadata_up_to_date': True,
        })))
        writes.append((PREFIX + 'agent.' + name, json.dumps({
            'agent_counter': 1, 'agent_keys': keyring, 'agent_ports': port,
            'agent_timestamp': stamp,
            'agent_config_deps': {'deps': deps, 'last_config': stamp},
        })))

    # Drop records for hosts from an earlier, larger seed.
    existing = json.loads(mon_command(cluster, {'prefix': 'config-key ls'}))
    names = set(inventory)
    stale = []
    for k in existing:
        for kind in ('host.', 'agent.'):
            if k.startswith(PREFIX + kind):
                # host.<name>, host.<name>.devices.<n>, agent.<name>
                name = k[len(PREFIX + kind):].split('.devices.')[0]
                if name.startswith(args.prefix) and name not in names:
                    stale.append(k)

    t0 = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(args.threads) as ex:
        list(ex.map(lambda kv: kv_set(cluster, *kv), writes))
        list(ex.map(lambda k: mon_command(cluster, {'prefix': 'config-key rm', 'key': k}), stale))
    kv_set(cluster, PREFIX + 'inventory', json.dumps(inventory))
    print(f'seeded {args.hosts} hosts ({len(writes) + 1} keys, {len(stale)} stale removed) '
          f'in {time.monotonic() - t0:.1f}s', file=sys.stderr)

    os.makedirs(args.out, exist_ok=True)
    cert, key = listener_cert(root_cert, root_key, args.addr)
    with open(os.path.join(args.out, 'listener.crt'), 'w') as f:
        f.write(cert)
    with open(os.path.join(args.out, 'listener.key'), 'w') as f:
        f.write(key)
    with open(os.path.join(args.out, 'root_ca.pem'), 'w') as f:
        f.write(root_cert)
    with open(os.path.join(args.out, 'hosts.json'), 'w') as f:
        json.dump({'fsid': fsid, 'endpoint': f'{mgr_ip}:{args.endpoint_port}',
                   'addr': args.addr, 'hosts': hosts}, f)

    if not args.no_failover:
        # The agent endpoint picks its port with mgr_util.test_port_allocation(),
        # which binds without SO_REUSEADDR and so treats TIME_WAIT connections
        # from earlier agent traffic as "in use" and moves to the next port.
        # Let them drain so the endpoint comes back on --endpoint-port.
        t = time.monotonic()
        while any(st != '0A' for st in port_sockets(args.endpoint_port)):
            if time.monotonic() - t > 120:
                print('endpoint port still has TIME_WAIT sockets after 120s', file=sys.stderr)
                break
            time.sleep(2)
        waited = time.monotonic() - t
        if waited > 1:
            print(f'waited {waited:.0f}s for TIME_WAIT on port {args.endpoint_port} to drain',
                  file=sys.stderr)
        mgrmap = json.loads(mon_command(cluster, {'prefix': 'mgr dump', 'format': 'json'}))
        mon_command(cluster, {'prefix': 'mgr fail', 'who': mgrmap['active_name']})
        # Wait until the reloaded module reports itself available with every host.
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            time.sleep(2)
            try:
                ret, out, _ = cluster.mgr_command(
                    json.dumps({'prefix': 'orch host ls', 'format': 'json'}), b'', timeout=30)
                if ret == 0 and len(json.loads(out)) == args.hosts:
                    break
            except Exception:
                pass
        else:
            print('timed out waiting for cephadm to load the seeded hosts', file=sys.stderr)
            return 1
        for _ in range(30):
            if '0A' in port_sockets(args.endpoint_port):
                break
            time.sleep(1)
        else:
            print(f'agent endpoint is not listening on port {args.endpoint_port}', file=sys.stderr)
            return 1
        print(f'cephadm reloaded with {args.hosts} hosts', file=sys.stderr)
    cluster.shutdown()
    return 0


if __name__ == '__main__':
    sys.exit(main())
