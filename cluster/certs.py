#!/usr/bin/env python3
"""
Write TLS material for agentd into --out (read-only against the cluster):

  endpoint-rsa.{crt,key}  the mgr agent endpoint's own cert (RSA-4096),
                          so agentd pays the same handshake cost as the mgr
  endpoint-ec.{crt,key}   ECDSA P-256 cert for the same IP, signed by the
                          cephadm root CA (handshake-cost variant)
  mgr-client.{crt,key}    client cert for mgr->agent messages (agents
                          require one signed by the root CA)
  root_ca.pem
"""
import argparse
import datetime
import ipaddress
import json
import os
import sys

import rados
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

PREFIX = 'mgr/cephadm/cert_store.'


def kv_get(cluster, key):
    ret, out, errs = cluster.mon_command(json.dumps({'prefix': 'config-key get', 'key': key}), b'')
    if ret != 0:
        raise RuntimeError(f'{key}: {errs}')
    return json.loads(out)


def issue(root_cert_pem, root_key_pem, cn, ip):
    root_cert = x509.load_pem_x509_certificate(root_cert_pem.encode())
    root_key = serialization.load_pem_private_key(root_key_pem.encode(), password=None)
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    b = (x509.CertificateBuilder()
         .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
         .issuer_name(root_cert.subject)
         .public_key(key.public_key())
         .serial_number(x509.random_serial_number())
         .not_valid_before(now - datetime.timedelta(days=1))
         .not_valid_after(now + datetime.timedelta(days=365)))
    if ip:
        b = b.add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(ip))]),
                            critical=False)
    cert = b.sign(root_key, hashes.SHA256())
    return (cert.public_bytes(serialization.Encoding.PEM).decode(),
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                              serialization.NoEncryption()).decode())


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--conf', default=os.environ.get('CEPH_CONF'))
    p.add_argument('--ip', default='127.0.0.1')
    p.add_argument('--out', default='/mon/vcluster/agentd')
    args = p.parse_args()
    cluster = rados.Rados(conffile=args.conf)
    cluster.connect()
    root_cert = kv_get(cluster, PREFIX + 'cert.cephadm_root_ca_cert')['cert']
    root_key = kv_get(cluster, PREFIX + 'key.cephadm_root_ca_key')['key']
    ep_cert = next(iter(kv_get(cluster, PREFIX + 'cert.cephadm-signed_agent_cert').values()))['cert']
    ep_key = next(iter(kv_get(cluster, PREFIX + 'key.cephadm-signed_agent_key').values()))['key']
    cluster.shutdown()

    os.makedirs(args.out, exist_ok=True)
    files = {'root_ca.pem': root_cert, 'endpoint-rsa.crt': ep_cert, 'endpoint-rsa.key': ep_key}
    files['endpoint-ec.crt'], files['endpoint-ec.key'] = issue(root_cert, root_key, 'agentd', args.ip)
    files['mgr-client.crt'], files['mgr-client.key'] = issue(root_cert, root_key, 'mgr', None)
    for name, data in files.items():
        with open(os.path.join(args.out, name), 'w') as f:
            f.write(data)
    print(f'wrote {", ".join(files)} to {args.out}', file=sys.stderr)


if __name__ == '__main__':
    main()
