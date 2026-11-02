#!/usr/bin/env python3
"""
Write a hosts.json for agentsim and `agentd --hosts-file`, without a
cluster: N hosts with random agent keyrings, for runs where agentd keeps
state in memory only (--persist none).

  gen_hosts.py --hosts 20000 --out /mon/agents-20000.json
"""
import argparse
import base64
import json
import random
import uuid

p = argparse.ArgumentParser()
p.add_argument('--hosts', type=int, required=True)
p.add_argument('--out', required=True)
p.add_argument('--prefix', default='abhost-')
p.add_argument('--endpoint', default='127.0.0.1:7160')
p.add_argument('--seed', type=int, default=1)
a = p.parse_args()
rng = random.Random(a.seed)
hosts = []
for i in range(a.hosts):
    name = f'{a.prefix}{i:05d}'
    secret = base64.b64encode(rng.randbytes(28)).decode()
    hosts.append({'name': name, 'keyring': f'[client.agent.{name}]\n\tkey = {secret}\n', 'port': 20000 + i % 40000})
with open(a.out, 'w') as f:
    json.dump({'fsid': str(uuid.UUID(int=rng.getrandbits(128))), 'endpoint': a.endpoint,
               'addr': '127.0.0.1', 'hosts': hosts}, f)
