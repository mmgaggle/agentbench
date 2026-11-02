#!/usr/bin/env python3
"""
Where the bytes and parse time of one agent report go.

  serialization.py body.json

Reports: size; share in the double-encoded `facts`/`volume` strings and
their escaping overhead; share that is JSON field names (roughly what a
schema'd binary encoding such as protobuf drops); the mgr-side parse cost
(json.loads of the body plus the two inner documents); and how well the
body compresses.
"""
import json
import lzma
import sys
import time
import zlib

raw = open(sys.argv[1], 'rb').read()
doc = json.loads(raw)
facts_s, volume_s = doc['facts'], doc['volume']
facts, volume = json.loads(facts_s), json.loads(volume_s)


def key_bytes(o):
    """Bytes spent on field names: "key": per member."""
    if isinstance(o, dict):
        return sum(len(k.encode()) + 3 + key_bytes(v) for k, v in o.items())
    if isinstance(o, list):
        return sum(key_bytes(v) for v in o)
    return 0


inner = len(json.dumps(facts_s)) + len(json.dumps(volume_s))
inner_plain = len(json.dumps(facts, separators=(',', ':'))) + len(json.dumps(volume, separators=(',', ':')))
single = dict(doc, facts=facts, volume=volume)
single_bytes = json.dumps(single, separators=(',', ':')).encode()
keys = key_bytes(single)

print(f'report body          {len(raw):7d} bytes')
print(f'  facts+volume       {inner:7d} bytes as escaped strings, {inner_plain} as plain JSON '
      f'(escaping +{100 * (inner - inner_plain) / inner_plain:.0f}%)')
print(f'  without double encoding {len(single_bytes):7d} bytes')
print(f'  field names        {keys:7d} bytes ({100 * keys / len(single_bytes):.0f}% of the single-encoded body)')
for name, f in (('zlib -1', lambda b: zlib.compress(b, 1)), ('zlib -6', lambda b: zlib.compress(b, 6)),
                ('xz -1', lambda b: lzma.compress(b, preset=1))):
    print(f'  {name:18} {len(f(raw)):7d} bytes ({len(raw) / len(f(raw)):.1f}x)')


def bench(fn, n=2000):
    fn()
    t = time.perf_counter()
    for _ in range(n):
        fn()
    return (time.perf_counter() - t) / n * 1e6


outer = bench(lambda: json.loads(raw))
inner_t = bench(lambda: (json.loads(facts_s), json.loads(volume_s)))
single_t = bench(lambda: json.loads(single_bytes))
print(f'python json.loads    body {outer:5.0f} us + facts/volume {inner_t:5.0f} us '
      f'= {(outer + inner_t) / 1000:.2f} ms per report')
print(f'                     single-encoded body in one pass: {single_t:5.0f} us')
