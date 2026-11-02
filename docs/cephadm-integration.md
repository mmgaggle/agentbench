# How cephadm would consume agentd's state (sketch)

Design sketch, not implemented. Code references are to ceph
`src/pybind/mgr/cephadm` (main, August 2026); numbers are from the
agentbench runs in this repo.

## The rule

agentd absorbs every agent message; cephadm only sees **changes**. The
mgr's Python work then follows how often hosts change, not how many hosts
report. Everything that reads `HostCache` today (scheduler, serve loop,
`orch ps`, `orch device ls`, config checks, memory autotune) keeps reading
the same structures.

## Who owns what

| | owner | notes |
|---|---|---|
| inventory, labels, specs, placement, deploy/remove over SSH | cephadm | unchanged |
| agent identity: keyrings, certs, which hosts may report | cephadm | pushed to agentd |
| observed state of agent hosts: daemons (`ls`), devices, networks, facts | agentd | cephadm keeps a replica in `HostCache` |
| usage samples (memory, cpu%) | agentd | reaches cephadm at a slow rate |
| agent liveness | agentd | report timestamps first, SWIM later |
| agent connections and mgr->agent messages | agentd | replaces `AgentMessageThread` |
| hosts without an agent, or whose agent is down | cephadm over SSH | unchanged fallback |
| `mgr/cephadm/host.<h>`, `agent.<h>` KV records | cephadm only | agentd writes nothing to the mon and needs no cephx key |

## What changes in cephadm

| today | with agentd |
|---|---|
| `HostData.index()` / `handle_metadata()` run per report on cherrypy workers (`agent.py:676`, `:723`) | one `AgentdFeed` thread applies batches of changes |
| 3 KV writes per report: `save_host`, `save_host_devices`, `save_agent` | `save_host` / `save_host_devices` only when the feed changes that host's daemons or devices |
| `get_error_daemons()` twice and `update_failed_daemon_health_check()` per report, each scanning every daemon in the cluster | per-host failed set updated from the change; the health check is rebuilt once per batch, only if a set changed |
| `_agent_down()`: `agent_timestamp` older than refresh x multiplier (`agent.py:911`) | the host's liveness from agentd |
| `offline_hosts_remove()` on every report (`agent.py:687`) | on a liveness transition to up |
| `_request_agent_acks()` starts an `AgentMessageThread` per host, each generating a new RSA-4096 cert (`agent.py:802`, `:890`) | one batched `POST /v1/refresh` |
| the agent's config deps contain the active mgr's IP (`CephadmAgent.get_dependencies`, `services/cephadmservice.py:2061`), so every mgr failover reconfigures every agent | agents target agentd; a mgr failover touches no agent |

## agentd API for cephadm

A separate listener from the agents' port, mutual TLS with a cert from
cephadm's `cert_mgr`. cephadm is its only client.

### Membership

`PUT /v1/hosts` (whole set) and `PATCH /v1/hosts` (adds and removes):

```json
{"gen": 812,
 "hosts": [{"host": "node-0042", "addr": "10.1.0.42", "keyring_sha256": "9f2c..."}]}
```

agentd accepts reports only from listed hosts. It keeps a digest of each
keyring, not the keyring, and compares it with the digest of what the agent
sends.

### Change feed

`GET /v1/changes?epoch=E&since=S&max=2000&wait=1`

```json
{"epoch": "a41c07e2", "seq": 918273, "more": false,
 "hosts": [
   {"host": "node-0042", "seq": 918270,
    "daemons": [ {"name": "osd.17", "state": "running", "...": "..."} ],
    "ack": 8,
    "liveness": {"state": "up", "since": "2026-10-31T09:12:04Z"}}
 ]}
```

- **Level-triggered.** agentd keeps, per host and per part (`daemons`,
  `devices`, `networks`, `facts`, `usage`, `liveness`, `ack`,
  `config_deps`), the sequence number of the part's last material change. A
  poll returns every host with a part newer than `since`, and each part
  carries the host's current full value (the full daemon list, never a
  diff). Five changes to a host between polls arrive as one entry. There is
  no queue, so nothing can be dropped or overflow, and cephadm can apply a
  part without knowing what came before.
- **Material** is the per-part form of `HostState::fingerprint()` that
  agentd already uses for the write-on-change policy: usage fields and
  refresh timestamps don't count.
- **Index:** a `BTreeMap<seq, host>`, so a poll is a range scan over the
  changed hosts only.
- **Pacing:** the poll returns as soon as anything is newer than `since`,
  or after `wait`. cephadm controls its load with `max` and its poll rate;
  agentd never calls into the mgr.
- **Epoch:** random per agentd start. A poll with the wrong epoch gets 410,
  and cephadm resyncs.

### Resync

`POST /v1/resync`: cephadm sends per-part fingerprints of what it holds
(the `host.<h>` records it loaded at startup); agentd returns only the parts
that differ, plus the current `seq`.

```json
{"hosts": {"node-0042": {"daemons": "6c1f...", "devices": "02be...",
                         "networks": "77a0...", "facts": "d913..."}}}
```

After a mgr failover where nothing changed, the answer is nearly empty. A
full dump would be about 50 KB per host, or 2.5 GB at 50,000 hosts.

agentd answers `unknown` for a host that has not reported since agentd
started. cephadm then keeps its replica: no data is not the same as no
daemons.

### Usage

Usage values change on every report, so they are not material changes.
Instead each host's `usage` part is marked changed at most once per usage
interval (default 5 minutes, as in the v2 agent). cephadm patches
`memory_usage` and `cpu_percentage` on the existing `DaemonDescription`
objects in place, without rebuilding them. `orch ps` and the memory
autotuner (`autotune.py:48`) read them from there.

### Commands

- `POST /v1/refresh {"hosts": {"node-0042": 8, "node-0107": 3}}` replaces
  `_request_agent_acks(increment=True)`; the values are the new counters.
  agentd delivers them over whatever the agent speaks: v1 agents through
  the existing listener protocol (10-byte length + JSON) with one reused
  client cert, v2 agents on their open connection, and when every host is
  targeted, a SWIM broadcast. The agent's next report carries `ack`, which
  reaches cephadm as the `ack` part.
- `POST /v1/hosts/<h>/agent-config {"deps": [...], "config": {...}}`
  replaces the HTTP config push in `_check_agent()`. Delivery appears in
  the feed as the host's `config_deps` part, which drives
  `agent_config_successfully_delivered()`.
- `GET /v1/status`: epoch, role, connected agents, agentd version.

## cephadm side

```python
class AgentdFeed(threading.Thread):
    def run(self) -> None:
        while self.mgr.run:
            try:
                if self.cursor is None:
                    self.epoch, self.cursor = self._resync()
                r = self.client.changes(self.epoch, self.cursor, max=2000, wait=1.0)
            except AgentdGone:            # 410: agentd restarted
                self.cursor = None
                continue
            except AgentdUnreachable:
                self._agentd_unreachable()  # one warning; no per-host fallback
                time.sleep(5)
                continue
            self.mgr.remove_health_warning('CEPHADM_AGENTD_UNREACHABLE')
            self._apply(r['hosts'])
            self.cursor = r['seq']

    def _apply(self, hosts: List[Dict[str, Any]]) -> None:
        kick = health = False
        for h in hosts:
            host = h['host']
            if 'daemons' in h:
                count_old = len(self.mgr.cache.get_daemons_by_host(host))
                self.mgr._process_ls_output(host, h['daemons'])   # includes save_host()
                failed = {dd.name() for dd in self.mgr.cache.get_daemons_by_host(host)
                          if dd.status == DaemonDescriptionStatus.error}
                if failed != self.failed.get(host, set()):
                    self.failed[host] = failed
                    health = kick = True
                if len(self.mgr.cache.get_daemons_by_host(host)) != count_old:
                    kick = True
            if 'devices' in h:
                self.mgr.cache.update_host_devices(host, Devices.from_json(h['devices']).devices)
                self.mgr.cache.save_host_devices(host)
            if 'networks' in h:
                self.mgr.cache.update_host_networks(host, h['networks'])
            if 'facts' in h:
                self.mgr.cache.update_host_facts(host, h['facts'])
            if 'usage' in h:
                self._patch_usage(host, h['usage'])
            if 'liveness' in h:
                kick |= self._liveness(host, h['liveness'])  # agent_down set, offline_hosts_remove
            if 'config_deps' in h:
                self._config_delivered(host, h['config_deps'])
            if h.get('ack') == self.mgr.agent_cache.agent_counter.get(host):
                was = self.mgr.cache.all_host_metadata_up_to_date()
                self.mgr.cache.metadata_up_to_date[host] = True
                kick |= not was and self.mgr.cache.all_host_metadata_up_to_date()
        if health:
            self.mgr.set_failed_daemon_health_check(self.failed)  # keeps the spec_deleted filter
        if kick:
            self.mgr._kick_serve_loop()
```

The health check keeps its `spec_deleted` filter, which needs the specs.
That is why this sketch keeps it in cephadm rather than moving it to
agentd, which already maintains a failed set.

`HostCache` has no lock today: up to 30 cherrypy workers mutate it during
reports, relying on the GIL. A single feed thread is no worse.

## Acks after a deploy

```
cephadm serve loop               agentd                      agent on node-0042
_apply_service()
  deploy over SSH  --------------------------------------->  daemon starts
  append_tmp_daemon()
  metadata_up_to_date = False
  counter 7 -> 8
  POST /v1/refresh {node-0042: 8} ->
                                   counter 8 ------------->   rescan
                                   <-----------------------   report, ack 8
AgentdFeed poll <----------------  {daemons: [...], ack: 8}
  _process_ls_output()  (drops the tmp daemon)
  metadata_up_to_date = True, kick
```

The semantics are today's (`serve.py:1166`, `agent.py:890`). The difference
is the cost of each round trip: no RSA keygen and no Python thread per
host. With v2 agents the daemon change usually reaches agentd before the
refresh request does (1-3 ms change lag in the benchmark); the ack still
gates `all_host_metadata_up_to_date()`.

## Liveness and freshness

- `_agent_down(host)` returns agentd's liveness, keeping its existing
  draining and no-agent-deployed checks. With v1 and v2 agents agentd marks
  an agent down after refresh x multiplier without a message, as cephadm
  does today. With SWIM it is the suspicion timeout (16.8 s median at 4,000
  members in the benchmark).
- An up host's state is current by construction, since agentd would have
  reported a change. So `last_daemon_update` and `dd.last_refresh` no
  longer move on every report, and `orch ps` shows them as REFRESHED.
  cephadm should display the time of the latest poll for hosts whose agent
  is up. `host_needs_daemon_refresh()` only matters for hosts on the SSH
  fallback, so nothing else depends on the timestamp.

## Failure handling

| what fails | cephadm does |
|---|---|
| one agent | that host goes to SSH refresh, as today |
| agentd restarts | new epoch, so cephadm resyncs. agentd refills from agents: v2 agents send a full snapshot on reconnect, v1 agents send their next report (within 20 s). Hosts not yet heard from come back `unknown`, and cephadm keeps its replica for them. |
| agentd unreachable from cephadm | one `CEPHADM_AGENTD_UNREACHABLE` warning; keep the last replica; don't mark agents down. Today's per-host fallback would start an SSH refresh of every host at once. |
| mgr failover | the new mgr loads `host.<h>` from KV as today, resyncs (nearly empty answer), and carries on. No agent is reconfigured and no agent reconnects. |
| agentd's host | see below |

agentd is deployed by cephadm like other daemons, placed on the mgr hosts.
One instance is active; the others answer agents with a redirect to it. The
active cephadm assigns the role with a fencing epoch (`PUT /v1/role
{"active": true, "epoch": <mgrmap epoch>}`, `false` to the others), and an
instance refuses a role change carrying an older epoch. The active agentd
does not follow the active mgr, so a mgr failover moves no agents. Agents
get the list of agentd addresses in their config. Today's Python agent
takes a single `target_ip`, so in step 1 below it gets the active agentd's
address and is reconfigured only when agentd itself fails over.

## Persistence

agentd writes nothing to the mon; its state rebuilds from the agents.
cephadm keeps writing `host.<h>`, because it needs a warm cache after
failover and for hosts whose agent is down, but only when the feed changes
a host's daemons or devices. At steady state that write-on-change policy
wrote nothing in the B3 runs. `agent.<h>` is written when the counter,
port or config deps change, not per report, and `agent_timestamp` is no
longer persisted, since liveness lives in agentd.

## Cost on the mgr

Thread CPU per host update, from the A+ runs at 1,000 hosts:
`_process_ls_output` 2.3 ms, `save_host` 1.2 ms (plus about 8 ms of wall
time waiting on the mon), `Devices.from_json` 0.45 ms. Applying a change
to a host's daemons therefore costs cephadm about 3.5 ms of GIL. Today
every report costs that much (A+) or 15.2 ms (stock), whether or not
anything changed.

| hosts | reports/s today (20 s period) | stock mgr GIL for reports | host changes/s at 1% per period | cephadm GIL with agentd |
|---|---|---|---|---|
| 1,000 | 50 | 76% | 0.5 | under 0.2% |
| 4,000 | 200 | over capacity | 2 | under 1% |
| 50,000 | 2,500 | over capacity | 25 | about 9% |

The 1%-per-period change rate is the assumption used for the v2 benchmark,
not a measurement of a real cluster. Rolling upgrades and redeploys change
many hosts, but at the pace of cephadm's own SSH deploys. Usage patches add
work at hosts / 300 per second; they are attribute updates on existing
objects and have not been measured.

## Rollout

1. **Python agents unchanged.** agentd serves `/data` v1 (the prototype
   already does). cephadm points the agent config's `target_ip` and
   `target_port` at agentd, runs `AgentdFeed` instead of `HostData`, and
   sends acks and config through agentd. This removes all per-report work
   from the mgr; B3 carried 4,000 hosts at 3.2 ms p99 with 0.2 cores.
2. **v2 agents** (Rust, or an updated Python agent): a persistent
   connection, deltas, and change detection driven by podman and systemd
   events.
3. **SWIM:** liveness, plus broadcasts for "refresh all" and "agentd moved";
   heartbeats are dropped.

Each step stands on its own.

## Open questions

- Keep `HostCache` as a full replica, or have `orch ps` and `orch device ls`
  query agentd on large clusters? Python memory for about a million
  `DaemonDescription` objects (50,000 hosts) has not been measured and may
  be the next limit.
- Usage at a slow rate in the feed, or fetched on demand by `orch ps` and
  the autotuner?
- The node-proxy endpoints share the cherrypy server with the agent
  endpoint: move them to agentd as well, or leave them?
- Shipping agentd: a Rust binary in the ceph container image, deployed as a
  new cephadm daemon type.
