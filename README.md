# agentbench: cephadm agent load on ceph-mgr, and a Rust endpoint prototype

Simulates thousands of cephadm agents reporting host metadata to the
cephadm mgr module's agent endpoint (`POST https://<mgr>:7150/data`,
`HostData.index()` in `src/pybind/mgr/cephadm/agent.py`). It measures how
many reports the mgr can absorb and at what latency, and how responsive the
mgr and mon stay for other requests at each load level. `agentd` is a
standalone Rust implementation of the same endpoint for A/B comparison.

## Layout

| path | what |
|---|---|
| `cluster/up.sh` | fresh vstart cluster (1 mon, 1 mgr, 3 memstore OSDs) with cephadm + prometheus, `use_agent`, orchestrator paused, SSH pointed at a closed port |
| `cluster/seed.py` | writes N fake hosts (inventory, host cache, agent keyring/port/counter/config deps) to the cephadm KV store and fails the mgr over to load them; writes `agents/hosts.json` and a listener cert |
| `cluster/certs.py` | TLS material for agentd: the mgr endpoint's own RSA-4096 cert, an ECDSA variant, a mgr->agent client cert |
| `cluster/instrument.sh on/off` + `agentbench_instr.py` | wraps the cephadm agent path (handler, `_process_ls_output`, `save_host`, `set_store`, health-check scans, ...) and records wall and thread-CPU time per call to `/ab/run/instr.json` |
| `rust/agentsim` | agent simulator / load generator |
| `rust/agentd` | Rust prototype of the agent endpoint |
| `rust/proto` | shared payload generator, TLS helpers, v2 message builders, SWIM member (`proto::swim`, foca) |
| `rust/swimsim` | N simulated agents as SWIM members against `agentd --swim`: join, steady traffic, signal broadcasts, failure detection |
| `bench/run.py` | one scenario: seed, prime, agentsim + probes + sampler over the same window |
| `bench/probe.py` | latency of other requests during the run (`orch host ls`, `orch ps --hostname`, `balancer status`, `pg stat`, mon `health`, prometheus scrape, agentd query) |
| `bench/sample.py` | per-second CPU/RSS of mgr, mon, OSDs, agentd, agentsim; busiest mgr threads |
| `bench/report.py` | tables + `digest.json` from result directories |
| `bench/sweep_baseline.sh`, `bench/sweep_agentd.sh` | the A and B sweeps |
| `bench/sweep_aplus.sh`, `cluster/patch_aplus.py` | A+: the mgr with per-host failed-daemon change detection |
| `bench/sweep_v2.sh`, `cluster/gen_hosts.py` | v1 vs v2 agent protocol against a cluster-free `agentd --hosts-file` (4k-50k hosts) |
| `bench/sweep_swim.sh` | SWIM liveness/signals at 1k-4k members |
| `bench/scale/run.sh` | ceph-agent + agentd scale test: N simulated hosts (`ceph-agent sim`), join, deploy, change, rack freeze, agentd restart, image changes with blob cleanup, agent rollouts ([docs/agent-scale-test.md](docs/agent-scale-test.md)) |
| `bench/scale/mgr.py`, `follow.py`, `until.py`, `analyze.py`, `swimrate.py` | its mgr side (render, seal, sign, publish, paced agent rollout), change-feed followers, phase waits, summary, SWIM traffic per 10 s |
| `bench/scale/blobmem.sh`, `blobmem.py` | agentd memory per stored blob |
| `bench/scale/threads.py`, `downs.py`, `meltdown.sh`, `hogtest.sh`, `hog.py`, `meltdown_table.py` | per-thread CPU of the simulator (SWIM vs agent threads), hosts down over time after a phase (with the members' rejoins and Lifeguard counters), the reruns of the 5,000-host all-at-once upgrade, a paced rollout with the simulator's CPUs taken away for 70 s, one summary row per run |
| `rust/vendor/foca/` | foca 2.0.0 with Lifeguard and `restore_membership` added (`PATCHES.md`) |
| `vm/paced-upgrade.sh` | agent rollout on the VMs with `intentctl push --canary/--rate/--max-in-flight` |
| `bench/build_report.py`, `report_template.html`, `charts.js` | the published report (`digest.json` + `collect_proto.py` output) |
| `docs/cephadm-integration.md` | sketch: how cephadm would consume agentd's state (change feed, acks, liveness, failover) |
| `rust/ceph-agent` | host agent: signed per-host intents as Quadlet units over systemd's D-Bus API, unit state, jobs, join, sealed secrets, SWIM, self-upgrade (see below) |
| `rust/intentctl` | the mgr's side: join tokens and approvals, renders/seals/signs/stores intents, status, events |
| `vm/` | QEMU/KVM test VMs on bravo (rootless `agentvm` container, Fedora 44 guests): `demo.sh`, `join-agents.sh`, `agent-selfupgrade.sh` |
| `vm/ceph/bootstrap.py` | deploys Ceph (mons, mgrs, OSDs) on the VMs through intents only; `--reset` removes it |
| `vm/ceph/mgr-agentd/`, `vm/ceph/orch-demo.sh` | `agentd` orchestrator mgr module: `orch ps` / `host ls` / `daemon restart` and health checks from agentd |
| `docs/agent-executes-intent.md` | sketch: a Rust agent that replaces deploys over SSH with signed per-host intents, Quadlet units, systemd D-Bus state and SWIM liveness |

## Environment (bravo)

* Container `agentbench` (rootless podman, host network, `--init`), image
  `localhost/agentbench:tooling` = the `cephdev` build container + asyncssh
  and rustup. Mounts: `~/src/mdsbench/ceph` read-only at `/work/ceph` (the
  ceph build, main 9e49f5cb896), `~/src/agentbench` at `/ab`, and a 24G
  tmpfs at `/mon`.
* The cluster lives on tmpfs (`/mon/vcluster`) so the mon's paxos commits
  are not at the mercy of the box's consumer NVMe (fsync stalls of several
  seconds were observed with the mon on disk; production mons sit on PLP
  SSDs). Results from the disk-backed attempt are in `results-disk/`.
* Separate container from `cephdev` so `stop.sh`'s pkill cannot touch the
  mdsbench cluster. Ports: mon 45000, agent endpoint 7150, prometheus
  19283, agentd 7160 (TLS) / 7161 (admin), simulated agent listeners
  20000+i.
* CPU split: cluster + agentd on CCX0 (cpus 0-7,16-23), agentsim, probes
  and sampler on CCX1 (8-15,24-31).

```
ssh bravo
podman exec -it agentbench bash
/ab/cluster/up.sh && /ab/cluster/instrument.sh on
python3 /ab/cluster/seed.py --hosts 1000 && python3 /ab/cluster/certs.py
python3 /ab/bench/run.py --name A-agents-n1000 --agents 1000
python3 /ab/bench/run.py --name B-agents-n1000 --agents 1000 --no-seed --agentd '--persist writeback'
python3 /ab/bench/report.py /ab/results 'A-*' 'B*'
```

## What a simulated agent does

Per report, like `CephadmAgent.run()` in `src/cephadm/cephadm.py`: new TCP
+ TLS connection (no session resumption, like urllib), `POST /data` with
`host`, `ls` (agent, crash, node-exporter, ceph-exporter and 16 OSDs by
default), `networks`, `facts` (HostFacts JSON string) and `volume`
(ceph-volume inventory JSON string, 18 devices), `ack`, `keyring`, `port`;
about 50 KB per report. Usage fields (memory, cpu, load, uptime) change on
every report. 10 s timeout. Then it sleeps `period - int(avg of last 3
iteration times)`. Each agent also runs the mgr->agent listener (mutual
TLS, 10-byte length + JSON counter, `ACK`) and reports immediately when the
counter changes.

## Benchmark settings that differ from a production cluster

* Orchestrator paused (no service application, which would try to deploy
  over SSH); the serve loop's refresh/health part still runs.
* `agent_down_multiplier` raised so missed reports never trigger SSH
  fallback refreshes; report gaps are measured client-side instead
  (`max_report_gap` in summary.json, `agents_over_3x_period` = agents the
  mgr would have marked down at the default multiplier).
* `mon_config_key_max_entry_size` raised to 4 MiB: cephadm stores the whole
  inventory in one key, which exceeds the 64 KiB default beyond roughly
  800 hosts.
* Instrumentation is on for the A runs.

## Agent protocol v2 and SWIM

`agentsim --proto v2` agents keep one TLS connection to agentd
(`POST /v2/report`, HTTP keep-alive), authenticate with their keyring once,
send a full snapshot, then versioned deltas (Poisson `--change-rate` per
host per period), usage every `--usage-every` periods and heartbeats.
agentd applies a delta only on its base version, else answers 409 and the
agent resyncs. `agentd --hosts-file hosts.json --persist none` runs without a
cluster (`cluster/gen_hosts.py`).

`agentd --swim` runs a SWIM controller member (foca, LAN defaults) at
127.3.0.1:7946; `swimsim --agents N` runs N members at 127.2.x.y. Admin:
`POST /swim/broadcast` (signal), `GET /swim/down`. One process cannot
simulate 10,000 members (foca's linear member scan times N members); 4,000
works on 16 threads.

## ceph-agent: deploying with signed intents and Quadlet

`ceph-agent` replaces deploys over SSH (docs/agent-executes-intent.md). The
mgr (here `intentctl`) publishes a signed per-host intent in agentd: Quadlet
`.container` texts and config files by SHA-256, desired unit states, restart
keys and removals. The agent long-polls agentd for it, verifies the Ed25519
signature, fetches blobs, pulls images, writes files atomically and the
Quadlet files to `/etc/containers/systemd`, runs `daemon-reload`, and
starts, restarts or stops units over D-Bus (zbus). It subscribes to
`PropertiesChanged` on its units and posts status to agentd on every
material change, and every 30 s otherwise. agentd keeps a log of unit
transitions and applied generations (`GET /v1/events`).

Rules: only `remove` deletes a unit (data archived to
`/var/lib/ceph/<fsid>/removed/`); unknown units are reported as strays;
starts and restarts the agent asks for run `reset-failed` first, so only
systemd's automatic restarts count toward `StartLimitBurst`; a unit
systemd gave up on stays failed until its restart key changes. Daemons run
under systemd, not the agent: they start at boot via
`ceph-<fsid>.target` and are restarted by systemd while the agent is down.

Test VMs (bravo has no sudo and no qemu, so the VMs run in a rootless
podman container with `/dev/kvm`; a bridge, dnsmasq and NAT live in the
container's network namespace, VMs boot UEFI):

```
ssh bravo
~/src/agentbench/vm/container.sh                 # agentvm container + packages
podman exec agentvm /ab/vm/net.sh                # br0 10.77.0.1/24, taps, DHCP, NAT
podman exec agentvm /ab/vm/mkvm.sh               # Fedora 44 Cloud disks + cloud-init seeds
podman exec agentvm /ab/vm/start.sh              # node1-3 at 10.77.0.11-13
podman exec agentbench bash -lc 'cd /ab/rust && cargo build --release -p agentd -p ceph-agent -p intentctl'
podman exec agentvm /ab/vm/cluster-init.sh       # CA, agentd cert, keyrings, intent key
podman exec agentvm /ab/vm/agentd.sh             # agentd on 10.77.0.1:7160/7161
podman exec agentvm /ab/vm/join-agents.sh        # tokens -> ceph-agent join -> approve; agent as a Type=notify unit
podman exec agentvm /ab/vm/demo.sh               # deploy, config, restart, crash, secret, swim, partition, shutdown, ...
podman exec agentvm python3 /ab/vm/ceph/bootstrap.py   # a Ceph cluster from intents
podman exec agentvm /ab/vm/ceph/orch-demo.sh     # ceph orch backed by agentd
podman exec agentvm /ab/vm/agent-selfupgrade.sh /path/to/ceph-agent 0.2.x
```

Joining: `intentctl token` issues a one-time token bound to a host name
(agentd's address, the CA's hash, a secret only the mgr and the host know);
`ceph-agent join <token>` pins the CA, sends a CSR and an X25519 sealing key
MACed with the secret; `intentctl approve` checks token and MAC, signs the
certificate (CN = host), and answers with a MACed decision carrying the
intent keys and SWIM settings. agentd only relays both. Files marked
`secret` are sealed to the host's key; generations are owned by the mgr
(`mgr.json`), so they survive agentd restarts.

SWIM (`agentd --swim-bind`): agents gossip a digest (applied generation,
state version, failed units, mode); agentd pokes hosts whose digest is ahead
of their last status (through the long-poll and by gossip) and logs
liveness (`up`/`down`/`left`; agents leave on host shutdown via a logind
inhibitor). With SWIM, periodic status drops to every 300 s.

Jobs run once per id in a container (`podman_args`, `dirs`, timeouts), with
results in the status; units can wait for jobs (`after_jobs`). Self-upgrade:
the intent's `agent` names a binary blob; the agent smoke-tests it, switches
`/usr/local/bin/ceph-agent` (a link into `/usr/local/lib/ceph-agent/`) and
exits; systemd's `OnFailure=` rolls back a binary that can't start, and one
that can't reach agentd within 2 minutes rolls itself back.

## Scale test of ceph-agent and agentd

`ceph-agent sim` runs thousands of agents in one process, each a whole
agent over in-memory systemd with its own TLS connections from its own
loopback address. `ceph-agent sim-join` joins them with tokens first.
`bench/scale/run.sh N` drives a run inside the agentbench container and
writes `results/scale/n<N>/`; results and findings are in
[docs/agent-scale-test.md](docs/agent-scale-test.md).

```
podman exec agentbench /ab/bench/scale/run.sh 5000                          # with SWIM, 40 hosts/s start
podman exec agentbench env SWIM=0 RAMP_RATE=0 /ab/bench/scale/run.sh 10000  # HTTP path, mass start
podman exec agentbench env TAG=upgrades PHASES="deploy ceph-upgrade agent-upgrade-paced agent-upgrade-all" \
    BLOB_GRACE=120 UPGRADE_RATE=20 /ab/bench/scale/run.sh 5000              # blob cleanup, agent rollouts
python3 bench/scale/analyze.py results/scale
python3 bench/scale/swimrate.py results/scale/n5000-upgrades agent-upgrade-all
python3 bench/scale/downs.py results/scale/n5000-freeze-upgrades-64k agent-upgrade-all-freeze
```

`PHASES` picks the phases after join and start (default `deploy steady
change freeze restart`); `ceph-upgrade` may be named more than once, each
time with a new image. `TAG` suffixes the result directory.
`AGENT_SIZE` sets the agent binary a rollout ships (default 8 MB) and
`SWIM_THREADS` the SWIM threads per simulator process (default 1).
The agents' SWIM members keep their identity and membership across a
restart; `SWIM_RENAME=1` and `SWIM_VIEW=0` make them start as they did
before (a new identity, an empty membership). `SWIM_LIFEGUARD=1` turns on
Lifeguard in every member, agentd's included.

Stop the VMs first (`podman exec agentvm /ab/vm/stop.sh`): the run uses
every core except the 4 threads for agentd and the 4 for the mgr side.

