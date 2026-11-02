# A Rust agent that deploys with Quadlet instead of cephadm over SSH

Started as a design sketch; a working prototype now exists (status below).
It builds on [cephadm-integration.md](cephadm-integration.md) (agentd's
change feed, acks, failover). Code references are to ceph main (August
2026).

## Prototype status (2026-10-31)

Built:
- `rust/ceph-agent`, the agent;
- agentd's intent relay, join relay and SWIM member (`intents.rs`, `main.rs`);
- `rust/intentctl`, the mgr's side: tokens, approvals, signing, sealing;
- `vm/ceph/bootstrap.py`, which renders Ceph daemons the way cephadm would;
- `vm/ceph/mgr-agentd/`, an orchestrator mgr module.

It runs on three Fedora 44 VMs on bravo (`vm/`). Those VMs run a Ceph
Tentacle cluster (3 mons, 2 mgrs, 6 OSDs) deployed with no SSH to the
hosts. A scale test with up to 10,000 simulated hosts, and the fixes it
led to, is in [agent-scale-test.md](agent-scale-test.md): one agentd on 2
cores pushed a ceph.conf change to 140,000 daemons, all applied in 2.6 s,
using 0.01 cores when idle.

| piece | status |
|---|---|
| signed intents, blobs by hash, mgr-owned generations | done (Ed25519; agentd relays, the agent verifies) |
| blobs dropped once no current intent names them | done (reference counts, 10-minute grace) |
| files (atomic, mode, owner, drift repair), Quadlet units over D-Bus | done |
| restart keys, `reset-failed` before agent-initiated starts, systemd's give-up respected | done |
| removal by tombstone with data archived; strays reported | done |
| unit state from `PropertiesChanged`; usage from cgroups | done |
| join tokens: CA pinned by hash, MACed request and decision, mgr signs the host certificate | done |
| mutual TLS to agentd; host identity from the certificate's CN | done (keyring header still accepted) |
| secrets sealed to the host's X25519 key | done (agentd only holds ciphertext) |
| SWIM: liveness, pokes, leaving on shutdown; per-host digests sent straight to agentd | done |
| jobs (one run per id, recorded on disk) and `after_jobs` on units | done |
| self-upgrade with smoke test, OnFailure rollback and a commit deadline | done |
| agent rollouts paced by the mgr: canaries, hosts/s, hosts in flight, halt on failure | done (`intentctl push --canary/--rate`, `bench/scale/mgr.py agent-upgrade`) |
| image prefetch; pulls spread over `pull_spread_s` | done |
| Ceph daemons: mon (mkfs job), mgr, OSD (`ceph-volume` jobs, activate in ExecStartPre) | done, in `vm/ceph/bootstrap.py` |
| mgr side: orchestrator backend (`orch ps`, `host ls`, `daemon restart`, health checks) | done as a separate module, not in cephadm |
| maintenance mode | stops units; digest carries the mode; no `ceph orch host maintenance` yet |
| service specs and placement in the mgr; OSD spec previews; upgrades | not yet |

Measured on the VMs (3 hosts, image already pulled where noted):

| what | time |
|---|---|
| join three hosts with tokens | 3.3 s |
| Ceph from intents: quorum of 3 mons / active mgr / 6 OSDs up (images present) | 2.5 s / 4.9 s / 18.2 s |
| `ceph-volume lvm batch` job, 2 disks | 3.1 s per host |
| config-only change / restart, push to applied | 55 ms / 190 ms |
| daemon crash to the agent noticing | about 80 ms |
| frozen agent declared down by SWIM / host shutdown seen as "left" | 13.5 s / 0.4 s |
| missed status noticed through the digest | about 1 s |
| self-upgrade of three hosts (8 MB binary) / bad binary rolled back by systemd | 3–4 s / 0.3 s |
| `ceph orch daemon restart osd.4` to a new OSD process | 2.0 s |

Lessons the prototype taught:
- **`--ipc=host` for anything that runs LVM.** LVM waits for udev on SysV
  semaphores; without it, `lvcreate` hangs forever (cephadm does this).
- **AES-256 cephx keys.** Tentacle rejects the old AES-128 key type, so
  keys come from `ceph-authtool --gen-print-key`.
- **An empty mon data directory.** `ceph-mon --mkfs` exits 0 without doing
  anything when the directory isn't empty.
- **`RunInit=true`.** A daemon running as PID 1 ignores SIGTERM, so every
  stop takes 10 s (cephadm passes `--init`).
- **`reset-failed` before agent-initiated starts.** Without it, requested
  restarts count toward systemd's start limit.
- **Mgr restarts follow module changes.** A standby mgr loads its modules
  when it starts, so new module files must change the mgr's restart key.

## Today

Every change to a host is the mgr running the `cephadm` binary over SSH as
root:

- **Deploy and redeploy:** `_orch deploy` with an `exchange.Deploy` JSON
  document on stdin (`serve.py:1584`). The binary writes
  `/var/lib/ceph/<fsid>/<daemon>/unit.run` (a `podman run` command line),
  `unit.poststop` and the config files, and starts the template unit
  `ceph-<fsid>@<daemon>.service` (`templates/ceph.service.j2`: `Type=forking`,
  `KillMode=none`, pid and cid files). OSDs run `ceph-volume lvm activate`
  as a prestart container inside `unit.run` (`cephadm.py:1450`). Init
  containers and sidecars get their own unit templates.
- **Everything else:** `ceph-volume` (9 call sites), `check-host`,
  `prepare-host`, `pull`, `inspect-image`, `registry-login`, `rm-daemon`,
  `unit`, `deploy-file` / `remove-file`, `host-maintenance`,
  `disk-rescan`, and `_orch check-online`, which `OfflineHostWatcher` runs
  every 20 s against hosts with nfs, haproxy or keepalived daemons.

The mgr holds an SSH key that is root on every host.

## Shape

```
                 mgr (cephadm)                         agentd
  specs -> placement -> per-host intent  --signed-->  stores, relays,
           (generation N, rendered units)             notifies agents
        <-- change feed: applied generation,          <-- reports
            unit states, job results                   <-- SWIM digests
                                                            |
  host ---------------------------------------------------- | -------------
   ceph-agent (Rust, Type=notify, WatchdogSec)              |
     intent -> files + Quadlet units -> systemd Reload/Start/Restart/Stop
     systemd D-Bus PropertiesChanged -> unit state -> report
     cgroup v2 files -> usage
     SWIM member <-> other agents and agentd
   /etc/containers/systemd/ceph-<fsid>-osd.17.container -> osd.17 service
```

- cephadm stops issuing commands and starts publishing **desired state per
  host**, versioned by a generation. The agent makes the host match it and
  reports what it applied.
- systemd and Quadlet run the daemons. The agent is in neither the data
  path nor the boot path: units are enabled under `ceph-<fsid>.target`, so
  daemons start at boot and are restarted by systemd whether or not the
  agent is running.
- systemd is also the source of daemon state. The agent subscribes to unit
  changes over D-Bus instead of polling `cephadm ls`.
- SWIM carries host liveness, and a small per-host digest that lets agentd
  notice anything it missed.

## The intent document

```json
{"host": "node-0042", "fsid": "2d6f...", "generation": 57,
 "mode": "normal",
 "agent": {"version": "20.3.0"},
 "prefetch": ["quay.io/ceph/ceph@sha256:9e1f..."],
 "files": {
   "/etc/ceph/ceph.conf": {"sha256": "6a1e...", "mode": "0644"},
   "/var/lib/ceph/2d6f.../osd.17/keyring": {"sealed": "...", "mode": "0600", "owner": "167:167"}
 },
 "units": {
   "osd.17": {"quadlet": "91c3...", "state": "running", "restart_key": "4be0..."},
   "crash.node-0042": {"quadlet": "c07d...", "state": "running", "restart_key": "e5a2..."}
 },
 "remove": ["mgr.node-0042.xyzabc"],
 "signature": "..."}
```

- **Content by hash.** Files and Quadlet texts are blobs the agent fetches
  once from agentd (`GET /v1/blobs/<sha256>`) and caches. Most of them are
  the same on every host (`ceph.conf`, the crash and node-exporter units
  apart from their names), so a cluster-wide change is one new blob plus
  small per-host documents. agentd counts which blobs the hosts' current
  intents name and drops the others after a grace period (10 minutes by
  default), so the Quadlet texts of old images and old agent binaries
  don't pile up.
- **Secrets are sealed** to the host's key (keyrings, registry
  credentials), so agentd relays ciphertext it cannot read.
- **Signed by the mgr.** The agent checks the signature against the
  cluster CA and accepts only a higher generation. agentd and the network
  can delay an intent but can't forge or replay one.
- **`restart_key`** is cephadm's existing reconfig-vs-redeploy decision
  (`_check_daemons` comparing deps) in a form the agent can act on: when
  it changes, the agent restarts the unit; when only files change, it
  writes them and leaves the daemon running.
- **Removal is explicit.** A unit leaves only through `remove`. A unit that
  is on the host but missing from the intent is reported as a stray, as
  today, and left alone, so a truncated or corrupted intent can't delete
  daemons. Data directories are archived to `removed/`, as `rm-daemon`
  does now, never deleted.
- **Kept on the host** (`/var/lib/ceph/<fsid>/agent/intent.json`), so after
  a reboot the agent reconciles from the last verified intent before it
  reaches agentd.

## What a unit looks like

Rendered by the mgr, written by the agent to
`/etc/containers/systemd/ceph-<fsid>-osd.17.container`. Abbreviated: the
flags and mounts are today's `unit.run` for an OSD, translated.

```ini
[Unit]
Description=Ceph osd.17 for 2d6f...
After=network-online.target local-fs.target time-sync.target
Wants=network-online.target
PartOf=ceph-2d6f....target
Before=ceph-2d6f....target
StartLimitIntervalSec=30min
StartLimitBurst=5

[Container]
ContainerName=ceph-2d6f...-osd-17
Image=quay.io/ceph/ceph@sha256:9e1f...
Network=host
PodmanArgs=--privileged --pids-limit=-1 --entrypoint=/usr/bin/ceph-osd
Exec=-n osd.17 -f --setuser ceph --setgroup ceph --default-log-to-file=false --default-log-to-stderr=true
Environment=CONTAINER_IMAGE=quay.io/ceph/ceph@sha256:9e1f... NODE_NAME=node-0042
Volume=/var/lib/ceph/2d6f.../osd.17:/var/lib/ceph/osd/ceph-17:z
Volume=/var/lib/ceph/2d6f.../osd.17/config:/etc/ceph/ceph.conf:z
Volume=/var/run/ceph/2d6f...:/var/run/ceph:z
Volume=/var/log/ceph/2d6f...:/var/log/ceph:z
Volume=/dev:/dev
Volume=/run/udev:/run/udev
Volume=/run/lvm:/run/lvm

[Service]
# today's prestart container in unit.run
ExecStartPre=/usr/bin/podman run --rm --privileged ... --entrypoint /usr/sbin/ceph-volume quay.io/ceph/ceph@sha256:9e1f... lvm activate 17 <osd-fsid> --no-systemd
Restart=on-failure
RestartSec=10s
TimeoutStartSec=200
TimeoutStopSec=120
LimitNOFILE=1048576
LimitNPROC=1048576

[Install]
WantedBy=ceph-2d6f....target
```

Quadlet generates `ceph-<fsid>-osd.17.service` from this on `daemon-reload`.
The generated unit is `Type=notify` (conmon reports when the container is
up) and stops the container itself, which replaces `Type=forking`,
`KillMode=none`, the pid and cid files, and the `unit.stop` /
`unit.poststop` scripts. Init containers and sidecars become units of
their own, ordered with `Before=` / `After=` and `PartOf=`, as their
templates already are.

**Where rendering lives.** Today the per-daemon-type container logic
(mounts, flags, init containers, sidecars) is in the `cephadm` binary
(`cephadmlib/daemons/`). This sketch moves it into the mgr module, which
already has the specs, the config, and the host facts that the rendering
depends on, such as podman version and the image's uid and gid. The agent
then knows nothing about Ceph daemon types: it writes files, manages
units, runs jobs and reports. It changes rarely, and a new daemon type
needs no agent release.

## The agent's loop

```text
loop select {
  intent from agentd  -> verify signature, generation > applied; persist
  systemd signal      -> update observed unit state; report a delta
  SWIM event          -> membership, broadcasts (e.g. "generation bumped")
  every 60 s          -> drift check: file hashes, unit files, unit states
}
on a newer generation, or drift:
  1. pull images (prefetch + units), a few at a time, with jitter
  2. write changed files: tmp + fsync + rename, owner, mode, SELinux label
  3. write changed Quadlet files, delete removed ones
  4. one systemd Reload() if any unit file changed
  5. per unit: StartUnit / RestartUnit (restart_key changed) / StopUnit
     (state stopped) / StopUnit + archive (remove)
  6. wait for each job's JobRemoved result; report applied generation and
     per-unit results ("failed: image pull", "start timeout", ...)
```

D-Bus calls are on `org.freedesktop.systemd1.Manager` (`Subscribe`,
`Reload`, `StartUnit`, `RestartUnit`, `StopUnit`, `ResetFailedUnit`), and
state arrives as `PropertiesChanged` on each unit (zbus, pure Rust). The
agent itself runs `Type=notify` with `WatchdogSec`, and pets the watchdog
from its own loops, not from remote reachability, so an agentd outage
doesn't make systemd restart every agent.

## Daemon state from systemd

| systemd (`ActiveState` / `SubState` / `Result`) | what cephadm shows |
|---|---|
| `active` / `running` | running |
| `activating` / `auto-restart` | restarting; invisible today between reports |
| `failed`, `Result=exit-code`, `signal`, `start-limit-hit` | error, with the reason and `NRestarts` |
| `inactive`, and the intent says stopped | stopped |

- Changes reach agentd within milliseconds of systemd seeing them (1-3 ms
  change lag in the v2 benchmark), then the change feed. Today a crash
  waits for the next agent report, up to 20 s, plus whatever backlog the
  mgr has.
- `NRestarts` makes flapping visible, so cephadm could raise a new health
  warning for a daemon that keeps restarting.
- Container details (id, image id, version, start time) come from one
  podman inspect per start, not per report.
- Usage comes from the unit's cgroup (`memory.current`, `cpu.stat` under
  `system.slice/<unit>`) instead of podman queries.

## Linked to SWIM

| state | how it shows | cephadm today |
|---|---|---|
| up | SWIM member alive | report within refresh x multiplier |
| shutting down | the agent takes a logind delay inhibitor, and on `PrepareForShutdown` leaves SWIM and tells agentd: immediate | looks like a failure, 60 s |
| maintenance | the intent sets `mode: maintenance`; the agent stops `ceph-<fsid>.target`, stays a member, and says so in its digest | `host-maintenance` over SSH |
| failed | SWIM suspicion runs out: 16.8 s median at 4,000 members | 60 s, or 20 s SSH polling for nfs, haproxy and keepalived hosts |

- SWIM replaces `_agent_down()`'s timer, `OfflineHostWatcher`'s SSH probes,
  and `check-online`, for every host rather than only the hosts running
  rescheduled services.
- **Per-host digest:** each agent sends agentd's SWIM member
  `{applied_generation, state_version, failed_units}`, a dozen bytes, as one
  UDP datagram with every report. If a digest is ahead of what agentd
  holds, agentd asks that agent to resync. A dropped connection or a lost
  report then can't hide a change, and the v2 connection needs no
  heartbeats. (The prototype first gossiped digests to every member. Only
  agentd reads them, and at 5,000 hosts gossiping them cost every member
  O(cluster) per change; see agent-scale-test.md.)
- **Fan-out:** a cluster-wide change (new mon addresses in `ceph.conf`, CA
  rotation) is a SWIM broadcast "generation bumped". Agents fetch their
  intents with jitter. An upgrade doesn't use this, because cephadm moves
  it host by host.
- **Gossip is encrypted** with a shared AEAD key, which is how Serf does
  it, rotated through the intent.

## Imperative work: jobs

Not everything is desired state. These become jobs, defined as
`{id, kind, args, timeout}` with results in the change feed. The agent
records each job on disk, so it reports results after a restart and never
runs the same id twice.

| today over SSH | with the agent |
|---|---|
| `_orch deploy`, `update-osd-service`, `unit` | units in the intent |
| `rm-daemon` | `remove` in the intent |
| `deploy-file`, `remove-file`, `registry-login` | files and sealed secrets in the intent |
| `pull`, `inspect-image` | `prefetch`; image digests in facts |
| `host-maintenance` | `mode` in the intent |
| `check-host`, `prepare-host`, `check-online` | facts and SWIM |
| `ls`, `gather-facts`, `list-networks`, `list-rdma` | agent reports, already today |
| `ceph-volume` (create OSDs, preview, zap), `disk-rescan`, `adopt` | jobs, in the ceph container as now |
| `setup-ssh-user` | gone |

OSD creation stays two steps, as today: a `ceph-volume lvm batch` job
creates the OSDs and returns their ids, and cephadm then adds their units
to the intent.

## Joining a host without SSH

1. `ceph orch host join-token --ttl 1h` prints a one-time token with the
   CA's fingerprint, as `kubeadm join` does.
2. On the host: install `ceph-agent`, a static binary with its unit, and
   run `ceph-agent join --token ... --endpoint <agentd>`.
3. The agent generates a key and sends a CSR, its facts and the token.
   cephadm checks the token, adds the host to the inventory (today's
   `orch host add`), and signs the certificate. The agent receives its
   certificate, the CA, the gossip key, SWIM seeds and generation 1.
4. cephadm refuses to place daemons on a host whose facts fail its checks,
   such as missing podman or lvm2, or no time sync.

`cephadm bootstrap` still runs locally on the first host and installs the
agent there. The agent upgrades itself from the ceph image when
`agent.version` changes: it extracts the new binary, switches a symlink and
restarts through systemd. An `OnFailure=` unit switches back if the new
version doesn't rejoin SWIM.

## Trust

| | today | with agents |
|---|---|---|
| mgr compromised | root on every host (SSH key) | root on every host (intent signing key) |
| agentd compromised | n/a | can delay intents, can't forge them or read secrets |
| network position | SSH host keys | mTLS to agentd, signed intents, encrypted gossip |
| a host compromised | can't reach other hosts | same; its certificate only speaks for itself |

The signing key lives where the SSH key lives now, in the mgr's KV store.

## What changes in cephadm

- `_create_daemon()` writes the unit into the host's intent, marks the
  daemon pending as `append_tmp_daemon()` does now, and returns.
  Completion and errors come back through the feed. Today the serve loop
  waits on SSH sessions.
- `_check_daemons()` reconfigs and redeploys become `restart_key` and file
  changes.
- `_write_client_files()` becomes files in the intents of `_admin` hosts.
- `upgrade.py` keeps its ordering and ok-to-stop checks. It sets
  `prefetch` everywhere at the start, then changes images host by host and
  waits for each applied generation instead of each SSH call.
- `_run_cephadm()` stays for hosts without the new agent, so mixed
  clusters work during the migration, and as an optional break-glass path.

## Rollout

1. **The Rust agent reports** (v2 and SWIM); deploys stay on SSH.
   [cephadm-integration.md](cephadm-integration.md) covers this step.
2. **The agent executes deploys** by running `cephadm _orch deploy`
   locally with the same `exchange.Deploy` document cephadm sends over SSH
   today. Rendering and units are unchanged, so this is the smallest step
   that takes SSH out of steady-state deploys.
3. **Quadlet backend:** the mgr renders units, and the agent watches systemd
   over D-Bus. Daemons move at their next redeploy, for example during an
   upgrade, when they restart anyway.
4. **Jobs and join tokens.** The cluster's SSH key becomes optional.

## Constraints and open questions

- **Quadlet needs podman 4.4 or later.** That rules out Ubuntu 22.04
  (podman 3.4); RHEL 9.2+ and Ubuntu 24.04 qualify. Quadlet doesn't drive
  docker, which cephadm supports today. Hosts that can't use Quadlet stay
  on step 2, or docker support goes.
- **Unit names change:** Quadlet names the service after the file
  (`ceph-<fsid>-osd.17.service` instead of `ceph-<fsid>@osd.17.service`).
  Template Quadlet units in newer podman might keep today's names; to be
  checked. Docs and operator habits depend on the names.
- **Rendering in the mgr** means moving `cephadmlib/daemons/` out of the
  binary. How much of the binary remains for `bootstrap` and `shell`?
- **ceph-volume orchestration** is the largest piece of imperative work and
  the least declarative. Jobs can wrap it, but it may deserve its own
  design.
- **What to measure:**
  - a cluster-wide config change at 4,000 hosts: SSH today vs a
    generation broadcast;
  - daemon crash to health warning: next report vs a systemd signal;
  - a deploy's critical path in the serve loop.

  Simulated agents on bravo can cover the first two with a fake systemd
  backend.
