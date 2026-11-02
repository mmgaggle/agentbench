# Scale test: ceph-agent and agentd (2026-10-31)

How far one agentd and the intent protocol go, measured with up to 10,000
simulated hosts (140,000 daemons) on bravo, and what had to change for them
to get there. The design is in [agent-executes-intent.md](agent-executes-intent.md).

## Setup

- **Hosts** are the real agent code: `ceph-agent sim` runs many agents in
  one process, each with its own TLS connections from its own loopback
  address (127.2.x.y), reconciler, reporter, job runner and SWIM member, over
  the in-memory systemd backend. Every host joined with a token (`ceph-agent
  sim-join`), so it holds a certificate signed by the mgr and a sealing
  key.
- **Each host** runs 12 OSDs, crash and node-exporter: 14 Quadlet units,
  13 keyrings sealed to the host, and one shared ceph.conf. The ceph daemons
  restart when ceph.conf changes.
- **The mgr side** is Python, as cephadm would be (`bench/scale/mgr.py`). It
  renders, seals and signs intents and publishes them over one keep-alive
  connection. Two followers read agentd's change feed, side by side:
  - the orchestrator module's read path as it was (`follow.py`): a status
    request per changed host, plus the full summary on liveness, intent and
    applied events;
  - a lean one (`follow.py --batch`): one batch status request, plus
    `/v1/liveness`.
- **CPUs** (Ryzen AI Max+ 395, 16 cores / 32 threads):

  | what | cores (threads) |
  |---|---|
  | agentd | 2 (4) |
  | the mgr side | 2 (4) |
  | the simulated hosts | 12 (24) |

  12 agent processes plus one 40-host "rack".
- **Phases** (`bench/scale/run.sh`):
  1. **join:** every host joins at once.
  2. **start:** agents start at 40 hosts/s with SWIM, or all at once without.
  3. **deploy.**
  4. **steady:** 3 minutes idle.
  5. **change:** a new ceph.conf, so every ceph daemon restarts.
  6. **freeze:** the rack is stopped with SIGSTOP, then thawed.
  7. **restart:** agentd is killed with `kill -9` and restarted; the mgr
     re-teaches members and republishes.

## Results

1,000–5,000 hosts run with SWIM. 10,000 hosts run without simulated SWIM
members; see "What one machine can't simulate" below. Times are wall clock.
"Applied" means agentd received the host's status for the new generation.

| | 1,000 | 2,500 | 5,000 | 10,000 |
|---|---|---|---|---|
| daemons | 14,000 | 35,000 | 70,000 | 140,000 |
| join storm: all hosts joined | 2.2 s | 3.3 s | 6.9 s | 9.3 s |
| deploy: mgr publishes (4 procs) | 1.0 s | 2.5 s | 5.1 s | 10.4 s |
| deploy: all hosts applied ¹ | 6.4 s | 16.4 s | 34.3 s | 66.8 s |
| ceph.conf change: mgr publishes (1 proc, CPU) | 0.14 s (0.10 s) | 0.56 s (0.30 s) | 1.17 s (0.62 s) | 2.2–2.4 s (1.1 s) |
| ceph.conf change: all hosts applied | **0.21 s** | **0.73 s** | **1.85 s** | **2.55 s** |
| per host, store to applied p99 | 0.11 s | 0.20 s | 0.43 s | 0.45–0.56 s |
| frozen rack (40 hosts) all declared down | 13.2–17.1 s | 14.9–19.5 s | 16.4–20.4 s | — |
| false suspicions of other hosts | 0 | 0 | 0 | — |
| thawed rack up | 0.47 s | 0.39 s | 0.49 s | — |
| agentd `kill -9`: every host reconnected and reported (and SWIM up) | 3.4 s | 5.4 s | 4.9 s | 3.0 s ² |
| agentd CPU, steady | 0.002 cores | 0.003 | 0.008 | 0.010 |
| agentd CPU, whole change | 0.4 s | 0.8 s | 2.5 s | 3.4 s (peak 1.4 cores) |
| agentd RSS after deploy | 0.3 GB | 0.7 GB | 1.5 GB | 2.4 GB |
| per agent, steady: CPU / SWIM | 0.008% / 2.3 pkt/s, 107 B/s | 0.006% / same | 0.008% / same | 0.002% (no SWIM) |

¹ Limited by the simulator: every host's apply (about 50 ms of CPU each:
fetch 27 blobs, decrypt 13 keyrings, write 28 files, start 14 units) runs
on 24 threads at once. On real hosts each one has its own CPU. On the VMs,
a config-only change took 55 ms and a restart 190 ms from push to applied.

² With the 8,192-entry listen backlog. With tokio's default of 1,024,
10,000 agents starting at once took 4.6–7.8 s and the reconnect 6.0 s,
because dropped SYNs are retried after 1 s and 3 s. With the larger
backlog, the cold start took 1.7 s.

A real agent idling on the VMs, with real systemd and Ceph daemons, used
0.18% of a core (1.07 s of CPU in 10 minutes), 9 MiB and 3 threads. That
was the agent before the "report only on change" fix below.

### The mgr's read path

At 10,000 hosts:

| phase | module's path: CPU / requests / data / worst lag | batch path |
|---|---|---|
| deploy | 6.3 s / 20,090 / 293 MB / 1.3 s | 2.0 s / 201 / 96 MB / 1.1 s |
| ceph.conf change | 1.8 s / 11,654 / 96 MB / 3.2 s | 0.7 s / 9 / 89 MB / 1.6 s |
| agentd restart + republish | 13.8 s / 30,106 / 545 MB / 2.2 s | 4.8 s / 223 / 181 MB / 1.3 s |

The module now uses the batch path (`vm/ceph/mgr-agentd/module.py`).

## What the test found

Fixed, with before and after:

1. **Status after an agentd restart came 30 s late.** An agent reports
   after reconnecting, but only once its first long-poll returned. The
   first poll after connecting now doesn't wait. All hosts reported again:
   30 s before, 2–3 s after.
2. **A restarted agentd relearned SWIM membership only by chance**, as
   members happened to probe it. At 200 hosts one host was still missing
   after 300 s; at 2,500, 19 were.
   - Agents now re-announce when they reconnect.
   - agentd's reply to every status post says whether its SWIM member sees
     the host, and the agent re-announces, with backoff, until it does.
   - agentd's SWIM socket asks for a 4 MB receive buffer instead of 208 KB;
     a burst of announces was being dropped.
3. **SWIM news that reached a restarted agentd before the mgr re-taught it
   its members was filed under `member-<idx>` and never moved to the
   host.** foca reports a member up only once, so the host stayed "not up"
   (1 of 1,000). It is now moved when the members arrive.
4. **The drift check made every host report every minute.** Each
   reconcile pass, including the periodic one that changes nothing, bumped
   the state version. At 2,500 hosts the follower had 35 polls with events
   in 3 idle minutes; after the fix it had 1. A pass that changes nothing
   now doesn't report or rewrite `applied.json`.
5. **Per-host digests were gossiped to every member.** foca keeps one
   broadcast per origin and de-duplicates its queue with a linear scan on
   every add, so each member did O(cluster) work per change, for news only
   agentd reads. At 5,000 hosts on the old code the simulator never
   converged: 3,940 of 5,000 up after 15 minutes, and 2,270 false
   suspicions when a rack was frozen. The digest now goes to agentd as one
   datagram outside SWIM.
6. **The event ring held a fixed 100,000 events.** A cluster-wide change
   makes about 15 per host, so at 10,000 hosts it overran the ring before a
   1 s poller came back, and events were lost. agentd now:
   - keeps 32 per host (at least 100,000), stored serialized;
   - answers with `first`, so a consumer that fell behind knows to resync;
   - indexes the ring instead of scanning it on every poll.
7. **The listen backlog was 1,024**, so the 10,000-agent reconnect was
   about twice as slow (note ² above).
8. **intentctl rewrote its whole state file for every join it approved**,
   O(N²) in total. It now decides a batch, saves once, then answers. Tokens
   are issued in batches.
9. **The mgr module made one status request per changed host** and pulled
   the full summary on every applied event. It now makes one batch request
   (`POST /v1/hosts-status`) and reads `/v1/liveness`.
10. **A committed agent could be rolled back by being restarted** (found on
    the VMs while checking the fixes). The self-upgrade drop-in
    (`StartLimitBurst=3` in 120 s, `OnFailure=` rollback) stayed in place
    after the upgrade committed. Four routine restarts in two minutes hit
    the start limit, and systemd silently put node1 back on the previous
    binary. The hooks now exist only while an upgrade is pending: the agent
    removes them when it commits, and at startup when nothing is pending.
    The rollback unit also checks that an upgrade is pending.

Not fixed:

- **agentd's memory grew with everything it relayed**, and **agent
  upgrades weren't paced**: both are now addressed, see "Follow-up" below.
- **SWIM is sensitive to CPU starvation.** A member that can't answer a
  probe within 500 ms gets suspected. foca's work during churn grows with
  the length of its update queue, which is what made the simulator's
  flapping sustain itself.

## Follow-up: blob cleanup and paced agent upgrades

### Blob cleanup

agentd counts, for each blob, the hosts' current intents that name it,
and drops a blob once nothing has named it for `--blob-grace-s` (default
600 s). The count moves when a host's intent is replaced, under the same
lock as the check that rejects an intent naming a missing blob, so a sweep
can't drop a blob between the two. The grace covers the mgr uploading blobs
before the intent that names them, and an agent still fetching the
previous generation's. `DELETE /v1/hosts/<host>` releases a removed host's
intent, status and blobs. `/stats` reports `blobs`, `blob_bytes`,
`blobs_unreferenced`, `blob_gc` and `rss_mb`.

Looking for where the memory went found a bigger leak first: a blob that
arrived in one read was kept as a slice of hyper's read buffer, holding the
whole buffer. agentd now copies a blob when it stores it
(`bench/scale/blobmem.sh`):

| stored blobs | agentd memory per blob, before | after |
|---|---|---|
| 100,000 × 1,100 B | 5,424 B | 1,299 B |
| 20,000 × 200 B | 4,564 B | 412 B |

At scale, with a Ceph image change (`PHASES=... ceph-upgrade`: every ceph
daemon's Quadlet file names the new image) and a 120 s grace:

| | 5,000 hosts (SWIM) | 10,000 hosts (no SWIM) |
|---|---|---|
| agentd memory after the first deploy, before → now | 1.49 GB → 0.66 GB | 2.43 GB → 1.34 GB |
| blobs the image change left unused | 65,000 (60 MB) | 130,000 (121 MB) |
| sweep that dropped them | 34 ms | 75 ms, then `malloc_trim` 46 ms |
| agentd memory before → after the sweep | 780 → 832 MB (before `malloc_trim`) | 1,523 → 1,382 MB |

glibc keeps freed memory for reuse rather than returning it, so the first
5,000-host run shows no drop; agentd now calls `malloc_trim(0)` after a
sweep that freed more than 16 MB.

Five image changes in a row at 10,000 hosts (`BLOB_GRACE=30`): each left
130,000 blobs unused and each sweep brought the store back to 270,001
blobs. agentd's memory after each sweep was 1,316, 1,390, 1,462, 1,493 and
1,508 MB: it grew by 74, 72, 31 and 15 MB a cycle, leveling off at about
1.5 GB. Before, each image change kept its 130,000 blobs for good, and at
5.4 KB each that was about 0.7 GB a cycle.

### Paced agent upgrades

The mgr paces a rollout by choosing when each host's intent starts naming
the new binary (`bench/scale/mgr.py agent-upgrade`; `intentctl push
--canary/--rate/--max-in-flight/--max-failures` for the VMs):

1. **Canaries** (`--canary`, spread over the host list) get it first, all
   at once. Any failure among them halts the rollout.
2. After they are all done and `--soak` seconds have passed, the other
   hosts follow at `--rate` hosts/s, with at most `--max-in-flight` hosts
   given the binary but not yet running it.
3. A host is **done** when agentd hears from an agent process running the
   new binary: agentd's event feed now has an `agent` event whenever a
   host's agent process changes (binary or boot). It **failed** when an
   agent process reports an upgrade error or a rollback, or after
   `--host-timeout`. More than `--max-failures` failures halt the rollout:
   hosts not yet started keep their old intent.

The simulator restarts an agent in-process for an upgrade: it downloads the
binary (8 MB) and checks its hash, stops the agent's tasks and SWIM member
without leaving, waits 0.5 s, and starts again with the new binary's hash,
a new boot time and a new SWIM identity (`bump`+1); the daemons keep
running.

| 5,000 hosts, SWIM, 8 MB binary | paced: 10 canaries, 10 s soak, 20 hosts/s, ≤ 100 in flight | all at once |
|---|---|---|
| all hosts on the new binary | 261 s | 6.5 s |
| per host, intent stored to the new agent reporting, p50 / max | 0.53 / 0.75 s | 1.98 / 2.94 s |
| upgrade failures | 0 | 0 |
| simulator CPU (12 cores) | 6.6 cores | 11.9 cores, for minutes: every SWIM thread pegged (below) |
| SWIM packets sent, all members | ~80,000/s for the whole rollout (idle: 11,300/s) | full packets, ~38 MB/s, for minutes |
| hosts falsely reported down | none | up to 62 at once, from 23 s after the rollout |
| a 40-host rack frozen during the rollout: declared down, first / p50 / max | 16.1 / 17.7 / 21.2 s (quiet cluster: 16.4–20.4 s) | 25.6 / 53.6 / 112 s, 5 hosts not within 180 s |
| other hosts declared down meanwhile | none | 3,562; up to 1,294 at once, still ~800 after 5 minutes |

With 2,500 hosts, all at once with the rack frozen 5 s in: every host
upgraded in 3.0 s, the rack was declared down in 15.2–18.8 s, no other host
was, and gossip was back to idle about 40 s later. The simulator used 2.7
of its 12 cores. At 10,000 hosts (no SWIM), all at once: 80 GB of binaries
served in 11.9 s, agentd 32 s of CPU (peak 3.5 cores), p50 3.5 s per host.

What this shows:

- **One restarted agent isn't reported down.** It comes back with a newer
  identity for the same address (`bump`+1), announces it to agentd, and
  foca renames the member.
- **Every restart is a membership update** that each member gossips about
  4·log10(N) times. At 20 hosts/s every member sent about 16 packets/s
  (6.5 KB/s) for the whole rollout, 7 times its idle rate. Failure detection
  kept its speed: foca sends the updates with the most transmissions left
  first (a max-heap on `remaining_tx`), so fresh news jumps the backlog.
- **All at once, the updates arrive together.** At 2,500 hosts that was a
  40 s burst that did no harm. At 5,000, the simulator's SWIM threads
  saturated, probes went unanswered, and SWIM melted down, as the mass join
  did before. The reruns below show that this is the simulator's CPU, and
  that once started it doesn't heal.
- **So agent upgrades are paced.** 20 hosts/s took 5,000 hosts in 4.4
  minutes with no effect on liveness. The canaries and the halt keep a bad
  binary to a few hosts: on the VMs, a canary whose new binary crashed was
  rolled back and the rollout stopped before the other nodes (below).
- **Measuring false downs needs a window past the suspicion timeout.**
  The first all-at-once run counted down events only until the last host
  was done (6.5 s) and saw none. The follower's log showed 62 hosts down
  from 23 s on. `mgr.py agent-upgrade` now keeps counting for
  `--watch-after` (60 s).

### What melted SWIM down (2026-11-01)

The 5,000-host run above again, twice, changing one thing each time
(`bench/scale/meltdown.sh`): a 64 KB agent binary instead of 8 MB, and two
SWIM threads per simulator process instead of one. Each simulator process
runs the SWIM members of about 420 hosts on its SWIM threads. Per-thread
CPU was recorded (`threads.py`) and hosts down were counted from the
rollout's start (`downs.py`, from the batch follower's log).

| 5,000 hosts, all at once, a 40-host rack frozen 5 s in | 8 MB, 1 SWIM thread per process (above) | 64 KB, 1 thread | 8 MB, 2 threads |
|---|---|---|---|
| per host, intent stored to the new agent reporting, p99 | 2.9 s | 1.2 s | 4.3 s |
| SWIM threads at ≥ 90%, from 30 s on | not recorded (simulator at ~12 cores) | 12 of 12, for 10 minutes | 24 of 24, all the simulator's CPUs, to the end of the run |
| agent threads | not recorded | idle | idle |
| SWIM packets sent after the rollout | ~27,000/s, ~35 MB/s | ~28,000/s, ~37 MB/s | ~36,000/s, ~47 MB/s |
| hosts down: peak / mean over 60–300 s / at 300 s | 1,294 / 875 / 802 | 1,009 / 849 / 977 | 762 / 581 / 431 |
| frozen rack declared down: first / p50 / max | 25.6 / 53.6 / 112 s, 35 of 40 | 23.5 / 54.5 / 139 s, 37 of 40 | 25.7 / 41.0 / 70.7 s, 40 of 40 |

Hosts down include the frozen rack while it was down (thawed about 190 s
in, and 85 s in the last run). The paced rollouts in the same runs were
clean again: only the rack went down, in 16.4–22.4 s and 16.3–19.5 s. With
one SWIM thread per process, 3 of the 12 ran at 98–99% during the paced
rollout; with two, the busiest ran at 83%. The 3 test VMs (about one core,
not pinned) were running during these two runs and not during the first.

- **Not the binary.** With 64 KB the rollout was over in 3.2 s and the
  meltdown was the same.
- **The simulator's SWIM threads.** Every SWIM thread was pegged within
  30 s and stayed pegged; the agents' threads were idle. Twice the threads
  used every CPU the simulator has and cut the hosts down by about a third,
  but didn't stop it. After a mass restart, the SWIM work of 5,000 members
  needs more than those 24 logical CPUs: more than 4.7 ms of CPU per member
  per second. A real host has that many times over, so the 2,500-host run (no
  false downs, a 40 s burst) is the better guide to real hardware.
- **Once started, it doesn't heal.** A member declared down can only come
  back under a new identity (foca renews it), so every false down is
  another rename to gossip; packets stay full and more probes go
  unanswered. At the end of each run, 5–10 minutes in, 400–1,000 hosts were
  still down and flapping. foca has nothing that damps this: the probe
  period, probe timeout and suspicion timeout are fixed, and a refutation
  travels only by gossip. On real hosts the trigger would be many agents
  starved at once (a CPU quota, swap, overloaded hosts) rather than an
  upgrade. Lifeguard's local health awareness is the damping: a member that
  misses acks probes less often and waits longer before suspecting others.

The next section finds what that SWIM work was. It wasn't the renames.

### Keeping SWIM state across a restart, and Lifeguard (2026-11-01)

Two changes to the agent's SWIM member, and a patched foca:

- **The same identity after a restart.** A member's identity is its host
  index, its address and a rejoin counter (`swim-bump` in the state
  directory). Agents used to bump the counter at every start, so every
  restart was a rename that each member had to hear about. Now the counter
  changes only when foca renews the identity because the cluster declared
  the member down; the first member it talks to says so (TurnUndead).
- **The same membership after a restart.** A member saves its membership
  when it stops (`swim-members` in the state directory). When it starts, it
  restores the file if it is less than 5 minutes old, through a new
  `Foca::restore_membership` that gossips none of it.
- **Lifeguard**, in a vendored foca (`rust/vendor/foca`, `PATCHES.md`):
  - local health, dynamic suspicion and the buddy system, as in memberlist,
    but without its nacks;
  - off unless `--swim-lifeguard` is given to agentd and the agents
    (`intentctl approve --swim-lifeguard` puts it in a host's join config);
  - without Lifeguard, its wire format is stock foca's. With it,
    suspicions carry who raised them, which stock foca can't read, so it
    has to be turned on in every member at once.

Simulator flags bring the old behaviour back for comparison: `SWIM_RENAME=1`
(a new identity at every start) and `SWIM_VIEW=0` (an empty membership).

The same 5,000-host runs as above, with the rollout all at once. The
original run and the 64 KB run are from the previous section:

| 5,000 hosts, all at once, a 40-host rack frozen 5 s in | original agents (first run) | original agents again | same identity | same identity and membership | original agents, Lifeguard | same identity and membership, Lifeguard |
|---|---|---|---|---|---|---|
| SWIM threads at ≥ 90%, from 30 s on | not recorded | 8–12 of 12 | 6–12 of 12 | none | 12 of 12 | none |
| hosts down: peak / mean over 60–300 s / at 300 s | 1,294 / 875 / 802 | 960 / 627 / 621 | 1,436 / 774 / 726 | 40 / 0 / 0 (the rack) | 79 / 41 / 15 | 40 / 0 / 0 (the rack) |
| members that rejoined (were declared down), first 300 s | not recorded | 24,389 | 23,392 | 69 | 13,246 | 85 |
| other hosts declared down while the rack was frozen | 3,562 | 3,441 | 2,094 | 0 | 198 | 0 |
| frozen rack declared down: first / p50 / max | 25.6 / 53.6 / 112 s, 35 of 40 | 29.9 / 51.2 / 94.4 s, 38 of 40 | 27.3 / 50.1 / 79.5 s | 15.8 / 17.7 / 22.3 s | 53.1 / 82.8 / 95.6 s, 36 of 40 | 15.8 / 17.5 / 19.6 s |
| SWIM sent 100 s in | 29,000 packets/s, 39 MB/s | 34,500/s, 47 MB/s | 33,000/s, 44 MB/s | 11,900/s, 1.0 MB/s | 31,500/s, 43 MB/s | 11,900/s, 1.0 MB/s |

"Hosts down" is what agentd's member saw. "Rejoined" counts every member
that learned it had been declared down by anyone and took a new identity.

The paced rollouts in the same runs (20 hosts/s) were clean in all of them.
The rack was down in 16–24 s, with no false downs. The SWIM load still
differed:

| 5,000 hosts, 20 a second | original agents | same identity | same identity and membership | original agents, Lifeguard |
|---|---|---|---|---|
| SWIM, 60–270 s in | 7.4–8.2 cores, 2–3 threads near 100% | 1.9–3.1 cores, 1 thread near 100% | 0.3–0.7 cores (idle) | 7.5–8.0 cores, 2–3 threads near 100% |
| SWIM sent 100 s in | 81,600 packets/s, 40 MB/s | 39,000/s, 19 MB/s | 21,000/s, 4.2 MB/s | 81,000/s, 40 MB/s |

- **Not the renames.** Keeping the identity left the meltdown as it was:
  every SWIM thread was pegged 10–20 s after the rollout started, before any
  host went down.
- **The membership.** A restarted member started with only the few members
  agentd's reply listed. It then learned the other 5,000 one by one, from the
  packets it received, and foca gossips every member that is new to it as
  news. 5,000 members relearning 5,000 each filled every packet for minutes,
  and foca applies each update with a linear search of its member list.
  Restoring the membership removed all of it:
  - the gossip rose for 20 s and was back to its idle rate (about 1 MB/s)
    within 100 s;
  - only the rack went down, detected as fast as in a quiet cluster;
  - the rollout took 38 s, against 93–195 s.
- **Lifeguard limits the damage but doesn't heal the overload.**
  - With the original agents, agentd saw about 15 times fewer hosts down
    (41 on average over 60–300 s, against 627) and there were 17 times
    fewer false downs (198 against 3,441). But the relearning is gossip, not
    probing, so every SWIM thread stayed pegged, and members kept being
    declared down by someone and rejoining (45 a second).
  - About 4,000 of the 5,000 members had raised their local health, and
    unconfirmed suspicions last up to 6 times longer, so the frozen rack
    took 53–96 s to be noticed and 4 of its hosts weren't within 180 s.
  - With the membership restored as well, Lifeguard changed nothing that
    was measured. The rack was detected in 15.8–19.6 s, the rollout took
    35 s, and SWIM peaked at 5.3 cores for 15 s, against 4.0 without it.
    The paced rollout was the same as without it.
  - It adds work when members are already overloaded. Every member keeps
    a record of every suspicion it hears (about 700 each, at the worst of
    this run) and looks them up linearly. It also gossips up to 3
    confirmations per suspicion instead of 1. In the paced rollout it cost
    nothing measurable: 7.5–8.0 SWIM cores, against 7.4–8.2 without it.

### A CPU disturbance (2026-11-01)

The rerun of the original agents first ran while a `cargo build` used the
simulator's CPUs for about 70 s during the paced rollout. Before the build,
SWIM ran at about 6 cores with 1–2 threads near 100%. Then all 12 SWIM
threads pegged, hosts went down, and it never recovered. For the 15 minutes
left in the run, 550–850 hosts were down at any time and members rejoined
37,000 times (`n5000-freeze-upgrades-rename-disturbed`). That is the
metastable failure Lifeguard is meant for, so `agent-upgrade-paced-hog` in
`run.sh` repeats it on purpose: two busy loops on each of the simulator's
CPUs for 70 s, 90 s into a paced rollout (`hogtest.sh`).

| 5,000 hosts, paced; the simulator's CPUs taken 90–160 s in | original agents | original agents, Lifeguard | same identity and membership | same identity and membership, Lifeguard |
|---|---|---|---|---|
| members declared down by the end of the hog | 4,571 | 1,876 | 4,564 | 120 |
| hosts down: peak / mean over 300–660 s | 1,254 / 638 | 103 / 19 | 1,108 / 568 | 28 / 13 |
| members declared down by 660 s | 41,339 | 21,247 | 42,895 | 22,188 |
| SWIM after the hog | every thread pegged to the end of the run | the same | the same | the same |

Before the hog, SWIM ran at 7–8 cores with the original agents and at
0.3–0.6 with the new ones.

- **No variant recovered.** In all four, the simulator's SWIM threads were
  pegged from the hog's end to the run's end, 8 minutes later. Members
  kept being declared down by someone and rejoining under a new identity:
  about 75 a second without Lifeguard, about 40 with it.
- **Restoring the membership doesn't help here.** It removes the work an
  upgrade causes, but this starts with false downs. Each member declared
  down rejoins under a new identity, that rename is news to every member,
  and every update a member applies costs a search of its whole member
  list. While there is no news, a packet costs almost nothing. Once packets
  fill with updates, each one costs a few dozen searches of 5,000 members.
  Each SWIM thread in the simulator runs about 420 members, so the step
  from idle to saturated is short.
- **Lifeguard keeps the overload from turning into hosts down.**
  - From 300 s on, 30–45 times fewer hosts were down on average.
  - With the new agents it also held through the hog itself: 120 members
    were declared down by its end, against 4,564 without it.
  - But it collapsed within 10 s of the CPUs coming back: about 3,200
    members raised their local health at once and SWIM pegged. These runs
    don't show why.
  - Once the overload is there, Lifeguard doesn't shrink it, because the
    work isn't probing.

What would: making an update cheap to apply (foca keeps its members in a
list it searches; an index by address makes each update O(1)). Lifeguard's
suspicion records could share the index. Neither is built yet.

## Checked on the VMs

The fixed agent (0.2.4, then 0.2.5), agentd and mgr module, on the 3-VM
Tentacle cluster with real systemd and podman:

| check | result |
|---|---|
| new agentd, old (0.2.2) agents | all hosts reported and SWIM up 4 s after agentd started |
| self-upgrade 0.2.2 → 0.2.3 → 0.2.4 | 3.1 s / 4.1 s; rollback hooks gone after commit |
| agent restarted 5 times in 20 s | stays on 0.2.4 (before the fix: rolled back to 0.2.2) |
| upgrade to a binary that passes `--version` and then crashes | rolled back in about 1 s, marked bad, error reported |
| `ceph orch ps`, `orch daemon restart osd.4` (module, batch reads) | new OSD process in 2.0 s |
| frozen agent → `AGENTD_HOST_DOWN` / thaw → cleared | 15.4 s / 1.6 s |
| frozen agent declared down by SWIM / up after thaw | 13.7 s / 8 ms |
| TCP to agentd blocked, a daemon crashes | agentd sees the direct digest ahead of the last status: `resync "state 12 < 13"` |
| host powered off | "leaving" from the direct digest, "left" 0.3 s later |
| `bootstrap.py` re-run | HEALTH_OK, 6/6 OSDs |
| agentd with blob cleanup, 60 s grace (`AGENTD_ARGS=--blob-grace-s 60 vm/agentd.sh`) | old specs' blobs dropped; the 0.2.5 binary kept while intents name it |
| `vm/paced-upgrade.sh` with a binary that passes `--version`, then crashes; 1 canary | canary rolled back by systemd; rollout halted 2.6 s in; node2 and node3 never given it |
| the same with agent 0.2.5 (canary, 10 s soak, 0.2 hosts/s, 1 in flight) | 3 nodes in 18.3 s, 2.6 s each from publish to the new agent reporting; daemons not restarted; HEALTH_OK |

## What one machine can't simulate

- **More than about 5,000 SWIM members.** Steady state is cheap: 2.3
  packets/s and 0.008% of a core per member, flat from 1,000 to 5,000. But
  joining 10,000 members, even at 40 a second, built a gossip backlog that
  costs each member 0.1–0.25% of a core. 10,000 of those exceed the 12
  cores the simulator has. Its SWIM threads saturated, members flapped, and
  the flapping kept them saturated; the processes also grew to 15 GB. So
  10,000 hosts were run without simulated SWIM members. Membership at
  10,000 isn't measured here: the earlier swimsim study covered up to 4,000
  (17 s detection, no false positives).
- **Per-host apply times under load.** Those are the simulator's CPU, not
  the agent's (note ¹).
- **Simulated SWIM members get their own threads** (`--swim-threads`,
  `SWIM_THREADS` in `run.sh`), as a real host's SWIM is never starved by its
  own agent's work. But each thread runs hundreds of members. A mass
  restart of 5,000 saturated them while restarted members started from an
  empty membership; with it restored, it doesn't (above). foca's linear
  search of its member list for every update it applies is part of why so
  few threads can't keep up, and probably of the 10,000-member join
  backlog too, since a joining member also gossips every member it learns
  of.

## Running it

```
podman exec agentbench bash -lc 'cd /ab/rust && cargo build --release -p agentd -p ceph-agent -p intentctl'
podman exec agentbench /ab/bench/scale/run.sh 5000          # SWIM, ramped start
podman exec agentbench env SWIM=0 RAMP_RATE=0 /ab/bench/scale/run.sh 10000
python3 bench/scale/analyze.py results/scale > results/scale/summary.json
```

Results are in `results/scale/n<N>/`:
- `results.jsonl`: the mgr and the timings;
- `phases.txt`;
- `samples.jsonl`: per-second CPU and RSS;
- `follow*.jsonl`;
- `sim-*.out`: SWIM counters.

The runs before the fixes are in `results/scale-before/` and
`results/scale-massstart/`, and abandoned attempts are in
`results/scale-attempts/`.
