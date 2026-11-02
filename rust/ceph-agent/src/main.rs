//! ceph-agent: makes a host match the desired state the mgr publishes for
//! it, and reports what systemd says about its daemons.
//!
//! The mgr signs a per-host intent (units as Quadlet files, config files,
//! images, removals) and stores it in agentd; the agent long-polls agentd
//! for it, verifies the signature, writes the files, has systemd regenerate
//! and start/restart/stop the units over D-Bus, and posts status whenever a
//! unit changes state. Daemons run under systemd, not under the agent: they
//! keep running and restarting while the agent is down.
//!
//!   ceph-agent run [--config /etc/ceph-agent/agent.json]

mod client;
mod jobs;
mod join;
mod member;
mod reconcile;
mod sdnotify;
mod systemd;
mod upgrade;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use proto::intent;
use serde::Deserialize;
use tokio::sync::{mpsc, watch};

use reconcile::{Ctx, Shared};
use systemd::{Backend, Fake, Systemd};

fn default_state_dir() -> PathBuf {
    "/var/lib/ceph-agent".into()
}
fn default_quadlet_dir() -> PathBuf {
    "/etc/containers/systemd".into()
}
fn default_unit_dir() -> PathBuf {
    "/etc/systemd/system".into()
}
fn default_data_dir() -> PathBuf {
    "/var/lib/ceph".into()
}
fn default_drift() -> u64 {
    60
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub host: String,
    /// host:port of agentd's agent endpoint
    pub agentd: String,
    /// TLS name to verify agentd's certificate against (default: the host
    /// part of `agentd`)
    #[serde(default)]
    pub server_name: Option<String>,
    /// Cluster CA that signed agentd's certificate
    pub ca: PathBuf,
    /// TLS client identity from `ceph-agent join`
    #[serde(default)]
    pub cert: Option<PathBuf>,
    #[serde(default)]
    pub key: Option<PathBuf>,
    /// Instead of a certificate: the agent keyring (cephadm's v1 auth)
    #[serde(default)]
    pub keyring: Option<PathBuf>,
    /// X25519 key that sealed secrets are encrypted to
    #[serde(default)]
    pub seal_key: Option<PathBuf>,
    #[serde(default)]
    pub swim: Option<proto::join::SwimConfig>,
    /// Public keys (base64 Ed25519) allowed to sign intents
    pub trusted_keys: Vec<PathBuf>,
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    #[serde(default = "default_quadlet_dir")]
    pub quadlet_dir: PathBuf,
    #[serde(default = "default_unit_dir")]
    pub unit_dir: PathBuf,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Reconcile this often even without a new intent
    #[serde(default = "default_drift")]
    pub drift_secs: u64,
    /// Post status at least this often (default 30 s, or 300 s with SWIM:
    /// digests catch missed reports)
    #[serde(default)]
    pub report_secs: Option<u64>,
    /// In-memory systemd, for tests and simulation
    #[serde(default)]
    pub fake_systemd: bool,
    #[serde(default = "yes")]
    pub pull_images: bool,
    /// Connect to agentd from this address (the simulator: each host its own)
    #[serde(default)]
    pub source_addr: Option<std::net::IpAddr>,
    /// The simulator: the agent binary this agent "runs" (after a simulated
    /// upgrade), instead of /proc/self/exe
    #[serde(skip)]
    pub sim_binary: Option<intent::AgentSpec>,
    /// The simulator: an agent upgrade downloads and checks the binary,
    /// then restarts the agent in-process
    #[serde(skip)]
    pub sim_upgrade: bool,
}

#[derive(Parser)]
#[command(version)]
enum Cmd {
    /// Run the agent
    Run {
        #[arg(long, default_value = "/etc/ceph-agent/agent.json")]
        config: PathBuf,
    },
    /// Join the cluster with a token from the mgr; writes the config
    Join {
        token: String,
        #[arg(long, default_value = "/etc/ceph-agent")]
        etc: PathBuf,
        #[arg(long, default_value = "/var/lib/ceph-agent")]
        state_dir: PathBuf,
    },
    /// Simulation: join hosts `from`..`from+count` concurrently, host i with
    /// line i of `tokens`, into DIR/h<i>/, its SWIM member on 127.2.x.y
    SimJoin {
        #[arg(long)]
        tokens: PathBuf,
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 0)]
        from: usize,
        #[arg(long)]
        count: usize,
        #[arg(long, default_value_t = 2)]
        threads: usize,
    },
    /// Simulation: run joined hosts `from`..`from+count` in this process,
    /// each a whole agent (agentd client, reconciler, reporter, SWIM
    /// member) over in-memory systemd
    Sim {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long, default_value_t = 0)]
        from: usize,
        #[arg(long)]
        count: usize,
        #[arg(long, default_value_t = 2)]
        threads: usize,
        /// Spread agent starts over this many seconds
        #[arg(long, default_value_t = 0.0)]
        ramp_s: f64,
        /// Threads for the hosts' SWIM members (0: share the agents')
        #[arg(long, default_value_t = 1)]
        swim_threads: usize,
        /// Run the agents without SWIM members (reporting every 300 s, as
        /// with SWIM): one machine can't run the SWIM members of 10,000
        /// hosts, whose work is per-host-cheap but adds up on one box
        #[arg(long)]
        no_swim: bool,
        /// Take a new SWIM identity at every agent start, as agents did
        /// before they kept theirs (each restart is then a rename that
        /// every member hears about)
        #[arg(long)]
        swim_rename_on_start: bool,
        /// Start every member without the membership it had before the
        /// restart, as agents did before they saved it
        #[arg(long)]
        swim_forget_view: bool,
        /// Lifeguard on every member (agentd needs --swim-lifeguard too)
        #[arg(long)]
        swim_lifeguard: bool,
    },
}

fn main() -> Result<()> {
    let cmd = Cmd::parse();
    let threads = match &cmd {
        Cmd::Sim { threads, .. } | Cmd::SimJoin { threads, .. } => *threads,
        _ => 2,
    };
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build()?;
    match cmd {
        Cmd::Run { config } => {
            let cfg: Config = serde_json::from_slice(&std::fs::read(&config).with_context(|| format!("{}", config.display()))?)
                .with_context(|| format!("parsing {}", config.display()))?;
            rt.block_on(run(cfg))
        }
        Cmd::Join { token, etc, state_dir } => rt.block_on(join::join(&token, &etc, &state_dir)),
        Cmd::SimJoin { tokens, dir, from, count, .. } => rt.block_on(sim_join(tokens, dir, from, count)),
        Cmd::Sim { dir, from, count, ramp_s, swim_threads, no_swim, swim_rename_on_start, swim_forget_view, swim_lifeguard, .. } => {
            use std::sync::atomic::Ordering::Relaxed;
            member::RENAME_ON_START.store(swim_rename_on_start, Relaxed);
            member::FORGET_VIEW.store(swim_forget_view, Relaxed);
            member::LIFEGUARD.store(swim_lifeguard, Relaxed);
            if swim_threads > 0 {
                let swim_rt = tokio::runtime::Builder::new_multi_thread().worker_threads(swim_threads).thread_name("swim").enable_all().build()?;
                let _ = member::SWIM_RUNTIME.set(swim_rt.handle().clone());
                std::mem::forget(swim_rt);
            }
            rt.block_on(sim(dir, from, count, ramp_s, no_swim))
        }
    }
}

fn sim_host_dir(dir: &std::path::Path, i: usize) -> PathBuf {
    dir.join(format!("h{i:05}"))
}

/// Simulated host i's address: its SWIM member binds here.
fn sim_addr(i: usize) -> String {
    format!("127.2.{}.{}", (i >> 8) & 255, i & 255)
}

async fn sim_join(tokens: PathBuf, dir: PathBuf, from: usize, count: usize) -> Result<()> {
    let lines: Vec<String> = std::fs::read_to_string(&tokens)?.lines().map(String::from).collect();
    let t0 = std::time::Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for i in from..from + count {
        let token = lines.get(i).with_context(|| format!("no token for host {i}"))?.clone();
        let hd = sim_host_dir(&dir, i);
        set.spawn(async move {
            let t = std::time::Instant::now();
            let r = join::join_as(&token, &hd.join("etc"), &hd.join("state"), Some(&sim_addr(i))).await;
            (i, r, t.elapsed())
        });
    }
    let (mut ok, mut lat) = (0, Vec::new());
    while let Some(r) = set.join_next().await {
        let (i, r, d) = r?;
        match r {
            Ok(()) => {
                ok += 1;
                lat.push(d.as_secs_f64());
            }
            Err(e) => eprintln!("host {i}: {e:#}"),
        }
    }
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |x: f64| lat.get(((lat.len() as f64 * x) as usize).min(lat.len().saturating_sub(1))).copied().unwrap_or(0.0);
    println!(
        "joined {ok}/{count} in {:.2}s; per host p50 {:.2}s p99 {:.2}s max {:.2}s",
        t0.elapsed().as_secs_f64(),
        q(0.5),
        q(0.99),
        q(1.0)
    );
    if ok != count {
        anyhow::bail!("{} hosts failed to join", count - ok);
    }
    Ok(())
}

async fn sim(dir: PathBuf, from: usize, count: usize, ramp_s: f64, no_swim: bool) -> Result<()> {
    let gap = Duration::from_secs_f64(ramp_s / count.max(1) as f64);
    for i in from..from + count {
        let hd = sim_host_dir(&dir, i);
        let path = hd.join("etc/agent.json");
        let mut cfg: Config = serde_json::from_slice(&std::fs::read(&path).with_context(|| format!("{}", path.display()))?)?;
        cfg.fake_systemd = true;
        cfg.pull_images = false;
        cfg.quadlet_dir = hd.join("quadlet");
        cfg.unit_dir = hd.join("units");
        cfg.data_dir = hd.join("data");
        cfg.source_addr = Some(sim_addr(i).parse()?);
        cfg.sim_upgrade = true;
        if no_swim {
            cfg.swim = None;
            cfg.report_secs = Some(300);
        }
        tokio::spawn(async move {
            let host = cfg.host.clone();
            // the daemons outlive the agent process
            let units = systemd::FakeUnits::default();
            loop {
                match run_agent(cfg.clone(), Some(units.clone())).await {
                    Ok(Some(next)) => {
                        // the process exits, systemd starts the new binary
                        tokio::time::sleep(SIM_RESTART).await;
                        cfg.sim_binary = Some(next);
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("{host}: agent exited: {e:#}");
                        break;
                    }
                }
            }
        });
        if !gap.is_zero() {
            tokio::time::sleep(gap).await;
        }
    }
    eprintln!("sim: {count} agents started (hosts {from}..{})", from + count);
    let t = member::TRAFFIC.clone();
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    loop {
        tick.tick().await;
        use std::sync::atomic::Ordering::Relaxed;
        println!(
            "{} swim pkts_sent {} pkts_recv {} bytes_sent {} send_errors {} renames {} rejoins {} restored {} health {} unhealthy {} suspicions {}",
            intent::wall_ns() / 1_000_000,
            t.pkts_sent.load(Relaxed),
            t.pkts_recv.load(Relaxed),
            t.bytes_sent.load(Relaxed),
            t.send_errors.load(Relaxed),
            t.renames.load(Relaxed),
            t.rejoins.load(Relaxed),
            t.restored.load(Relaxed),
            t.health.load(Relaxed),
            t.unhealthy.load(Relaxed),
            t.suspicions.load(Relaxed)
        );
    }
}

/// A simulated agent upgrade: exit, and systemd starting the new binary
const SIM_RESTART: Duration = Duration::from_millis(500);

async fn run(cfg: Config) -> Result<()> {
    run_agent(cfg, None).await.map(|_| ())
}

/// Runs one agent "process". Returns only in the simulator, with the binary
/// to restart into after an agent upgrade (a real agent exits instead).
async fn run_agent(cfg: Config, fake_units: Option<systemd::FakeUnits>) -> Result<Option<intent::AgentSpec>> {
    let (backend, events) = if cfg.fake_systemd {
        let (f, rx) = Fake::with_units(cfg.quadlet_dir.clone(), fake_units.unwrap_or_default());
        (Backend::Fake(f), rx)
    } else {
        let (s, rx) = Systemd::connect().await?;
        (Backend::Systemd(s), rx)
    };
    let swim_cfg = cfg.swim.clone();
    let trusted = cfg.trusted_keys.iter().map(|p| intent::load_verifying_key(p)).collect::<Result<Vec<_>>>()?;
    let client = client::Client::new(&cfg)?;
    let (ctx, job_rx) = Ctx::new(cfg, backend, Some(client))?;
    let ctx: Shared = Arc::new(ctx);
    // everything this "process" runs, to stop it on a simulated restart
    let mut tasks = vec![tokio::spawn(jobs::runner(ctx.clone(), job_rx))];
    if let Some(msg) = upgrade::on_start(&ctx, &ctx.self_sha) {
        eprintln!("{msg}");
        ctx.meta.lock().unwrap().apply_errors.push(msg);
    }
    let saved = ctx.load_saved_intent(&trusted);
    eprintln!(
        "ceph-agent {} ({}) on {}: agentd {}, saved intent generation {}",
        env!("CARGO_PKG_VERSION"),
        &ctx.self_sha[..12.min(ctx.self_sha.len())],
        ctx.cfg.host,
        ctx.cfg.agentd,
        saved.as_ref().map(|i| i.generation).unwrap_or(0)
    );
    let (itx, mut irx) = watch::channel(saved.map(Arc::new));

    let (poke_tx, mut poke_rx) = mpsc::unbounded_channel();
    let member = match &swim_cfg {
        Some(sc) => {
            let m = member::Member::start(&ctx, sc, poke_tx)?;
            if !ctx.backend.is_fake() {
                let (c, m2) = (ctx.clone(), m.clone());
                tokio::spawn(async move {
                    if let Err(e) = member::on_shutdown(c, m2).await {
                        eprintln!("shutdown inhibitor: {e:#}");
                    }
                });
            }
            Some(m)
        }
        None => None,
    };
    let c = ctx.clone();
    tasks.push(tokio::spawn(async move {
        while poke_rx.recv().await.is_some() {
            c.dirty.notify_one();
        }
    }));

    tasks.push(tokio::spawn(poll_intents(ctx.clone(), trusted, itx, member.clone())));
    tasks.push(tokio::spawn(unit_events(ctx.clone(), events)));
    // report once at startup, so agentd sees the agent before any intent
    ctx.dirty.notify_one();
    tasks.push(tokio::spawn(report(ctx.clone(), member.clone())));
    let c = ctx.clone();
    tasks.push(tokio::spawn(async move {
        let mut t = tokio::time::interval(Duration::from_secs(30));
        loop {
            t.tick().await;
            c.sample_usage();
        }
    }));
    // The watchdog proves the runtime is alive, not that agentd is
    // reachable: an agentd outage must not make systemd restart every agent.
    if let Some(iv) = sdnotify::watchdog_interval() {
        tokio::spawn(async move {
            let mut t = tokio::time::interval(iv);
            loop {
                t.tick().await;
                sdnotify::notify("WATCHDOG=1");
            }
        });
    }
    sdnotify::notify("READY=1");

    let mut drift = tokio::time::interval(Duration::from_secs(ctx.cfg.drift_secs));
    drift.tick().await;
    let mut pending = true;
    loop {
        if pending {
            let it = irx.borrow_and_update().clone();
            if let Some(i) = it {
                ctx.reconcile(&i).await;
            }
        }
        tokio::select! {
            biased;
            _ = ctx.restart.notified() => {
                let next = ctx.restart_to.lock().unwrap().clone();
                for t in &tasks {
                    t.abort();
                }
                if let Some(m) = &member {
                    m.stop().await;
                }
                return Ok(next);
            }
            r = irx.changed() => { r?; pending = true; }
            _ = drift.tick() => { pending = true; }
            _ = ctx.kick.notified() => { pending = true; }
        }
    }
}

async fn poll_intents(
    ctx: Shared,
    trusted: Vec<ed25519_dalek::VerifyingKey>,
    tx: watch::Sender<Option<Arc<intent::Intent>>>,
    member: Option<Arc<member::Member>>,
) {
    let client = ctx.client.as_ref().expect("agentd client");
    let mut have = tx.borrow().as_ref().map(|i| i.generation).unwrap_or(0);
    let mut backoff = 1;
    // After (re)connecting, report and re-announce: agentd may have
    // restarted and lost every host's status and its SWIM membership. The
    // first poll after connecting doesn't wait, so that happens at once
    // rather than when the long-poll returns.
    let mut fresh = true;
    loop {
        let r = client.intent(have, if fresh { 0 } else { 30 }).await;
        if r.is_ok() && fresh {
            fresh = false;
            ctx.dirty.notify_one();
            if let Some(m) = &member {
                m.announce();
            }
        }
        match r {
            Ok(client::Poll::Intent(env)) => {
                backoff = 1;
                match intent::open(&env, &trusted) {
                    Ok(i) if i.host != ctx.cfg.host => {
                        ctx.set_intent_errors(vec![format!("intent generation {} is for host {}", i.generation, i.host)]);
                        have = have.max(env.generation);
                    }
                    Ok(i) => {
                        have = i.generation;
                        if let Err(e) = ctx.save_envelope(&env) {
                            eprintln!("saving intent: {e:#}");
                        }
                        ctx.set_intent_errors(vec![]);
                        eprintln!("intent generation {}: {} units, {} files, {} removals", i.generation, i.units.len(), i.files.len(), i.remove.len());
                        tx.send_replace(Some(Arc::new(i)));
                    }
                    Err(e) => {
                        eprintln!("rejected intent generation {}: {e:#}", env.generation);
                        ctx.set_intent_errors(vec![format!("rejected intent generation {}: {e:#}", env.generation)]);
                        have = have.max(env.generation);
                    }
                }
            }
            Ok(client::Poll::Poke) => {
                backoff = 1;
                ctx.dirty.notify_one();
            }
            Ok(client::Poll::Nothing) => backoff = 1,
            Err(e) => {
                fresh = true;
                eprintln!("agentd: {e:#}; retrying in {backoff}s");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(30);
            }
        }
    }
}

/// systemd unit changes -> refreshed unit status. Bursts are coalesced:
/// one refresh per unit per wakeup.
async fn unit_events(ctx: Shared, mut rx: mpsc::UnboundedReceiver<String>) {
    while let Some(u) = rx.recv().await {
        let mut units = BTreeSet::from([u]);
        while let Ok(u) = rx.try_recv() {
            units.insert(u);
        }
        for u in units {
            let d = ctx.unit_daemon.lock().unwrap().get(&u).cloned();
            if let Some(d) = d {
                ctx.refresh_unit(&d, &u, None).await;
            }
        }
    }
}

/// Posts status on every material change (after a 20 ms debounce) and at
/// least every report_secs, and gossips the digest whether or not the post
/// got through.
async fn report(ctx: Shared, member: Option<Arc<member::Member>>) {
    let client = ctx.client.as_ref().expect("agentd client");
    let secs = ctx.cfg.report_secs.unwrap_or(if member.is_some() { 300 } else { 30 });
    let period = Duration::from_secs(secs);
    let mut swim_retry = Duration::from_secs(2);
    loop {
        tokio::select! {
            _ = ctx.dirty.notified() => tokio::time::sleep(Duration::from_millis(20)).await,
            _ = tokio::time::sleep(period) => {}
        }
        if ctx.meta.lock().unwrap().shutting_down {
            continue;
        }
        let st = ctx.host_status();
        if let Some(m) = &member {
            m.publish(&st);
        }
        let r = client.status(&st).await;
        if let Ok(swim_up) = &r {
            ctx.posted.store(true, std::sync::atomic::Ordering::Relaxed);
            // agentd's SWIM member doesn't see us (it restarted, or our
            // announce was lost): announce again and check back, backing off
            match (&member, swim_up) {
                (Some(m), Some(false)) => {
                    m.announce();
                    let (c, d) = (ctx.clone(), swim_retry);
                    tokio::spawn(async move {
                        tokio::time::sleep(d).await;
                        c.dirty.notify_one();
                    });
                    swim_retry = (swim_retry * 2).min(Duration::from_secs(60));
                }
                _ => swim_retry = Duration::from_secs(2),
            }
        }
        if let Err(e) = r {
            eprintln!("posting status: {e:#}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            ctx.dirty.notify_one();
        }
    }
}
