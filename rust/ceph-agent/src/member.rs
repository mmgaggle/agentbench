//! The agent's SWIM member (proto::swim): host liveness, a digest of the
//! host's state sent to agentd, pokes from agentd, and leaving the cluster
//! on host shutdown.
//!
//! The digest lets agentd notice a status report it never got (lost, or the
//! agent's connection is down) and ask for it, so status doesn't need to
//! be resent often for safety. It goes to agentd's member as one datagram
//! rather than as gossip: only agentd reads it, and gossiping every host's
//! digest to every member cost each member O(cluster size) per change.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use proto::intent::{HostStatus, Mode};
use proto::join::SwimConfig;
use proto::swim::{self, Ctrl, Digest, Gossip, GossipHandler, Id, Node, Traffic, CONTROLLER};
use tokio::sync::mpsc;

use crate::reconcile::{write_atomic, Shared};

/// SWIM packet counters for every member this process runs (one, or the
/// simulator's thousands).
pub static TRAFFIC: std::sync::LazyLock<Arc<Traffic>> = std::sync::LazyLock::new(Default::default);

/// Where members run, if not on the agent's runtime: the simulator gives
/// SWIM threads of its own, as a real host's SWIM is never starved by its
/// own agent's work (thousands of simulated agents applying an intent at
/// once otherwise delay probe acks into false suspicions).
pub static SWIM_RUNTIME: std::sync::OnceLock<tokio::runtime::Handle> = std::sync::OnceLock::new();

pub struct Member {
    idx: u32,
    boot: u64,
    ctrl: mpsc::UnboundedSender<Ctrl>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// The simulator's switch back to taking a new identity at every start,
/// to reproduce what a mass restart cost before members kept theirs.
pub static RENAME_ON_START: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The simulator's switch for starting every member without the
/// membership it had before the restart (as before members saved it).
pub static FORGET_VIEW: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The simulator's switch for Lifeguard on every member (real agents get
/// it from their join config).
pub static LIFEGUARD: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The identity's rejoin counter, persisted. A restarted agent keeps the
/// identity it had: if the cluster still has it alive (a quick restart, as
/// in an upgrade) there is nothing to gossip, where a new identity is a
/// rename every member has to hear about. If the cluster declared it down
/// meanwhile, the first member it talks to says so (TurnUndead) and foca
/// renews it, which `Member::start` saves here.
fn start_bump(path: &Path) -> u16 {
    let n: u16 = std::fs::read_to_string(path).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0);
    if !RENAME_ON_START.load(std::sync::atomic::Ordering::Relaxed) {
        return n;
    }
    let n = n.wrapping_add(1);
    save_bump(path, n);
    n
}

fn save_bump(path: &Path, n: u16) {
    let _ = write_atomic(path, n.to_string().as_bytes(), 0o644, None);
}

impl Member {
    /// `poked` gets a message when agentd asks this host for its status.
    pub fn start(ctx: &Shared, cfg: &SwimConfig, poked: mpsc::UnboundedSender<()>) -> Result<Arc<Member>> {
        let idx = cfg.idx;
        let bump_file = ctx.cfg.state_dir.join("swim-bump");
        let id = Id { idx, addr: cfg.bind.parse().with_context(|| format!("swim bind {}", cfg.bind))?, bump: start_bump(&bump_file) };
        let seeds = cfg
            .seeds
            .iter()
            .map(|s| Ok(Id { idx: CONTROLLER, addr: s.parse()?, bump: 0 }))
            .collect::<Result<Vec<_>>>()?;
        let handler = GossipHandler::new(move |g| {
            if let Gossip::Poke { hosts, .. } = g {
                if hosts.is_empty() || hosts.contains(&idx) {
                    let _ = poked.send(());
                }
            }
        });
        let (tx, rx) = mpsc::unbounded_channel();
        use std::sync::atomic::Ordering::Relaxed;
        let view = (!FORGET_VIEW.load(Relaxed)).then(|| ctx.cfg.state_dir.join("swim-members"));
        let node = Node {
            id,
            cluster_size: cfg.cluster_size.unwrap_or(1000),
            seeds,
            seed: swim::wall_ns() ^ idx as u64,
            direct: None,
            view,
            lifeguard: cfg.lifeguard || LIFEGUARD.load(Relaxed),
        };
        let member = async move {
            let r = swim::run(node, rx, TRAFFIC.clone(), handler, |n| match &n {
                swim::foca::OwnedNotification::MemberDown(m) if m.idx == CONTROLLER => {
                    eprintln!("swim: agentd's member is down");
                }
                swim::foca::OwnedNotification::Rejoin(new) => save_bump(&bump_file, new.bump),
                _ => {}
            })
            .await;
            if let Err(e) = r {
                eprintln!("swim: {e:#}");
            }
        };
        let task = match SWIM_RUNTIME.get() {
            Some(rt) => rt.spawn(member),
            None => tokio::spawn(member),
        };
        eprintln!("swim: member {idx} at {} (bump {}), seeds {:?}", cfg.bind, id.bump, cfg.seeds);
        Ok(Arc::new(Member { idx, boot: ctx.boot, ctrl: tx, task: std::sync::Mutex::new(Some(task)) }))
    }

    /// Stops answering without leaving, as when the agent process exits;
    /// returns once the socket is closed (the simulator then restarts the
    /// agent on the same address).
    pub async fn stop(&self) {
        let _ = self.ctrl.send(Ctrl::Stop);
        let task = self.task.lock().unwrap().take();
        if let Some(t) = task {
            let _ = tokio::time::timeout(Duration::from_secs(5), t).await;
        }
    }

    /// Sends the digest straight to agentd's member: one datagram, which
    /// costs the rest of the cluster nothing.
    fn send(&self, st: &HostStatus, mode: u8) {
        let failed = st.units.values().filter(|u| u.active_state == "failed" || u.apply_error.is_some()).count() as u16;
        let d = Digest {
            idx: self.idx,
            seq: swim::wall_ns() / 1_000_000,
            boot: self.boot,
            applied_generation: st.applied_generation,
            state_version: st.state_version,
            failed_units: failed,
            mode,
        };
        let _ = self.ctrl.send(Ctrl::Direct(swim::encode_gossip(&Gossip::Digest(d))));
    }

    /// Re-announces to agentd's member, after reconnecting to agentd.
    pub fn announce(&self) {
        let _ = self.ctrl.send(Ctrl::Announce);
    }

    /// Sends the host's digest; the reporter calls this with every status
    /// post (on changes, and every report_secs).
    pub fn publish(&self, st: &HostStatus) {
        let mode = if st.mode == Mode::Maintenance { swim::MODE_MAINTENANCE } else { swim::MODE_NORMAL };
        self.send(st, mode);
    }

    /// Announces the shutdown, then leaves the cluster: agentd sees "left"
    /// at once instead of suspecting a failure.
    pub async fn leave(&self, st: &HostStatus) {
        self.send(st, swim::MODE_SHUTDOWN);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let _ = self.ctrl.send(Ctrl::Leave);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[zbus::proxy(
    interface = "org.freedesktop.login1.Manager",
    default_service = "org.freedesktop.login1",
    default_path = "/org/freedesktop/login1"
)]
trait Login {
    fn inhibit(&self, what: &str, who: &str, why: &str, mode: &str) -> zbus::Result<zbus::zvariant::OwnedFd>;
    #[zbus(signal)]
    fn prepare_for_shutdown(&self, start: bool) -> zbus::Result<()>;
}

/// Holds a logind "delay" inhibitor; when the host starts shutting down,
/// tells agentd and the cluster, then releases it.
pub async fn on_shutdown(ctx: Shared, member: Arc<Member>) -> Result<()> {
    let conn = zbus::Connection::system().await?;
    let login = LoginProxy::new(&conn).await?;
    let mut signals = login.receive_prepare_for_shutdown().await?;
    let mut inhibitor = Some(login.inhibit("shutdown", "ceph-agent", "announce leaving the cluster", "delay").await?);
    while let Some(sig) = signals.next().await {
        if !sig.args()?.start {
            // shutdown cancelled
            if inhibitor.is_none() {
                inhibitor = Some(login.inhibit("shutdown", "ceph-agent", "announce leaving the cluster", "delay").await?);
            }
            continue;
        }
        eprintln!("host is shutting down: leaving the cluster");
        ctx.meta.lock().unwrap().shutting_down = true;
        let st = ctx.host_status();
        if let Some(c) = &ctx.client {
            let _ = tokio::time::timeout(Duration::from_secs(1), c.status(&st)).await;
        }
        member.leave(&st).await;
        drop(inhibitor.take());
    }
    Ok(())
}
