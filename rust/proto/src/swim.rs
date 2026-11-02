//! SWIM membership (foca) for agents and the controller.
//!
//! Every agent and agentd run one member each. Members probe one random
//! member per second and gossip membership changes, so each member's load
//! is constant regardless of cluster size and news reaches everyone in
//! O(log n) rounds. agentd learns about agents that stopped responding from
//! the membership instead of from missing heartbeats, and sends signals to
//! all agents (e.g. "refresh", "new config version") as a custom broadcast
//! instead of one connection per agent.
//!
//! Identities are a host index, the member's UDP address and a rejoin
//! counter, which agents keep across restarts (ceph-agent's member.rs).
//! The simulator (`sim_id`) maps index i to a loopback address so a whole
//! cluster can run on one machine: agent i at
//! 127.2.(i>>8 & 255).(i & 255):30000+(i>>16), the controller at
//! 127.3.0.1:7946. Real agents use their host's address.
//!
//! Two broadcast vocabularies: `SignalHandler` (the simulator's versioned
//! signals) and `GossipHandler` (real agents: agentd's pokes). Per-host
//! digests go to agentd directly (`Ctrl::Direct`), not to every member.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub use foca;

use anyhow::Result;
use foca::{AccumulatingRuntime, BroadcastHandler, Config, Foca, Identity, Invalidates, Lifeguard, Member, OwnedNotification, PostcardCodec, Timer};
use rand::SeedableRng;
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

pub const CONTROLLER: u32 = u32::MAX;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Id {
    pub idx: u32,
    pub addr: SocketAddr,
    pub bump: u16,
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}@{}#{}", self.idx, self.addr, self.bump)
    }
}

/// The simulator's identity for member `idx` (loopback address).
pub fn sim_id(idx: u32) -> Id {
    Id { idx, addr: sockaddr(idx), bump: 0 }
}

impl Identity for Id {
    type Addr = u32;

    // Rejoin right away with a new identity if declared down by mistake.
    fn renew(&self) -> Option<Self> {
        Some(Id { idx: self.idx, addr: self.addr, bump: self.bump.wrapping_add(1) })
    }

    fn addr(&self) -> u32 {
        self.idx
    }

    fn win_addr_conflict(&self, other: &Self) -> bool {
        (self.bump.wrapping_sub(other.bump) as i16) > 0
    }
}

pub fn sockaddr(idx: u32) -> SocketAddr {
    if idx == CONTROLLER {
        return SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 3, 0, 1), 7946));
    }
    SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(127, 2, ((idx >> 8) & 0xff) as u8, (idx & 0xff) as u8),
        30000 + (idx >> 16) as u16,
    ))
}

pub fn wall_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// A controller-to-agents signal carried as a foca custom broadcast.
#[derive(Serialize, Deserialize)]
pub struct Signal {
    pub version: u32,
    pub sent_ns: u64,
}

pub struct SignalKey(u32);

impl Invalidates for SignalKey {
    fn invalidates(&self, other: &Self) -> bool {
        self.0 > other.0
    }
}

#[derive(Debug)]
pub struct SwimError(String);

impl std::fmt::Display for SwimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SwimError {}

/// Keeps only the newest signal and reports first receipt of each version.
pub struct SignalHandler {
    latest: u32,
    on_signal: Box<dyn FnMut(&Signal) + Send>,
}

impl SignalHandler {
    pub fn new(on_signal: impl FnMut(&Signal) + Send + 'static) -> Self {
        SignalHandler { latest: 0, on_signal: Box::new(on_signal) }
    }
}

impl BroadcastHandler<Id> for SignalHandler {
    type Key = SignalKey;
    type Error = SwimError;

    fn receive_item(&mut self, data: &[u8], _sender: Option<&Id>) -> Result<Option<SignalKey>, SwimError> {
        let sig: Signal = postcard::from_bytes(data).map_err(|e| SwimError(format!("bad signal: {e}")))?;
        if sig.version <= self.latest {
            return Ok(None);
        }
        self.latest = sig.version;
        (self.on_signal)(&sig);
        Ok(Some(SignalKey(sig.version)))
    }
}

pub fn encode_signal(sig: &Signal) -> Vec<u8> {
    postcard::to_allocvec(sig).unwrap()
}

/// What real members gossip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Gossip {
    /// agent -> agentd (and everyone on the way): a summary of the host,
    /// so agentd notices when it has missed a status report.
    Digest(Digest),
    /// agentd -> agents: post status and re-poll now. Empty `hosts` = all.
    Poke { seq: u64, hosts: Vec<u32> },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Digest {
    pub idx: u32,
    /// wall ms when sent; orders digests from one member across restarts
    pub seq: u64,
    /// agent process start (wall ms); state_version restarts with it
    pub boot: u64,
    pub applied_generation: u64,
    pub state_version: u64,
    pub failed_units: u16,
    /// 0 normal, 1 maintenance, 2 shutting down
    pub mode: u8,
}

pub const MODE_NORMAL: u8 = 0;
pub const MODE_MAINTENANCE: u8 = 1;
pub const MODE_SHUTDOWN: u8 = 2;

pub fn encode_gossip(g: &Gossip) -> Vec<u8> {
    postcard::to_allocvec(g).unwrap()
}

pub fn decode_gossip(data: &[u8]) -> Option<Gossip> {
    postcard::from_bytes(data).ok()
}

pub struct GossipKey {
    kind: u8,
    origin: u32,
    seq: u64,
}

impl Invalidates for GossipKey {
    fn invalidates(&self, other: &Self) -> bool {
        self.kind == other.kind && self.origin == other.origin && self.seq > other.seq
    }
}

/// Keeps the newest message per (kind, origin) and reports first receipt.
pub struct GossipHandler {
    latest: HashMap<(u8, u32), u64>,
    on_msg: Box<dyn FnMut(&Gossip) + Send>,
}

impl GossipHandler {
    pub fn new(on_msg: impl FnMut(&Gossip) + Send + 'static) -> Self {
        GossipHandler { latest: HashMap::new(), on_msg: Box::new(on_msg) }
    }
}

impl BroadcastHandler<Id> for GossipHandler {
    type Key = GossipKey;
    type Error = SwimError;

    fn receive_item(&mut self, data: &[u8], _sender: Option<&Id>) -> Result<Option<GossipKey>, SwimError> {
        let msg: Gossip = postcard::from_bytes(data).map_err(|e| SwimError(format!("bad gossip: {e}")))?;
        let (kind, origin, seq) = match &msg {
            Gossip::Digest(d) => (0u8, d.idx, d.seq),
            Gossip::Poke { seq, .. } => (1u8, CONTROLLER, *seq),
        };
        let last = self.latest.entry((kind, origin)).or_insert(0);
        if seq <= *last {
            return Ok(None);
        }
        *last = seq;
        (self.on_msg)(&msg);
        Ok(Some(GossipKey { kind, origin, seq }))
    }
}

pub enum Ctrl {
    /// Stop answering, as if the host died.
    Stop,
    /// Tell the cluster we're leaving, then stop.
    Leave,
    /// Start disseminating this broadcast.
    Broadcast(Vec<u8>),
    /// Send this to the seeds (agentd) as one datagram, outside SWIM.
    Direct(Vec<u8>),
    /// Announce to the seeds again: a restarted seed (agentd) has
    /// forgotten the membership and would otherwise relearn it only as
    /// members happen to probe it.
    Announce,
}

/// Packet counters, shared by every member a process runs.
#[derive(Default)]
pub struct Traffic {
    pub pkts_sent: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub pkts_recv: AtomicU64,
    pub send_errors: AtomicU64,
    /// renames each member learned of (a member that restarted or rejoined
    /// under a new identity), summed over members
    pub renames: AtomicU64,
    /// times one of these members was declared down and rejoined
    pub rejoins: AtomicU64,
    /// Lifeguard: local health multipliers summed over members, and how
    /// many members are above zero
    pub health: AtomicI64,
    pub unhealthy: AtomicI64,
    /// Lifeguard: suspicions whose confirmations members are counting,
    /// summed over members
    pub suspicions: AtomicI64,
    /// members that started from a saved membership
    pub restored: AtomicU64,
}

pub struct Node {
    pub id: Id,
    pub cluster_size: u32,
    pub seeds: Vec<Id>,
    pub seed: u64,
    /// Receives datagrams sent with `Ctrl::Direct` (without the tag).
    pub direct: Option<Box<dyn FnMut(&[u8]) + Send>>,
    /// Where to keep the membership across a restart: saved when the
    /// member stops, restored when it starts again soon after
    pub view: Option<PathBuf>,
    /// Lifeguard (foca's local health, dynamic suspicion, buddy system)
    pub lifeguard: bool,
}

/// A saved membership older than this isn't restored: the cluster has
/// moved on, and joining afresh relearns it.
const VIEW_MAX_AGE: Duration = Duration::from_secs(300);

fn save_view(path: &Path, members: Vec<Member<Id>>) {
    let Ok(data) = postcard::to_allocvec(&members) else { return };
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, data).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

fn load_view(path: &Path) -> Option<Vec<Member<Id>>> {
    let age = std::fs::metadata(path).ok()?.modified().ok()?.elapsed().ok()?;
    if age > VIEW_MAX_AGE {
        return None;
    }
    postcard::from_bytes(&std::fs::read(path).ok()?).ok()
}

/// Tags a datagram that bypasses SWIM: a member's message for agentd alone
/// (its digest). Gossiping per-host digests to every member made each
/// member's work grow with the cluster (foca keeps one broadcast per origin
/// and de-duplicates its queue on every add), for news only agentd reads.
pub const DIRECT: &[u8] = b"CDG1";

/// Runs one SWIM member until `Ctrl::Stop` / `Ctrl::Leave` (or the sender
/// is dropped).
pub async fn run<H>(
    node: Node,
    mut ctrl: mpsc::UnboundedReceiver<Ctrl>,
    traffic: Arc<Traffic>,
    handler: H,
    mut on_notify: impl FnMut(OwnedNotification<Id>) + Send,
) -> Result<()>
where
    H: BroadcastHandler<Id>,
{
    let socket = UdpSocket::bind(node.id.addr).await?;
    // agentd's member takes bursts from every member at once (announces
    // after it restarts): the 208 KB default drops them. The kernel caps
    // this at net.core.rmem_max; memory is used only while packets queue.
    let _ = socket2::SockRef::from(&socket).set_recv_buffer_size(4 << 20);
    let mut config = Config::new_lan(NonZeroU32::new(node.cluster_size.max(2)).unwrap());
    if node.lifeguard {
        config.lifeguard = Some(Lifeguard::default());
    }
    let rng = rand::rngs::StdRng::seed_from_u64(node.seed);
    let mut foca = Foca::with_custom_broadcast(node.id, config, rng, PostcardCodec, handler);
    let mut rt = AccumulatingRuntime::new();
    let mut timers: BinaryHeap<Reverse<(Instant, u64, Timer<Id>)>> = BinaryHeap::new();
    let mut seq = 0u64;
    let seeds = node.seeds;
    let mut direct = node.direct;
    let view = node.view;
    // Back from a restart: the membership as it was, so that relearning
    // it doesn't flood the cluster with news it already has
    if let Some(saved) = view.as_deref().and_then(load_view) {
        if foca.restore_membership(saved, &mut rt).is_ok() {
            traffic.restored.fetch_add(1, Ordering::Relaxed);
        }
    }
    for s in &seeds {
        let _ = foca.announce(*s, &mut rt);
    }
    let save = |foca: &Foca<Id, PostcardCodec, rand::rngs::StdRng, H>| {
        if let Some(p) = &view {
            save_view(p, foca.iter_membership_state().cloned().collect());
        }
    };
    // this member's share of Traffic::health / unhealthy / suspicions
    let mut health = (0u8, 0usize);
    let track_health = |foca: &Foca<Id, PostcardCodec, rand::rngs::StdRng, H>, health: &mut (u8, usize)| {
        let (h, n) = (foca.local_health(), foca.suspicions_tracked());
        if h != health.0 {
            traffic.health.fetch_add(h as i64 - health.0 as i64, Ordering::Relaxed);
            traffic.unhealthy.fetch_add((h > 0) as i64 - (health.0 > 0) as i64, Ordering::Relaxed);
        }
        if n != health.1 {
            traffic.suspicions.fetch_add(n as i64 - health.1 as i64, Ordering::Relaxed);
        }
        *health = (h, n);
    };
    let untrack = |health: (u8, usize)| {
        traffic.health.fetch_sub(health.0 as i64, Ordering::Relaxed);
        traffic.unhealthy.fetch_sub((health.0 > 0) as i64, Ordering::Relaxed);
        traffic.suspicions.fetch_sub(health.1 as i64, Ordering::Relaxed);
    };
    let mut buf = vec![0u8; 2048];
    loop {
        // Act on whatever foca asked for.
        while let Some((to, data)) = rt.to_send() {
            // send_to().await rather than try_send_to(): the latter gives
            // WouldBlock until the reactor has seen the socket writable, which
            // would silently drop a new member's first packet (its announce).
            match socket.send_to(&data, to.addr).await {
                Ok(_) => {
                    traffic.pkts_sent.fetch_add(1, Ordering::Relaxed);
                    traffic.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
                }
                Err(_) => {
                    traffic.send_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        while let Some((after, ev)) = rt.to_schedule() {
            seq += 1;
            timers.push(Reverse((Instant::now() + after, seq, ev)));
        }
        let mut rejoined = false;
        while let Some(n) = rt.to_notify() {
            match &n {
                OwnedNotification::Rejoin(_) => {
                    rejoined = true;
                    traffic.rejoins.fetch_add(1, Ordering::Relaxed);
                }
                OwnedNotification::Rename(..) => {
                    traffic.renames.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            on_notify(n);
        }
        // A member that restarted under an identity the cluster had
        // declared down learns so from its first reply, before it has the
        // membership: announce the renewed identity to get it.
        if rejoined && foca.num_members() <= seeds.len() {
            for s in &seeds {
                let _ = foca.announce(*s, &mut rt);
            }
            continue;
        }
        track_health(&foca, &mut health);
        let next = timers.peek().map(|Reverse((t, _, _))| *t);
        tokio::select! {
            r = socket.recv_from(&mut buf) => {
                if let Ok((n, _)) = r {
                    traffic.pkts_recv.fetch_add(1, Ordering::Relaxed);
                    if buf[..n].starts_with(DIRECT) {
                        if let Some(f) = direct.as_mut() {
                            f(&buf[DIRECT.len()..n]);
                        }
                    } else {
                        let _ = foca.handle_data(&buf[..n], &mut rt);
                    }
                }
            }
            _ = async {
                match next {
                    Some(t) => tokio::time::sleep_until(t.into()).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                let now = Instant::now();
                while let Some(Reverse((t, _, _))) = timers.peek() {
                    if *t > now {
                        break;
                    }
                    let Reverse((_, _, ev)) = timers.pop().unwrap();
                    let _ = foca.handle_timer(ev, &mut rt);
                }
            }
            c = ctrl.recv() => match c {
                Some(Ctrl::Broadcast(d)) => {
                    let _ = foca.add_broadcast(&d);
                    let _ = foca.broadcast(&mut rt);
                }
                Some(Ctrl::Direct(d)) => {
                    let mut msg = DIRECT.to_vec();
                    msg.extend_from_slice(&d);
                    for s in &seeds {
                        match socket.send_to(&msg, s.addr).await {
                            Ok(_) => {
                                traffic.pkts_sent.fetch_add(1, Ordering::Relaxed);
                                traffic.bytes_sent.fetch_add(msg.len() as u64, Ordering::Relaxed);
                            }
                            Err(_) => {
                                traffic.send_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                Some(Ctrl::Announce) => {
                    for s in &seeds {
                        let _ = foca.announce(*s, &mut rt);
                    }
                }
                Some(Ctrl::Leave) => {
                    let _ = foca.leave_cluster(&mut rt);
                    while let Some((to, data)) = rt.to_send() {
                        let _ = socket.send_to(&data, to.addr).await;
                    }
                    save(&foca);
                    untrack(health);
                    return Ok(());
                }
                Some(Ctrl::Stop) | None => {
                    save(&foca);
                    untrack(health);
                    return Ok(());
                }
            }
        }
    }
}
