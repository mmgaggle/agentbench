//! Intent relay: the mgr (intentctl for now) stores signed per-host intents
//! and content-addressed blobs on the admin port; agents long-poll for their
//! intent, fetch blobs and post status on the agent port.
//!
//! agentd can't forge or alter an intent (the agent checks the mgr's
//! signature); it decodes the payload only to reject intents that
//! reference blobs it doesn't have, or that don't advance the generation,
//! and to count which blobs the hosts' current intents use.
//!
//! Blobs no current intent references are dropped after a grace period
//! (`gc`): the mgr uploads blobs before the intent that names them, and an
//! agent still applying the previous generation may fetch them a little
//! longer. Without that, every image change, keyring rotation and agent
//! version stayed in memory until agentd restarted.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
use proto::intent::{sha256_hex, unb64, wall_ns, Envelope, HostStatus, Intent};
use serde_json::{json, Value};
use tokio::sync::watch;

/// Events kept: at least this many, and at least EVENTS_PER_HOST per host,
/// as one cluster-wide change makes ~15 per host (a unit event per
/// restarted daemon, intent, applied); with a fixed 100k, a 10,000-host
/// change overran the ring before a 1 s poller came back.
const EVENTS_KEPT: usize = 100_000;
const EVENTS_PER_HOST: usize = 32;

/// Why a long-poll returned.
pub enum Wake {
    Intent(Arc<Envelope>),
    /// agentd thinks it missed a status report: post one
    Poke,
    Timeout,
}

struct Slot {
    poke: Arc<tokio::sync::Notify>,
    /// SWIM: latest digest from the host and when it arrived
    digest: Option<(proto::swim::Digest, u64)>,
    /// digest seq we already asked the host to resync for
    poked_seq: u64,
    /// SWIM liveness: up / down / left, since (wall ns)
    liveness: Option<(&'static str, u64)>,
    envelope: Option<Arc<Envelope>>,
    /// blobs the current envelope references (counted in Blob::refs)
    blobs: Vec<String>,
    /// wall ns the current envelope was stored
    stored_ns: u64,
    gen: watch::Sender<u64>,
    status: Option<HostStatus>,
    status_received_ns: u64,
    status_reports: u64,
}

impl Slot {
    fn new() -> Self {
        Slot {
            poke: Default::default(),
            digest: None,
            poked_seq: 0,
            liveness: None,
            envelope: None,
            blobs: Vec::new(),
            stored_ns: 0,
            gen: watch::channel(0).0,
            status: None,
            status_received_ns: 0,
            status_reports: 0,
        }
    }
}

struct Blob {
    data: Bytes,
    /// current intents that reference it
    refs: u32,
    /// wall ns it was stored or last stopped being referenced
    idle_since: u64,
}

#[derive(Default, Clone, Copy, serde::Serialize)]
pub struct GcStats {
    pub runs: u64,
    pub removed: u64,
    pub removed_bytes: u64,
    pub last_ms: f64,
}

#[derive(Default)]
pub struct Intents {
    /// Lock order: blobs, then hosts.
    blobs: RwLock<HashMap<String, Blob>>,
    blob_bytes: AtomicU64,
    gc_stats: Mutex<GcStats>,
    hosts: Mutex<HashMap<String, Slot>>,
    /// Unit state transitions and applied generations as agents report them:
    /// the raw material for cephadm's change feed, and for latency
    /// measurements.
    /// Kept serialized (with their seq): a third of the memory of a JSON
    /// tree, and a poll's response is a concatenation.
    events: Mutex<(u64, VecDeque<Box<str>>)>,
    /// Hosts agentd has a slot for, to size the event ring
    host_hint: std::sync::atomic::AtomicUsize,
    joins: Mutex<HashMap<String, Join>>,
    members: Mutex<HashMap<String, Member>>,
    /// agentd runs a SWIM member (liveness comes from it)
    pub swim: std::sync::atomic::AtomicBool,
}

impl Intents {
    pub fn put_blob(&self, data: Bytes) -> String {
        let sha = sha256_hex(&data);
        let now = wall_ns();
        let mut blobs = self.blobs.write().unwrap();
        match blobs.entry(sha.clone()) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                // uploaded again: an intent naming it is on its way
                if e.get().refs == 0 {
                    e.get_mut().idle_since = now;
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                // a body hyper read in one piece is a slice of the
                // connection's read buffer: kept as is, a 1 KB blob held
                // ~5 KB (270,000 blobs on 10,000 hosts: 1.3 GB)
                self.blob_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                e.insert(Blob { data: Bytes::copy_from_slice(&data), refs: 0, idle_since: now });
            }
        }
        sha
    }

    pub fn blob(&self, sha: &str) -> Option<Bytes> {
        self.blobs.read().unwrap().get(sha).map(|b| b.data.clone())
    }

    /// Drops blobs that no current intent has referenced for `grace`;
    /// returns this run's count (`removed`, `removed_bytes`) and time.
    pub fn gc(&self, grace: Duration) -> GcStats {
        let t0 = std::time::Instant::now();
        let now = wall_ns();
        let grace = grace.as_nanos() as u64;
        let (mut removed, mut bytes) = (0, 0);
        {
            let mut blobs = self.blobs.write().unwrap();
            blobs.retain(|_, b| {
                let keep = b.refs > 0 || now.saturating_sub(b.idle_since) < grace;
                if !keep {
                    removed += 1;
                    bytes += b.data.len() as u64;
                }
                keep
            });
            if blobs.capacity() > 4 * blobs.len().max(1024) {
                blobs.shrink_to_fit();
            }
        }
        self.blob_bytes.fetch_sub(bytes, Ordering::Relaxed);
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        let mut st = self.gc_stats.lock().unwrap();
        st.runs += 1;
        st.removed += removed;
        st.removed_bytes += bytes;
        st.last_ms = ms;
        GcStats { runs: st.runs, removed, removed_bytes: bytes, last_ms: ms }
    }

    /// Blob store size: (blobs, bytes, unreferenced blobs, gc counters)
    pub fn blob_stats(&self) -> (usize, u64, usize, GcStats) {
        let blobs = self.blobs.read().unwrap();
        let idle = blobs.values().filter(|b| b.refs == 0).count();
        (blobs.len(), self.blob_bytes.load(Ordering::Relaxed), idle, *self.gc_stats.lock().unwrap())
    }

    /// Moves the references of `slot`'s current envelope to `want`.
    fn rereference(blobs: &mut HashMap<String, Blob>, slot: &mut Slot, want: Vec<String>) {
        for b in &want {
            if let Some(e) = blobs.get_mut(b) {
                e.refs += 1;
            }
        }
        let now = wall_ns();
        for b in std::mem::replace(&mut slot.blobs, want) {
            if let Some(e) = blobs.get_mut(&b) {
                e.refs -= 1;
                if e.refs == 0 {
                    e.idle_since = now;
                }
            }
        }
    }

    /// The mgr removed the host: forget its intent and status, so its
    /// blobs can go.
    pub fn forget_host(&self, host: &str) -> bool {
        let mut blobs = self.blobs.write().unwrap();
        let mut hosts = self.hosts.lock().unwrap();
        let Some(mut slot) = hosts.remove(host) else { return false };
        Self::rereference(&mut blobs, &mut slot, Vec::new());
        drop(hosts);
        drop(blobs);
        self.event(json!({"kind": "host-removed", "host": host, "at_ns": wall_ns()}));
        true
    }

    pub fn put_intent(&self, env: Envelope) -> Result<u64, String> {
        let payload = unb64(&env.payload).map_err(|e| e.to_string())?;
        let intent: Intent = serde_json::from_slice(&payload).map_err(|e| format!("payload: {e}"))?;
        if intent.host != env.host || intent.generation != env.generation {
            return Err("envelope host/generation don't match the payload".into());
        }
        let mut want = intent.blobs();
        want.extend(intent.agent.iter().map(|a| a.sha256.clone()));
        want.sort();
        want.dedup();
        // the blob check and the reference count happen under one lock, so
        // gc can't drop a blob in between
        let mut blobs = self.blobs.write().unwrap();
        let missing: Vec<&str> = want.iter().filter(|b| !blobs.contains_key(*b)).map(|b| b.as_str()).collect();
        if !missing.is_empty() {
            return Err(format!("missing blobs: {}", missing.join(", ")));
        }
        let mut hosts = self.hosts.lock().unwrap();
        self.host_hint.store(hosts.len() + 1, Ordering::Relaxed);
        let slot = hosts.entry(env.host.clone()).or_insert_with(Slot::new);
        let have = *slot.gen.borrow();
        if env.generation <= have {
            return Err(format!("generation {} is not newer than {have}", env.generation));
        }
        let g = env.generation;
        Self::rereference(&mut blobs, slot, want);
        slot.envelope = Some(Arc::new(env));
        slot.stored_ns = wall_ns();
        slot.gen.send_replace(g);
        drop(hosts);
        drop(blobs);
        self.event(json!({"kind": "intent", "host": intent.host, "generation": g, "at_ns": wall_ns()}));
        Ok(g)
    }

    pub fn intent(&self, host: &str) -> Option<Arc<Envelope>> {
        self.hosts.lock().unwrap().get(host).and_then(|s| s.envelope.clone())
    }

    /// Long-poll: the host's envelope once its generation exceeds `have`,
    /// a poke, or a timeout after `wait`.
    pub async fn wait_intent(&self, host: &str, have: u64, wait: Duration) -> Wake {
        let (mut rx, poke) = {
            let mut hosts = self.hosts.lock().unwrap();
            let slot = hosts.entry(host.to_string()).or_insert_with(Slot::new);
            (slot.gen.subscribe(), slot.poke.clone())
        };
        let ready = tokio::select! {
            r = tokio::time::timeout(wait, rx.wait_for(|g| *g > have)) => matches!(r, Ok(Ok(_))),
            _ = poke.notified() => return Wake::Poke,
        };
        match (ready, self.intent(host)) {
            (true, Some(env)) => Wake::Intent(env),
            _ => Wake::Timeout,
        }
    }

    /// Stores a host's status. Returns whether agentd's SWIM member sees
    /// the host up (None without SWIM): a host it doesn't see re-announces.
    pub fn put_status(&self, st: HostStatus) -> Option<bool> {
        let now = wall_ns();
        let mut out = Vec::new();
        let swim_up;
        {
            let mut hosts = self.hosts.lock().unwrap();
            self.host_hint.store(hosts.len() + 1, Ordering::Relaxed);
            let slot = hosts.entry(st.host.clone()).or_insert_with(Slot::new);
            let prev = slot.status.take();
            let prev_units = prev.as_ref().map(|p| &p.units);
            for (daemon, u) in &st.units {
                let old = prev_units.and_then(|m| m.get(daemon));
                if old.map(|o| o.material() != u.material()).unwrap_or(true) {
                    out.push(json!({
                        "kind": "unit", "host": st.host, "daemon": daemon,
                        "from": old.map(|o| format!("{}/{}", o.active_state, o.sub_state)),
                        "to": format!("{}/{}", u.active_state, u.sub_state),
                        "result": u.result, "n_restarts": u.n_restarts, "apply_error": u.apply_error,
                        "observed_ns": u.observed_ns, "sent_ns": st.sent_ns, "received_ns": now,
                    }));
                }
            }
            if let Some(p) = &prev {
                for daemon in p.units.keys().filter(|d| !st.units.contains_key(*d)) {
                    out.push(json!({"kind": "unit", "host": st.host, "daemon": daemon, "to": "gone",
                                    "sent_ns": st.sent_ns, "received_ns": now}));
                }
            }
            for (id, j) in &st.jobs {
                let old = prev.as_ref().and_then(|p| p.jobs.get(id));
                if old.map(|o| o.state != j.state).unwrap_or(true) {
                    out.push(json!({
                        "kind": "job", "host": st.host, "job": id, "state": j.state, "exit_code": j.exit_code,
                        "error": j.error, "secs": (j.finished_ns.saturating_sub(j.started_ns)) as f64 / 1e9,
                        "sent_ns": st.sent_ns, "received_ns": now,
                    }));
                }
            }
            // a new agent process (restart, upgrade, rollback): the mgr's
            // rollout follows these
            if prev.as_ref().map(|p| (&p.agent_sha256, p.agent_boot)) != Some((&st.agent_sha256, st.agent_boot)) {
                out.push(json!({"kind": "agent", "host": st.host, "version": st.agent_version, "sha256": st.agent_sha256,
                                "boot": st.agent_boot, "from_sha256": prev.as_ref().map(|p| &p.agent_sha256),
                                "errors": st.errors, "sent_ns": st.sent_ns, "received_ns": now}));
            }
            if prev.as_ref().map(|p| p.applied_generation) != Some(st.applied_generation) {
                out.push(json!({"kind": "applied", "host": st.host, "generation": st.applied_generation,
                                "intent_stored_ns": slot.stored_ns, "sent_ns": st.sent_ns, "received_ns": now}));
            }
            slot.status = Some(st);
            slot.status_received_ns = now;
            slot.status_reports += 1;
            swim_up = self.swim.load(Ordering::Relaxed).then(|| matches!(slot.liveness, Some(("up", _))));
        }
        for e in out {
            self.event(e);
        }
        swim_up
    }

    fn event(&self, mut e: Value) {
        let keep = EVENTS_KEPT.max(EVENTS_PER_HOST * self.host_hint.load(Ordering::Relaxed));
        let mut ev = self.events.lock().unwrap();
        ev.0 += 1;
        e["seq"] = json!(ev.0);
        ev.1.push_back(e.to_string().into_boxed_str());
        while ev.1.len() > keep {
            ev.1.pop_front();
        }
    }

    /// Events after `since`, as the JSON response body. Sequence numbers
    /// are contiguous, so the ring is indexed rather than scanned. `first`
    /// is the oldest event still kept: a consumer whose `since` is older
    /// than that has missed events and must resync.
    pub fn events_since(&self, since: u64) -> String {
        let ev = self.events.lock().unwrap();
        let first = ev.0 + 1 - ev.1.len() as u64;
        let skip = since.saturating_add(1).saturating_sub(first).min(ev.1.len() as u64) as usize;
        let len: usize = ev.1.range(skip..).map(|e| e.len() + 1).sum();
        let mut out = String::with_capacity(len + 64);
        out.push_str(&format!("{{\"seq\":{},\"first\":{first},\"events\":[", ev.0));
        for (i, e) in ev.1.range(skip..).enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(e);
        }
        out.push_str("]}");
        out
    }

    pub fn status(&self, host: &str) -> Option<Value> {
        let hosts = self.hosts.lock().unwrap();
        let s = hosts.get(host)?;
        Some(json!({
            "intended_generation": *s.gen.borrow(),
            "status": s.status,
            "status_received_ns": s.status_received_ns,
            "status_reports": s.status_reports,
        }))
    }

    /// Latest status of each named host (null if none): the batch form of
    /// `status`, so a consumer of the event feed re-reads every changed
    /// host in one request.
    pub fn statuses(&self, names: &[String]) -> Value {
        let hosts = self.hosts.lock().unwrap();
        let m: serde_json::Map<String, Value> =
            names.iter().map(|h| (h.clone(), hosts.get(h).and_then(|s| s.status.as_ref()).map(|st| json!(st)).unwrap_or(Value::Null))).collect();
        Value::Object(m)
    }

    /// One line per host: intended vs applied generation and unit states.
    pub fn summary(&self) -> Value {
        let hosts = self.hosts.lock().unwrap();
        let mut out = BTreeMap::new();
        for (h, s) in hosts.iter() {
            let units: BTreeMap<&String, Value> = s
                .status
                .iter()
                .flat_map(|st| st.units.iter())
                .map(|(d, u)| (d, json!(format!("{}/{}{}", u.active_state, u.sub_state, if u.apply_error.is_some() { " (apply error)" } else { "" }))))
                .collect();
            out.insert(
                h.clone(),
                json!({
                    "liveness": s.liveness.map(|(st, at)| json!({"state": st, "age_s": (wall_ns() - at) as f64 / 1e9})),
                    "digest": s.digest.map(|(d, _)| json!({"applied": d.applied_generation, "state_version": d.state_version, "failed": d.failed_units, "mode": d.mode})),
                    "intended": *s.gen.borrow(),
                    "applied": s.status.as_ref().map(|st| st.applied_generation),
                    "units": units,
                    "errors": s.status.as_ref().map(|st| &st.errors),
                    "strays": s.status.as_ref().map(|st| &st.strays),
                    "last_report_age_s": if s.status_received_ns > 0 { Some((wall_ns() - s.status_received_ns) as f64 / 1e9) } else { None },
                }),
            );
        }
        json!(out)
    }

    /// Per host: SWIM liveness, whether a status arrived, applied
    /// generation. Cheap enough to poll, unlike the summary.
    pub fn liveness_map(&self) -> Value {
        let hosts = self.hosts.lock().unwrap();
        let m: serde_json::Map<String, Value> = hosts
            .iter()
            .filter(|(h, _)| !h.starts_with("member-"))
            .map(|(h, s)| (h.clone(), json!([s.liveness.map(|l| l.0), s.status.is_some(), s.status.as_ref().map(|st| st.applied_generation)])))
            .collect();
        Value::Object(m)
    }

    pub fn host_count(&self) -> usize {
        self.hosts.lock().unwrap().len()
    }
}

/// A pending join: agentd only relays it. The mgr checks the request's MAC
/// with the token secret and answers with a decision MACed the same way.
struct Join {
    req: proto::join::JoinRequest,
    received_ns: u64,
    decision: watch::Sender<Option<Arc<proto::join::JoinDecision>>>,
}

#[derive(Clone, Debug, Default)]
pub struct Member {
    pub swim_idx: Option<u32>,
}

const MAX_PENDING_JOINS: usize = 1024;

impl Intents {
    pub fn submit_join(&self, req: proto::join::JoinRequest) -> Result<String, String> {
        let mut joins = self.joins.lock().unwrap();
        let pending = joins.values().filter(|j| j.decision.borrow().is_none()).count();
        if pending >= MAX_PENDING_JOINS {
            return Err("too many pending joins".into());
        }
        let id = proto::join::random_id().map_err(|e| e.to_string())?;
        self.event(json!({"kind": "join-request", "host": req.host, "join_id": id, "at_ns": wall_ns()}));
        joins.insert(id.clone(), Join { req, received_ns: wall_ns(), decision: watch::channel(None).0 });
        Ok(id)
    }

    /// None: no such join. Some(None): still pending after `wait`.
    pub async fn wait_join(&self, id: &str, wait: Duration) -> Option<Option<Arc<proto::join::JoinDecision>>> {
        let mut rx = self.joins.lock().unwrap().get(id)?.decision.subscribe();
        let _ = tokio::time::timeout(wait, rx.wait_for(|d| d.is_some())).await;
        let d = rx.borrow().clone();
        Some(d)
    }

    pub fn pending_joins(&self) -> Value {
        let joins = self.joins.lock().unwrap();
        let v: Vec<Value> = joins
            .iter()
            .filter(|(_, j)| j.decision.borrow().is_none())
            .map(|(id, j)| json!({"join_id": id, "received_ns": j.received_ns, "request": j.req}))
            .collect();
        json!(v)
    }

    pub fn decide_join(&self, id: &str, d: proto::join::JoinDecision) -> Result<(), String> {
        let host = {
            let joins = self.joins.lock().unwrap();
            let j = joins.get(id).ok_or("no such join")?;
            if j.decision.borrow().is_some() {
                return Err("already decided".into());
            }
            if d.approved && d.host != j.req.host {
                return Err("decision is for another host".into());
            }
            j.req.host.clone()
        };
        if d.approved {
            let idx = d.swim.as_ref().map(|s| s.idx);
            self.members.lock().unwrap().insert(d.host.clone(), Member { swim_idx: idx });
            if let Some(i) = idx {
                self.adopt(i, &d.host);
            }
        }
        self.event(json!({"kind": "join-decision", "host": host, "join_id": id, "approved": d.approved,
                          "reason": d.reason, "at_ns": wall_ns()}));
        if let Some(j) = self.joins.lock().unwrap().get(id) {
            j.decision.send_replace(Some(Arc::new(d)));
        }
        Ok(())
    }

    /// The mgr's view of joined hosts (after an agentd restart, or when it
    /// removes hosts): replaces agentd's.
    pub fn set_members(&self, m: HashMap<String, Member>) {
        let named: Vec<(u32, String)> = m.iter().filter_map(|(h, mm)| mm.swim_idx.map(|i| (i, h.clone()))).collect();
        *self.members.lock().unwrap() = m;
        for (idx, host) in named {
            self.adopt(idx, &host);
        }
    }

    /// SWIM news about member `idx` that arrived before agentd knew its
    /// host (after a restart, agents re-announce before the mgr re-teaches
    /// the members) was kept under "member-<idx>": move it to the host.
    /// foca reports a member up once, so it would otherwise never arrive.
    fn adopt(&self, idx: u32, host: &str) {
        let mut hosts = self.hosts.lock().unwrap();
        let Some(m) = hosts.remove(&format!("member-{idx}")) else { return };
        let slot = hosts.entry(host.to_string()).or_insert_with(Slot::new);
        if slot.liveness.map(|(_, at)| at).unwrap_or(0) < m.liveness.map(|(_, at)| at).unwrap_or(0) {
            slot.liveness = m.liveness;
        }
        if slot.digest.map(|(d, _)| d.seq).unwrap_or(0) < m.digest.map(|(d, _)| d.seq).unwrap_or(0) {
            slot.digest = m.digest;
        }
    }

    pub fn is_member(&self, host: &str) -> bool {
        self.members.lock().unwrap().contains_key(host)
    }

    pub fn host_for_idx(&self, idx: u32) -> Option<String> {
        self.members.lock().unwrap().iter().find(|(_, m)| m.swim_idx == Some(idx)).map(|(h, _)| h.clone())
    }
}

impl Intents {
    fn host_name(&self, idx: u32) -> String {
        self.host_for_idx(idx).unwrap_or_else(|| format!("member-{idx}"))
    }

    /// SWIM membership change for member `idx`.
    pub fn liveness(&self, idx: u32, up: bool) {
        let host = self.host_name(idx);
        let now = wall_ns();
        let state = {
            let mut hosts = self.hosts.lock().unwrap();
            let slot = hosts.entry(host.clone()).or_insert_with(Slot::new);
            let left = slot.digest.map(|(d, _)| d.mode == proto::swim::MODE_SHUTDOWN).unwrap_or(false);
            let state = if up { "up" } else if left { "left" } else { "down" };
            slot.liveness = Some((state, now));
            state
        };
        self.event(json!({"kind": "liveness", "host": host, "state": state, "at_ns": now}));
    }

    pub fn digest(&self, d: proto::swim::Digest) {
        let host = self.host_name(d.idx);
        let now = wall_ns();
        let mut hosts = self.hosts.lock().unwrap();
        let slot = hosts.entry(host.clone()).or_insert_with(Slot::new);
        if slot.digest.map(|(old, _)| old.seq >= d.seq).unwrap_or(false) {
            return; // reordered or repeated
        }
        slot.digest = Some((d, now));
        drop(hosts);
        if d.mode == proto::swim::MODE_SHUTDOWN {
            self.event(json!({"kind": "leaving", "host": host, "at_ns": now}));
        }
    }

    /// Hosts whose gossiped digest is ahead of the last status agentd got
    /// (for at least `grace`): wake their long-poll and return their member
    /// indexes for a SWIM poke.
    pub fn resync_stale(&self, grace: Duration) -> Vec<u32> {
        let now = wall_ns();
        let mut out = Vec::new();
        let mut ev = Vec::new();
        let mut hosts = self.hosts.lock().unwrap();
        for (h, s) in hosts.iter_mut() {
            let Some((d, at)) = s.digest else { continue };
            if now - at < grace.as_nanos() as u64 || s.poked_seq >= d.seq {
                continue;
            }
            let behind = match &s.status {
                None => Some("no status yet".to_string()),
                Some(st) if st.agent_boot != d.boot => Some("agent restarted".into()),
                Some(st) if st.state_version < d.state_version => Some(format!("state {} < {}", st.state_version, d.state_version)),
                Some(st) if st.applied_generation != d.applied_generation => Some("applied generation differs".into()),
                _ => None,
            };
            if let Some(reason) = behind {
                s.poked_seq = d.seq;
                s.poke.notify_one();
                out.push(d.idx);
                ev.push(json!({"kind": "resync", "host": h, "reason": reason, "at_ns": now}));
            }
        }
        drop(hosts);
        for e in ev {
            self.event(e);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(host: &str, generation: u64, blobs: &[&str], agent: Option<&str>) -> Envelope {
        let files: serde_json::Map<String, Value> =
            blobs.iter().enumerate().map(|(i, b)| (format!("/etc/f{i}"), json!({"sha256": b, "mode": "0644"}))).collect();
        let payload = json!({
            "host": host, "fsid": "f", "generation": generation, "files": files, "units": {},
            "agent": agent.map(|a| json!({"version": "x", "sha256": a})),
        });
        Envelope {
            host: host.into(),
            generation,
            key_id: "k".into(),
            payload: proto::intent::b64(payload.to_string().as_bytes()),
            sig: String::new(),
        }
    }

    #[test]
    fn gc_drops_only_unreferenced_blobs_after_grace() {
        let i = Intents::default();
        let a = i.put_blob(Bytes::from_static(b"a"));
        let b = i.put_blob(Bytes::from_static(b"b"));
        let c = i.put_blob(Bytes::from_static(b"c"));
        let bin = i.put_blob(Bytes::from_static(b"agent binary"));
        i.put_intent(envelope("h1", 1, &[&a, &b], Some(&bin))).unwrap();
        i.put_intent(envelope("h2", 1, &[&a], None)).unwrap();
        // c was never referenced, but is within the grace period
        assert_eq!(i.gc(Duration::from_secs(600)).removed, 0);
        assert_eq!(i.gc(Duration::ZERO).removed, 1);
        assert!(i.blob(&c).is_none());

        // h1 moves on to c: b and the binary lose their last reference;
        // a is still h2's
        let c = i.put_blob(Bytes::from_static(b"c"));
        i.put_intent(envelope("h1", 2, &[&a, &c], None)).unwrap();
        assert_eq!(i.gc(Duration::ZERO).removed, 2);
        assert!(i.blob(&a).is_some() && i.blob(&c).is_some());
        assert!(i.blob(&b).is_none() && i.blob(&bin).is_none());

        // a rejected intent changes no reference
        assert!(i.put_intent(envelope("h2", 1, &[&c], None)).is_err());
        assert!(i.put_intent(envelope("h2", 2, &[&b], None)).is_err());
        assert_eq!(i.gc(Duration::ZERO).removed, 0);

        // removing h2 releases a; h1 still holds a and c
        assert!(i.forget_host("h2"));
        assert_eq!(i.gc(Duration::ZERO).removed, 0);
        i.put_intent(envelope("h1", 3, &[&c], None)).unwrap();
        assert!(i.forget_host("h1"));
        assert_eq!(i.gc(Duration::ZERO).removed, 2);
        assert_eq!(i.blob_stats().0, 0);
        assert_eq!(i.blob_stats().1, 0);
    }

    #[test]
    fn stored_blob_does_not_pin_the_read_buffer() {
        let i = Intents::default();
        let buf = Bytes::from(vec![7u8; 8192]);
        let sha = i.put_blob(buf.slice(0..10));
        let held = i.blob(&sha).unwrap();
        // a copy, not a view into `buf`
        assert!(!buf.as_ptr_range().contains(&held.as_ptr()));
        assert_eq!(i.blob_stats().1, 10);
    }

    #[test]
    fn agent_event_on_new_process() {
        let i = Intents::default();
        let st = |boot: u64, sha: &str| -> HostStatus {
            serde_json::from_value(json!({
                "host": "h1", "fsid": "f", "agent_version": "1", "agent_sha256": sha, "applied_generation": 1, "state_version": 1,
                "agent_boot": boot, "mode": "normal", "units": {}, "strays": [], "errors": [], "sent_ns": 0,
            }))
            .unwrap()
        };
        let kinds = |i: &Intents, since: u64| -> Vec<String> {
            let v: Value = serde_json::from_str(&i.events_since(since)).unwrap();
            v["events"].as_array().unwrap().iter().map(|e| e["kind"].as_str().unwrap().to_string()).collect()
        };
        i.put_status(st(1, "aaa"));
        assert_eq!(kinds(&i, 0), ["agent", "applied"]);
        i.put_status(st(1, "aaa"));
        assert_eq!(kinds(&i, 2), Vec::<String>::new());
        i.put_status(st(2, "bbb"));
        let v: Value = serde_json::from_str(&i.events_since(2)).unwrap();
        assert_eq!(v["events"][0]["kind"], "agent");
        assert_eq!(v["events"][0]["sha256"], "bbb");
        assert_eq!(v["events"][0]["from_sha256"], "aaa");
    }
}
