//! Persisting host/agent records to the mon config-key store, in the same
//! keys and format cephadm uses (host.<h>, host.<h>.devices.0, agent.<h>).
//!
//! Modes:
//!   none       keep state in memory only
//!   sync       before answering the agent, write the three records one
//!              after another, like save_host() + save_agent() in the mgr
//!   writeback  answer immediately; a flusher writes dirty hosts every
//!              --flush-ms, many hosts in parallel (the mon batches
//!              concurrent writes into one paxos proposal)
//!
//! Policy:
//!   always     write on every report (what the mgr does)
//!   changed    write only when something the orchestrator acts on changed
//!              (daemon set/state/image/version, devices, networks, agent
//!              counter/port/key), plus a checkpoint every --checkpoint-secs
//!              to refresh usage samples and timestamps

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::sync::{mpsc, Semaphore};

use crate::rados::Rados;
use crate::state::Cache;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    None,
    Sync,
    Writeback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Policy {
    Always,
    Changed,
}

pub struct Persister {
    rados: Option<Arc<Rados>>,
    pub mode: Mode,
    policy: Policy,
    checkpoint: Duration,
    sem: Arc<Semaphore>,
    dirty: mpsc::UnboundedSender<String>,
    pub writes: AtomicU64,
    pub skipped: AtomicU64,
    pub errors: AtomicU64,
    pub queued: AtomicU64,
    pub latency: Mutex<Histogram<u64>>,
}

type Records = Vec<(String, Vec<u8>)>;

impl Persister {
    pub fn new(
        rados: Option<Arc<Rados>>,
        mode: Mode,
        policy: Policy,
        checkpoint: Duration,
        max_inflight: usize,
    ) -> (Arc<Persister>, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let p = Arc::new(Persister {
            rados,
            mode,
            policy,
            checkpoint,
            sem: Arc::new(Semaphore::new(max_inflight)),
            dirty: tx,
            writes: AtomicU64::new(0),
            skipped: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            latency: Mutex::new(Histogram::new_with_bounds(1, 600_000_000, 3).unwrap()),
        });
        (p, rx)
    }

    /// Serializes the host's records if the policy says they should be
    /// written now.
    fn take(&self, cache: &Cache, host: &str) -> Option<Records> {
        let mut h = cache.hosts.get(host)?.lock().unwrap();
        h.dirty = false;
        if self.policy == Policy::Changed {
            let fp = h.fingerprint();
            let fresh = h.persisted_at.is_some_and(|t| t.elapsed() < self.checkpoint);
            if fp == h.persisted_fingerprint && fresh {
                self.skipped.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            h.persisted_fingerprint = fp;
        }
        h.persisted_at = Some(Instant::now());
        let mut recs = Vec::with_capacity(3);
        if !h.devices.is_empty() {
            recs.push((format!("mgr/cephadm/host.{host}.devices.0"), h.devices_record()));
        }
        recs.push((format!("mgr/cephadm/host.{host}"), h.host_record()));
        recs.push((format!("mgr/cephadm/agent.{host}"), h.agent_record()));
        Some(recs)
    }

    async fn write(self: &Arc<Self>, recs: Records) {
        let Some(rados) = self.rados.clone() else { return };
        let _permit = self.sem.clone().acquire_owned().await.unwrap();
        for (k, v) in recs {
            let rados = rados.clone();
            let t = Instant::now();
            let r = tokio::task::spawn_blocking(move || rados.kv_set(&k, &v)).await;
            let us = t.elapsed().as_micros() as u64;
            self.latency.lock().unwrap().saturating_record(us.max(1));
            match r {
                Ok(Ok(())) => self.writes.fetch_add(1, Ordering::Relaxed),
                _ => self.errors.fetch_add(1, Ordering::Relaxed),
            };
        }
    }

    /// Called after a report for `host` was processed.
    pub async fn on_update(self: &Arc<Self>, cache: &Cache, host: &str) {
        match self.mode {
            Mode::None => {}
            Mode::Sync => {
                if let Some(recs) = self.take(cache, host) {
                    self.write(recs).await;
                }
            }
            Mode::Writeback => {
                let mut h = cache.hosts[host].lock().unwrap();
                if !h.dirty {
                    h.dirty = true;
                    self.queued.fetch_add(1, Ordering::Relaxed);
                    let _ = self.dirty.send(host.to_string());
                }
            }
        }
    }

    /// Writeback flusher.
    pub async fn flusher(self: Arc<Self>, cache: Arc<Cache>, mut rx: mpsc::UnboundedReceiver<String>, every: Duration) {
        let mut tick = tokio::time::interval(every);
        loop {
            tick.tick().await;
            let mut batch = Vec::new();
            while let Ok(h) = rx.try_recv() {
                batch.push(h);
            }
            self.queued.fetch_sub(batch.len() as u64, Ordering::Relaxed);
            for host in batch {
                if let Some(recs) = self.take(&cache, &host) {
                    let me = self.clone();
                    tokio::spawn(async move { me.write(recs).await });
                }
            }
        }
    }
}
