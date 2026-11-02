//! Makes the host match its intent: blobs, images, files, Quadlet units and
//! unit states. Idempotent: it runs on every new intent and periodically,
//! to repair drift and retry failures.
//!
//! Safety rules from the design: only `remove` deletes a unit, a unit the
//! intent doesn't mention is reported as a stray and left alone, and a
//! removed daemon's data directory is archived, never deleted.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions, Permissions};
use std::io::Write as _;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use proto::intent::{self, sha256_hex, wall_ns, AgentSpec, Desired, Envelope, FileSpec, HostStatus, Intent, Mode, UnitSpec, UnitStatus};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::client::Client;
use crate::systemd::{Backend, UnitObs};
use crate::Config;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AppliedUnit {
    pub quadlet: String,
    pub restart_key: String,
}

/// What the agent last applied successfully; persisted so a restarted
/// agent knows which restart keys it has honored.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Applied {
    pub generation: u64,
    pub fsid: String,
    pub units: BTreeMap<String, AppliedUnit>,
}

#[derive(Default)]
pub struct Meta {
    /// job ids in the current intent
    pub job_ids: Vec<String>,
    pub shutting_down: bool,
    pub fsid: String,
    pub mode: Mode,
    pub state_version: u64,
    pub apply_errors: Vec<String>,
    pub intent_errors: Vec<String>,
    pub strays: Vec<String>,
}

struct Backoff {
    spec: String,
    attempts: u32,
    next: Instant,
    error: String,
}

pub struct Ctx {
    /// process start, wall ms
    pub boot: u64,
    /// SHA-256 of the running binary
    pub self_sha: String,
    /// a status report reached agentd since startup
    pub posted: std::sync::atomic::AtomicBool,
    pub cfg: Config,
    seal: Option<x25519_dalek::StaticSecret>,
    pub backend: Backend,
    pub client: Option<Client>,
    pub applied: Mutex<Applied>,
    /// daemon -> unit status as reported
    pub observed: Mutex<BTreeMap<String, UnitStatus>>,
    /// unit name -> daemon, to route systemd events
    pub unit_daemon: Mutex<HashMap<String, String>>,
    /// daemon -> cgroup path, for usage
    cgroups: Mutex<HashMap<String, String>>,
    pub meta: Mutex<Meta>,
    /// Signals the reporter that something material changed.
    pub dirty: Notify,
    /// Asks the main loop for another reconcile pass (a job finished).
    pub kick: Notify,
    pub jobs: crate::jobs::Jobs,
    backoff: Mutex<HashMap<String, Backoff>>,
    /// The simulator: this agent "process" should end and start again as
    /// `restart_to` (a simulated agent upgrade)
    pub restart: Notify,
    pub restart_to: Mutex<Option<AgentSpec>>,
}

fn check(result: String, verb: &str) -> Result<()> {
    if result == "done" {
        Ok(())
    } else {
        bail!("{verb} job {result}")
    }
}

/// Writes `data` to `path` atomically (temp file, fsync, rename) unless the
/// content already matches; fixes mode and owner either way. Returns
/// whether the content changed.
pub fn write_file(path: &Path, data: &[u8], spec: &FileSpec) -> Result<bool> {
    let mode = spec.mode_bits()?;
    let owner = spec.owner_ids()?;
    if fs::read(path).map(|c| c == data).unwrap_or(false) {
        let meta = fs::metadata(path)?;
        if meta.permissions().mode() & 0o7777 != mode {
            fs::set_permissions(path, Permissions::from_mode(mode))?;
        }
        if let Some((u, g)) = owner {
            if meta.uid() != u || meta.gid() != g {
                std::os::unix::fs::chown(path, Some(u), Some(g))?;
            }
        }
        return Ok(false);
    }
    write_atomic(path, data, mode, owner)?;
    Ok(true)
}

pub fn write_atomic(path: &Path, data: &[u8], mode: u32, owner: Option<(u32, u32)>) -> Result<()> {
    let dir = path.parent().context("path has no parent")?;
    fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(".{}.agent-tmp", path.file_name().context("no file name")?.to_string_lossy()));
    {
        let mut f = OpenOptions::new().write(true).create(true).truncate(true).mode(mode).open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::set_permissions(&tmp, Permissions::from_mode(mode))?;
    if let Some((u, g)) = owner {
        std::os::unix::fs::chown(&tmp, Some(u), Some(g))?;
    }
    fs::rename(&tmp, path)?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

fn quadlet_images(text: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(text)
        .lines()
        .filter_map(|l| l.trim().strip_prefix("Image="))
        .map(|s| s.trim().to_string())
        .collect()
}

async fn image_present(img: &str) -> bool {
    tokio::process::Command::new("podman").args(["image", "exists", img]).status().await.map(|s| s.success()).unwrap_or(false)
}

async fn pull_image(img: &str) -> Result<()> {
    let out = tokio::process::Command::new("podman").args(["pull", "-q", img]).output().await?;
    if !out.status.success() {
        bail!("podman pull: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}

fn spec_key(u: &UnitSpec) -> String {
    format!("{}/{}", u.quadlet, u.restart_key)
}

impl Ctx {
    pub fn new(cfg: Config, backend: Backend, client: Option<Client>) -> Result<(Ctx, tokio::sync::mpsc::UnboundedReceiver<(String, proto::intent::JobSpec)>)> {
        fs::create_dir_all(cfg.state_dir.join("blobs"))?;
        let (jobs, job_rx) = crate::jobs::Jobs::new(&cfg.state_dir);
        let applied = fs::read(cfg.state_dir.join("applied.json"))
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let seal = match &cfg.seal_key {
            Some(p) => Some(proto::seal::load_secret(p)?),
            None => None,
        };
        Ok((Ctx {
            self_sha: cfg.sim_binary.as_ref().map(|a| a.sha256.clone()).unwrap_or_else(crate::upgrade::self_sha),
            restart: Notify::new(),
            restart_to: Mutex::new(None),
            posted: Default::default(),
            kick: Notify::new(),
            jobs,
            boot: wall_ns() / 1_000_000,
            seal,
            cfg,
            backend,
            client,
            applied: Mutex::new(applied),
            observed: Default::default(),
            unit_daemon: Default::default(),
            cgroups: Default::default(),
            meta: Default::default(),
            dirty: Notify::new(),
            backoff: Default::default(),
        }, job_rx))
    }

    /// Something material changed: bump the state version, wake the reporter.
    pub fn changed(&self) {
        self.meta.lock().unwrap().state_version += 1;
        self.dirty.notify_one();
    }

    /// The last verified intent, so a rebooted host converges before it
    /// reaches agentd.
    pub fn load_saved_intent(&self, trusted: &[ed25519_dalek::VerifyingKey]) -> Option<Intent> {
        let env: Envelope = serde_json::from_slice(&fs::read(self.cfg.state_dir.join("intent.json")).ok()?).ok()?;
        match intent::open(&env, trusted) {
            Ok(i) if i.host == self.cfg.host => Some(i),
            Ok(_) => None,
            Err(e) => {
                eprintln!("ignoring saved intent: {e:#}");
                None
            }
        }
    }

    pub fn save_envelope(&self, env: &Envelope) -> Result<()> {
        write_atomic(&self.cfg.state_dir.join("intent.json"), &serde_json::to_vec(env)?, 0o600, None)
    }

    pub fn set_intent_errors(&self, errors: Vec<String>) {
        let mut m = self.meta.lock().unwrap();
        if m.intent_errors != errors {
            m.intent_errors = errors;
            m.state_version += 1;
            drop(m);
            self.dirty.notify_one();
        }
    }

    pub async fn blob(&self, sha: &str) -> Result<Vec<u8>> {
        let p = self.cfg.state_dir.join("blobs").join(sha);
        if let Ok(b) = fs::read(&p) {
            if sha256_hex(&b) == sha {
                return Ok(b);
            }
        }
        let client = self.client.as_ref().context("blob is not cached and there is no agentd")?;
        let b = client.blob(sha).await?;
        if sha256_hex(&b) != sha {
            bail!("content does not match its hash");
        }
        write_atomic(&p, &b, 0o600, None)?;
        Ok(b.to_vec())
    }

    /// Makes sure `ceph.target` and `ceph-<fsid>.target` exist and are
    /// enabled, so the Quadlet units (WantedBy=ceph-<fsid>.target) start at
    /// boot without the agent.
    fn ensure_targets(&self, fsid: &str) -> Result<bool> {
        let dir = &self.cfg.unit_dir;
        let mut changed = false;
        let ceph = b"[Unit]\nDescription=All Ceph clusters and services\n\n[Install]\nWantedBy=multi-user.target\n";
        let cluster = format!(
            "[Unit]\nDescription=Ceph cluster {fsid}\nPartOf=ceph.target\nBefore=ceph.target\n\n[Install]\nWantedBy=multi-user.target ceph.target\n"
        );
        for (name, text) in [("ceph.target".to_string(), ceph.to_vec()), (format!("ceph-{fsid}.target"), cluster.into_bytes())] {
            let spec = FileSpec { sha256: sha256_hex(&text), mode: "0644".into(), owner: None, sealed: false };
            changed |= write_file(&dir.join(&name), &text, &spec)?;
        }
        for (wants, unit) in [("multi-user.target.wants", "ceph.target".to_string()), ("ceph.target.wants", format!("ceph-{fsid}.target"))] {
            let link = dir.join(wants).join(&unit);
            if fs::symlink_metadata(&link).is_err() {
                fs::create_dir_all(dir.join(wants))?;
                std::os::unix::fs::symlink(format!("../{unit}"), &link)?;
                changed = true;
            }
        }
        Ok(changed)
    }

    pub async fn reconcile(&self, it: &Intent) {
        let t0 = Instant::now();
        let mut errors = Vec::new();
        let mut unit_err: BTreeMap<String, String> = BTreeMap::new();

        let mut blobs: HashMap<String, Vec<u8>> = HashMap::new();
        for sha in it.blobs() {
            match self.blob(&sha).await {
                Ok(b) => {
                    blobs.insert(sha, b);
                }
                Err(e) => errors.push(format!("blob {sha}: {e:#}")),
            }
        }

        if self.cfg.pull_images && !self.backend.is_fake() {
            let mut images: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for img in &it.prefetch {
                images.entry(img.clone()).or_default();
            }
            for (d, u) in &it.units {
                for img in blobs.get(&u.quadlet).map(|t| quadlet_images(t)).unwrap_or_default() {
                    images.entry(img).or_default().push(d.clone());
                }
            }
            let present = futures_util::future::join_all(images.keys().map(|i| image_present(i))).await;
            images = images.into_iter().zip(present).filter(|(_, p)| !p).map(|(kv, _)| kv).collect();
            if !images.is_empty() && it.pull_spread_s > 0 {
                let wait = intent::pull_offset(&it.host, it.pull_spread_s);
                eprintln!("pulling {} image(s) after a {wait}s offset (spread {}s)", images.len(), it.pull_spread_s);
                tokio::time::sleep(Duration::from_secs(wait as u64)).await;
            }
            let pulls = images.iter().map(|(img, _)| pull_image(img));
            let results = futures_util::future::join_all(pulls).await;
            for ((img, daemons), r) in images.iter().zip(results) {
                if let Err(e) = r {
                    if daemons.is_empty() {
                        errors.push(format!("prefetch {img}: {e:#}"));
                    }
                    for d in daemons {
                        unit_err.insert(d.clone(), format!("image {img}: {e:#}"));
                    }
                }
            }
        }

        for (path, spec) in &it.files {
            let Some(b) = blobs.get(&spec.sha256) else { continue };
            let plain = if spec.sealed {
                match &self.seal {
                    Some(k) => proto::seal::open(k, b),
                    None => Err(anyhow::anyhow!("sealed, and this host has no seal key")),
                }
            } else {
                Ok(b.clone())
            };
            if let Err(e) = plain.and_then(|p| write_file(Path::new(path), &p, spec)) {
                errors.push(format!("file {path}: {e:#}"));
            }
        }

        for (id, spec) in &it.jobs {
            match self.jobs.schedule(id, spec) {
                Ok(true) => eprintln!("job {id}: queued"),
                Ok(false) => {}
                Err(e) => errors.push(e),
            }
        }
        self.meta.lock().unwrap().job_ids = it.jobs.keys().cloned().collect();

        let mut reload = false;
        if !self.backend.is_fake() {
            match self.ensure_targets(&it.fsid) {
                Ok(c) => reload |= c,
                Err(e) => errors.push(format!("targets: {e:#}")),
            }
        }
        for d in &it.remove {
            let unit = intent::unit_name(&it.fsid, d);
            let qf = self.cfg.quadlet_dir.join(intent::quadlet_file(&it.fsid, d));
            if !qf.exists() {
                continue;
            }
            if let Ok(o) = self.backend.observe(&unit).await {
                if o.is_active() {
                    if let Err(e) = self.backend.stop(&unit).await.and_then(|r| check(r, "stop")) {
                        errors.push(format!("remove {d}: {e:#}"));
                        continue;
                    }
                }
            }
            if let Err(e) = fs::remove_file(&qf) {
                errors.push(format!("remove {d}: {e:#}"));
                continue;
            }
            // a daemon that exits non-zero on SIGTERM leaves a failed record
            let _ = self.backend.reset_failed(&unit).await;
            reload = true;
            let data = self.cfg.data_dir.join(&it.fsid).join(d);
            if data.is_dir() {
                let dest = self.cfg.data_dir.join(&it.fsid).join("removed").join(format!("{d}_{}", wall_ns() / 1_000_000_000));
                if let Err(e) = fs::create_dir_all(dest.parent().unwrap()).and_then(|_| fs::rename(&data, &dest)) {
                    errors.push(format!("archive {}: {e}", data.display()));
                }
            }
            self.applied.lock().unwrap().units.remove(d);
            self.observed.lock().unwrap().remove(d);
            self.unit_daemon.lock().unwrap().remove(&unit);
            eprintln!("removed {d}");
        }

        let mut file_changed = HashSet::new();
        for (d, u) in &it.units {
            let Some(text) = blobs.get(&u.quadlet) else {
                unit_err.entry(d.clone()).or_insert_with(|| "Quadlet blob unavailable".into());
                continue;
            };
            let qf = self.cfg.quadlet_dir.join(intent::quadlet_file(&it.fsid, d));
            let spec = FileSpec { sha256: u.quadlet.clone(), mode: "0644".into(), owner: None, sealed: false };
            match write_file(&qf, text, &spec) {
                Ok(true) => {
                    file_changed.insert(d.clone());
                    reload = true;
                }
                Ok(false) => {}
                Err(e) => {
                    unit_err.insert(d.clone(), format!("write {}: {e:#}", qf.display()));
                }
            }
        }
        if reload {
            if let Err(e) = self.backend.reload().await {
                errors.push(format!("daemon-reload: {e:#}"));
            }
        }

        let maintenance = it.mode == Mode::Maintenance;
        for (d, u) in &it.units {
            let unit = intent::unit_name(&it.fsid, d);
            self.unit_daemon.lock().unwrap().insert(unit.clone(), d.clone());
            let r = match unit_err.get(d) {
                Some(e) => Err(anyhow::anyhow!("{e}")),
                None => self.apply_unit(d, u, &unit, file_changed.contains(d), maintenance).await,
            };
            let err = match r {
                Ok(()) => {
                    self.backoff.lock().unwrap().remove(d);
                    self.applied
                        .lock()
                        .unwrap()
                        .units
                        .insert(d.clone(), AppliedUnit { quadlet: u.quadlet.clone(), restart_key: u.restart_key.clone() });
                    None
                }
                Err(e) => {
                    let e = format!("{e:#}");
                    self.note_failure(d, u, &e);
                    Some(e)
                }
            };
            self.refresh_unit(d, &unit, Some(err)).await;
        }

        // daemons that left the intent are strays now, not managed units
        self.observed.lock().unwrap().retain(|d, _| it.units.contains_key(d));
        self.unit_daemon.lock().unwrap().retain(|_, d| it.units.contains_key(d));
        let strays = self.strays(it);
        let (applied, new_generation) = {
            let mut a = self.applied.lock().unwrap();
            let new = a.generation != it.generation;
            a.generation = it.generation;
            a.fsid = it.fsid.clone();
            (a.clone(), new)
        };
        // a pass that changed nothing (the periodic drift check, usually)
        // neither rewrites the state nor reports: at scale, every host
        // reporting each minute is most of agentd's load and of the gossip
        let path = self.cfg.state_dir.join("applied.json");
        let bytes = serde_json::to_vec(&applied).unwrap();
        if fs::read(&path).map(|b| b != bytes).unwrap_or(true) {
            if let Err(e) = write_atomic(&path, &bytes, 0o600, None) {
                errors.push(format!("saving applied state: {e:#}"));
            }
        }
        // the agent's own binary last: a real upgrade ends the process. Its
        // error is reported like the others (a pass that changes nothing
        // must not count as a change)
        if let Some(a) = &it.agent {
            let r = if !self.backend.is_fake() {
                crate::upgrade::apply(self, a, &self.self_sha).await
            } else if self.cfg.sim_upgrade {
                crate::upgrade::sim_apply(self, a).await
            } else {
                Ok(())
            };
            if let Err(e) = r {
                eprintln!("agent upgrade: {e:#}");
                errors.push(format!("agent upgrade: {e:#}"));
            }
        }
        let changed = {
            let mut m = self.meta.lock().unwrap();
            let changed = new_generation || m.fsid != it.fsid || m.mode != it.mode || m.apply_errors != errors || m.strays != strays;
            m.fsid = it.fsid.clone();
            m.mode = it.mode;
            m.apply_errors = errors;
            m.strays = strays;
            if changed {
                m.state_version += 1;
            }
            changed
        };
        if changed {
            self.dirty.notify_one();
        }
        let failed = unit_err.len() + self.observed.lock().unwrap().values().filter(|u| u.apply_error.is_some()).count();
        eprintln!("applied generation {} in {:.0} ms ({} units, {failed} with errors)", it.generation, t0.elapsed().as_secs_f64() * 1000.0, it.units.len());
    }

    async fn apply_unit(&self, d: &str, u: &UnitSpec, unit: &str, file_changed: bool, maintenance: bool) -> Result<()> {
        for j in &u.after_jobs {
            match self.jobs.get(j).map(|s| s.state) {
                Some(s) if s == "succeeded" => {}
                Some(s) if s == "failed" => bail!("job {j} failed"),
                Some(s) => bail!("waiting for job {j} ({s})"),
                None => bail!("waiting for job {j} (not in the intent)"),
            }
        }
        let obs = self.backend.observe(unit).await?;
        if obs.load_state != "loaded" {
            bail!("unit is {} after daemon-reload (did Quadlet reject the file?)", obs.load_state);
        }
        if u.state == Desired::Stopped || maintenance {
            if obs.is_active() {
                check(self.backend.stop(unit).await?, "stop")?;
            }
            return Ok(());
        }
        let (prev_key, applied_same) = {
            let a = self.applied.lock().unwrap();
            let p = a.units.get(d);
            (p.map(|a| a.restart_key.clone()), p.is_some_and(|a| a.quadlet == u.quadlet && a.restart_key == u.restart_key))
        };
        let needs_restart = file_changed || prev_key.is_some_and(|k| k != u.restart_key);
        // Starts and restarts the agent asks for reset the unit's failed
        // state and start-rate counter first (as cephadm does with
        // `systemctl reset-failed`), so only systemd's own automatic
        // restarts count toward StartLimitBurst.
        match obs.active_state.as_str() {
            "active" | "reloading" | "activating" => {
                if needs_restart {
                    self.backend.reset_failed(unit).await?;
                    check(self.backend.restart(unit).await?, "restart")?;
                }
            }
            // The agent started this exact spec and systemd has since given
            // up on it (crash loop past StartLimitBurst). Like cephadm, leave
            // it failed and reported until the spec changes, e.g. a new
            // restart key from `ceph orch daemon restart`.
            "failed" if applied_same && !file_changed => {}
            state => {
                if let Some(b) = self.backoff.lock().unwrap().get(d) {
                    if b.spec == spec_key(u) && Instant::now() < b.next {
                        bail!("{} (attempt {}, retrying later)", b.error, b.attempts);
                    }
                }
                if state == "failed" || state == "inactive" {
                    self.backend.reset_failed(unit).await?;
                }
                check(self.backend.start(unit).await?, "start")?;
            }
        }
        Ok(())
    }

    fn note_failure(&self, d: &str, u: &UnitSpec, error: &str) {
        let mut b = self.backoff.lock().unwrap();
        let e = b.entry(d.to_string()).or_insert(Backoff { spec: spec_key(u), attempts: 0, next: Instant::now(), error: String::new() });
        if e.spec != spec_key(u) {
            e.spec = spec_key(u);
            e.attempts = 0;
        }
        if !error.contains("retrying later") && !error.starts_with("waiting for job") {
            e.attempts += 1;
            e.error = error.to_string();
            e.next = Instant::now() + Duration::from_secs((10u64 << e.attempts.min(8)).min(1800));
        }
    }

    fn strays(&self, it: &Intent) -> Vec<String> {
        let prefix = format!("ceph-{}-", it.fsid);
        let mut v: Vec<String> = fs::read_dir(&self.cfg.quadlet_dir)
            .map(|rd| {
                rd.flatten()
                    .filter_map(|e| {
                        let n = e.file_name().to_string_lossy().to_string();
                        let d = n.strip_prefix(&prefix)?.strip_suffix(".container")?.to_string();
                        (!it.units.contains_key(&d) && !it.remove.contains(&d)).then_some(d)
                    })
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    /// Reads the unit from systemd and records it. `apply_error`:
    /// Some(x) replaces the unit's apply error, None keeps it.
    pub async fn refresh_unit(&self, d: &str, unit: &str, apply_error: Option<Option<String>>) {
        let obs = match self.backend.observe(unit).await {
            Ok(o) => o,
            Err(e) => UnitObs { load_state: format!("error: {e}"), ..Default::default() },
        };
        if !obs.control_group.is_empty() {
            self.cgroups.lock().unwrap().insert(d.to_string(), obs.control_group.clone());
        }
        let mut observed = self.observed.lock().unwrap();
        let old = observed.get(d).cloned();
        let mut st = UnitStatus {
            unit: unit.to_string(),
            load_state: obs.load_state,
            active_state: obs.active_state,
            sub_state: obs.sub_state,
            result: obs.result,
            n_restarts: obs.n_restarts,
            main_pid: obs.main_pid,
            exec_main_status: obs.exec_main_status,
            active_since_us: obs.active_since_us,
            observed_ns: old.as_ref().map(|o| o.observed_ns).unwrap_or(0),
            apply_error: match apply_error {
                Some(e) => e,
                None => old.as_ref().and_then(|o| o.apply_error.clone()),
            },
            memory_bytes: old.as_ref().and_then(|o| o.memory_bytes),
            cpu_usec: old.as_ref().and_then(|o| o.cpu_usec),
        };
        let changed = old.as_ref().map(|o| o.material() != st.material()).unwrap_or(true);
        if changed {
            st.observed_ns = wall_ns();
        }
        observed.insert(d.to_string(), st);
        drop(observed);
        if changed {
            self.meta.lock().unwrap().state_version += 1;
            self.dirty.notify_one();
        }
    }

    pub fn sample_usage(&self) {
        let cgroups = self.cgroups.lock().unwrap().clone();
        let mut observed = self.observed.lock().unwrap();
        for (d, cg) in cgroups {
            if let Some(u) = observed.get_mut(&d) {
                let (m, c) = crate::systemd::cgroup_usage(&cg);
                u.memory_bytes = m;
                u.cpu_usec = c;
            }
        }
    }

    pub fn host_status(&self) -> HostStatus {
        let m = self.meta.lock().unwrap();
        HostStatus {
            host: self.cfg.host.clone(),
            fsid: m.fsid.clone(),
            agent_version: self.cfg.sim_binary.as_ref().map(|a| a.version.clone()).unwrap_or_else(|| env!("CARGO_PKG_VERSION").into()),
            agent_sha256: self.self_sha.clone(),
            applied_generation: self.applied.lock().unwrap().generation,
            state_version: m.state_version,
            agent_boot: self.boot,
            mode: m.mode,
            units: self.observed.lock().unwrap().clone(),
            strays: m.strays.clone(),
            errors: m.intent_errors.iter().chain(m.apply_errors.iter()).cloned().collect(),
            jobs: self.jobs.statuses(&m.job_ids),
            sent_ns: wall_ns(),
        }
    }
}

pub type Shared = Arc<Ctx>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::systemd::Fake;
    use std::path::PathBuf;

    struct T {
        ctx: Arc<Ctx>,
        root: PathBuf,
        job_rx: Option<tokio::sync::mpsc::UnboundedReceiver<(String, proto::intent::JobSpec)>>,
    }

    fn setup(name: &str) -> T {
        let root = std::env::temp_dir().join(format!("ceph-agent-test-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let cfg = Config {
            host: "node1".into(),
            agentd: "127.0.0.1:1".into(),
            server_name: None,
            ca: root.join("ca.pem"),
            cert: None,
            key: None,
            keyring: None,
            seal_key: None,
            swim: None,
            trusted_keys: vec![],
            state_dir: root.join("state"),
            quadlet_dir: root.join("quadlet"),
            unit_dir: root.join("units"),
            data_dir: root.join("data"),
            drift_secs: 60,
            report_secs: None,
            fake_systemd: true,
            pull_images: false,
            source_addr: None,
            sim_binary: None,
            sim_upgrade: false,
        };
        fs::create_dir_all(&cfg.quadlet_dir).unwrap();
        let (fake, _rx) = Fake::new(cfg.quadlet_dir.clone());
        std::mem::forget(_rx);
        let (ctx, job_rx) = Ctx::new(cfg, Backend::Fake(fake), None).unwrap();
        T { ctx: Arc::new(ctx), root, job_rx: Some(job_rx) }
    }

    impl T {
        fn blob(&self, data: &str) -> String {
            let sha = sha256_hex(data.as_bytes());
            fs::write(self.ctx.cfg.state_dir.join("blobs").join(&sha), data).unwrap();
            sha
        }
        fn calls(&self) -> Vec<String> {
            let Backend::Fake(f) = &self.ctx.backend else { unreachable!() };
            std::mem::take(&mut *f.calls.lock().unwrap())
        }
        fn unit(&self, d: &str) -> UnitStatus {
            self.ctx.observed.lock().unwrap()[d].clone()
        }
    }

    fn unit(q: &str, key: &str) -> UnitSpec {
        UnitSpec { quadlet: q.into(), state: Desired::Running, restart_key: key.into(), after_jobs: vec![] }
    }

    #[tokio::test]
    async fn converge_restart_remove() {
        let t = setup("converge");
        let web = t.blob("[Container]\nImage=busybox\nExec=httpd -f\n");
        let bad = t.blob("[Container]\nImage=busybox\nExec=false\n");
        let conf1 = t.blob("one\n");
        let conf_path = t.root.join("data/f/web/httpd.conf").to_string_lossy().to_string();
        let mut it = Intent {
            host: "node1".into(),
            fsid: "f".into(),
            generation: 1,
            mode: Mode::Normal,
            prefetch: vec![],
            files: BTreeMap::from([(conf_path.clone(), FileSpec { sha256: conf1, mode: "0600".into(), owner: None, sealed: false })]),
            units: BTreeMap::from([("web".into(), unit(&web, "1")), ("bad".into(), unit(&bad, "1"))]),
            remove: vec![],
            jobs: BTreeMap::new(),
            agent: None,
            pull_spread_s: 0,
        };
        t.ctx.reconcile(&it).await;
        assert_eq!(fs::read_to_string(&conf_path).unwrap(), "one\n");
        assert_eq!(fs::metadata(&conf_path).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(t.unit("web").active_state, "active");
        assert_eq!(t.unit("web").apply_error, None);
        assert_eq!(t.unit("bad").active_state, "failed");
        assert!(t.unit("bad").apply_error.as_deref().unwrap().contains("start job failed"));
        assert_eq!(t.calls(), vec!["reload", "start ceph-f-bad.service", "start ceph-f-web.service"]);

        // a file change alone rewrites the file and restarts nothing; the
        // failed unit is in backoff, so it isn't retried yet
        it.generation = 2;
        it.files.get_mut(&conf_path).unwrap().sha256 = t.blob("two\n");
        t.ctx.reconcile(&it).await;
        assert_eq!(fs::read_to_string(&conf_path).unwrap(), "two\n");
        assert_eq!(t.calls(), Vec::<String>::new());
        assert!(t.unit("bad").apply_error.as_deref().unwrap().contains("retrying later"));

        // a new restart key restarts
        it.generation = 3;
        it.units.get_mut("web").unwrap().restart_key = "2".into();
        t.ctx.reconcile(&it).await;
        assert_eq!(t.calls(), vec!["restart ceph-f-web.service"]);

        // a unit that leaves `units` without being in `remove` is a stray
        it.generation = 4;
        it.units.remove("bad");
        t.ctx.reconcile(&it).await;
        assert_eq!(t.ctx.host_status().strays, vec!["bad"]);
        assert_eq!(t.calls(), Vec::<String>::new());

        // removal stops the unit, deletes its Quadlet file, archives data
        fs::create_dir_all(t.root.join("data/f/web")).unwrap();
        it.generation = 5;
        it.units.remove("web");
        it.remove = vec!["web".into(), "bad".into()];
        t.ctx.reconcile(&it).await;
        assert_eq!(t.calls(), vec!["stop ceph-f-web.service", "reload"]);
        assert!(!t.ctx.cfg.quadlet_dir.join("ceph-f-web.container").exists());
        assert!(!t.root.join("data/f/web").exists());
        assert_eq!(fs::read_dir(t.root.join("data/f/removed")).unwrap().count(), 1);
        let st = t.ctx.host_status();
        assert_eq!(st.applied_generation, 5);
        assert!(st.units.is_empty() && st.strays.is_empty(), "{st:?}");
        let _ = fs::remove_dir_all(&t.root);
    }

    #[tokio::test]
    async fn failed_by_systemd_waits_for_a_new_restart_key() {
        let t = setup("gaveup");
        let web = t.blob("[Container]\nImage=busybox\n");
        let mut it = Intent {
            host: "node1".into(),
            fsid: "f".into(),
            generation: 1,
            mode: Mode::Normal,
            prefetch: vec![],
            files: BTreeMap::new(),
            units: BTreeMap::from([("web".into(), unit(&web, "1"))]),
            remove: vec![],
            jobs: BTreeMap::new(),
            agent: None,
            pull_spread_s: 0,
        };
        t.ctx.reconcile(&it).await;
        assert_eq!(t.calls(), vec!["reload", "start ceph-f-web.service"]);
        let Backend::Fake(f) = &t.ctx.backend else { unreachable!() };
        f.give_up("ceph-f-web.service");
        // drift pass: the agent doesn't override systemd's start limit
        t.ctx.reconcile(&it).await;
        assert_eq!(t.calls(), Vec::<String>::new());
        assert_eq!(t.unit("web").active_state, "failed");
        assert_eq!(t.unit("web").apply_error, None);
        // `ceph orch daemon restart`: a new restart key starts it again
        it.generation = 2;
        it.units.get_mut("web").unwrap().restart_key = "2".into();
        t.ctx.reconcile(&it).await;
        assert_eq!(t.calls(), vec!["start ceph-f-web.service"]);
        assert_eq!(t.unit("web").active_state, "active");
        let _ = fs::remove_dir_all(&t.root);
    }

    #[tokio::test]
    async fn units_wait_for_their_jobs() {
        let mut t = setup("jobs");
        tokio::spawn(crate::jobs::runner(t.ctx.clone(), t.job_rx.take().unwrap()));
        let web = t.blob("[Container]\nImage=busybox\n");
        let job = |args: &[&str]| proto::intent::JobSpec {
            image: "busybox".into(),
            entrypoint: None,
            args: args.iter().map(|s| s.to_string()).collect(),
            privileged: false,
            volumes: vec![],
            env: vec![],
            dirs: vec![],
            podman_args: vec![],
            timeout_s: 10,
        };
        let mut it = Intent {
            host: "node1".into(),
            fsid: "f".into(),
            generation: 1,
            mode: Mode::Normal,
            prefetch: vec![],
            files: BTreeMap::new(),
            units: BTreeMap::from([
                ("web".into(), UnitSpec { after_jobs: vec!["mkfs-web".into()], ..unit(&web, "1") }),
                ("other".into(), UnitSpec { after_jobs: vec!["broken".into()], ..unit(&web, "1") }),
            ]),
            remove: vec![],
            jobs: BTreeMap::from([("mkfs-web".into(), job(&["mkfs"])), ("broken".into(), job(&["fail"]))]),
            agent: None,
            pull_spread_s: 0,
        };
        t.ctx.reconcile(&it).await;
        // the fake jobs finish at once; the runner kicks another pass
        tokio::time::sleep(Duration::from_millis(50)).await;
        t.ctx.reconcile(&it).await;
        let st = t.ctx.host_status();
        assert_eq!(st.jobs["mkfs-web"].state, "succeeded");
        assert_eq!(st.jobs["broken"].state, "failed");
        assert_eq!(st.units["web"].active_state, "active");
        assert_eq!(st.units["other"].apply_error.as_deref(), Some("job broken failed"));
        // a job id runs once, even when the intent keeps naming it
        it.generation = 2;
        t.ctx.reconcile(&it).await;
        assert_eq!(fs::read_dir(t.ctx.cfg.state_dir.join("jobs")).unwrap().count(), 2);
        let _ = fs::remove_dir_all(&t.root);
    }

    #[tokio::test]
    async fn crash_is_a_material_change() {
        let t = setup("crash");
        let web = t.blob("[Container]\nImage=busybox\n");
        let it = Intent {
            host: "node1".into(),
            fsid: "f".into(),
            generation: 1,
            mode: Mode::Normal,
            prefetch: vec![],
            files: BTreeMap::new(),
            units: BTreeMap::from([("web".into(), unit(&web, "1"))]),
            remove: vec![],
            jobs: BTreeMap::new(),
            agent: None,
            pull_spread_s: 0,
        };
        t.ctx.reconcile(&it).await;
        let v0 = t.ctx.host_status().state_version;
        t.ctx.refresh_unit("web", "ceph-f-web.service", None).await;
        assert_eq!(t.ctx.host_status().state_version, v0, "no change, no new version");
        let Backend::Fake(f) = &t.ctx.backend else { unreachable!() };
        f.crash_and_restart("ceph-f-web.service");
        t.ctx.refresh_unit("web", "ceph-f-web.service", None).await;
        let st = t.ctx.host_status();
        assert_eq!(st.state_version, v0 + 1);
        assert_eq!(st.units["web"].n_restarts, 1);
        let _ = fs::remove_dir_all(&t.root);
    }

}
