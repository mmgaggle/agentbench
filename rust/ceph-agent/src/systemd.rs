//! Unit control and unit state through systemd's D-Bus API, and an
//! in-memory fake with the same interface for tests and for simulating many
//! hosts on one machine.
//!
//! State changes arrive as `PropertiesChanged` signals on the units' object
//! paths; the backend forwards the unit name and the caller reads the
//! properties it cares about.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use zbus::zvariant::OwnedObjectPath;

#[zbus::proxy(
    interface = "org.freedesktop.systemd1.Manager",
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait Manager {
    fn subscribe(&self) -> zbus::Result<()>;
    fn reload(&self) -> zbus::Result<()>;
    fn start_unit(&self, name: &str, mode: &str) -> zbus::Result<OwnedObjectPath>;
    fn stop_unit(&self, name: &str, mode: &str) -> zbus::Result<OwnedObjectPath>;
    fn restart_unit(&self, name: &str, mode: &str) -> zbus::Result<OwnedObjectPath>;
    fn reset_failed_unit(&self, name: &str) -> zbus::Result<()>;
    fn load_unit(&self, name: &str) -> zbus::Result<OwnedObjectPath>;
    #[zbus(signal)]
    fn job_removed(&self, id: u32, job: zbus::zvariant::ObjectPath<'_>, unit: &str, result: &str) -> zbus::Result<()>;
}

#[zbus::proxy(interface = "org.freedesktop.systemd1.Unit", default_service = "org.freedesktop.systemd1")]
trait Unit {
    #[zbus(property)]
    fn load_state(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn active_state(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn sub_state(&self) -> zbus::Result<String>;
    #[zbus(property)]
    fn active_enter_timestamp(&self) -> zbus::Result<u64>;
}

#[zbus::proxy(interface = "org.freedesktop.systemd1.Service", default_service = "org.freedesktop.systemd1")]
trait Service {
    #[zbus(property)]
    fn result(&self) -> zbus::Result<String>;
    #[zbus(property, name = "NRestarts")]
    fn n_restarts(&self) -> zbus::Result<u32>;
    #[zbus(property, name = "MainPID")]
    fn main_pid(&self) -> zbus::Result<u32>;
    #[zbus(property)]
    fn exec_main_status(&self) -> zbus::Result<i32>;
    #[zbus(property)]
    fn control_group(&self) -> zbus::Result<String>;
}

/// What systemd says about one unit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnitObs {
    pub load_state: String,
    pub active_state: String,
    pub sub_state: String,
    pub result: String,
    pub n_restarts: u32,
    pub main_pid: u32,
    pub exec_main_status: i32,
    pub active_since_us: u64,
    pub control_group: String,
}

impl UnitObs {
    pub fn is_active(&self) -> bool {
        matches!(self.active_state.as_str(), "active" | "reloading" | "activating")
    }
}

/// JobRemoved can arrive before the caller starts waiting for the job, so
/// results are parked until claimed.
#[derive(Default)]
struct Jobs {
    inner: Mutex<(HashMap<String, oneshot::Sender<String>>, HashMap<String, (String, Instant)>)>,
}

impl Jobs {
    fn removed(&self, job: String, result: String) {
        let mut g = self.inner.lock().unwrap();
        if let Some(tx) = g.0.remove(&job) {
            let _ = tx.send(result);
        } else {
            g.1.retain(|_, (_, t)| t.elapsed() < Duration::from_secs(600));
            g.1.insert(job, (result, Instant::now()));
        }
    }

    async fn wait(&self, job: &str, timeout: Duration) -> Result<String> {
        let rx = {
            let mut g = self.inner.lock().unwrap();
            if let Some((r, _)) = g.1.remove(job) {
                return Ok(r);
            }
            let (tx, rx) = oneshot::channel();
            g.0.insert(job.to_string(), tx);
            rx
        };
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(r)) => Ok(r),
            _ => {
                self.inner.lock().unwrap().0.remove(job);
                bail!("job {job} did not finish within {}s", timeout.as_secs())
            }
        }
    }
}

pub struct Systemd {
    conn: zbus::Connection,
    mgr: ManagerProxy<'static>,
    jobs: Arc<Jobs>,
    /// unit object path -> unit name, for units we watch
    paths: Arc<Mutex<HashMap<String, String>>>,
}

impl Systemd {
    pub async fn connect() -> Result<(Systemd, mpsc::UnboundedReceiver<String>)> {
        let conn = zbus::Connection::system().await.context("connecting to the system bus")?;
        let mgr = ManagerProxy::new(&conn).await?;
        // without Subscribe(), systemd doesn't emit unit and job signals
        mgr.subscribe().await?;
        let jobs = Arc::new(Jobs::default());
        let mut removed = mgr.receive_job_removed().await?;
        let j = jobs.clone();
        tokio::spawn(async move {
            while let Some(sig) = removed.next().await {
                if let Ok(a) = sig.args() {
                    j.removed(a.job.to_string(), a.result.to_string());
                }
            }
        });

        let rule = zbus::MatchRule::builder()
            .msg_type(zbus::message::Type::Signal)
            .interface("org.freedesktop.DBus.Properties")?
            .member("PropertiesChanged")?
            .path_namespace("/org/freedesktop/systemd1/unit")?
            .build();
        let mut stream = zbus::MessageStream::for_match_rule(rule, &conn, Some(4096)).await?;
        let paths: Arc<Mutex<HashMap<String, String>>> = Default::default();
        let (tx, rx) = mpsc::unbounded_channel();
        let p = paths.clone();
        tokio::spawn(async move {
            while let Some(m) = stream.next().await {
                let Ok(m) = m else { continue };
                let hdr = m.header();
                let Some(path) = hdr.path() else { continue };
                let unit = p.lock().unwrap().get(path.as_str()).cloned();
                if let Some(u) = unit {
                    let _ = tx.send(u);
                }
            }
        });
        Ok((Systemd { conn, mgr, jobs, paths }, rx))
    }

    async fn job(&self, call: impl std::future::Future<Output = zbus::Result<OwnedObjectPath>>, timeout: Duration) -> Result<String> {
        let job = call.await?;
        self.jobs.wait(job.as_str(), timeout).await
    }

    async fn observe(&self, unit: &str) -> Result<UnitObs> {
        let path = self.mgr.load_unit(unit).await?;
        self.paths.lock().unwrap().insert(path.as_str().to_string(), unit.to_string());
        let u = UnitProxy::builder(&self.conn)
            .path(path.clone())?
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()
            .await?;
        let s = ServiceProxy::builder(&self.conn)
            .path(path)?
            .cache_properties(zbus::proxy::CacheProperties::No)
            .build()
            .await?;
        Ok(UnitObs {
            load_state: u.load_state().await?,
            active_state: u.active_state().await?,
            sub_state: u.sub_state().await?,
            active_since_us: u.active_enter_timestamp().await.unwrap_or(0),
            result: s.result().await.unwrap_or_default(),
            n_restarts: s.n_restarts().await.unwrap_or(0),
            main_pid: s.main_pid().await.unwrap_or(0),
            exec_main_status: s.exec_main_status().await.unwrap_or(0),
            control_group: s.control_group().await.unwrap_or_default(),
        })
    }
}

/// In-memory systemd: units exist when their Quadlet file does; a unit whose
/// Quadlet text contains `Exec=false` fails to start.
/// The fake systemd's units (fails, state). Shared so the daemons keep
/// running when the simulator restarts an agent, as they do on a host.
pub type FakeUnits = std::sync::Arc<Mutex<HashMap<String, (bool /*fails*/, UnitObs)>>>;

pub struct Fake {
    quadlet_dir: PathBuf,
    units: FakeUnits,
    loaded: Mutex<HashSet<String>>,
    tx: mpsc::UnboundedSender<String>,
    pub calls: Mutex<Vec<String>>,
}

impl Fake {
    #[cfg(test)]
    pub fn new(quadlet_dir: PathBuf) -> (Fake, mpsc::UnboundedReceiver<String>) {
        Self::with_units(quadlet_dir, Default::default())
    }

    pub fn with_units(quadlet_dir: PathBuf, units: FakeUnits) -> (Fake, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Fake { quadlet_dir, units, loaded: Default::default(), tx, calls: Default::default() }, rx)
    }

    fn reload(&self) -> Result<()> {
        let mut loaded = HashSet::new();
        let mut units = self.units.lock().unwrap();
        if let Ok(rd) = std::fs::read_dir(&self.quadlet_dir) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if let Some(base) = name.strip_suffix(".container") {
                    let unit = format!("{base}.service");
                    let fails = std::fs::read_to_string(e.path()).unwrap_or_default().contains("Exec=false");
                    units.entry(unit.clone()).or_insert((fails, UnitObs::default())).0 = fails;
                    loaded.insert(unit);
                }
            }
        }
        for (u, (_, o)) in units.iter_mut() {
            o.load_state = if loaded.contains(u) { "loaded" } else { "not-found" }.into();
        }
        *self.loaded.lock().unwrap() = loaded;
        self.calls.lock().unwrap().push("reload".into());
        Ok(())
    }

    fn set(&self, unit: &str, active: &str, sub: &str, result: &str, restart: bool) {
        let mut units = self.units.lock().unwrap();
        let (_, o) = units.entry(unit.to_string()).or_default();
        o.active_state = active.into();
        o.sub_state = sub.into();
        o.result = result.into();
        if restart {
            o.n_restarts += 1;
        }
        if active == "active" {
            // a (re)started daemon is a new process, as with systemd
            static PID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1000);
            o.active_since_us = proto::intent::wall_ns() / 1000;
            o.main_pid = PID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            o.main_pid = 0;
        }
        let _ = self.tx.send(unit.to_string());
    }

    fn start(&self, unit: &str, verb: &str) -> Result<String> {
        self.calls.lock().unwrap().push(format!("{verb} {unit}"));
        if !self.loaded.lock().unwrap().contains(unit) {
            bail!("Unit {unit} not found.");
        }
        let fails = self.units.lock().unwrap().get(unit).map(|u| u.0).unwrap_or(false);
        if fails {
            self.set(unit, "failed", "failed", "exit-code", false);
            Ok("failed".into())
        } else {
            self.set(unit, "active", "running", "success", false);
            Ok("done".into())
        }
    }

    fn stop(&self, unit: &str) -> Result<String> {
        self.calls.lock().unwrap().push(format!("stop {unit}"));
        self.set(unit, "inactive", "dead", "success", false);
        Ok("done".into())
    }

    /// Test hook: the daemon kept crashing and systemd gave up on it.
    #[cfg(test)]
    pub fn give_up(&self, unit: &str) {
        self.set(unit, "failed", "failed", "exit-code", true);
    }

    /// Test hook: the daemon crashed and systemd restarted it.
    #[cfg(test)]
    pub fn crash_and_restart(&self, unit: &str) {
        self.set(unit, "active", "running", "success", true);
    }

    fn observe(&self, unit: &str) -> UnitObs {
        let mut o = self.units.lock().unwrap().get(unit).map(|u| u.1.clone()).unwrap_or_default();
        if o.load_state.is_empty() {
            o.load_state = if self.loaded.lock().unwrap().contains(unit) { "loaded" } else { "not-found" }.into();
        }
        if o.active_state.is_empty() {
            o.active_state = "inactive".into();
            o.sub_state = "dead".into();
        }
        o
    }
}

pub enum Backend {
    Systemd(Systemd),
    Fake(Fake),
}

const JOB_TIMEOUT: Duration = Duration::from_secs(330);

impl Backend {
    /// `systemctl daemon-reload`: reruns generators, so Quadlet picks up
    /// changed `.container` files.
    pub async fn reload(&self) -> Result<()> {
        match self {
            Backend::Systemd(s) => Ok(s.mgr.reload().await?),
            Backend::Fake(f) => f.reload(),
        }
    }

    /// Returns the job result: done, failed, timeout, dependency, ...
    pub async fn start(&self, unit: &str) -> Result<String> {
        match self {
            Backend::Systemd(s) => s.job(s.mgr.start_unit(unit, "replace"), JOB_TIMEOUT).await,
            Backend::Fake(f) => f.start(unit, "start"),
        }
    }

    pub async fn restart(&self, unit: &str) -> Result<String> {
        match self {
            Backend::Systemd(s) => s.job(s.mgr.restart_unit(unit, "replace"), JOB_TIMEOUT).await,
            Backend::Fake(f) => f.start(unit, "restart"),
        }
    }

    pub async fn stop(&self, unit: &str) -> Result<String> {
        match self {
            Backend::Systemd(s) => s.job(s.mgr.stop_unit(unit, "replace"), JOB_TIMEOUT).await,
            Backend::Fake(f) => f.stop(unit),
        }
    }

    pub async fn reset_failed(&self, unit: &str) -> Result<()> {
        match self {
            Backend::Systemd(s) => Ok(s.mgr.reset_failed_unit(unit).await?),
            Backend::Fake(_) => Ok(()),
        }
    }

    /// Reads the unit's state and starts watching it for changes.
    pub async fn observe(&self, unit: &str) -> Result<UnitObs> {
        match self {
            Backend::Systemd(s) => s.observe(unit).await,
            Backend::Fake(f) => Ok(f.observe(unit)),
        }
    }

    pub fn is_fake(&self) -> bool {
        matches!(self, Backend::Fake(_))
    }
}

/// Memory and CPU of a unit from its cgroup (v2).
pub fn cgroup_usage(control_group: &str) -> (Option<u64>, Option<u64>) {
    if control_group.is_empty() {
        return (None, None);
    }
    let dir = PathBuf::from("/sys/fs/cgroup").join(control_group.trim_start_matches('/'));
    let mem = std::fs::read_to_string(dir.join("memory.current")).ok().and_then(|s| s.trim().parse().ok());
    let cpu = std::fs::read_to_string(dir.join("cpu.stat"))
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("usage_usec ").and_then(|v| v.trim().parse().ok())));
    (mem, cpu)
}
