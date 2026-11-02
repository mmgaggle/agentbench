//! Jobs: imperative work named in the intent (ceph-volume, a mon's mkfs,
//! zapping a disk). Each job id runs at most once, in a container; its
//! record under state_dir/jobs keeps it from running again, also across
//! agent restarts. A job that was running when the agent died is reported
//! as failed rather than rerun: retrying is the mgr's decision (a new id).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use proto::intent::{wall_ns, JobSpec, JobStatus};
use tokio::sync::mpsc;

use crate::reconcile::{write_atomic, Shared};

const STDOUT_KEEP: usize = 64 << 10;
const STDERR_KEEP: usize = 8 << 10;

pub struct Jobs {
    dir: PathBuf,
    status: Mutex<BTreeMap<String, JobStatus>>,
    queue: mpsc::UnboundedSender<(String, JobSpec)>,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
}

fn tail(b: &[u8], keep: usize) -> String {
    let s = &b[b.len().saturating_sub(keep)..];
    String::from_utf8_lossy(s).into_owned()
}

impl Jobs {
    pub fn new(state_dir: &std::path::Path) -> (Jobs, mpsc::UnboundedReceiver<(String, JobSpec)>) {
        let dir = state_dir.join("jobs");
        let _ = std::fs::create_dir_all(&dir);
        let mut status = BTreeMap::new();
        for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let Some(id) = e.file_name().to_str().and_then(|n| n.strip_suffix(".json")).map(|s| s.to_string()) else { continue };
            let Ok(mut st) = std::fs::read(e.path()).map_err(anyhow::Error::from).and_then(|b| Ok(serde_json::from_slice::<JobStatus>(&b)?)) else {
                continue;
            };
            if st.state == "running" || st.state == "pending" {
                st.state = "failed".into();
                st.error = Some("the agent restarted while the job was running".into());
                st.finished_ns = wall_ns();
                let _ = write_atomic(&e.path(), &serde_json::to_vec(&st).unwrap(), 0o600, None);
            }
            status.insert(id, st);
        }
        let (tx, rx) = mpsc::unbounded_channel();
        (Jobs { dir, status: Mutex::new(status), queue: tx }, rx)
    }

    fn record(&self, id: &str, st: JobStatus) {
        let _ = write_atomic(&self.dir.join(format!("{id}.json")), &serde_json::to_vec(&st).unwrap(), 0o600, None);
        self.status.lock().unwrap().insert(id.to_string(), st);
    }

    pub fn get(&self, id: &str) -> Option<JobStatus> {
        self.status.lock().unwrap().get(id).cloned()
    }

    /// Queues the job unless it has a record already. Returns whether it
    /// was queued.
    pub fn schedule(&self, id: &str, spec: &JobSpec) -> Result<bool, String> {
        if !valid_id(id) {
            return Err(format!("job id {id:?} must be 1-128 of [A-Za-z0-9._-]"));
        }
        if self.status.lock().unwrap().contains_key(id) {
            return Ok(false);
        }
        self.record(id, JobStatus { state: "pending".into(), ..Default::default() });
        let _ = self.queue.send((id.to_string(), spec.clone()));
        Ok(true)
    }

    pub fn statuses(&self, ids: &[String]) -> BTreeMap<String, JobStatus> {
        let s = self.status.lock().unwrap();
        ids.iter().filter_map(|i| s.get(i).map(|st| (i.clone(), st.clone()))).collect()
    }
}

async fn run_one(fake: bool, id: &str, spec: &JobSpec) -> JobStatus {
    let mut st = JobStatus { state: "running".into(), started_ns: wall_ns(), ..Default::default() };
    if fake {
        let fail = spec.args.iter().any(|a| a == "fail");
        st.state = if fail { "failed" } else { "succeeded" }.into();
        st.exit_code = Some(if fail { 1 } else { 0 });
        st.stdout = spec.args.join(" ");
        st.finished_ns = wall_ns();
        return st;
    }
    for d in &spec.dirs {
        if let Err(e) = std::fs::create_dir_all(d) {
            st.state = "failed".into();
            st.error = Some(format!("creating {d}: {e}"));
            st.finished_ns = wall_ns();
            return st;
        }
    }
    let name = format!("ceph-job-{id}");
    let _ = tokio::process::Command::new("podman").args(["rm", "-f", &name]).output().await;
    let mut cmd = tokio::process::Command::new("podman");
    cmd.args(["run", "--rm", "--init", "--net=host", "--name", &name]);
    if spec.privileged {
        cmd.arg("--privileged");
    }
    for v in &spec.volumes {
        cmd.args(["-v", v]);
    }
    for e in &spec.env {
        cmd.args(["-e", e]);
    }
    cmd.args(&spec.podman_args);
    if let Some(ep) = &spec.entrypoint {
        cmd.args(["--entrypoint", ep]);
    }
    cmd.arg(&spec.image).args(&spec.args).kill_on_drop(true);
    match tokio::time::timeout(Duration::from_secs(spec.timeout_s), cmd.output()).await {
        Ok(Ok(out)) => {
            st.exit_code = out.status.code();
            st.state = if out.status.success() { "succeeded" } else { "failed" }.into();
            st.stdout = tail(&out.stdout, STDOUT_KEEP);
            st.stderr = tail(&out.stderr, STDERR_KEEP);
        }
        Ok(Err(e)) => {
            st.state = "failed".into();
            st.error = Some(format!("podman: {e}"));
        }
        Err(_) => {
            let _ = tokio::process::Command::new("podman").args(["rm", "-f", &name]).output().await;
            st.state = "failed".into();
            st.error = Some(format!("timed out after {}s", spec.timeout_s));
        }
    }
    st.finished_ns = wall_ns();
    st
}

/// Runs queued jobs one at a time and wakes the reconciler when one ends,
/// so units waiting on it start right away.
pub async fn runner(ctx: Shared, mut rx: mpsc::UnboundedReceiver<(String, JobSpec)>) {
    while let Some((id, spec)) = rx.recv().await {
        let started = JobStatus { state: "running".into(), started_ns: wall_ns(), ..Default::default() };
        ctx.jobs.record(&id, started);
        ctx.changed();
        eprintln!("job {id}: running {}", spec.image);
        let st = run_one(ctx.backend.is_fake(), &id, &spec).await;
        eprintln!(
            "job {id}: {} (exit {:?}) in {:.1}s",
            st.state,
            st.exit_code,
            (st.finished_ns.saturating_sub(st.started_ns)) as f64 / 1e9
        );
        ctx.jobs.record(&id, st);
        ctx.changed();
        ctx.kick.notify_one();
    }
}
