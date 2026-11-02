//! Self-upgrade. The intent names the agent binary (a blob, by hash). The
//! agent installs it under /usr/local/lib/ceph-agent/<hash>/, checks that it
//! runs (`--version`), points /usr/local/bin/ceph-agent at it and exits;
//! systemd (Restart=always) starts the new binary.
//!
//! Two ways back:
//! * a binary that can't start fails the unit three times in a row, and
//!   systemd runs ceph-agent-rollback.service (OnFailure=), which points the
//!   link back at the previous binary;
//! * a binary that starts but doesn't post a status to agentd within two
//!   minutes points the link back itself and exits.
//! Either way the version is remembered as bad and not tried again.
//!
//! The rollback hooks (a start limit and OnFailure=) exist only while an
//! upgrade is uncommitted: left in place, they turned an admin restarting
//! a healthy agent a few times into a silent downgrade.
//! The rollback unit also checks that an upgrade is pending.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use proto::intent::{sha256_hex, wall_ns, AgentSpec};
use serde::{Deserialize, Serialize};

use crate::reconcile::{write_atomic, Ctx, Shared};

pub const BIN: &str = "/usr/local/bin/ceph-agent";
const LIB: &str = "/usr/local/lib/ceph-agent";
const COMMIT_WITHIN: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pending {
    pub from: String,
    pub to: String,
    pub previous: PathBuf,
    pub at_ns: u64,
}

pub fn self_sha() -> String {
    // once per process: the simulator runs thousands of agents in one
    static SHA: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    SHA.get_or_init(|| fs::read("/proc/self/exe").map(|b| sha256_hex(&b)).unwrap_or_default()).clone()
}

fn pending_path(ctx: &Ctx) -> PathBuf {
    ctx.cfg.state_dir.join("agent-upgrade.json")
}

fn bad_path(ctx: &Ctx) -> PathBuf {
    ctx.cfg.state_dir.join("agent-bad-versions")
}

pub fn is_bad(ctx: &Ctx, sha: &str) -> bool {
    fs::read_to_string(bad_path(ctx)).map(|s| s.lines().any(|l| l == sha)).unwrap_or(false)
}

fn mark_bad(ctx: &Ctx, sha: &str) {
    let mut s = fs::read_to_string(bad_path(ctx)).unwrap_or_default();
    s.push_str(sha);
    s.push('\n');
    let _ = write_atomic(&bad_path(ctx), s.as_bytes(), 0o644, None);
}

fn point_bin_at(target: &Path) -> Result<()> {
    let tmp = format!("{BIN}.switch");
    let _ = fs::remove_file(&tmp);
    std::os::unix::fs::symlink(target, &tmp)?;
    fs::rename(&tmp, BIN)?;
    Ok(())
}

const ROLLBACK_UNIT: &str = "[Unit]
Description=Roll ceph-agent back to its previous binary

[Service]
Type=oneshot
ExecStart=/bin/sh -c '[ -f /var/lib/ceph-agent/agent-upgrade.json ] && p=$(cat /var/lib/ceph-agent/agent-previous) && [ -x \"$p\" ] && ln -sfn \"$p\" /usr/local/bin/ceph-agent.rollback && mv -T /usr/local/bin/ceph-agent.rollback /usr/local/bin/ceph-agent && systemctl reset-failed ceph-agent.service && systemctl start ceph-agent.service'
";

const DROPIN: &str = "[Unit]
OnFailure=ceph-agent-rollback.service
StartLimitIntervalSec=120
StartLimitBurst=3
";

/// Installs `spec`'s binary and switches to it; on success the process
/// exits and doesn't return.
pub async fn apply(ctx: &Ctx, spec: &AgentSpec, self_sha: &str) -> Result<()> {
    if spec.sha256 == self_sha {
        return Ok(());
    }
    if is_bad(ctx, &spec.sha256) {
        bail!("agent {} ({}) was rolled back before; not trying it again", spec.version, &spec.sha256[..12]);
    }
    let bin = ctx.blob(&spec.sha256).await?;
    let dir = Path::new(LIB).join(&spec.sha256[..16]);
    let new = dir.join("ceph-agent");
    write_atomic(&new, &bin, 0o755, None)?;
    let out = tokio::time::timeout(Duration::from_secs(10), tokio::process::Command::new(&new).arg("--version").output())
        .await
        .context("new agent: --version timed out")??;
    let v = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() || !v.starts_with("ceph-agent") {
        mark_bad(ctx, &spec.sha256);
        bail!("new agent {} doesn't run: {}", spec.version, String::from_utf8_lossy(&out.stderr).trim());
    }

    // keep the current binary as the way back
    let previous = match fs::read_link(BIN) {
        Ok(p) => p,
        Err(_) => {
            let keep = Path::new(LIB).join(&self_sha[..16]).join("ceph-agent");
            write_atomic(&keep, &fs::read(BIN)?, 0o755, None)?;
            keep
        }
    };
    write_atomic(&ctx.cfg.state_dir.join("agent-previous"), previous.to_string_lossy().as_bytes(), 0o644, None)?;
    write_atomic(&ctx.cfg.unit_dir.join("ceph-agent-rollback.service"), ROLLBACK_UNIT.as_bytes(), 0o644, None)?;
    write_atomic(&dropin_path(ctx), DROPIN.as_bytes(), 0o644, None)?;
    ctx.backend.reload().await?;
    let p = Pending { from: self_sha.to_string(), to: spec.sha256.clone(), previous, at_ns: wall_ns() };
    write_atomic(&pending_path(ctx), &serde_json::to_vec(&p)?, 0o644, None)?;
    point_bin_at(&new)?;
    eprintln!("upgrading to agent {} ({}); exiting so systemd starts it", spec.version, &spec.sha256[..12]);
    if let Some(c) = &ctx.client {
        let _ = tokio::time::timeout(Duration::from_secs(2), c.status(&ctx.host_status())).await;
    }
    std::process::exit(0);
}

/// The simulator's upgrade: fetch the binary and check its hash as `apply`
/// does, then end this agent "process" so the simulator starts it again as
/// the new version (new boot, new connections, new SWIM identity).
pub async fn sim_apply(ctx: &Ctx, spec: &AgentSpec) -> Result<()> {
    if spec.sha256 == ctx.self_sha || ctx.restart_to.lock().unwrap().is_some() {
        return Ok(());
    }
    let client = ctx.client.as_ref().context("no agentd")?;
    let got = client.blob_sha256(&spec.sha256).await?;
    if got != spec.sha256 {
        bail!("agent binary does not match its hash");
    }
    *ctx.restart_to.lock().unwrap() = Some(spec.clone());
    ctx.restart.notify_one();
    Ok(())
}

fn dropin_path(ctx: &Ctx) -> PathBuf {
    ctx.cfg.unit_dir.join("ceph-agent.service.d/10-rollback.conf")
}

/// Removes the rollback hooks once no upgrade is pending.
async fn disarm(ctx: &Shared) {
    if fs::remove_file(dropin_path(ctx)).is_ok() {
        if let Err(e) = ctx.backend.reload().await {
            eprintln!("removing the upgrade rollback hooks: {e:#}");
        }
    }
}

/// At startup: finish or undo an upgrade in progress. Returns a message
/// for the status report when an upgrade was rolled back.
pub fn on_start(ctx: &Shared, self_sha: &str) -> Option<String> {
    let Some(p) = fs::read(pending_path(ctx)).ok().and_then(|b| serde_json::from_slice::<Pending>(&b).ok()) else {
        // nothing pending: hooks left by an older agent must not roll
        // this one back when it's restarted a few times
        let c = ctx.clone();
        tokio::spawn(async move { disarm(&c).await });
        return None;
    };
    if self_sha == p.from {
        // systemd's rollback put us back
        mark_bad(ctx, &p.to);
        let _ = fs::remove_file(pending_path(ctx));
        let c = ctx.clone();
        tokio::spawn(async move { disarm(&c).await });
        return Some(format!("agent upgrade to {} failed to start and was rolled back", &p.to[..12]));
    }
    if self_sha != p.to {
        let _ = fs::remove_file(pending_path(ctx));
        let c = ctx.clone();
        tokio::spawn(async move { disarm(&c).await });
        return None;
    }
    // we are the new binary: commit once agentd has heard from us
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let ok = tokio::time::timeout(COMMIT_WITHIN, async {
            while !ctx.posted.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .is_ok();
        if ok {
            let _ = fs::remove_file(pending_path(&ctx));
            disarm(&ctx).await;
            eprintln!("upgrade to {} committed", &p.to[..12]);
        } else {
            eprintln!("no status reached agentd within {}s of the upgrade: rolling back", COMMIT_WITHIN.as_secs());
            mark_bad(&ctx, &p.to);
            let _ = fs::remove_file(pending_path(&ctx));
            if point_bin_at(&p.previous).is_ok() {
                std::process::exit(1);
            }
        }
    });
    None
}
