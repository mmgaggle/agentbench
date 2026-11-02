//! Per-host desired state ("intent") that the mgr signs and the host agent
//! applies, and the status the agent reports back. See
//! docs/agent-executes-intent.md.
//!
//! Files and Quadlet units are referenced by the SHA-256 of their content;
//! the bytes travel separately as blobs. The signature covers the exact
//! intent JSON bytes carried in the envelope, so agentd can store and relay
//! an intent without being able to change it.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use ed25519_dalek::{Signer as _, SigningKey, Verifier as _, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

pub fn sha256_hex(data: &[u8]) -> String {
    let d = sha2::Sha256::digest(data);
    let mut s = String::with_capacity(64);
    for b in d {
        let _ = write!(s, "{b:02x}");
    }
    s
}

pub fn b64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

pub fn unb64(s: &str) -> Result<Vec<u8>> {
    Ok(base64::engine::general_purpose::STANDARD.decode(s.trim())?)
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    #[default]
    Normal,
    Maintenance,
}

fn default_file_mode() -> String {
    "0644".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileSpec {
    pub sha256: String,
    /// Octal permission bits, e.g. "0600".
    #[serde(default = "default_file_mode")]
    pub mode: String,
    /// "uid:gid"; unchanged when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// The blob is sealed to the host's key (proto::seal); `sha256` is the
    /// sealed blob's hash, and the agent writes the plaintext.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sealed: bool,
}

impl FileSpec {
    pub fn mode_bits(&self) -> Result<u32> {
        u32::from_str_radix(self.mode.trim_start_matches("0o"), 8).with_context(|| format!("file mode {:?}", self.mode))
    }

    pub fn owner_ids(&self) -> Result<Option<(u32, u32)>> {
        let Some(o) = &self.owner else { return Ok(None) };
        let (u, g) = o.split_once(':').with_context(|| format!("owner {o:?} is not uid:gid"))?;
        Ok(Some((u.parse()?, g.parse()?)))
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Desired {
    #[default]
    Running,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnitSpec {
    /// SHA-256 of the Quadlet `.container` text.
    pub quadlet: String,
    #[serde(default)]
    pub state: Desired,
    /// Changes whenever the daemon must restart: cephadm's redeploy vs
    /// reconfig decision. File changes alone don't restart a unit.
    #[serde(default)]
    pub restart_key: String,
    /// Jobs that must have succeeded before the unit starts (e.g. an OSD's
    /// ceph-volume prepare, a mon's mkfs).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after_jobs: Vec<String>,
}

fn default_job_timeout() -> u64 {
    600
}

/// Imperative work: one container run, at most once per job id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobSpec {
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub privileged: bool,
    /// host:container[:options]
    #[serde(default)]
    pub volumes: Vec<String>,
    /// NAME=value
    #[serde(default)]
    pub env: Vec<String>,
    /// More `podman run` arguments, e.g. --ipc=host (LVM's udev semaphores)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub podman_args: Vec<String>,
    /// Host directories to create first (bind-mount sources must exist)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dirs: Vec<String>,
    #[serde(default = "default_job_timeout")]
    pub timeout_s: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobStatus {
    /// pending, running, succeeded, failed
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// last 64 KiB of stdout, last 8 KiB of stderr
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub error: Option<String>,
    pub started_ns: u64,
    pub finished_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Intent {
    pub host: String,
    pub fsid: String,
    pub generation: u64,
    #[serde(default)]
    pub mode: Mode,
    /// Images to have on the host before any unit needs them.
    #[serde(default)]
    pub prefetch: Vec<String>,
    /// Spread pulls of missing images over this many seconds: each host
    /// waits a fixed offset derived from its name, so a new image doesn't
    /// send every host to the registry at once.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pull_spread_s: u32,
    #[serde(default)]
    pub files: BTreeMap<String, FileSpec>,
    /// Keyed by daemon name (e.g. "osd.17").
    #[serde(default)]
    pub units: BTreeMap<String, UnitSpec>,
    /// Daemons to stop and remove. A unit on the host that is neither in
    /// `units` nor here is a stray and is left alone.
    #[serde(default)]
    pub remove: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub jobs: BTreeMap<String, JobSpec>,
    /// The agent binary this host should run
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentSpec {
    pub version: String,
    /// blob of the binary
    pub sha256: String,
}

fn is_zero(v: &u32) -> bool {
    *v == 0
}

/// Seconds host `host` waits before pulling, within a `spread`-second window.
pub fn pull_offset(host: &str, spread: u32) -> u32 {
    if spread == 0 {
        return 0;
    }
    let h = sha256_hex(host.as_bytes());
    (u32::from_str_radix(&h[..8], 16).unwrap() as u64 * spread as u64 / u32::MAX as u64) as u32
}

pub fn unit_base(fsid: &str, daemon: &str) -> String {
    format!("ceph-{fsid}-{daemon}")
}

/// systemd service Quadlet generates for a daemon's `.container` file.
pub fn unit_name(fsid: &str, daemon: &str) -> String {
    format!("{}.service", unit_base(fsid, daemon))
}

pub fn quadlet_file(fsid: &str, daemon: &str) -> String {
    format!("{}.container", unit_base(fsid, daemon))
}

impl Intent {
    /// Every blob the intent references.
    pub fn blobs(&self) -> Vec<String> {
        let mut v: Vec<String> = self.files.values().map(|f| f.sha256.clone()).collect();
        v.extend(self.units.values().map(|u| u.quadlet.clone()));
        // the agent binary is fetched only when it's time to upgrade
        v.sort();
        v.dedup();
        v
    }
}

/// What agentd stores and relays. `generation` and `host` repeat the signed
/// payload's values for agentd's bookkeeping; the agent trusts only the
/// payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub host: String,
    pub generation: u64,
    pub key_id: String,
    /// base64 of the intent JSON bytes
    pub payload: String,
    /// base64 Ed25519 signature over those bytes
    pub sig: String,
}

pub fn key_id(vk: &VerifyingKey) -> String {
    sha256_hex(vk.as_bytes())[..16].to_string()
}

pub fn sign(key: &SigningKey, intent: &Intent) -> Envelope {
    let payload = serde_json::to_vec(intent).expect("intent serializes");
    let sig = key.sign(&payload);
    Envelope {
        host: intent.host.clone(),
        generation: intent.generation,
        key_id: key_id(&key.verifying_key()),
        payload: b64(&payload),
        sig: b64(&sig.to_bytes()),
    }
}

/// Verifies an envelope against the trusted keys and returns the intent.
pub fn open(env: &Envelope, trusted: &[VerifyingKey]) -> Result<Intent> {
    let vk = trusted
        .iter()
        .find(|k| key_id(k) == env.key_id)
        .with_context(|| format!("intent signed by untrusted key {}", env.key_id))?;
    let payload = unb64(&env.payload)?;
    let sig: [u8; 64] = unb64(&env.sig)?.try_into().map_err(|_| anyhow::anyhow!("signature is not 64 bytes"))?;
    vk.verify(&payload, &ed25519_dalek::Signature::from_bytes(&sig)).context("bad intent signature")?;
    let intent: Intent = serde_json::from_slice(&payload)?;
    if intent.host != env.host || intent.generation != env.generation {
        bail!("envelope says {}@{}, payload says {}@{}", env.host, env.generation, intent.host, intent.generation);
    }
    Ok(intent)
}

pub fn generate_key() -> Result<SigningKey> {
    use std::io::Read as _;
    let mut seed = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut seed)?;
    Ok(SigningKey::from_bytes(&seed))
}

pub fn load_signing_key(path: &std::path::Path) -> Result<SigningKey> {
    let b: [u8; 32] = unb64(&std::fs::read_to_string(path).with_context(|| format!("{}", path.display()))?)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("{}: not a 32-byte key", path.display()))?;
    Ok(SigningKey::from_bytes(&b))
}

pub fn load_verifying_key(path: &std::path::Path) -> Result<VerifyingKey> {
    let b: [u8; 32] = unb64(&std::fs::read_to_string(path).with_context(|| format!("{}", path.display()))?)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("{}: not a 32-byte key", path.display()))?;
    Ok(VerifyingKey::from_bytes(&b)?)
}

/// One daemon's unit as systemd sees it.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnitStatus {
    pub unit: String,
    pub load_state: String,
    pub active_state: String,
    pub sub_state: String,
    /// Service `Result`: success, exit-code, signal, start-limit-hit, ...
    pub result: String,
    pub n_restarts: u32,
    pub main_pid: u32,
    pub exec_main_status: i32,
    /// Wall-clock µs when the unit last entered `active`.
    pub active_since_us: u64,
    /// Wall-clock ns when the agent observed the latest change.
    pub observed_ns: u64,
    /// Last failure applying this unit's intent, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_usec: Option<u64>,
}

impl UnitStatus {
    /// The parts that make up a change worth reporting (not usage).
    pub fn material(&self) -> impl PartialEq + '_ {
        (
            &self.load_state,
            &self.active_state,
            &self.sub_state,
            &self.result,
            self.n_restarts,
            self.main_pid,
            &self.apply_error,
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostStatus {
    pub host: String,
    pub fsid: String,
    pub agent_version: String,
    /// SHA-256 of the running agent binary
    #[serde(default)]
    pub agent_sha256: String,
    /// Generation of the last intent the agent finished applying (errors
    /// included: they are reported per unit and retried).
    pub applied_generation: u64,
    /// Bumps on every material change; restarts with the agent process.
    pub state_version: u64,
    /// When the agent process started (wall ms), to tell state_version
    /// sequences apart.
    #[serde(default)]
    pub agent_boot: u64,
    pub mode: Mode,
    pub units: BTreeMap<String, UnitStatus>,
    /// Ceph units on the host that the intent doesn't mention.
    pub strays: Vec<String>,
    /// Problems not tied to one unit (bad signature, blob fetch, ...).
    pub errors: Vec<String>,
    /// Jobs in the current intent
    #[serde(default)]
    pub jobs: BTreeMap<String, JobStatus>,
    pub sent_ns: u64,
}

pub fn wall_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_open() {
        let k = generate_key().unwrap();
        let mut i = Intent {
            host: "node1".into(),
            fsid: "f".into(),
            generation: 3,
            mode: Mode::Normal,
            prefetch: vec![],
            files: BTreeMap::new(),
            units: BTreeMap::new(),
            remove: vec![],
            jobs: BTreeMap::new(),
            agent: None,
            pull_spread_s: 0,
        };
        i.units.insert("web".into(), UnitSpec { quadlet: sha256_hex(b"x"), state: Desired::Running, restart_key: "1".into(), after_jobs: vec![] });
        let env = sign(&k, &i);
        assert_eq!(open(&env, &[k.verifying_key()]).unwrap(), i);

        let other = generate_key().unwrap();
        assert!(open(&env, &[other.verifying_key()]).is_err());

        let mut tampered = env.clone();
        i.generation = 4;
        tampered.payload = b64(&serde_json::to_vec(&i).unwrap());
        assert!(open(&tampered, &[k.verifying_key()]).is_err());
    }
}
