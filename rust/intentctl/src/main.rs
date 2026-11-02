//! intentctl: plays the mgr's part for the ceph-agent prototype. It renders
//! a per-host intent from a spec, uploads the blobs, signs the intent and
//! stores it in agentd, and reads back what the agents report.
//!
//!   intentctl keygen --out intent.key           (writes intent.key.pub too)
//!   intentctl token --state mgr.json --host node4 --agentd 10.77.0.1:7160 --server-name agentd.lan --ca ca.pem
//!   intentctl approve --admin ... --state mgr.json --ca-cert ca.pem --ca-key ca.key --intent-key intent.key [--expect N]
//!   intentctl push --admin 10.77.0.1:7161 --key intent.key --host node1 --spec web.json [--set k=v]
//!   intentctl push ... --host node1 --host node2 ... --canary 1 --rate 0.5 --max-in-flight 2   (paced rollout)
//!   intentctl wait --admin ... --host node1 [--generation G]
//!   intentctl status --admin ... [--host node1]
//!   intentctl events --admin ... [--since N] [--follow]
//!
//! A spec names files and Quadlet units by local path; `{fsid}`, `{host}`,
//! `{daemon}` (in a unit's Quadlet text) and `--set` variables are
//! substituted in paths, file contents and Quadlet texts:
//!
//!   {"fsid": "...",
//!    "files": {"/var/lib/ceph/{fsid}/web.{host}/www/index.html": {"src": "index.html", "mode": "0644"}},
//!    "units": {"web.{host}": {"quadlet": "web.container", "restart_on": [], "restart_nonce": "1"}},
//!    "remove": []}

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use proto::intent::{self, sha256_hex, Desired, Envelope, FileSpec, Intent, Mode, UnitSpec};
use proto::join::{self, JoinDecision, JoinRequest};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Parser)]
enum Cmd {
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Issue one-time join tokens (the mgr's `orch host add`): one line per
    /// host, in order
    Token {
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        host: Vec<String>,
        /// More hosts, one per line
        #[arg(long)]
        hosts_file: Option<PathBuf>,
        #[arg(long)]
        agentd: String,
        #[arg(long)]
        server_name: String,
        /// Cluster CA certificate; the token pins its hash
        #[arg(long)]
        ca: PathBuf,
        #[arg(long, default_value_t = 3600)]
        ttl: u64,
    },
    /// Answer pending join requests: check token and MAC, sign the host's
    /// certificate, record its sealing key
    Approve {
        #[arg(long)]
        admin: String,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        ca_cert: PathBuf,
        #[arg(long)]
        ca_key: PathBuf,
        #[arg(long)]
        intent_key: PathBuf,
        /// agentd's SWIM member, given to joining hosts as their seed
        #[arg(long)]
        swim_seed: Option<String>,
        #[arg(long, default_value_t = 7946)]
        swim_port: u16,
        /// Cluster size to size the hosts' gossip for (default: the agent's)
        #[arg(long)]
        swim_cluster_size: Option<u32>,
        /// Lifeguard on the hosts' SWIM members (agentd needs it too)
        #[arg(long)]
        swim_lifeguard: bool,
        /// One summary line per batch instead of a line per host
        #[arg(long)]
        quiet: bool,
        /// Keep answering until this many hosts were approved
        #[arg(long, default_value_t = 0)]
        expect: usize,
        #[arg(long, default_value_t = 300)]
        timeout: u64,
    },
    Push {
        #[arg(long)]
        admin: String,
        #[arg(long)]
        key: PathBuf,
        /// Repeat for several hosts; each gets its own rendering
        #[arg(long, required = true)]
        host: Vec<String>,
        /// `{host}` in the path: a spec per host
        #[arg(long)]
        spec: PathBuf,
        /// mgr state (hosts' sealing keys), for files marked secret
        #[arg(long)]
        state: Option<PathBuf>,
        /// Extra template variables, k=v
        #[arg(long)]
        set: Vec<String>,
        /// Cluster fsid (overrides the spec's)
        #[arg(long)]
        fsid: Option<String>,
        /// Wait until every host has applied the new generation
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 300)]
        timeout: u64,
        #[command(flatten)]
        pace: Pace,
    },
    /// Push the joined hosts and their SWIM indexes to agentd
    Members {
        #[arg(long)]
        admin: String,
        #[arg(long)]
        state: PathBuf,
    },
    Wait {
        #[arg(long)]
        admin: String,
        #[arg(long, required = true)]
        host: Vec<String>,
        /// Default: the host's stored intent generation
        #[arg(long)]
        generation: Option<u64>,
        #[arg(long, default_value_t = 300)]
        timeout: u64,
    },
    Status {
        #[arg(long)]
        admin: String,
        #[arg(long)]
        host: Option<String>,
    },
    Events {
        #[arg(long)]
        admin: String,
        #[arg(long, default_value_t = 0)]
        since: u64,
        #[arg(long)]
        follow: bool,
    },
}

fn default_mode() -> String {
    "0644".into()
}

#[derive(Deserialize)]
struct FileSrc {
    #[serde(default)]
    src: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default = "default_mode")]
    mode: String,
    #[serde(default)]
    owner: Option<String>,
    /// seal to the host's key (keyrings, credentials)
    #[serde(default)]
    secret: bool,
    /// binary file: no template substitution
    #[serde(default)]
    raw: bool,
}

#[derive(Deserialize)]
struct UnitSrc {
    quadlet: String,
    #[serde(default)]
    state: Desired,
    /// files (intent paths) whose change restarts the unit
    #[serde(default)]
    restart_on: Vec<String>,
    /// bump to force a restart
    #[serde(default)]
    restart_nonce: String,
    #[serde(default)]
    after_jobs: Vec<String>,
}

#[derive(Deserialize)]
struct Spec {
    #[serde(default)]
    fsid: String,
    #[serde(default)]
    mode: Mode,
    #[serde(default)]
    prefetch: Vec<String>,
    #[serde(default)]
    pull_spread_s: u32,
    #[serde(default)]
    files: BTreeMap<String, FileSrc>,
    #[serde(default)]
    units: BTreeMap<String, UnitSrc>,
    #[serde(default)]
    remove: Vec<String>,
    /// job id -> JobSpec (strings get the usual substitutions)
    #[serde(default)]
    jobs: BTreeMap<String, Value>,
    /// {"src": path to the agent binary, "version": "..."}
    #[serde(default)]
    agent: Option<AgentSrc>,
}

/// A paced rollout (any of these set): the canaries first, all at once;
/// once they're done and `soak` has passed, the other hosts at `rate` per
/// second with at most `max_in_flight` unfinished. A host is done when it
/// has applied its intent without errors and, if the intent names an agent
/// binary, an agent process running that binary has reported. More than
/// `max_failures` failed hosts (any, among the canaries) halt the rollout.
#[derive(clap::Args, Clone)]
struct Pace {
    #[arg(long, default_value_t = 0)]
    canary: usize,
    /// hosts/s after the canaries (0: all at once)
    #[arg(long, default_value_t = 0.0)]
    rate: f64,
    #[arg(long, default_value_t = 0)]
    max_in_flight: usize,
    #[arg(long, default_value_t = 0)]
    max_failures: usize,
    #[arg(long, default_value_t = 10)]
    soak: u64,
    /// a host that isn't done after this long has failed
    #[arg(long, default_value_t = 300)]
    host_timeout: u64,
}

impl Pace {
    fn active(&self) -> bool {
        self.canary > 0 || self.rate > 0.0 || self.max_in_flight > 0
    }
}

/// Where a host stands in a paced rollout.
enum Progress {
    Pending,
    Done,
    Failed(String),
}

fn progress(admin: &str, host: &str, generation: u64, agent_sha: Option<&str>) -> Progress {
    let Ok(v) = http_json(admin, "GET", &format!("/v1/hosts/{host}/status"), b"") else { return Progress::Pending };
    let st = &v["status"];
    // errors count only once this generation is applied: until then they
    // may be the previous intent's (a rolled-back agent applied the new
    // generation before it tried the binary, so its error comes after)
    if st["applied_generation"].as_u64().unwrap_or(0) < generation {
        return Progress::Pending;
    }
    let errors: Vec<&str> = st["errors"].as_array().map(|a| a.iter().filter_map(|e| e.as_str()).collect()).unwrap_or_default();
    if let Some(e) = errors.iter().find(|e| e.contains("agent upgrade") || e.contains("rolled back")).or(errors.first()) {
        return Progress::Failed(e.to_string());
    }
    let unit_errors: Vec<String> = st["units"]
        .as_object()
        .map(|m| {
            m.iter()
                .filter(|(_, u)| u["apply_error"].is_string() || u["active_state"] == "failed")
                .map(|(d, u)| format!("{d}: {}", u["apply_error"].as_str().unwrap_or("failed")))
                .collect()
        })
        .unwrap_or_default();
    if !unit_errors.is_empty() {
        return Progress::Failed(unit_errors.join("; "));
    }
    match agent_sha {
        Some(sha) if st["agent_sha256"].as_str() != Some(sha) => Progress::Pending,
        _ => Progress::Done,
    }
}

#[derive(Deserialize)]
struct AgentSrc {
    src: String,
    version: String,
}

/// Minimal HTTP/1.1 client for agentd's plain-HTTP admin port.
fn http(addr: &str, method: &str, path: &str, body: &[u8]) -> Result<(u16, Vec<u8>)> {
    let mut s = std::net::TcpStream::connect(addr).with_context(|| format!("connecting to {addr}"))?;
    s.set_read_timeout(Some(Duration::from_secs(60)))?;
    write!(s, "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len())?;
    s.write_all(body)?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf)?;
    let split = buf.windows(4).position(|w| w == b"\r\n\r\n").context("bad HTTP response")?;
    let head = String::from_utf8_lossy(&buf[..split]);
    let code: u16 = head.split_whitespace().nth(1).context("no status")?.parse()?;
    Ok((code, buf[split + 4..].to_vec()))
}

fn http_json(addr: &str, method: &str, path: &str, body: &[u8]) -> Result<Value> {
    let (code, b) = http(addr, method, path, body)?;
    if code != 200 {
        bail!("{method} {path}: {code} {}", String::from_utf8_lossy(&b));
    }
    Ok(serde_json::from_slice(&b)?)
}

fn subst(s: &str, vars: &BTreeMap<String, String>) -> String {
    let mut out = s.to_string();
    for (k, v) in vars {
        out = out.replace(&format!("{{{k}}}"), v);
    }
    out
}

fn put_blob(admin: &str, data: &[u8]) -> Result<String> {
    let v = http_json(admin, "PUT", "/v1/blobs", data)?;
    let sha = v["sha256"].as_str().context("no sha256")?.to_string();
    if sha != sha256_hex(data) {
        bail!("agentd stored blob under {sha}");
    }
    Ok(sha)
}

fn current_generation(admin: &str, host: &str) -> Result<u64> {
    let (code, b) = http(admin, "GET", &format!("/v1/hosts/{host}/intent"), b"")?;
    match code {
        200 => Ok(serde_json::from_slice::<Envelope>(&b)?.generation),
        404 => Ok(0),
        c => bail!("GET intent: {c} {}", String::from_utf8_lossy(&b)),
    }
}

#[derive(Default, Serialize, Deserialize)]
struct TokenRec {
    secret: String,
    host: String,
    expires_unix: u64,
    used: bool,
}

#[derive(Default, Serialize, Deserialize)]
struct HostRec {
    seal_pub: String,
    swim_idx: u32,
    addr: Option<String>,
    cert_sha256: String,
    joined_unix: u64,
    facts: Value,
}

/// What the mgr keeps about hosts and tokens (cephadm's inventory, here a
/// file). Sealing keys come from here, never from agentd.
#[derive(Default, Serialize, Deserialize)]
struct MgrState {
    tokens: BTreeMap<String, TokenRec>,
    hosts: BTreeMap<String, HostRec>,
    next_idx: u32,
    /// Intent generation per host: owned by the mgr, so it stays monotonic
    /// when agentd restarts and forgets what it relayed.
    #[serde(default)]
    generations: BTreeMap<String, u64>,
}

impl MgrState {
    fn load(p: &Path) -> Result<MgrState> {
        match std::fs::read(p) {
            Ok(b) => Ok(serde_json::from_slice(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(MgrState::default()),
            Err(e) => Err(e.into()),
        }
    }
    fn save(&self, p: &Path) -> Result<()> {
        let tmp = p.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(tmp, p)?;
        Ok(())
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

fn sign_csr(csr_pem: &str, host: &str, ca_cert: &str, ca_key: &str) -> Result<String> {
    let key = rcgen::KeyPair::from_pem(ca_key)?;
    let ca = rcgen::CertificateParams::from_ca_cert_pem(ca_cert)?.self_signed(&key)?;
    let mut csr = rcgen::CertificateSigningRequestParams::from_pem(csr_pem)?;
    // The names come from the token, not from what the host asked for.
    csr.params.distinguished_name = rcgen::DistinguishedName::new();
    csr.params.distinguished_name.push(rcgen::DnType::CommonName, host);
    csr.params.subject_alt_names = vec![rcgen::SanType::DnsName(host.to_string().try_into()?)];
    csr.params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    csr.params.is_ca = rcgen::IsCa::ExplicitNoCa;
    Ok(csr.signed_by(&ca, &key)?.pem())
}

/// Decides one pending join. Returns the decision and, when approved, the
/// host record to keep.
fn decide(st: &mut MgrState, req: &JoinRequest, a: &ApproveArgs) -> Result<(JoinDecision, Option<String>)> {
    let reject = |reason: String, secret: Option<&str>| -> Result<(JoinDecision, Option<String>)> {
        let mut d = JoinDecision { approved: false, reason, host: req.host.clone(), cert_pem: String::new(), intent_keys: vec![], swim: None, mac: String::new() };
        if let Some(s) = secret {
            d.mac = join::decision_mac(s, &req.token_id, &d)?;
        }
        Ok((d, None))
    };
    let Some(t) = st.tokens.get(&req.token_id) else {
        return reject(format!("unknown token {}", req.token_id), None);
    };
    let secret = t.secret.clone();
    if !join::mac_eq(&req.mac, &join::request_mac(&secret, req)?) {
        return reject("request MAC does not verify".into(), None);
    }
    if t.used {
        return reject("token already used".into(), Some(&secret));
    }
    if unix_now() > t.expires_unix {
        return reject("token expired".into(), Some(&secret));
    }
    if t.host != req.host {
        return reject(format!("token is for {}", t.host), Some(&secret));
    }
    let cert_pem = sign_csr(&req.csr_pem, &req.host, &a.ca_cert, &a.ca_key)?;
    let idx = match st.hosts.get(&req.host) {
        Some(h) => h.swim_idx,
        None => {
            st.next_idx += 1;
            st.next_idx - 1
        }
    };
    let addr = req.facts["addr"].as_str().map(|s| s.to_string());
    let swim = match (&a.swim_seed, &addr) {
        (Some(seed), Some(ip)) => {
            Some(join::SwimConfig {
                idx,
                bind: format!("{ip}:{}", a.swim_port),
                seeds: vec![seed.clone()],
                cluster_size: a.swim_cluster_size,
                lifeguard: a.swim_lifeguard,
            })
        }
        _ => None,
    };
    let mut d = JoinDecision {
        approved: true,
        reason: String::new(),
        host: req.host.clone(),
        cert_pem: cert_pem.clone(),
        intent_keys: vec![a.intent_pub.clone()],
        swim,
        mac: String::new(),
    };
    d.mac = join::decision_mac(&secret, &req.token_id, &d)?;
    let der = proto::tls_pem_der(&cert_pem)?;
    st.tokens.get_mut(&req.token_id).unwrap().used = true;
    st.hosts.insert(
        req.host.clone(),
        HostRec { seal_pub: req.seal_pub.clone(), swim_idx: idx, addr, cert_sha256: sha256_hex(&der), joined_unix: unix_now(), facts: req.facts.clone() },
    );
    Ok((d, Some(req.host.clone())))
}

struct ApproveArgs {
    ca_cert: String,
    ca_key: String,
    intent_pub: String,
    swim_seed: Option<String>,
    swim_port: u16,
    swim_cluster_size: Option<u32>,
    swim_lifeguard: bool,
}

fn render(admin: &str, spec: &Spec, dir: &Path, host: &str, generation: u64, extra: &BTreeMap<String, String>, state: Option<&MgrState>) -> Result<Intent> {
    let mut vars = extra.clone();
    vars.insert("fsid".into(), spec.fsid.clone());
    vars.insert("host".into(), host.into());
    let mut files = BTreeMap::new();
    for (path, f) in &spec.files {
        let bytes = match (&f.src, &f.content) {
            (Some(src), _) => std::fs::read(dir.join(src)).with_context(|| src.to_string())?,
            (None, Some(c)) => c.clone().into_bytes(),
            (None, None) => bail!("file {path}: needs src or content"),
        };
        let plain = if f.raw { bytes } else { subst(&String::from_utf8(bytes).with_context(|| format!("{path}: not UTF-8 (raw?)"))?, &vars).into_bytes() };
        let blob = if f.secret {
            let h = state.and_then(|s| s.hosts.get(host)).with_context(|| format!("file {path} is secret, and {host} has no sealing key (joined?)"))?;
            proto::seal::seal(&proto::seal::parse_public(&h.seal_pub)?, &plain)?
        } else {
            plain
        };
        let sha = put_blob(admin, &blob)?;
        files.insert(subst(path, &vars), FileSpec { sha256: sha, mode: f.mode.clone(), owner: f.owner.clone(), sealed: f.secret });
    }
    let mut units = BTreeMap::new();
    for (name, u) in &spec.units {
        let daemon = subst(name, &vars);
        let mut uvars = vars.clone();
        uvars.insert("daemon".into(), daemon.clone());
        let text = std::fs::read_to_string(dir.join(&u.quadlet)).with_context(|| u.quadlet.clone())?;
        let quadlet = put_blob(admin, subst(&text, &uvars).as_bytes())?;
        let mut key = format!("{quadlet}\n");
        for p in &u.restart_on {
            let p = subst(p, &uvars);
            let f = files.get(&p).with_context(|| format!("unit {daemon}: restart_on {p} is not in files"))?;
            key.push_str(&f.sha256);
            key.push('\n');
        }
        key.push_str(&subst(&u.restart_nonce, &uvars));
        units.insert(
            daemon,
            UnitSpec {
                quadlet,
                state: u.state,
                restart_key: sha256_hex(key.as_bytes())[..16].to_string(),
                after_jobs: u.after_jobs.iter().map(|j| subst(j, &uvars)).collect(),
            },
        );
    }
    Ok(Intent {
        host: host.into(),
        fsid: spec.fsid.clone(),
        generation,
        mode: spec.mode,
        prefetch: spec.prefetch.iter().map(|p| subst(p, &vars)).collect(),
        pull_spread_s: spec.pull_spread_s,
        files,
        units,
        remove: spec.remove.iter().map(|r| subst(r, &vars)).collect(),
        jobs: spec
            .jobs
            .iter()
            .map(|(id, j)| Ok((subst(id, &vars), serde_json::from_str(&subst(&j.to_string(), &vars))?)))
            .collect::<Result<_>>()?,
        agent: match &spec.agent {
            Some(a) => {
                let bin = std::fs::read(dir.join(&a.src)).with_context(|| a.src.clone())?;
                Some(proto::intent::AgentSpec { version: a.version.clone(), sha256: put_blob(admin, &bin)? })
            }
            None => None,
        },
    })
}

/// Publishes `hosts` paced as `pace` says (see Pace); fails if halted.
fn rollout(admin: &str, hosts: &[String], pace: &Pace, publish: &mut dyn FnMut(&str) -> Result<(u64, Option<String>)>) -> Result<()> {
    let t0 = Instant::now();
    let mut queue: std::collections::VecDeque<&String> = hosts.iter().collect();
    let mut canaries = pace.canary.min(queue.len());
    let mut in_canary = canaries > 0;
    let mut soak_until: Option<Instant> = None;
    let mut in_flight: BTreeMap<String, (u64, Option<String>, Instant)> = BTreeMap::new();
    let (mut done, mut failed) = (0usize, Vec::<(String, String)>::new());
    let (mut tokens, mut last) = (1.0f64, Instant::now());
    let mut halted = false;
    loop {
        if !halted {
            if in_canary {
                if canaries > 0 {
                    while canaries > 0 {
                        let h = queue.pop_front().unwrap();
                        let (g, a) = publish(h)?;
                        in_flight.insert(h.clone(), (g, a, Instant::now()));
                        canaries -= 1;
                    }
                    println!("rollout: {} canary host(s) published", in_flight.len());
                } else if in_flight.is_empty() {
                    let until = *soak_until.get_or_insert_with(|| {
                        println!("rollout: canaries done; soaking {}s", pace.soak);
                        Instant::now() + Duration::from_secs(pace.soak)
                    });
                    if Instant::now() >= until {
                        in_canary = false;
                        last = Instant::now();
                    }
                }
            } else {
                let budget = if pace.rate > 0.0 {
                    tokens = (tokens + last.elapsed().as_secs_f64() * pace.rate).min(pace.rate.max(1.0));
                    tokens as usize
                } else {
                    usize::MAX
                };
                last = Instant::now();
                let limit = if pace.max_in_flight > 0 { pace.max_in_flight } else { usize::MAX };
                let mut n = 0;
                while n < budget && in_flight.len() < limit {
                    let Some(h) = queue.pop_front() else { break };
                    let (g, a) = publish(h)?;
                    in_flight.insert(h.clone(), (g, a, Instant::now()));
                    n += 1;
                }
                if pace.rate > 0.0 {
                    tokens -= n as f64;
                }
            }
        }
        let mut settled = Vec::new();
        for (h, (g, a, since)) in &in_flight {
            match progress(admin, h, *g, a.as_deref()) {
                Progress::Done => {
                    println!("{h}: done after {:.1}s", since.elapsed().as_secs_f64());
                    done += 1;
                    settled.push(h.clone());
                }
                Progress::Failed(why) => {
                    println!("{h}: FAILED after {:.1}s: {why}", since.elapsed().as_secs_f64());
                    failed.push((h.clone(), why));
                    settled.push(h.clone());
                }
                Progress::Pending if since.elapsed() > Duration::from_secs(pace.host_timeout) => {
                    println!("{h}: FAILED: not done after {}s", pace.host_timeout);
                    failed.push((h.clone(), "timed out".into()));
                    settled.push(h.clone());
                }
                Progress::Pending => {}
            }
        }
        for h in settled {
            in_flight.remove(&h);
        }
        if !halted && !failed.is_empty() && (in_canary || failed.len() > pace.max_failures) {
            halted = true;
            println!("rollout: halting after {} failed host(s); {} host(s) not started", failed.len(), queue.len());
        }
        if in_flight.is_empty() && (halted || queue.is_empty()) {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    println!("rollout: {done} done, {} failed, {} not started, in {:.1}s", failed.len(), queue.len(), t0.elapsed().as_secs_f64());
    if halted {
        bail!("rollout halted: {}", failed.iter().map(|(h, w)| format!("{h}: {w}")).collect::<Vec<_>>().join("; "));
    }
    Ok(())
}

fn wait_applied(admin: &str, targets: &[(String, u64)], timeout: Duration) -> Result<()> {
    let t0 = Instant::now();
    let mut pending: Vec<(String, u64)> = targets.to_vec();
    while !pending.is_empty() {
        if t0.elapsed() > timeout {
            bail!("timed out waiting for {pending:?}");
        }
        let mut still = Vec::new();
        for (h, g) in pending {
            let v = http_json(admin, "GET", &format!("/v1/hosts/{h}/status"), b"").unwrap_or(Value::Null);
            let applied = v["status"]["applied_generation"].as_u64().unwrap_or(0);
            if applied >= g {
                let units: Vec<String> = v["status"]["units"]
                    .as_object()
                    .map(|m| {
                        m.iter()
                            .map(|(d, u)| {
                                let err = u["apply_error"].as_str().map(|e| format!(" [{e}]")).unwrap_or_default();
                                format!("{d}={}/{}{err}", u["active_state"].as_str().unwrap_or("?"), u["sub_state"].as_str().unwrap_or("?"))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                println!("{h}: generation {applied} applied after {:.2}s: {}", t0.elapsed().as_secs_f64(), units.join(" "));
            } else {
                still.push((h, g));
            }
        }
        pending = still;
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn main() -> Result<()> {
    match Cmd::parse() {
        Cmd::Keygen { out } => {
            let k = intent::generate_key()?;
            std::fs::write(&out, intent::b64(&k.to_bytes()))?;
            let mut pubpath = out.clone().into_os_string();
            pubpath.push(".pub");
            std::fs::write(&pubpath, intent::b64(k.verifying_key().as_bytes()))?;
            println!("key {} -> {} (+ .pub)", intent::key_id(&k.verifying_key()), out.display());
        }
        Cmd::Token { state, mut host, hosts_file, agentd, server_name, ca, ttl } => {
            if let Some(f) = hosts_file {
                host.extend(std::fs::read_to_string(&f)?.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from));
            }
            let mut st = MgrState::load(&state)?;
            let der = proto::tls_pem_der(&std::fs::read_to_string(&ca)?)?;
            let mut out = String::new();
            for h in host {
                let t = join::Token {
                    v: 1,
                    agentd: agentd.clone(),
                    server_name: server_name.clone(),
                    ca_sha256: sha256_hex(&der),
                    id: join::random_id()?,
                    secret: join::random_b64(32)?,
                    host: h.clone(),
                };
                st.tokens.insert(t.id.clone(), TokenRec { secret: t.secret.clone(), host: h, expires_unix: unix_now() + ttl, used: false });
                out.push_str(&t.encode());
                out.push('\n');
            }
            st.save(&state)?;
            print!("{out}");
        }
        Cmd::Approve { admin, state, ca_cert, ca_key, intent_key, swim_seed, swim_port, swim_cluster_size, swim_lifeguard, quiet, expect, timeout } => {
            let a = ApproveArgs {
                ca_cert: std::fs::read_to_string(&ca_cert)?,
                ca_key: std::fs::read_to_string(&ca_key)?,
                intent_pub: intent::b64(intent::load_signing_key(&intent_key)?.verifying_key().as_bytes()),
                swim_seed,
                swim_port,
                swim_cluster_size,
                swim_lifeguard,
            };
            let t0 = Instant::now();
            let mut approved = 0;
            let mut st = MgrState::load(&state)?;
            loop {
                // Decide the whole batch, record it, then answer: a host
                // never holds a certificate the mgr's state doesn't know.
                let pending = http_json(&admin, "GET", "/v1/joins", b"")?;
                let mut decided = Vec::new();
                for p in pending.as_array().into_iter().flatten() {
                    let id = p["join_id"].as_str().context("join_id")?.to_string();
                    let req: JoinRequest = serde_json::from_value(p["request"].clone())?;
                    let (d, ok) = decide(&mut st, &req, &a)?;
                    decided.push((id, req.host, d, ok));
                }
                if !decided.is_empty() {
                    st.save(&state)?;
                }
                let n = decided.len();
                for (id, host, d, ok) in decided {
                    http_json(&admin, "POST", &format!("/v1/joins/{id}"), &serde_json::to_vec(&d)?)?;
                    match ok {
                        Some(h) => {
                            approved += 1;
                            if !quiet {
                                println!("approved {h} (join {id}, swim idx {})", st.hosts[&h].swim_idx);
                            }
                        }
                        None => println!("rejected {host} (join {id}): {}", d.reason),
                    }
                }
                if quiet && n > 0 {
                    println!("{:.1}s: answered {n}, {approved} approved in all", t0.elapsed().as_secs_f64());
                }
                if approved >= expect {
                    break;
                }
                if t0.elapsed() > Duration::from_secs(timeout) {
                    bail!("approved {approved} of {expect} before the timeout");
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        Cmd::Push { admin, key, host, spec, state, set, fsid, wait, timeout, pace } => {
            let mut mgr = match &state {
                Some(p) => Some(MgrState::load(p)?),
                None => None,
            };
            let sk = intent::load_signing_key(&key)?;
            // a spec per host when the path names {host}
            let load_spec = |h: &str| -> Result<(Spec, PathBuf)> {
                let p = PathBuf::from(spec.to_string_lossy().replace("{host}", h));
                let mut s: Spec = serde_json::from_slice(&std::fs::read(&p).with_context(|| format!("{}", p.display()))?)
                    .with_context(|| format!("{}", p.display()))?;
                if let Some(f) = &fsid {
                    s.fsid = f.clone();
                }
                Ok((s, p.parent().unwrap_or(Path::new(".")).to_path_buf()))
            };
            let mut extra = BTreeMap::new();
            for kv in &set {
                let (k, v) = kv.split_once('=').with_context(|| format!("--set {kv}: expected k=v"))?;
                extra.insert(k.to_string(), v.to_string());
            }
            let mut publish = |h: &str| -> Result<(u64, Option<String>)> {
                let known = mgr.as_ref().and_then(|m| m.generations.get(h)).copied().unwrap_or(0);
                let g = current_generation(&admin, h)?.max(known) + 1;
                let (s, dir) = load_spec(h)?;
                let it = render(&admin, &s, &dir, h, g, &extra, mgr.as_ref())?;
                let env = intent::sign(&sk, &it);
                http_json(&admin, "PUT", &format!("/v1/hosts/{h}/intent"), &serde_json::to_vec(&env)?)?;
                println!("{h}: generation {g} stored ({} units, {} files, {} removals)", it.units.len(), it.files.len(), it.remove.len());
                if let (Some(m), Some(p)) = (mgr.as_mut(), &state) {
                    m.generations.insert(h.to_string(), g);
                    m.save(p)?;
                }
                Ok((g, it.agent.map(|a| a.sha256)))
            };
            if pace.active() {
                return rollout(&admin, &host, &pace, &mut publish);
            }
            let mut targets = Vec::new();
            for h in &host {
                let (g, _) = publish(h)?;
                targets.push((h.clone(), g));
            }
            if wait {
                wait_applied(&admin, &targets, Duration::from_secs(timeout))?;
            }
        }
        Cmd::Members { admin, state } => {
            let st = MgrState::load(&state)?;
            let m: BTreeMap<&String, u32> = st.hosts.iter().map(|(h, r)| (h, r.swim_idx)).collect();
            let v = http_json(&admin, "PUT", "/v1/members", &serde_json::to_vec(&m)?)?;
            println!("agentd has {} members", v["members"]);
        }
        Cmd::Wait { admin, host, generation, timeout } => {
            let mut targets = Vec::new();
            for h in host {
                let g = match generation {
                    Some(g) => g,
                    None => current_generation(&admin, &h)?,
                };
                targets.push((h, g));
            }
            wait_applied(&admin, &targets, Duration::from_secs(timeout))?;
        }
        Cmd::Status { admin, host } => {
            let v = match host {
                Some(h) => http_json(&admin, "GET", &format!("/v1/hosts/{h}/status"), b"")?,
                None => http_json(&admin, "GET", "/v1/intent-status", b"")?,
            };
            println!("{}", serde_json::to_string_pretty(&v)?);
        }
        Cmd::Events { admin, mut since, follow } => loop {
            let v = http_json(&admin, "GET", &format!("/v1/events?since={since}"), b"")?;
            for e in v["events"].as_array().into_iter().flatten() {
                println!("{e}");
            }
            since = v["seq"].as_u64().unwrap_or(since);
            if !follow {
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        },
    }
    Ok(())
}
