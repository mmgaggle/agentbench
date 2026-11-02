//! `ceph-agent join <token>`: turns a mgr-issued token into a host identity
//! (proto::join). Writes, under /etc/ceph-agent:
//!   ca.pem            cluster CA, pinned by the token's hash
//!   host.key/.crt     TLS client identity (CN = host), signed by the mgr
//!   seal.key          X25519 key the mgr seals secrets to
//!   intent-N.pub      keys allowed to sign this host's intents
//!   agent.json        the agent's config

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use hyper::{Method, StatusCode};
use proto::intent::{b64, sha256_hex};
use proto::join::{decision_mac, mac_eq, request_mac, JoinDecision, JoinRequest, Token};
use serde_json::json;

use crate::client::one_shot;
use crate::reconcile::write_atomic;

/// The address this host uses to reach agentd (the source IP the kernel
/// picks), which is where its SWIM member will listen.
fn local_addr_towards(agentd: &str) -> Option<String> {
    let s = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect(agentd).ok()?;
    Some(s.local_addr().ok()?.ip().to_string())
}

fn facts(agentd: &str, addr: Option<&str>) -> serde_json::Value {
    if let Some(a) = addr {
        // simulated host: no podman to ask, and the address is given
        return json!({"hostname": "sim", "addr": a});
    }
    let os = fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|s| s.lines().find_map(|l| l.strip_prefix("PRETTY_NAME=").map(|v| v.trim_matches('"').to_string())));
    let podman = std::process::Command::new("podman")
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string());
    json!({
        "hostname": fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default().trim(),
        "addr": local_addr_towards(agentd),
        "os": os,
        "kernel": fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default().trim(),
        "podman": podman,
    })
}

fn write_private(path: &Path, data: &[u8]) -> Result<()> {
    write_atomic(path, data, 0o600, None)
}

pub async fn join(token: &str, etc: &Path, state_dir: &Path) -> Result<()> {
    join_as(token, etc, state_dir, None).await
}

/// `addr`: the address to report (and run SWIM on) instead of the one
/// facing agentd, for the simulator.
pub async fn join_as(token: &str, etc: &Path, state_dir: &Path, addr: Option<&str>) -> Result<()> {
    let t = Token::decode(token)?;
    let source: Option<std::net::IpAddr> = match addr {
        Some(a) => Some(a.parse()?),
        None => None,
    };
    fs::create_dir_all(etc)?;
    fs::set_permissions(etc, fs::Permissions::from_mode(0o700))?;

    // The CA comes over an unverified connection and is trusted only if
    // its hash matches the token.
    let (st, ca) = one_shot(source, &t.agentd, &t.server_name, proto::tls::agent_client_config(), Method::GET, "/v1/join/ca", vec![], 30).await?;
    if st != StatusCode::OK {
        bail!("fetching the CA: {st}");
    }
    let ca_pem = String::from_utf8(ca.to_vec())?;
    let der = rustls_pemfile::certs(&mut ca_pem.as_bytes()).next().context("no certificate in the CA")??;
    if sha256_hex(der.as_ref()) != t.ca_sha256 {
        bail!("agentd's CA does not match the token: {} != {}", sha256_hex(der.as_ref()), t.ca_sha256);
    }
    write_atomic(&etc.join("ca.pem"), ca_pem.as_bytes(), 0o644, None)?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(der)?;
    let verified = Arc::new(
        rustls::ClientConfig::builder_with_provider(proto::tls::provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let key = rcgen::KeyPair::generate()?;
    let mut params = rcgen::CertificateParams::new(vec![t.host.clone()])?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.distinguished_name.push(rcgen::DnType::CommonName, t.host.clone());
    let csr_pem = params.serialize_request(&key)?.pem()?;
    let seal = proto::seal::generate()?;
    write_private(&etc.join("host.key"), key.serialize_pem().as_bytes())?;
    write_private(&etc.join("seal.key"), b64(&seal.to_bytes()).as_bytes())?;

    let mut req = JoinRequest {
        token_id: t.id.clone(),
        host: t.host.clone(),
        csr_pem,
        seal_pub: b64(&proto::seal::public_of(&seal)),
        facts: facts(&t.agentd, addr),
        mac: String::new(),
    };
    req.mac = request_mac(&t.secret, &req)?;
    // agentd holds a bounded number of joins for the mgr; when many hosts
    // join at once it turns the rest away (503) until the mgr catches up
    let mut backoff = 1.0f64;
    let b = loop {
        let (st, b) = one_shot(source, &t.agentd, &t.server_name, verified.clone(), Method::POST, "/v1/join", serde_json::to_vec(&req)?, 30).await?;
        match st {
            StatusCode::ACCEPTED => break b,
            StatusCode::SERVICE_UNAVAILABLE => {
                let jitter = (proto::intent::wall_ns() % 1000) as f64 / 1000.0;
                tokio::time::sleep(std::time::Duration::from_secs_f64(backoff * (0.5 + jitter))).await;
                backoff = (backoff * 2.0).min(30.0);
            }
            st => bail!("join request: {st} {}", String::from_utf8_lossy(&b)),
        }
    };
    let join_id = serde_json::from_slice::<serde_json::Value>(&b)?["join_id"].as_str().context("no join id")?.to_string();
    eprintln!("join request {join_id} for {} sent; waiting for the mgr", t.host);

    let d: JoinDecision = loop {
        let path = format!("/v1/join/{join_id}?wait=30");
        let (st, b) = one_shot(source, &t.agentd, &t.server_name, verified.clone(), Method::GET, &path, vec![], 45).await?;
        match st {
            StatusCode::OK => break serde_json::from_slice(&b)?,
            StatusCode::NO_CONTENT => continue,
            s => bail!("waiting for the decision: {s} {}", String::from_utf8_lossy(&b)),
        }
    };
    if !mac_eq(&d.mac, &decision_mac(&t.secret, &t.id, &d)?) {
        bail!("the decision is not from the mgr (bad MAC)");
    }
    if !d.approved {
        bail!("join rejected: {}", d.reason);
    }
    if d.host != t.host {
        bail!("the decision is for {}, not {}", d.host, t.host);
    }
    write_atomic(&etc.join("host.crt"), d.cert_pem.as_bytes(), 0o644, None)?;
    let mut trusted = Vec::new();
    for (i, k) in d.intent_keys.iter().enumerate() {
        let p = etc.join(format!("intent-{i}.pub"));
        write_atomic(&p, k.as_bytes(), 0o644, None)?;
        trusted.push(p);
    }
    let cfg = json!({
        "host": t.host,
        "agentd": t.agentd,
        "server_name": t.server_name,
        "ca": etc.join("ca.pem"),
        "cert": etc.join("host.crt"),
        "key": etc.join("host.key"),
        "seal_key": etc.join("seal.key"),
        "trusted_keys": trusted,
        "state_dir": state_dir,
        "swim": d.swim,
    });
    write_atomic(&etc.join("agent.json"), serde_json::to_string_pretty(&cfg)?.as_bytes(), 0o600, None)?;
    eprintln!("joined as {}; config in {}", t.host, etc.join("agent.json").display());
    Ok(())
}
