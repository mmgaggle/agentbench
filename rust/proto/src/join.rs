//! Joining a host without SSH (docs/agent-executes-intent.md).
//!
//! The mgr issues a one-time token bound to a host name. The token carries
//! agentd's address, the cluster CA's fingerprint and a secret that only the
//! mgr and the joining host know. agentd relays the join request and the
//! mgr's answer but never sees the secret, and both messages carry an HMAC
//! under it, so agentd can't substitute the host's keys or the intent keys
//! the host will trust.
//!
//!   agent -> agentd   GET  /v1/join/ca          (pinned by the token's CA hash)
//!   agent -> agentd   POST /v1/join             JoinRequest
//!   mgr   -> agentd   GET  /v1/joins            pending requests
//!   mgr   -> agentd   POST /v1/joins/<id>       JoinDecision
//!   agent -> agentd   GET  /v1/join/<id>?wait=S the decision

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use hmac::Mac as _;
use serde::{Deserialize, Serialize};

use crate::intent::{b64, sha256_hex, unb64};

type HmacSha256 = hmac::Hmac<sha2::Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Token {
    pub v: u32,
    /// agentd's agent endpoint, host:port
    pub agentd: String,
    /// TLS name in agentd's certificate
    pub server_name: String,
    /// SHA-256 (hex) of the cluster CA certificate, DER
    pub ca_sha256: String,
    /// Public token id; the mgr finds the secret by it
    pub id: String,
    /// base64 secret shared by the mgr and the joining host
    pub secret: String,
    pub host: String,
}

impl Token {
    pub fn encode(&self) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap())
    }

    pub fn decode(s: &str) -> Result<Token> {
        let b = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s.trim()).context("token is not base64url")?;
        let t: Token = serde_json::from_slice(&b).context("token is not a join token")?;
        if t.v != 1 {
            bail!("unsupported token version {}", t.v);
        }
        Ok(t)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub token_id: String,
    pub host: String,
    /// PKCS#10 request for the host's TLS client certificate
    pub csr_pem: String,
    /// X25519 public key (base64) the mgr seals secrets to
    pub seal_pub: String,
    /// What the host says about itself (addresses, OS, podman, ...)
    #[serde(default)]
    pub facts: serde_json::Value,
    /// base64 HMAC-SHA256 under the token secret, see `request_mac`
    pub mac: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SwimConfig {
    /// The host's member index (agentd maps it back to the host)
    pub idx: u32,
    /// Where this member listens, ip:port
    pub bind: String,
    /// Members to announce to: agentd's member, ip:port
    pub seeds: Vec<String>,
    /// Expected cluster size; sizes gossip retransmissions and the
    /// suspicion timeout (foca's LAN profile). Default 1000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_size: Option<u32>,
    /// Lifeguard extensions to SWIM (proto::swim); every member of the
    /// cluster, agentd's included, must agree
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lifeguard: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinDecision {
    pub approved: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub cert_pem: String,
    /// base64 Ed25519 keys allowed to sign this host's intents
    #[serde(default)]
    pub intent_keys: Vec<String>,
    #[serde(default)]
    pub swim: Option<SwimConfig>,
    /// base64 HMAC-SHA256 under the token secret, see `decision_mac`
    #[serde(default)]
    pub mac: String,
}

fn mac_parts(secret: &str, parts: &[&[u8]]) -> Result<Vec<u8>> {
    let mut m = HmacSha256::new_from_slice(&unb64(secret)?).expect("any key length");
    for p in parts {
        // length-prefix every field so field boundaries can't shift
        m.update(&(p.len() as u64).to_be_bytes());
        m.update(p);
    }
    Ok(m.finalize().into_bytes().to_vec())
}

pub fn request_mac(secret: &str, r: &JoinRequest) -> Result<String> {
    Ok(b64(&mac_parts(secret, &[b"join-request", r.token_id.as_bytes(), r.host.as_bytes(), r.csr_pem.as_bytes(), r.seal_pub.as_bytes()])?))
}

pub fn decision_mac(secret: &str, token_id: &str, d: &JoinDecision) -> Result<String> {
    let keys = d.intent_keys.join(",");
    let swim = serde_json::to_vec(&d.swim)?;
    Ok(b64(&mac_parts(secret, &[b"join-decision", token_id.as_bytes(), d.host.as_bytes(), d.cert_pem.as_bytes(), keys.as_bytes(), &swim])?))
}

/// Constant-time comparison of two base64 MACs.
pub fn mac_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn random_b64(n: usize) -> Result<String> {
    use std::io::Read as _;
    let mut b = vec![0u8; n];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b64(&b))
}

pub fn random_id() -> Result<String> {
    Ok(sha256_hex(random_b64(16)?.as_bytes())[..12].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macs_bind_the_fields() {
        let secret = random_b64(32).unwrap();
        let mut r = JoinRequest {
            token_id: "t1".into(),
            host: "node4".into(),
            csr_pem: "csr".into(),
            seal_pub: "pub".into(),
            facts: serde_json::Value::Null,
            mac: String::new(),
        };
        r.mac = request_mac(&secret, &r).unwrap();
        assert!(mac_eq(&r.mac, &request_mac(&secret, &r).unwrap()));
        let mut swapped = r.clone();
        swapped.seal_pub = "agentd's key".into();
        assert!(!mac_eq(&r.mac, &request_mac(&secret, &swapped).unwrap()));
        assert!(!mac_eq(&r.mac, &request_mac(&random_b64(32).unwrap(), &r).unwrap()));

        let t = Token {
            v: 1,
            agentd: "10.77.0.1:7160".into(),
            server_name: "agentd.lan".into(),
            ca_sha256: "ab".into(),
            id: "t1".into(),
            secret,
            host: "node4".into(),
        };
        assert_eq!(Token::decode(&t.encode()).unwrap(), t);
    }
}
