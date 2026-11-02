//! The agent's side of agentd: HTTP/1.1 over TLS (agentd's certificate
//! checked against the cluster CA), two persistent connections: one parked
//! in the intent long-poll, one for blobs and status.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use proto::intent::{b64, Envelope, HostStatus};
use proto::tls;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::TlsConnector;

use crate::Config;

type Conn = Mutex<Option<SendRequest<Full<Bytes>>>>;

pub enum Poll {
    Intent(Envelope),
    /// agentd wants a status report
    Poke,
    Nothing,
}

pub struct Client {
    addr: String,
    source: Option<std::net::IpAddr>,
    server_name: ServerName<'static>,
    tls: TlsConnector,
    /// keyring authorization header, for hosts without a certificate
    auth: Option<String>,
    poll: Conn,
    rpc: Conn,
}

impl Client {
    pub fn new(cfg: &Config) -> Result<Client> {
        let auth = match &cfg.keyring {
            Some(k) => {
                let keyring = std::fs::read_to_string(k).with_context(|| format!("{}", k.display()))?;
                Some(format!("Agent {} {}", cfg.host, b64(keyring.as_bytes())))
            }
            None => None,
        };
        let mut roots = rustls::RootCertStore::empty();
        for c in tls::load_certs(&cfg.ca)? {
            roots.add(c)?;
        }
        let builder = rustls::ClientConfig::builder_with_provider(tls::provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots);
        let tls = match (&cfg.cert, &cfg.key) {
            (Some(c), Some(k)) => builder.with_client_auth_cert(tls::load_certs(c)?, tls::load_key(k)?)?,
            _ => {
                if auth.is_none() {
                    anyhow::bail!("config needs cert and key, or a keyring");
                }
                builder.with_no_client_auth()
            }
        };
        let name = match &cfg.server_name {
            Some(n) => n.clone(),
            None => cfg.agentd.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or_else(|| cfg.agentd.clone()),
        };
        Ok(Client {
            addr: cfg.agentd.clone(),
            source: cfg.source_addr,
            server_name: ServerName::try_from(name)?.to_owned(),
            tls: TlsConnector::from(std::sync::Arc::new(tls)),
            auth,
            poll: Mutex::new(None),
            rpc: Mutex::new(None),
        })
    }

    async fn connect(&self) -> Result<SendRequest<Full<Bytes>>> {
        let tcp = tokio::time::timeout(Duration::from_secs(10), tcp_connect(&self.addr, self.source)).await.context("connect timed out")??;
        tcp.set_nodelay(true)?;
        let tls = tokio::time::timeout(Duration::from_secs(10), self.tls.connect(self.server_name.clone(), tcp))
            .await
            .context("TLS handshake timed out")??;
        let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(sender)
    }

    async fn call(&self, conn: &Conn, method: Method, path: &str, body: Vec<u8>, timeout: Duration) -> Result<(StatusCode, Bytes)> {
        let mut g = conn.lock().await;
        if g.as_ref().map(|s| s.is_closed()).unwrap_or(true) {
            *g = Some(self.connect().await?);
        }
        let s = g.as_mut().unwrap();
        let mut req = Request::builder().method(method).uri(path).header("host", self.addr.as_str()).header("content-type", "application/json");
        if let Some(a) = &self.auth {
            req = req.header("authorization", a.as_str());
        }
        let req = req.body(Full::new(Bytes::from(body)))?;
        let r = tokio::time::timeout(timeout, async {
            s.ready().await?;
            let resp = s.send_request(req).await?;
            let status = resp.status();
            let body = resp.into_body().collect().await?.to_bytes();
            anyhow::Ok((status, body))
        })
        .await;
        match r {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                *g = None;
                Err(e)
            }
            Err(_) => {
                *g = None;
                bail!("request to agentd timed out")
            }
        }
    }

    /// Long-poll for an envelope newer than `have`.
    pub async fn intent(&self, have: u64, wait: u64) -> Result<Poll> {
        let path = format!("/v1/agent/intent?have={have}&wait={wait}");
        let (st, b) = self.call(&self.poll, Method::GET, &path, vec![], Duration::from_secs(wait + 15)).await?;
        match st {
            StatusCode::OK => Ok(Poll::Intent(serde_json::from_slice(&b)?)),
            StatusCode::NO_CONTENT => Ok(Poll::Nothing),
            StatusCode::RESET_CONTENT => Ok(Poll::Poke),
            s => bail!("agentd: {s} {}", String::from_utf8_lossy(&b)),
        }
    }

    pub async fn blob(&self, sha: &str) -> Result<Bytes> {
        let (st, b) = self.call(&self.rpc, Method::GET, &format!("/v1/agent/blobs/{sha}"), vec![], Duration::from_secs(120)).await?;
        if st != StatusCode::OK {
            bail!("agentd: {st} {}", String::from_utf8_lossy(&b));
        }
        Ok(b)
    }

    /// Downloads a blob only to hash it (the simulator's agent upgrade:
    /// agentd's cost, without thousands of binaries in memory).
    pub async fn blob_sha256(&self, sha: &str) -> Result<String> {
        use sha2::Digest;
        let mut g = self.rpc.lock().await;
        if g.as_ref().map(|s| s.is_closed()).unwrap_or(true) {
            *g = Some(self.connect().await?);
        }
        let s = g.as_mut().unwrap();
        let req = Request::builder().method(Method::GET).uri(format!("/v1/agent/blobs/{sha}")).header("host", self.addr.as_str()).body(Full::new(Bytes::new()))?;
        let r = tokio::time::timeout(Duration::from_secs(120), async {
            s.ready().await?;
            let resp = s.send_request(req).await?;
            if resp.status() != StatusCode::OK {
                bail!("agentd: {}", resp.status());
            }
            let mut body = resp.into_body();
            let mut h = sha2::Sha256::new();
            while let Some(f) = body.frame().await {
                if let Ok(d) = f?.into_data() {
                    h.update(&d);
                }
            }
            anyhow::Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect::<String>())
        })
        .await;
        match r {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => {
                *g = None;
                Err(e)
            }
            Err(_) => {
                *g = None;
                bail!("blob download timed out")
            }
        }
    }

    /// Posts status; agentd answers whether its SWIM member sees this host
    /// (`swim_up`, absent without SWIM).
    pub async fn status(&self, s: &HostStatus) -> Result<Option<bool>> {
        let (st, b) = self.call(&self.rpc, Method::POST, "/v1/agent/status", serde_json::to_vec(s)?, Duration::from_secs(15)).await?;
        if st != StatusCode::OK {
            bail!("agentd: {st} {}", String::from_utf8_lossy(&b));
        }
        Ok(serde_json::from_slice::<serde_json::Value>(&b).ok().and_then(|v| v["swim_up"].as_bool()))
    }
}

async fn tcp_connect(addr: &str, source: Option<std::net::IpAddr>) -> Result<TcpStream> {
    let Some(src) = source else { return Ok(TcpStream::connect(addr).await?) };
    let to = tokio::net::lookup_host(addr).await?.next().context("agentd address")?;
    let s = if to.is_ipv4() { tokio::net::TcpSocket::new_v4()? } else { tokio::net::TcpSocket::new_v6()? };
    s.bind(std::net::SocketAddr::new(src, 0))?;
    Ok(s.connect(to).await?)
}

/// One request on a fresh connection, for joining (before the host has an
/// identity).
#[allow(clippy::too_many_arguments)]
pub async fn one_shot(
    source: Option<std::net::IpAddr>,
    addr: &str,
    server_name: &str,
    tls: std::sync::Arc<rustls::ClientConfig>,
    method: Method,
    path: &str,
    body: Vec<u8>,
    timeout_s: u64,
) -> Result<(StatusCode, Bytes)> {
    let name = ServerName::try_from(server_name.to_string())?;
    tokio::time::timeout(Duration::from_secs(timeout_s), async {
        let tcp = tcp_connect(addr, source).await?;
        let tls = TlsConnector::from(tls).connect(name, tcp).await?;
        let (mut s, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header("host", addr)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(body)))?;
        let resp = s.send_request(req).await?;
        let st = resp.status();
        Ok((st, resp.into_body().collect().await?.to_bytes()))
    })
    .await
    .context("request to agentd timed out")?
}
