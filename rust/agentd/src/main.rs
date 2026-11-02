//! agentd: a standalone prototype of the cephadm agent endpoint.
//!
//! Serves the same HTTPS POST /data protocol as the cephadm mgr module's
//! AgentEndpoint, validates and applies reports with the same semantics
//! (state.rs), persists the same config-key records (persist.rs), and
//! sends mgr->agent counter updates like AgentMessageThread. A plain HTTP
//! admin port serves stats and read queries:
//!
//!   GET /stats            counters and latency percentiles
//!   GET /hosts            host list
//!   GET /host/<name>      daemons on a host (`orch ps --hostname`)
//!   GET /health           failed daemons (CEPHADM_FAILED_DAEMON)
//!
//! v2 (POST /v2/report): agents keep one TLS connection open, authenticate
//! with their keyring once, send a full snapshot, then only versioned
//! deltas, usage samples and heartbeats.
//!
//! Intents (intents.rs), admin port:
//!   PUT /v1/blobs                 store a blob, returns its sha256
//!   PUT /v1/hosts/<h>/intent      store a signed intent envelope
//!   GET /v1/hosts/<h>/intent      the stored envelope
//!   GET /v1/hosts/<h>/status      the agent's latest status
//!   DELETE /v1/hosts/<h>          forget a removed host (intent, status)
//!   GET /v1/intent-status         intended vs applied generation, per host
//!   GET /v1/events?since=N        unit transitions and applied generations
//!   GET  /v1/joins                pending join requests
//!   PUT  /v1/members              joined hosts and their SWIM indexes (the mgr's)
//!   POST /v1/joins/<id>           the mgr's decision
//! and on the agent port, authenticated by a client certificate from the
//! cluster CA (common name = host) or `Authorization: Agent <host> <base64
//! keyring>`:
//!   GET  /v1/agent/intent?have=G&wait=S   long-poll for a newer intent
//!   GET  /v1/agent/blobs/<sha256>
//!   POST /v1/agent/status
//! and unauthenticated, for joining (proto::join):
//!   GET  /v1/join/ca   POST /v1/join   GET /v1/join/<id>?wait=S

mod intents;
mod persist;
mod rados;
mod state;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use proto::tls;
use rustls::pki_types::ServerName;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use persist::{Mode, Persister, Policy};
use state::{Cache, Outcome, V2Outcome, V2_KINDS};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "/mon/vcluster/ceph.conf")]
    conf: String,
    #[arg(long, default_value = "client.admin")]
    id: String,
    /// Agent endpoint (HTTPS)
    #[arg(long, default_value = "127.0.0.1:7160")]
    listen: SocketAddr,
    /// Admin/query endpoint (plain HTTP)
    #[arg(long, default_value = "127.0.0.1:7161")]
    admin: SocketAddr,
    /// TLS material written by cluster/certs.py
    #[arg(long, default_value = "/mon/vcluster/agentd")]
    cert_dir: PathBuf,
    /// Endpoint key: the mgr's own RSA-4096 cert, or an ECDSA P-256 one
    #[arg(long, value_enum, default_value = "rsa")]
    key: KeyType,
    #[arg(long, value_enum, default_value = "writeback")]
    persist: Mode,
    #[arg(long, value_enum, default_value = "always")]
    policy: Policy,
    #[arg(long, default_value_t = 1000)]
    flush_ms: u64,
    #[arg(long, default_value_t = 300)]
    checkpoint_secs: u64,
    /// Hosts whose records may be in flight to the mon at once
    #[arg(long, default_value_t = 256)]
    max_inflight: usize,
    /// Tokio worker threads (default: CPUs in the affinity mask)
    #[arg(long)]
    threads: Option<usize>,
    /// Parallel config-key gets at startup
    #[arg(long, default_value_t = 64)]
    load_threads: usize,
    /// Run a SWIM member (the controller) for agent liveness and signals
    /// (simulation: loopback addresses, swimsim's signals)
    #[arg(long)]
    swim: bool,
    /// Run agentd's SWIM member here for real agents (gossip: digests,
    /// pokes, liveness events), e.g. 10.77.0.1:7946
    #[arg(long)]
    swim_bind: Option<SocketAddr>,
    /// Expected number of hosts, to size gossip (foca's LAN profile)
    #[arg(long, default_value_t = 1000)]
    swim_cluster_size: u32,
    /// Lifeguard extensions to SWIM; the agents must have them too
    #[arg(long)]
    swim_lifeguard: bool,
    /// Drop blobs no current intent has referenced for this long
    #[arg(long, default_value_t = 600)]
    blob_grace_s: u64,
    /// Take hosts and keyrings from this hosts.json instead of the cluster's
    /// KV store; with --persist none, run without a cluster at all
    #[arg(long)]
    hosts_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum KeyType {
    Rsa,
    Ec,
}

/// What the controller's SWIM member has learned.
#[derive(Default)]
struct SwimState {
    up: std::collections::HashSet<u32>,
    /// host index -> wall-clock ns it was declared down
    down_ns: std::collections::HashMap<u32, u64>,
    up_events: u64,
    down_events: u64,
    renames: u64,
}

struct Swim {
    state: Arc<Mutex<SwimState>>,
    traffic: Arc<proto::swim::Traffic>,
    ctrl: tokio::sync::mpsc::UnboundedSender<proto::swim::Ctrl>,
    version: std::sync::atomic::AtomicU32,
}

struct App {
    /// --swim-bind member's packets
    gossip_traffic: Arc<proto::swim::Traffic>,
    /// cluster CA, served to joining hosts (they pin its hash)
    ca_pem: String,
    swim: Option<Swim>,
    intents: Arc<intents::Intents>,
    cache: Arc<Cache>,
    persist: Arc<Persister>,
    agent_tls: TlsConnector,
    handler_us: Mutex<Histogram<u64>>,
    v2_handler_us: Mutex<Histogram<u64>>,
    conns: std::sync::atomic::AtomicU64,
    conns_open: std::sync::atomic::AtomicI64,
    tls_errors: std::sync::atomic::AtomicU64,
    started: Instant,
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 600_000_000, 3).unwrap()
}

fn bytes_response(code: StatusCode, b: Bytes) -> Response<Full<Bytes>> {
    Response::builder().status(code).header("Content-Type", "application/octet-stream").body(Full::new(b)).unwrap()
}

fn query(req: &Request<Incoming>, key: &str) -> Option<String> {
    req.uri().query()?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| v.to_string())
    })
}

async fn read_body(req: Request<Incoming>) -> Result<Bytes, Response<Full<Bytes>>> {
    read_body_limit(req, 64 << 20).await
}

/// Returns free heap pages to the system (glibc's malloc_trim); the time
/// it took, in ms.
fn release_free_memory() -> f64 {
    let t0 = Instant::now();
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        unsafe {
            malloc_trim(0);
        }
    }
    t0.elapsed().as_secs_f64() * 1000.0
}

fn rss_mb() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: f64 = s.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok()?;
    Some((kb / 1024.0 * 10.0).round() / 10.0)
}

async fn read_body_limit(req: Request<Incoming>, limit: usize) -> Result<Bytes, Response<Full<Bytes>>> {
    match Limited::new(req.into_body(), limit).collect().await {
        Ok(b) => Ok(b.to_bytes()),
        Err(e) => Err(json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() }))),
    }
}

fn json_response(code: StatusCode, v: &Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(code)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(v).unwrap())))
        .unwrap()
}

impl App {
    /// AgentMessageThread: tell the agent on `host` the current counter so it
    /// resends up-to-date metadata. 10-byte zero-padded length + JSON, over
    /// mutual TLS; retried after 3s and 5s on connection errors.
    fn request_ack(self: &Arc<Self>, host: &str, addr: &str, port: u16, counter: u64) {
        let (app, host, addr) = (self.clone(), host.to_string(), addr.to_string());
        tokio::spawn(async move {
            let payload = json!({ "counter": counter }).to_string();
            let msg = format!("{:010}{}", payload.len(), payload);
            for wait in [3u64, 5] {
                let r = async {
                    let s = TcpStream::connect((addr.as_str(), port)).await?;
                    let name = ServerName::try_from(addr.clone())?;
                    let mut s = app.agent_tls.connect(name, s).await?;
                    s.write_all(msg.as_bytes()).await?;
                    let mut buf = [0u8; 1024];
                    let _ = s.read(&mut buf).await?;
                    anyhow::Ok(())
                };
                match tokio::time::timeout(Duration::from_secs(10), r).await {
                    Ok(Ok(())) => break,
                    _ => tokio::time::sleep(Duration::from_secs(wait)).await,
                }
            }
            if let Some(h) = app.cache.hosts.get(&host) {
                h.lock().unwrap().messaging = false;
            }
        });
    }

    async fn data(self: &Arc<Self>, req: Request<Incoming>) -> Response<Full<Bytes>> {
        let t0 = Instant::now();
        let body = match Limited::new(req.into_body(), 64 << 20).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => return json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
        };
        let app = self.clone();
        let outcome = self.cache.handle(&body, &|h, a, p, c| app.request_ack(h, a, p, c));
        let resp = match outcome {
            Outcome::BadRequest(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e })),
            Outcome::Done(result, persist_host) => {
                if let Some(h) = persist_host {
                    self.persist.on_update(&self.cache, &h).await;
                }
                json_response(StatusCode::OK, &json!({ "result": result }))
            }
        };
        self.handler_us.lock().unwrap().saturating_record(t0.elapsed().as_micros().max(1) as u64);
        resp
    }

    async fn v2(self: &Arc<Self>, req: Request<Incoming>, bound: &Mutex<Option<String>>) -> Response<Full<Bytes>> {
        let t0 = Instant::now();
        let body = match Limited::new(req.into_body(), 64 << 20).collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => return json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
        };
        let outcome = self.cache.handle_v2(&mut bound.lock().unwrap(), &body);
        let resp = match outcome {
            V2Outcome::Ok { version, counter, persist } => {
                if let Some(h) = persist {
                    self.persist.on_update(&self.cache, &h).await;
                }
                json_response(StatusCode::OK, &json!({ "version": version, "counter": counter }))
            }
            V2Outcome::Resync { version } => json_response(StatusCode::CONFLICT, &json!({ "resync": true, "version": version })),
            V2Outcome::Unauthorized(e) => json_response(StatusCode::UNAUTHORIZED, &json!({ "error": e })),
            V2Outcome::BadRequest(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e })),
        };
        self.v2_handler_us.lock().unwrap().saturating_record(t0.elapsed().as_micros().max(1) as u64);
        resp
    }

    /// `Authorization: Agent <host> <base64 keyring>`, checked against the
    /// host's agent keyring.
    fn agent_host(&self, req: &Request<Incoming>, peer: Option<&str>) -> Result<String, Response<Full<Bytes>>> {
        let deny = |m: &str| json_response(StatusCode::UNAUTHORIZED, &json!({ "error": m }));
        if let Some(host) = peer {
            // rustls already verified the chain to the cluster CA
            return if self.intents.is_member(host) || self.cache.hosts.contains_key(host) {
                Ok(host.to_string())
            } else {
                Err(deny("certificate for an unknown host"))
            };
        }
        let h = req.headers().get("authorization").and_then(|v| v.to_str().ok()).unwrap_or("");
        let mut it = h.splitn(3, ' ');
        let (Some("Agent"), Some(host), Some(k)) = (it.next(), it.next(), it.next()) else {
            return Err(deny("missing Agent authorization"));
        };
        let keyring = proto::intent::unb64(k).ok().and_then(|b| String::from_utf8(b).ok());
        match (self.cache.hosts.get(host), keyring) {
            (Some(e), Some(k)) if e.lock().unwrap().agent_key.as_deref() == Some(k.as_str()) => Ok(host.to_string()),
            _ => Err(deny("bad agent credentials")),
        }
    }

    /// Join relay (unauthenticated): agentd stores requests and hands back
    /// the mgr's decision; both are MACed under a secret agentd never sees.
    async fn join_req(self: &Arc<Self>, req: Request<Incoming>) -> Response<Full<Bytes>> {
        let path = req.uri().path().to_string();
        match (req.method().clone(), path.as_str()) {
            (Method::GET, "/v1/join/ca") => {
                Response::builder().status(StatusCode::OK).body(Full::new(Bytes::from(self.ca_pem.clone()))).unwrap()
            }
            (Method::POST, "/v1/join") => {
                let body = match read_body(req).await {
                    Ok(b) => b,
                    Err(r) => return r,
                };
                match serde_json::from_slice::<proto::join::JoinRequest>(&body) {
                    Ok(j) => match self.intents.submit_join(j) {
                        Ok(id) => json_response(StatusCode::ACCEPTED, &json!({ "join_id": id })),
                        Err(e) => json_response(StatusCode::SERVICE_UNAVAILABLE, &json!({ "error": e })),
                    },
                    Err(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
                }
            }
            (Method::GET, p) => {
                let wait = query(&req, "wait").and_then(|v| v.parse().ok()).unwrap_or(0u64).min(300);
                match self.intents.wait_join(&p[9..], Duration::from_secs(wait)).await {
                    None => json_response(StatusCode::NOT_FOUND, &json!({ "error": "no such join" })),
                    Some(None) => Response::builder().status(StatusCode::NO_CONTENT).body(Full::new(Bytes::new())).unwrap(),
                    Some(Some(d)) => json_response(StatusCode::OK, &json!(*d)),
                }
            }
            _ => json_response(StatusCode::NOT_FOUND, &json!({})),
        }
    }

    async fn agent_req(self: &Arc<Self>, req: Request<Incoming>, peer: Option<&str>) -> Response<Full<Bytes>> {
        let host = match self.agent_host(&req, peer) {
            Ok(h) => h,
            Err(r) => return r,
        };
        let path = req.uri().path().to_string();
        match (req.method().clone(), path.as_str()) {
            (Method::GET, "/v1/agent/intent") => {
                let have = query(&req, "have").and_then(|v| v.parse().ok()).unwrap_or(0);
                let wait = query(&req, "wait").and_then(|v| v.parse().ok()).unwrap_or(0u64).min(300);
                match self.intents.wait_intent(&host, have, Duration::from_secs(wait)).await {
                    intents::Wake::Intent(env) => json_response(StatusCode::OK, &json!(*env)),
                    // 205: post your status, then poll again
                    intents::Wake::Poke => Response::builder().status(StatusCode::RESET_CONTENT).body(Full::new(Bytes::new())).unwrap(),
                    intents::Wake::Timeout => Response::builder().status(StatusCode::NO_CONTENT).body(Full::new(Bytes::new())).unwrap(),
                }
            }
            (Method::GET, p) if p.starts_with("/v1/agent/blobs/") => match self.intents.blob(&p[16..]) {
                Some(b) => bytes_response(StatusCode::OK, b),
                None => json_response(StatusCode::NOT_FOUND, &json!({ "error": "no such blob" })),
            },
            (Method::POST, "/v1/agent/status") => {
                let body = match read_body(req).await {
                    Ok(b) => b,
                    Err(r) => return r,
                };
                match serde_json::from_slice::<proto::intent::HostStatus>(&body) {
                    Ok(st) if st.host == host => {
                        let swim_up = self.intents.put_status(st);
                        json_response(StatusCode::OK, &json!({ "swim_up": swim_up }))
                    }
                    Ok(_) => json_response(StatusCode::FORBIDDEN, &json!({ "error": "status for another host" })),
                    Err(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
                }
            }
            _ => json_response(StatusCode::NOT_FOUND, &json!({})),
        }
    }

    async fn admin_req(self: &Arc<Self>, req: Request<Incoming>) -> Response<Full<Bytes>> {
        let path = req.uri().path().to_string();
        let host_path = |suffix: &str| -> Option<String> {
            path.strip_prefix("/v1/hosts/")?.strip_suffix(suffix).map(|h| h.to_string())
        };
        match (req.method().clone(), path.as_str()) {
            // blobs include agent binaries
            (Method::PUT, "/v1/blobs") => match read_body_limit(req, 512 << 20).await {
                Ok(b) => json_response(StatusCode::OK, &json!({ "sha256": self.intents.put_blob(b) })),
                Err(r) => r,
            },
            (Method::PUT, _) if host_path("/intent").is_some() => {
                let host = host_path("/intent").unwrap();
                let body = match read_body(req).await {
                    Ok(b) => b,
                    Err(r) => return r,
                };
                match serde_json::from_slice::<proto::intent::Envelope>(&body) {
                    Ok(env) if env.host == host => match self.intents.put_intent(env) {
                        Ok(g) => json_response(StatusCode::OK, &json!({ "generation": g })),
                        Err(e) => json_response(StatusCode::CONFLICT, &json!({ "error": e })),
                    },
                    Ok(_) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": "envelope is for another host" })),
                    Err(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
                }
            }
            (Method::DELETE, p) if p.starts_with("/v1/hosts/") && !p[10..].contains('/') => {
                if self.intents.forget_host(&p[10..]) {
                    json_response(StatusCode::OK, &json!({}))
                } else {
                    json_response(StatusCode::NOT_FOUND, &json!({}))
                }
            }
            (Method::GET, _) if host_path("/intent").is_some() => match self.intents.intent(&host_path("/intent").unwrap()) {
                Some(env) => json_response(StatusCode::OK, &json!(*env)),
                None => json_response(StatusCode::NOT_FOUND, &json!({})),
            },
            (Method::GET, _) if host_path("/status").is_some() => match self.intents.status(&host_path("/status").unwrap()) {
                Some(v) => json_response(StatusCode::OK, &v),
                None => json_response(StatusCode::NOT_FOUND, &json!({})),
            },
            (Method::GET, "/v1/intent-status") => json_response(StatusCode::OK, &self.intents.summary()),
            (Method::GET, "/v1/liveness") => json_response(StatusCode::OK, &self.intents.liveness_map()),
            (Method::POST, "/v1/hosts-status") => {
                let body = match read_body(req).await {
                    Ok(b) => b,
                    Err(r) => return r,
                };
                match serde_json::from_slice::<Vec<String>>(&body) {
                    Ok(names) => json_response(StatusCode::OK, &self.intents.statuses(&names)),
                    Err(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
                }
            }
            (Method::GET, "/v1/joins") => json_response(StatusCode::OK, &self.intents.pending_joins()),
            (Method::PUT, "/v1/members") => {
                let body = match read_body(req).await {
                    Ok(b) => b,
                    Err(r) => return r,
                };
                match serde_json::from_slice::<std::collections::HashMap<String, Option<u32>>>(&body) {
                    Ok(m) => {
                        let n = m.len();
                        self.intents.set_members(m.into_iter().map(|(h, idx)| (h, intents::Member { swim_idx: idx })).collect());
                        json_response(StatusCode::OK, &json!({ "members": n }))
                    }
                    Err(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
                }
            }
            (Method::POST, p) if p.starts_with("/v1/joins/") => {
                let id = p[10..].to_string();
                let body = match read_body(req).await {
                    Ok(b) => b,
                    Err(r) => return r,
                };
                match serde_json::from_slice::<proto::join::JoinDecision>(&body) {
                    Ok(d) => match self.intents.decide_join(&id, d) {
                        Ok(()) => json_response(StatusCode::OK, &json!({})),
                        Err(e) => json_response(StatusCode::CONFLICT, &json!({ "error": e })),
                    },
                    Err(e) => json_response(StatusCode::BAD_REQUEST, &json!({ "error": e.to_string() })),
                }
            }
            (Method::GET, "/v1/events") => {
                let since = query(&req, "since").and_then(|v| v.parse().ok()).unwrap_or(0);
                Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(self.intents.events_since(since))))
                    .unwrap()
            }
            _ => self.admin(&req),
        }
    }

    fn stats(&self) -> Value {
        let c = &self.cache.counters;
        let h = self.handler_us.lock().unwrap();
        let h2 = self.v2_handler_us.lock().unwrap();
        let lag = self.cache.v2.lag_us.lock().unwrap();
        let p = self.persist.latency.lock().unwrap();
        let q = |h: &Histogram<u64>, x: f64| if h.is_empty() { 0.0 } else { h.value_at_quantile(x) as f64 / 1000.0 };
        let v2 = &self.cache.v2;
        let kinds: serde_json::Map<String, Value> = V2_KINDS
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (k.to_string(), json!({"count": v2.count[i].load(Ordering::Relaxed), "bytes": v2.bytes[i].load(Ordering::Relaxed)}))
            })
            .collect();
        let swim = self.swim.as_ref().map(|sw| {
            let st = sw.state.lock().unwrap();
            let t = &sw.traffic;
            json!({
                "up": st.up.len(), "down": st.down_ns.len(), "up_events": st.up_events,
                "down_events": st.down_events, "renames": st.renames,
                "pkts_sent": t.pkts_sent.load(Ordering::Relaxed), "pkts_recv": t.pkts_recv.load(Ordering::Relaxed),
                "bytes_sent": t.bytes_sent.load(Ordering::Relaxed),
            })
        });
        let g = &self.gossip_traffic;
        let blobs = self.intents.blob_stats();
        json!({
            "gossip": {
                "pkts_sent": g.pkts_sent.load(Ordering::Relaxed), "pkts_recv": g.pkts_recv.load(Ordering::Relaxed),
                "bytes_sent": g.bytes_sent.load(Ordering::Relaxed), "send_errors": g.send_errors.load(Ordering::Relaxed),
                "renames": g.renames.load(Ordering::Relaxed),
            },
            "intent_hosts": self.intents.host_count(),
            "swim": swim,
            "blobs": blobs.0,
            "blob_bytes": blobs.1,
            "blobs_unreferenced": blobs.2,
            "blob_gc": blobs.3,
            "rss_mb": rss_mb(),
            "connections_open": self.conns_open.load(Ordering::Relaxed),
            "v2": {
                "kinds": kinds,
                "resync": v2.resync.load(Ordering::Relaxed),
                "unauthorized": v2.unauthorized.load(Ordering::Relaxed),
                "handler_ms": { "count": h2.len(), "p50": q(&h2, 0.5), "p99": q(&h2, 0.99), "max": h2.max() as f64 / 1000.0 },
                "change_lag_ms": { "count": lag.len(), "p50": q(&lag, 0.5), "p99": q(&lag, 0.99), "max": lag.max() as f64 / 1000.0 },
            },
            "uptime_s": self.started.elapsed().as_secs_f64(),
            "hosts": self.cache.hosts.len(),
            "connections": self.conns.load(Ordering::Relaxed),
            "tls_errors": self.tls_errors.load(Ordering::Relaxed),
            "messages": c.messages.load(Ordering::Relaxed),
            "ok": c.ok.load(Ordering::Relaxed),
            "bad_metadata": c.bad.load(Ordering::Relaxed),
            "failed": c.failed.load(Ordering::Relaxed),
            "kicks": c.kicks.load(Ordering::Relaxed),
            "ack_requests": c.ack_requests.load(Ordering::Relaxed),
            "health_updates": c.health_updates.load(Ordering::Relaxed),
            "not_up_to_date": self.cache.not_up_to_date.load(Ordering::Relaxed),
            "failed_daemons": self.cache.failed.lock().unwrap().values().map(|v| v.len()).sum::<usize>(),
            "handler_ms": { "count": h.len(), "p50": q(&h, 0.5), "p99": q(&h, 0.99), "max": h.max() as f64 / 1000.0 },
            "persist": {
                "mode": format!("{:?}", self.persist.mode),
                "writes": self.persist.writes.load(Ordering::Relaxed),
                "skipped": self.persist.skipped.load(Ordering::Relaxed),
                "errors": self.persist.errors.load(Ordering::Relaxed),
                "queued": self.persist.queued.load(Ordering::Relaxed),
                "kv_set_ms": { "count": p.len(), "p50": q(&p, 0.5), "p99": q(&p, 0.99), "max": p.max() as f64 / 1000.0 },
            },
        })
    }

    fn admin(&self, req: &Request<Incoming>) -> Response<Full<Bytes>> {
        let path = req.uri().path();
        if req.method() == Method::POST && path == "/swim/broadcast" {
            let Some(sw) = &self.swim else { return json_response(StatusCode::NOT_FOUND, &json!({})) };
            let version = sw.version.fetch_add(1, Ordering::Relaxed) + 1;
            let sent_ns = proto::swim::wall_ns();
            let data = proto::swim::encode_signal(&proto::swim::Signal { version, sent_ns });
            let _ = sw.ctrl.send(proto::swim::Ctrl::Broadcast(data));
            return json_response(StatusCode::OK, &json!({ "version": version, "sent_ns": sent_ns }));
        }
        if req.method() != Method::GET {
            return json_response(StatusCode::METHOD_NOT_ALLOWED, &json!({}));
        }
        match path {
            "/swim/down" => match &self.swim {
                Some(sw) => {
                    let st = sw.state.lock().unwrap();
                    let m: serde_json::Map<String, Value> = st.down_ns.iter().map(|(k, v)| (k.to_string(), json!(v))).collect();
                    json_response(StatusCode::OK, &Value::Object(m))
                }
                None => json_response(StatusCode::NOT_FOUND, &json!({})),
            },
            "/stats" => json_response(StatusCode::OK, &self.stats()),
            "/hosts" => {
                let mut v: Vec<Value> = self
                    .cache
                    .hosts
                    .values()
                    .map(|h| {
                        let h = h.lock().unwrap();
                        json!({"hostname": h.name, "addr": h.addr, "daemons": h.daemons.len(),
                               "last_daemon_update": h.last_daemon_update})
                    })
                    .collect();
                v.sort_by(|a, b| a["hostname"].as_str().cmp(&b["hostname"].as_str()));
                json_response(StatusCode::OK, &Value::Array(v))
            }
            "/health" => {
                let f = self.cache.failed.lock().unwrap();
                let detail: Vec<&String> = f.values().flatten().collect();
                json_response(StatusCode::OK, &json!({"CEPHADM_FAILED_DAEMON": {"count": detail.len(), "detail": detail}}))
            }
            p if p.starts_with("/host/") => match self.cache.hosts.get(&p[6..]) {
                Some(h) => {
                    let h = h.lock().unwrap();
                    json_response(StatusCode::OK, &json!(h.daemons.values().collect::<Vec<_>>()))
                }
                None => json_response(StatusCode::NOT_FOUND, &json!({})),
            },
            _ => json_response(StatusCode::NOT_FOUND, &json!({})),
        }
    }
}

/// Loads inventory + host/agent records the way the cephadm module does at
/// startup, with parallel config-key gets.
fn load_cache(r: &Arc<rados::Rados>, threads: usize) -> Result<Cache> {
    let fsid: Value = serde_json::from_slice(&r.mon_command(r#"{"prefix":"fsid","format":"json"}"#, b"")?)?;
    let fsid = fsid["fsid"].as_str().context("fsid")?.to_string();
    let inventory: Map<String, Value> = serde_json::from_slice(&r.kv_get("mgr/cephadm/inventory")?)?;
    let keys: Vec<String> = inventory
        .keys()
        .flat_map(|h| {
            [format!("mgr/cephadm/host.{h}"), format!("mgr/cephadm/host.{h}.devices.0"), format!("mgr/cephadm/agent.{h}")]
        })
        .collect();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let records = Mutex::new(Map::new());
    std::thread::scope(|s| {
        for _ in 0..threads {
            s.spawn(|| loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= keys.len() {
                    break;
                }
                if let Ok(v) = r.kv_get(&keys[i]) {
                    records.lock().unwrap().insert(keys[i].clone(), Value::String(String::from_utf8_lossy(&v).into_owned()));
                }
            });
        }
    });
    Ok(Cache::load(&fsid, &inventory, &records.into_inner().unwrap()))
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = args.threads {
        rt.worker_threads(n);
    }
    rt.enable_all().build()?.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let t = Instant::now();
    let r = if args.hosts_file.is_some() && args.persist == Mode::None {
        None
    } else {
        Some(Arc::new(rados::Rados::connect(&args.conf, &args.id)?))
    };
    let cache = Arc::new(match &args.hosts_file {
        Some(path) => {
            let hf = proto::HostsFile::load(path)?;
            let hosts: Vec<(String, String, u16)> = hf.hosts.iter().map(|h| (h.name.clone(), h.keyring.clone(), h.port)).collect();
            Cache::from_hosts(&hf.fsid, &hosts)
        }
        None => {
            let r = r.clone().context("rados")?;
            let n = args.load_threads;
            tokio::task::spawn_blocking(move || load_cache(&r, n)).await??
        }
    });
    eprintln!("agentd: loaded {} hosts in {:.1}s", cache.hosts.len(), t.elapsed().as_secs_f64());

    let (key, crt) = match args.key {
        KeyType::Rsa => ("endpoint-rsa.key", "endpoint-rsa.crt"),
        KeyType::Ec => ("endpoint-ec.key", "endpoint-ec.crt"),
    };
    let ca = args.cert_dir.join("root_ca.pem");
    let acceptor = TlsAcceptor::from(tls::server_config_optional_mtls(&args.cert_dir.join(crt), &args.cert_dir.join(key), &ca)?);
    let ca_pem = std::fs::read_to_string(&ca)?;

    // mgr->agent client: verify the agent against the cephadm root CA and
    // present a client cert it signed.
    let mut roots = rustls::RootCertStore::empty();
    for c in tls::load_certs(&args.cert_dir.join("root_ca.pem"))? {
        roots.add(c)?;
    }
    let agent_tls = rustls::ClientConfig::builder_with_provider(tls::provider())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_client_auth_cert(
            tls::load_certs(&args.cert_dir.join("mgr-client.crt"))?,
            tls::load_key(&args.cert_dir.join("mgr-client.key"))?,
        )?;

    let (persist, rx) = Persister::new(r.clone(), args.persist, args.policy, Duration::from_secs(args.checkpoint_secs), args.max_inflight);
    if args.persist == Mode::Writeback {
        tokio::spawn(persist.clone().flusher(cache.clone(), rx, Duration::from_millis(args.flush_ms)));
    }
    let swim = if args.swim {
        use proto::swim::{self, foca::OwnedNotification, sim_id, Node, SignalHandler, Traffic, CONTROLLER};
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let traffic = Arc::new(Traffic::default());
        let state = Arc::new(Mutex::new(SwimState::default()));
        let st = state.clone();
        let node = Node { id: sim_id(CONTROLLER), cluster_size: cache.hosts.len() as u32 + 1, seeds: vec![], seed: 42, direct: None, view: None, lifeguard: false };
        tokio::spawn(swim::run(node, rx, traffic.clone(), SignalHandler::new(|_| {}), move |n| {
            let mut s = st.lock().unwrap();
            match n {
                OwnedNotification::MemberUp(id) => {
                    s.up.insert(id.idx);
                    s.up_events += 1;
                }
                OwnedNotification::MemberDown(id) => {
                    s.up.remove(&id.idx);
                    s.down_ns.insert(id.idx, swim::wall_ns());
                    s.down_events += 1;
                }
                OwnedNotification::Rename(_, _) => s.renames += 1,
                _ => {}
            }
        }));
        eprintln!("agentd: SWIM controller at {}", swim::sockaddr(CONTROLLER));
        Some(Swim { state, traffic, ctrl: tx, version: Default::default() })
    } else {
        None
    };
    let intents = Arc::new(intents::Intents::default());
    {
        let (i, grace) = (intents.clone(), Duration::from_secs(args.blob_grace_s));
        tokio::spawn(async move {
            let mut t = tokio::time::interval((grace / 4).clamp(Duration::from_secs(1), Duration::from_secs(60)));
            loop {
                t.tick().await;
                let st = i.gc(grace);
                if st.removed > 0 {
                    // glibc keeps freed memory for reuse; give a large
                    // sweep's back to the system
                    let trim_ms = if st.removed_bytes > 16 << 20 { release_free_memory() } else { 0.0 };
                    eprintln!(
                        "blob gc: dropped {} unreferenced blobs ({} bytes) in {:.1} ms, trim {trim_ms:.1} ms",
                        st.removed, st.removed_bytes, st.last_ms
                    );
                }
            }
        });
    }
    let gossip_traffic = Arc::new(proto::swim::Traffic::default());
    if let Some(bind) = args.swim_bind {
        intents.swim.store(true, Ordering::Relaxed);
        use proto::swim::{self, foca::OwnedNotification, Gossip, GossipHandler, Id, Node, CONTROLLER};
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (i1, i2) = (intents.clone(), intents.clone());
        // digests come straight from each agent (older agents gossip them)
        let i0 = intents.clone();
        let direct = Box::new(move |data: &[u8]| {
            if let Some(Gossip::Digest(d)) = swim::decode_gossip(data) {
                i0.digest(d);
            }
        });
        let handler = GossipHandler::new(move |g| {
            if let Gossip::Digest(d) = g {
                i1.digest(*d);
            }
        });
        let node = Node {
            id: Id { idx: CONTROLLER, addr: bind, bump: 0 },
            cluster_size: args.swim_cluster_size,
            seeds: vec![],
            seed: proto::swim::wall_ns(),
            direct: Some(direct),
            view: None,
            lifeguard: args.swim_lifeguard,
        };
        tokio::spawn(swim::run(node, rx, gossip_traffic.clone(), handler, move |n| match n {
            OwnedNotification::MemberUp(id) => i2.liveness(id.idx, true),
            OwnedNotification::MemberDown(id) => i2.liveness(id.idx, false),
            _ => {}
        }));
        // digests ahead of the last status: wake the host's long-poll and
        // poke it over gossip too, in case that connection is gone
        let i3 = intents.clone();
        tokio::spawn(async move {
            let mut t = tokio::time::interval(Duration::from_secs(1));
            loop {
                t.tick().await;
                let stale = i3.resync_stale(Duration::from_secs(1));
                if !stale.is_empty() {
                    let g = Gossip::Poke { seq: proto::swim::wall_ns() / 1_000_000, hosts: stale };
                    let _ = tx.send(proto::swim::Ctrl::Broadcast(proto::swim::encode_gossip(&g)));
                }
            }
        });
        eprintln!("agentd: SWIM member at {bind}");
    }
    let app = Arc::new(App {
        gossip_traffic,
        ca_pem,
        swim,
        intents,
        cache,
        persist,
        agent_tls: TlsConnector::from(Arc::new(agent_tls)),
        handler_us: Mutex::new(hist()),
        v2_handler_us: Mutex::new(hist()),
        conns: Default::default(),
        conns_open: Default::default(),
        tls_errors: Default::default(),
        started: Instant::now(),
    });

    let admin = TcpListener::bind(args.admin).await?;
    let aapp = app.clone();
    tokio::spawn(async move {
        loop {
            let Ok((s, _)) = admin.accept().await else { continue };
            let app = aapp.clone();
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req| {
                    let app = app.clone();
                    async move { Ok::<_, Infallible>(app.admin_req(req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(s), svc).await;
            });
        }
    });

    // every agent reconnects at once after a restart: tokio's default
    // backlog (1024) drops SYNs, and the kernel retries them after 1 s, 3 s
    let l = {
        let sock = if args.listen.is_ipv4() { tokio::net::TcpSocket::new_v4()? } else { tokio::net::TcpSocket::new_v6()? };
        sock.set_reuseaddr(true)?;
        sock.bind(args.listen)?;
        sock.listen(8192)?
    };
    eprintln!(
        "agentd: serving {} (key {:?}, persist {:?}/{:?}), admin {}",
        args.listen, args.key, args.persist, args.policy, args.admin
    );
    loop {
        let (s, _) = l.accept().await?;
        let _ = s.set_nodelay(true);
        let (app, acceptor) = (app.clone(), acceptor.clone());
        tokio::spawn(async move {
            app.conns.fetch_add(1, Ordering::Relaxed);
            let tls = match tokio::time::timeout(Duration::from_secs(30), acceptor.accept(s)).await {
                Ok(Ok(t)) => t,
                _ => {
                    app.tls_errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            app.conns_open.fetch_add(1, Ordering::Relaxed);
            // the host a verified client certificate names
            let peer: Option<Arc<str>> = tls.get_ref().1.peer_certificates().and_then(|c| c.first()).and_then(|der| {
                let (_, cert) = x509_parser::parse_x509_certificate(der.as_ref()).ok()?;
                let cn = cert.subject().iter_common_name().next()?.as_str().ok()?;
                Some(Arc::from(cn))
            });
            // v2: the host this connection authenticated as
            let bound = Arc::new(Mutex::new(None::<String>));
            let app2 = app.clone();
            let svc = hyper::service::service_fn(move |req: Request<Incoming>| {
                let (app, bound, peer) = (app2.clone(), bound.clone(), peer.clone());
                async move {
                    let path = req.uri().path().to_string();
                    Ok::<_, Infallible>(match (req.method() == Method::POST, path.as_str()) {
                        (true, "/data") => app.data(req).await,
                        (true, "/v2/report") => app.v2(req, &bound).await,
                        (_, p) if p.starts_with("/v1/agent/") => app.agent_req(req, peer.as_deref()).await,
                        (_, p) if p.starts_with("/v1/join") => app.join_req(req).await,
                        _ => json_response(StatusCode::NOT_FOUND, &json!({})),
                    })
                }
            });
            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tls), svc).await;
            app.conns_open.fetch_sub(1, Ordering::Relaxed);
        });
    }
}
