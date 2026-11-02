//! v2 agents: one persistent TLS connection each (HTTP/1.1 keep-alive),
//! keyring sent once in a full snapshot, then only what changed.
//!
//! Each simulated host changes on a Poisson schedule (`--change-rate`
//! changes per host per period: mostly daemon restarts, sometimes a daemon
//! failing or recovering) and reports the change at once with the time it
//! happened, so agentd can measure how stale its view was. Usage samples go
//! every `--usage-every` periods; a heartbeat goes when nothing else has
//! been sent for a period. A 409 from agentd (version mismatch) or any
//! connection error makes the agent send a full snapshot again.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper::Request;
use hyper_util::rt::TokioIo;
use proto::AgentState;
use serde_json::{json, Value};
use tokio::net::TcpSocket;

use crate::{bind_src, hist, Ctx, Outcome, Response};

pub const KINDS: [&str; 4] = ["full", "delta", "usage", "heartbeat"];

pub struct KindStat {
    ok: u64,
    err: u64,
    bytes: u64,
    lat: Histogram<u64>,
}

pub struct V2Stats {
    measure_from: Instant,
    measure_to: Instant,
    kinds: BTreeMap<&'static str, KindStat>,
    connects: u64,
    connect_errors: u64,
    resyncs: u64,
}

impl V2Stats {
    pub fn new(measure_from: Instant, measure_to: Instant) -> Self {
        let kinds = KINDS
            .iter()
            .map(|k| (*k, KindStat { ok: 0, err: 0, bytes: 0, lat: hist() }))
            .collect();
        V2Stats { measure_from, measure_to, kinds, connects: 0, connect_errors: 0, resyncs: 0 }
    }

    fn in_window(&self, t: Instant) -> bool {
        t >= self.measure_from && t < self.measure_to
    }

    fn record(&mut self, kind: &'static str, start: Instant, ok: bool, bytes: usize) {
        if !self.in_window(start) {
            return;
        }
        let k = self.kinds.get_mut(kind).unwrap();
        if ok {
            k.ok += 1;
            k.bytes += bytes as u64;
            k.lat.saturating_record(start.elapsed().as_micros().max(1) as u64);
        } else {
            k.err += 1;
        }
    }

    pub fn summary(&self, window: f64, agents: usize, period: f64) -> Value {
        let q = |h: &Histogram<u64>, x: f64| if h.is_empty() { 0.0 } else { h.value_at_quantile(x) as f64 / 1000.0 };
        let mut kinds = serde_json::Map::new();
        let (mut reqs, mut bytes) = (0u64, 0u64);
        for (name, k) in &self.kinds {
            reqs += k.ok;
            bytes += k.bytes;
            kinds.insert(
                name.to_string(),
                json!({
                    "per_sec": k.ok as f64 / window, "errors": k.err,
                    "bytes_per_sec": k.bytes as f64 / window,
                    "avg_bytes": if k.ok > 0 { k.bytes / k.ok } else { 0 },
                    "p50_ms": q(&k.lat, 0.5), "p99_ms": q(&k.lat, 0.99), "max_ms": k.lat.max() as f64 / 1000.0,
                }),
            );
        }
        json!({
            "kinds": kinds,
            "requests_per_sec": reqs as f64 / window,
            "request_bytes_per_sec": bytes as f64 / window,
            "bytes_per_agent_per_period": bytes as f64 / window * period / agents as f64,
            "connects": self.connects,
            "connect_errors": self.connect_errors,
            "resyncs": self.resyncs,
        })
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut x = self.0;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^ (x >> 31)
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Exponential inter-arrival time for `rate` events per second.
    fn exp(&mut self, rate: f64) -> Duration {
        Duration::from_secs_f64(-(1.0 - self.unit()).ln() / rate)
    }
}

/// Wall-clock nanoseconds of an Instant in the past (or now).
fn wall_ns(t: Instant) -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    now.saturating_sub(Instant::now().saturating_duration_since(t).as_nanos() as u64)
}

async fn connect(ctx: &Ctx) -> Result<SendRequest<Full<Bytes>>> {
    let sock = TcpSocket::new_v4()?;
    bind_src(ctx, &sock);
    let tcp = sock.connect(ctx.target).await?;
    tcp.set_nodelay(true)?;
    let tls = ctx.connector.connect(ctx.server_name.clone(), tcp).await?;
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(sender)
}

async fn send(s: &mut SendRequest<Full<Bytes>>, ctx: &Ctx, body: Vec<u8>) -> Result<u16> {
    s.ready().await?;
    let req = Request::post("/v2/report")
        .header(hyper::header::HOST, ctx.target.to_string())
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body)))?;
    let resp = s.send_request(req).await?;
    let code = resp.status().as_u16();
    resp.into_body().collect().await?;
    Ok(code)
}

pub async fn agent(ctx: Arc<Ctx>, idx: usize) {
    let a = &ctx.args;
    let period = a.period;
    let mut rng = Rng(crate::proto_hash(idx) | 1);
    tokio::time::sleep(Duration::from_secs_f64(period * rng.unit())).await;
    let host = &ctx.hosts.hosts[idx];
    let mut st = AgentState::new(ctx.gen.daemon_names(idx, &host.name).len());
    let rate = a.change_rate / period;
    let usage_period = Duration::from_secs_f64(a.usage_every * period);
    let far = Duration::from_secs(10 * 365 * 86400);
    let now = Instant::now();
    let mut next_change = now + if rate > 0.0 { rng.exp(rate) } else { far };
    let mut next_usage = now + usage_period.mul_f64(rng.unit());
    let mut next_hb = now + Duration::from_secs_f64(period);
    let mut sender: Option<SendRequest<Full<Bytes>>> = None;
    let mut need_full = true;
    let mut seq = 0u64;
    let timeout = Duration::from_secs_f64(a.timeout);

    while ctx.running() {
        if sender.as_ref().map_or(true, |s| s.is_closed()) {
            match tokio::time::timeout(timeout, connect(&ctx)).await {
                Ok(Ok(s)) => {
                    sender = Some(s);
                    need_full = true;
                    ctx.v2.lock().unwrap().connects += 1;
                }
                _ => {
                    ctx.v2.lock().unwrap().connect_errors += 1;
                    tokio::time::sleep(Duration::from_secs_f64(1.0 + rng.unit())).await;
                    continue;
                }
            }
        }
        let (kind, body) = if need_full {
            ("full", ctx.gen.v2_full(idx, host, &st, 1, seq))
        } else {
            let wake = next_change.min(next_usage).min(next_hb);
            tokio::time::sleep_until(wake.into()).await;
            if !ctx.running() {
                break;
            }
            let now = Instant::now();
            if now >= next_change {
                let changed_at = wall_ns(next_change);
                let k = st.mutate(rng.next());
                next_change += rng.exp(rate);
                ("delta", ctx.gen.v2_delta(idx, host, &st, k, seq, changed_at))
            } else if now >= next_usage {
                next_usage += usage_period;
                ("usage", ctx.gen.v2_usage(idx, host, &st, seq))
            } else {
                ("heartbeat", ctx.gen.v2_heartbeat(host, &st))
            }
        };
        seq += 1;
        let len = body.len();
        let start = Instant::now();
        ctx.stats.lock().unwrap().started(start);
        let res = tokio::time::timeout(timeout, send(sender.as_mut().unwrap(), &ctx, body)).await;
        let (outcome, detail) = match res {
            Ok(Ok(200)) => {
                if kind == "full" {
                    need_full = false;
                }
                (Outcome::Ok, String::new())
            }
            Ok(Ok(409)) => {
                need_full = true;
                ctx.v2.lock().unwrap().resyncs += 1;
                (Outcome::Ok, String::new())
            }
            Ok(Ok(code)) => {
                need_full = true;
                sender = None;
                (Outcome::HttpErr, format!("HTTP {code}"))
            }
            Ok(Err(e)) => {
                need_full = true;
                sender = None;
                (Outcome::ConnErr, format!("{e}"))
            }
            Err(_) => {
                need_full = true;
                sender = None;
                (Outcome::Timeout, format!("no response in {}s", a.timeout))
            }
        };
        let ok = outcome == Outcome::Ok;
        ctx.v2.lock().unwrap().record(kind, start, ok, len);
        let r = Response { outcome, connect_us: 0, tls_us: 0, resp_us: start.elapsed().as_micros() as u64, detail };
        ctx.stats.lock().unwrap().record(idx, start, &r);
        next_hb = Instant::now() + Duration::from_secs_f64(period);
    }
}
