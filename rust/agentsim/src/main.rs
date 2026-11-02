//! agentsim: simulates many cephadm agents reporting to an agent endpoint
//! (the cephadm mgr module's /data endpoint, or agentd).
//!
//! Each simulated agent behaves like CephadmAgent.run(): it opens a new TLS
//! connection, POSTs its metadata, waits up to 10s for the response, then
//! sleeps `period - int(avg(last 3 iteration times))`. A listener per agent
//! (--listen) accepts mgr->agent counter updates like MgrListener and wakes
//! the agent so it reports with the new ack.
//!
//! --saturate C replaces the agent loops with C closed-loop senders cycling
//! through the hosts, to find peak throughput.
//!
//! --proto v2 runs v2 agents instead (see v2.rs): persistent connections and
//! versioned deltas, for agentd's /v2/report.

mod v2;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use proto::{tls, HostProfile, HostsFile, PayloadGen};
use rustls::pki_types::ServerName;
use serde::Serialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket};
use tokio::sync::Notify;
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Parser, Debug, Clone, Serialize)]
struct Args {
    /// hosts.json written by cluster/seed.py
    #[arg(long, default_value = "/mon/vcluster/agents/hosts.json")]
    hosts_file: PathBuf,
    /// Endpoint host:port (default: the one in the hosts file)
    #[arg(long)]
    target: Option<String>,
    #[arg(long, default_value = "/data")]
    path: String,
    /// Number of agents to simulate (default: every host in the file)
    #[arg(long)]
    agents: Option<usize>,
    /// Agent refresh period in seconds (mgr/cephadm/agent_refresh_rate)
    #[arg(long, default_value_t = 20.0)]
    period: f64,
    /// Measured seconds, after warmup
    #[arg(long, default_value_t = 120.0)]
    duration: f64,
    /// Seconds of load before measurement starts (default: one period)
    #[arg(long)]
    warmup: Option<f64>,
    /// Closed-loop peak-throughput mode with this many concurrent senders
    #[arg(long)]
    saturate: Option<usize>,
    /// Per-request timeout in seconds (the agent's urlopen timeout)
    #[arg(long, default_value_t = 10.0)]
    timeout: f64,
    /// OSDs (and data devices) per simulated host
    #[arg(long, default_value_t = 16)]
    osds: usize,
    /// Report one daemon in error state on every Nth message of each agent
    #[arg(long, default_value_t = 0)]
    error_every: u64,
    /// Before measuring, send one report per agent with this concurrency
    #[arg(long, default_value_t = 0)]
    prime: usize,
    /// Run a mgr->agent listener for each agent (needs listener.crt/key)
    #[arg(long)]
    listen: bool,
    /// Spread client connections over this many 127.0.1.x source addresses
    /// so TIME_WAIT does not exhaust ephemeral ports on loopback
    #[arg(long, default_value_t = 16)]
    src_ips: u8,
    #[arg(long, default_value_t = 8)]
    threads: usize,
    /// Output directory for summary.json and timeseries.csv
    #[arg(long)]
    out: PathBuf,
    /// Free-form label stored in the summary
    #[arg(long, default_value = "")]
    label: String,
    /// Write one report body for the first host to this file and exit
    #[arg(long)]
    dump_body: Option<PathBuf>,
    /// v1: a full report on a new connection every period (today's agent).
    /// v2: persistent connection, deltas on change (agentd only).
    #[arg(long, default_value = "v1")]
    proto: String,
    /// v2: material changes per host per period (Poisson)
    #[arg(long, default_value_t = 0.01)]
    change_rate: f64,
    /// v2: send daemon usage samples every this many periods
    #[arg(long, default_value_t = 15.0)]
    usage_every: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    AppErr,
    HttpErr,
    Timeout,
    ConnErr,
}
const OUTCOMES: [&str; 5] = ["ok", "app_err", "http_err", "timeout", "conn_err"];

struct Bucket {
    started: u64,
    done: [u64; 5],
    lat: Histogram<u64>,
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()
}

struct Stats {
    t0: Instant,
    measure_from: Instant,
    measure_to: Instant,
    buckets: Vec<Bucket>,
    counts: [u64; 5],
    total: Histogram<u64>,
    connect: Histogram<u64>,
    handshake: Histogram<u64>,
    response: Histogram<u64>,
    errors: Vec<String>,
    ok_times: Vec<Vec<f64>>,
    listener_msgs: u64,
}

impl Stats {
    fn bucket(&mut self, t: Instant) -> &mut Bucket {
        let i = t.saturating_duration_since(self.t0).as_secs() as usize;
        while self.buckets.len() <= i {
            self.buckets.push(Bucket { started: 0, done: [0; 5], lat: hist() });
        }
        &mut self.buckets[i]
    }

    fn started(&mut self, t: Instant) {
        self.bucket(t).started += 1;
    }

    fn record(&mut self, agent: usize, start: Instant, r: &Response) {
        let end = Instant::now();
        let total_us = end.duration_since(start).as_micros().max(1) as u64;
        let o = r.outcome as usize;
        {
            let b = self.bucket(end);
            b.done[o] += 1;
            if r.outcome == Outcome::Ok {
                b.lat.saturating_record(total_us);
            }
        }
        if start >= self.measure_from && start < self.measure_to {
            self.counts[o] += 1;
            if r.outcome == Outcome::Ok {
                self.total.saturating_record(total_us);
                self.connect.saturating_record(r.connect_us.max(1));
                self.handshake.saturating_record(r.tls_us.max(1));
                self.response.saturating_record(r.resp_us.max(1));
            }
        }
        if r.outcome == Outcome::Ok {
            self.ok_times[agent].push(end.duration_since(self.t0).as_secs_f64());
        } else if self.errors.len() < 20 {
            let msg = format!("{:?}: {}", r.outcome, r.detail);
            if !self.errors.contains(&msg) {
                self.errors.push(msg);
            }
        }
    }
}

struct Response {
    outcome: Outcome,
    connect_us: u64,
    tls_us: u64,
    resp_us: u64,
    detail: String,
}

impl Response {
    fn fail(outcome: Outcome, detail: String) -> Self {
        Response { outcome, connect_us: 0, tls_us: 0, resp_us: 0, detail }
    }
}

struct Ctx {
    args: Args,
    hosts: HostsFile,
    n: usize,
    target: SocketAddr,
    server_name: ServerName<'static>,
    connector: TlsConnector,
    gen: PayloadGen,
    acks: Vec<AtomicU64>,
    seqs: Vec<AtomicU64>,
    wake: Vec<Notify>,
    stats: Mutex<Stats>,
    v2: Mutex<v2::V2Stats>,
    base: Instant,
    end_ns: AtomicU64,
    src_next: AtomicUsize,
}

impl Ctx {
    fn running(&self) -> bool {
        (self.base.elapsed().as_nanos() as u64) < self.end_ns.load(Ordering::Relaxed)
    }
}

fn new_stats(t0: Instant, warmup: f64, end: Instant, n: usize) -> Stats {
    Stats {
        t0,
        measure_from: t0 + Duration::from_secs_f64(warmup),
        measure_to: end,
        buckets: Vec::new(),
        counts: [0; 5],
        total: hist(),
        connect: hist(),
        handshake: hist(),
        response: hist(),
        errors: Vec::new(),
        ok_times: vec![Vec::new(); n],
        listener_msgs: 0,
    }
}

/// Parses a complete HTTP response from `buf`, or None if more bytes are
/// needed (`eof` forces completion for bodies without Content-Length).
fn parse_response(buf: &[u8], eof: bool) -> Option<(u16, Vec<u8>)> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut resp = httparse::Response::new(&mut headers);
    let hlen = match resp.parse(buf) {
        Ok(httparse::Status::Complete(n)) => n,
        _ => return None,
    };
    let code = resp.code.unwrap_or(0);
    let cl = resp
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .and_then(|h| std::str::from_utf8(h.value).ok()?.trim().parse::<usize>().ok());
    match cl {
        Some(cl) if buf.len() >= hlen + cl => Some((code, buf[hlen..hlen + cl].to_vec())),
        None if eof => Some((code, buf[hlen..].to_vec())),
        _ => None,
    }
}

/// Spread client connections over 127.0.1.x source addresses so TIME_WAIT
/// sockets do not exhaust ephemeral ports on loopback.
fn bind_src(ctx: &Ctx, sock: &TcpSocket) {
    if ctx.args.src_ips > 0 && ctx.target.ip().is_loopback() {
        let i = (ctx.src_next.fetch_add(1, Ordering::Relaxed) % ctx.args.src_ips as usize) as u8;
        let _ = sock.bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 1, 1 + i)), 0));
    }
}

async fn post(ctx: &Ctx, body: &[u8]) -> Response {
    let t0 = Instant::now();
    let sock = match TcpSocket::new_v4() {
        Ok(s) => s,
        Err(e) => return Response::fail(Outcome::ConnErr, format!("socket: {e}")),
    };
    bind_src(ctx, &sock);
    let tcp = match sock.connect(ctx.target).await {
        Ok(s) => s,
        Err(e) => return Response::fail(Outcome::ConnErr, format!("connect: {e}")),
    };
    let _ = tcp.set_nodelay(true);
    let t1 = Instant::now();
    let mut tls = match ctx.connector.connect(ctx.server_name.clone(), tcp).await {
        Ok(s) => s,
        Err(e) => return Response::fail(Outcome::ConnErr, format!("tls: {e}")),
    };
    let t2 = Instant::now();
    let head = format!(
        "POST {} HTTP/1.1\r\nAccept-Encoding: identity\r\nContent-Type: application/json\r\nContent-Length: {}\r\nHost: {}\r\nUser-Agent: Python-urllib/3.12\r\nConnection: close\r\n\r\n",
        ctx.args.path,
        body.len(),
        ctx.target
    );
    if let Err(e) = async {
        tls.write_all(head.as_bytes()).await?;
        tls.write_all(body).await?;
        tls.flush().await
    }
    .await
    {
        return Response::fail(Outcome::ConnErr, format!("write: {e}"));
    }
    let mut buf = Vec::with_capacity(1024);
    let parsed = loop {
        match tls.read_buf(&mut buf).await {
            Ok(0) => break parse_response(&buf, true),
            Ok(_) => {
                if let Some(r) = parse_response(&buf, false) {
                    break Some(r);
                }
            }
            Err(e) => match parse_response(&buf, true) {
                Some(r) => break Some(r),
                None => return Response::fail(Outcome::ConnErr, format!("read: {e}")),
            },
        }
    };
    let t3 = Instant::now();
    let (code, rbody) = match parsed {
        Some(r) => r,
        None => return Response::fail(Outcome::ConnErr, "truncated response".into()),
    };
    let us = |a: Instant, b: Instant| b.duration_since(a).as_micros() as u64;
    let mut r = Response {
        outcome: Outcome::Ok,
        connect_us: us(t0, t1),
        tls_us: us(t1, t2),
        resp_us: us(t2, t3),
        detail: String::new(),
    };
    if code != 200 {
        r.outcome = Outcome::HttpErr;
        r.detail = format!("HTTP {code}: {}", String::from_utf8_lossy(&rbody).chars().take(200).collect::<String>());
        return r;
    }
    let result = serde_json::from_slice::<serde_json::Value>(&rbody)
        .ok()
        .and_then(|v| v.get("result").and_then(|s| s.as_str()).map(str::to_string))
        .unwrap_or_else(|| String::from_utf8_lossy(&rbody).into_owned());
    if result != "Successfully processed metadata." {
        r.outcome = Outcome::AppErr;
        r.detail = result.chars().take(200).collect();
    }
    r
}

async fn send_one(ctx: &Ctx, idx: usize) -> Outcome {
    let seq = ctx.seqs[idx].fetch_add(1, Ordering::Relaxed);
    let err = if ctx.args.error_every > 0 && seq % ctx.args.error_every == ctx.args.error_every - 1 {
        Some(4)
    } else {
        None
    };
    let body = ctx.gen.body(idx, &ctx.hosts.hosts[idx], ctx.acks[idx].load(Ordering::Relaxed), seq, err);
    let start = Instant::now();
    ctx.stats.lock().unwrap().started(start);
    let r = match tokio::time::timeout(Duration::from_secs_f64(ctx.args.timeout), post(ctx, &body)).await {
        Ok(r) => r,
        Err(_) => Response::fail(Outcome::Timeout, format!("no response in {}s", ctx.args.timeout)),
    };
    ctx.stats.lock().unwrap().record(idx, start, &r);
    r.outcome
}

async fn agent_loop(ctx: Arc<Ctx>, idx: usize) {
    // Spread first reports uniformly over one period.
    let offset = ctx.args.period * (proto_hash(idx) % 10_000) as f64 / 10_000.0;
    tokio::time::sleep(Duration::from_secs_f64(offset)).await;
    let mut recent = [0f64; 3];
    let mut ri = 0;
    while ctx.running() {
        let start = Instant::now();
        send_one(&ctx, idx).await;
        recent[ri] = start.elapsed().as_secs_f64();
        ri = (ri + 1) % 3;
        let nz = recent.iter().filter(|t| **t > 0.0).count().max(1);
        let avg = recent.iter().sum::<f64>() / nz as f64;
        let wait = (ctx.args.period.floor() - avg.floor()).max(0.0);
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs_f64(wait)) => {}
            _ = ctx.wake[idx].notified() => {}
        }
    }
}

fn proto_hash(i: usize) -> u64 {
    let mut x = (i as u64).wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x ^ (x >> 31)
}

async fn saturate_loop(ctx: Arc<Ctx>, next: Arc<AtomicUsize>) {
    while ctx.running() {
        let idx = next.fetch_add(1, Ordering::Relaxed) % ctx.n;
        send_one(&ctx, idx).await;
    }
}

/// MgrListener: 10-byte zero-padded length, JSON {"counter": N[, "config"]},
/// reply "ACK", then report immediately with the new counter.
async fn listener(ctx: Arc<Ctx>, idx: usize, acceptor: TlsAcceptor) -> Result<()> {
    let addr: SocketAddr = format!("{}:{}", ctx.hosts.addr, ctx.hosts.hosts[idx].port).parse()?;
    let l = TcpListener::bind(addr).await.with_context(|| format!("bind {addr}"))?;
    loop {
        let (s, _) = l.accept().await?;
        let acceptor = acceptor.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let Ok(mut s) = acceptor.accept(s).await else { return };
            let mut len = [0u8; 10];
            if s.read_exact(&mut len).await.is_err() {
                return;
            }
            let Some(n) = std::str::from_utf8(&len).ok().and_then(|x| x.trim().parse::<usize>().ok()) else { return };
            let mut payload = vec![0u8; n];
            if s.read_exact(&mut payload).await.is_err() {
                return;
            }
            let Ok(v) = serde_json::from_slice::<serde_json::Value>(&payload) else { return };
            if let Some(c) = v.get("counter").and_then(|c| c.as_u64()) {
                ctx.acks[idx].store(c, Ordering::Relaxed);
                let _ = s.write_all(b"ACK").await;
                let _ = s.shutdown().await;
                ctx.stats.lock().unwrap().listener_msgs += 1;
                ctx.wake[idx].notify_one();
            }
        });
    }
}

fn pct(h: &Histogram<u64>, q: f64) -> f64 {
    if h.is_empty() {
        return 0.0;
    }
    h.value_at_quantile(q) as f64 / 1000.0
}

fn hist_json(h: &Histogram<u64>) -> serde_json::Value {
    json!({
        "count": h.len(), "mean_ms": h.mean() / 1000.0,
        "p50_ms": pct(h, 0.5), "p90_ms": pct(h, 0.9), "p99_ms": pct(h, 0.99),
        "p999_ms": pct(h, 0.999), "max_ms": h.max() as f64 / 1000.0,
    })
}

fn quantile(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * q).round() as usize]
}

fn main() -> Result<()> {
    let args = Args::parse();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    let hosts = HostsFile::load(&args.hosts_file)?;
    let n = args.agents.unwrap_or(hosts.hosts.len());
    if n == 0 || n > hosts.hosts.len() {
        bail!("--agents {n} but hosts file has {}", hosts.hosts.len());
    }
    let target: SocketAddr = args.target.clone().unwrap_or(hosts.endpoint.clone()).parse()?;
    let warmup = args.warmup.unwrap_or(if args.saturate.is_some() { 5.0 } else { args.period });
    std::fs::create_dir_all(&args.out)?;

    let now = Instant::now();
    let ctx = Arc::new(Ctx {
        server_name: ServerName::IpAddress(target.ip().into()),
        connector: TlsConnector::from(tls::agent_client_config()),
        gen: PayloadGen::new(&hosts.fsid, HostProfile { osds: args.osds }),
        acks: (0..n).map(|_| AtomicU64::new(1)).collect(),
        seqs: (0..n).map(|_| AtomicU64::new(0)).collect(),
        wake: (0..n).map(|_| Notify::new()).collect(),
        stats: Mutex::new(new_stats(now, 0.0, now, n)),
        v2: Mutex::new(v2::V2Stats::new(now, now)),
        base: now,
        end_ns: AtomicU64::new(u64::MAX),
        src_next: AtomicUsize::new(0),
        hosts,
        n,
        target,
        args: args.clone(),
    });

    let sample = ctx.gen.body(0, &ctx.hosts.hosts[0], 1, 0, None);
    if let Some(p) = &args.dump_body {
        std::fs::write(p, &sample)?;
        return Ok(());
    }
    eprintln!("agentsim: {n} agents -> {target}{}, body {} bytes", args.path, sample.len());

    if args.listen {
        let dir = args.hosts_file.parent().unwrap();
        let acceptor = TlsAcceptor::from(tls::server_config(&dir.join("listener.crt"), &dir.join("listener.key"))?);
        for idx in 0..n {
            let ctx = ctx.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Err(e) = listener(ctx, idx, acceptor).await {
                    eprintln!("listener {idx}: {e:#}");
                }
            });
        }
    }

    let mut prime_secs = 0.0;
    if args.prime > 0 {
        let t = Instant::now();
        let next = Arc::new(AtomicUsize::new(0));
        let ok = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..args.prime {
            let (ctx, next, ok) = (ctx.clone(), next.clone(), ok.clone());
            tasks.push(tokio::spawn(async move {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= ctx.n {
                        break;
                    }
                    if send_one(&ctx, i).await == Outcome::Ok {
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        for t in tasks {
            t.await?;
        }
        prime_secs = t.elapsed().as_secs_f64();
        eprintln!("primed {} / {n} agents in {prime_secs:.1}s", ok.load(Ordering::Relaxed));
    }

    // Reset stats and set the clock for the measured run.
    let t0 = Instant::now();
    let end = t0 + Duration::from_secs_f64(warmup + args.duration);
    *ctx.stats.lock().unwrap() = new_stats(t0, warmup, end, n);
    *ctx.v2.lock().unwrap() = v2::V2Stats::new(t0 + Duration::from_secs_f64(warmup), end);
    ctx.end_ns.store(end.duration_since(ctx.base).as_nanos() as u64, Ordering::Relaxed);

    let mut tasks = Vec::new();
    if let Some(c) = args.saturate {
        let next = Arc::new(AtomicUsize::new(0));
        for _ in 0..c {
            tasks.push(tokio::spawn(saturate_loop(ctx.clone(), next.clone())));
        }
    } else {
        for idx in 0..n {
            if args.proto == "v2" {
                tasks.push(tokio::spawn(v2::agent(ctx.clone(), idx)));
            } else {
                tasks.push(tokio::spawn(agent_loop(ctx.clone(), idx)));
            }
        }
    }
    // Progress line every 10s.
    let pctx = ctx.clone();
    let progress = tokio::spawn(async move {
        let (mut last_ok, mut last_bad, mut last_t) = (0u64, 0u64, Instant::now());
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let s = pctx.stats.lock().unwrap();
            let ok: u64 = s.buckets.iter().map(|b| b.done[0]).sum();
            let bad: u64 = s.buckets.iter().map(|b| b.done[1..].iter().sum::<u64>()).sum();
            let from = s.buckets.len().saturating_sub(10);
            let mut h = hist();
            for b in &s.buckets[from..] {
                h.add(&b.lat).ok();
            }
            let dt = last_t.elapsed().as_secs_f64();
            eprintln!(
                "t={:>4}s ok/s={:>7.1} err/s={:>6.1} p50={:>8.1}ms p99={:>8.1}ms",
                s.buckets.len(),
                (ok - last_ok) as f64 / dt,
                (bad - last_bad) as f64 / dt,
                pct(&h, 0.5),
                pct(&h, 0.99)
            );
            (last_ok, last_bad, last_t) = (ok, bad, Instant::now());
        }
    });
    for t in tasks {
        t.await?;
    }
    progress.abort();

    let s = ctx.stats.lock().unwrap();
    let window = args.duration;
    let mut gaps: Vec<f64> = Vec::new();
    let (from, to) = (warmup, warmup + args.duration);
    for times in &s.ok_times {
        let mut last = from;
        let mut max_gap = 0f64;
        for &t in times.iter().filter(|&&t| t >= from && t <= to) {
            max_gap = max_gap.max(t - last);
            last = t;
        }
        max_gap = max_gap.max(to - last);
        gaps.push(max_gap);
    }
    let stale = |mult: f64| gaps.iter().filter(|g| **g > mult * args.period).count();
    let summary = json!({
        "label": args.label,
        "args": args,
        "mode": if args.saturate.is_some() { "saturate" } else { "agents" },
        "agents": n,
        "body_bytes": sample.len(),
        "prime_secs": prime_secs,
        "window_secs": window,
        "offered_per_sec": if args.saturate.is_some() { serde_json::Value::Null } else { json!(n as f64 / args.period) },
        "ok_per_sec": s.counts[0] as f64 / window,
        "counts": OUTCOMES.iter().zip(s.counts.iter()).map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
        "latency": hist_json(&s.total),
        "connect": hist_json(&s.connect),
        "tls_handshake": hist_json(&s.handshake),
        "response": hist_json(&s.response),
        "max_report_gap": if args.saturate.is_some() { serde_json::Value::Null } else { json!({
            "p50_s": quantile(&mut gaps.clone(), 0.5),
            "p99_s": quantile(&mut gaps.clone(), 0.99),
            "max_s": quantile(&mut gaps.clone(), 1.0),
            "agents_over_1_5x_period": stale(1.5),
            "agents_over_3x_period": stale(3.0),
        })},
        "listener_msgs": s.listener_msgs,
        "errors": s.errors,
        "v2": if args.proto == "v2" { ctx.v2.lock().unwrap().summary(window, n, args.period) } else { serde_json::Value::Null },
    });
    std::fs::write(args.out.join("summary.json"), serde_json::to_string_pretty(&summary)?)?;
    let mut csv = String::from("t,started,ok,app_err,http_err,timeout,conn_err,p50_ms,p90_ms,p99_ms,max_ms\n");
    for (i, b) in s.buckets.iter().enumerate() {
        csv.push_str(&format!(
            "{i},{},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2}\n",
            b.started,
            b.done[0],
            b.done[1],
            b.done[2],
            b.done[3],
            b.done[4],
            pct(&b.lat, 0.5),
            pct(&b.lat, 0.9),
            pct(&b.lat, 0.99),
            b.lat.max() as f64 / 1000.0
        ));
    }
    std::fs::write(args.out.join("timeseries.csv"), csv)?;
    eprintln!(
        "done: ok/s={:.1} counts={:?} p50={:.1}ms p99={:.1}ms",
        s.counts[0] as f64 / window,
        s.counts,
        pct(&s.total, 0.5),
        pct(&s.total, 0.99)
    );
    Ok(())
}
