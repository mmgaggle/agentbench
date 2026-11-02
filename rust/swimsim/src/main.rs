//! swimsim: N simulated agents as SWIM members (foca), against the SWIM
//! controller in `agentd --swim`.
//!
//! 1. Members join by announcing to the controller, staggered over
//!    --join-secs, and learn the rest of the cluster by gossip.
//! 2. Once the controller sees every member up: steady-state traffic per
//!    member over --steady seconds.
//! 3. --signals controller broadcasts (POST /swim/broadcast): how many
//!    members got each one and how fast.
//! 4. --kill members stop answering at once: how long until the controller
//!    declares each one down, and whether it wrongly declared any live
//!    member down.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use proto::swim::{self, sim_id, wall_ns, Ctrl, Node, SignalHandler, Traffic, CONTROLLER};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Parser, Debug, Clone, serde::Serialize)]
struct Args {
    #[arg(long)]
    agents: u32,
    /// agentd admin address
    #[arg(long, default_value = "127.0.0.1:7161")]
    admin: String,
    #[arg(long, default_value_t = 20.0)]
    join_secs: f64,
    #[arg(long, default_value_t = 300.0)]
    converge_timeout: f64,
    /// Extra settling time after the controller sees every member
    #[arg(long, default_value_t = 30.0)]
    settle: f64,
    #[arg(long, default_value_t = 60.0)]
    steady: f64,
    #[arg(long, default_value_t = 3)]
    signals: u32,
    #[arg(long, default_value_t = 20.0)]
    signal_wait: f64,
    #[arg(long, default_value_t = 50)]
    kill: u32,
    #[arg(long, default_value_t = 90.0)]
    kill_wait: f64,
    #[arg(long, default_value_t = 16)]
    threads: usize,
    #[arg(long)]
    out: PathBuf,
}

/// Minimal HTTP/1.0 client for agentd's admin port.
async fn http(admin: &str, method: &str, path: &str) -> Result<Value> {
    let mut s = tokio::net::TcpStream::connect(admin).await?;
    s.write_all(format!("{method} {path} HTTP/1.0\r\nHost: {admin}\r\nContent-Length: 0\r\n\r\n").as_bytes())
        .await?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf);
    let body = text.split_once("\r\n\r\n").map(|(_, b)| b).context("bad HTTP response")?;
    Ok(serde_json::from_str(body)?)
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * q).round() as usize]
}

fn dist(mut v: Vec<f64>) -> Value {
    json!({"n": v.len(), "p50": pct(&mut v, 0.5), "p90": pct(&mut v, 0.9), "p99": pct(&mut v, 0.99), "max": pct(&mut v, 1.0)})
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
    std::fs::create_dir_all(&args.out)?;
    let n = args.agents;
    let traffic = Arc::new(Traffic::default());
    // first receipt (wall ns) of each signal version, per member
    let max_signals = args.signals as usize + 1;
    let received: Arc<Vec<Vec<AtomicU64>>> =
        Arc::new((0..max_signals).map(|_| (0..n).map(|_| AtomicU64::new(0)).collect()).collect());
    let member_downs = Arc::new(AtomicU64::new(0));
    let mut ctrls = Vec::with_capacity(n as usize);
    let t0 = Instant::now();

    for i in 0..n {
        let (tx, rx) = mpsc::unbounded_channel();
        ctrls.push(tx);
        let (traffic, received, member_downs) = (traffic.clone(), received.clone(), member_downs.clone());
        let delay = Duration::from_secs_f64(args.join_secs * i as f64 / n as f64);
        let node = Node {
            id: sim_id(i),
            cluster_size: n + 1,
            seeds: vec![sim_id(CONTROLLER)],
            seed: 0x5eed ^ i as u64,
            direct: None,
            view: None,
            lifeguard: false,
        };
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let handler = SignalHandler::new(move |sig| {
                if let Some(v) = received.get(sig.version as usize) {
                    v[i as usize].store(wall_ns(), Ordering::Relaxed);
                }
            });
            let res = swim::run(node, rx, traffic, handler, move |note| {
                if matches!(note, swim::foca::OwnedNotification::Defunct) {
                    member_downs.fetch_add(1, Ordering::Relaxed);
                }
            })
            .await;
            if let Err(e) = res {
                eprintln!("member {i}: {e:#}");
            }
        });
    }

    // Wait until the controller sees every member.
    let converged = loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let up = http(&args.admin, "GET", "/stats").await.ok().and_then(|s| s["swim"]["up"].as_u64()).unwrap_or(0);
        eprintln!("t={:>5.0}s controller sees {up}/{n} members up", t0.elapsed().as_secs_f64());
        if up >= n as u64 {
            break t0.elapsed().as_secs_f64();
        }
        if t0.elapsed().as_secs_f64() > args.converge_timeout {
            bail!("controller saw only {up}/{n} members after {}s", args.converge_timeout);
        }
    };
    tokio::time::sleep(Duration::from_secs_f64(args.settle)).await;

    // Steady state: traffic per member and at the controller.
    let snap = |t: &Traffic| {
        (t.pkts_sent.load(Ordering::Relaxed), t.bytes_sent.load(Ordering::Relaxed), t.pkts_recv.load(Ordering::Relaxed))
    };
    let (s0, c0) = (snap(&traffic), http(&args.admin, "GET", "/stats").await?);
    tokio::time::sleep(Duration::from_secs_f64(args.steady)).await;
    let (s1, c1) = (snap(&traffic), http(&args.admin, "GET", "/stats").await?);
    let secs = args.steady;
    let per_member = |a: u64, b: u64| (b - a) as f64 / secs / n as f64;
    let ctl = |k: &str| (c1["swim"][k].as_u64().unwrap_or(0) - c0["swim"][k].as_u64().unwrap_or(0)) as f64 / secs;
    let steady = json!({
        "member_pkts_sent_per_sec": per_member(s0.0, s1.0),
        "member_bytes_sent_per_sec": per_member(s0.1, s1.1),
        "member_pkts_recv_per_sec": per_member(s0.2, s1.2),
        "controller_pkts_sent_per_sec": ctl("pkts_sent"),
        "controller_pkts_recv_per_sec": ctl("pkts_recv"),
        "controller_bytes_sent_per_sec": ctl("bytes_sent"),
        "controller_down_events": c1["swim"]["down_events"].as_u64().unwrap_or(0) - c0["swim"]["down_events"].as_u64().unwrap_or(0),
    });
    eprintln!("steady: {steady}");

    // Signals: dissemination of a controller broadcast to every member.
    let mut signals = Vec::new();
    for _ in 0..args.signals {
        let r = http(&args.admin, "POST", "/swim/broadcast").await?;
        let (version, sent) = (r["version"].as_u64().unwrap() as usize, r["sent_ns"].as_u64().unwrap());
        tokio::time::sleep(Duration::from_secs_f64(args.signal_wait)).await;
        let lat: Vec<f64> = received[version.min(max_signals - 1)]
            .iter()
            .map(|t| t.load(Ordering::Relaxed))
            .filter(|t| *t > 0)
            .map(|t| t.saturating_sub(sent) as f64 / 1e6)
            .collect();
        let got = lat.len();
        let d = dist(lat);
        eprintln!("signal v{version}: {got}/{n} members, latency ms {d}");
        signals.push(json!({"version": version, "received": got, "of": n, "latency_ms": d}));
    }

    // Failures: stop --kill members at once, see when the controller notices.
    let mut killed: HashMap<u32, u64> = HashMap::new();
    // Spread the victims evenly over the index space.
    let step = (n / args.kill.max(1)).max(1);
    for k in 0..args.kill.min(n) {
        let i = (k * step + step / 2) % n;
        killed.insert(i, wall_ns());
        let _ = ctrls[i as usize].send(Ctrl::Stop);
    }
    let kill_start = wall_ns();
    tokio::time::sleep(Duration::from_secs_f64(args.kill_wait)).await;
    let down = http(&args.admin, "GET", "/swim/down").await?;
    let down: HashMap<u32, u64> = down
        .as_object()
        .context("down map")?
        .iter()
        .filter_map(|(k, v)| Some((k.parse().ok()?, v.as_u64()?)))
        .collect();
    let detect: Vec<f64> =
        killed.iter().filter_map(|(i, t)| down.get(i).map(|d| d.saturating_sub(*t) as f64 / 1e9)).collect();
    let undetected = killed.len() - detect.len();
    let false_down = down.iter().filter(|(i, t)| !killed.contains_key(i) && **t >= kill_start).count();
    let detection = json!({
        "killed": killed.len(), "undetected": undetected, "false_down_during_test": false_down,
        "detect_secs": dist(detect),
    });
    eprintln!("detection: {detection}");

    let summary = json!({
        "args": args, "agents": n, "converge_secs": converged,
        "steady": steady, "signals": signals, "detection": detection,
        "members_declared_down_by_others": member_downs.load(Ordering::Relaxed),
        "send_errors": traffic.send_errors.load(Ordering::Relaxed),
    });
    std::fs::write(args.out.join("summary.json"), serde_json::to_string_pretty(&summary)?)?;
    Ok(())
}
