//! Host/agent cache and the /data message handling, following
//! cephadm's HostData.check_request_fields() + handle_metadata() and
//! CephadmOrchestrator._process_ls_output().
//!
//! Differences from the mgr module, by design:
//!  * per-host locking instead of the GIL, so messages for different hosts
//!    are processed in parallel;
//!  * the failed-daemon set and the "hosts not up to date" count are kept
//!    incrementally instead of rescanning every daemon in the cluster on
//!    every message;
//!  * persistence is decoupled from the request (see persist.rs).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

/// orchestrator.KNOWN_DAEMON_TYPES on the ceph build under test.
pub const KNOWN_DAEMON_TYPES: &[&str] = &[
    "mon", "mgr", "osd", "mds", "rgw", "nfs", "iscsi", "nvmeof", "rbd-mirror", "cephfs-mirror",
    "ceph-exporter", "crash", "alertmanager", "grafana", "node-exporter", "prometheus", "loki",
    "promtail", "alloy", "mgmt-gateway", "oauth2-proxy", "elasticsearch", "jaeger-collector",
    "jaeger-query", "jaeger-agent", "haproxy", "keepalived", "container", "agent", "node-proxy",
    "snmp-gateway", "smb", "nvmeof-gw",
];

/// ISO 8601 UTC with microseconds, as ceph.utils.datetime_to_str().
pub fn now_str() -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = d.as_secs() as i64;
    let (days, sod) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    // civil_from_days (Howard Hinnant)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}Z",
        sod / 3600,
        sod / 60 % 60,
        sod % 60,
        d.subsec_micros()
    )
}

/// One entry of the agent's `ls` (cephadm list_daemons) output.
#[derive(Deserialize)]
pub struct LsEntry {
    #[serde(default)]
    style: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    fsid: String,
    state: Option<String>,
    container_id: Option<String>,
    container_image_id: Option<String>,
    container_image_name: Option<String>,
    container_image_digests: Option<Vec<String>>,
    version: Option<String>,
    created: Option<String>,
    started: Option<String>,
    last_configured: Option<String>,
    last_deployed: Option<String>,
    memory_usage: Option<u64>,
    memory_request: Option<u64>,
    memory_limit: Option<u64>,
    cpu_percentage: Option<String>,
    service_name: Option<String>,
    ports: Option<Vec<u16>>,
    ip: Option<String>,
    systemd_unit: Option<String>,
    rank: Option<Value>,
    rank_generation: Option<Value>,
}

/// The /data request body.
#[derive(Deserialize)]
pub struct AgentMsg {
    host: Option<String>,
    keyring: Option<String>,
    port: Option<Value>,
    ack: Option<Value>,
    ls: Option<Vec<LsEntry>>,
    networks: Option<Value>,
    facts: Option<String>,
    volume: Option<String>,
}

/// DaemonDescription.to_json() for the fields the agent path sets.
#[derive(Serialize, Clone, PartialEq)]
pub struct Daemon {
    daemon_type: String,
    daemon_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_name: Option<String>,
    daemon_name: String,
    hostname: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_image_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_image_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    container_image_digests: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory_usage: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory_request: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    memory_limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_percentage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<i32>,
    status_desc: String,
    is_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    ports: Option<Vec<u16>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rank: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rank_generation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    systemd_unit: Option<String>,
    pending_daemon_config: bool,
    user_stopped: bool,
    last_refresh: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    started: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_deployed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_configured: Option<String>,
}

impl Daemon {
    fn is_error(&self) -> bool {
        self.status == Some(-1)
    }

    /// Fields whose change matters to the orchestrator (not usage samples
    /// or refresh stamps), for the `changed` persistence policy.
    fn material(&self) -> (&str, &Option<String>, &Option<String>, &Option<String>, &Option<i32>, &Option<String>) {
        (&self.daemon_name, &self.container_id, &self.container_image_id, &self.version, &self.status, &self.started)
    }
}

#[derive(Default)]
pub struct HostState {
    pub name: String,
    pub addr: String,
    pub daemons: BTreeMap<String, Daemon>,
    pub networks: Value,
    pub facts: Value,
    pub devices: Vec<Value>,
    pub last_daemon_update: Option<String>,
    pub last_device_update: Option<String>,
    pub last_device_change: Option<String>,
    pub last_network_update: Option<String>,
    pub last_facts_update: Option<String>,
    /// Preserved verbatim from the host cache record (host checks,
    /// daemon config deps, ...): agentd never changes these.
    pub preserved: Map<String, Value>,
    pub metadata_up_to_date: bool,
    /// v2: the agent state version this cache reflects
    pub version: u64,
    // AgentCache
    pub agent_counter: Option<u64>,
    pub agent_key: Option<String>,
    pub agent_port: Option<u16>,
    pub agent_timestamp: Option<String>,
    pub agent_config_deps: Value,
    pub messaging: bool,
    // persistence bookkeeping
    pub dirty: bool,
    pub persisted_fingerprint: u64,
    pub persisted_at: Option<Instant>,
}

impl HostState {
    /// save_host(): the mgr/cephadm/host.<name> record.
    pub fn host_record(&self) -> Vec<u8> {
        let mut j = self.preserved.clone();
        j.insert("daemons".into(), json!(self.daemons));
        j.insert("devices".into(), json!([]));
        j.entry("osdspec_previews").or_insert(json!([]));
        j.entry("osdspec_last_applied").or_insert(json!({}));
        j.entry("daemon_config_deps").or_insert(json!({}));
        for (k, v) in [
            ("last_daemon_update", &self.last_daemon_update),
            ("last_device_update", &self.last_device_update),
            ("last_device_change", &self.last_device_change),
            ("last_network_update", &self.last_network_update),
        ] {
            if let Some(v) = v {
                j.insert(k.into(), json!(v));
            }
        }
        j.insert("networks_and_interfaces".into(), self.networks.clone());
        j.insert("metadata_up_to_date".into(), json!(self.metadata_up_to_date));
        serde_json::to_vec(&j).unwrap()
    }

    /// save_host_devices(): mgr/cephadm/host.<name>.devices.0
    pub fn devices_record(&self) -> Vec<u8> {
        serde_json::to_vec(&json!({"devices": self.devices, "entries": 1})).unwrap()
    }

    /// AgentCache.save_agent(): mgr/cephadm/agent.<name>
    pub fn agent_record(&self) -> Vec<u8> {
        let mut j = Map::new();
        if !self.agent_config_deps.is_null() {
            j.insert("agent_config_deps".into(), self.agent_config_deps.clone());
        }
        if let Some(c) = self.agent_counter {
            j.insert("agent_counter".into(), json!(c));
        }
        if let Some(k) = &self.agent_key {
            j.insert("agent_keys".into(), json!(k));
        }
        if let Some(p) = self.agent_port {
            j.insert("agent_ports".into(), json!(p));
        }
        if let Some(t) = &self.agent_timestamp {
            j.insert("agent_timestamp".into(), json!(t));
        }
        serde_json::to_vec(&j).unwrap()
    }

    /// Hash of what the orchestrator acts on; usage samples and refresh
    /// timestamps are excluded.
    pub fn fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for d in self.daemons.values() {
            d.material().hash(&mut h);
        }
        for d in &self.devices {
            let mut d = d.clone();
            if let Some(o) = d.as_object_mut() {
                o.remove("created");
            }
            d.to_string().hash(&mut h);
        }
        self.networks.to_string().hash(&mut h);
        (self.agent_counter, &self.agent_key, self.agent_port).hash(&mut h);
        self.metadata_up_to_date.hash(&mut h);
        h.finish()
    }
}

#[derive(Default)]
pub struct Counters {
    pub messages: AtomicU64,
    pub ok: AtomicU64,
    pub bad: AtomicU64,
    pub failed: AtomicU64,
    pub kicks: AtomicU64,
    pub ack_requests: AtomicU64,
    pub health_updates: AtomicU64,
}

pub enum Outcome {
    /// Result string for the response, and the host to persist (if any).
    Done(String, Option<String>),
    /// Body was not JSON (cherrypy json_in answers 400).
    BadRequest(String),
}

pub struct Cache {
    pub fsid: String,
    pub hosts: HashMap<String, Arc<Mutex<HostState>>>,
    /// Failed daemons per host (CEPHADM_FAILED_DAEMON detail), maintained
    /// incrementally.
    pub failed: Mutex<BTreeMap<String, Vec<String>>>,
    pub not_up_to_date: AtomicUsize,
    pub counters: Counters,
    pub v2: V2Stats,
}

fn as_int(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

impl Cache {
    /// Builds the cache from the cephadm KV records.
    pub fn load(fsid: &str, inventory: &Map<String, Value>, records: &Map<String, Value>) -> Cache {
        let mut hosts = HashMap::new();
        let mut not_up = 0;
        let mut failed: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, spec) in inventory {
            let mut h = HostState {
                name: name.clone(),
                addr: spec.get("addr").and_then(|a| a.as_str()).unwrap_or(name).to_string(),
                ..Default::default()
            };
            let parse = |k: String| records.get(&k).and_then(|v| v.as_str()).and_then(|s| serde_json::from_str::<Value>(s).ok());
            if let Some(Value::Object(mut j)) = parse(format!("mgr/cephadm/host.{name}")) {
                h.metadata_up_to_date = j.get("metadata_up_to_date").and_then(|v| v.as_bool()).unwrap_or(false);
                h.networks = j.remove("networks_and_interfaces").unwrap_or(json!({}));
                for k in ["last_daemon_update", "last_device_update", "last_device_change", "last_network_update"] {
                    let v = j.remove(k).and_then(|v| v.as_str().map(str::to_string));
                    match k {
                        "last_daemon_update" => h.last_daemon_update = v,
                        "last_device_update" => h.last_device_update = v,
                        "last_device_change" => h.last_device_change = v,
                        _ => h.last_network_update = v,
                    }
                }
                if let Some(Value::Object(ds)) = j.remove("daemons") {
                    for (dn, d) in ds {
                        if let Some(d) = daemon_from_record(name, &d) {
                            if d.is_error() && d.daemon_type != "agent" {
                                failed.entry(name.clone()).or_default().push(format!(
                                    "daemon {} on {} is in {} state",
                                    dn, name, d.status_desc
                                ));
                            }
                            h.daemons.insert(dn, d);
                        }
                    }
                }
                j.remove("devices");
                j.remove("metadata_up_to_date");
                h.preserved = j;
            }
            if let Some(Value::Object(d)) = parse(format!("mgr/cephadm/host.{name}.devices.0")) {
                if let Some(Value::Array(devs)) = d.get("devices") {
                    h.devices = devs.clone();
                }
            }
            if let Some(Value::Object(a)) = parse(format!("mgr/cephadm/agent.{name}")) {
                h.agent_counter = a.get("agent_counter").and_then(as_int).map(|c| c as u64);
                h.agent_key = a.get("agent_keys").and_then(|k| k.as_str()).map(str::to_string);
                h.agent_port = a.get("agent_ports").and_then(as_int).map(|p| p as u16);
                h.agent_timestamp = a.get("agent_timestamp").and_then(|t| t.as_str()).map(str::to_string);
                h.agent_config_deps = a.get("agent_config_deps").cloned().unwrap_or(Value::Null);
            }
            if !h.metadata_up_to_date {
                not_up += 1;
            }
            h.persisted_fingerprint = h.fingerprint();
            hosts.insert(name.clone(), Arc::new(Mutex::new(h)));
        }
        Cache {
            fsid: fsid.to_string(),
            hosts,
            failed: Mutex::new(failed),
            not_up_to_date: AtomicUsize::new(not_up),
            counters: Counters::default(),
            v2: V2Stats::new(),
        }
    }

    /// A cache for hosts known only from hosts.json (no KV state): for
    /// running agentd without a cluster.
    pub fn from_hosts(fsid: &str, hosts: &[(String, String, u16)]) -> Cache {
        let map = hosts
            .iter()
            .map(|(name, key, port)| {
                let h = HostState {
                    name: name.clone(),
                    addr: "127.0.0.1".into(),
                    agent_key: Some(key.clone()),
                    agent_port: Some(*port),
                    agent_counter: Some(1),
                    ..Default::default()
                };
                (name.clone(), Arc::new(Mutex::new(h)))
            })
            .collect();
        Cache {
            fsid: fsid.to_string(),
            hosts: map,
            failed: Mutex::new(BTreeMap::new()),
            not_up_to_date: AtomicUsize::new(hosts.len()),
            counters: Counters::default(),
            v2: V2Stats::new(),
        }
    }

    /// HostData.index(): validate, then handle_metadata(). `request_ack` is
    /// called with (host, addr, port, counter) when the agent must be told
    /// to resend with the current counter.
    pub fn handle(&self, body: &[u8], request_ack: &dyn Fn(&str, &str, u16, u64)) -> Outcome {
        self.counters.messages.fetch_add(1, Ordering::Relaxed);
        let msg: AgentMsg = match serde_json::from_slice(body) {
            Ok(m) => m,
            Err(e) => return Outcome::BadRequest(format!("Invalid JSON document: {e}")),
        };
        let (host, port, ack) = match self.check_request_fields(&msg) {
            Ok(v) => v,
            Err(e) => {
                self.counters.bad.fetch_add(1, Ordering::Relaxed);
                return Outcome::Done(format!("Bad metadata: {e}"), None);
            }
        };
        let entry = self.hosts.get(&host).unwrap();
        let mut h = entry.lock().unwrap();
        match self.handle_metadata(&mut h, msg, port, ack, request_ack) {
            Ok(r) => {
                self.counters.ok.fetch_add(1, Ordering::Relaxed);
                r
            }
            Err(e) => {
                self.counters.failed.fetch_add(1, Ordering::Relaxed);
                Outcome::Done(format!("Failed to update metadata with metadata from agent on host {host}: {e}"), None)
            }
        }
    }

    fn check_request_fields(&self, m: &AgentMsg) -> Result<(String, u16, u64), String> {
        let host = m.host.clone().ok_or("No host in metadata from agent (\"host\" field)")?;
        let entry = self.hosts.get(&host).ok_or_else(|| format!("Received metadata from agent on unknown hostname {host}"))?;
        let keyring = m
            .keyring
            .as_ref()
            .ok_or_else(|| format!("Agent on host {host} not reporting its keyring for validation (\"keyring\" field)"))?;
        {
            let h = entry.lock().unwrap();
            let stored = h.agent_key.as_ref().ok_or_else(|| format!("No agent keyring stored for host {host}. Cannot verify agent"))?;
            if stored != keyring {
                return Err(format!("Got wrong keyring from agent on host {host}."));
            }
        }
        let port = m
            .port
            .as_ref()
            .ok_or_else(|| format!("Agent on host {host} not reporting its listener port (\"port\" fields)"))?;
        let port = as_int(port).ok_or_else(|| format!("Agent on host {host} reported a non-integer port"))? as u16;
        let ack = m.ack.as_ref().ok_or_else(|| format!("Agent on host {host} not reporting its counter value (\"ack\" field)"))?;
        let ack = as_int(ack)
            .ok_or_else(|| format!("Counter value from agent on host {host} could not be converted to an integer"))?
            as u64;
        Ok((host, port, ack))
    }

    fn handle_metadata(
        &self,
        h: &mut HostState,
        m: AgentMsg,
        port: u16,
        ack: u64,
        request_ack: &dyn Fn(&str, &str, u16, u64),
    ) -> Result<Outcome, String> {
        h.agent_port = Some(port);
        let Some(counter) = h.agent_counter else {
            h.agent_counter = Some(1);
            self.counters.ack_requests.fetch_add(1, Ordering::Relaxed);
            request_ack(&h.name, &h.addr, port, 1);
            return Ok(Outcome::Done(
                format!(
                    "Got metadata from agent on host {} with no known counter entry. Starting counter at 1 and requesting new metadata",
                    h.name
                ),
                Some(h.name.clone()),
            ));
        };
        let now = now_str();
        h.agent_timestamp = Some(now.clone());

        let errors_old = Self::error_set(h);
        let count_old = h.daemons.len();

        let up_to_date = ack == counter;
        if !up_to_date && !h.messaging {
            h.messaging = true;
            self.counters.ack_requests.fetch_add(1, Ordering::Relaxed);
            request_ack(&h.name, &h.addr, port, counter);
        }

        let has_ls = m.ls.as_ref().is_some_and(|l| !l.is_empty());
        if let Some(ls) = m.ls.filter(|l| !l.is_empty()) {
            self.process_ls(h, ls, &now)?;
        }
        if let Some(n) = m.networks.filter(|n| n.as_object().is_some_and(|o| !o.is_empty())) {
            h.networks = n;
            h.last_network_update = Some(now.clone());
        }
        if let Some(f) = m.facts.filter(|f| !f.is_empty()) {
            h.facts = serde_json::from_str(&f).map_err(|e| format!("facts: {e}"))?;
            h.last_facts_update = Some(now.clone());
        }
        if let Some(v) = m.volume.filter(|v| !v.is_empty()) {
            let devs: Vec<Value> = serde_json::from_str(&v).map_err(|e| format!("volume: {e}"))?;
            Self::set_devices(h, devs, &now)?;
        }

        self.note_changes(h, &errors_old, count_old);
        if up_to_date && has_ls && !h.metadata_up_to_date {
            h.metadata_up_to_date = true;
            if self.not_up_to_date.fetch_sub(1, Ordering::Relaxed) == 1 {
                // all hosts up to date: the mgr kicks its serve loop here
                self.counters.kicks.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(Outcome::Done("Successfully processed metadata.".into(), Some(h.name.clone())))
    }

    fn error_set(h: &HostState) -> BTreeSet<String> {
        h.daemons.values().filter(|d| d.is_error()).map(|d| d.daemon_name.clone()).collect()
    }

    /// After a host changed: update the failed-daemon health detail
    /// (update_failed_daemon_health_check(), incrementally) and count a
    /// serve-loop kick when the mgr would have kicked.
    fn note_changes(&self, h: &HostState, errors_old: &BTreeSet<String>, count_old: usize) {
        let errors_new = Self::error_set(h);
        if &errors_new != errors_old {
            let detail: Vec<String> = h
                .daemons
                .values()
                .filter(|d| d.is_error() && d.daemon_type != "agent")
                .map(|d| format!("daemon {} on {} is in {} state", d.daemon_name, h.name, d.status_desc))
                .collect();
            let mut failed = self.failed.lock().unwrap();
            if detail.is_empty() {
                failed.remove(&h.name);
            } else {
                failed.insert(h.name.clone(), detail);
            }
            self.counters.health_updates.fetch_add(1, Ordering::Relaxed);
        }
        if &errors_new != errors_old || count_old != h.daemons.len() {
            self.counters.kicks.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// HostCache.update_host_devices(), with the device-change check.
    fn set_devices(h: &mut HostState, devs: Vec<Value>, now: &str) -> Result<(), String> {
        if devs.iter().any(|d| d.get("path").and_then(|p| p.as_str()).is_none()) {
            return Err("Device: missing path".into());
        }
        let strip = |v: &Vec<Value>| -> Vec<Value> {
            v.iter()
                .map(|d| {
                    let mut d = d.clone();
                    if let Some(o) = d.as_object_mut() {
                        o.remove("created");
                    }
                    d
                })
                .collect()
        };
        if h.last_device_change.is_none() || strip(&h.devices) != strip(&devs) {
            h.last_device_change = Some(now.to_string());
        }
        h.devices = devs;
        h.last_device_update = Some(now.to_string());
        Ok(())
    }

    /// One `ls` entry as _process_ls_output() turns it into a
    /// DaemonDescription; Ok(None) for entries the mgr skips.
    fn daemon_from_ls(&self, h: &HostState, d: LsEntry, now: &str) -> Result<Option<Daemon>, String> {
        if !d.style.starts_with("cephadm") || d.fsid != self.fsid {
            return Ok(None);
        }
        let name = d.name.clone();
        let Some((daemon_type, daemon_id)) = name.split_once('.') else { return Ok(None) };
        if !KNOWN_DAEMON_TYPES.contains(&daemon_type) {
            return Ok(None);
        }
        let (status, status_desc) = match d.state.as_deref() {
            None => (None, "unknown".to_string()),
            Some("running") => (Some(1), "running".to_string()),
            Some("stopped") => (Some(0), "stopped".to_string()),
            Some("error") => (Some(-1), "error".to_string()),
            Some("unknown") => (Some(-2), "unknown".to_string()),
            Some(s) => return Err(format!("'{s}'")),
        };
        let cached = h.daemons.get(&name);
        Ok(Some(Daemon {
            daemon_type: daemon_type.to_string(),
            daemon_id: daemon_id.to_string(),
            service_name: d.service_name,
            daemon_name: name.clone(),
            hostname: h.name.clone(),
            container_id: d.container_id.map(|c| c.chars().take(12).collect()),
            container_image_id: d.container_image_id,
            container_image_name: d.container_image_name,
            container_image_digests: d.container_image_digests,
            memory_usage: d.memory_usage,
            memory_request: d.memory_request,
            memory_limit: d.memory_limit,
            cpu_percentage: d.cpu_percentage,
            version: d.version,
            status,
            status_desc,
            is_active: false,
            ports: d.ports,
            ip: d.ip,
            rank: d.rank.as_ref().and_then(as_int),
            rank_generation: d.rank_generation.as_ref().and_then(as_int),
            systemd_unit: d.systemd_unit,
            pending_daemon_config: cached.is_some_and(|c| c.pending_daemon_config),
            user_stopped: cached.is_some_and(|c| c.user_stopped),
            last_refresh: now.to_string(),
            created: d.created,
            started: d.started,
            last_deployed: d.last_deployed,
            last_configured: d.last_configured,
        }))
    }

    /// CephadmOrchestrator._process_ls_output()
    fn process_ls(&self, h: &mut HostState, ls: Vec<LsEntry>, now: &str) -> Result<(), String> {
        let mut dm = BTreeMap::new();
        for d in ls {
            if let Some(dd) = self.daemon_from_ls(h, d, now)? {
                dm.insert(dd.daemon_name.clone(), dd);
            }
        }
        h.daemons = dm;
        h.last_daemon_update = Some(now.to_string());
        Ok(())
    }

    /// v2 protocol: one persistent connection per agent; `bound` is the host
    /// this connection authenticated as (keyring on its first message).
    pub fn handle_v2(&self, bound: &mut Option<String>, body: &[u8]) -> V2Outcome {
        let m: V2Msg = match serde_json::from_slice(body) {
            Ok(m) => m,
            Err(e) => return V2Outcome::BadRequest(format!("Invalid JSON document: {e}")),
        };
        let Some(entry) = self.hosts.get(&m.host) else {
            self.v2.unauthorized.fetch_add(1, Ordering::Relaxed);
            return V2Outcome::Unauthorized(format!("unknown host {}", m.host));
        };
        let mut h = entry.lock().unwrap();
        if bound.as_deref() != Some(m.host.as_str()) {
            match (&m.keyring, &h.agent_key) {
                (Some(k), Some(stored)) if k == stored => *bound = Some(m.host.clone()),
                _ => {
                    self.v2.unauthorized.fetch_add(1, Ordering::Relaxed);
                    return V2Outcome::Unauthorized(format!("connection is not authenticated as {}", m.host));
                }
            }
        }
        let now = now_str();
        let errors_old = Self::error_set(&h);
        let count_old = h.daemons.len();
        let kind = if m.full {
            if let Some(p) = m.port {
                h.agent_port = Some(p);
            }
            if let Some(ls) = m.daemons {
                if let Err(e) = self.process_ls(&mut h, ls, &now) {
                    return V2Outcome::BadRequest(e);
                }
            }
            if let Some(devs) = m.devices {
                if let Err(e) = Self::set_devices(&mut h, devs, &now) {
                    return V2Outcome::BadRequest(e);
                }
            }
            if let Some(n) = m.networks {
                h.networks = n;
                h.last_network_update = Some(now.clone());
            }
            if let Some(f) = m.facts {
                h.facts = f;
                h.last_facts_update = Some(now.clone());
            }
            h.version = m.version;
            if m.ack.unwrap_or(1) == h.agent_counter.unwrap_or(1) && !h.metadata_up_to_date {
                h.metadata_up_to_date = true;
                if self.not_up_to_date.fetch_sub(1, Ordering::Relaxed) == 1 {
                    self.counters.kicks.fetch_add(1, Ordering::Relaxed);
                }
            }
            V2Kind::Full
        } else if m.upsert.is_some() || m.remove.is_some() {
            if m.base != Some(h.version) {
                self.v2.resync.fetch_add(1, Ordering::Relaxed);
                return V2Outcome::Resync { version: h.version };
            }
            for d in m.upsert.unwrap_or_default() {
                match self.daemon_from_ls(&h, d, &now) {
                    Ok(Some(dd)) => {
                        h.daemons.insert(dd.daemon_name.clone(), dd);
                    }
                    Ok(None) => {}
                    Err(e) => return V2Outcome::BadRequest(e),
                }
            }
            for name in m.remove.unwrap_or_default() {
                h.daemons.remove(&name);
            }
            h.version = m.version;
            h.last_daemon_update = Some(now.clone());
            if let Some(t) = m.changed_at_ns {
                let lag = now_ns().saturating_sub(t) / 1000;
                self.v2.lag_us.lock().unwrap().saturating_record(lag.max(1));
            }
            V2Kind::Delta
        } else if let Some(u) = m.usage {
            if m.version != h.version {
                self.v2.resync.fetch_add(1, Ordering::Relaxed);
                return V2Outcome::Resync { version: h.version };
            }
            for (name, (mem, cpu)) in u {
                if let Some(d) = h.daemons.get_mut(&name) {
                    d.memory_usage = Some(mem);
                    d.cpu_percentage = Some(cpu);
                    d.last_refresh = now.clone();
                }
            }
            V2Kind::Usage
        } else {
            if m.version != h.version {
                self.v2.resync.fetch_add(1, Ordering::Relaxed);
                return V2Outcome::Resync { version: h.version };
            }
            V2Kind::Heartbeat
        };
        h.agent_timestamp = Some(now);
        self.note_changes(&h, &errors_old, count_old);
        self.v2.count[kind as usize].fetch_add(1, Ordering::Relaxed);
        self.v2.bytes[kind as usize].fetch_add(body.len() as u64, Ordering::Relaxed);
        V2Outcome::Ok {
            version: h.version,
            counter: h.agent_counter.unwrap_or(1),
            persist: matches!(kind, V2Kind::Full | V2Kind::Delta).then(|| h.name.clone()),
        }
    }
}

pub fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// The v2 message (see proto::PayloadGen::v2_*).
#[derive(Deserialize)]
pub struct V2Msg {
    host: String,
    keyring: Option<String>,
    port: Option<u16>,
    ack: Option<u64>,
    version: u64,
    base: Option<u64>,
    #[serde(default)]
    full: bool,
    changed_at_ns: Option<u64>,
    daemons: Option<Vec<LsEntry>>,
    upsert: Option<Vec<LsEntry>>,
    remove: Option<Vec<String>>,
    devices: Option<Vec<Value>>,
    networks: Option<Value>,
    facts: Option<Value>,
    usage: Option<HashMap<String, (u64, String)>>,
}

#[derive(Clone, Copy, Debug)]
pub enum V2Kind {
    Full = 0,
    Delta = 1,
    Usage = 2,
    Heartbeat = 3,
}
pub const V2_KINDS: [&str; 4] = ["full", "delta", "usage", "heartbeat"];

pub enum V2Outcome {
    Ok { version: u64, counter: u64, persist: Option<String> },
    Resync { version: u64 },
    Unauthorized(String),
    BadRequest(String),
}

pub struct V2Stats {
    pub count: [AtomicU64; 4],
    pub bytes: [AtomicU64; 4],
    pub resync: AtomicU64,
    pub unauthorized: AtomicU64,
    /// change at the agent -> applied here, microseconds
    pub lag_us: Mutex<Histogram<u64>>,
}

impl V2Stats {
    fn new() -> Self {
        V2Stats {
            count: Default::default(),
            bytes: Default::default(),
            resync: AtomicU64::new(0),
            unauthorized: AtomicU64::new(0),
            lag_us: Mutex::new(Histogram::new_with_bounds(1, 600_000_000, 3).unwrap()),
        }
    }
}

fn daemon_from_record(host: &str, d: &Value) -> Option<Daemon> {
    let s = |k: &str| d.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let daemon_type = s("daemon_type")?;
    let daemon_id = s("daemon_id")?;
    Some(Daemon {
        daemon_name: format!("{daemon_type}.{daemon_id}"),
        daemon_type,
        daemon_id,
        service_name: s("service_name"),
        hostname: host.to_string(),
        container_id: s("container_id"),
        container_image_id: s("container_image_id"),
        container_image_name: s("container_image_name"),
        container_image_digests: d.get("container_image_digests").and_then(|v| serde_json::from_value(v.clone()).ok()),
        memory_usage: d.get("memory_usage").and_then(|v| v.as_u64()),
        memory_request: d.get("memory_request").and_then(|v| v.as_u64()),
        memory_limit: d.get("memory_limit").and_then(|v| v.as_u64()),
        cpu_percentage: s("cpu_percentage"),
        version: s("version"),
        status: d.get("status").and_then(|v| v.as_i64()).map(|v| v as i32),
        status_desc: s("status_desc").unwrap_or_else(|| "unknown".into()),
        is_active: false,
        ports: d.get("ports").and_then(|v| serde_json::from_value(v.clone()).ok()),
        ip: s("ip"),
        rank: d.get("rank").and_then(|v| v.as_i64()),
        rank_generation: d.get("rank_generation").and_then(|v| v.as_i64()),
        systemd_unit: s("systemd_unit"),
        pending_daemon_config: d.get("pending_daemon_config").and_then(|v| v.as_bool()).unwrap_or(false),
        user_stopped: d.get("user_stopped").and_then(|v| v.as_bool()).unwrap_or(false),
        last_refresh: s("last_refresh").unwrap_or_default(),
        created: s("created"),
        started: s("started"),
        last_deployed: s("last_deployed"),
        last_configured: s("last_configured"),
    })
}
