//! Shared pieces of the agent benchmark: the hosts file written by
//! cluster/seed.py, a synthetic cephadm agent payload generator, and TLS
//! helpers.

pub mod intent;
pub mod join;
pub mod seal;
pub mod swim;

use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct HostEntry {
    pub name: String,
    pub keyring: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HostsFile {
    pub fsid: String,
    pub endpoint: String,
    pub addr: String,
    pub hosts: Vec<HostEntry>,
}

impl HostsFile {
    pub fn load(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Ok(serde_json::from_str(&s)?)
    }
}

/// Shape of the host an agent reports on.
#[derive(Debug, Clone, Copy)]
pub struct HostProfile {
    /// OSD daemons (and data devices) per host.
    pub osds: usize,
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn hex64(seed: u64) -> String {
    let mut s = String::with_capacity(64);
    for i in 0..4 {
        let _ = write!(s, "{:016x}", splitmix(seed.wrapping_mul(31).wrapping_add(i)));
    }
    s
}

fn uuid(seed: u64) -> String {
    let h = hex64(seed);
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

const IMAGE: &str = "quay.io/ceph/ceph@sha256:7c4b6bd4f5b9f1d3a02b1f0f0e3c5d7e9a8b6c4d2e0f1a3b5c7d9e1f3a5b7c9d";
const VERSION: &str = "20.2.0";
const STAMP: &str = "2026-10-01T12:00:00.000000Z";

/// Generates the JSON body a cephadm agent POSTs to the mgr's /data
/// endpoint (see CephadmAgent.run in src/cephadm/cephadm.py). `seq` varies
/// the fields that change on every real report (memory usage, cpu, load,
/// uptime) so consecutive messages are never byte-identical.
pub struct PayloadGen {
    pub fsid: String,
    pub profile: HostProfile,
}

impl PayloadGen {
    pub fn new(fsid: &str, profile: HostProfile) -> Self {
        PayloadGen { fsid: fsid.to_string(), profile }
    }

    pub fn daemon_names(&self, idx: usize, host: &str) -> Vec<String> {
        let mut v = vec![
            format!("agent.{host}"),
            format!("crash.{host}"),
            format!("node-exporter.{host}"),
            format!("ceph-exporter.{host}"),
        ];
        for k in 0..self.profile.osds {
            v.push(format!("osd.{}", idx * self.profile.osds + k));
        }
        v
    }

    /// One `cephadm ls` entry. `gen` counts restarts of the daemon: each
    /// restart gives it a new container id and start time, as cephadm sees it.
    fn ls_entry(&self, out: &mut String, name: &str, state: &str, seq: u64, k: u64, gen: u32) {
        let seed = splitmix(name.len() as u64 ^ name.bytes().fold(0u64, |a, b| a.wrapping_mul(131).wrapping_add(b as u64)))
            ^ (gen as u64).wrapping_mul(0x9e3779b97f4a7c15);
        let started = if gen == 0 {
            STAMP.to_string()
        } else {
            format!("2026-10-{:02}T{:02}:{:02}:{:02}.000000Z", 2 + gen / 1440 % 28, gen / 60 % 24, gen % 60, k % 60)
        };
        let fsid = &self.fsid;
        let (svc, ports) = match name.split('.').next().unwrap() {
            "osd" => ("osd.all-available-devices".to_string(), "[]".to_string()),
            "node-exporter" => ("node-exporter".to_string(), "[9100]".to_string()),
            "ceph-exporter" => ("ceph-exporter".to_string(), "[9926]".to_string()),
            t => (t.to_string(), "[]".to_string()),
        };
        let mem = 200_000_000 + ((seq.wrapping_mul(7919) + k * 104_729) % 3_000_000_000);
        let cpu = ((seq + k * 37) % 400) as f64 / 100.0;
        let _ = write!(
            out,
            r#"{{"style":"cephadm:v1","name":"{name}","fsid":"{fsid}","systemd_unit":"ceph-{fsid}@{name}","enabled":true,"state":"{state}","service_name":"{svc}","ports":{ports},"ip":null,"deployed_by":["{IMAGE}"],"rank":null,"rank_generation":null,"extra_container_args":null,"extra_entrypoint_args":null,"memory_request":null,"memory_limit":null,"container_id":"{cid}","container_image_name":"{IMAGE}","container_image_id":"{iid}","container_image_digests":["{IMAGE}"],"memory_usage":{mem},"cpu_percentage":"{cpu:.2}%","version":"{VERSION}","started":"{started}","created":"{STAMP}","deployed":"{STAMP}","configured":"{STAMP}"}}"#,
            cid = hex64(seed),
            iid = hex64(0x1234),
        );
    }

    pub fn networks(&self, out: &mut String, idx: usize) {
        let (a, b) = ((idx >> 8) & 0xff, idx & 0xff);
        let _ = write!(
            out,
            r#"{{"10.{a}.{b}.0/24":{{"bond0":["10.{a}.{b}.10"]}},"172.16.{b}.0/24":{{"bond1":["172.16.{b}.10"]}},"fe80::/64":{{"bond0":["fe80::a00:{a:x}ff:fe{b:02x}:1"],"bond1":["fe80::a00:{a:x}ff:fe{b:02x}:2"]}}}}"#
        );
    }

    /// HostFacts.dump(): a JSON document, embedded as a string.
    pub fn facts(&self, idx: usize, host: &str, seq: u64) -> String {
        let mut f = String::with_capacity(8192);
        let disk = |f: &mut String, i: usize, rot: bool| {
            let _ = write!(
                f,
                r#"{{"description":"{} {} ({:.1}TB)","dev_name":"{}","disk_size_bytes":{},"model":"{}","rev":"{}","vendor":"{}","wwid":"naa.5000c500{:08x}"}}"#,
                if rot { "SEAGATE" } else { "SAMSUNG" },
                if rot { "ST16000NM004J" } else { "MZQL21T9HCJR" },
                if rot { 16.0 } else { 1.9 },
                if rot { format!("sd{}", (b'a' + (i % 26) as u8) as char) } else { format!("nvme{i}n1") },
                if rot { 16_000_900_661_248u64 } else { 1_920_383_410_176u64 },
                if rot { "ST16000NM004J" } else { "MZQL21T9HCJR-00A07" },
                if rot { "E004" } else { "GDC5602Q" },
                if rot { "SEAGATE" } else { "Samsung" },
                splitmix((idx * 1000 + i) as u64) as u32,
            );
        };
        let n = self.profile.osds;
        let load = (seq % 500) as f64 / 100.0;
        let _ = write!(
            f,
            r#"{{"arch":"x86_64","bios_date":"03/14/2025","bios_version":"1.9.2","cpu_cores":32,"cpu_count":2,"cpu_load":{{"1min":{load:.2},"5min":{l5:.2},"15min":{l15:.2}}},"cpu_model":"AMD EPYC 9354 32-Core Processor","cpu_threads":128,"flash_capacity":"3.8TB","flash_capacity_bytes":3840766820352,"flash_count":2,"flash_list":["#,
            l5 = load * 0.9,
            l15 = load * 0.8
        );
        for i in 0..2 {
            if i > 0 {
                f.push(',');
            }
            disk(&mut f, i, false);
        }
        let _ = write!(f, r#"],"hdd_capacity":"{:.1}TB","hdd_capacity_bytes":{},"hdd_count":{n},"hdd_list":["#, 16.0 * n as f64, 16_000_900_661_248u64 * n as u64);
        for i in 0..n {
            if i > 0 {
                f.push(',');
            }
            disk(&mut f, i, true);
        }
        let _ = write!(f, r#"],"hostname":"{host}","interfaces":{{"#);
        let nics = [("eno1", "bond0"), ("eno2", "bond0"), ("ens3f0", "bond1"), ("ens3f1", "bond1")];
        for (i, (nic, bond)) in nics.iter().enumerate() {
            if i > 0 {
                f.push(',');
            }
            let _ = write!(
                f,
                r#""{nic}":{{"driver":"ice","iftype":"physical","ipv4_address":"","ipv6_address":"","lower_devs_list":[],"mtu":9000,"nic_type":"ethernet","operstate":"up","speed":25000,"upper_devs_list":["{bond}"]}}"#
            );
        }
        let (a, b) = ((idx >> 8) & 0xff, idx & 0xff);
        let _ = write!(
            f,
            r#","bond0":{{"driver":"bonding","iftype":"logical","ipv4_address":"10.{a}.{b}.10/24","ipv6_address":"fe80::a00:{a:x}ff:fe{b:02x}:1/64","lower_devs_list":["eno1","eno2"],"mtu":9000,"nic_type":"bonding","operstate":"up","speed":50000,"upper_devs_list":[]}},"bond1":{{"driver":"bonding","iftype":"logical","ipv4_address":"172.16.{b}.10/24","ipv6_address":"fe80::a00:{a:x}ff:fe{b:02x}:2/64","lower_devs_list":["ens3f0","ens3f1"],"mtu":9000,"nic_type":"bonding","operstate":"up","speed":50000,"upper_devs_list":[]}}}},"kernel":"5.14.0-503.15.1.el9_5.x86_64","kernel_parameters":{{"net.ipv4.ip_nonlocal_bind":"0","net.ipv4.ip_forward":"0","net.core.somaxconn":"4096","net.core.rmem_max":"16777216","net.core.wmem_max":"16777216","vm.swappiness":"10","vm.min_free_kbytes":"1048576","kernel.pid_max":"4194304","fs.aio-max-nr":"1048576","fs.file-max":"26234859"}},"kernel_security":{{"SELINUX":"enforcing","SELINUXTYPE":"targeted","description":"SELinux: Enabled(enforcing, targeted)","type":"SELinux"}},"memory_available_kb":{avail},"memory_free_kb":{free},"memory_total_kb":395930584,"model":"PowerEdge R7625 (0B3C)","nic_count":6,"operating_system":"Red Hat Enterprise Linux 9.5 (Plow)","selinux_enabled":true,"shortname":"{host}","fqdn":"{host}.example.com","subscribed":"Unknown","system_uptime":{up:.2},"timestamp":{ts:.6},"vendor":"Dell Inc."}}"#,
            avail = 300_000_000 + seq % 10_000_000,
            free = 100_000_000 + seq % 10_000_000,
            up = 1_000_000.0 + seq as f64 * 20.0,
            ts = 1_790_000_000.0 + seq as f64 * 20.0,
        );
        f
    }

    /// `ceph-volume inventory --format=json`: a JSON list, embedded as a string.
    pub fn volume(&self, idx: usize) -> String {
        let mut v = String::with_capacity(2048 * (self.profile.osds + 2));
        v.push('[');
        for i in 0..self.profile.osds + 2 {
            if i > 0 {
                v.push(',');
            }
            let os_disk = i >= self.profile.osds;
            let path = if os_disk { format!("/dev/nvme{}n1", i - self.profile.osds) } else { format!("/dev/sd{}", (b'a' + (i % 26) as u8) as char) };
            let rot = if os_disk { "0" } else { "1" };
            let size: u64 = if os_disk { 1_920_383_410_176 } else { 16_000_900_661_248 };
            let osd_id = idx * self.profile.osds + i;
            let lvs = if os_disk {
                "[]".to_string()
            } else {
                format!(
                    r#"[{{"name":"osd-block-{u}","osd_id":"{osd_id}","cluster_name":"ceph","type":"block","osd_fsid":"{u}","cluster_fsid":"{fsid}","osdspec_affinity":"all-available-devices","block_uuid":"{bu}"}}]"#,
                    u = uuid((osd_id as u64) << 1),
                    bu = uuid(((osd_id as u64) << 1) | 1),
                    fsid = self.fsid
                )
            };
            let _ = write!(
                v,
                r#"{{"path":"{path}","sys_api":{{"removable":"0","ro":"0","vendor":"{vendor}","model":"{model}","rev":"E004","sas_address":"","sas_device_handle":"","support_discard":"0","rotational":"{rot}","nr_requests":"256","partitions":{{}},"scheduler_mode":"mq-deadline","size":{size}.0,"sectors":0,"sectorsize":"512","human_readable_size":"{hr}","path":"{path}","locked":1,"type":"disk","device_nodes":"{dn}","actuators":null}},"ceph_device_lvm":{ceph},"lsm_data":{{}},"available":false,"rejected_reasons":["Has a FileSystem","Insufficient space (<10 extents) on vgs","LVM detected","locked"],"device_id":"{vendor}_{model}_ZL2{serial:08X}","lvs":{lvs},"created":"{STAMP}","crush_device_class":null,"being_replaced":false}}"#,
                vendor = if os_disk { "Samsung" } else { "SEAGATE" },
                model = if os_disk { "MZQL21T9HCJR-00A07" } else { "ST16000NM004J" },
                hr = if os_disk { "1.75 TB" } else { "14.55 TB" },
                dn = path.trim_start_matches("/dev/"),
                ceph = !os_disk,
                serial = splitmix((idx * 1000 + i) as u64) as u32,
            );
        }
        v.push(']');
        v
    }

    /// Renders one /data POST body.
    pub fn body(&self, idx: usize, host: &HostEntry, ack: u64, seq: u64, error_daemon: Option<usize>) -> Vec<u8> {
        let names = self.daemon_names(idx, &host.name);
        let mut out = String::with_capacity(1100 * names.len() + 64 * 1024);
        let _ = write!(out, r#"{{"host":"{}","ls":["#, host.name);
        for (k, name) in names.iter().enumerate() {
            if k > 0 {
                out.push(',');
            }
            let state = if error_daemon == Some(k) { "error" } else { "running" };
            self.ls_entry(&mut out, name, state, seq, k as u64, 0);
        }
        out.push_str(r#"],"networks":"#);
        self.networks(&mut out, idx);
        out.push_str(r#","facts":""#);
        out.push_str(&self.facts(idx, &host.name, seq).replace('"', "\\\""));
        out.push_str(r#"","volume":""#);
        out.push_str(&self.volume(idx).replace('"', "\\\""));
        let _ = write!(out, r#"","ack":"{ack}","keyring":"#);
        out.push_str(&serde_json::to_string(&host.keyring).unwrap());
        let _ = write!(out, r#","port":"{}"}}"#, host.port);
        out.into_bytes()
    }

    // ---- v2: persistent connection, versioned deltas -------------------
    //
    //   full       {"host","keyring","port","ack","version","full":true,
    //               "daemons":[ls...],"devices":[...],"networks":{},"facts":{}}
    //   delta      {"host","version","base","changed_at_ns","upsert":[ls]}
    //   usage      {"host","version","usage":{"<daemon>":[bytes,"cpu%"]}}
    //   heartbeat  {"host","version"}
    //
    // Everything is single-encoded JSON (no documents inside strings). The
    // keyring travels only in `full`, which opens every connection.

    fn v2_daemons(&self, out: &mut String, idx: usize, host: &str, st: &AgentState, seq: u64) {
        out.push('[');
        for (k, name) in self.daemon_names(idx, host).iter().enumerate() {
            if k > 0 {
                out.push(',');
            }
            self.ls_entry(out, name, st.state_str(k), seq, k as u64, st.gens[k]);
        }
        out.push(']');
    }

    pub fn v2_full(&self, idx: usize, host: &HostEntry, st: &AgentState, ack: u64, seq: u64) -> Vec<u8> {
        let mut out = String::with_capacity(64 * 1024);
        let _ = write!(out, r#"{{"host":"{}","keyring":"#, host.name);
        out.push_str(&serde_json::to_string(&host.keyring).unwrap());
        let _ = write!(out, r#","port":{},"ack":{ack},"version":{},"full":true,"daemons":"#, host.port, st.version);
        self.v2_daemons(&mut out, idx, &host.name, st, seq);
        out.push_str(r#","devices":"#);
        out.push_str(&self.volume(idx));
        out.push_str(r#","networks":"#);
        self.networks(&mut out, idx);
        out.push_str(r#","facts":"#);
        out.push_str(&self.facts(idx, &host.name, seq));
        out.push('}');
        out.into_bytes()
    }

    pub fn v2_delta(&self, idx: usize, host: &HostEntry, st: &AgentState, k: usize, seq: u64, changed_at_ns: u64) -> Vec<u8> {
        let mut out = String::with_capacity(1500);
        let _ = write!(
            out,
            r#"{{"host":"{}","version":{},"base":{},"changed_at_ns":{changed_at_ns},"upsert":["#,
            host.name,
            st.version,
            st.version - 1
        );
        let name = &self.daemon_names(idx, &host.name)[k];
        self.ls_entry(&mut out, name, st.state_str(k), seq, k as u64, st.gens[k]);
        out.push_str("]}");
        out.into_bytes()
    }

    pub fn v2_usage(&self, idx: usize, host: &HostEntry, st: &AgentState, seq: u64) -> Vec<u8> {
        let mut out = String::with_capacity(64 * (st.gens.len() + 2));
        let _ = write!(out, r#"{{"host":"{}","version":{},"usage":{{"#, host.name, st.version);
        for (k, name) in self.daemon_names(idx, &host.name).iter().enumerate() {
            if k > 0 {
                out.push(',');
            }
            let mem = 200_000_000 + ((seq.wrapping_mul(7919) + k as u64 * 104_729) % 3_000_000_000);
            let cpu = ((seq + k as u64 * 37) % 400) as f64 / 100.0;
            let _ = write!(out, r#""{name}":[{mem},"{cpu:.2}%"]"#);
        }
        out.push_str("}}");
        out.into_bytes()
    }

    pub fn v2_heartbeat(&self, host: &HostEntry, st: &AgentState) -> Vec<u8> {
        format!(r#"{{"host":"{}","version":{}}}"#, host.name, st.version).into_bytes()
    }
}

/// What a v2 agent has reported: a state version and, per daemon, a restart
/// generation and a state.
#[derive(Clone, Debug)]
pub struct AgentState {
    pub version: u64,
    pub gens: Vec<u32>,
    /// 0 running, 1 error, 2 stopped
    pub states: Vec<u8>,
}

impl AgentState {
    pub fn new(daemons: usize) -> Self {
        AgentState { version: 1, gens: vec![0; daemons], states: vec![0; daemons] }
    }

    pub fn state_str(&self, k: usize) -> &'static str {
        match self.states[k] {
            1 => "error",
            2 => "stopped",
            _ => "running",
        }
    }

    /// Applies one material change chosen by `r` (uniform u64) and returns
    /// the daemon index it touched: mostly restarts, sometimes a daemon
    /// failing or recovering.
    pub fn mutate(&mut self, r: u64) -> usize {
        let k = (r % self.gens.len() as u64) as usize;
        match (r >> 32) % 10 {
            0 => self.states[k] = if self.states[k] == 1 { 0 } else { 1 },
            _ => {
                self.gens[k] += 1;
                self.states[k] = 0;
            }
        }
        self.version += 1;
        k
    }
}

/// DER of the first certificate in a PEM text.
pub fn tls_pem_der(pem: &str) -> Result<Vec<u8>> {
    let c = rustls_pemfile::certs(&mut pem.as_bytes()).next().context("no certificate in PEM")??;
    Ok(c.as_ref().to_vec())
}

pub mod tls {
    use super::*;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
    use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};

    pub fn provider() -> Arc<CryptoProvider> {
        Arc::new(rustls::crypto::ring::default_provider())
    }

    /// Accepts any server certificate but still checks the handshake
    /// signature, like an agent minus chain validation.
    #[derive(Debug)]
    struct AnyCert(Arc<CryptoProvider>);

    impl ServerCertVerifier for AnyCert {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> std::result::Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
        }
        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
            verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
        }
        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    /// Client config matching the agent's urllib usage: a full handshake on
    /// every request (no session resumption).
    pub fn agent_client_config() -> Arc<ClientConfig> {
        let p = provider();
        let mut cfg = ClientConfig::builder_with_provider(p.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyCert(p)))
            .with_no_client_auth();
        cfg.resumption = rustls::client::Resumption::disabled();
        Arc::new(cfg)
    }

    pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
        let mut r = std::io::BufReader::new(std::fs::File::open(path).with_context(|| format!("{}", path.display()))?);
        Ok(rustls_pemfile::certs(&mut r).collect::<std::result::Result<_, _>>()?)
    }

    pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
        let mut r = std::io::BufReader::new(std::fs::File::open(path).with_context(|| format!("{}", path.display()))?);
        rustls_pemfile::private_key(&mut r)?.context("no private key found")
    }

    /// Server config that verifies client certificates against `client_ca`
    /// when a client presents one, and still accepts clients that don't.
    pub fn server_config_optional_mtls(cert: &Path, key: &Path, client_ca: &Path) -> Result<Arc<ServerConfig>> {
        let mut roots = rustls::RootCertStore::empty();
        for c in load_certs(client_ca)? {
            roots.add(c)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
            .allow_unauthenticated()
            .build()?;
        let cfg = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(verifier)
            .with_single_cert(load_certs(cert)?, load_key(key)?)?;
        Ok(Arc::new(cfg))
    }

    pub fn server_config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>> {
        let cfg = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(load_certs(cert)?, load_key(key)?)?;
        Ok(Arc::new(cfg))
    }
}
