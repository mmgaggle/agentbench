//! sd_notify(3) without libsystemd: READY=1 and watchdog pings over
//! $NOTIFY_SOCKET.

use std::time::Duration;

pub fn notify(msg: &str) {
    let Ok(path) = std::env::var("NOTIFY_SOCKET") else { return };
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::net::UnixDatagram;
        let Ok(sock) = UnixDatagram::unbound() else { return };
        if let Some(name) = path.strip_prefix('@') {
            use std::os::linux::net::SocketAddrExt;
            if let Ok(addr) = std::os::unix::net::SocketAddr::from_abstract_name(name.as_bytes()) {
                let _ = sock.send_to_addr(msg.as_bytes(), &addr);
            }
        } else {
            let _ = sock.send_to(msg.as_bytes(), &path);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (path, msg);
}

/// Half of WatchdogSec, when systemd set one.
pub fn watchdog_interval() -> Option<Duration> {
    let us: u64 = std::env::var("WATCHDOG_USEC").ok()?.parse().ok()?;
    Some(Duration::from_micros(us / 2))
}
