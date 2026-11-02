//! Minimal librados binding: just enough to issue mon commands
//! (config-key get/set/dump) the way ceph-mgr's set_store does.

use std::ffi::{c_char, c_int, c_void, CString};
use std::ptr;

use anyhow::{bail, Result};

#[allow(non_camel_case_types)]
type rados_t = *mut c_void;

extern "C" {
    fn rados_create2(cluster: *mut rados_t, clustername: *const c_char, name: *const c_char, flags: u64) -> c_int;
    fn rados_conf_read_file(cluster: rados_t, path: *const c_char) -> c_int;
    fn rados_connect(cluster: rados_t) -> c_int;
    fn rados_shutdown(cluster: rados_t);
    fn rados_mon_command(
        cluster: rados_t,
        cmd: *mut *const c_char,
        cmdlen: usize,
        inbuf: *const c_char,
        inbuflen: usize,
        outbuf: *mut *mut c_char,
        outbuflen: *mut usize,
        outs: *mut *mut c_char,
        outslen: *mut usize,
    ) -> c_int;
    fn rados_buffer_free(buf: *mut c_char);
}

pub struct Rados(rados_t);

// librados handles are safe to share between threads.
unsafe impl Send for Rados {}
unsafe impl Sync for Rados {}

impl Rados {
    pub fn connect(conf: &str, name: &str) -> Result<Rados> {
        let mut h: rados_t = ptr::null_mut();
        let cluster = CString::new("ceph")?;
        let name = CString::new(name)?;
        let conf = CString::new(conf)?;
        unsafe {
            let r = rados_create2(&mut h, cluster.as_ptr(), name.as_ptr(), 0);
            if r < 0 {
                bail!("rados_create2: {r}");
            }
            let r = rados_conf_read_file(h, conf.as_ptr());
            if r < 0 {
                rados_shutdown(h);
                bail!("rados_conf_read_file: {r}");
            }
            let r = rados_connect(h);
            if r < 0 {
                rados_shutdown(h);
                bail!("rados_connect: {r}");
            }
        }
        Ok(Rados(h))
    }

    /// Blocking mon command; returns the output buffer.
    pub fn mon_command(&self, cmd: &str, inbuf: &[u8]) -> Result<Vec<u8>> {
        let c = CString::new(cmd)?;
        let mut cmds = [c.as_ptr()];
        let (mut outbuf, mut outbuflen, mut outs, mut outslen) = (ptr::null_mut(), 0usize, ptr::null_mut(), 0usize);
        let r = unsafe {
            rados_mon_command(
                self.0,
                cmds.as_mut_ptr(),
                1,
                inbuf.as_ptr() as *const c_char,
                inbuf.len(),
                &mut outbuf,
                &mut outbuflen,
                &mut outs,
                &mut outslen,
            )
        };
        let take = |p: *mut c_char, n: usize| -> Vec<u8> {
            if p.is_null() {
                return Vec::new();
            }
            let v = unsafe { std::slice::from_raw_parts(p as *const u8, n).to_vec() };
            unsafe { rados_buffer_free(p) };
            v
        };
        let out = take(outbuf, outbuflen);
        let status = take(outs, outslen);
        if r < 0 {
            bail!("mon command failed ({r}): {}", String::from_utf8_lossy(&status));
        }
        Ok(out)
    }

    pub fn kv_get(&self, key: &str) -> Result<Vec<u8>> {
        self.mon_command(&serde_json::json!({"prefix": "config-key get", "key": key}).to_string(), b"")
    }

    pub fn kv_set(&self, key: &str, val: &[u8]) -> Result<()> {
        self.mon_command(&serde_json::json!({"prefix": "config-key set", "key": key}).to_string(), val)?;
        Ok(())
    }
}

impl Drop for Rados {
    fn drop(&mut self) {
        unsafe { rados_shutdown(self.0) }
    }
}
