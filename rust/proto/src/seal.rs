//! Secrets sealed to one host: the mgr encrypts a keyring or credential to
//! the host's X25519 key, so agentd stores and relays ciphertext it can't
//! read.
//!
//! Format: "CSEAL1" || ephemeral public key (32) || ChaCha20-Poly1305
//! ciphertext. The key is HKDF-SHA256 of the X25519 shared secret, salted
//! with both public keys; every message has a fresh ephemeral key, so a zero
//! nonce is safe.

use anyhow::{bail, Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use x25519_dalek::{PublicKey, StaticSecret};

const MAGIC: &[u8] = b"CSEAL1";
const INFO: &[u8] = b"ceph-agent seal v1";

fn random32() -> Result<[u8; 32]> {
    use std::io::Read as _;
    let mut b = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b)
}

pub fn generate() -> Result<StaticSecret> {
    Ok(StaticSecret::from(random32()?))
}

pub fn public_of(secret: &StaticSecret) -> [u8; 32] {
    PublicKey::from(secret).to_bytes()
}

fn key(shared: &[u8; 32], eph: &[u8; 32], recipient: &[u8; 32]) -> Key {
    let mut salt = [0u8; 64];
    salt[..32].copy_from_slice(eph);
    salt[32..].copy_from_slice(recipient);
    let hk = hkdf::Hkdf::<sha2::Sha256>::new(Some(&salt), shared);
    let mut k = [0u8; 32];
    hk.expand(INFO, &mut k).expect("32 bytes is a valid HKDF length");
    k.into()
}

pub fn is_sealed(data: &[u8]) -> bool {
    data.starts_with(MAGIC)
}

pub fn seal(recipient: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let eph = StaticSecret::from(random32()?);
    let eph_pub = PublicKey::from(&eph).to_bytes();
    let shared = eph.diffie_hellman(&PublicKey::from(*recipient));
    let cipher = ChaCha20Poly1305::new(&key(shared.as_bytes(), &eph_pub, recipient));
    let ct = cipher.encrypt(Nonce::from_slice(&[0u8; 12]), plaintext).map_err(|_| anyhow::anyhow!("encryption failed"))?;
    let mut out = Vec::with_capacity(MAGIC.len() + 32 + ct.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&eph_pub);
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn open(secret: &StaticSecret, sealed: &[u8]) -> Result<Vec<u8>> {
    if !is_sealed(sealed) || sealed.len() < MAGIC.len() + 32 + 16 {
        bail!("not a sealed blob");
    }
    let eph_pub: [u8; 32] = sealed[MAGIC.len()..MAGIC.len() + 32].try_into().unwrap();
    let me = public_of(secret);
    let shared = secret.diffie_hellman(&PublicKey::from(eph_pub));
    let cipher = ChaCha20Poly1305::new(&key(shared.as_bytes(), &eph_pub, &me));
    cipher
        .decrypt(Nonce::from_slice(&[0u8; 12]), &sealed[MAGIC.len() + 32..])
        .map_err(|_| anyhow::anyhow!("sealed blob is not for this host, or was altered"))
}

pub fn load_secret(path: &std::path::Path) -> Result<StaticSecret> {
    let b: [u8; 32] = crate::intent::unb64(&std::fs::read_to_string(path).with_context(|| format!("{}", path.display()))?)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("{}: not a 32-byte key", path.display()))?;
    Ok(StaticSecret::from(b))
}

pub fn parse_public(b64: &str) -> Result<[u8; 32]> {
    crate::intent::unb64(b64)?.try_into().map_err(|_| anyhow::anyhow!("not a 32-byte public key"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_wrong_host() {
        let host = generate().unwrap();
        let other = generate().unwrap();
        let s = seal(&public_of(&host), b"[client.osd.3]\n\tkey = AQ...\n").unwrap();
        assert!(is_sealed(&s));
        assert_eq!(open(&host, &s).unwrap(), b"[client.osd.3]\n\tkey = AQ...\n");
        assert!(open(&other, &s).is_err());
        let mut t = s.clone();
        let n = t.len();
        t[n - 1] ^= 1;
        assert!(open(&host, &t).is_err());
    }
}
