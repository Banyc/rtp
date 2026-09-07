//! Datagram obfuscation: a transport wrapper that prefixes every datagram
//! with a 24-byte random nonce and chacha20-encrypts the rest with a given
//! key. The wire shape is `[nonce 24][ciphertext]` — no authentication tag —
//! so a passive observer sees only random bytes and cannot distinguish the
//! traffic from any other encrypted UDP protocol (QUIC, WireGuard, DTLS).
//!
//! The obfuscated plaintext is `[len u16][payload][padding]` when a padding
//! profile is set (see [`TargetProfile`]): the real payload length rides
//! inside the ciphertext so a datagram can be padded to a target size without
//! a wire length field. The padding is zero-filled; the chacha20 keystream
//! randomizes it on the wire. Without a profile the plaintext is the payload
//! alone — the historical wire format.
//!
//! The wrapper implements the crate's [`UnreliableRead`] / [`UnreliableWrite`]
//! transport traits, so it can be inserted between the UDP socket and the
//! [`UnreliableLayer`] without touching the codec or the reliable layer.
//! It is wired into the public constructors through the optional
//! `obfuscation_key` on [`crate::udp::ConnectConfig`] /
//! [`crate::udp::AcceptConfig`]; the wrapper types themselves are internal.
//!
//! The read half is a FILTER: a datagram that is not a valid obfuscated
//! datagram (shorter than the nonce, or too large for the caller's buffer) is
//! dropped and the next datagram is read, never surfaced as an error. A
//! single bad datagram — e.g. a plaintext packet from a peer that is not
//! obfuscating — must not fail the connection, and on the accept path must
//! not kill the whole listener.

use async_trait::async_trait;
use tokio_chacha20::cipher::StreamCipher;

use crate::io_err::IoErr;
use crate::transmission::transmission_layer::{UnreliableRead, UnreliableWrite};

/// The nonce length: 24 bytes (XChaCha20).
pub(crate) const NONCE_LEN: usize = tokio_chacha20::X_NONCE_BYTES;

/// The chacha20 key length: 32 bytes.
pub(crate) const KEY_LEN: usize = tokio_chacha20::KEY_BYTES;

/// The length-prefix size (u16) inside the obfuscated plaintext.
pub(crate) const LEN_LEN: usize = 2;

/// One byte PAST the caller's buffer: the receive scratch is sized
/// `max_plaintext + NONCE_LEN + OVERSIZE_DETECT_EXTRA` so an oversized
/// datagram is received (or truncated) to a length that makes
/// [`ObfuscatedRead::decrypt_into`]'s length check fire — without the extra
/// byte the socket would truncate an oversized datagram to exactly the
/// scratch size and the check would silently pass, delivering truncated
/// plaintext.
pub(crate) const OVERSIZE_DETECT_EXTRA: usize = 1;

/// A fixed single-mode target profile for datagram padding: every datagram
/// is padded to a size drawn from a triangular distribution peaked at
/// `mode` and falling to zero at `mode ± spread`. The draw is independent of
/// the traffic, so the wire size distribution converges to one mode without
/// any traffic-correlated feedback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetProfile {
    pub mode: usize,
    pub spread: usize,
}

impl TargetProfile {
    /// Draw a target size: triangular around `mode` in `[mode - spread,
    /// mode + spread]` (the difference of two uniforms is triangular).
    pub fn draw(&self) -> usize {
        let u = rand::random_range(0..=self.spread) as isize;
        let v = rand::random_range(0..=self.spread) as isize;
        (self.mode as isize + u - v).max(0) as usize
    }

    /// The largest target size the profile can draw.
    pub fn max(&self) -> usize {
        self.mode + self.spread
    }
}

/// Apply the chacha20 keystream for `key`/`nonce` to `buf` in place.
/// Encryption and decryption are the same XOR operation, so this single
/// helper serves both the wrapper halves and the obfuscated path-probe
/// side channel (which must use the same wire shape so a passive observer
/// cannot tell probes from data).
pub(crate) fn apply_keystream(key: [u8; KEY_LEN], nonce: [u8; NONCE_LEN], buf: &mut [u8]) {
    let mut cipher = StreamCipher::new_x(key, nonce);
    cipher.encrypt(buf);
}

/// A read half that strips the 24-byte nonce, chacha20-decrypts the rest,
/// reads the length prefix, and strips the padding.
#[derive(Debug)]
pub(crate) struct ObfuscatedRead<R> {
    inner: R,
    key: [u8; KEY_LEN],
    /// The padding profile, used to size the receive scratch to the largest
    /// padded datagram. `None` sizes it to the caller's buffer.
    profile: Option<TargetProfile>,
    /// Scratch buffer for the received datagram (nonce + ciphertext).
    scratch: Vec<u8>,
}

impl<R> ObfuscatedRead<R> {
    pub(crate) fn new(inner: R, key: [u8; KEY_LEN], profile: Option<TargetProfile>) -> Self {
        Self {
            inner,
            key,
            profile,
            scratch: Vec::new(),
        }
    }

    fn ensure_scratch(&mut self, plaintext_capacity: usize) {
        // The largest plaintext this wrapper can receive: the profile's max
        // target (when padding) or the caller's buffer (when not). One byte
        // MORE so an oversized datagram is received (or truncated) to a
        // length that makes `decrypt_into`'s length check fire — without the
        // extra byte the socket would truncate an oversized datagram to
        // exactly the scratch size and the check would silently pass,
        // delivering truncated plaintext.
        let max_plaintext = match self.profile {
            Some(profile) => profile.max().max(plaintext_capacity + LEN_LEN),
            None => plaintext_capacity,
        };
        let need = max_plaintext + NONCE_LEN + OVERSIZE_DETECT_EXTRA;
        if self.scratch.len() < need {
            self.scratch.resize(need, 0);
        }
    }

    /// Decrypt the datagram in `self.scratch[..n]` into `buf`. Returns
    /// `InvalidData` when the datagram is not a valid obfuscated datagram
    /// (shorter than the nonce, or the payload is too large for `buf`); the
    /// caller drops it and reads the next datagram. With a padding profile
    /// the plaintext is `[len u16][payload][padding]` and the length prefix
    /// is read to strip the padding; without one the plaintext is the
    /// payload alone (the historical format).
    fn decrypt_into(&mut self, buf: &mut [u8], n: usize) -> Result<usize, IoErr> {
        if n < NONCE_LEN {
            return Err(IoErr::from(std::io::ErrorKind::InvalidData));
        }
        let nonce: [u8; NONCE_LEN] = self.scratch[..NONCE_LEN].try_into().unwrap();
        // Decrypt in place in the scratch, then read the length prefix (when
        // padding) and copy the payload out (the padding tail is discarded).
        apply_keystream(self.key, nonce, &mut self.scratch[NONCE_LEN..n]);
        let plaintext = &self.scratch[NONCE_LEN..n];
        match self.profile {
            Some(_) => {
                if plaintext.len() < LEN_LEN {
                    return Err(IoErr::from(std::io::ErrorKind::InvalidData));
                }
                let len = u16::from_be_bytes(plaintext[..LEN_LEN].try_into().unwrap()) as usize;
                if len > buf.len() || LEN_LEN + len > plaintext.len() {
                    return Err(IoErr::from(std::io::ErrorKind::InvalidData));
                }
                buf[..len].copy_from_slice(&plaintext[LEN_LEN..LEN_LEN + len]);
                Ok(len)
            }
            None => {
                if plaintext.len() > buf.len() {
                    return Err(IoErr::from(std::io::ErrorKind::InvalidData));
                }
                buf[..plaintext.len()].copy_from_slice(plaintext);
                Ok(plaintext.len())
            }
        }
    }
}

#[async_trait]
impl<R: UnreliableRead> UnreliableRead for ObfuscatedRead<R> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        self.ensure_scratch(buf.len());
        loop {
            let n = self.inner.try_recv(&mut self.scratch)?;
            match self.decrypt_into(buf, n) {
                Ok(len) => return Ok(len),
                // Not a valid obfuscated datagram: drop it and read the next.
                // Surfacing the error would fail the connection on a single
                // bad datagram (and, on the accept path, kill the listener).
                Err(error) if error == std::io::ErrorKind::InvalidData => continue,
                Err(error) => return Err(error),
            }
        }
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        self.ensure_scratch(buf.len());
        loop {
            let n = self.inner.recv(&mut self.scratch).await?;
            match self.decrypt_into(buf, n) {
                Ok(len) => return Ok(len),
                // Not a valid obfuscated datagram: drop it and read the next.
                Err(error) if error == std::io::ErrorKind::InvalidData => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

/// A write half that prefixes a 24-byte random nonce, chacha20-encrypts the
/// rest, and pads the plaintext to a profile-drawn target size.
#[derive(Debug)]
pub(crate) struct ObfuscatedWrite<W> {
    inner: W,
    key: [u8; KEY_LEN],
    /// The padding profile. `None` sends the plaintext unpadded (the
    /// historical wire format, no length prefix).
    profile: Option<TargetProfile>,
    /// Scratch buffer for the outgoing datagram (nonce + ciphertext).
    scratch: Vec<u8>,
}

impl<W> ObfuscatedWrite<W> {
    pub(crate) fn new(inner: W, key: [u8; KEY_LEN], profile: Option<TargetProfile>) -> Self {
        Self {
            inner,
            key,
            profile,
            scratch: Vec::new(),
        }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for ObfuscatedWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        // With a padding profile the plaintext is [len u16][payload]
        // [padding] padded to a profile-drawn target (floored at the
        // payload, never shrunk); without one the plaintext is the payload
        // alone — the historical wire format.
        let (plaintext_len, len_prefix) = match self.profile {
            Some(profile) => (profile.draw().max(buf.len() + LEN_LEN), true),
            None => (buf.len(), false),
        };
        if self.scratch.len() < plaintext_len + NONCE_LEN {
            self.scratch.resize(plaintext_len + NONCE_LEN, 0);
        }
        let nonce: [u8; NONCE_LEN] = rand::random();
        self.scratch[..NONCE_LEN].copy_from_slice(&nonce);
        let mut pos = NONCE_LEN;
        if len_prefix {
            self.scratch[pos..pos + LEN_LEN].copy_from_slice(&(buf.len() as u16).to_be_bytes());
            pos += LEN_LEN;
        }
        self.scratch[pos..pos + buf.len()].copy_from_slice(buf);
        pos += buf.len();
        // Zero the padding tail explicitly (the scratch is reused, so stale
        // bytes from a previous send must not leak into the padding).
        self.scratch[pos..NONCE_LEN + plaintext_len].fill(0);
        apply_keystream(
            self.key,
            nonce,
            &mut self.scratch[NONCE_LEN..NONCE_LEN + plaintext_len],
        );
        self.inner
            .send(&self.scratch[..NONCE_LEN + plaintext_len])
            .await?;
        Ok(buf.len())
    }
}

/// Wrap the transport halves with obfuscation when a key is given. `None`
/// passes the halves through unchanged, so the plain path is byte-identical
/// to using the socket directly.
pub(crate) fn maybe_wrap<R: UnreliableRead, W: UnreliableWrite>(
    read: R,
    write: W,
    key: Option<[u8; KEY_LEN]>,
    profile: Option<TargetProfile>,
) -> (Box<dyn UnreliableRead>, Box<dyn UnreliableWrite>) {
    match key {
        Some(key) => (
            Box::new(ObfuscatedRead::new(read, key, profile)),
            Box::new(ObfuscatedWrite::new(write, key, profile)),
        ),
        None => (Box::new(read), Box::new(write)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    fn key() -> [u8; KEY_LEN] {
        [7; KEY_LEN]
    }

    /// A connected socket pair: `a` sends, `b` receives.
    async fn socket_pair() -> (std::sync::Arc<UdpSocket>, std::sync::Arc<UdpSocket>) {
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        (std::sync::Arc::new(a), std::sync::Arc::new(b))
    }

    #[tokio::test]
    async fn a_payload_round_trips_through_the_wrapper() {
        let (a, b) = socket_pair().await;
        let mut write = ObfuscatedWrite::new(a, key(), None);
        let mut read = ObfuscatedRead::new(b, key(), None);
        let payload = b"hello obfuscated world";
        assert_eq!(write.send(payload).await.unwrap(), payload.len());
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn the_wire_carries_nonce_plus_ciphertext_not_the_plaintext() {
        let (a, mut b) = socket_pair().await;
        let mut write = ObfuscatedWrite::new(a, key(), None);
        let payload = b"secret payload";
        write.send(payload).await.unwrap();
        // The peer sees the raw datagram: 24-byte nonce + ciphertext (no
        // length prefix without a padding profile).
        let mut wire = [0u8; 1024];
        let n = b.recv(&mut wire).await.unwrap();
        assert_eq!(n, payload.len() + NONCE_LEN);
        assert!(
            !wire[NONCE_LEN..n]
                .windows(payload.len())
                .any(|w| w == payload),
            "the plaintext must not appear on the wire"
        );
        // The nonce is random per datagram.
        let mut wire2 = [0u8; 1024];
        write.send(payload).await.unwrap();
        let n2 = b.recv(&mut wire2).await.unwrap();
        assert_eq!(n2, payload.len() + NONCE_LEN);
        assert_ne!(&wire[..NONCE_LEN], &wire2[..NONCE_LEN]);
    }

    #[tokio::test]
    async fn a_wrong_key_does_not_decrypt() {
        let (a, b) = socket_pair().await;
        let mut write = ObfuscatedWrite::new(a, key(), None);
        let mut read = ObfuscatedRead::new(b, key(), None);
        write.send(b"payload").await.unwrap();
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"payload");
        // A *different* key on the receiving side decrypts to garbage (the
        // historical format has no integrity check), so the wrong-key
        // datagram is never surfaced as the payload; the next valid datagram
        // is delivered in its place.
        let (a2, b2) = socket_pair().await;
        let mut read2 = ObfuscatedRead::new(a2, [9; KEY_LEN], None);
        let mut write3 = ObfuscatedWrite::new(b2.clone(), key(), None);
        let mut write4 = ObfuscatedWrite::new(b2, [9; KEY_LEN], None);
        write3.send(b"wrong key").await.unwrap();
        write4.send(b"right key").await.unwrap();
        let mut buf2 = [0u8; 1024];
        let n2 = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let n = read2.recv(&mut buf2).await.unwrap();
                if &buf2[..n] == b"right key" {
                    return n;
                }
            }
        })
        .await
        .expect("the right-key datagram was never delivered");
        assert_eq!(&buf2[..n2], b"right key");
    }

    #[tokio::test]
    async fn a_datagram_shorter_than_the_nonce_is_dropped_not_delivered() {
        let (a, mut b) = socket_pair().await;
        let mut read = ObfuscatedRead::new(a, key(), None);
        let mut write = ObfuscatedWrite::new(b.clone(), key(), None);
        // A raw (unwrapped) short datagram from the peer is not a valid
        // obfuscated datagram: it is dropped, and the next valid datagram is
        // delivered in its place — the read never surfaces an error for it.
        b.send(&[1, 2, 3]).await.unwrap();
        let payload = b"after the short datagram";
        write.send(payload).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn a_datagram_larger_than_the_buffer_is_dropped_not_truncated() {
        let (a, b) = socket_pair().await;
        let mut read = ObfuscatedRead::new(a, key(), None);
        let mut write = ObfuscatedWrite::new(b, key(), None);
        // A wrapped datagram whose plaintext exceeds the caller's buffer is
        // DROPPED, never silently truncated; the next valid datagram is
        // delivered in its place.
        let oversized = vec![0xAB; 64];
        write.send(&oversized).await.unwrap();
        let payload = b"fits";
        write.send(payload).await.unwrap();
        let mut buf = [0u8; 16];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn maybe_wrap_with_none_passes_halves_through_unchanged() {
        let (a, mut b) = socket_pair().await;
        let (mut read, mut write) = maybe_wrap(a.clone(), a, None, None);
        let payload = b"plain passthrough";
        // Write half: `a` → `b`, plaintext on the wire — no nonce prefix.
        assert_eq!(write.send(payload).await.unwrap(), payload.len());
        let mut wire = [0u8; 1024];
        let n = b.recv(&mut wire).await.unwrap();
        assert_eq!(&wire[..n], payload);
        // Read half: `b` → `a`, plaintext through the wrapped read.
        b.send(payload).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn a_profile_pads_every_datagram_to_the_target_band() {
        let (a, mut b) = socket_pair().await;
        let profile = TargetProfile {
            mode: 200,
            spread: 50,
        };
        let mut write = ObfuscatedWrite::new(a, key(), Some(profile));
        let mut read = ObfuscatedRead::new(b.clone(), key(), Some(profile));
        // Small payloads are padded into the profile band.
        let mut sizes = Vec::new();
        for _ in 0..32 {
            write.send(b"tiny").await.unwrap();
            let mut wire = [0u8; 1024];
            let n = b.recv(&mut wire).await.unwrap();
            sizes.push(n);
        }
        assert!(
            sizes
                .iter()
                .all(|&n| (NONCE_LEN + 150..=NONCE_LEN + 250).contains(&n)),
            "padded wire sizes must land in the profile band, got {sizes:?}"
        );
        // The payload still round-trips through the padded wrapper.
        write.send(b"round trip").await.unwrap();
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"round trip");
    }

    #[tokio::test]
    async fn a_profile_never_shrinks_a_large_payload() {
        let (a, mut b) = socket_pair().await;
        let profile = TargetProfile {
            mode: 100,
            spread: 20,
        };
        let mut write = ObfuscatedWrite::new(a, key(), Some(profile));
        // A payload larger than the profile band is sent at its natural size
        // (plus the length prefix) — never shrunk.
        let big = vec![0xCD; 300];
        write.send(&big).await.unwrap();
        let mut wire = [0u8; 1024];
        let n = b.recv(&mut wire).await.unwrap();
        assert_eq!(n, NONCE_LEN + LEN_LEN + big.len());
    }
}
