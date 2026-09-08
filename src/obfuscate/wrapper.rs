//! The obfuscation wrapper: the only layer that composes the masking layer
//! ([`super::mask`]) with the padding format ([`super::padding`]). A write
//! pads the payload under the profile, masks it under a fresh nonce, and
//! sends `[nonce 24][ciphertext]`; a read strips the nonce, applies the
//! keystream, and unwraps the padded plaintext into the caller's buffer.
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

use super::mask::{KEY_LEN, NONCE_LEN, apply_keystream};
use super::padding::{self, PaddingSettings};
use crate::io_err::IoErr;
use crate::transmission::transmission_layer::{UnreliableRead, UnreliableWrite};

/// One byte PAST the caller's buffer: the receive scratch is sized
/// `max_plaintext + NONCE_LEN + OVERSIZE_DETECT_EXTRA` so an oversized
/// datagram is received (or truncated) to a length that makes
/// [`ObfuscatedRead::decrypt_into`]'s length check fire — without the extra
/// byte the socket would truncate an oversized datagram to exactly the
/// scratch size and the check would silently pass, delivering truncated
/// plaintext.
pub(crate) const OVERSIZE_DETECT_EXTRA: usize = 1;

/// The obfuscation settings for one transport half: the masking key and the
/// padding settings (`None` = historical unpadded format).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Obfuscation {
    pub key: [u8; KEY_LEN],
    pub settings: Option<PaddingSettings>,
}

/// A read half that strips the 24-byte nonce, chacha20-decrypts the rest,
/// reads the length prefix, and strips the padding.
#[derive(Debug)]
pub(crate) struct ObfuscatedRead<R> {
    inner: R,
    settings: Obfuscation,
    /// Scratch buffer for the received datagram (nonce + ciphertext).
    scratch: Vec<u8>,
}

impl<R> ObfuscatedRead<R> {
    pub(crate) fn new(inner: R, settings: Obfuscation) -> Self {
        Self {
            inner,
            settings,
            scratch: Vec::new(),
        }
    }

    fn ensure_scratch(&mut self, plaintext_capacity: usize) {
        // The largest plaintext this wrapper can receive: the settings' max
        // target (when padding) or the caller's buffer (when not). One byte
        // MORE so an oversized datagram is received (or truncated) to a
        // length that makes `decrypt_into`'s length check fire — without the
        // extra byte the socket would truncate an oversized datagram to
        // exactly the scratch size and the check would silently pass,
        // delivering truncated plaintext.
        let max_plaintext = padding::max_plaintext(plaintext_capacity, self.settings.settings);
        let need = max_plaintext + NONCE_LEN + OVERSIZE_DETECT_EXTRA;
        if self.scratch.len() < need {
            self.scratch.resize(need, 0);
        }
    }

    /// Decrypt the datagram in `self.scratch[..n]` into `buf`. Returns
    /// `InvalidData` when the datagram is not a valid obfuscated datagram
    /// (shorter than the nonce, or the payload is too large for `buf`); the
    /// caller drops it and reads the next datagram. With padding settings
    /// the plaintext is `[len u16][payload][padding]` and the length prefix
    /// is read to strip the padding; without them the plaintext is the
    /// payload alone (the historical format).
    fn decrypt_into(&mut self, buf: &mut [u8], n: usize) -> Result<usize, IoErr> {
        if n < NONCE_LEN {
            return Err(IoErr::from(std::io::ErrorKind::InvalidData));
        }
        let nonce: [u8; NONCE_LEN] = self.scratch[..NONCE_LEN].try_into().unwrap();
        // Decrypt in place in the scratch, then read the length prefix (when
        // padding) and copy the payload out (the padding tail is discarded).
        apply_keystream(self.settings.key, nonce, &mut self.scratch[NONCE_LEN..n]);
        let plaintext = &self.scratch[NONCE_LEN..n];
        padding::decode_plaintext(plaintext, buf, self.settings.settings)
            .ok_or_else(|| IoErr::from(std::io::ErrorKind::InvalidData))
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
/// rest, and pads the plaintext to a settings-drawn target size.
#[derive(Debug)]
pub(crate) struct ObfuscatedWrite<W> {
    inner: W,
    settings: Obfuscation,
    /// Scratch buffer for the outgoing datagram (nonce + ciphertext).
    scratch: Vec<u8>,
}

impl<W> ObfuscatedWrite<W> {
    pub(crate) fn new(inner: W, settings: Obfuscation) -> Self {
        Self {
            inner,
            settings,
            scratch: Vec::new(),
        }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for ObfuscatedWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        // With padding settings the plaintext is [len u16][payload]
        // [padding] padded to a settings-drawn target (floored at the
        // payload, never shrunk); without them the plaintext is the payload
        // alone — the historical wire format.
        let max_plaintext = padding::max_plaintext(buf.len(), self.settings.settings);
        if self.scratch.len() < max_plaintext + NONCE_LEN {
            self.scratch.resize(max_plaintext + NONCE_LEN, 0);
        }
        let nonce: [u8; NONCE_LEN] = rand::random();
        self.scratch[..NONCE_LEN].copy_from_slice(&nonce);
        let plaintext_len = padding::encode_plaintext(
            buf,
            &mut self.scratch[NONCE_LEN..NONCE_LEN + max_plaintext],
            self.settings.settings,
        );
        apply_keystream(
            self.settings.key,
            nonce,
            &mut self.scratch[NONCE_LEN..NONCE_LEN + plaintext_len],
        );
        self.inner
            .send(&self.scratch[..NONCE_LEN + plaintext_len])
            .await?;
        Ok(buf.len())
    }
}

/// Wrap the transport halves with obfuscation when settings are given.
/// `None` passes the halves through unchanged, so the plain path is
/// byte-identical to using the socket directly.
pub(crate) fn maybe_wrap<R: UnreliableRead, W: UnreliableWrite>(
    read: R,
    write: W,
    settings: Option<Obfuscation>,
) -> (Box<dyn UnreliableRead>, Box<dyn UnreliableWrite>) {
    match settings {
        Some(settings) => (
            Box::new(ObfuscatedRead::new(read, settings)),
            Box::new(ObfuscatedWrite::new(write, settings)),
        ),
        None => (Box::new(read), Box::new(write)),
    }
}

#[cfg(test)]
mod tests {
    use super::padding::{PayloadSized, TargetKind};
    use super::*;
    use tokio::net::UdpSocket;

    fn settings() -> Obfuscation {
        Obfuscation {
            key: [7; KEY_LEN],
            settings: None,
        }
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
        let mut write = ObfuscatedWrite::new(a, settings());
        let mut read = ObfuscatedRead::new(b, settings());
        let payload = b"hello obfuscated world";
        assert_eq!(write.send(payload).await.unwrap(), payload.len());
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn the_wire_carries_nonce_plus_ciphertext_not_the_plaintext() {
        let (a, mut b) = socket_pair().await;
        let mut write = ObfuscatedWrite::new(a, settings());
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
        let mut write = ObfuscatedWrite::new(a, settings());
        let mut read = ObfuscatedRead::new(b, settings());
        write.send(b"payload").await.unwrap();
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"payload");
        // A *different* key on the receiving side decrypts to garbage (the
        // historical format has no integrity check), so the wrong-key
        // datagram is never surfaced as the payload; the next valid datagram
        // is delivered in its place.
        let (a2, b2) = socket_pair().await;
        let mut read2 = ObfuscatedRead::new(
            a2,
            Obfuscation {
                key: [9; KEY_LEN],
                settings: None,
            },
        );
        let mut write3 = ObfuscatedWrite::new(b2.clone(), settings());
        let mut write4 = ObfuscatedWrite::new(
            b2,
            Obfuscation {
                key: [9; KEY_LEN],
                settings: None,
            },
        );
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
        let mut read = ObfuscatedRead::new(a, settings());
        let mut write = ObfuscatedWrite::new(b.clone(), settings());
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
        let mut read = ObfuscatedRead::new(a, settings());
        let mut write = ObfuscatedWrite::new(b, settings());
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
        let (mut read, mut write) = maybe_wrap(a.clone(), a, None);
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
        let settings = Obfuscation {
            key: [7; KEY_LEN],
            settings: Some(PaddingSettings {
                target: TargetKind::Triangular {
                    mode: 200,
                    spread: 50,
                },
                payload_sized: PayloadSized::Dynamic,
            }),
        };
        let mut write = ObfuscatedWrite::new(a, settings);
        let mut read = ObfuscatedRead::new(b.clone(), settings);
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
    async fn a_fixed_profile_pads_every_datagram_to_the_same_size() {
        let (a, mut b) = socket_pair().await;
        let settings = Obfuscation {
            key: [7; KEY_LEN],
            settings: Some(PaddingSettings {
                target: TargetKind::Fixed(200),
                payload_sized: PayloadSized::Dynamic,
            }),
        };
        let mut write = ObfuscatedWrite::new(a, settings);
        let mut read = ObfuscatedRead::new(b.clone(), settings);
        // Small payloads are padded to exactly the fixed size.
        let mut sizes = Vec::new();
        for _ in 0..16 {
            write.send(b"tiny").await.unwrap();
            let mut wire = [0u8; 1024];
            let n = b.recv(&mut wire).await.unwrap();
            sizes.push(n);
        }
        assert!(
            sizes.iter().all(|&n| n == NONCE_LEN + 200),
            "padded wire sizes must all be the fixed size, got {sizes:?}"
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
        let mut write = ObfuscatedWrite::new(
            a,
            Obfuscation {
                key: [7; KEY_LEN],
                settings: Some(PaddingSettings {
                    target: TargetKind::Triangular {
                        mode: 100,
                        spread: 20,
                    },
                    payload_sized: PayloadSized::Dynamic,
                }),
            },
        );
        // A payload larger than the profile band is sent at its natural size
        // (plus the length prefix) — never shrunk.
        let big = vec![0xCD; 300];
        write.send(&big).await.unwrap();
        let mut wire = [0u8; 1024];
        let n = b.recv(&mut wire).await.unwrap();
        assert_eq!(n, NONCE_LEN + padding::LEN_LEN + big.len());
    }
}
