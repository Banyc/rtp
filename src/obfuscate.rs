//! Datagram obfuscation: a transport wrapper that prefixes every datagram
//! with a 24-byte random nonce and chacha20-encrypts the rest with a given
//! key. The wire shape is `[nonce 24][ciphertext]` — no length field, no
//! authentication tag — so a passive observer sees only random bytes and
//! cannot distinguish the traffic from any other encrypted UDP protocol
//! (QUIC, WireGuard, DTLS).
//!
//! The wrapper implements the crate's [`UnreliableRead`] / [`UnreliableWrite`]
//! transport traits, so it can be inserted between the UDP socket and the
//! [`UnreliableLayer`] without touching the codec or the reliable layer.

use async_trait::async_trait;
use tokio_chacha20::cipher::StreamCipher;

use crate::io_err::IoErr;
use crate::transmission::transmission_layer::{UnreliableRead, UnreliableWrite};

/// The nonce length: 24 bytes (XChaCha20).
pub const NONCE_LEN: usize = tokio_chacha20::X_NONCE_BYTES;

/// A read half that strips the 24-byte nonce and chacha20-decrypts the rest.
#[derive(Debug)]
pub struct ObfuscatedRead<R> {
    inner: R,
    key: [u8; tokio_chacha20::KEY_BYTES],
    /// Scratch buffer for the received datagram (nonce + ciphertext).
    scratch: Vec<u8>,
}

impl<R> ObfuscatedRead<R> {
    pub fn new(inner: R, key: [u8; tokio_chacha20::KEY_BYTES]) -> Self {
        Self {
            inner,
            key,
            scratch: Vec::new(),
        }
    }

    fn ensure_scratch(&mut self, plaintext_capacity: usize) {
        if self.scratch.len() < plaintext_capacity + NONCE_LEN {
            self.scratch.resize(plaintext_capacity + NONCE_LEN, 0);
        }
    }

    /// Decrypt the datagram in `self.scratch[..n]` into `buf`.
    fn decrypt_into(&mut self, buf: &mut [u8], n: usize) -> Result<usize, IoErr> {
        if n < NONCE_LEN {
            return Err(IoErr::from(std::io::ErrorKind::InvalidData));
        }
        let nonce: [u8; NONCE_LEN] = self.scratch[..NONCE_LEN].try_into().unwrap();
        let ciphertext_len = n - NONCE_LEN;
        if ciphertext_len > buf.len() {
            return Err(IoErr::from(std::io::ErrorKind::InvalidData));
        }
        buf[..ciphertext_len].copy_from_slice(&self.scratch[NONCE_LEN..n]);
        // chacha20 encryption and decryption are the same XOR operation.
        let mut cipher = StreamCipher::new_x(self.key, nonce);
        cipher.encrypt(&mut buf[..ciphertext_len]);
        Ok(ciphertext_len)
    }
}

#[async_trait]
impl<R: UnreliableRead> UnreliableRead for ObfuscatedRead<R> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        self.ensure_scratch(buf.len());
        let n = self.inner.try_recv(&mut self.scratch)?;
        self.decrypt_into(buf, n)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        self.ensure_scratch(buf.len());
        let n = self.inner.recv(&mut self.scratch).await?;
        self.decrypt_into(buf, n)
    }
}

/// A write half that prefixes a 24-byte random nonce and chacha20-encrypts
/// the rest.
#[derive(Debug)]
pub struct ObfuscatedWrite<W> {
    inner: W,
    key: [u8; tokio_chacha20::KEY_BYTES],
    /// Scratch buffer for the outgoing datagram (nonce + ciphertext).
    scratch: Vec<u8>,
}

impl<W> ObfuscatedWrite<W> {
    pub fn new(inner: W, key: [u8; tokio_chacha20::KEY_BYTES]) -> Self {
        Self {
            inner,
            key,
            scratch: Vec::new(),
        }
    }
}

#[async_trait]
impl<W: UnreliableWrite> UnreliableWrite for ObfuscatedWrite<W> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        if self.scratch.len() < buf.len() + NONCE_LEN {
            self.scratch.resize(buf.len() + NONCE_LEN, 0);
        }
        let nonce: [u8; NONCE_LEN] = rand::random();
        self.scratch[..NONCE_LEN].copy_from_slice(&nonce);
        self.scratch[NONCE_LEN..NONCE_LEN + buf.len()].copy_from_slice(buf);
        let mut cipher = StreamCipher::new_x(self.key, nonce);
        cipher.encrypt(&mut self.scratch[NONCE_LEN..NONCE_LEN + buf.len()]);
        self.inner
            .send(&self.scratch[..NONCE_LEN + buf.len()])
            .await?;
        Ok(buf.len())
    }
}

/// Wrap a connected UDP socket with the obfuscation layer, returning the
/// read and write halves. The socket must be connected (or otherwise
/// usable for both `recv` and `send`).
pub fn wrap_connected_socket(
    socket: std::sync::Arc<tokio::net::UdpSocket>,
    key: [u8; tokio_chacha20::KEY_BYTES],
) -> (
    ObfuscatedRead<std::sync::Arc<tokio::net::UdpSocket>>,
    ObfuscatedWrite<std::sync::Arc<tokio::net::UdpSocket>>,
) {
    (
        ObfuscatedRead::new(socket.clone(), key),
        ObfuscatedWrite::new(socket, key),
    )
}

/// Build the transport halves for a connected socket, obfuscating only
/// when a key is given. `None` passes datagrams through unchanged, so the
/// obfuscation layer is strictly opt-in: the plain path is byte-identical
/// to using the socket directly.
pub fn wrap_connected_socket_opt(
    socket: std::sync::Arc<tokio::net::UdpSocket>,
    key: Option<[u8; tokio_chacha20::KEY_BYTES]>,
) -> (Box<dyn UnreliableRead>, Box<dyn UnreliableWrite>) {
    match key {
        Some(key) => {
            let (read, write) = wrap_connected_socket(socket, key);
            (Box::new(read), Box::new(write))
        }
        None => (Box::new(socket.clone()), Box::new(socket)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    fn key() -> [u8; tokio_chacha20::KEY_BYTES] {
        [7; tokio_chacha20::KEY_BYTES]
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
        // `a` sends, `b` receives: the write half wraps `a`, the read half
        // wraps `b`.
        let mut write = ObfuscatedWrite::new(a, key());
        let mut read = ObfuscatedRead::new(b, key());
        let payload = b"hello obfuscated world";
        assert_eq!(write.send(payload).await.unwrap(), payload.len());
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], payload);
    }

    #[tokio::test]
    async fn the_wire_carries_nonce_plus_ciphertext_not_the_plaintext() {
        let (a, mut b) = socket_pair().await;
        let mut write = ObfuscatedWrite::new(a, key());
        let payload = b"secret payload";
        write.send(payload).await.unwrap();
        // The peer sees the raw datagram: 24-byte nonce + ciphertext.
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
        // Same key on both halves: the payload round-trips.
        let (a, b) = socket_pair().await;
        let mut write = ObfuscatedWrite::new(a, key());
        let mut read = ObfuscatedRead::new(b, key());
        write.send(b"payload").await.unwrap();
        let mut buf = [0u8; 1024];
        let n = read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"payload");
        // A *different* key on the receiving side must yield garbage.
        let (a2, b2) = socket_pair().await;
        let mut read2 = ObfuscatedRead::new(a2, [9; tokio_chacha20::KEY_BYTES]);
        let mut write3 = ObfuscatedWrite::new(b2, key());
        write3.send(b"payload").await.unwrap();
        let mut buf2 = [0u8; 1024];
        let n2 = read2.recv(&mut buf2).await.unwrap();
        assert_ne!(&buf2[..n2], b"payload");
    }

    #[tokio::test]
    async fn a_datagram_shorter_than_the_nonce_is_rejected() {
        let (a, mut b) = socket_pair().await;
        let mut read = ObfuscatedRead::new(a, key());
        // Send a raw (unwrapped) short datagram from the peer.
        b.send(&[1, 2, 3]).await.unwrap();
        let mut buf = [0u8; 1024];
        let err = read.recv(&mut buf).await.unwrap_err();
        assert_eq!(err, std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn a_none_key_passes_datagrams_through_unchanged() {
        let (a, mut b) = socket_pair().await;
        let (mut read, mut write) = wrap_connected_socket_opt(a, None);
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
}
