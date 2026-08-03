use crate::io_err::IoErr;
use crate::transmission::transmission_layer::UnreliableRead;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
const MAGIC: [u8; 8] = [0xf7, b'R', b'T', b'P', b'E', b'X', 1, 0];
pub const PROBE_LEN: usize = 32;
const DIR_PROBE: u8 = 0x00;
const DIR_ECHO: u8 = 0x01;
const DIR_OFFSET: usize = 8;
const NONCE_OFFSET: usize = 9;
const TIMESTAMP_OFFSET: usize = 17;
const RATE_PER_SOURCE: f64 = 16.0;
const BURST_PER_SOURCE: f64 = 32.0;
const MAX_TRACKED_SOURCES: usize = 4096;
pub fn encode_probe(nonce: u64, timestamp_micros: u64) -> [u8; PROBE_LEN] {
    let mut bytes = [0; PROBE_LEN];
    bytes[..MAGIC.len()].copy_from_slice(&MAGIC);
    bytes[DIR_OFFSET] = DIR_PROBE;
    bytes[NONCE_OFFSET..TIMESTAMP_OFFSET].copy_from_slice(&nonce.to_be_bytes());
    bytes[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8].copy_from_slice(&timestamp_micros.to_be_bytes());
    bytes
}
fn is_probe_packet(datagram: &[u8]) -> bool {
    datagram.len() == PROBE_LEN && datagram[..MAGIC.len()] == MAGIC
}
pub fn decode_echo(datagram: &[u8]) -> Option<(u64, u64)> {
    if !is_probe_packet(datagram) || datagram[DIR_OFFSET] != DIR_ECHO {
        return None;
    }
    let nonce = u64::from_be_bytes(datagram[NONCE_OFFSET..TIMESTAMP_OFFSET].try_into().unwrap());
    let timestamp = u64::from_be_bytes(
        datagram[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    Some((nonce, timestamp))
}
#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    updated: Instant,
}
impl TokenBucket {
    fn allow(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.updated).as_secs_f64();
        self.tokens = (self.tokens + elapsed * RATE_PER_SOURCE).min(BURST_PER_SOURCE);
        self.updated = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}
#[derive(Debug, Default)]
struct RateLimiter {
    buckets: HashMap<IpAddr, TokenBucket>,
}
impl RateLimiter {
    fn allow(&mut self, source: IpAddr, now: Instant) -> bool {
        if !self.buckets.contains_key(&source) && self.buckets.len() >= MAX_TRACKED_SOURCES {
            self.buckets.retain(|_, bucket| {
                let elapsed = now.duration_since(bucket.updated).as_secs_f64();
                (bucket.tokens + elapsed * RATE_PER_SOURCE) < BURST_PER_SOURCE
            });
            if self.buckets.len() >= MAX_TRACKED_SOURCES {
                return false;
            }
        }
        self.buckets
            .entry(source)
            .or_insert(TokenBucket {
                tokens: BURST_PER_SOURCE,
                updated: now,
            })
            .allow(now)
    }
}
#[derive(Debug)]
pub(crate) struct ProbeResponder {
    echo: Option<std::net::UdpSocket>,
    limiter: Mutex<RateLimiter>,
    send_error_count: AtomicUsize,
}
impl ProbeResponder {
    pub(crate) fn new(echo: Option<std::net::UdpSocket>) -> Self {
        Self {
            echo,
            limiter: Mutex::new(RateLimiter::default()),
            send_error_count: AtomicUsize::new(0),
        }
    }
    #[cfg(test)]
    pub(crate) fn send_error_count(&self) -> usize {
        self.send_error_count.load(Ordering::Relaxed)
    }
    pub(crate) fn observe(&self, from: &SocketAddr, datagram: &[u8]) -> bool {
        if !is_probe_packet(datagram) {
            return false;
        }
        if datagram[DIR_OFFSET] != DIR_PROBE {
            return true;
        }
        if !self
            .limiter
            .lock()
            .unwrap()
            .allow(from.ip(), Instant::now())
        {
            return true;
        }
        if let Some(echo) = &self.echo {
            let mut reply = [0; PROBE_LEN];
            reply.copy_from_slice(datagram);
            reply[DIR_OFFSET] = DIR_ECHO;
            if echo.send_to(&reply, from).is_err() {
                self.send_error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        true
    }
}
#[derive(Debug)]
pub struct ProbeTap {
    socket: Arc<tokio_udp::UdpSocket>,
    echoes: tokio::sync::mpsc::Receiver<u64>,
    dropped_echoes: Arc<AtomicUsize>,
}
impl ProbeTap {
    pub fn send_probe(&self, nonce: u64, timestamp_micros: u64) -> std::io::Result<()> {
        self.socket
            .try_send(&encode_probe(nonce, timestamp_micros))
            .map(drop)
    }
    pub fn try_recv_echo(&mut self) -> Option<u64> {
        self.echoes.try_recv().ok()
    }
    pub fn dropped_echoes(&self) -> usize {
        self.dropped_echoes.load(Ordering::Relaxed)
    }
}
pub(crate) fn client_probe_tap(
    socket: Arc<tokio_udp::UdpSocket>,
) -> (ProbeTap, EchoInterceptRead<Arc<tokio_udp::UdpSocket>>) {
    let (echo_tx, echoes) = tokio::sync::mpsc::channel(64);
    let dropped_echoes = Arc::new(AtomicUsize::new(0));
    let peer_ip = socket
        .peer_addr()
        .map(|addr| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    let read = EchoInterceptRead {
        inner: Arc::clone(&socket),
        echo_tx,
        dropped_echoes: Arc::clone(&dropped_echoes),
        limiter: Mutex::new(RateLimiter::default()),
        peer_ip,
    };
    (
        ProbeTap {
            socket,
            echoes,
            dropped_echoes,
        },
        read,
    )
}
#[derive(Debug)]
pub(crate) struct EchoInterceptRead<R> {
    inner: R,
    echo_tx: tokio::sync::mpsc::Sender<u64>,
    dropped_echoes: Arc<AtomicUsize>,
    limiter: Mutex<RateLimiter>,
    peer_ip: IpAddr,
}
impl<R> EchoInterceptRead<R> {
    fn filter(&mut self, buf: &[u8]) -> Option<()> {
        if !is_probe_packet(buf) {
            return Some(());
        }
        if let Some((nonce, _timestamp)) = decode_echo(buf) {
            let now = Instant::now();
            if self.limiter.lock().unwrap().allow(self.peer_ip, now)
                && self.echo_tx.try_send(nonce).is_ok()
            {
                return None;
            }
            self.dropped_echoes.fetch_add(1, Ordering::Relaxed);
        }
        None
    }
}
#[async_trait]
impl<R: UnreliableRead> UnreliableRead for EchoInterceptRead<R> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        loop {
            let n = self.inner.try_recv(buf)?;
            if self.filter(&buf[..n]).is_some() {
                return Ok(n);
            }
        }
    }
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        loop {
            let n = self.inner.recv(buf).await?;
            if self.filter(&buf[..n]).is_some() {
                return Ok(n);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    #[test]
    fn echo_decode_roundtrip_requires_flipped_direction() {
        let probe = encode_probe(7, 42);
        assert!(decode_echo(&probe).is_none(), "unflipped probe is no echo");
        let mut echo = probe;
        echo[DIR_OFFSET] = DIR_ECHO;
        assert_eq!(decode_echo(&echo), Some((7, 42)));
        assert!(decode_echo(&echo[..PROBE_LEN - 1]).is_none());
    }
    #[test]
    fn probe_magic_is_distinct_from_the_handshake_magic() {
        let probe = encode_probe(1, 2);
        assert_ne!(probe[..8], [0xf7, b'R', b'T', b'P', b'O', b'P', 1, 0]);
        assert_ne!(PROBE_LEN, 18);
    }
    #[test]
    fn rate_limiter_drains_and_refills_per_source() {
        let mut limiter = RateLimiter::default();
        let source: IpAddr = "192.0.2.1".parse().unwrap();
        let other: IpAddr = "192.0.2.2".parse().unwrap();
        let t0 = Instant::now();
        for _ in 0..BURST_PER_SOURCE as usize {
            assert!(limiter.allow(source, t0));
        }
        assert!(!limiter.allow(source, t0), "burst exhausted");
        assert!(
            limiter.allow(other, t0),
            "sources are limited independently"
        );
        assert!(limiter.allow(source, t0 + Duration::from_secs(1)));
    }

    #[derive(Debug)]
    struct EchoFeed(Vec<Vec<u8>>, usize);
    #[async_trait]
    impl UnreliableRead for EchoFeed {
        fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
            if self.1 >= self.0.len() {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            let pkt = &self.0[self.1];
            self.1 += 1;
            buf[..pkt.len()].copy_from_slice(pkt);
            Ok(pkt.len())
        }
        async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
            self.try_recv(buf)
        }
    }

    #[test]
    fn echo_intercept_counts_forwarded_nonces_dropped_when_channel_full() {
        let (echo_tx, mut echoes) = tokio::sync::mpsc::channel(2);
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut probe = encode_probe(1, 2);
        probe[DIR_OFFSET] = DIR_ECHO;
        let mut read = EchoInterceptRead {
            inner: EchoFeed(vec![probe.to_vec(); 3], 0),
            echo_tx,
            dropped_echoes: Arc::clone(&dropped),
            limiter: Mutex::new(RateLimiter::default()),
            peer_ip: "127.0.0.1".parse().unwrap(),
        };
        let mut buf = [0; PROBE_LEN];
        loop {
            match read.try_recv(&mut buf) {
                Ok(_) => continue,
                Err(error) if error == std::io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("mock feed failed: {error:?}"),
            }
        }
        assert_eq!(echoes.try_recv(), Ok(1));
        assert_eq!(echoes.try_recv(), Ok(1));
        assert!(echoes.try_recv().is_err());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn responder_counts_echo_send_failures() {
        use std::os::fd::{AsRawFd, FromRawFd};
        let echo = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let raw = echo.as_raw_fd();
        let closer = unsafe { std::fs::File::from_raw_fd(raw) };
        let responder = ProbeResponder::new(Some(echo));
        drop(closer);
        let from: SocketAddr = "127.0.0.1:9".parse().unwrap();
        assert!(responder.observe(&from, &encode_probe(1, 2)));
        assert_eq!(responder.send_error_count(), 1);
        std::mem::forget(responder);
    }
}
