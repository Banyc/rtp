use crate::io_err::IoErr;
use crate::obfuscate::padding;
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
/// Random padding tail appended to the probe plaintext so probes do not
/// fingerprint as a fixed-size datagram. The core fields stay at offsets
/// `0..PROBE_LEN`; the decoder ignores the tail.
pub const MAX_PROBE_PAD: usize = 200;
pub const MAX_PROBE_PLAINTEXT: usize = PROBE_LEN + MAX_PROBE_PAD;
const DIR_PROBE: u8 = 0x00;
const DIR_ECHO: u8 = 0x01;
const DIR_OFFSET: usize = 8;
const NONCE_OFFSET: usize = 9;
const TIMESTAMP_OFFSET: usize = 17;
const RATE_PER_SOURCE: f64 = 16.0;
const BURST_PER_SOURCE: f64 = 32.0;
const MAX_TRACKED_SOURCES: usize = 4096;

/// A probe / probe-echo exchange: a per-probe `nonce` for matching the echo
/// back to its probe and a `timestamp_micros` (epoch-relative) for the
/// receive-side latency measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeEcho {
    pub nonce: u64,
    pub timestamp_micros: u64,
}

pub fn encode_probe(echo: ProbeEcho) -> [u8; PROBE_LEN] {
    let mut bytes = [0; PROBE_LEN];
    bytes[..MAGIC.len()].copy_from_slice(&MAGIC);
    bytes[DIR_OFFSET] = DIR_PROBE;
    bytes[NONCE_OFFSET..TIMESTAMP_OFFSET].copy_from_slice(&echo.nonce.to_be_bytes());
    bytes[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8]
        .copy_from_slice(&echo.timestamp_micros.to_be_bytes());
    bytes
}
fn is_probe_packet(datagram: &[u8]) -> bool {
    datagram.len() >= PROBE_LEN
        && datagram.len() <= MAX_PROBE_PLAINTEXT
        && datagram[..MAGIC.len()] == MAGIC
}
pub fn decode_echo(datagram: &[u8]) -> Option<ProbeEcho> {
    if !is_probe_packet(datagram) || datagram[DIR_OFFSET] != DIR_ECHO {
        return None;
    }
    let nonce = u64::from_be_bytes(datagram[NONCE_OFFSET..TIMESTAMP_OFFSET].try_into().unwrap());
    let timestamp = u64::from_be_bytes(
        datagram[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    Some(ProbeEcho {
        nonce,
        timestamp_micros: timestamp,
    })
}

/// The minimum wire length of an obfuscated probe: the 24-byte obfuscation
/// nonce plus the length prefix plus the 32-byte probe plaintext. A passive
/// observer sees only random-looking datagrams — the probe magic never
/// appears on the wire.
pub const OBFUSCATED_PROBE_LEN: usize = padding::LEN_LEN + PROBE_LEN + crate::obfuscate::NONCE_LEN;
/// The maximum wire length of an obfuscated probe (nonce + length prefix +
/// padded plaintext).
pub const MAX_OBFUSCATED_PROBE_LEN: usize =
    padding::LEN_LEN + MAX_PROBE_PLAINTEXT + crate::obfuscate::NONCE_LEN;

/// Encode `echo` as an obfuscated probe datagram into `out` (reusing its
/// capacity): a 24-byte random nonce followed by the chacha20-encrypted
/// probe plaintext, using the same wire shape as the datagram obfuscation
/// wrapper so probes are indistinguishable from data traffic. With a
/// padding profile the plaintext is `[len u16][probe core][zero padding]`
/// and the wire length varies per probe; without one the plaintext is the
/// probe core alone (the historical format).
pub fn encode_probe_obfuscated(
    echo: ProbeEcho,
    key: [u8; crate::obfuscate::KEY_LEN],
    profile: Option<padding::PaddingProfile>,
    out: &mut Vec<u8>,
) {
    let nonce: [u8; crate::obfuscate::NONCE_LEN] = rand::random();
    let core = encode_probe(echo);
    match profile {
        Some(_) => {
            // [len u16][core][random padding] — the padded data format.
            let target = padding::LEN_LEN + PROBE_LEN + rand::random_range(0..=MAX_PROBE_PAD);
            let max_plaintext = padding::LEN_LEN + PROBE_LEN + MAX_PROBE_PAD;
            out.resize(crate::obfuscate::NONCE_LEN + max_plaintext, 0);
            out[..crate::obfuscate::NONCE_LEN].copy_from_slice(&nonce);
            let plaintext_len = padding::encode_plaintext(
                &core,
                &mut out[crate::obfuscate::NONCE_LEN..],
                padding::PadSettings { profile, target },
            );
            crate::obfuscate::apply_keystream(
                key,
                nonce,
                &mut out[crate::obfuscate::NONCE_LEN..crate::obfuscate::NONCE_LEN + plaintext_len],
            );
            out.truncate(crate::obfuscate::NONCE_LEN + plaintext_len);
        }
        None => {
            // [core][random padding] — the historical probe format: the
            // probe always randomized its size, independent of any profile.
            let pad_len = rand::random_range(0..=MAX_PROBE_PAD);
            let plaintext_len = PROBE_LEN + pad_len;
            out.resize(crate::obfuscate::NONCE_LEN + plaintext_len, 0);
            out[..crate::obfuscate::NONCE_LEN].copy_from_slice(&nonce);
            out[crate::obfuscate::NONCE_LEN..crate::obfuscate::NONCE_LEN + PROBE_LEN]
                .copy_from_slice(&core);
            // The padding tail is zero-filled by resize; the chacha20
            // keystream randomizes it on the wire.
            crate::obfuscate::apply_keystream(key, nonce, &mut out[crate::obfuscate::NONCE_LEN..]);
        }
    }
}

/// Decode an obfuscated probe-echo datagram. Returns `None` when the
/// datagram is not an obfuscated probe echo (wrong length, wrong key, or
/// not an echo). The expected format follows the padding profile.
pub fn decode_echo_obfuscated(
    datagram: &[u8],
    key: [u8; crate::obfuscate::KEY_LEN],
    profile: Option<padding::PaddingProfile>,
) -> Option<ProbeEcho> {
    // The wire length bounds follow the profile: the padded format carries
    // a length prefix, the historical format does not.
    let (min, max) = match profile {
        Some(_) => (OBFUSCATED_PROBE_LEN, MAX_OBFUSCATED_PROBE_LEN),
        None => (
            PROBE_LEN + crate::obfuscate::NONCE_LEN,
            MAX_PROBE_PLAINTEXT + crate::obfuscate::NONCE_LEN,
        ),
    };
    if datagram.len() < min || datagram.len() > max {
        return None;
    }
    let nonce: [u8; crate::obfuscate::NONCE_LEN] =
        datagram[..crate::obfuscate::NONCE_LEN].try_into().ok()?;
    // Decrypt into a stack buffer: the length checks above bound the
    // plaintext, so there is no per-call allocation.
    let mut plaintext = [0u8; padding::LEN_LEN + MAX_PROBE_PLAINTEXT];
    let plaintext_len = datagram.len() - crate::obfuscate::NONCE_LEN;
    plaintext[..plaintext_len].copy_from_slice(&datagram[crate::obfuscate::NONCE_LEN..]);
    crate::obfuscate::apply_keystream(key, nonce, &mut plaintext[..plaintext_len]);
    match profile {
        Some(_) => {
            // [len u16][core][padding]: read the length, strip the padding.
            let mut core = [0u8; PROBE_LEN];
            let len = padding::decode_plaintext(&plaintext[..plaintext_len], &mut core, profile)?;
            if len != PROBE_LEN {
                return None;
            }
            decode_echo(&core[..len])
        }
        None => {
            // [core][padding]: the core is at the front, the tail is
            // ignored (the historical format).
            decode_echo(&plaintext[..PROBE_LEN])
        }
    }
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
/// The outcome of a probe check on one datagram at the listener dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Observe {
    /// The datagram was a probe: echoed (when the echo socket exists) and
    /// consumed — it must not be routed to a connection.
    Consumed,
    /// The datagram is not a valid obfuscated datagram (shorter than the
    /// nonce): drop it, exactly like the obfuscation wrapper drops invalid
    /// datagrams on the read path.
    Dropped,
    /// The datagram is not a probe: route it. `usize` is the plaintext
    /// length; when a key is set the datagram buffer has been decrypted in
    /// place at the front, so the caller truncates the packet to that
    /// length and the connection never decrypts again.
    Data(usize),
}

#[derive(Debug)]
pub(crate) struct ProbeResponder {
    echo: Option<std::net::UdpSocket>,
    limiter: Mutex<RateLimiter>,
    send_error_count: AtomicUsize,
    /// The obfuscation key for the probe side channel, fixed at listener
    /// construction. When set, every datagram is decrypted in place before
    /// the probe check, so the listener routes decrypted bytes to
    /// connections (which never decrypt again); probes are echoed with the
    /// same key. `None` keeps the historical plaintext probe channel.
    key: Option<[u8; crate::obfuscate::KEY_LEN]>,
    /// The padding profile, fixed at listener construction; the dispatch
    /// strips the length prefix and padding only when it is set.
    profile: Option<crate::obfuscate::padding::PaddingProfile>,
}
impl ProbeResponder {
    pub(crate) fn new(
        echo: Option<std::net::UdpSocket>,
        key: Option<[u8; crate::obfuscate::KEY_LEN]>,
        profile: Option<crate::obfuscate::padding::PaddingProfile>,
    ) -> Self {
        Self {
            echo,
            limiter: Mutex::new(RateLimiter::default()),
            send_error_count: AtomicUsize::new(0),
            key,
            profile,
        }
    }
    #[cfg(test)]
    pub(crate) fn send_error_count(&self) -> usize {
        self.send_error_count.load(Ordering::Relaxed)
    }
    /// Check `datagram` for a probe, decrypting it in place when a key is
    /// set. A probe is echoed and consumed; anything else is routed (the
    /// buffer holds the plaintext at the front when a key was set).
    pub(crate) fn observe(&self, from: &SocketAddr, datagram: &mut [u8]) -> Observe {
        let probe: &[u8] = match self.key {
            Some(key) => {
                if datagram.len() < crate::obfuscate::NONCE_LEN {
                    return Observe::Dropped;
                }
                let nonce: [u8; crate::obfuscate::NONCE_LEN] =
                    datagram[..crate::obfuscate::NONCE_LEN].try_into().unwrap();
                let ciphertext_len = datagram.len() - crate::obfuscate::NONCE_LEN;
                // Decrypt in place: move the ciphertext down over the nonce
                // and XOR the keystream, so the routed packet is the
                // plaintext and the connection never decrypts again.
                datagram.copy_within(crate::obfuscate::NONCE_LEN.., 0);
                crate::obfuscate::apply_keystream(key, nonce, &mut datagram[..ciphertext_len]);
                match padding::decode_plaintext_in_place(datagram, ciphertext_len, self.profile) {
                    Some(len) => &mut datagram[..len],
                    None => return Observe::Dropped,
                }
            }
            None => datagram,
        };
        let probe_len = probe.len();
        let is_probe = is_probe_packet(probe);
        if !is_probe {
            return Observe::Data(probe_len);
        }
        let is_dir_probe = probe[DIR_OFFSET] == DIR_PROBE;
        if !is_dir_probe {
            return Observe::Consumed;
        }
        if !self
            .limiter
            .lock()
            .unwrap()
            .allow(from.ip(), Instant::now())
        {
            return Observe::Consumed;
        }
        if let Some(echo) = &self.echo {
            match self.key {
                Some(key) => match self.profile {
                    Some(_) => {
                        // [nonce][len][core flipped][padding] mirroring the
                        // probe's wire length. Flip the direction in a stack
                        // copy of the core (the first PROBE_LEN bytes; a
                        // malformed probe's extra tail is dropped), then
                        // encode the reply plaintext and encrypt in place.
                        let total = datagram.len();
                        let mut core = [0u8; PROBE_LEN];
                        core.copy_from_slice(&datagram[..PROBE_LEN]);
                        core[DIR_OFFSET] = DIR_ECHO;
                        let nonce: [u8; crate::obfuscate::NONCE_LEN] = rand::random();
                        datagram[..crate::obfuscate::NONCE_LEN].copy_from_slice(&nonce);
                        let plaintext_len = padding::encode_plaintext(
                            &core,
                            &mut datagram[crate::obfuscate::NONCE_LEN..total],
                            padding::PadSettings {
                                profile: self.profile,
                                target: total - crate::obfuscate::NONCE_LEN,
                            },
                        );
                        crate::obfuscate::apply_keystream(
                            key,
                            nonce,
                            &mut datagram[crate::obfuscate::NONCE_LEN
                                ..crate::obfuscate::NONCE_LEN + plaintext_len],
                        );
                        if echo
                            .send_to(
                                &datagram[..crate::obfuscate::NONCE_LEN + plaintext_len],
                                from,
                            )
                            .is_err()
                        {
                            self.send_error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    None => {
                        // Historical format: [nonce][core flipped][padding]
                        // mirroring the probe's padded length — shift the
                        // whole decrypted plaintext right by the nonce, flip
                        // the direction, and encrypt in place.
                        let total = crate::obfuscate::NONCE_LEN + probe_len;
                        datagram.copy_within(0..probe_len, crate::obfuscate::NONCE_LEN);
                        let nonce: [u8; crate::obfuscate::NONCE_LEN] = rand::random();
                        datagram[..crate::obfuscate::NONCE_LEN].copy_from_slice(&nonce);
                        datagram[crate::obfuscate::NONCE_LEN + DIR_OFFSET] = DIR_ECHO;
                        crate::obfuscate::apply_keystream(
                            key,
                            nonce,
                            &mut datagram[crate::obfuscate::NONCE_LEN..total],
                        );
                        if echo.send_to(&datagram[..total], from).is_err() {
                            self.send_error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                },
                None => {
                    let mut reply = [0; PROBE_LEN];
                    reply.copy_from_slice(&datagram[..probe_len]);
                    reply[DIR_OFFSET] = DIR_ECHO;
                    if echo.send_to(&reply, from).is_err() {
                        self.send_error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        Observe::Consumed
    }
}
#[derive(Debug)]
pub struct EchoDemux {
    socket: Arc<tokio_udp::UdpSocket>,
    echoes: tokio::sync::mpsc::Receiver<ProbeEcho>,
    dropped_echoes: Arc<AtomicUsize>,
    /// The obfuscation key for the probe side channel. When set, probes are
    /// sent obfuscated (nonce + chacha20) so they are indistinguishable
    /// from the obfuscated data channel; `None` keeps the plaintext probe
    /// channel.
    key: Option<[u8; crate::obfuscate::KEY_LEN]>,
    /// The padding profile; obfuscated probes use the length-prefixed
    /// format only when it is set.
    profile: Option<crate::obfuscate::padding::PaddingProfile>,
    /// Reused scratch for encoding obfuscated probes.
    scratch: Vec<u8>,
}
impl EchoDemux {
    pub fn send_probe(&mut self, echo: ProbeEcho) -> std::io::Result<()> {
        match self.key {
            Some(key) => {
                encode_probe_obfuscated(echo, key, self.profile, &mut self.scratch);
                self.socket.try_send(&self.scratch).map(drop)
            }
            None => self.socket.try_send(&encode_probe(echo)).map(drop),
        }
    }
    pub fn try_recv_echo(&mut self) -> Option<ProbeEcho> {
        self.echoes.try_recv().ok()
    }
    pub fn dropped_echoes(&self) -> usize {
        self.dropped_echoes.load(Ordering::Relaxed)
    }
}
/// Build the client-side probe tap and read filter. `read` is the
/// (possibly obfuscated) read half of the transport: the filter sits AFTER
/// the obfuscation wrapper so it sees decrypted datagrams and can
/// intercept probe echoes that the wrapper already unwrapped. `key` is the
/// obfuscation key used to send probes (and to decrypt echoes on candidate
/// sockets); it must match the key the peer's probe responder uses.
pub(crate) fn client_echo_demux<R: UnreliableRead>(
    socket: Arc<tokio_udp::UdpSocket>,
    read: R,
    key: Option<[u8; crate::obfuscate::KEY_LEN]>,
    profile: Option<crate::obfuscate::padding::PaddingProfile>,
) -> (EchoDemux, EchoInterceptRead<R>) {
    let (echo_tx, echoes) = tokio::sync::mpsc::channel(64);
    let dropped_echoes = Arc::new(AtomicUsize::new(0));
    let peer_ip = socket
        .peer_addr()
        .map(|addr| addr.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    let read = EchoInterceptRead {
        inner: read,
        echo_tx,
        dropped_echoes: Arc::clone(&dropped_echoes),
        limiter: Mutex::new(RateLimiter::default()),
        peer_ip,
    };
    (
        EchoDemux {
            socket,
            echoes,
            dropped_echoes,
            key,
            profile,
            scratch: Vec::new(),
        },
        read,
    )
}
#[derive(Debug)]
pub(crate) struct EchoInterceptRead<R> {
    inner: R,
    echo_tx: tokio::sync::mpsc::Sender<ProbeEcho>,
    dropped_echoes: Arc<AtomicUsize>,
    limiter: Mutex<RateLimiter>,
    peer_ip: IpAddr,
}
impl<R> EchoInterceptRead<R> {
    fn filter(&mut self, buf: &[u8]) -> Option<()> {
        if !is_probe_packet(buf) {
            return Some(());
        }
        if let Some(echo) = decode_echo(buf) {
            let now = Instant::now();
            if self.limiter.lock().unwrap().allow(self.peer_ip, now)
                && self.echo_tx.try_send(echo).is_ok()
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

    fn test_profile() -> Option<crate::obfuscate::padding::PaddingProfile> {
        Some(crate::obfuscate::padding::PaddingProfile::Random {
            mode: 200,
            spread: 50,
        })
    }

    #[test]
    fn echo_decode_roundtrip_requires_flipped_direction() {
        let probe = encode_probe(ProbeEcho {
            nonce: 7,
            timestamp_micros: 42,
        });
        assert!(decode_echo(&probe).is_none(), "unflipped probe is no echo");
        let mut echo = probe;
        echo[DIR_OFFSET] = DIR_ECHO;
        assert_eq!(
            decode_echo(&echo),
            Some(ProbeEcho {
                nonce: 7,
                timestamp_micros: 42
            })
        );
        assert!(decode_echo(&echo[..PROBE_LEN - 1]).is_none());
    }

    #[test]
    fn obfuscated_probe_round_trips_and_hides_the_magic_from_the_wire() {
        let key = [7; crate::obfuscate::KEY_LEN];
        let echo = ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        };
        let mut wire = Vec::new();
        encode_probe_obfuscated(echo, key, test_profile(), &mut wire);
        assert!(
            (OBFUSCATED_PROBE_LEN..=MAX_OBFUSCATED_PROBE_LEN).contains(&wire.len()),
            "wire length {} must be in [{OBFUSCATED_PROBE_LEN}, {MAX_OBFUSCATED_PROBE_LEN}]",
            wire.len()
        );
        // The probe magic must never appear on the wire.
        assert!(
            !wire.windows(MAGIC.len()).any(|w| w == MAGIC),
            "the probe magic leaked onto the wire"
        );
        // The plaintext probe must not appear either.
        let plain = encode_probe(echo);
        assert!(
            !wire.windows(plain.len()).any(|w| w == plain),
            "the plaintext probe leaked onto the wire"
        );
        // A flipped-direction echo round-trips through the obfuscated codec.
        let mut echo_wire = wire;
        // Re-encode with the direction flipped: decrypt, flip, re-encrypt.
        let nonce: [u8; crate::obfuscate::NONCE_LEN] =
            echo_wire[..crate::obfuscate::NONCE_LEN].try_into().unwrap();
        crate::obfuscate::apply_keystream(
            key,
            nonce,
            &mut echo_wire[crate::obfuscate::NONCE_LEN..],
        );
        echo_wire[crate::obfuscate::NONCE_LEN + padding::LEN_LEN + DIR_OFFSET] = DIR_ECHO;
        crate::obfuscate::apply_keystream(
            key,
            nonce,
            &mut echo_wire[crate::obfuscate::NONCE_LEN..],
        );
        assert_eq!(
            decode_echo_obfuscated(&echo_wire, key, test_profile()),
            Some(echo),
            "the obfuscated echo must decode back to the probe"
        );
        // A wrong key must not decode.
        assert_eq!(
            decode_echo_obfuscated(&echo_wire, [9; crate::obfuscate::KEY_LEN], test_profile()),
            None
        );
        // A raw (unobfuscated) echo must not decode as an obfuscated one.
        let mut raw_echo = plain;
        raw_echo[DIR_OFFSET] = DIR_ECHO;
        assert_eq!(decode_echo_obfuscated(&raw_echo, key, test_profile()), None);
    }
    #[test]
    fn probe_wire_sizes_vary_across_packets() {
        let key = [7; crate::obfuscate::KEY_LEN];
        let echo = ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        };
        let mut sizes = std::collections::HashSet::new();
        let mut wire = Vec::new();
        for _ in 0..64 {
            encode_probe_obfuscated(echo, key, test_profile(), &mut wire);
            sizes.insert(wire.len());
        }
        assert!(sizes.len() > 1, "probe wire sizes must vary, got {sizes:?}");
        assert!(
            sizes
                .iter()
                .all(|&n| (OBFUSCATED_PROBE_LEN..=MAX_OBFUSCATED_PROBE_LEN).contains(&n)),
            "probe wire sizes must stay in [{OBFUSCATED_PROBE_LEN}, {MAX_OBFUSCATED_PROBE_LEN}], got {sizes:?}"
        );
    }

    #[test]
    fn unpadded_probe_wire_sizes_stay_randomized_like_v0_0_88() {
        // Without a profile the obfuscated probe keeps the historical
        // format: [nonce][core][random padding], so its wire size varies in
        // [PROBE_LEN + NONCE_LEN, MAX_PROBE_PLAINTEXT + NONCE_LEN] instead of
        // fingerprinting as a fixed-size datagram.
        let key = [7; crate::obfuscate::KEY_LEN];
        let echo = ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        };
        let mut sizes = std::collections::HashSet::new();
        let mut wire = Vec::new();
        for _ in 0..64 {
            encode_probe_obfuscated(echo, key, None, &mut wire);
            sizes.insert(wire.len());
        }
        assert!(
            sizes.len() > 1,
            "unpadded probe wire sizes must vary, got {sizes:?}"
        );
        let min = PROBE_LEN + crate::obfuscate::NONCE_LEN;
        let max = MAX_PROBE_PLAINTEXT + crate::obfuscate::NONCE_LEN;
        assert!(
            sizes.iter().all(|&n| (min..=max).contains(&n)),
            "unpadded probe wire sizes must stay in [{min}, {max}], got {sizes:?}"
        );
        // The randomized probe still decodes as an echo after the direction
        // is flipped.
        let mut echo_wire = wire;
        let nonce: [u8; crate::obfuscate::NONCE_LEN] =
            echo_wire[..crate::obfuscate::NONCE_LEN].try_into().unwrap();
        crate::obfuscate::apply_keystream(
            key,
            nonce,
            &mut echo_wire[crate::obfuscate::NONCE_LEN..],
        );
        echo_wire[crate::obfuscate::NONCE_LEN + DIR_OFFSET] = DIR_ECHO;
        crate::obfuscate::apply_keystream(
            key,
            nonce,
            &mut echo_wire[crate::obfuscate::NONCE_LEN..],
        );
        assert_eq!(
            decode_echo_obfuscated(&echo_wire, key, None),
            Some(echo),
            "the unpadded obfuscated echo must decode back to the probe"
        );
    }

    #[test]
    fn probe_magic_is_distinct_from_the_handshake_magic() {
        let probe = encode_probe(ProbeEcho {
            nonce: 1,
            timestamp_micros: 2,
        });
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
        let mut probe = encode_probe(ProbeEcho {
            nonce: 1,
            timestamp_micros: 2,
        });
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
        assert_eq!(
            echoes.try_recv(),
            Ok(ProbeEcho {
                nonce: 1,
                timestamp_micros: 2
            })
        );
        assert_eq!(
            echoes.try_recv(),
            Ok(ProbeEcho {
                nonce: 1,
                timestamp_micros: 2
            })
        );
        assert!(echoes.try_recv().is_err());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn responder_counts_echo_send_failures() {
        use std::os::fd::{AsRawFd, FromRawFd};
        let echo = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let raw = echo.as_raw_fd();
        let closer = unsafe { std::fs::File::from_raw_fd(raw) };
        let responder = ProbeResponder::new(Some(echo), None, None);
        drop(closer);
        let from: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let mut probe = encode_probe(ProbeEcho {
            nonce: 1,
            timestamp_micros: 2,
        });
        assert_eq!(
            responder.observe(&from, &mut probe),
            Observe::Consumed,
            "a probe must be consumed (echoed)"
        );
        assert_eq!(responder.send_error_count(), 1);
        std::mem::forget(responder);
    }
}
