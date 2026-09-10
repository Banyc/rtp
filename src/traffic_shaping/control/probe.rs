use crate::io_err::IoErr;
use crate::obfuscate::padding::{self, PaddingSettings, TargetKind};
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
/// The probe's random padding tail bound: the probe plaintext is
/// `[len u16][core][padding]` with a uniform target in
/// `[LEN_LEN + PROBE_LEN, LEN_LEN + PROBE_LEN + MAX_PROBE_PAD]`, so probes
/// never fingerprint as a fixed-size datagram. The core fields stay at
/// offsets `0..PROBE_LEN`; the decoder ignores the tail.
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

/// The probe channel's padding settings: uniform random over the probe's
/// format range, dynamic payload-sized (the core length rides in a u16
/// prefix). The probe always randomizes its size so it does not fingerprint
/// as a fixed-size datagram.
pub fn probe_settings() -> PaddingSettings {
    PaddingSettings::dynamic_target(TargetKind::Uniform {
        lo: padding::LEN_LEN + PROBE_LEN,
        hi: padding::LEN_LEN + PROBE_LEN + MAX_PROBE_PAD,
    })
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
/// Is `plaintext` (the decrypted datagram body) an obfuscated probe? The
/// probe is always the dynamic format `[len u16][core 32][padding]` with
/// `len == PROBE_LEN` and the probe magic at offset 2, sized within the
/// settings' max target. Non-destructive, so a non-probe datagram (data or
/// handshake, which may carry a different length prefix or none at all) is
/// routed intact.
fn is_probe_plaintext(plaintext: &[u8], settings: PaddingSettings) -> bool {
    plaintext.len() >= padding::LEN_LEN + PROBE_LEN
        && plaintext.len() <= settings.max()
        && u16::from_be_bytes(plaintext[..padding::LEN_LEN].try_into().unwrap()) as usize
            == PROBE_LEN
        && plaintext[padding::LEN_LEN..padding::LEN_LEN + MAGIC.len()] == MAGIC
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
/// the probe's max padded plaintext).
pub const MAX_OBFUSCATED_PROBE_LEN: usize =
    padding::LEN_LEN + MAX_PROBE_PLAINTEXT + crate::obfuscate::NONCE_LEN;

/// Encode `echo` as an obfuscated probe datagram into `out` (reusing its
/// capacity): a 24-byte random nonce followed by the chacha20-encrypted
/// probe plaintext, using the same wire shape as the datagram obfuscation
/// wrapper so probes are indistinguishable from data traffic. The plaintext
/// is `[len u16][probe core][zero padding]` padded to a uniform random
/// target (see [`probe_settings`]).
pub fn encode_probe_obfuscated(
    echo: ProbeEcho,
    key: [u8; crate::obfuscate::KEY_LEN],
    settings: PaddingSettings,
    out: &mut Vec<u8>,
) {
    let nonce: [u8; crate::obfuscate::NONCE_LEN] = rand::random();
    let core = encode_probe(echo);
    let max_plaintext = padding::max_plaintext(PROBE_LEN, Some(settings));
    out.resize(crate::obfuscate::NONCE_LEN + max_plaintext, 0);
    out[..crate::obfuscate::NONCE_LEN].copy_from_slice(&nonce);
    let plaintext_len = padding::encode_plaintext(
        &core,
        &mut out[crate::obfuscate::NONCE_LEN..],
        Some(settings),
    );
    crate::obfuscate::apply_keystream(
        key,
        nonce,
        &mut out[crate::obfuscate::NONCE_LEN..crate::obfuscate::NONCE_LEN + plaintext_len],
    );
    out.truncate(crate::obfuscate::NONCE_LEN + plaintext_len);
}

/// Decode an obfuscated probe-echo datagram. Returns `None` when the
/// datagram is not an obfuscated probe echo (wrong length, wrong key, or
/// not an echo). The expected format follows the settings: the dynamic
/// format carries a length prefix and pads to the settings' max target.
pub fn decode_echo_obfuscated(
    datagram: &[u8],
    key: [u8; crate::obfuscate::KEY_LEN],
    settings: PaddingSettings,
) -> Option<ProbeEcho> {
    // The wire length bounds follow the settings: the dynamic format
    // carries a length prefix and pads to the settings' max target.
    let (min, max) = (
        OBFUSCATED_PROBE_LEN,
        crate::obfuscate::NONCE_LEN + settings.max().max(padding::LEN_LEN + PROBE_LEN),
    );
    if datagram.len() < min || datagram.len() > max {
        return None;
    }
    let nonce: [u8; crate::obfuscate::NONCE_LEN] =
        datagram[..crate::obfuscate::NONCE_LEN].try_into().ok()?;
    // Decrypt only the head (the length prefix and the probe core); the
    // padding tail is never read, so the scratch stays bounded by the core
    // size regardless of the target.
    let plaintext_len = datagram.len() - crate::obfuscate::NONCE_LEN;
    let head_len = (padding::LEN_LEN + PROBE_LEN).min(plaintext_len);
    let mut head = [0u8; padding::LEN_LEN + PROBE_LEN];
    head[..head_len].copy_from_slice(
        &datagram[crate::obfuscate::NONCE_LEN..crate::obfuscate::NONCE_LEN + head_len],
    );
    crate::obfuscate::apply_keystream(key, nonce, &mut head[..head_len]);
    // [len u16][core][padding]: read the length, strip the padding.
    let mut core = [0u8; PROBE_LEN];
    let len = padding::decode_plaintext(&head[..head_len], &mut core, Some(settings))?;
    if len != PROBE_LEN {
        return None;
    }
    decode_echo(&core[..len])
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
    /// The probe channel's padding settings, fixed at construction (see
    /// [`probe_settings`]); the dispatch strips the length prefix and
    /// padding with them.
    settings: PaddingSettings,
    /// The data channel's padding settings (the listener's padding
    /// profile): non-probe datagrams are decoded with these, so the
    /// connection reads the payload (not the length prefix and padding).
    data_settings: Option<PaddingSettings>,
}
impl ProbeResponder {
    pub(crate) fn new(
        echo: Option<std::net::UdpSocket>,
        key: Option<[u8; crate::obfuscate::KEY_LEN]>,
        settings: PaddingSettings,
        data_settings: Option<PaddingSettings>,
    ) -> Self {
        Self {
            echo,
            limiter: Mutex::new(RateLimiter::default()),
            send_error_count: AtomicUsize::new(0),
            key,
            settings,
            data_settings,
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
                // The probe is always the dynamic format `[len u16][core]
                // [padding]` with `len == PROBE_LEN` and the probe magic at
                // offset 2. Check it without modifying the buffer, so a
                // non-probe datagram (data or handshake, which may carry a
                // different length prefix or none at all) is routed intact.
                if is_probe_plaintext(&datagram[..ciphertext_len], self.settings) {
                    // Strip the length prefix and padding: move the core to
                    // the front.
                    datagram.copy_within(padding::LEN_LEN..padding::LEN_LEN + PROBE_LEN, 0);
                    &mut datagram[..PROBE_LEN]
                } else {
                    // Not a probe: decode with the data channel's settings
                    // (strips the length prefix and padding when the data
                    // channel pads; keeps the whole plaintext when it does
                    // not).
                    match padding::decode_plaintext_in_place(
                        datagram,
                        ciphertext_len,
                        self.data_settings,
                    ) {
                        Some(len) => &mut datagram[..len],
                        None => return Observe::Dropped,
                    }
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
                Some(key) => {
                    // The echo mirrors the probe's plaintext length, so the
                    // reply randomizes its size exactly like the probe.
                    // Flip the direction in a stack copy of the core (the
                    // first PROBE_LEN bytes; a malformed probe's extra tail
                    // is dropped), then encode the reply plaintext and
                    // encrypt in place.
                    let total = datagram.len();
                    let mut core = [0u8; PROBE_LEN];
                    core.copy_from_slice(&datagram[..PROBE_LEN]);
                    core[DIR_OFFSET] = DIR_ECHO;
                    let nonce: [u8; crate::obfuscate::NONCE_LEN] = rand::random();
                    datagram[..crate::obfuscate::NONCE_LEN].copy_from_slice(&nonce);
                    let plaintext_len = padding::encode_plaintext(
                        &core,
                        &mut datagram[crate::obfuscate::NONCE_LEN..total],
                        Some(PaddingSettings::dynamic_target(TargetKind::Fixed(
                            total - crate::obfuscate::NONCE_LEN,
                        ))),
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
                    // Copy only the 32-byte core: a raw probe may carry a
                    // padding tail (up to MAX_PROBE_PLAINTEXT bytes), and
                    // copying the whole datagram into the 32-byte reply
                    // would panic — a crafted padded probe was a remote DoS
                    // on the no-key listener. The tail is dropped, exactly
                    // like the obfuscated path drops a malformed probe's
                    // extra tail.
                    let mut reply = [0; PROBE_LEN];
                    reply.copy_from_slice(&datagram[..PROBE_LEN]);
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
    /// The probe channel's padding settings (see [`probe_settings`]).
    settings: PaddingSettings,
    /// Reused scratch for encoding obfuscated probes.
    scratch: Vec<u8>,
}
impl EchoDemux {
    pub fn send_probe(&mut self, echo: ProbeEcho) -> std::io::Result<()> {
        match self.key {
            Some(key) => {
                encode_probe_obfuscated(echo, key, self.settings, &mut self.scratch);
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
    settings: PaddingSettings,
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
            settings,
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
        // The echo is the probe core (32 bytes) with the direction flipped.
        // It arrives either already unwrapped by the obfuscation wrapper
        // (the 32-byte core, when the data channel pads) or as the raw
        // dynamic probe format `[len u16][core][padding]` (when the data
        // channel does not pad, the wrapper passes the whole plaintext
        // through).
        let core = if buf.len() >= padding::LEN_LEN + PROBE_LEN
            && u16::from_be_bytes(buf[..padding::LEN_LEN].try_into().unwrap()) as usize == PROBE_LEN
            && buf[padding::LEN_LEN..padding::LEN_LEN + MAGIC.len()] == MAGIC
        {
            &buf[padding::LEN_LEN..padding::LEN_LEN + PROBE_LEN]
        } else if is_probe_packet(buf) {
            &buf[..PROBE_LEN]
        } else {
            return Some(());
        };
        if let Some(echo) = decode_echo(core) {
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
        encode_probe_obfuscated(echo, key, probe_settings(), &mut wire);
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
            decode_echo_obfuscated(&echo_wire, key, probe_settings()),
            Some(echo),
            "the obfuscated echo must decode back to the probe"
        );
        // A wrong key must not decode.
        assert_eq!(
            decode_echo_obfuscated(&echo_wire, [9; crate::obfuscate::KEY_LEN], probe_settings()),
            None
        );
        // A raw (unobfuscated) echo must not decode as an obfuscated one.
        let mut raw_echo = plain;
        raw_echo[DIR_OFFSET] = DIR_ECHO;
        assert_eq!(
            decode_echo_obfuscated(&raw_echo, key, probe_settings()),
            None
        );
    }
    #[test]
    fn probe_wire_sizes_vary_uniformly() {
        // The probe always randomizes its size: a uniform draw over the
        // probe's format range, so probes never fingerprint as a fixed-size
        // datagram.
        let key = [7; crate::obfuscate::KEY_LEN];
        let echo = ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        };
        let mut sizes = std::collections::HashSet::new();
        let mut wire = Vec::new();
        for _ in 0..64 {
            encode_probe_obfuscated(echo, key, probe_settings(), &mut wire);
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
    fn responder_echoes_a_probe_mirroring_its_length() {
        let key = [7; crate::obfuscate::KEY_LEN];
        let echo = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let responder = ProbeResponder::new(Some(echo), Some(key), probe_settings(), None);
        // The prober is the source of the probe; the echo is sent back to
        // it, so it can read the reply.
        let prober = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let from = prober.local_addr().unwrap();
        let probe = ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        };
        let mut wire = Vec::new();
        encode_probe_obfuscated(probe, key, probe_settings(), &mut wire);
        let probe_len = wire.len();
        assert_eq!(
            responder.observe(&from, &mut wire),
            Observe::Consumed,
            "a probe must be consumed (echoed)"
        );
        // The echo mirrors the probe's wire length and decodes back, so the
        // reply randomizes its size exactly like the probe.
        let mut buf = [0u8; 1024];
        let (n, _) = prober.recv_from(&mut buf).unwrap();
        assert_eq!(n, probe_len, "the echo must mirror the probe's wire length");
        assert_eq!(
            decode_echo_obfuscated(&buf[..n], key, probe_settings()),
            Some(probe),
            "the echo must decode back to the probe"
        );
        // The echo wire is obfuscated: neither the probe magic nor the
        // plaintext echo core appears on the wire.
        assert!(
            !buf[..n].windows(MAGIC.len()).any(|w| w == MAGIC),
            "the probe magic leaked onto the echo wire"
        );
        let mut plain_echo = encode_probe(probe);
        plain_echo[DIR_OFFSET] = DIR_ECHO;
        assert!(
            !buf[..n].windows(plain_echo.len()).any(|w| w == plain_echo),
            "the plaintext echo leaked onto the wire"
        );
    }

    #[test]
    fn responder_counts_echo_send_failures() {
        use std::os::fd::{AsRawFd, FromRawFd};
        let echo = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let raw = echo.as_raw_fd();
        let closer = unsafe { std::fs::File::from_raw_fd(raw) };
        let responder = ProbeResponder::new(Some(echo), None, probe_settings(), None);
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

    #[test]
    fn responder_echoes_only_the_core_of_a_padded_raw_probe() {
        // A raw probe with a padding tail (33..=MAX_PROBE_PLAINTEXT bytes)
        // is a valid probe packet: the decoder ignores the tail. The echo
        // must copy only the 32-byte core — copying the whole datagram into
        // the 32-byte reply would panic, and a crafted padded probe was a
        // remote DoS on the no-key listener.
        let echo = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let responder = ProbeResponder::new(Some(echo), None, probe_settings(), None);
        let prober = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let from = prober.local_addr().unwrap();
        let probe = ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        };
        let mut padded = encode_probe(probe).to_vec();
        padded.extend_from_slice(&[0xAB; 64]); // a padding tail
        assert_eq!(
            responder.observe(&from, &mut padded),
            Observe::Consumed,
            "a padded raw probe must be consumed (echoed), not panic"
        );
        // The echo is exactly the 32-byte core with the direction flipped.
        let mut buf = [0u8; 1024];
        let (n, _) = prober.recv_from(&mut buf).unwrap();
        assert_eq!(n, PROBE_LEN, "the echo must be the 32-byte core");
        assert_eq!(
            decode_echo(&buf[..n]),
            Some(probe),
            "the echo must decode back to the probe"
        );
    }
}
