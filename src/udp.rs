use core::{net::SocketAddr, num::NonZeroUsize};
use std::{path::Path, sync::Arc};

#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};

use async_trait::async_trait;
use fec::proto::{data_mss, symbol_size};
use tokio::net::UdpSocket;
use udp_listener::{Conn, ConnRead, ConnWrite, Packet, UtpListener};

use crate::{
    socket::{ReadSocket, WriteSocket, client_opening_handshake, server_opening_handshake, socket},
    transmission::{
        fec::{FecConfig, FecState},
        fec_tuning::{FecTuning, fec_tuning_from_env},
        frame_delivery::{FrameDelivery, frame_delivery_from_env},
        transmission_layer::{self, UnreliableLayer, UnreliableRead, UnreliableWrite},
    },
};

pub const NO_FEC_MSS: usize = 1424;
/// Maximum user-configured MSS. Datagrams larger than this are rejected before
/// they reach the kernel because on some platforms (notably macOS) oversized
/// UDP sends fail with `EMSGSIZE` and are treated as fatal connection errors.
pub const MAX_MSS: usize = 64 * 1024;
const DISPATCHER_BUF_SIZE: usize = 1024;

type IdentityUdpListener = UtpListener<UdpSocket, SocketAddr, Packet>;
type IdentityConn = Conn<UdpSocket, SocketAddr, Packet>;
type IdentityConnRead = ConnRead<Packet>;

#[cfg(unix)]
pub type MaybeRawFd = RawFd;
#[cfg(not(unix))]
pub type MaybeRawFd = ();
pub fn maybe_raw_fd(udp: &UdpSocket) -> MaybeRawFd {
    cfg_select! {
        unix      => udp.as_raw_fd(),
        not(unix) => ()
    }
}

pub type Handshake = tokio::task::JoinHandle<std::io::Result<Accepted>>;

#[derive(Debug)]
pub struct Listener {
    listener: IdentityUdpListener,
    local_addr: SocketAddr,
    raw_fd: MaybeRawFd,
}
impl Listener {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;
        let local_addr = udp.local_addr()?;
        let raw_fd = maybe_raw_fd(&udp);
        let listener = UtpListener::new_identity_dispatch(
            udp,
            NonZeroUsize::new(DISPATCHER_BUF_SIZE).unwrap(),
        );
        Ok(Self {
            listener,
            local_addr,
            raw_fd,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// [`Self::accept()`] but without handshake
    pub async fn accept_without_handshake(&self, fec: bool) -> std::io::Result<Accepted> {
        let mss = NO_FEC_MSS;
        let tuning = FecTuning::default();
        let frame_delivery = frame_delivery_from_env();
        self.accept_without_handshake_with_mss_fec_tuning_and_frame_delivery(
            fec,
            mss,
            tuning,
            frame_delivery,
        )
        .await
    }

    /// Side-effect: This method also dispatches pkts to all the accepted UDP sockets.
    ///
    /// You should keep this method in a loop.
    pub async fn accept(&self, fec: bool) -> std::io::Result<Handshake> {
        let mss = NO_FEC_MSS;
        let tuning = fec_tuning_from_env();
        let frame_delivery = frame_delivery_from_env();
        self.accept_with_mss_fec_tuning_and_frame_delivery(fec, mss, tuning, frame_delivery)
            .await
    }

    /// [`Self::accept()`] but without handshake and with a custom MSS.
    pub async fn accept_without_handshake_with_mss(
        &self,
        fec: bool,
        mss: usize,
    ) -> std::io::Result<Accepted> {
        let tuning = fec_tuning_from_env();
        let frame_delivery = frame_delivery_from_env();
        self.accept_without_handshake_with_mss_fec_tuning_and_frame_delivery(
            fec,
            mss,
            tuning,
            frame_delivery,
        )
        .await
    }

    /// [`Self::accept()`] with a custom MSS.
    ///
    /// # Panics
    /// Panics if `mss` exceeds [`MAX_MSS`] or is too small for the codec/FEC
    /// overhead. Both peers must use the same `mss`; there is no in-band
    /// negotiation.
    pub async fn accept_with_mss(&self, fec: bool, mss: usize) -> std::io::Result<Handshake> {
        let tuning = fec_tuning_from_env();
        let frame_delivery = frame_delivery_from_env();
        self.accept_with_mss_fec_tuning_and_frame_delivery(fec, mss, tuning, frame_delivery)
            .await
    }

    /// [`Self::accept()`] but without handshake, with a custom MSS and a
    /// per-connection [`FecTuning`].  Both peers must agree on the MSS and
    /// the FEC flag; the FEC tuning is likewise out-of-band (no in-band
    /// negotiation).  See [`FecTuning`] for the large-MSS recipe and the
    /// platform-fragmentation caveat.
    pub async fn accept_without_handshake_with_mss_and_fec_tuning(
        &self,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
    ) -> std::io::Result<Accepted> {
        let frame_delivery = frame_delivery_from_env();
        self.accept_without_handshake_with_mss_fec_tuning_and_frame_delivery(
            fec,
            mss,
            tuning,
            frame_delivery,
        )
        .await
    }

    /// [`Self::accept()`] with a custom MSS and a per-connection
    /// [`FecTuning`].
    ///
    /// # Panics
    /// Panics if `mss` exceeds [`MAX_MSS`] or is too small for the codec/FEC
    /// overhead. Both peers must use the same `mss`; there is no in-band
    /// negotiation.  The FEC tuning is likewise out-of-band — both peers
    /// must pass the same [`FecTuning`] for the parity depth to match.
    pub async fn accept_with_mss_and_fec_tuning(
        &self,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
    ) -> std::io::Result<Handshake> {
        let frame_delivery = frame_delivery_from_env();
        self.accept_with_mss_fec_tuning_and_frame_delivery(fec, mss, tuning, frame_delivery)
            .await
    }

    /// [`Self::accept()`] but without handshake, with a custom MSS, a
    /// per-connection [`FecTuning`], and an explicit [`FrameDelivery`].
    /// Both peers must enable frame delivery together; there is no in-band
    /// negotiation — same coupling as the FEC flag.
    pub async fn accept_without_handshake_with_mss_fec_tuning_and_frame_delivery(
        &self,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
        frame_delivery: FrameDelivery,
    ) -> std::io::Result<Accepted> {
        let accepted = self.listener.accept().await?;
        let handshake = false;
        accept(
            accepted,
            handshake,
            fec,
            mss,
            tuning,
            frame_delivery,
            self.raw_fd,
        )
        .await
    }

    /// [`Self::accept()`] with a custom MSS, a per-connection
    /// [`FecTuning`], and an explicit [`FrameDelivery`].  Both peers must
    /// enable frame delivery together; there is no in-band negotiation —
    /// same coupling as the FEC flag.
    ///
    /// # Panics
    /// Panics if `mss` exceeds [`MAX_MSS`] or is too small for the codec/FEC
    /// overhead. Both peers must use the same `mss`; there is no in-band
    /// negotiation.  The FEC tuning is likewise out-of-band — both peers
    /// must pass the same [`FecTuning`] for the parity depth to match.
    pub async fn accept_with_mss_fec_tuning_and_frame_delivery(
        &self,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
        frame_delivery: FrameDelivery,
    ) -> std::io::Result<Handshake> {
        let accepted = self.listener.accept().await?;
        let handshake = true;
        Ok(tokio::spawn(accept(
            accepted,
            handshake,
            fec,
            mss,
            tuning,
            frame_delivery,
            self.raw_fd,
        )))
    }
}
#[derive(Debug)]
pub struct Accepted {
    pub read: ReadSocket,
    pub write: WriteSocket,
    pub peer_addr: SocketAddr,
}
async fn accept(
    accepted: IdentityConn,
    handshake: bool,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
    raw_fd: MaybeRawFd,
) -> std::io::Result<Accepted> {
    let peer_addr = *accepted.conn_key();
    let (read, write) = accepted.split();
    let write = RawFdConnWrite {
        inner: write,
        raw_fd,
        peer: Some(peer_addr),
    };
    let mut unreliable_layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        mss,
        tuning,
        frame_delivery,
    );
    if handshake {
        server_opening_handshake(&mut unreliable_layer).await?;
    }
    let (read, write) = socket(unreliable_layer, None);
    Ok(Accepted {
        read,
        write,
        peer_addr,
    })
}

pub async fn connect_without_handshake(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    log_config: Option<LogConfig<'_>>,
    fec: bool,
) -> std::io::Result<Connected> {
    let handshake = false;
    let mss = NO_FEC_MSS;
    let tuning = FecTuning::default();
    let frame_delivery = frame_delivery_from_env();
    connect_with_mss_fec_tuning_and_frame_delivery(
        bind,
        addr,
        log_config,
        handshake,
        fec,
        mss,
        tuning,
        frame_delivery,
    )
    .await
}
pub async fn connect(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    log_config: Option<LogConfig<'_>>,
    fec: bool,
) -> std::io::Result<Connected> {
    let handshake = true;
    let mss = NO_FEC_MSS;
    let tuning = fec_tuning_from_env();
    let frame_delivery = frame_delivery_from_env();
    connect_with_mss_fec_tuning_and_frame_delivery(
        bind,
        addr,
        log_config,
        handshake,
        fec,
        mss,
        tuning,
        frame_delivery,
    )
    .await
}
pub async fn connect_without_handshake_with_mss(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    log_config: Option<LogConfig<'_>>,
    fec: bool,
    mss: usize,
) -> std::io::Result<Connected> {
    let handshake = false;
    let tuning = fec_tuning_from_env();
    let frame_delivery = frame_delivery_from_env();
    connect_with_mss_fec_tuning_and_frame_delivery(
        bind,
        addr,
        log_config,
        handshake,
        fec,
        mss,
        tuning,
        frame_delivery,
    )
    .await
}
/// Connect to `addr` with a custom MSS.
///
/// # Panics
/// Panics if `mss` exceeds [`MAX_MSS`] or is too small for the codec/FEC
/// overhead.
///
/// # Platform notes
/// On macOS, datagrams larger than the kernel `net.inet.udp.maxdgram`
/// (default 9216 bytes) fail with `EMSGSIZE`. Because the symbol size derives
/// from the configured `mss`, both peers must use the same value; there is no
/// in-band negotiation.
pub async fn connect_with_mss(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    log_config: Option<LogConfig<'_>>,
    handshake: bool,
    fec: bool,
    mss: usize,
) -> std::io::Result<Connected> {
    let tuning = fec_tuning_from_env();
    let frame_delivery = frame_delivery_from_env();
    connect_with_mss_fec_tuning_and_frame_delivery(
        bind,
        addr,
        log_config,
        handshake,
        fec,
        mss,
        tuning,
        frame_delivery,
    )
    .await
}

/// Connect to `addr` with a custom MSS and a per-connection [`FecTuning`].
///
/// # Panics
/// Panics if `mss` exceeds [`MAX_MSS`] or is too small for the codec/FEC
/// overhead.
///
/// # Platform notes
/// On macOS, datagrams larger than the kernel `net.inet.udp.maxdgram`
/// (default 9216 bytes) fail with `EMSGSIZE`. Because the symbol size derives
/// from the configured `mss`, both peers must use the same value; there is no
/// in-band negotiation.  The FEC tuning is likewise out-of-band — both peers
/// must pass the same [`FecTuning`] for the parity depth to match.  The
/// large-MSS recipe targets loopback / jumbo / fragmentation-tolerant paths;
/// real WANs IP-fragment 8 KiB UDP and one lost fragment kills the whole
/// symbol, inverting the benefit.
pub async fn connect_with_mss_and_fec_tuning(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    log_config: Option<LogConfig<'_>>,
    handshake: bool,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
) -> std::io::Result<Connected> {
    let frame_delivery = frame_delivery_from_env();
    connect_with_mss_fec_tuning_and_frame_delivery(
        bind,
        addr,
        log_config,
        handshake,
        fec,
        mss,
        tuning,
        frame_delivery,
    )
    .await
}

/// Connect to `addr` with a custom MSS, a per-connection [`FecTuning`],
/// and an explicit [`FrameDelivery`].  Both peers must enable frame delivery
/// together; there is no in-band negotiation — same coupling as the FEC flag.
///
/// # Panics
/// Panics if `mss` exceeds [`MAX_MSS`] or is too small for the codec/FEC
/// overhead.
#[allow(clippy::too_many_arguments)]
pub async fn connect_with_mss_fec_tuning_and_frame_delivery(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    log_config: Option<LogConfig<'_>>,
    handshake: bool,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
) -> std::io::Result<Connected> {
    let udp = UdpSocket::bind(bind).await?;
    udp.connect(addr).await?;
    let local_addr = udp.local_addr()?;
    let peer_addr = udp.peer_addr()?;
    let log_config = match log_config {
        Some(c) => Some(
            c.transmission_layer_log_config(local_addr, peer_addr)
                .await?,
        ),
        None => None,
    };
    let udp = Arc::new(udp);
    let mut unreliable_layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        Arc::clone(&udp),
        udp,
        fec,
        mss,
        tuning,
        frame_delivery,
    );
    if handshake {
        client_opening_handshake(&mut unreliable_layer).await?;
    }
    let (read, write) = socket(unreliable_layer, log_config);
    Ok(Connected {
        read,
        write,
        local_addr,
        peer_addr,
    })
}
#[derive(Debug)]
pub struct Connected {
    pub read: ReadSocket,
    pub write: WriteSocket,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

pub(crate) fn wrap_fec(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        NO_FEC_MSS,
        FecTuning::default(),
        FrameDelivery::default(),
    )
}

#[allow(dead_code)] // used in tests; kept as a pub(crate) convenience wrapper
pub(crate) fn wrap_fec_with_mss(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: usize,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        mss,
        fec_tuning_from_env(),
        FrameDelivery::default(),
    )
}

#[allow(dead_code)] // used in tests
pub(crate) fn wrap_fec_with_mss_and_fec_tuning(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
) -> UnreliableLayer {
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        read,
        write,
        fec,
        mss,
        tuning,
        FrameDelivery::default(),
    )
}

pub(crate) fn wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
) -> UnreliableLayer {
    let (mss, fec_state, tuning) = checked_mss_and_fec(fec, mss, tuning, frame_delivery);
    UnreliableLayer {
        utp_read: Box::new(read),
        utp_write: Box::new(write),
        mss,
        fec: fec_state,
        fec_tuning: tuning,
        frame_delivery,
    }
}

fn checked_mss_and_fec(
    fec: bool,
    mss: usize,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
) -> (NonZeroUsize, Option<FecState>, FecTuning) {
    assert!(
        mss <= MAX_MSS,
        "mss {mss} exceeds the {MAX_MSS}-byte datagram ceiling"
    );
    let fec_state = if fec {
        let symbol_size = symbol_size(mss).expect("mss too small for the FEC header");
        Some(FecState::new(FecConfig {
            symbol_size,
            interactive_parity_depth: tuning.interactive_parity_depth,
        }))
    } else {
        None
    };
    let mss = if fec {
        data_mss(mss).expect("mss too small for the FEC header")
    } else {
        mss
    };
    assert!(
        crate::codec::data_overhead() < mss,
        "mss {mss} leaves no room for the codec payload"
    );
    // In frame-delivery mode, the first packet of each frame carries a
    // 4-byte frame-length header (FRAME_DATA_TS), so the MSS must leave
    // room for `frame_data_overhead()` (data_overhead + 4), not just
    // `data_overhead()`.  A too-small MSS would yield 0-byte-payload first
    // packets.
    if frame_delivery.enabled {
        assert!(
            crate::codec::frame_data_overhead() < mss,
            "mss {mss} leaves no room for the first-frame header"
        );
    }
    // FEC off → depth is irrelevant; normalise to the default so the field is
    // inert. When FEC is on, clamp to 1 so a misconfigured 0 cannot disable
    // parity entirely (the stock path always emits at least 1).
    let tuning = if fec_state.is_none() {
        FecTuning::default()
    } else {
        FecTuning {
            interactive_parity_depth: tuning.interactive_parity_depth.max(1),
            ..tuning
        }
    };
    (NonZeroUsize::new(mss).unwrap(), fec_state, tuning)
}

// Accepted socket
#[async_trait]
impl UnreliableRead for IdentityConnRead {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let pkt = Self::recv(self).try_recv().map_err(|e| match e {
            tokio::sync::mpsc::error::TryRecvError::Empty => std::io::ErrorKind::WouldBlock,
            tokio::sync::mpsc::error::TryRecvError::Disconnected => {
                std::io::ErrorKind::UnexpectedEof
            }
        })?;
        let min_len = buf.len().min(pkt.len());
        buf[..min_len].copy_from_slice(&pkt[..min_len]);
        Ok(min_len)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let pkt = Self::recv(self)
            .recv()
            .await
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        let min_len = buf.len().min(pkt.len());
        buf[..min_len].copy_from_slice(&pkt[..min_len]);
        Ok(min_len)
    }
}
/// `ConnWrite<UdpSocket>` wrapper that carries the socket's raw fd for
/// interface-backpressure fallback on Unix.  On non-Unix, behaves
/// identically to the stock `ConnWrite<UdpSocket>` path.
#[derive(Debug)]
pub(crate) struct RawFdConnWrite {
    inner: ConnWrite<UdpSocket>,
    raw_fd: MaybeRawFd,
    peer: Option<core::net::SocketAddr>,
}

#[async_trait]
impl UnreliableWrite for RawFdConnWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        match self.inner.try_send(buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => {
                cfg_select! {
                    target_os = "macos" => raw_sendto_fallback(self.raw_fd, buf, self.peer).await,
                    not(target_os = "macos") => self.inner.send(buf)
                            .await
                            .map_err(|e| normalize_send_err(e).kind()),
                }
            }
            Err(e) => Err(normalize_send_err(e).kind()),
        }
    }
}

// Connected socket
#[async_trait]
impl UnreliableRead for Arc<UdpSocket> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        UdpSocket::try_recv(self, buf).map_err(|e| normalize_send_err(e).kind())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        UdpSocket::recv(self, buf)
            .await
            .map_err(|e| normalize_send_err(e).kind())
    }
}
#[async_trait]
impl UnreliableWrite for Arc<UdpSocket> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        match UdpSocket::try_send(self, buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => {
                #[cfg(target_os = "macos")]
                {
                    raw_sendto_fallback(self.as_raw_fd(), buf, None).await
                }
                #[cfg(not(target_os = "macos"))]
                {
                    UdpSocket::send(self, buf)
                        .await
                        .map_err(|e| normalize_send_err(e).kind())
                }
            }
            Err(e) => Err(normalize_send_err(e).kind()),
        }
    }
}

/// Returns `true` if `code` is the raw OS errno for `ENOBUFS` on the current
/// platform (macOS `55`, Linux `105`).
fn is_enobufs_raw_os_error(code: i32) -> bool {
    cfg_select! {
        target_os = "macos" => code == 55,
        target_os = "linux" => code == 105,
        _ => false,
    }
}

/// Normalize transient UDP send-buffer exhaustion (ENOBUFS / ENOBUFS-equivalent
/// OS errors) to [`std::io::ErrorKind::WouldBlock`].
///
/// UDP has no flow control: when the kernel send buffer is full the OS
/// reports a transient error (macOS errno 55 `ENOBUFS`, Linux errno 105
/// `ENOBUFS`). These are not fatal — the packet is simply dropped and the
/// caller should treat it as transient backpressure (equivalent to a loss
/// event). Reliability is provided above this layer by the reliable layer's
/// retransmit logic, so dropping an outgoing packet here is recoverable.
///
/// All other errors are passed through unchanged.
pub(crate) fn normalize_send_err(e: std::io::Error) -> std::io::Error {
    if let Some(code) = e.raw_os_error()
        && is_enobufs_raw_os_error(code)
    {
        return std::io::ErrorKind::WouldBlock.into();
    }
    e
}

/// Decide whether a failed [`UnreliableWrite::try_send`] should fall back to
/// the async `send` path (waiting for the socket to become writable), or to
/// the raw-fd fallback ([`raw_sendto_fallback`]).
///
/// We only want to wait when the error is a genuine "would block" from a
/// non-blocking socket that is not currently writable. If the kernel
/// reported `ENOBUFS` (transient send-buffer exhaustion) we must *not*
/// wait — the socket is writable, the packet was simply dropped, and
/// blocking here would spin on a ready-but-lossy path. In that case the
/// error is normalized to [`std::io::ErrorKind::WouldBlock`] by
/// [`normalize_send_err`] and surfaced to the caller as a loss event.
pub(crate) fn should_wait_after_try_send(e: &std::io::Error) -> bool {
    if e.kind() != std::io::ErrorKind::WouldBlock {
        return false;
    }
    match e.raw_os_error() {
        Some(code) => !is_enobufs_raw_os_error(code),
        None => true,
    }
}

/// On macOS, kqueue EVFILT_WRITE tracks only socket sndbuf, not mbuf/
/// interface-queue pressure — so tokio UDP writability readiness is
/// *poisoned* under interface backpressure: it reports writable, the send
/// returns EAGAIN, and a readiness-await parks forever.  Fall back to
/// bounded raw `send` / `sendto` retries on the socket's raw fd instead
/// of ever awaiting writability.  Interrupted/EINTR is retried (without
/// consuming a retry) and each retry backs off with increasing delay so
/// the send buffer has time to drain.
///
/// When `peer` is `Some`, the socket is unconnected and `send_to` is used
/// to address the peer directly.  When `None`, the socket is connected
/// and a plain `send` suffices.
///
/// The raw fd is borrowed via `std::net::UdpSocket::from_raw_fd` so the
/// OS handles the sockaddr encoding — this avoids both the byte-order bug
/// of hand-rolled `sockaddr_in` (`from_be_bytes` stores 127.0.0.1 as
/// memory [1,0,0,127] on little-endian) and the Linux build break from
/// the BSD-only `sin_len`/`sin6_len` fields.  The borrowed socket is
/// `mem::forget`ten so the fd is never closed.
///
/// Returns `Err(WouldBlock)` when retry budget is exhausted — the caller
/// must retry later, not treat the packet as sent.
pub(crate) async fn raw_sendto_fallback(
    raw_fd: MaybeRawFd,
    buf: &[u8],
    peer: Option<core::net::SocketAddr>,
) -> Result<usize, std::io::ErrorKind> {
    const BACKOFFS_US: [u64; 5] = [1_000, 2_000, 4_000, 8_000, 16_000];
    #[cfg(not(unix))]
    {
        let _ = (raw_fd, buf, peer);
        return Err(std::io::ErrorKind::Unsupported);
    }
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;
        let socket = unsafe { std::net::UdpSocket::from_raw_fd(raw_fd) };
        let mut attempt = 0;
        loop {
            let res = match &peer {
                Some(peer) => socket.send_to(buf, peer),
                None => socket.send(buf),
            };
            match res {
                Ok(n) => {
                    core::mem::forget(socket);
                    return Ok(n);
                }
                Err(err) => {
                    let kind = err.kind();
                    match kind {
                        std::io::ErrorKind::Interrupted => continue,
                        std::io::ErrorKind::WouldBlock => {
                            if attempt >= BACKOFFS_US.len() {
                                core::mem::forget(socket);
                                return Err(std::io::ErrorKind::WouldBlock);
                            }
                            tokio::time::sleep(std::time::Duration::from_micros(
                                BACKOFFS_US[attempt],
                            ))
                            .await;
                            attempt += 1;
                        }
                        _ => {
                            core::mem::forget(socket);
                            return Err(normalize_send_err(err).kind());
                        }
                    }
                }
            }
        }
    }
}

/// Test-only utilities for simulating packet loss without OS-level network
/// shaping. Compiled only under `test` builds so production code is completely
/// unaffected — there is no global drop flag on the production
/// `UnreliableRead`/`UnreliableWrite` impls.
///
/// Loss is per-instance, not global: each test creates a [`LossRate`] and
/// injects it into the wrappers it wants to be lossy, so tests never interfere
/// with each other.
#[cfg(test)]
pub mod testing {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    /// A toggable loss rate in basis points (0–10_000), owned by a single test
    /// and shared (via `Arc`) between the read and write wrappers of one
    /// connection. 0 means no loss; 10_000 means drop every packet.
    ///
    /// Create one per test with [`LossRate::new`] and pass clones to
    /// [`LossyRead::new`] / [`LossyWrite::new`].
    #[derive(Debug, Clone)]
    pub struct LossRate(Arc<AtomicUsize>);

    impl LossRate {
        /// New loss rate of `bps` basis points (500 = 5%). Clamped to
        /// `[0, 10_000]`.
        pub fn new(bps: usize) -> Self {
            Self(Arc::new(AtomicUsize::new(bps.min(10_000))))
        }

        /// Set the loss rate to `bps` basis points. Clamped to
        /// `[0, 10_000]`.
        pub fn set(&self, bps: usize) {
            self.0.store(bps.min(10_000), Ordering::Relaxed);
        }

        /// Current loss rate in basis points.
        pub fn get(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }

        /// Returns `true` with probability `bps / 10_000`.
        fn roll(&self) -> bool {
            let bps = self.0.load(Ordering::Relaxed);
            bps > 0 && rand::random::<u32>() % 10_000 < bps as u32
        }
    }

    /// Wrapper around any `UnreliableRead` that drops a fraction of received
    /// packets per the injected [`LossRate`]. Dropped packets are skipped (recv
    /// keeps waiting for the next one); `try_recv` reports `WouldBlock`.
    #[derive(Debug)]
    pub struct LossyRead<R: UnreliableRead> {
        inner: R,
        rate: LossRate,
    }

    impl<R: UnreliableRead> LossyRead<R> {
        pub fn new(read: R, rate: LossRate) -> Self {
            Self { inner: read, rate }
        }
    }

    #[async_trait]
    impl<R: UnreliableRead + Send + Sync + 'static> UnreliableRead for LossyRead<R> {
        fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            let n = self.inner.try_recv(buf)?;
            if self.rate.roll() {
                return Err(std::io::ErrorKind::WouldBlock);
            }
            Ok(n)
        }

        async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            loop {
                let n = self.inner.recv(buf).await?;
                if !self.rate.roll() {
                    return Ok(n);
                }
            }
        }
    }

    /// Wrapper around any `UnreliableWrite` that drops a fraction of sent
    /// packets per the injected [`LossRate`]. A dropped send reports success
    /// (the data is "written" then silently discarded), simulating a packet
    /// lost in flight after the sender's kernel has accepted it.
    #[derive(Debug)]
    pub struct LossyWrite<W: UnreliableWrite> {
        inner: W,
        rate: LossRate,
    }

    impl<W: UnreliableWrite> LossyWrite<W> {
        pub fn new(write: W, rate: LossRate) -> Self {
            Self { inner: write, rate }
        }
    }

    #[async_trait]
    impl<W: UnreliableWrite + Send + Sync + 'static> UnreliableWrite for LossyWrite<W> {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
            if self.rate.roll() {
                return Ok(buf.len());
            }
            self.inner.send(buf).await
        }
    }

    /// Like `wrap_fec` but wraps the read/write pair in lossy injectors driven
    /// by `rate`. Each connection should get its own `LossRate` (or a shared
    /// one if you want both directions of a single link to share loss state).
    pub fn wrap_fec_lossy<R, W>(read: R, write: W, fec: bool, rate: LossRate) -> UnreliableLayer
    where
        R: UnreliableRead + Send + Sync + 'static,
        W: UnreliableWrite + Send + Sync + 'static,
    {
        wrap_fec_lossy_with_mss(read, write, fec, NO_FEC_MSS, rate)
    }

    pub fn wrap_fec_lossy_with_mss<R, W>(
        read: R,
        write: W,
        fec: bool,
        mss: usize,
        rate: LossRate,
    ) -> UnreliableLayer
    where
        R: UnreliableRead + Send + Sync + 'static,
        W: UnreliableWrite + Send + Sync + 'static,
    {
        wrap_fec_lossy_with_mss_and_fec_tuning(read, write, fec, mss, fec_tuning_from_env(), rate)
    }

    /// Like `wrap_fec_lossy_with_mss` but takes an explicit `FecTuning` and
    /// threads it through the same `checked_mss_and_fec` /
    /// `wrap_fec_with_mss_and_fec_tuning` construction path production uses.
    /// Only the lossy read/write injection differs from production; the FEC
    /// state, MSS normalisation, and tuning clamping are identical, so a
    /// regression that silently disables FEC at a non-default MSS is caught.
    pub fn wrap_fec_lossy_with_mss_and_fec_tuning<R, W>(
        read: R,
        write: W,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
        rate: LossRate,
    ) -> UnreliableLayer
    where
        R: UnreliableRead + Send + Sync + 'static,
        W: UnreliableWrite + Send + Sync + 'static,
    {
        let (mss, fec_state, tuning) =
            checked_mss_and_fec(fec, mss, tuning, FrameDelivery::default());
        UnreliableLayer {
            utp_read: Box::new(LossyRead::new(read, rate.clone())),
            utp_write: Box::new(LossyWrite::new(write, rate)),
            mss,
            fec: fec_state,
            fec_tuning: tuning,
            frame_delivery: FrameDelivery::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig<'a> {
    pub log_dir_path: &'a Path,
}
impl LogConfig<'_> {
    pub(crate) async fn transmission_layer_log_config(
        &self,
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    ) -> std::io::Result<transmission_layer::LogConfig> {
        tokio::fs::create_dir_all(&self.log_dir_path).await?;
        let file_name = format!("{local_addr}_{peer_addr}.csv");
        Ok(transmission_layer::LogConfig {
            reliable_layer_log_path: self.log_dir_path.join(file_name),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect() {
        let fec = true;
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr();
        let msg_1 = b"hello";
        tokio::spawn(async move {
            loop {
                let accepted = listener.accept(fec).await.unwrap();
                tokio::spawn(async move {
                    let mut accepted = accepted.await.unwrap().unwrap();
                    accepted.write.send(msg_1).await.unwrap();
                    let mut buf = [0; 1];
                    accepted.read.recv(&mut buf).await.unwrap();
                });
            }
        });
        let connected = connect(
            "0.0.0.0:0",
            addr,
            Some(LogConfig {
                log_dir_path: Path::new("target/tests"),
            }),
            fec,
        )
        .await
        .unwrap();
        println!("connected");
        let mut buf = [0; 1024];
        let n = connected.read.recv(&mut buf).await.unwrap();
        assert_eq!(msg_1, &buf[..n]);
    }

    #[test]
    fn require_fn_to_be_send() {
        fn require_send<T: Send>(_t: T) {}
        require_send(connect("0.0.0.0:0", "0.0.0.0:0", None, false));
    }

    #[test]
    fn should_wait_after_plain_wouldblock() {
        // A WouldBlock with no underlying raw OS error (e.g. synthesized by a
        // non-blocking mpsc channel) should fall back to the async wait path.
        let e = std::io::Error::from(std::io::ErrorKind::WouldBlock);
        assert!(should_wait_after_try_send(&e));
    }

    #[test]
    fn should_not_wait_after_enobufs() {
        // ENOBUFS is normalized to WouldBlock, but the raw OS error is
        // preserved on the error, so `should_wait_after_try_send` must
        // return false — waiting would spin on a writable-but-lossy socket.
        #[cfg(target_os = "macos")]
        let code = 55;
        #[cfg(target_os = "linux")]
        let code = 105;
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            // On other platforms we have no ENOBUFS mapping; skip the raw-code
            // assertion but still verify that a plain WouldBlock waits.
            let e = std::io::Error::from(std::io::ErrorKind::WouldBlock);
            assert!(should_wait_after_try_send(&e));
            return;
        }

        let e = std::io::Error::from_raw_os_error(code);
        // Note: `Error::from_raw_os_error` does *not* map ENOBUFS to
        // `WouldBlock` — that mapping is performed by `normalize_send_err`.
        // Here we only check that `should_wait_after_try_send` returns false
        // for an error carrying the ENOBUFS raw code regardless of its
        // `kind()`.
        assert!(!should_wait_after_try_send(&e));

        // After normalization the raw OS error is stripped and the kind
        // becomes WouldBlock, so the wait path is taken again.
        let normalized = normalize_send_err(e);
        assert!(normalized.raw_os_error().is_none());
        assert!(should_wait_after_try_send(&normalized));
    }

    #[derive(Debug)]
    struct Dummy;
    #[async_trait]
    impl UnreliableRead for Dummy {
        fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            Err(std::io::ErrorKind::WouldBlock)
        }
        async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            Err(std::io::ErrorKind::WouldBlock)
        }
    }
    #[async_trait]
    impl UnreliableWrite for Dummy {
        async fn send(&mut self, _buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
            Ok(0)
        }
    }

    #[test]
    fn checked_mss_default_matches_legacy_derivation() {
        let layer = wrap_fec_with_mss(Dummy, Dummy, false, NO_FEC_MSS);
        assert_eq!(layer.mss.get(), NO_FEC_MSS);
        assert!(layer.fec.is_none());
    }

    #[test]
    #[should_panic(expected = "datagram ceiling")]
    fn checked_mss_rejects_oversized() {
        let _ = wrap_fec_with_mss(Dummy, Dummy, false, MAX_MSS + 1);
    }

    #[test]
    #[should_panic(expected = "leaves no room for the codec payload")]
    fn checked_mss_rejects_undersized() {
        let _ = wrap_fec_with_mss(Dummy, Dummy, false, 1);
    }

    #[test]
    fn checked_mss_fec_default_matches_legacy_derivation() {
        let layer = wrap_fec_with_mss(Dummy, Dummy, true, NO_FEC_MSS);
        assert!(layer.fec.is_some());
        // The final MSS after reserving the FEC header is smaller than the raw
        // user-provided NO_FEC_MSS, but it must still leave room for the codec
        // payload.
        assert!(layer.mss.get() < NO_FEC_MSS);
        assert!(crate::codec::data_overhead() < layer.mss.get());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect_with_large_mss() {
        let fec = false;
        let mss = 8192;
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr();
        let msg = {
            let mut buf = vec![0u8; 64 * 1024];
            for byte in &mut buf {
                *byte = rand::random();
            }
            buf
        };
        let msg_for_server = msg.clone();

        tokio::spawn(async move {
            loop {
                let accepted = listener
                    .accept_without_handshake_with_mss(fec, mss)
                    .await
                    .unwrap();
                let msg = msg_for_server.clone();
                tokio::spawn(async move {
                    let mut accepted = accepted;
                    let mut buf = vec![0; msg.len()];
                    let n = accepted.read.recv(&mut buf).await.unwrap();
                    assert_eq!(msg, &buf[..n]);
                    accepted.write.send(b"\x01").await.unwrap();
                });
            }
        });

        let mut connected = connect_without_handshake_with_mss("0.0.0.0:0", addr, None, fec, mss)
            .await
            .unwrap();
        let mut buf = [0; 1];
        connected.write.send(&msg).await.unwrap();
        connected.read.recv(&mut buf).await.unwrap();
    }

    /// Invariant 1: a writer whose `try_send` always returns WouldBlock
    /// must not hang — the raw-fd fallback delivers or returns a bounded
    /// error.  A hang (timeout) is failure.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn udp_send_never_parks_on_tokio_writability() {
        let a = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect("127.0.0.1:1").await.unwrap();
        let buf = vec![0u8; 1424];

        let result = tokio::time::timeout(std::time::Duration::from_millis(500), {
            let a = Arc::clone(&a);
            async move {
                let mut w: Box<dyn UnreliableWrite> = Box::new(a);
                w.send(&buf).await
            }
        })
        .await;

        match result {
            Ok(Ok(_)) | Ok(Err(_)) => {}
            Err(_elapsed) => panic!("send hung on WouldBlock (>500 ms)"),
        }
    }

    /// Fix #1: `raw_fallback_sends_to_peer_on_unconnected_socket` — bind an
    /// unconnected `UdpSocket`, send via the raw fallback with
    /// `Some(127.0.0.1:port)`, and assert the datagram arrives at that peer.
    /// Before the fix, `u32::from_be_bytes(octets)` stored 127.0.0.1 as memory
    /// `[1,0,0,127]` on little-endian (macOS), so the datagram went to the
    /// wrong address and never arrived.
    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "multi_thread")]
    async fn raw_fallback_sends_to_peer_on_unconnected_socket() {
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();

        // Bind an unconnected socket so `peer` is `Some`.
        let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sender.set_nonblocking(true).unwrap();
        let raw_fd = std::os::fd::AsRawFd::as_raw_fd(&sender);

        let payload = b"raw-fallback-test";
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            raw_sendto_fallback(raw_fd, payload, Some(peer_addr)),
        )
        .await;

        match result {
            Ok(Ok(n)) => assert_eq!(n, payload.len()),
            Ok(Err(e)) => panic!("raw_sendto_fallback failed: {e:?}"),
            Err(_) => panic!("raw_sendto_fallback hung"),
        }

        let mut buf = [0u8; 32];
        let (n, from) =
            tokio::time::timeout(std::time::Duration::from_secs(1), peer.recv_from(&mut buf))
                .await
                .expect("peer recv timed out")
                .expect("peer recv failed");
        assert_eq!(&buf[..n], payload);
        assert_eq!(
            from.ip(),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
        );
    }

    /// Fix #10: `frame_delivery_mss_to_small_for_first_frame_header_errors`
    /// — constructing a frame-delivery connection with an MSS too small for
    /// the first-frame header errors instead of silently producing 0-byte-
    /// payload first packets.
    #[test]
    #[should_panic(expected = "first-frame header")]
    fn frame_delivery_mss_to_small_for_first_frame_header_errors() {
        use crate::transmission::frame_delivery::FrameDelivery;
        // An MSS that is large enough for `data_overhead` but too small for
        // `frame_data_overhead` (data_overhead + 4).
        let mss = crate::codec::data_overhead() + 1;
        let _ = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Dummy,
            Dummy,
            false,
            mss,
            crate::transmission::fec_tuning::FecTuning::default(),
            FrameDelivery::enabled(),
        );
    }
}
