use core::{net::SocketAddr, num::NonZeroUsize};
use std::{path::Path, sync::Arc};

#[cfg(unix)]
use std::os::fd::AsRawFd;

use async_trait::async_trait;
use tokio::net::UdpSocket;
use tokio_udp::UdpSocket as VectoredUdpSocket;

use crate::io_err::IoErr;
#[cfg(test)]
use crate::transmission::transmission_layer::UnreliableLayer;
use crate::{
    delivery::frame::{FrameMode, frame_delivery_from_env},
    handshake::{client_opening_handshake, server_opening_handshake},
    socket::{
        ConnReader, ConnWriter, FrameByteReader, FrameByteWriter, SessionHandle,
        into_frame_io_parts, socket, socket_with_watchdog_tuning,
    },
    transmission::{
        fec_tuning::{FecTuning, fec_tuning_from_env},
        transmission_layer::{
            self, UnreliableRead, UnreliableWrite, instream_group_fec_from_env, rtx_dup_from_env,
        },
        watchdog_tuning::WatchdogTuning,
    },
};

pub use raw_send::{MaybeRawFd, maybe_raw_fd};
pub(crate) use raw_send::{normalize_send_err, raw_sendto_fallback, should_wait_after_try_send};

mod layer;
pub(crate) use layer::{MssError, ValidMss, wrap_fec_with_mss_and_fec_tuning_and_frame_delivery};
#[cfg(test)]
pub(crate) use layer::{checked_mss_and_fec, wrap_fec};

mod raw_send;
/// Test-only utilities for simulating packet loss without OS-level network
/// shaping. Compiled only under `test` builds so production code is completely
/// unaffected — there is no global drop flag on the production
/// `UnreliableRead`/`UnreliableWrite` impls.
///
/// Loss is per-instance, not global: each test creates a [`BasisPoints`] and
/// injects it into the wrappers it wants to be lossy, so tests never interfere
/// with each other.
#[cfg(test)]
pub mod testing;

pub const NO_FEC_MSS: usize = 1424;
/// Maximum user-configured MSS. Datagrams larger than this are rejected before
/// they reach the kernel because on some platforms (notably macOS) oversized
/// UDP sends fail with `EMSGSIZE` and are treated as fatal connection errors.
pub const MAX_MSS: usize = 64 * 1024;
const DISPATCHER_BUF_SIZE: usize = 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MssConfig {
    #[default]
    Default,
    Custom(usize),
}
impl MssConfig {
    pub fn resolve(self) -> Result<ValidMss, MssError> {
        match self {
            Self::Default => ValidMss::try_new(NO_FEC_MSS),
            Self::Custom(mss) => ValidMss::try_new(mss),
        }
    }
}

type IdentityUdpListener = UtpListener<VectoredUdpSocket, SocketAddr, Packet>;
type IdentityConn = Conn<VectoredUdpSocket, SocketAddr, Packet>;
type IdentityConnRead = ConnRead<Packet>;

async fn resolve_socket_addrs(
    addr: impl tokio::net::ToSocketAddrs,
) -> std::io::Result<Vec<SocketAddr>> {
    let addrs = tokio::net::lookup_host(addr).await?.collect::<Vec<_>>();
    if addrs.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "address resolved to no socket addresses",
        ));
    }
    Ok(addrs)
}

async fn bind_udp(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<VectoredUdpSocket> {
    let mut last_error = None;
    for addr in resolve_socket_addrs(addr).await? {
        match VectoredUdpSocket::bind(addr).await {
            Ok(socket) => return Ok(socket),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.expect("resolve_socket_addrs returned at least one address"))
}

fn dialable_addr(addr: SocketAddr) -> SocketAddr {
    if !addr.ip().is_unspecified() {
        return addr;
    }
    let loopback = match addr.ip() {
        core::net::IpAddr::V4(_) => core::net::IpAddr::V4(core::net::Ipv4Addr::LOCALHOST),
        core::net::IpAddr::V6(_) => core::net::IpAddr::V6(core::net::Ipv6Addr::LOCALHOST),
    };
    SocketAddr::new(loopback, addr.port())
}

async fn connect_udp(
    socket: &VectoredUdpSocket,
    addr: impl tokio::net::ToSocketAddrs,
) -> std::io::Result<()> {
    let mut last_error = None;
    for addr in resolve_socket_addrs(addr).await? {
        match socket.connect(dialable_addr(addr)).await {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.expect("resolve_socket_addrs returned at least one address"))
}

pub type AcceptTask = std::pin::Pin<Box<dyn Future<Output = std::io::Result<Accepted>> + Send>>;

#[derive(Debug)]
pub struct Listener {
    listener: IdentityUdpListener,
    local_addr: SocketAddr,
    raw_fd: MaybeRawFd,
}
impl Listener {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let udp = bind_udp(addr).await?;
        let local_addr = udp.local_addr()?;
        let raw_fd = maybe_raw_fd(&udp);
        let responder = crate::path_probe::ProbeResponder::new(probe_echo_socket(&udp));
        let dispatch: Classify<SocketAddr, SocketAddr, Packet> =
            Arc::new(move |addr: &SocketAddr, packet: Packet| {
                if responder.observe(addr, packet.as_ref()) {
                    return None;
                }
                Some((*addr, packet))
            });
        let listener = UtpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUF_SIZE).unwrap(),
            dispatch,
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

    /// [`Self::accept_without_handshake_with()`] with the default
    /// [`AcceptConfig`] (env-tuned).
    pub async fn accept_without_handshake(&self) -> std::io::Result<Accepted> {
        self.accept_without_handshake_with(AcceptConfig::default())
            .await
    }

    /// Accept a connection and run the server opening handshake in the
    /// returned, unspawned accept future.  All tuning (FEC, MSS, frame
    /// delivery, retransmission armor, in-stream group FEC) comes from
    /// `config`; see [`AcceptConfig`] for the env-vs-default rules.
    ///
    /// Side-effect: This method also dispatches pkts to all the accepted UDP sockets.
    ///
    /// The returned future must be spawned (e.g. into a [`tokio::task::JoinSet`])
    /// for the handshake and session setup to run; awaiting it inline would
    /// block the accept loop on the handshake.  You should keep this method in
    /// a loop.
    pub async fn accept_with(&self, config: AcceptConfig) -> std::io::Result<AcceptTask> {
        let accepted = self.listener.poll_next_conn().await?;
        let raw_fd = self.raw_fd;
        Ok(Box::pin(async move {
            accept(
                accepted,
                true,
                config.fec,
                config.mss.resolve()?,
                config.fec_tuning,
                config.frame_delivery,
                config.rtx_dup,
                config.instream_group_fec,
                raw_fd,
            )
            .await
        }))
    }

    /// [`Self::accept_with()`] but without the server opening handshake;
    /// returns the accepted session directly.
    pub async fn accept_without_handshake_with(
        &self,
        config: AcceptConfig,
    ) -> std::io::Result<Accepted> {
        let accepted = self.listener.poll_next_conn().await?;
        accept(
            accepted,
            false,
            config.fec,
            config.mss.resolve()?,
            config.fec_tuning,
            config.frame_delivery,
            config.rtx_dup,
            config.instream_group_fec,
            self.raw_fd,
        )
        .await
    }

    /// [`Self::accept_without_handshake_with()`] wrapped into
    /// [`FrameDeliveryIo`] halves; the accepted session skips the server
    /// opening handshake.  Returns an unspawned accept future that must be
    /// spawned (e.g. into a [`tokio::task::JoinSet`]) by the caller.
    pub async fn accept_frame_delivery(
        &self,
        config: AcceptConfig,
    ) -> std::io::Result<FrameDeliveryAccept> {
        let accepted = self.listener.poll_next_conn().await?;
        let raw_fd = self.raw_fd;
        let local_addr = self.local_addr;
        Ok(Box::pin(async move {
            let accepted = accept(
                accepted,
                false,
                config.fec,
                config.mss.resolve()?,
                config.fec_tuning,
                FrameMode::enabled(),
                config.rtx_dup,
                config.instream_group_fec,
                raw_fd,
            )
            .await?;
            let Accepted {
                read,
                write,
                supervisor,
                peer_addr,
            } = accepted;
            make_frame_delivery_io(read, write, supervisor, local_addr, peer_addr, None)
        }))
    }
}
#[derive(Debug)]
pub struct Accepted {
    pub read: ConnReader,
    pub write: ConnWriter,
    pub supervisor: SessionHandle,
    pub peer_addr: SocketAddr,
}

#[derive(Debug)]
pub struct FrameDeliveryIo {
    pub read: FrameByteReader,
    pub write: FrameByteWriter,
    pub supervisor: SessionHandle,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub probe_tap: Option<crate::path_probe::EchoDemux>,
}
pub type FrameDeliveryAccept =
    std::pin::Pin<Box<dyn Future<Output = std::io::Result<FrameDeliveryIo>> + Send>>;

/// Tuning for [`Listener::accept_with`] / [`Listener::accept_without_handshake_with`].
///
/// `Default` reads the process environment once: `fec_tuning` and
/// `frame_delivery` come from `RTP_FEC_TUNING` / `RTP_FRAME_DELIVERY`,
/// `rtx_dup` from `RTP_RTX_DUP`, and `instream_group_fec` from
/// `RTP_INSTREAM_GROUP_FEC`.  Override the fields explicitly to opt out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptConfig {
    pub fec: bool,
    pub mss: MssConfig,
    pub fec_tuning: FecTuning,
    pub frame_delivery: FrameMode,
    pub rtx_dup: bool,
    pub instream_group_fec: bool,
}

impl Default for AcceptConfig {
    fn default() -> Self {
        Self {
            fec: false,
            mss: MssConfig::Default,
            fec_tuning: fec_tuning_from_env(),
            frame_delivery: frame_delivery_from_env(),
            rtx_dup: rtx_dup_from_env(),
            instream_group_fec: instream_group_fec_from_env(),
        }
    }
}

/// Tuning for [`connect_with`] / [`FrameDeliveryIo::connect`].
///
/// `Default` reads the process environment once; see [`AcceptConfig`] for the
/// env-vs-default rules.
#[derive(Debug, Clone)]
pub struct ConnectConfig<'a> {
    pub log_config: Option<LogConfig<'a>>,
    pub handshake: bool,
    pub fec: bool,
    pub mss: MssConfig,
    pub fec_tuning: FecTuning,
    pub frame_delivery: FrameMode,
    pub rtx_dup: bool,
    pub instream_group_fec: bool,
    pub watchdog: Option<WatchdogTuning>,
}

impl<'a> Default for ConnectConfig<'a> {
    fn default() -> Self {
        Self {
            log_config: None,
            handshake: true,
            fec: false,
            mss: MssConfig::Default,
            fec_tuning: fec_tuning_from_env(),
            frame_delivery: frame_delivery_from_env(),
            rtx_dup: rtx_dup_from_env(),
            instream_group_fec: instream_group_fec_from_env(),
            watchdog: None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept(
    accepted: IdentityConn,
    handshake: bool,
    fec: bool,
    mss: ValidMss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
    rtx_dup: bool,
    instream_group_fec: bool,
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
        Box::new(read),
        Box::new(write),
        fec,
        mss,
        tuning,
        frame_delivery,
    )?;
    unreliable_layer.rtx_dup = rtx_dup;
    unreliable_layer.instream_group_fec = instream_group_fec;
    if handshake {
        server_opening_handshake(&mut unreliable_layer).await?;
    }
    let (read, write, supervisor) = socket(unreliable_layer, None);
    Ok(Accepted {
        read,
        write,
        supervisor,
        peer_addr,
    })
}

fn make_frame_delivery_io(
    read: ConnReader,
    write: ConnWriter,
    supervisor: SessionHandle,
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    probe_tap: Option<crate::path_probe::EchoDemux>,
) -> std::io::Result<FrameDeliveryIo> {
    let (read, write) = into_frame_io_parts(read, write)?.into_parts();
    Ok(FrameDeliveryIo {
        read,
        write,
        supervisor,
        local_addr,
        peer_addr,
        probe_tap,
    })
}

/// Connect to `addr` with the given [`ConnectConfig`].  `bind` is the local
/// address to bind.  Both peers must agree on the FEC flag, the MSS, and
/// frame delivery — there is no in-band negotiation; see [`ConnectConfig`]
/// for the env-vs-default tuning rules.
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
pub async fn connect_with(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    config: ConnectConfig<'_>,
) -> std::io::Result<Connected> {
    connect_configured(bind, addr, config).await
}

async fn connect_configured(
    bind: impl tokio::net::ToSocketAddrs,
    addr: impl tokio::net::ToSocketAddrs,
    config: ConnectConfig<'_>,
) -> std::io::Result<Connected> {
    let udp = bind_udp(bind).await?;
    connect_udp(&udp, addr).await?;
    connect_bound(udp, config).await
}

#[derive(Debug)]
pub struct Connected {
    pub read: ConnReader,
    pub write: ConnWriter,
    pub supervisor: SessionHandle,
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub probe_tap: Option<crate::path_probe::EchoDemux>,
}

impl FrameDeliveryIo {
    pub async fn connect(
        bind: impl tokio::net::ToSocketAddrs,
        addr: impl tokio::net::ToSocketAddrs,
        config: ConnectConfig<'_>,
    ) -> std::io::Result<Self> {
        let connected = connect_configured(
            bind,
            addr,
            ConnectConfig {
                frame_delivery: FrameMode::enabled(),
                ..config
            },
        )
        .await?;
        let Connected {
            read,
            write,
            supervisor,
            local_addr,
            peer_addr,
            probe_tap,
        } = connected;
        make_frame_delivery_io(read, write, supervisor, local_addr, peer_addr, probe_tap)
    }
    pub async fn connect_with_socket(
        socket: VectoredUdpSocket,
        addr: SocketAddr,
        config: ConnectConfig<'_>,
    ) -> std::io::Result<Self> {
        let connected = connect_with_socket(
            socket,
            addr,
            ConnectConfig {
                frame_delivery: FrameMode::enabled(),
                ..config
            },
        )
        .await?;
        let Connected {
            read,
            write,
            supervisor,
            local_addr,
            peer_addr,
            probe_tap,
        } = connected;
        make_frame_delivery_io(read, write, supervisor, local_addr, peer_addr, probe_tap)
    }
}

// Accepted socket
#[async_trait]
impl UnreliableRead for IdentityConnRead {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        let pkt = self.read_half().try_recv().map_err(|e| match e {
            tokio::sync::mpsc::error::TryRecvError::Empty => std::io::ErrorKind::WouldBlock,
            tokio::sync::mpsc::error::TryRecvError::Disconnected => {
                std::io::ErrorKind::UnexpectedEof
            }
        })?;
        let min_len = buf.len().min(pkt.len());
        buf[..min_len].copy_from_slice(&pkt[..min_len]);
        Ok(min_len)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        let pkt = self
            .read_half()
            .recv()
            .await
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        let min_len = buf.len().min(pkt.len());
        buf[..min_len].copy_from_slice(&pkt[..min_len]);
        Ok(min_len)
    }
}
/// `ConnWrite<VectoredUdpSocket>` wrapper that carries the socket's raw fd for
/// interface-backpressure fallback on Unix.  On non-Unix, behaves
/// identically to the stock `ConnWrite<VectoredUdpSocket>` path.
#[derive(Debug)]
pub(crate) struct RawFdConnWrite {
    inner: ConnWrite<VectoredUdpSocket>,
    raw_fd: MaybeRawFd,
    peer: Option<core::net::SocketAddr>,
}

#[async_trait]
impl UnreliableWrite for RawFdConnWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        match self.inner.try_send(buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => {
                cfg_select! {
                    target_os = "macos" => raw_sendto_fallback(self.raw_fd, buf, self.peer).await,
                    not(target_os = "macos") => self.inner.send(buf).await.map_err(normalize_send_err),
                }
            }
            Err(e) => Err(normalize_send_err(e)),
        }
    }

    async fn send_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> Result<usize, IoErr> {
        self.inner
            .send_vectored(bufs)
            .await
            .map_err(normalize_send_err)
    }
}

// Connected socket
#[async_trait]
impl UnreliableRead for Arc<UdpSocket> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        UdpSocket::try_recv(self, buf).map_err(normalize_send_err)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        UdpSocket::recv(self, buf).await.map_err(normalize_send_err)
    }
}
#[async_trait]
impl UnreliableWrite for Arc<UdpSocket> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        match UdpSocket::try_send(self, buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => {
                #[cfg(target_os = "macos")]
                {
                    raw_sendto_fallback(self.as_raw_fd(), buf, None).await
                }
                #[cfg(not(target_os = "macos"))]
                {
                    UdpSocket::send(self, buf).await.map_err(normalize_send_err)
                }
            }
            Err(e) => Err(normalize_send_err(e)),
        }
    }
}

// ── tokio_udp integration ─────────────────────────────────────────────
// Use fully-qualified inherent methods to avoid recursive trait calls.

#[async_trait]
impl UnreliableRead for std::sync::Arc<tokio_udp::UdpSocket> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        tokio_udp::UdpSocket::try_recv(self, buf).map_err(normalize_send_err)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        tokio_udp::UdpSocket::recv(self, buf)
            .await
            .map_err(normalize_send_err)
    }
}

#[async_trait]
impl UnreliableWrite for std::sync::Arc<tokio_udp::UdpSocket> {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        match tokio_udp::UdpSocket::try_send(self, buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => tokio_udp::UdpSocket::send(self, buf)
                .await
                .map_err(normalize_send_err),
            Err(e) => Err(normalize_send_err(e)),
        }
    }

    async fn send_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> Result<usize, IoErr> {
        tokio_udp::UdpSocket::send_vectored(self, bufs)
            .await
            .map_err(normalize_send_err)
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

use udp_listener::{Classify, Conn, ConnRead, ConnWrite, Packet, UtpListener};
fn probe_echo_socket(udp: &VectoredUdpSocket) -> Option<std::net::UdpSocket> {
    let echo = udp.try_clone_std().ok()?;
    echo.set_nonblocking(true).ok()?;
    Some(echo)
}
async fn connect_bound(
    udp: VectoredUdpSocket,
    config: ConnectConfig<'_>,
) -> std::io::Result<Connected> {
    let ConnectConfig {
        log_config,
        handshake,
        fec,
        mss,
        fec_tuning,
        frame_delivery,
        rtx_dup,
        instream_group_fec,
        watchdog,
    } = config;
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
    let (probe_tap, filtered_read) = crate::path_probe::client_echo_demux(Arc::clone(&udp));
    let mut unreliable_layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        Box::new(filtered_read),
        Box::new(udp),
        fec,
        mss.resolve()?,
        fec_tuning,
        frame_delivery,
    )?;
    unreliable_layer.rtx_dup = rtx_dup;
    unreliable_layer.instream_group_fec = instream_group_fec;
    if handshake {
        client_opening_handshake(&mut unreliable_layer).await?;
    }
    let (read, write, supervisor) = match watchdog {
        Some(tuning) => socket_with_watchdog_tuning(unreliable_layer, log_config, tuning),
        None => socket(unreliable_layer, log_config),
    };
    Ok(Connected {
        read,
        write,
        supervisor,
        local_addr,
        peer_addr,
        probe_tap: Some(probe_tap),
    })
}
pub async fn connect_with_socket(
    socket: VectoredUdpSocket,
    addr: SocketAddr,
    config: ConnectConfig<'_>,
) -> std::io::Result<Connected> {
    socket.connect(dialable_addr(addr)).await?;
    connect_bound(socket, config).await
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
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
                let accepted = listener
                    .accept_with(AcceptConfig {
                        fec,
                        ..AcceptConfig::default()
                    })
                    .await
                    .unwrap();
                tokio::spawn(async move {
                    let mut accepted = accepted.await.unwrap();
                    accepted.write.send(msg_1).await.unwrap();
                    let mut buf = [0; 1];
                    accepted.read.recv(&mut buf).await.unwrap();
                });
            }
        });
        let mut connected = connect_with(
            "0.0.0.0:0",
            addr,
            ConnectConfig {
                log_config: Some(LogConfig {
                    log_dir_path: Path::new("target/tests"),
                }),
                fec,
                ..ConnectConfig::default()
            },
        )
        .await
        .unwrap();
        println!("connected");
        let mut buf = [0; 1024];
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            connected.read.recv(&mut buf),
        )
        .await
        .expect("client: timed out waiting for the echo")
        .unwrap();
        assert_eq!(msg_1, &buf[..n]);
    }

    #[tokio::test]
    async fn identity_listener_uses_tokio_udp_transport() {
        fn require_tokio_udp(_listener: &UtpListener<VectoredUdpSocket, SocketAddr, Packet>) {}
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        require_tokio_udp(&listener.listener);
        #[cfg(unix)]
        assert!(tokio_udp::is_vectored_supported());
    }

    #[test]
    fn require_fn_to_be_send() {
        fn require_send<T: Send>(_t: T) {}
        require_send(connect_with(
            "0.0.0.0:0",
            "0.0.0.0:0",
            ConnectConfig::default(),
        ));
    }

    #[derive(Debug)]
    struct Dummy;
    #[async_trait]
    impl UnreliableRead for Dummy {
        fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
        async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
    }
    #[async_trait]
    impl UnreliableWrite for Dummy {
        async fn send(&mut self, _buf: &[u8]) -> Result<usize, IoErr> {
            Ok(0)
        }
    }

    #[test]
    fn checked_mss_default_matches_legacy_derivation() {
        let layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(Dummy),
            Box::new(Dummy),
            false,
            ValidMss::try_new(NO_FEC_MSS).unwrap(),
            FecTuning::default(),
            FrameMode::default(),
        )
        .unwrap();
        assert_eq!(layer.mss.get(), NO_FEC_MSS);
        assert!(layer.fec.is_none());
    }

    #[test]
    fn checked_mss_rejects_oversized() {
        assert!(matches!(
            ValidMss::try_new(MAX_MSS + 1),
            Err(MssError::ExceedsDatagramCeiling { .. })
        ));
    }

    #[test]
    fn checked_mss_rejects_undersized() {
        assert!(matches!(
            ValidMss::try_new(1),
            Err(MssError::NoRoomForCodecPayload { .. })
        ));
    }

    #[test]
    fn checked_mss_fec_default_matches_legacy_derivation() {
        let layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(Dummy),
            Box::new(Dummy),
            true,
            ValidMss::try_new(NO_FEC_MSS).unwrap(),
            FecTuning::default(),
            FrameMode::default(),
        )
        .unwrap();
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
                    .accept_without_handshake_with(AcceptConfig {
                        fec,
                        mss: MssConfig::Custom(mss),
                        ..AcceptConfig::default()
                    })
                    .await
                    .unwrap();
                let msg = msg_for_server.clone();
                tokio::spawn(async move {
                    let mut accepted = accepted;
                    let mut buf = vec![0; msg.len()];
                    let n = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        accepted.read.recv(&mut buf),
                    )
                    .await
                    .expect("server: timed out waiting for the large message")
                    .unwrap();
                    let mut offset = n;
                    while offset < msg.len() {
                        let k = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            accepted.read.recv(&mut buf[offset..]),
                        )
                        .await
                        .expect("server: timed out reading the rest of the large message")
                        .unwrap();
                        if k == 0 {
                            break;
                        }
                        offset += k;
                    }
                    assert_eq!(msg, &buf[..offset]);
                    accepted.write.send(b"\x01").await.unwrap();
                });
            }
        });

        let mut connected = connect_with(
            "0.0.0.0:0",
            addr,
            ConnectConfig {
                handshake: false,
                fec,
                mss: MssConfig::Custom(mss),
                ..ConnectConfig::default()
            },
        )
        .await
        .unwrap();
        let mut buf = [0; 1];
        // `send` stages only up to the send-rate cap at once, so loop until the
        // whole message is handed to the reliable layer.
        let mut written = 0;
        while written < msg.len() {
            let n = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                connected.write.send(&msg[written..]),
            )
            .await
            .expect("client: timed out sending the large message")
            .unwrap();
            assert!(0 < n, "send must make progress");
            written += n;
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            connected.read.recv(&mut buf),
        )
        .await
        .expect("client: timed out waiting for the server ack")
        .unwrap();
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

    /// Fix #10: `frame_delivery_mss_to_small_for_first_frame_header_errors`
    /// — constructing a frame-delivery connection with an MSS too small for
    /// the first-frame header errors instead of silently producing 0-byte-
    /// payload first packets.
    #[test]
    fn frame_delivery_mss_to_small_for_first_frame_header_errors() {
        use crate::delivery::frame::FrameMode;
        // An MSS that is large enough for `data_overhead` but too small for
        // `frame_data_overhead` (data_overhead + 4).
        let mss = crate::codec::data_overhead() + 1;
        let res = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(Dummy),
            Box::new(Dummy),
            false,
            ValidMss::try_new(mss).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            FrameMode::enabled(),
        );
        assert!(matches!(
            res,
            Err(MssError::NoRoomForFirstFrameHeader { .. })
        ));
    }

    #[test]
    fn mss_config_resolves_default_and_custom_values() {
        assert_eq!(MssConfig::Default.resolve().unwrap().get(), NO_FEC_MSS);
        assert_eq!(MssConfig::Custom(9_000).resolve().unwrap().get(), 9_000);
    }

    fn spawn_accept_loop(listener: Listener) -> SocketAddr {
        let addr = listener.local_addr();
        tokio::spawn(async move {
            loop {
                if listener.accept_without_handshake().await.is_err() {
                    break;
                }
            }
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn listener_echoes_probe_with_direction_flipped() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = spawn_accept_loop(listener);
        let prober = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        prober.connect(addr).await.unwrap();
        let probe = crate::path_probe::encode_probe(crate::path_probe::ProbeEcho {
            nonce: 0xDEAD_BEEF,
            timestamp_micros: 12345,
        });
        prober.send(&probe).await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), prober.recv(&mut buf))
            .await
            .expect("probe echo timed out")
            .unwrap();
        let echo = crate::path_probe::decode_echo(&buf[..n]).expect("not a probe echo");
        assert_eq!(
            echo,
            crate::path_probe::ProbeEcho {
                nonce: 0xDEAD_BEEF,
                timestamp_micros: 12345
            }
        );
        assert_eq!(buf[..8], probe[..8]);
        assert_eq!(buf[9..n], probe[9..]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn probes_never_create_connection_state() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr();
        let accept =
            tokio::spawn(
                async move { listener.accept_without_handshake().await.unwrap().peer_addr },
            );
        let prober = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        prober
            .send_to(
                &crate::path_probe::encode_probe(crate::path_probe::ProbeEcho {
                    nonce: 1,
                    timestamp_micros: 2,
                }),
                addr,
            )
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        prober.recv(&mut buf).await.unwrap();
        assert!(!accept.is_finished(), "a probe created connection state");
        let real = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        real.send_to(b"payload", addr).await.unwrap();
        let accepted_peer = tokio::time::timeout(std::time::Duration::from_secs(2), accept)
            .await
            .expect("accept timed out")
            .unwrap();
        assert_eq!(accepted_peer, real.local_addr().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn probe_floods_are_rate_limited_per_source() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = spawn_accept_loop(listener);
        let prober = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        prober.connect(addr).await.unwrap();
        for nonce in 0..200u64 {
            let _ = prober
                .send(&crate::path_probe::encode_probe(
                    crate::path_probe::ProbeEcho {
                        nonce,
                        timestamp_micros: 0,
                    },
                ))
                .await;
        }
        let mut echoes = 0usize;
        let mut buf = [0u8; 64];
        while let Ok(Ok(n)) =
            tokio::time::timeout(std::time::Duration::from_millis(300), prober.recv(&mut buf)).await
        {
            if crate::path_probe::decode_echo(&buf[..n]).is_some() {
                echoes += 1;
            }
        }
        assert!(echoes >= 1, "no probe was answered at all");
        assert!(
            echoes <= 48,
            "flood was not rate limited: {echoes} echoes for 200 probes"
        );
    }

    fn spawn_greeting_server(listener: Listener, reply: &'static [u8]) -> SocketAddr {
        let addr = listener.local_addr();
        let listener = Arc::new(listener);
        tokio::spawn(async move {
            let mut accepted = listener.accept_without_handshake().await.unwrap();
            tokio::spawn({
                let listener = Arc::clone(&listener);
                async move {
                    loop {
                        if listener.accept_without_handshake().await.is_err() {
                            break;
                        }
                    }
                }
            });
            let mut buf = [0u8; 64];
            let _ = accepted.read.recv(&mut buf).await;
            accepted.write.send(reply).await.unwrap();
            let _ = accepted.read.recv(&mut buf).await;
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn probe_tap_probes_the_sessions_own_tuple() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = spawn_greeting_server(listener, b"data");
        let mut connected = connect_with(
            "127.0.0.1:0",
            addr,
            ConnectConfig {
                handshake: false,
                ..ConnectConfig::default()
            },
        )
        .await
        .unwrap();
        let mut tap = connected
            .probe_tap
            .take()
            .expect("client connects carry a tap");
        connected.write.send(b"hi").await.unwrap();
        let mut buf = [0; 16];
        let n = connected.read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"data");
        tap.send_probe(crate::path_probe::ProbeEcho {
            nonce: 99,
            timestamp_micros: 7,
        })
        .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if let Some(echo) = tap.try_recv_echo() {
                assert_eq!(echo.nonce, 99);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "probe echo never reached the tap"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connect_with_socket_preserves_the_bound_local_addr() {
        let listener = Listener::bind("127.0.0.1:0").await.unwrap();
        let addr = spawn_greeting_server(listener, b"hello");
        let socket = VectoredUdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let bound = socket.local_addr().unwrap();
        let mut connected = connect_with_socket(
            socket,
            addr,
            ConnectConfig {
                handshake: false,
                mss: MssConfig::Custom(NO_FEC_MSS),
                ..ConnectConfig::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(connected.local_addr, bound);
        connected.write.send(b"hi").await.unwrap();
        let mut buf = [0; 16];
        let n = connected.read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
    }

    #[test]
    fn dialable_addr_rewrites_only_an_unspecified_ip() {
        for (input, want) in [
            ("0.0.0.0:5", "127.0.0.1:5"),
            ("[::]:5", "[::1]:5"),
            ("127.0.0.1:5", "127.0.0.1:5"),
            ("192.168.1.2:5", "192.168.1.2:5"),
            ("[::1]:5", "[::1]:5"),
        ] {
            let got = dialable_addr(input.parse().unwrap());
            assert_eq!(got, want.parse::<SocketAddr>().unwrap(), "input: {input}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dialing_a_wildcard_listener_addr_stays_connected() {
        let listener = Listener::bind("0.0.0.0:0").await.unwrap();
        assert!(listener.local_addr().ip().is_unspecified());
        let addr = spawn_greeting_server(listener, b"hello");
        let mut connected = connect_with(
            "0.0.0.0:0",
            addr,
            ConnectConfig {
                handshake: false,
                ..ConnectConfig::default()
            },
        )
        .await
        .unwrap();
        assert!(!connected.local_addr.ip().is_unspecified());
        assert!(!connected.peer_addr.ip().is_unspecified());
        connected.write.send(b"hi").await.unwrap();
        let mut buf = [0; 16];
        let n = connected.read.recv(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
    }
}
