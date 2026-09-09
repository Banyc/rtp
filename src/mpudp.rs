use std::{io, net::SocketAddr, num::NonZeroUsize};

use async_trait::async_trait;
use mpudp::{conn::MpUdpConn, listen::MpUdpListener, read::MpUdpRead, write::MpUdpWrite};

use crate::io_err::IoErr;
use crate::{
    delivery::frame::FrameMode,
    socket::{ConnReader, ConnWriter, SessionHandle, socket},
    traffic_shaping::redundancy::{RetransmissionArmorConfig, fec_tuning::FecTuning},
    transmission::transmission_layer::{UnreliableRead, UnreliableWrite},
    udp::{
        AcceptConfig, ConnectConfig, LogConfig, Mss, MssConfig, PaddingPolicy,
        wrap_fec_with_mss_and_fec_tuning_and_frame_delivery,
    },
};

pub const MPUDP_MSS: usize = 1400;
const DISPATCHER_BUF_SIZE: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

#[derive(Debug)]
pub struct Listener {
    listener: MpUdpListener,
}
impl Listener {
    pub async fn bind(
        addrs: impl Iterator<Item = SocketAddr>,
        max_session_conns: NonZeroUsize,
    ) -> io::Result<Self> {
        let listener = MpUdpListener::bind(addrs, max_session_conns, DISPATCHER_BUF_SIZE).await?;
        Ok(Self { listener })
    }
    pub fn local_addrs(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.listener.local_addrs()
    }
    pub async fn accept_with(&mut self, config: AcceptConfig) -> io::Result<Conn> {
        let conn = self.listener.accept().await?;
        convert_conn(conn, None, LayerTuning::from_accept(config)?).await
    }
}
#[derive(Debug)]
pub struct Conn {
    pub read: ConnReader,
    pub write: ConnWriter,
    pub supervisor: SessionHandle,
}
impl Conn {
    pub async fn connect_with(
        addrs: impl Iterator<Item = SocketAddr>,
        config: ConnectConfig<'_>,
    ) -> io::Result<Self> {
        let conn = MpUdpConn::connect(addrs).await?;
        let (log_config, tuning) = LayerTuning::from_connect(config)?;
        convert_conn(conn, log_config, tuning).await
    }
}

/// The layer-tuning fields both [`AcceptConfig`] and [`ConnectConfig`]
/// carry, bundled so [`convert_conn`] takes one argument instead of nine.
struct LayerTuning {
    fec: bool,
    mss: Mss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
    retransmission_armor: RetransmissionArmorConfig,
    instream_group_fec: bool,
    metrics_observer: Option<crate::metrics::MetricsObserver>,
    obfuscation_key: Option<[u8; crate::obfuscate::KEY_LEN]>,
    /// The DPI-hiding padding policy, resolved from the config into the
    /// wrapper's padding settings and the write half's fitted-ACK-padding
    /// toggle at [`convert_conn`] (exactly one is ever active).
    padding: PaddingPolicy,
}

impl LayerTuning {
    fn from_accept(config: AcceptConfig) -> io::Result<Self> {
        Ok(Self {
            fec: config.fec,
            mss: resolve_mss(config.mss, config.obfuscation_key.is_some())?,
            tuning: config.fec_tuning,
            frame_delivery: config.frame_delivery,
            retransmission_armor: config.retransmission_armor,
            instream_group_fec: config.instream_group_fec,
            metrics_observer: config.metrics_observer,
            obfuscation_key: config.obfuscation_key,
            padding: config.padding,
        })
    }

    fn from_connect(config: ConnectConfig<'_>) -> io::Result<(Option<LogConfig<'_>>, Self)> {
        let log_config = config.log_config;
        Ok((
            log_config,
            Self {
                fec: config.fec,
                mss: resolve_mss(config.mss, config.obfuscation_key.is_some())?,
                tuning: config.fec_tuning,
                frame_delivery: config.frame_delivery,
                retransmission_armor: config.retransmission_armor,
                instream_group_fec: config.instream_group_fec,
                metrics_observer: config.metrics_observer,
                obfuscation_key: config.obfuscation_key,
                padding: config.padding,
            },
        ))
    }
}

/// Resolve the mpudp MSS: the shared [`MssConfig::Default`] resolves to the
/// single-path [`crate::udp::NO_FEC_MSS`], but mpudp's default is the more
/// conservative [`MPUDP_MSS`] (multi-path links have tighter MTU budgets).
/// A custom config is honored as-is. When datagram obfuscation is enabled,
/// the 24-byte nonce is reserved from the MSS (the wire datagram stays
/// within the configured MSS).
fn resolve_mss(config: MssConfig, obfuscated: bool) -> io::Result<Mss> {
    let mss = match config {
        MssConfig::Default => Mss::try_new(MPUDP_MSS),
        MssConfig::Custom(mss) => Mss::try_new(mss),
    }
    .map_err(io::Error::from)?;
    if obfuscated {
        mss.reduced_for_obfuscation().map_err(io::Error::from)
    } else {
        Ok(mss)
    }
}

async fn convert_conn(
    conn: MpUdpConn,
    log_config: Option<LogConfig<'_>>,
    tuning: LayerTuning,
) -> io::Result<Conn> {
    let log_config = match log_config {
        Some(c) => {
            let zero_addr = "0.0.0.0:0".parse().unwrap();
            Some(
                c.transmission_layer_log_config(zero_addr, zero_addr)
                    .await?,
            )
        }
        None => None,
    };
    let (r, w) = conn.into_split();
    // Datagram obfuscation (when a key is configured): every datagram is
    // prefixed with a 24-byte random nonce and the rest is chacha20-
    // encrypted, exactly like the single-path udp constructors.
    // Resolve the DPI-hiding policy: the wrapper's padding settings and
    // the write half's fitted-ACK-padding toggle (exactly one active).
    let (profile, ack_padding) = tuning.padding.resolve();
    let (r, w) = crate::obfuscate::maybe_wrap(
        r,
        w,
        tuning
            .obfuscation_key
            .map(|key| crate::obfuscate::Obfuscation {
                key,
                settings: profile,
            }),
    );
    let mut unreliable_layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        r,
        w,
        tuning.fec,
        tuning.mss,
        tuning.tuning,
        tuning.frame_delivery,
    )?;
    unreliable_layer.retransmission_armor = tuning.retransmission_armor;
    unreliable_layer.instream_group_fec = tuning.instream_group_fec;
    unreliable_layer.metrics_observer = tuning.metrics_observer;
    // Fitted ACK padding lives in the write half; the policy resolution
    // guarantees it never coexists with a padding profile.
    unreliable_layer.ack_padding = ack_padding;
    let (read, write, supervisor) = socket(unreliable_layer, log_config);
    let conn = Conn {
        read,
        write,
        supervisor,
    };
    Ok(conn)
}

#[async_trait]
impl UnreliableRead for MpUdpRead {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        loop {
            match self.try_recv(buf) {
                Ok(None) => return Err(io::ErrorKind::WouldBlock.into()),
                Ok(Some(n)) => return Ok(n),
                Err(mpudp::read::RecvError::BadPacket) => continue,
                Err(mpudp::read::RecvError::Dead) => {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
    }
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
        loop {
            match self.recv(buf).await {
                Ok(n) => return Ok(n),
                Err(mpudp::read::RecvError::BadPacket) => continue,
                Err(mpudp::read::RecvError::Dead) => {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
    }
}

#[async_trait]
impl UnreliableWrite for MpUdpWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        MpUdpWrite::send(self, buf)
            .await
            .map(|_| buf.len())
            .map_err(IoErr::from)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect() -> std::io::Result<()> {
        let max_session_conns = NonZeroUsize::new(1 << 10).unwrap();
        let mut listener = Listener::bind(
            ["127.0.0.1:0"].map(|x| x.parse().unwrap()).into_iter(),
            max_session_conns,
        )
        .await
        .unwrap();
        let addrs = listener.local_addrs().collect::<Vec<SocketAddr>>();
        let msg_1 = b"hello";

        // Notify handshakes: `echo_received` proves the client got the echo,
        // `receipt_seen` proves the server read and validated the client's
        // receipt, and `release_server` lets the client hold the server's
        // session open until the client has proved the wire is quiescent.
        // Each `notify_one()` is ordering-safe (a stored permit satisfies a
        // waiter registered later), so neither side can miss the other's
        // signal.
        let echo_received = std::sync::Arc::new(tokio::sync::Notify::new());
        let receipt_seen = std::sync::Arc::new(tokio::sync::Notify::new());
        let release_server = std::sync::Arc::new(tokio::sync::Notify::new());

        // One function-scoped JoinSet owns the listener and the accepted
        // read/write/supervisor halves together: a single supervised server
        // task holds all four, so no detached task can outlive the supervisor
        // or drop it mid-write.
        let mut server = tokio::task::JoinSet::new();
        server.spawn({
            let echo_received = std::sync::Arc::clone(&echo_received);
            let receipt_seen = std::sync::Arc::clone(&receipt_seen);
            let release_server = std::sync::Arc::clone(&release_server);
            async move {
                let mut accepted = listener.accept_with(AcceptConfig::default()).await.unwrap();
                println!("accepted");
                accepted.write.send(msg_1).await.unwrap();
                // Drain the staged echo before anything can proceed: `send`
                // only stages bytes, so the write driver must put them on the
                // wire first.
                accepted.write.send_buf_empty().await.unwrap();
                // Wait for the client's confirmation that the echo arrived.
                echo_received.notified().await;
                // Read the client's receipt: the reliable layer hands it up in
                // order, so this is a genuine application-level ack.
                let mut receipt = [0; 1];
                let n = accepted.read.recv(&mut receipt).await.unwrap();
                assert_eq!(n, 1);
                assert_eq!(receipt, [0]);
                // The receipt has been read and validated; tell the client.
                receipt_seen.notify_one();
                // Hold the session open until the client releases us: dropping
                // it now would tear the connection down before the client can
                // prove the wire is quiescent.
                release_server.notified().await;
            }
        });

        let mut connected = Conn::connect_with(
            addrs.into_iter(),
            ConnectConfig {
                log_config: Some(LogConfig {
                    log_dir_path: Path::new("target/tests"),
                }),
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
        // The echo is confirmed: release the server's wait.
        echo_received.notify_one();
        // Send the application-level receipt and drain it onto the wire.  The
        // drain result is propagated: the proof below only holds if the bytes
        // actually reached the wire.
        connected.write.send(b"\x00").await.unwrap();
        connected.write.send_buf_empty().await?;
        // Prove zero in-flight packets and that the receipt has been acked by
        // the peer's reliable layer: `no_data_to_send` waits until the send
        // buffer is empty AND every sent packet has been acked.
        connected.write.all_sent_data_acked().await?;
        // Await the server's application-level receipt confirmation separately.
        receipt_seen.notified().await;
        // Release the server's session and join it; the set must be empty.
        release_server.notify_one();
        server
            .join_next()
            .await
            .expect("server task missing")
            .unwrap();
        assert!(server.is_empty());
        Ok(())
    }

    fn mpudp_header(with_payload: bool) -> [u8; 17] {
        let mut header = [0; 17];
        header[..8].copy_from_slice(&1_u64.to_be_bytes());
        header[8..16].copy_from_slice(&1_u64.to_be_bytes());
        header[16] = u8::from(with_payload);
        header
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_corrupt_datagram_does_not_kill_the_session() {
        use crate::transmission::transmission_layer::UnreliableRead;
        use mpudp::listen::MpUdpListener;
        let mut listener = MpUdpListener::bind(
            ["127.0.0.1:0".parse().unwrap()].into_iter(),
            NonZeroUsize::new(1).unwrap(),
            DISPATCHER_BUF_SIZE,
        )
        .await
        .unwrap();
        let server_addr = listener.local_addrs().next().unwrap();
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        peer.connect(server_addr).await.unwrap();
        peer.send(&mpudp_header(false)).await.unwrap();
        let conn = listener.accept().await.unwrap();
        let (mut read, _write) = conn.into_split();
        peer.send(b"too short to be a header").await.unwrap();
        let mut good = mpudp_header(true).to_vec();
        good.extend_from_slice(b"ok");
        peer.send(&good).await.unwrap();
        let mut buf = [0; 64];
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            UnreliableRead::recv(&mut read, &mut buf),
        )
        .await
        .expect("the read path stalled after a corrupt datagram")
        .expect("a corrupt datagram from the peer's address killed the session");
        assert_eq!(&buf[..n], b"ok");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_send_reports_the_caller_s_bytes_not_the_wire_s() {
        let server = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let conn = MpUdpConn::connect([server.local_addr().unwrap()].into_iter())
            .await
            .unwrap();
        let (_read, mut write) = conn.into_split();
        let payload = b"a datagram of a known length";
        let n = UnreliableWrite::send(&mut write, payload).await.unwrap();
        assert_eq!(
            n,
            payload.len(),
            "the transport counted its own header, so every send looks short"
        );
    }

    #[test]
    fn require_fn_to_be_send() {
        fn require_send<T: Send>(_t: T) {}
        require_send(Conn::connect_with(
            ["0.0.0.0:0".parse().unwrap()].into_iter(),
            ConnectConfig::default(),
        ));
    }

    #[test]
    fn resolve_mss_reserves_the_obfuscation_nonce() {
        // Default (MPUDP_MSS) with obfuscation: reduced by the nonce so the
        // wire datagram stays within the configured MSS.
        let mss = resolve_mss(MssConfig::Default, true).unwrap();
        assert_eq!(mss.get(), MPUDP_MSS - crate::obfuscate::NONCE_LEN);
        // Without obfuscation: the full MPUDP_MSS.
        let mss = resolve_mss(MssConfig::Default, false).unwrap();
        assert_eq!(mss.get(), MPUDP_MSS);
        // A custom MSS too small to carry the nonce on top of the codec
        // payload is rejected.
        let err =
            resolve_mss(MssConfig::Custom(crate::codec::data_overhead() + 1), true).unwrap_err();
        assert!(
            err.to_string().contains("obfuscation nonce"),
            "a too-small MSS must fail naming the nonce reservation, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn obfuscation_round_trips_through_mpudp() -> std::io::Result<()> {
        const KEY: [u8; crate::obfuscate::KEY_LEN] = [7; crate::obfuscate::KEY_LEN];
        let max_session_conns = NonZeroUsize::new(1 << 10).unwrap();
        let mut listener = Listener::bind(
            ["127.0.0.1:0"].map(|x| x.parse().unwrap()).into_iter(),
            max_session_conns,
        )
        .await
        .unwrap();
        let addrs = listener.local_addrs().collect::<Vec<SocketAddr>>();
        let msg_1 = b"obfuscated mpudp hello";

        let echo_received = std::sync::Arc::new(tokio::sync::Notify::new());
        let receipt_seen = std::sync::Arc::new(tokio::sync::Notify::new());
        let release_server = std::sync::Arc::new(tokio::sync::Notify::new());

        let mut server = tokio::task::JoinSet::new();
        server.spawn({
            let echo_received = std::sync::Arc::clone(&echo_received);
            let receipt_seen = std::sync::Arc::clone(&receipt_seen);
            let release_server = std::sync::Arc::clone(&release_server);
            async move {
                let mut accepted = listener
                    .accept_with(AcceptConfig {
                        obfuscation_key: Some(KEY),
                        ..AcceptConfig::default()
                    })
                    .await
                    .unwrap();
                accepted.write.send(msg_1).await.unwrap();
                accepted.write.send_buf_empty().await.unwrap();
                echo_received.notified().await;
                let mut receipt = [0; 1];
                let n = accepted.read.recv(&mut receipt).await.unwrap();
                assert_eq!(n, 1);
                assert_eq!(receipt, [0]);
                receipt_seen.notify_one();
                release_server.notified().await;
            }
        });

        let mut connected = Conn::connect_with(
            addrs.into_iter(),
            ConnectConfig {
                obfuscation_key: Some(KEY),
                ..ConnectConfig::default()
            },
        )
        .await
        .unwrap();
        let mut buf = [0; 1024];
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            connected.read.recv(&mut buf),
        )
        .await
        .expect("client: timed out waiting for the obfuscated echo")
        .unwrap();
        assert_eq!(msg_1, &buf[..n]);
        echo_received.notify_one();
        connected.write.send(b"\x00").await.unwrap();
        connected.write.send_buf_empty().await?;
        connected.write.all_sent_data_acked().await?;
        receipt_seen.notified().await;
        release_server.notify_one();
        server
            .join_next()
            .await
            .expect("server task missing")
            .unwrap();
        assert!(server.is_empty());
        Ok(())
    }
}
