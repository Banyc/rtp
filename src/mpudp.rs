use std::{io, net::SocketAddr, num::NonZeroUsize};

use async_trait::async_trait;
use mpudp::{conn::MpUdpConn, listen::MpUdpListener, read::MpUdpRead, write::MpUdpWrite};

use crate::io_err::IoErr;
use crate::{
    delivery::frame::FrameMode,
    socket::{ConnReader, ConnWriter, SessionHandle, socket},
    transmission::{
        fec_tuning::FecTuning,
        transmission_layer::{UnreliableRead, UnreliableWrite},
    },
    udp::{
        AcceptConfig, ConnectConfig, LogConfig, ValidMss,
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
        convert_conn(conn, None, config.fec_tuning, config.frame_delivery).await
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
        convert_conn(
            conn,
            config.log_config,
            config.fec_tuning,
            config.frame_delivery,
        )
        .await
    }
}
async fn convert_conn(
    conn: MpUdpConn,
    log_config: Option<LogConfig<'_>>,
    tuning: FecTuning,
    frame_delivery: FrameMode,
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
    let unreliable_layer = wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        Box::new(r),
        Box::new(w),
        false,
        ValidMss::try_new(MPUDP_MSS).unwrap(),
        tuning,
        frame_delivery,
    )?;
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
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect() {
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
        // `receipt_acked` proves the server read the client's receipt.  Each
        // `notify_one()` is ordering-safe (a stored permit satisfies a waiter
        // registered later), so neither side can miss the other's signal.
        let echo_received = std::sync::Arc::new(tokio::sync::Notify::new());
        let receipt_acked = std::sync::Arc::new(tokio::sync::Notify::new());

        // One function-scoped JoinSet owns the listener and the accepted
        // read/write/supervisor halves together: a single supervised server
        // task holds all four, so no detached task can outlive the supervisor
        // or drop it mid-write.
        let mut server = tokio::task::JoinSet::new();
        server.spawn({
            let echo_received = std::sync::Arc::clone(&echo_received);
            let receipt_acked = std::sync::Arc::clone(&receipt_acked);
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
                receipt_acked.notify_one();
                // Drain anything still staged before the session is released.
                let _ = accepted.write.send_buf_empty().await;
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
        // drain result must not be ignored: the epilog below only proves the
        // server acked the receipt if it actually reached the wire.
        connected.write.send(b"\x00").await.unwrap();
        connected.write.send_buf_empty().await.unwrap();
        // Wait for the server to ack the receipt before releasing our session.
        receipt_acked.notified().await;
        // The server task has ended and released its listener and session.
        server
            .join_next()
            .await
            .expect("server task missing")
            .unwrap();
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
}
