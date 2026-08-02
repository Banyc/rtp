use std::{io, net::SocketAddr, num::NonZeroUsize};

use async_trait::async_trait;
use mpudp::{conn::MpUdpConn, listen::MpUdpListener, read::MpUdpRead, write::MpUdpWrite};

use crate::io_err::IoErr;
use crate::{
    socket::{ReadSocket, SessionSupervisor, WriteSocket, socket},
    transmission::{
        fec_tuning::FecTuning,
        frame_delivery::{FrameDelivery, frame_delivery_from_env},
        transmission_layer::{UnreliableRead, UnreliableWrite},
    },
    udp::{LogConfig, wrap_fec_with_mss_and_fec_tuning_and_frame_delivery},
};

pub const MSS: usize = 1400;
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
    pub async fn accept_without_handshake(&mut self) -> io::Result<Conn> {
        let conn = self.listener.accept().await?;
        let frame_delivery = frame_delivery_from_env();
        convert_conn(conn, None, FecTuning::default(), frame_delivery).await
    }

    pub async fn accept_without_handshake_with_fec_tuning(
        &mut self,
        tuning: FecTuning,
    ) -> io::Result<Conn> {
        let conn = self.listener.accept().await?;
        let frame_delivery = frame_delivery_from_env();
        convert_conn(conn, None, tuning, frame_delivery).await
    }

    pub async fn accept_without_handshake_with_fec_tuning_and_frame_delivery(
        &mut self,
        tuning: FecTuning,
        frame_delivery: FrameDelivery,
    ) -> io::Result<Conn> {
        let conn = self.listener.accept().await?;
        convert_conn(conn, None, tuning, frame_delivery).await
    }
}
#[derive(Debug)]
pub struct Conn {
    pub read: ReadSocket,
    pub write: WriteSocket,
    pub supervisor: SessionSupervisor,
}
impl Conn {
    pub async fn connect_without_handshake(
        addrs: impl Iterator<Item = SocketAddr>,
        log_config: Option<LogConfig<'_>>,
    ) -> io::Result<Self> {
        let conn = MpUdpConn::connect(addrs).await?;
        let frame_delivery = frame_delivery_from_env();
        convert_conn(conn, log_config, FecTuning::default(), frame_delivery).await
    }

    pub async fn connect_without_handshake_with_fec_tuning(
        addrs: impl Iterator<Item = SocketAddr>,
        log_config: Option<LogConfig<'_>>,
        tuning: FecTuning,
    ) -> io::Result<Self> {
        let conn = MpUdpConn::connect(addrs).await?;
        let frame_delivery = frame_delivery_from_env();
        convert_conn(conn, log_config, tuning, frame_delivery).await
    }

    pub async fn connect_without_handshake_with_fec_tuning_and_frame_delivery(
        addrs: impl Iterator<Item = SocketAddr>,
        log_config: Option<LogConfig<'_>>,
        tuning: FecTuning,
        frame_delivery: FrameDelivery,
    ) -> io::Result<Self> {
        let conn = MpUdpConn::connect(addrs).await?;
        convert_conn(conn, log_config, tuning, frame_delivery).await
    }
}
async fn convert_conn(
    conn: MpUdpConn,
    log_config: Option<LogConfig<'_>>,
    tuning: FecTuning,
    frame_delivery: FrameDelivery,
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
        r,
        w,
        false,
        MSS,
        tuning,
        frame_delivery,
    );
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
        tokio::spawn(async move {
            loop {
                let mut accepted = listener.accept_without_handshake().await.unwrap();
                println!("accepted");
                tokio::spawn(async move {
                    accepted.write.send(msg_1).await.unwrap();
                    let mut buf = [0; 1];
                    accepted.read.recv(&mut buf).await.unwrap();
                });
            }
        });
        let mut connected = Conn::connect_without_handshake(
            addrs.into_iter(),
            Some(LogConfig {
                log_dir_path: Path::new("target/tests"),
            }),
        )
        .await
        .unwrap();
        println!("connected");
        let mut buf = [0; 1024];
        let n = connected.read.recv(&mut buf).await.unwrap();
        assert_eq!(msg_1, &buf[..n]);
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
        require_send(Conn::connect_without_handshake(
            ["0.0.0.0:0".parse().unwrap()].into_iter(),
            None,
        ));
    }
}
