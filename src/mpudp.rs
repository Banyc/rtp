use std::{io, net::SocketAddr, num::NonZeroUsize};
use tokio::sync::Mutex as TokioMutex;

use async_trait::async_trait;
use mpudp::{conn::MpUdpConn, listen::MpUdpListener, read::MpUdpRead, write::MpUdpWrite};

use crate::{
    socket::{socket, ReadSocket, WriteSocket},
    transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite},
    udp::LogConfig,
};

pub const MSS: usize = 1400;
const DISPATCHER_BUF_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1024) };

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
        convert_conn(conn, None).await
    }
}
#[derive(Debug)]
pub struct Conn {
    pub read: ReadSocket,
    pub write: WriteSocket,
}
impl Conn {
    pub async fn connect_without_handshake(
        addrs: impl Iterator<Item = SocketAddr>,
        log_config: Option<LogConfig<'_>>,
    ) -> io::Result<Self> {
        let conn = MpUdpConn::connect(addrs).await?;
        convert_conn(conn, log_config).await
    }
}
async fn convert_conn(conn: MpUdpConn, log_config: Option<LogConfig<'_>>) -> io::Result<Conn> {
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
    let unreliable_layer = UnreliableLayer {
        utp_read: Box::new(r),
        utp_write: Box::new(AtomicMpUdpWrite::new(w)),
        mss: NonZeroUsize::new(MSS).unwrap(),
    };
    let (read, write) = socket(unreliable_layer, log_config);
    let conn = Conn { read, write };
    Ok(conn)
}

#[async_trait]
impl UnreliableRead for MpUdpRead {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        match self.try_recv(buf) {
            Ok(None) => Err(io::ErrorKind::WouldBlock),
            Ok(Some(n)) => Ok(n),
            Err(e) => Err(match e {
                mpudp::read::RecvError::Dead => io::ErrorKind::UnexpectedEof,
                mpudp::read::RecvError::BadPacket => io::ErrorKind::InvalidData,
            }),
        }
    }
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.recv(buf).await.map_err(|e| match e {
            mpudp::read::RecvError::Dead => io::ErrorKind::UnexpectedEof,
            mpudp::read::RecvError::BadPacket => io::ErrorKind::InvalidData,
        })
    }
}

#[derive(Debug)]
pub struct AtomicMpUdpWrite {
    write: TokioMutex<MpUdpWrite>,
}
impl AtomicMpUdpWrite {
    pub fn new(write: MpUdpWrite) -> Self {
        Self {
            write: TokioMutex::new(write),
        }
    }
}
#[async_trait]
impl UnreliableWrite for AtomicMpUdpWrite {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        let mut w = self.write.lock().await;
        w.send(buf).await.map_err(|e| e.kind())
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
        let connected = Conn::connect_without_handshake(
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

    #[test]
    fn require_fn_to_be_send() {
        fn require_send<T: Send>(_t: T) {}
        require_send(Conn::connect_without_handshake(
            ["0.0.0.0:0".parse().unwrap()].into_iter(),
            None,
        ));
    }
}
