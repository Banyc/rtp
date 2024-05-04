use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tokio::net::UdpSocket;
use tokio_util::bytes::Buf;
use udp_listener::{AcceptedUdpWrite, Packet, UdpListener};

use crate::{
    socket::{socket, ReadSocket, WriteSocket},
    transport_layer::{UnreliableLayer, UnreliableWrite},
};

const DISPATCHER_BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
pub struct Server<K> {
    listener: UdpListener<K, Packet>,
    local_addr: SocketAddr,
}
impl<K: DispatchKey> Server<K> {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;
        let local_addr = udp.local_addr()?;
        let listener = UdpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUFFER_SIZE).unwrap(),
            Arc::new(dispatch),
        );
        Ok(Self {
            listener,
            local_addr,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Side-effect: same as [`udp_listener::UdpListener::accept()`]
    pub async fn accept_without_handshake(&self) -> std::io::Result<Accepted<K>> {
        let accepted = self.listener.accept().await?;
        let dispatch_key = accepted.dispatch_key().clone();
        let (read, write) = accepted.split();
        let write = KeyedAcceptedUdpWrite::new(write, &dispatch_key);
        let unreliable_layer = UnreliableLayer {
            utp_read: Box::new(read),
            utp_write: Box::new(write),
            mss: NonZeroUsize::new(crate::udp::MSS.checked_sub(K::max_size()).unwrap()).unwrap(),
        };
        let (read, write) = socket(unreliable_layer, None);
        Ok(Accepted {
            read,
            write,
            dispatch_key,
        })
    }
}
#[derive(Debug)]
pub struct Accepted<K> {
    pub read: ReadSocket,
    pub write: WriteSocket,
    pub dispatch_key: K,
}

#[derive(Debug)]
pub struct Client<K> {
    listener: UdpListener<K, Packet>,
}
impl<K: DispatchKey> Client<K> {
    pub async fn connect_without_handshake(
        bind: impl tokio::net::ToSocketAddrs,
        server: impl tokio::net::ToSocketAddrs,
    ) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(bind).await?;
        udp.connect(server).await?;
        let listener = UdpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUFFER_SIZE).unwrap(),
            Arc::new(dispatch),
        );
        Ok(Self { listener })
    }

    /// Side-effect: same as [`udp_listener::UdpListener::accept()`]
    pub async fn dispatch(&self) -> std::io::Result<()> {
        loop {
            let _ = self.listener.accept().await?;
        }
    }

    pub fn open_without_handshake(&self, dispatch_key: K) -> Option<Connected> {
        let accepted = self.listener.open(dispatch_key.clone())?;
        let (read, write) = accepted.split();
        let write = KeyedAcceptedUdpWrite::new(write, &dispatch_key);
        let unreliable_layer = UnreliableLayer {
            utp_read: Box::new(read),
            utp_write: Box::new(write),
            mss: NonZeroUsize::new(crate::udp::MSS.checked_sub(K::max_size()).unwrap()).unwrap(),
        };
        let (read, write) = socket(unreliable_layer, None);
        Some(Connected { read, write })
    }
}
#[derive(Debug)]
pub struct Connected {
    pub read: ReadSocket,
    pub write: WriteSocket,
}

#[derive(Debug)]
pub struct KeyedAcceptedUdpWrite {
    write: AcceptedUdpWrite,
    buf: tokio::sync::Mutex<Vec<u8>>,
    data_offset: usize,
}
impl KeyedAcceptedUdpWrite {
    pub fn new<K: DispatchKey>(write: AcceptedUdpWrite, dispatch_key: &K) -> Self {
        let mut buf = vec![];
        let n = K::max_size();
        let fill = (0..n).map(|_| 0);
        buf.extend(fill);
        let n = dispatch_key.encode(&mut buf).unwrap();
        Self {
            write,
            buf: tokio::sync::Mutex::new(buf),
            data_offset: n,
        }
    }

    pub async fn send(&self, data: &[u8]) -> std::io::Result<usize> {
        let mut buf = self.buf.lock().await;
        buf.drain(self.data_offset..);
        buf.extend(data);
        self.write.send(&buf).await
    }
}
#[async_trait]
impl UnreliableWrite for KeyedAcceptedUdpWrite {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        Self::send(self, buf).await.map_err(|e| e.kind())
    }
}

pub trait DispatchKey:
    core::fmt::Debug + Clone + core::hash::Hash + Eq + Sized + Sync + Send + 'static
{
    /// Return the number of bytes written.
    ///
    /// Return [`None`] if something is wrong.
    fn encode(&self, buf: &mut [u8]) -> Option<usize>;
    /// Return the number of bytes read.
    ///
    /// Return [`None`] if something is wrong.
    fn decode(buf: &[u8]) -> Option<(usize, Self)>;
    /// Return max number of bytes of this type.
    fn max_size() -> usize;
}
impl DispatchKey for u8 {
    fn encode(&self, buf: &mut [u8]) -> Option<usize> {
        let mut wtr = std::io::Cursor::new(buf);
        wtr.write_u8(*self).ok()?;
        Some(usize::try_from(wtr.position()).unwrap())
    }

    fn decode(buf: &[u8]) -> Option<(usize, Self)> {
        let mut rdr = std::io::Cursor::new(buf);
        let this = rdr.read_u8().ok()?;
        Some((usize::try_from(rdr.position()).unwrap(), this))
    }

    fn max_size() -> usize {
        core::mem::size_of::<Self>()
    }
}
impl DispatchKey for u16 {
    fn encode(&self, buf: &mut [u8]) -> Option<usize> {
        let mut wtr = std::io::Cursor::new(buf);
        wtr.write_u16::<BigEndian>(*self).ok()?;
        Some(usize::try_from(wtr.position()).unwrap())
    }

    fn decode(buf: &[u8]) -> Option<(usize, Self)> {
        let mut rdr = std::io::Cursor::new(buf);
        let this = rdr.read_u16::<BigEndian>().ok()?;
        Some((usize::try_from(rdr.position()).unwrap(), this))
    }

    fn max_size() -> usize {
        core::mem::size_of::<Self>()
    }
}
impl DispatchKey for u32 {
    fn encode(&self, buf: &mut [u8]) -> Option<usize> {
        let mut wtr = std::io::Cursor::new(buf);
        wtr.write_u32::<BigEndian>(*self).ok()?;
        Some(usize::try_from(wtr.position()).unwrap())
    }

    fn decode(buf: &[u8]) -> Option<(usize, Self)> {
        let mut rdr = std::io::Cursor::new(buf);
        let this = rdr.read_u32::<BigEndian>().ok()?;
        Some((usize::try_from(rdr.position()).unwrap(), this))
    }

    fn max_size() -> usize {
        core::mem::size_of::<Self>()
    }
}
impl DispatchKey for u64 {
    fn encode(&self, buf: &mut [u8]) -> Option<usize> {
        let mut wtr = std::io::Cursor::new(buf);
        wtr.write_u64::<BigEndian>(*self).ok()?;
        Some(usize::try_from(wtr.position()).unwrap())
    }

    fn decode(buf: &[u8]) -> Option<(usize, Self)> {
        let mut rdr = std::io::Cursor::new(buf);
        let this = rdr.read_u64::<BigEndian>().ok()?;
        Some((usize::try_from(rdr.position()).unwrap(), this))
    }

    fn max_size() -> usize {
        core::mem::size_of::<Self>()
    }
}
impl DispatchKey for u128 {
    fn encode(&self, buf: &mut [u8]) -> Option<usize> {
        let mut wtr = std::io::Cursor::new(buf);
        wtr.write_u128::<BigEndian>(*self).ok()?;
        Some(usize::try_from(wtr.position()).unwrap())
    }

    fn decode(buf: &[u8]) -> Option<(usize, Self)> {
        let mut rdr = std::io::Cursor::new(buf);
        let this = rdr.read_u128::<BigEndian>().ok()?;
        Some((usize::try_from(rdr.position()).unwrap(), this))
    }

    fn max_size() -> usize {
        core::mem::size_of::<Self>()
    }
}

fn dispatch<K: DispatchKey>(_addr: SocketAddr, mut packet: Packet) -> Option<(K, Packet)> {
    let (n, key) = K::decode(&packet)?;
    packet.advance(n);
    Some((key, packet))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_connect() {
        let server = Server::<u8>::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        let key = 42;
        let msg_1 = b"hello";
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let server = Arc::new(server);
            let mut accepted = server.accept_without_handshake().await.unwrap();
            assert_eq!(accepted.dispatch_key, key);
            tokio::spawn({
                let server = server.clone();
                async move {
                    loop {
                        let _ = server.accept_without_handshake().await;
                    }
                }
            });
            let mut buf = vec![0; 1024];
            let n = accepted.read.recv(&mut buf).await.unwrap();
            let m = &buf[..n];
            assert_eq!(m, msg_1);
            accepted.write.send(msg_1).await.unwrap();
        });
        tasks.spawn(async move {
            let client = Client::<u8>::connect_without_handshake("0.0.0.0:0", addr)
                .await
                .unwrap();
            println!("connected");
            let client = Arc::new(client);
            tokio::spawn({
                let client = client.clone();
                async move {
                    loop {
                        client.dispatch().await.unwrap();
                    }
                }
            });
            let mut accepted = client.open_without_handshake(key).unwrap();
            accepted.write.send(msg_1).await.unwrap();
            let mut buf = vec![0; 1024];
            let n = accepted.read.recv(&mut buf).await.unwrap();
            let m = &buf[..n];
            assert_eq!(m, msg_1);
        });
        while let Some(res) = tasks.join_next().await {
            res.unwrap();
        }
    }
}
