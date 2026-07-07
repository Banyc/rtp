use core::{net::SocketAddr, num::NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tokio::net::UdpSocket;
use tokio_util::bytes::Buf;
use udp_listener::{ConnWrite, Packet, UtpListener};

use crate::{
    socket::{ReadSocket, WriteSocket, socket},
    transmission::{
        fec_tuning::{FecTuning, fec_tuning_from_env},
        transmission_layer::{UnreliableLayer, UnreliableWrite},
    },
    udp::{should_wait_after_try_send, wrap_fec, wrap_fec_with_mss_and_fec_tuning},
};

const DISPATCHER_BUF_SIZE: usize = 1024;

fn checked_keyed_mss<K: DispatchKey>(mss_after_fec: NonZeroUsize) -> NonZeroUsize {
    let key_size = K::max_size();
    let remaining = mss_after_fec
        .get()
        .checked_sub(key_size)
        .unwrap_or_else(|| {
            panic!("mss {mss_after_fec} leaves no room for the {key_size}-byte dispatch key")
        });
    let overhead = crate::codec::data_overhead();
    assert!(
        overhead < remaining,
        "mss {mss_after_fec} leaves no payload room after {key_size}-byte dispatch key and {overhead}-byte codec overhead"
    );
    NonZeroUsize::new(remaining).unwrap()
}

/// Apply the keyed-dispatch MSS reduction to an already-wrapped
/// [`UnreliableLayer`].  The dispatch key is prepended to every datagram, so
/// the effective MSS the reliable layer sees is `mss - key_size`.  The FEC
/// tuning is preserved unchanged.
fn apply_keyed_mss<K: DispatchKey>(layer: UnreliableLayer) -> UnreliableLayer {
    UnreliableLayer {
        utp_read: layer.utp_read,
        utp_write: layer.utp_write,
        mss: checked_keyed_mss::<K>(layer.mss),
        fec: layer.fec,
        fec_tuning: layer.fec_tuning,
    }
}

#[derive(Debug)]
pub struct Server<K> {
    listener: UtpListener<UdpSocket, K, Packet>,
    local_addr: SocketAddr,
}
impl<K: DispatchKey> Server<K> {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;
        let local_addr = udp.local_addr()?;
        let listener = UtpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUF_SIZE).unwrap(),
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

    /// [`Self::accept_without_handshake()`] with a custom MSS.
    pub async fn accept_without_handshake_with_mss(
        &self,
        fec: bool,
        mss: usize,
    ) -> std::io::Result<Accepted<K>> {
        let tuning = fec_tuning_from_env();
        self.accept_without_handshake_with_mss_and_fec_tuning(fec, mss, tuning)
            .await
    }

    /// [`Self::accept_without_handshake()`] with a custom MSS and a
    /// per-connection [`FecTuning`].  Both peers must agree on the MSS and the
    /// FEC flag; the FEC tuning is likewise out-of-band.
    pub async fn accept_without_handshake_with_mss_and_fec_tuning(
        &self,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
    ) -> std::io::Result<Accepted<K>> {
        let accepted = self.listener.accept().await?;
        let conn_key = accepted.conn_key().clone();
        let (read, write) = accepted.split();
        let write = KeyedConnWrite::new(write, &conn_key);
        let unreliable_layer = wrap_fec_with_mss_and_fec_tuning(read, write, fec, mss, tuning);
        let unreliable_layer = apply_keyed_mss::<K>(unreliable_layer);
        let (read, write) = socket(unreliable_layer, None);
        Ok(Accepted {
            read,
            write,
            dispatch_key: conn_key,
        })
    }

    /// Side-effect: same as [`udp_listener::UtpListener::accept()`]
    pub async fn accept_without_handshake(&self, fec: bool) -> std::io::Result<Accepted<K>> {
        let accepted = self.listener.accept().await?;
        let conn_key = accepted.conn_key().clone();
        let (read, write) = accepted.split();
        let write = KeyedConnWrite::new(write, &conn_key);
        let unreliable_layer = wrap_fec(read, write, fec);
        let unreliable_layer = apply_keyed_mss::<K>(unreliable_layer);
        let (read, write) = socket(unreliable_layer, None);
        Ok(Accepted {
            read,
            write,
            dispatch_key: conn_key,
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
    listener: UtpListener<UdpSocket, K, Packet>,
}
impl<K: DispatchKey> Client<K> {
    pub async fn connect_without_handshake(
        bind: impl tokio::net::ToSocketAddrs,
        server: impl tokio::net::ToSocketAddrs,
    ) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(bind).await?;
        udp.connect(server).await?;
        let listener = UtpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUF_SIZE).unwrap(),
            Arc::new(dispatch),
        );
        Ok(Self { listener })
    }

    /// Side-effect: same as [`udp_listener::UtpListener::accept()`]
    pub async fn dispatch(&self) -> std::io::Result<()> {
        loop {
            let _ = self.listener.accept().await?;
        }
    }

    /// [`Self::open_without_handshake()`] with a custom MSS.
    pub fn open_without_handshake_with_mss(
        &self,
        dispatch_key: K,
        fec: bool,
        mss: usize,
    ) -> Option<Connected> {
        let tuning = fec_tuning_from_env();
        self.open_without_handshake_with_mss_and_fec_tuning(dispatch_key, fec, mss, tuning)
    }

    /// [`Self::open_without_handshake()`] with a custom MSS and a
    /// per-connection [`FecTuning`].  Both peers must agree on the MSS and
    /// the FEC flag; the FEC tuning is likewise out-of-band.
    pub fn open_without_handshake_with_mss_and_fec_tuning(
        &self,
        dispatch_key: K,
        fec: bool,
        mss: usize,
        tuning: FecTuning,
    ) -> Option<Connected> {
        let accepted = self.listener.open(dispatch_key.clone())?;
        let (read, write) = accepted.split();
        let write = KeyedConnWrite::new(write, &dispatch_key);
        let unreliable_layer = wrap_fec_with_mss_and_fec_tuning(read, write, fec, mss, tuning);
        let unreliable_layer = apply_keyed_mss::<K>(unreliable_layer);
        let (read, write) = socket(unreliable_layer, None);
        Some(Connected { read, write })
    }

    pub fn open_without_handshake(&self, dispatch_key: K, fec: bool) -> Option<Connected> {
        let accepted = self.listener.open(dispatch_key.clone())?;
        let (read, write) = accepted.split();
        let write = KeyedConnWrite::new(write, &dispatch_key);
        let unreliable_layer = wrap_fec(read, write, fec);
        let unreliable_layer = apply_keyed_mss::<K>(unreliable_layer);
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
pub struct KeyedConnWrite {
    write: ConnWrite<UdpSocket>,
    buf: tokio::sync::Mutex<Vec<u8>>,
    data_offset: usize,
}
impl KeyedConnWrite {
    pub fn new<K: DispatchKey>(write: ConnWrite<UdpSocket>, conn_key: &K) -> Self {
        let mut buf = vec![];
        let n = K::max_size();
        let fill = (0..n).map(|_| 0);
        buf.extend(fill);
        let n = conn_key.encode(&mut buf).unwrap();
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
        match self.write.try_send(&buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => self.write.send(&buf).await,
            Err(e) => Err(e),
        }
    }
}
#[async_trait]
impl UnreliableWrite for KeyedConnWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        Self::send(self, buf)
            .await
            .map_err(|e| crate::udp::normalize_send_err(e).kind())
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

fn dispatch<K: DispatchKey>(_addr: &SocketAddr, mut pkt: Packet) -> Option<(K, Packet)> {
    let (n, key) = K::decode(&pkt)?;
    pkt.advance(n);
    Some((key, pkt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Hash, PartialEq, Eq)]
    struct HugeKey;
    impl DispatchKey for HugeKey {
        fn encode(&self, _buf: &mut [u8]) -> Option<usize> {
            Some(0)
        }

        fn decode(_buf: &[u8]) -> Option<(usize, Self)> {
            Some((0, HugeKey))
        }

        fn max_size() -> usize {
            crate::udp::NO_FEC_MSS + 1
        }
    }

    #[tokio::test]
    #[should_panic(expected = "dispatch key")]
    async fn test_key_too_large() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client =
            Client::<HugeKey>::connect_without_handshake("0.0.0.0:0", server.local_addr().unwrap())
                .await
                .unwrap();
        let _ = client.open_without_handshake(HugeKey, false).unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_key() {
        let fec = true;
        let server = Server::<u8>::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        let key = 42;
        let msg_1 = b"hello";
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let server = Arc::new(server);
            let mut accepted = server.accept_without_handshake(fec).await.unwrap();
            assert_eq!(accepted.dispatch_key, key);
            tokio::spawn({
                let server = server.clone();
                async move {
                    loop {
                        let _ = server.accept_without_handshake(fec).await;
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
            let mut accepted = client.open_without_handshake(key, fec).unwrap();
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

    /// A keyed client cannot receive inbound packets on an opened connection
    /// unless its `dispatch()` loop is polled: the dispatcher matches inbound
    /// packets to `open`ed connections.
    #[tokio::test(flavor = "multi_thread")]
    async fn client_open_requires_dispatch_polling_for_inbound_packets() {
        use std::time::Duration;

        let fec = false;
        let server = Server::<u8>::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        let key = 7;
        let ping = b"ping";
        let pong = b"pong";

        let server_task = tokio::spawn(async move {
            let mut accepted = server.accept_without_handshake(fec).await.unwrap();
            assert_eq!(accepted.dispatch_key, key);
            let mut buf = [0u8; 16];
            let n = accepted.read.recv(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], ping);
            accepted.write.send(pong).await.unwrap();
        });

        let client = Arc::new(
            Client::<u8>::connect_without_handshake("0.0.0.0:0", addr)
                .await
                .unwrap(),
        );
        let mut opened = client.open_without_handshake(key, fec).unwrap();

        // Send the ping before any dispatch loop is spawned. The server will
        // reply, but that reply will sit undelivered in the client socket until
        // dispatch() routes it to the opened connection.
        opened.write.send(ping).await.unwrap();

        // With dispatch unpoll, inbound delivery to the opened connection does
        // not happen. The 50 ms timeout should elapse before any data arrives,
        // even though the server's pong is already in the socket buffer.
        let mut buf = [0u8; 16];
        let timeout_result = tokio::time::timeout(Duration::from_millis(50), opened.read.recv(&mut buf))
            .await;
        assert!(
            timeout_result.is_err(),
            "inbound keyed packets must not be delivered unless dispatch is polled, even when the reply is already in the socket buffer"
        );

        // Start the dispatch loop on a clone. The server reply should now arrive.
        let dispatch_client = Arc::clone(&client);
        let dispatch_task = tokio::spawn(async move {
            loop {
                dispatch_client.dispatch().await.unwrap();
            }
        });

        let n = tokio::time::timeout(Duration::from_secs(1), opened.read.recv(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], pong);

        // Cleanup: close the read side so the dispatch loop can exit.
        drop(opened);
        dispatch_task.abort();
        server_task.await.unwrap();
    }
}
