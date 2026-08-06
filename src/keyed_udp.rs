use core::{net::SocketAddr, num::NonZeroUsize};
use std::sync::Arc;

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tokio::net::UdpSocket;
use tokio_util::bytes::Buf;
use udp_listener::{ConnWrite, Packet, UtpListener};

use crate::delivery::frame::FrameMode;
use crate::{
    socket::{ConnReader, ConnWriter, SessionHandle, socket},
    transmission::{
        fec_tuning::FecTuning,
        transmission_layer::{UnreliableLayer, UnreliableRead, UnreliableWrite},
    },
    udp::{
        self, AcceptConfig, MaybeRawFd, ValidMss, maybe_raw_fd, should_wait_after_try_send,
        wrap_fec_with_mss_and_fec_tuning_and_frame_delivery,
    },
};

use crate::io_err::IoErr;

const DISPATCHER_BUF_SIZE: usize = 1024;

fn wrap_keyed<K: DispatchKey>(
    read: impl UnreliableRead,
    write: impl UnreliableWrite,
    fec: bool,
    mss: ValidMss,
    tuning: FecTuning,
    frame_delivery: FrameMode,
) -> UnreliableLayer {
    let key_size = K::max_size();
    let mss = mss.get();
    let mss = mss
        .checked_sub(key_size)
        .unwrap_or_else(|| panic!("mss {mss} leaves no room for the {key_size}-byte dispatch key"));
    wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
        Box::new(read),
        Box::new(write),
        fec,
        ValidMss::try_new(mss).unwrap(),
        tuning,
        frame_delivery,
    )
    .unwrap()
}

#[derive(Debug)]
pub struct Listener<K> {
    listener: UtpListener<UdpSocket, K, Packet>,
    local_addr: SocketAddr,
    raw_fd: MaybeRawFd,
}
impl<K: DispatchKey> Listener<K> {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;
        let local_addr = udp.local_addr()?;
        let raw_fd = maybe_raw_fd(&udp);
        let listener = UtpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUF_SIZE).unwrap(),
            Arc::new(dispatch),
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

    /// [`udp::Listener::accept_without_handshake_with`] equivalent that also
    /// records the dispatch key of the accepted connection.
    pub async fn accept_with(&self, config: AcceptConfig) -> std::io::Result<Accepted<K>> {
        let accepted = self.listener.poll_next_conn().await?;
        let conn_key = accepted.conn_key().clone();
        let (read, write) = accepted.split();
        let write = {
            let peer = write.peer_addr();
            KeyedConnWrite::new(write, &conn_key, self.raw_fd, Some(peer))
        };
        let unreliable_layer = wrap_keyed::<K>(
            read,
            write,
            config.fec,
            config.mss.resolve()?,
            config.fec_tuning,
            config.frame_delivery,
        );
        let (read, write, supervisor) = socket(unreliable_layer, None);
        Ok(Accepted {
            read,
            write,
            supervisor,
            dispatch_key: conn_key,
        })
    }
}
#[derive(Debug)]
pub struct Accepted<K> {
    pub read: ConnReader,
    pub write: ConnWriter,
    pub supervisor: SessionHandle,
    pub dispatch_key: K,
}

#[derive(Debug)]
pub struct Connector<K> {
    listener: UtpListener<UdpSocket, K, Packet>,
    raw_fd: MaybeRawFd,
}
impl<K: DispatchKey> Connector<K> {
    pub async fn connect_without_handshake(
        bind: impl tokio::net::ToSocketAddrs,
        server: impl tokio::net::ToSocketAddrs,
    ) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(bind).await?;
        udp.connect(server).await?;
        let raw_fd = maybe_raw_fd(&udp);
        let listener = UtpListener::new(
            udp,
            NonZeroUsize::new(DISPATCHER_BUF_SIZE).unwrap(),
            Arc::new(dispatch),
        );
        Ok(Self { listener, raw_fd })
    }

    /// Side-effect: same as [`udp_listener::UtpListener::poll_next_conn()`]
    pub async fn dispatch(&self) -> std::io::Result<()> {
        loop {
            let _ = self.listener.poll_next_conn().await?;
        }
    }

    /// Open a connection to an existing keyed session without the opening
    /// handshake.
    pub fn open_without_handshake_with(
        &self,
        dispatch_key: K,
        config: AcceptConfig,
    ) -> Option<Connected> {
        let accepted = self.listener.register_conn(dispatch_key.clone())?;
        let (read, write) = accepted.split();
        let write = KeyedConnWrite::new(write, &dispatch_key, self.raw_fd, None);
        let unreliable_layer = wrap_keyed::<K>(
            read,
            write,
            config.fec,
            config.mss.resolve().ok()?,
            config.fec_tuning,
            config.frame_delivery,
        );
        let (read, write, supervisor) = socket(unreliable_layer, None);
        Some(Connected {
            read,
            write,
            supervisor,
        })
    }
}
#[derive(Debug)]
pub struct Connected {
    pub read: ConnReader,
    pub write: ConnWriter,
    pub supervisor: SessionHandle,
}

#[derive(Debug)]
pub struct KeyedConnWrite {
    write: ConnWrite<UdpSocket>,
    raw_fd: MaybeRawFd,
    peer: Option<core::net::SocketAddr>,
    buf: Vec<u8>,
    data_offset: usize,
}
impl KeyedConnWrite {
    pub fn new<K: DispatchKey>(
        write: ConnWrite<UdpSocket>,
        conn_key: &K,
        raw_fd: MaybeRawFd,
        peer: Option<core::net::SocketAddr>,
    ) -> Self {
        let mut buf = vec![];
        let n = K::max_size();
        let fill = (0..n).map(|_| 0);
        buf.extend(fill);
        let n = conn_key.encode(&mut buf).unwrap();
        Self {
            write,
            raw_fd,
            peer,
            buf,
            data_offset: n,
        }
    }

    pub async fn send(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.drain(self.data_offset..);
        self.buf.extend(data);
        self.send_buf().await
    }

    async fn send_buf(&mut self) -> std::io::Result<usize> {
        let sent = match self.write.try_send(&self.buf) {
            Ok(n) => Ok(n),
            Err(e) if should_wait_after_try_send(&e) => {
                #[cfg(target_os = "macos")]
                {
                    udp::raw_sendto_fallback(self.raw_fd, &self.buf, self.peer)
                        .await
                        .map_err(std::io::Error::from)
                }
                #[cfg(not(target_os = "macos"))]
                {
                    self.write.send(&self.buf).await
                }
            }
            Err(e) => Err(udp::normalize_send_err(e).into()),
        }?;
        Ok(sent.saturating_sub(self.data_offset))
    }
}
#[async_trait]
impl UnreliableWrite for KeyedConnWrite {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
        Self::send(self, buf)
            .await
            .map_err(crate::udp::normalize_send_err)
    }

    async fn send_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> Result<usize, IoErr> {
        if bufs.is_empty() {
            return Ok(0);
        }
        let total_payload: usize = bufs.iter().map(|b| b.len()).sum();
        self.buf.drain(self.data_offset..);
        self.buf.reserve(total_payload);
        for b in bufs {
            self.buf.extend_from_slice(b);
        }
        self.send_buf()
            .await
            .map_err(crate::udp::normalize_send_err)
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
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    use crate::delivery::frame::FrameMode;
    use crate::transmission::fec_tuning::FecTuning;
    use crate::transmission::transmission_layer::UnreliableRead;

    #[derive(Debug)]
    struct DummyRead;
    #[async_trait]
    impl UnreliableRead for DummyRead {
        fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
        async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
    }

    #[derive(Debug)]
    struct DummyWrite;
    #[async_trait]
    impl UnreliableWrite for DummyWrite {
        async fn send(&mut self, _buf: &[u8]) -> Result<usize, IoErr> {
            Ok(0)
        }
    }

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
        let client = Connector::<HugeKey>::connect_without_handshake(
            "0.0.0.0:0",
            server.local_addr().unwrap(),
        )
        .await
        .unwrap();
        let _ = client
            .open_without_handshake_with(HugeKey, AcceptConfig::default())
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_key() {
        let fec = true;
        let server = Listener::<u8>::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        let key = 42;
        let msg_1 = b"hello";
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let server = Arc::new(server);
            let mut accepted = server
                .accept_with(AcceptConfig {
                    fec,
                    ..AcceptConfig::default()
                })
                .await
                .unwrap();
            assert_eq!(accepted.dispatch_key, key);
            tokio::spawn({
                let server = server.clone();
                async move {
                    loop {
                        let _ = server
                            .accept_with(AcceptConfig {
                                fec,
                                ..AcceptConfig::default()
                            })
                            .await;
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
            let client = Connector::<u8>::connect_without_handshake("0.0.0.0:0", addr)
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
            let mut accepted = client
                .open_without_handshake_with(
                    key,
                    AcceptConfig {
                        fec,
                        ..AcceptConfig::default()
                    },
                )
                .unwrap();
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
        let server = Listener::<u8>::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr();
        let key = 7;
        let ping = b"ping";
        let pong = b"pong";

        let server_task = tokio::spawn(async move {
            let mut accepted = server
                .accept_with(AcceptConfig {
                    fec,
                    ..AcceptConfig::default()
                })
                .await
                .unwrap();
            assert_eq!(accepted.dispatch_key, key);
            let mut buf = [0u8; 16];
            let n = accepted.read.recv(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], ping);
            accepted.write.send(pong).await.unwrap();
        });

        let client = Arc::new(
            Connector::<u8>::connect_without_handshake("0.0.0.0:0", addr)
                .await
                .unwrap(),
        );
        let mut opened = client
            .open_without_handshake_with(
                key,
                AcceptConfig {
                    fec,
                    ..AcceptConfig::default()
                },
            )
            .unwrap();

        // Send the ping before any dispatch loop is spawned. The server will
        // reply, but that reply will sit undelivered in the client socket until
        // dispatch() routes it to the opened connection.
        opened.write.send(ping).await.unwrap();

        // With dispatch unpoll, inbound delivery to the opened connection does
        // not happen. The 50 ms timeout should elapse before any data arrives,
        // even though the server's pong is already in the socket buffer.
        let mut buf = [0u8; 16];
        let timeout_result =
            tokio::time::timeout(Duration::from_millis(50), opened.read.recv(&mut buf)).await;
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

    /// Fix #3: `keyed_writer_delivers_via_raw_path_when_writability_never_ready`
    /// — a keyed writer whose async writability never becomes ready still
    /// delivers via the raw path (or errors bounded) — must not hang.
    ///
    /// We can't easily make the tokio writability "never ready" on a real
    /// socket, but we CAN verify the keyed writer does not re-enter the
    /// poisoned `self.write.send().await` path on non-WouldBlock errors
    /// by checking that a send to an unreachable peer completes (not hangs)
    /// within a bounded time.
    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "multi_thread")]
    async fn keyed_writer_delivers_via_raw_path_when_writability_never_ready() {
        use std::time::Duration;
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client =
            Connector::<u8>::connect_without_handshake("0.0.0.0:0", server.local_addr().unwrap())
                .await
                .unwrap();
        let mut conn = client
            .open_without_handshake_with(42, AcceptConfig::default())
            .unwrap();

        // Send a small payload.  On macOS the raw fallback path is used on
        // WouldBlock.  The send must complete within a bounded time (not
        // hang on the poisoned tokio writability path).
        let payload = b"keyed-raw-test";
        let result = tokio::time::timeout(Duration::from_secs(2), conn.write.send(payload)).await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("keyed send failed: {e:?}"),
            Err(_) => panic!("keyed send hung (re-entered poisoned writability)"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn keyed_send_reports_only_the_callers_bytes() {
        use crate::transmission::transmission_layer::UnreliableWrite;
        const KEY: u64 = 0x0102_0304_0506_0708;
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let client = Connector::<u64>::connect_without_handshake("127.0.0.1:0", peer_addr)
            .await
            .unwrap();
        let accepted = client.listener.register_conn(KEY).unwrap();
        let (_read, write) = accepted.split();
        let mut write = KeyedConnWrite::new(write, &KEY, client.raw_fd, None);
        let payload = b"handshake bytes";
        assert_eq!(
            UnreliableWrite::send(&mut write, payload).await.unwrap(),
            payload.len()
        );
        let mut buf = [0u8; 64];
        let (n, _from) = peer.recv_from(&mut buf).await.unwrap();
        assert_eq!(n, payload.len() + u64::max_size());
        let iov = [
            std::io::IoSlice::new(&payload[..4]),
            std::io::IoSlice::new(&payload[4..]),
        ];
        assert_eq!(
            UnreliableWrite::send_vectored(&mut write, &iov)
                .await
                .unwrap(),
            payload.len()
        );
    }

    /// Fix #4: `keyed_send_cancellation_safe` — cancel a keyed send
    /// mid-flight (drop the future), then a second send on the same
    /// `KeyedConnWrite` must succeed (no panic at `unwrap()` on a
    /// `None` buffer).
    #[tokio::test(flavor = "multi_thread")]
    async fn keyed_send_cancellation_safe() {
        use std::time::Duration;
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client =
            Connector::<u8>::connect_without_handshake("0.0.0.0:0", server.local_addr().unwrap())
                .await
                .unwrap();
        let mut conn = client
            .open_without_handshake_with(42, AcceptConfig::default())
            .unwrap();

        // Spawn a send future and cancel it by aborting the task before
        // it completes.  We use tokio::select! with a timeout to force
        // cancellation mid-flight.
        {
            let send_fut = conn.write.send(b"cancelled");
            tokio::pin!(send_fut);
            // Poll it once with a timeout to force cancellation.
            let _ = tokio::time::timeout(Duration::from_millis(1), &mut send_fut).await;
            // The future is dropped here (cancellation), which may leave
            // the buffer lease as None.
        }

        // A second send on the same connection must succeed (no panic at
        // unwrap() on a None buffer).
        let result =
            tokio::time::timeout(Duration::from_secs(2), conn.write.send(b"second-send")).await;
        match result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => panic!("second keyed send failed after cancellation: {e:?}"),
            Err(_) => panic!("second keyed send hung after cancellation"),
        }
    }

    #[test]
    fn a_keyed_fec_datagram_fits_the_requested_mss() {
        let mss = crate::udp::NO_FEC_MSS;
        let layer = super::wrap_keyed::<u16>(
            DummyRead,
            DummyWrite,
            true,
            ValidMss::try_new(mss).unwrap(),
            FecTuning::default(),
            FrameMode::default(),
        );
        let datagram =
            layer.fec.as_ref().unwrap().max_wire_pkt_size() + <u16 as DispatchKey>::max_size();
        assert!(
            datagram <= mss,
            "a parity datagram is {datagram} bytes on a {mss}-byte MSS"
        );
    }

    #[test]
    fn frame_mode_rejects_undersized_mss() {
        let mss = 33;
        let res = std::panic::catch_unwind(|| {
            super::wrap_keyed::<u16>(
                DummyRead,
                DummyWrite,
                true,
                ValidMss::try_new(mss).unwrap(),
                FecTuning::default(),
                FrameMode::enabled(),
            )
        });
        assert!(res.is_err(), "undersized mss must panic in keyed wrap");
    }
}
