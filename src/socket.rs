use core::time::Duration;
use std::{sync::Arc, time::Instant};

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
};
use rand::TryRng;
use tokio::task::JoinSet;

use crate::{
    codec::in_cmd_space,
    transmission::transmission_layer::{
        LogConfig, RecvBufs, SendBufs, SendKillPkt, TransmissionLayer, UnreliableLayer,
    },
};

const TIMER_INTERVAL: Duration = Duration::from_millis(1);

pub type ReadStream = PollRead<ReadSocket>;

/// Async write stream that caps each staged slice at the reliable layer's
/// send-buffer capacity, removing the O(N^2) re-copy in the underlying
/// `PollWrite` (which otherwise buffers the entire caller slice every time it
/// starts a new write future).
///
/// `AsyncWrite` permits partial writes; the returned `n` is exactly what the
/// inner `WriteSocket` consumed. Truncating the buffer before delegating is
/// safe because `PollWrite` ignores the incoming buffer while a write future is
/// in flight, so we can never split an in-flight write.
#[derive(Debug)]
pub struct WriteStream {
    inner: PollWrite<WriteSocket>,
    max_stage: usize,
}

impl WriteStream {
    pub fn into_inner(self) -> WriteSocket {
        self.inner.into_inner()
    }

    pub fn inner(&self) -> &WriteSocket {
        self.inner.inner()
    }

    pub fn inner_mut(&mut self) -> &mut WriteSocket {
        self.inner.inner_mut()
    }
}

impl std::convert::AsRef<WriteSocket> for WriteStream {
    fn as_ref(&self) -> &WriteSocket {
        self.inner()
    }
}

impl std::convert::AsMut<WriteSocket> for WriteStream {
    fn as_mut(&mut self) -> &mut WriteSocket {
        self.inner_mut()
    }
}

impl tokio::io::AsyncWrite for WriteStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let max_stage = self.max_stage;
        let buf = if buf.len() > max_stage {
            &buf[..max_stage]
        } else {
            buf
        };
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl std::marker::Unpin for WriteStream {}

#[cfg(test)]
impl WriteStream {
    pub fn max_stage(&self) -> usize {
        self.max_stage
    }
}

/// Bidirectional async stream joining a read half and a [`WriteStream`].
#[derive(Debug)]
pub struct IoStream {
    read: ReadStream,
    write: WriteStream,
}

impl IoStream {
    pub fn into_split(self) -> (ReadSocket, WriteSocket) {
        (self.read.into_inner(), self.write.into_inner())
    }

    pub fn split(&self) -> (&ReadSocket, &WriteSocket) {
        (self.read.inner(), self.write.inner())
    }

    pub fn split_mut(&mut self) -> (&mut ReadSocket, &mut WriteSocket) {
        (self.read.inner_mut(), self.write.inner_mut())
    }
}

impl tokio::io::AsyncRead for IoStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for IoStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<WriteStream>;
    let _ = assert_send::<ReadStream>;
    let _ = assert_send::<IoStream>;
};

pub fn socket(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (ReadSocket, WriteSocket) {
    let read_shutdown = tokio_util::sync::CancellationToken::new();
    let write_shutdown = tokio_util::sync::CancellationToken::new();
    let transmission_layer = Arc::new(TransmissionLayer::new(unreliable_layer, log_config));
    let mut events = JoinSet::new();

    // Send timer
    events.spawn({
        let transmission_layer = Arc::clone(&transmission_layer);
        async move {
            let mut send_bufs = SendBufs::new();
            loop {
                // Register the resume_send notifier *before* reading the next
                // poll time so that a notification arriving between the last
                // send_pkts pass and this registration is stored as a permit
                // and wakes us immediately.
                let resume_send = transmission_layer.resume_send().notified();
                let now = Instant::now();
                let next_poll_time = transmission_layer.next_poll_send_time();
                let fast_poll_time = now + TIMER_INTERVAL;
                // When the rate limiter already has a token available (its
                // `next_token_time` is now or in the past), honoring it would
                // make `sleep_until` return immediately. If there is nothing
                // to send at that moment (idle / cwnd full / no data), the loop
                // would busy-spin on `Instant::now` + `send_pkts`. Fall back to
                // the 1ms poll in that case so we only wake immediately when the
                // limiter is actively pacing us.
                let poll_time = if next_poll_time <= now {
                    fast_poll_time
                } else {
                    next_poll_time.min(fast_poll_time)
                };
                tokio::select! {
                    () = tokio::time::sleep_until(poll_time.into()) => (),
                    () = resume_send => (),
                }
                if transmission_layer.send_pkts(&mut send_bufs).await.is_err() {
                    return;
                }
            }
        }
    });

    // Recv
    events.spawn({
        let read_shutdown = read_shutdown.clone();
        let transmission_layer = Arc::clone(&transmission_layer);
        async move {
            let mut recv_bufs = RecvBufs::new();
            let mut send_bufs = SendBufs::new();
            loop {
                // Read shutdown status must be checked before moving pkts to the read buf
                //   to prevent triggering sending kill pkt when the read shuts down right after the pkt moving.
                let is_read_shutdown = read_shutdown.is_cancelled();

                let recv_pkts = match transmission_layer
                    .recv_pkts(&mut recv_bufs, &mut send_bufs)
                    .await
                {
                    Ok(recv_pkts) => recv_pkts,
                    Err((_e, should_send_kill_pkt)) => {
                        match should_send_kill_pkt {
                            SendKillPkt::Yes => {
                                let _ = transmission_layer.send_kill_pkt(&mut send_bufs).await;
                            }
                            SendKillPkt::No => (),
                        }
                        return;
                    }
                };
                if is_read_shutdown && 0 < recv_pkts.num_payload_segments {
                    let _ = transmission_layer.send_kill_pkt(&mut send_bufs).await;
                }
            }
        }
    });

    tokio::spawn({
        let read_shutdown = read_shutdown.clone();
        let write_shutdown = write_shutdown.clone();
        let transmission_layer = Arc::clone(&transmission_layer);
        async move {
            let _event_guard = events;

            tokio::select! {
                () = write_shutdown.cancelled() => {
                    transmission_layer.send_fin_buf();
                }
                () = transmission_layer.some_error().cancelled() => (),
            }
            tokio::select! {
                () = read_shutdown.cancelled() => (),
                () = transmission_layer.some_error().cancelled() => (),
            }

            tokio::select! {
                () = transmission_layer.recv_fin().cancelled() => (),
                () = transmission_layer.some_error().cancelled() => (),
                () = tokio::time::sleep(Duration::from_secs(675)) => (),
            }
        }
    });

    let read = ReadSocket {
        transmission_layer: Arc::clone(&transmission_layer),
        _shutdown_guard: read_shutdown.drop_guard(),
    };
    let write = WriteSocket {
        transmission_layer: Arc::clone(&transmission_layer),
        send_bufs: SendBufs::new(),
        no_delay: true,
        _shutdown_guard: write_shutdown.drop_guard(),
    };
    (read, write)
}

#[derive(Debug)]
pub struct ReadSocket {
    transmission_layer: Arc<TransmissionLayer>,
    _shutdown_guard: tokio_util::sync::DropGuard,
}
impl ReadSocket {
    /// Return the number of bytes sent.
    ///
    /// You probably want to receive a fix-sized message like this:
    ///
    /// ```rust
    /// use tokio::io::AsyncReadExt;
    ///
    /// async fn f(read_socket: rtp::socket::ReadSocket, message: &mut [u8]) -> std::io::Result<()> {
    ///     let mut read_stream = read_socket.into_async_read();
    ///     read_stream.read_exact(message).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.transmission_layer.recv(data).await
    }

    /// Undo this method:
    ///
    /// ```rust
    /// fn f(read_stream: rtp::socket::ReadStream) -> rtp::socket::ReadSocket {
    ///     read_stream.into_inner()
    /// }
    /// ```
    pub fn into_async_read(self) -> ReadStream {
        PollRead::new(self)
    }

    /// Number of codec payloads recovered by FEC parity so far, or `None` when
    /// FEC is not enabled. Exposed for tests asserting parity recovery.
    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.transmission_layer.fec_recovered_symbols()
    }
}

#[derive(Debug)]
pub struct WriteSocket {
    transmission_layer: Arc<TransmissionLayer>,
    send_bufs: SendBufs,
    no_delay: bool,
    _shutdown_guard: tokio_util::sync::DropGuard,
}
impl WriteSocket {
    /// This method may only send partial `data`.
    ///
    /// Return the number of bytes sent.
    ///
    /// You probably want to send a message like this:
    ///
    /// ```rust
    /// use tokio::io::AsyncWriteExt;
    ///
    /// async fn f(write_socket: rtp::socket::WriteSocket, message: &[u8]) -> std::io::Result<()> {
    ///     let mut write_stream = write_socket.into_async_write();
    ///     write_stream.write_all(message).await
    /// }
    /// ```
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.transmission_layer
            .send(data, self.no_delay, &mut self.send_bufs)
            .await
    }

    pub fn is_send_buf_empty(&self) -> bool {
        self.transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .is_send_buf_empty()
    }

    pub async fn send_buf_empty(&self) -> Result<(), std::io::ErrorKind> {
        self.transmission_layer.send_buf_empty().await
    }

    /// Undo this method:
    ///
    /// ```rust
    /// fn f(write_stream: rtp::socket::WriteStream) -> rtp::socket::WriteSocket {
    ///     write_stream.into_inner()
    /// }
    /// ```
    pub fn into_async_write(self) -> WriteStream {
        let max_stage = self
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .send_data_buf_capacity();
        WriteStream {
            inner: PollWrite::new(self),
            max_stage,
        }
    }
}

pub fn unsplit(read: ReadStream, write: WriteStream) -> IoStream {
    IoStream { read, write }
}

impl AsyncAsyncRead for ReadSocket {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf).await.map_err(std::io::Error::from)
    }
}
impl AsyncAsyncWrite for WriteSocket {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf).await.map_err(std::io::Error::from)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.transmission_layer.throw_error()?;
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.transmission_layer.send_fin_buf();
        self.transmission_layer.no_data_to_send().await?;
        Ok(())
    }
}

const NUM_CONNECT_RETRIES: usize = 2;
const INIT_CONNECT_RTO: Duration = Duration::from_secs(1);

pub async fn client_opening_handshake(
    unreliable: &mut UnreliableLayer,
) -> Result<(), std::io::Error> {
    let mut challenge: [u8; 1] = [0; 1];
    let mut rng = rand::rngs::SysRng;
    loop {
        rng.try_fill_bytes(&mut challenge).unwrap();
        if !in_cmd_space(challenge[0]) {
            break;
        }
    }
    for i in 0..=NUM_CONNECT_RETRIES {
        if i == NUM_CONNECT_RETRIES {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
        }
        let _ = unreliable.utp_write.send(&challenge).await?;
        let mut resp = [0; 1];
        tokio::select! {
            res = unreliable.utp_read.recv(&mut resp) => {
                res?;
            }
            () = tokio::time::sleep(INIT_CONNECT_RTO.mul_f64((i + 1) as f64)) => continue,
        }
        if challenge == resp {
            break;
        }
    }

    neg_challenge(&mut challenge);
    let _ = unreliable.utp_write.send(&challenge).await?;
    Ok(())
}

pub async fn server_opening_handshake(
    unreliable: &mut UnreliableLayer,
) -> Result<(), std::io::Error> {
    let mut challenge = [0; 1];
    let n = unreliable.utp_read.try_recv(&mut challenge)?;
    if n != challenge.len() {
        return Err(std::io::ErrorKind::InvalidInput.into());
    }
    unreliable.utp_write.send(&challenge).await?;

    neg_challenge(&mut challenge);
    let mut resp = [0; 1];
    let due = Instant::now() + INIT_CONNECT_RTO;
    loop {
        tokio::select! {
            res = unreliable.utp_read.recv(&mut resp) => {
                res?;
            }
            () = tokio::time::sleep_until(due.into()) => {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
            }
        }
        if challenge == resp {
            break;
        }
    }
    Ok(())
}

fn neg_challenge(challenge: &mut [u8]) {
    for c in challenge.iter_mut() {
        *c ^= u8::MAX;
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UdpSocket,
    };

    use crate::udp::wrap_fec;

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_io() {
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let hello = b"hello";
        let world = b"world";

        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
        let (a_r, mut a_w) = socket(a, None);
        let (b_r, mut b_w) = socket(b, None);
        a_w.send(hello).await.unwrap();
        b_w.send(world).await.unwrap();

        let mut recv_buf = [0; 1024 * 64];
        a_r.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..world.len()], world);
        b_r.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..hello.len()], hello);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_async_io() {
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
        let (a_r, a_w) = socket(a, None);
        let (b_r, b_w) = socket(b, None);

        let mut send_buf = vec![0; 2 << 17];
        let mut recv_buf = send_buf.clone();

        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b = unsplit(b_r.into_async_read(), b_w.into_async_write());
        let mut transmission = JoinSet::new();
        let recv_all = Arc::new(tokio::sync::Notify::new());
        transmission.spawn({
            let send_buf = send_buf.clone();
            let recv_all = recv_all.clone();
            async move {
                let recv_all = recv_all.notified();
                a.write_all(&send_buf).await.unwrap();
                println!("{a:?}");
                recv_all.await;
            }
        });
        transmission.spawn(async move {
            b.read_exact(&mut recv_buf).await.unwrap();
            assert_eq!(send_buf, recv_buf);
            recv_all.notify_waiters();
        });
        while let Some(res) = transmission.join_next().await {
            res.unwrap();
        }
    }

    /// FEC must actually recover lost data symbols via parity under packet
    /// loss. This transfers a sizable buffer over a lossy link with FEC
    /// enabled and asserts `recovered_symbols > 0` on the receiver. Loss is
    /// injected via per-instance `LossRate` + `testing::wrap_fec_lossy` — the
    /// production `UnreliableRead`/`UnreliableWrite` impls are untouched and
    /// there is no global state.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss() {
        use crate::udp::testing::{LossRate, wrap_fec_lossy};

        // 3% loss is high enough to drop ~85 of ~2800 data packets (giving FEC
        // plenty to recover) but low enough that the reliable layer's
        // broken-pipe heuristic does not trip and abort the transfer under
        // concurrent test load. Each side gets its own local `LossRate`.
        let rate_a = LossRate::new(300); // 3% loss on a's link
        let rate_b = LossRate::new(300); // 3% loss on b's link

        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec_lossy(a.clone(), a, fec, rate_a);
        let b = wrap_fec_lossy(b.clone(), b, fec, rate_b);
        let (a_r, a_w) = socket(a, None);
        let (b_r, b_w) = socket(b, None);

        // 4 MiB: large enough that ~2800 packets cross the link, so 3% loss
        // produces ~85 dropped packets — plenty for FEC parity to recover
        // some of them reliably, even when some parity packets are themselves
        // lost.
        let mut send_buf = vec![0; 4 << 20];
        let mut recv_buf = send_buf.clone();
        for byte in &mut send_buf {
            *byte = rand::random();
        }

        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        // Keep `b_r` as a PollRead (not unsplit) so we can still query its
        // `ReadSocket` for FEC stats after the transfer. `PollRead` implements
        // `tokio::io::AsyncRead`, so `read_exact` works directly.
        let mut b_r = b_r.into_async_read();
        let b_w = b_w.into_async_write();

        let send_buf_clone = send_buf.clone();
        let recv_done = Arc::new(tokio::sync::Notify::new());
        let recv_done_clone = recv_done.clone();
        let sender = tokio::spawn(async move {
            // Keep the write side alive so the connection does not shut down
            // before the receiver finishes reading.
            let _b_w = b_w;
            a.write_all(&send_buf_clone).await.unwrap();
            recv_done_clone.notified().await;
            a
        });

        b_r.read_exact(&mut recv_buf).await.unwrap();
        assert_eq!(send_buf, recv_buf);
        recv_done.notify_waiters();
        sender.await.unwrap();

        // The receiver decoded incoming packets (data + parity) and FEC should
        // have reconstructed some lost data symbols from parity.
        let recovered = b_r.inner().fec_recovered_symbols();
        assert!(recovered.is_some(), "FEC should be enabled on the receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols under 3% loss, got 0"
        );
    }

    /// The async write stream must stage at most the reliable layer's send
    /// data buffer capacity per `poll_write`. Writing a slice much larger than
    /// that should consume at most that many bytes in a single poll.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_stream_max_stage_scales_with_mss() {
        use crate::udp::{wrap_fec, wrap_fec_with_mss};

        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        // Default-MSS case: the staging cap is exactly the fixed 8 KiB buffer.
        let b_wrapped = wrap_fec(b.clone(), b, false);
        let (_b_r, b_w) = socket(b_wrapped, None);
        assert_eq!(
            b_w.into_async_write().max_stage(),
            8 * 1024,
            "default-MSS staging buffer must be exactly 8 KiB"
        );

        let mss = 9_000;
        let a = wrap_fec_with_mss(a.clone(), a, false, mss);
        let (_a_r, a_w) = socket(a, None);
        let write_stream = a_w.into_async_write();
        let max_stage = write_stream.max_stage();

        let per_packet_payload = mss - crate::codec::data_overhead();
        assert_eq!(
            max_stage % per_packet_payload,
            0,
            "max_stage {max_stage} should be a multiple of per-packet payload {per_packet_payload}"
        );
        assert!(
            8 * 1024 < max_stage,
            "max_stage {max_stage} should exceed the default staging buffer of 8 KiB"
        );

        // Hold the write stream until after the assertion so the destructor is
        // not mistaken for the value under test.
        drop(write_stream);
    }

    /// The async write stream must stage at most the reliable layer's send
    /// data buffer capacity per `poll_write`. Writing a slice much larger than
    /// that should consume at most that many bytes in a single poll.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_stream_stages_at_most_the_send_buf_capacity() {
        use std::pin::Pin;
        use std::task::{Context, Poll};

        use tokio::io::AsyncWrite;

        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
        let (a_r, a_w) = socket(a, None);
        let (_b_r, b_w) = socket(b, None);

        // Keep the receiving read socket alive so the connection does not shut
        // down before the assertion runs.
        let _a_r = a_r;

        let capacity = a_w
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .send_data_buf_capacity();

        let mut write_stream = a_w.into_async_write();
        let big = vec![0u8; capacity * 4];

        // Poll exactly once. The first call may complete immediately if the
        // buffer has free space (it does on a fresh connection). Either way,
        // the number of bytes staged/consumed must not exceed capacity.
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let pinned = Pin::new(&mut write_stream);
        let poll = pinned.poll_write(&mut cx, &big);
        let n = match poll {
            Poll::Ready(Ok(n)) => n,
            Poll::Pending => 0,
            Poll::Ready(Err(e)) => panic!("poll_write failed: {e:?}"),
        };
        assert!(
            n <= capacity,
            "poll_write consumed {n} bytes, but capacity is {capacity}"
        );

        // Hold the write stream until after the assertion so the destructor is
        // not mistaken for the value under test.
        drop(write_stream);
        drop(b_w);
    }
}
