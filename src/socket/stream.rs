use std::sync::Arc;

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
};

use super::session::SessionHandle;

use crate::io_err::IoErr;
use crate::metrics::MetricsTerminationCause;
use crate::transmission::connection::Connection;

pub type AsyncReadAdapter = PollRead<ConnReader>;

#[derive(Debug)]
pub struct AsyncWriteAdapter {
    inner: PollWrite<ConnWriter>,
    max_write_bytes: usize,
    abort_session: Arc<Connection>,
}

impl AsyncWriteAdapter {
    pub fn into_inner(self) -> ConnWriter {
        self.inner.into_inner()
    }

    pub fn inner(&self) -> &ConnWriter {
        self.inner.inner()
    }

    pub fn inner_mut(&mut self) -> &mut ConnWriter {
        self.inner.inner_mut()
    }

    pub async fn send_kill_and_abort(&mut self) {
        self.abort_session
            .request_kill_and_abort(MetricsTerminationCause::LocalAbort);
    }
}

impl std::convert::AsRef<ConnWriter> for AsyncWriteAdapter {
    fn as_ref(&self) -> &ConnWriter {
        self.inner()
    }
}

impl std::convert::AsMut<ConnWriter> for AsyncWriteAdapter {
    fn as_mut(&mut self) -> &mut ConnWriter {
        self.inner_mut()
    }
}

impl tokio::io::AsyncWrite for AsyncWriteAdapter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let max_write_bytes = self.max_write_bytes;
        if buf.len() > max_write_bytes {
            if self.abort_session.frame_delivery_enabled() {
                // In frame-delivery mode a write is exactly one wire frame;
                // truncating would silently split one logical frame into
                // several, corrupting message boundaries on the receiver.
                // Mirror `validate_frame`, which rejects any frame above
                // `MAX_FRAME_LEN` with `InvalidInput`.
                return std::task::Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "frame-delivery write of {} bytes exceeds the maximum frame size of {max_write_bytes} bytes",
                        buf.len()
                    ),
                )));
            }
            // Stock byte-stream mode: a partial write is correct; the
            // caller's write_all loop consumes the remainder on later polls.
            let buf = &buf[..max_write_bytes];
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        } else {
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
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

impl std::marker::Unpin for AsyncWriteAdapter {}

#[cfg(test)]
impl AsyncWriteAdapter {
    pub fn max_write_bytes(&self) -> usize {
        self.max_write_bytes
    }
}

#[derive(Debug)]
pub struct IoStream {
    read: AsyncReadAdapter,
    write: AsyncWriteAdapter,
}

impl IoStream {
    pub fn into_split(self) -> (ConnReader, ConnWriter) {
        (self.read.into_inner(), self.write.into_inner())
    }

    pub fn split(&self) -> (&ConnReader, &ConnWriter) {
        (self.read.inner(), self.write.inner())
    }

    pub fn split_mut(&mut self) -> (&mut ConnReader, &mut ConnWriter) {
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

#[derive(Debug)]
pub struct FrameByteReader {
    inner: AsyncReadAdapter,
}
impl tokio::io::AsyncRead for FrameByteReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[derive(Debug)]
pub struct FrameByteWriter {
    inner: AsyncWriteAdapter,
}
impl FrameByteWriter {
    pub async fn send_kill_and_abort(&mut self) {
        self.inner.send_kill_and_abort().await;
    }
}
impl tokio::io::AsyncWrite for FrameByteWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub(crate) struct FrameIoParts {
    read: FrameByteReader,
    write: FrameByteWriter,
}
impl FrameIoParts {
    pub(crate) fn into_parts(self) -> (FrameByteReader, FrameByteWriter) {
        (self.read, self.write)
    }
}

const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<AsyncWriteAdapter>;
    let _ = assert_send::<AsyncReadAdapter>;
    let _ = assert_send::<IoStream>;
    let _ = assert_send::<FrameByteReader>;
    let _ = assert_send::<FrameByteWriter>;
    let _ = assert_send::<FrameIoParts>;
    let _ = assert_send::<SessionHandle>;
};

pub fn unsplit(read: AsyncReadAdapter, write: AsyncWriteAdapter) -> IoStream {
    IoStream { read, write }
}

#[derive(Debug)]
pub struct ConnReader {
    transmission_layer: Arc<Connection>,
    frame_buf: Vec<u8>,
    _shutdown_guard: tokio_util::sync::DropGuard,
}

impl ConnReader {
    pub(super) fn new(
        transmission_layer: Arc<Connection>,
        shutdown_guard: tokio_util::sync::DropGuard,
    ) -> Self {
        Self {
            transmission_layer,
            frame_buf: Vec::new(),
            _shutdown_guard: shutdown_guard,
        }
    }

    #[cfg(test)]
    pub(super) fn recv_fin_for_test(&self) -> &tokio_util::sync::CancellationToken {
        self.transmission_layer.recv_fin()
    }

    #[cfg(test)]
    pub(super) fn recv_eof_for_test(&self) -> &tokio_util::sync::CancellationToken {
        self.transmission_layer.recv_eof()
    }

    pub async fn recv(&mut self, data: &mut [u8]) -> Result<usize, IoErr> {
        if data.is_empty() {
            return Ok(0);
        }
        if !self.frame_buf.is_empty() {
            let n = self.frame_buf.len().min(data.len());
            data[..n].copy_from_slice(&self.frame_buf[..n]);
            self.frame_buf.drain(..n);
            return Ok(n);
        }
        if self.transmission_layer.frame_delivery_enabled() {
            match self.transmission_layer.recv_frame().await? {
                Some(frame) => {
                    let n = frame.len().min(data.len());
                    data[..n].copy_from_slice(&frame[..n]);
                    if n < frame.len() {
                        self.frame_buf.extend_from_slice(&frame[n..]);
                    }
                    Ok(n)
                }
                None => Ok(0),
            }
        } else {
            self.transmission_layer.recv(data).await
        }
    }

    pub async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>, std::io::Error> {
        if !self.frame_buf.is_empty() {
            // A previous frame was partially consumed byte-wise through
            // `recv`; silently clearing it would discard the remainder and
            // corrupt message boundaries. Mixing byte reads and frame reads
            // on one stream is a caller error, so surface it instead of
            // losing bytes.
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "cannot read a frame: {} bytes of the previous frame are still buffered from a byte-wise read; mixing byte reads and frame reads on one stream is not allowed",
                    self.frame_buf.len()
                ),
            ));
        }
        self.transmission_layer
            .recv_frame()
            .await
            .map_err(|kind| self.transmission_layer.io_error(kind))
    }

    pub fn into_async_read(self) -> AsyncReadAdapter {
        PollRead::new(self)
    }

    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.transmission_layer.fec_recovered_symbols()
    }
}

/// The write half of an RTP connection.
///
/// [`send`](ConnWriter::send) stages bytes into the connection and returns; a
/// driver task owned by the session packetizes and transmits them
/// asynchronously. Dropping this half closes the write side, and dropping the
/// [`SessionHandle`](crate::SessionHandle) that owns the driver tasks aborts
/// them, so bytes that are still staged — or staged and not yet acknowledged —
/// are discarded instead of sent. Await
/// [`all_sent_data_acked`](ConnWriter::all_sent_data_acked) for a full drain,
/// or [`send_buf_empty`](ConnWriter::send_buf_empty) for a packetization-only
/// barrier, before the connection is dropped.
#[derive(Debug)]
pub struct ConnWriter {
    transmission_layer: Arc<Connection>,
    _shutdown_guard: tokio_util::sync::DropGuard,
}

impl ConnWriter {
    pub(super) fn new(
        transmission_layer: Arc<Connection>,
        shutdown_guard: tokio_util::sync::DropGuard,
    ) -> Self {
        Self {
            transmission_layer,
            _shutdown_guard: shutdown_guard,
        }
    }

    /// Stages bytes for the send driver and returns how many were staged.
    ///
    /// This is a **partial** write, matching the `AsyncWrite` contract: only
    /// what the send stage can accept is staged, so the return value can be
    /// less than `data.len()`. A non-empty `data` always stages at least one
    /// byte — the call waits for stage space rather than returning `0` — and
    /// only an empty `data` returns `0`. Stage repeatedly until every byte has
    /// been accepted; `write_all` on the adapter from
    /// [`into_async_write`](Self::into_async_write) does that.
    ///
    /// The call returns once the bytes are staged, **not** when they reach the
    /// wire: the send driver packetizes and transmits them asynchronously.
    /// Await [`send_buf_empty`](Self::send_buf_empty) to wait for
    /// packetization and [`all_sent_data_acked`](Self::all_sent_data_acked)
    /// for acknowledgement — and do so before dropping, since dropping
    /// discards whatever is still staged (see [`ConnWriter`]).
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, IoErr> {
        self.transmission_layer.send(data).await
    }

    /// Stages one frame for the send driver and returns its length.
    ///
    /// Unlike [`send`](Self::send) this stages the whole frame or fails. The
    /// staging-not-delivery contract and the drop contract are the same as
    /// [`send`](Self::send)'s.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<usize, IoErr> {
        self.transmission_layer.send_frame(frame).await
    }

    pub fn is_send_buf_empty(&self) -> bool {
        self.transmission_layer.is_send_buf_empty()
    }

    /// Waits until the send staging buffer is empty: no staged application
    /// bytes remain and no FIN is pending. This is a local staging
    /// guarantee, NOT "everything is on the wire or acknowledged" — the
    /// reliable layer may still hold the data in the send window or in
    /// flight. To wait for full outbound drain (all data packetized,
    /// acknowledged, and nothing left to send), use
    /// [`ConnWriter::all_sent_data_acked`], which waits on
    /// `Connection::no_data_to_send`.
    pub async fn send_buf_empty(&self) -> Result<(), IoErr> {
        self.transmission_layer.send_buf_empty().await
    }

    /// Waits until there is no data left to send: the staging buffer is
    /// empty, the send window holds no in-flight packets, and every sent
    /// packet has been acknowledged. This is the full-drain counterpart of
    /// [`ConnWriter::send_buf_empty`], and the barrier to await before
    /// dropping the connection — see [`ConnWriter`].
    pub async fn all_sent_data_acked(&self) -> Result<(), IoErr> {
        self.transmission_layer.no_data_to_send().await
    }

    pub async fn send_kill_and_abort(&mut self) {
        self.transmission_layer
            .request_kill_and_abort(MetricsTerminationCause::LocalAbort);
    }

    pub fn into_async_write(self) -> AsyncWriteAdapter {
        let max_write_bytes = self.transmission_layer.write_unit_capacity();
        let abort_session = Arc::clone(&self.transmission_layer);
        AsyncWriteAdapter {
            inner: PollWrite::new(self),
            max_write_bytes,
            abort_session,
        }
    }
}

pub(crate) fn into_frame_io_parts(
    read: ConnReader,
    write: ConnWriter,
) -> std::io::Result<FrameIoParts> {
    if !Arc::ptr_eq(&read.transmission_layer, &write.transmission_layer) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RTP read and write halves belong to different connections",
        ));
    }
    let read_enabled = read.transmission_layer.frame_delivery_enabled();
    let write_enabled = write.transmission_layer.frame_delivery_enabled();
    if !read_enabled || !write_enabled {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RTP connection is not configured for frame delivery",
        ));
    }
    Ok(FrameIoParts {
        read: FrameByteReader {
            inner: read.into_async_read(),
        },
        write: FrameByteWriter {
            inner: write.into_async_write(),
        },
    })
}

impl AsyncAsyncRead for ConnReader {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf)
            .await
            .map_err(|kind| self.transmission_layer.io_error(kind))
    }
}

impl AsyncAsyncWrite for ConnWriter {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf)
            .await
            .map_err(|kind| self.transmission_layer.io_error(kind))
    }

    /// Flush is an honest barrier: it returns only once the send staging
    /// buffer has drained into the reliable layer — the send driver has
    /// consumed every staged byte (and any pending FIN) out of the staging
    /// buffer and packetized it. It does NOT wait for the data to be
    /// acknowledged on the wire; for full drain use
    /// [`ConnWriter::all_sent_data_acked`].
    async fn flush(&mut self) -> std::io::Result<()> {
        self.transmission_layer
            .check_error()
            .map_err(|kind| self.transmission_layer.io_error(kind))?;
        // Wait for the staging buffer to drain. `send_buf_empty` arms the
        // sent-data notification before inspecting the buffer (the same
        // pattern the send loops in `transmission/connection.rs` use), so a
        // wake between the check and the await is never lost; the driver
        // publishes that signal whenever it consumes staged bytes.
        self.transmission_layer
            .send_buf_empty()
            .await
            .map_err(|kind| self.transmission_layer.io_error(kind))?;
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.transmission_layer.send_fin_buf();
        self.all_sent_data_acked().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UdpSocket,
        task::JoinSet,
    };

    use super::super::session::socket;
    use crate::transmission::connection::Connection;
    use crate::transmission::test_doubles::{PendingRead, PendingWrite};
    use crate::udp::wrap_fec;

    use super::*;
    use core::time::Duration;

    /// The loss both `test_fec_recovers_under_loss*` impairments inject, as a
    /// fraction.  Their assertions read the receiver's recovered-symbol
    /// counter, which is only non-zero if the sender's FEC condition gate was
    /// open when a group whose data symbol was lost reached its burst tail,
    /// so the tests pin this as the gate's loss evidence (see
    /// [`pin_fec_loss_evidence`]).
    const IMPAIRED_LOSS: f64 = 0.08;

    /// Pin the FEC condition gate's loss evidence to the loss the impairment
    /// stands for.
    ///
    /// The gate decides on `max(congestion loss, sender-side recovery ratio)`.
    /// The congestion term comes from the delivery-rate controller's
    /// loss-event window: two smoothed RTTs wide, and it abstains until it
    /// holds 16 samples.  A single-packet ping-pong over loopback never
    /// accumulates that many, so the term is `None` on every gate refresh
    /// (measured) and the gate is driven by the recovery ratio alone — which
    /// is 0 whenever the last 64 primary sends held no retransmission, and
    /// which the hysteresis then turns into a closed gate.  A run whose
    /// repairs all happened to fall outside that ring therefore emits no
    /// parity at all and reports zero recovered symbols even though 8% of the
    /// traffic is being lost and repaired by ARQ.  Pin the loss evidence so an
    /// 8% loss path is treated as one for the whole run, and the recovered
    /// count measures the parity path instead of the timing of the repairs.
    fn pin_fec_loss_evidence(sender: &Arc<Connection>) {
        sender
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .pin_congestion_loss_ratio_for_test(IMPAIRED_LOSS);
    }

    /// Counts every datagram the layer above hands to the underlay.  Installed
    /// *inside* the loss wrapper, so it counts forwarded (wire) datagrams only;
    /// a datagram the loss injector discards never reaches it.
    #[derive(Debug)]
    struct CountingWrite<W> {
        inner: W,
        datagrams: Arc<std::sync::atomic::AtomicU64>,
        bytes: Arc<std::sync::atomic::AtomicU64>,
    }

    impl<W> CountingWrite<W> {
        fn new(
            inner: W,
        ) -> (
            Self,
            Arc<std::sync::atomic::AtomicU64>,
            Arc<std::sync::atomic::AtomicU64>,
        ) {
            let datagrams = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
            (
                Self {
                    inner,
                    datagrams: Arc::clone(&datagrams),
                    bytes: Arc::clone(&bytes),
                },
                datagrams,
                bytes,
            )
        }
    }

    #[async_trait::async_trait]
    impl<W: crate::transmission::transmission_layer::UnreliableWrite>
        crate::transmission::transmission_layer::UnreliableWrite for CountingWrite<W>
    {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            let n = self.inner.send(buf).await?;
            self.datagrams
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.bytes
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
            Ok(n)
        }
    }

    #[tokio::test]
    async fn empty_stock_io_is_an_immediate_noop() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (mut a_read, mut a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (_b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), a_write.send(&[]))
                .await
                .expect("empty stock write waited")
                .unwrap(),
            0
        );
        let mut empty = [];
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), a_read.recv(&mut empty))
                .await
                .expect("empty stock read waited")
                .unwrap(),
            0
        );
        assert_eq!(b_write.send(b"payload").await.unwrap(), 7);
        let mut buf = [0; 16];
        let n = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("payload receive timed out")
            .expect("empty read disturbed the RTP receive path");
        assert_eq!(&buf[..n], b"payload");
    }

    #[tokio::test]
    async fn empty_generic_frame_io_is_a_noop_but_empty_frame_is_invalid() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let frame_delivery = crate::delivery::frame::mode::FrameMode::enabled();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            false,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            false,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let (mut a_read, mut a_write, _a_supervisor) = socket(a_layer, None);
        let (_b_read, mut b_write, _b_supervisor) = socket(b_layer, None);
        assert_eq!(a_write.send(&[]).await.unwrap(), 0);
        assert_eq!(
            a_write.send_frame(&[]).await,
            Err(std::io::ErrorKind::InvalidInput.into())
        );
        let mut empty = [];
        assert_eq!(a_read.recv(&mut empty).await.unwrap(), 0);
        assert_eq!(b_write.send_frame(b"frame").await.unwrap(), 5);
        let frame = tokio::time::timeout(Duration::from_secs(2), a_read.recv_frame())
            .await
            .expect("frame receive timed out")
            .expect("empty generic read disturbed frame delivery")
            .expect("empty generic read falsely exposed EOF");
        assert_eq!(frame, b"frame");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_io() {
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let hello = b"hello";
        let world = b"world";
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (mut a_r, mut a_w, _a_supervisor) = socket(a, None);
        let (mut b_r, mut b_w, _b_supervisor) = socket(b, None);
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
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss() {
        use crate::udp::testing::{BasisPoints, wrap_fec_lossy};
        // 8% loss: the FEC condition gate enables at 5% measured congestion
        // loss, and the pinned loss evidence below presents that 8% to it.
        let rate_a = BasisPoints::new(800);
        let rate_b = BasisPoints::new(800);
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec_lossy(a.clone(), a, fec, rate_a);
        let b = wrap_fec_lossy(b.clone(), b, fec, rate_b);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
        pin_fec_loss_evidence(&a_w.transmission_layer);
        let mut a_w = a_w;
        let mut a_r = a_r;
        let mut b_r = b_r;
        let mut b_w = b_w;
        // One message at a time, waiting for the echo before the next: each
        // message is a self-contained burst followed by a quiet gap, so the
        // sender reaches a genuine tail-FEC flush between messages (the loss
        // gate is open: 8% loss >> the 5% enable threshold) and the parity
        // crosses loopback well before any retransmission RTO could deliver
        // the lost symbol.  With ~15% effective loss (an independent roll on
        // the read and write wrapper) over 512 single-packet messages, at
        // least one message is dropped while its parity is in flight, so
        // recovery is all but certain — unlike a single bulk write_all,
        // which starves the assertion on timing: parity emission depends on
        // scarce spare-capacity moments mid-burst and the counter is read
        // before late tail parity decodes.
        let msg_len = 256;
        let n_msgs = 512;
        let mut sent = Vec::with_capacity(n_msgs);
        for i in 0..n_msgs {
            let mut m = vec![0u8; msg_len];
            for byte in &mut m {
                *byte = (i as u8).wrapping_add(rand::random());
            }
            sent.push(m);
        }
        let sent_for_server = sent.clone();
        let mut server_tasks = tokio::task::JoinSet::new();
        server_tasks.spawn(async move {
            let mut buf = vec![0u8; msg_len];
            for (idx, expected) in sent_for_server.iter().enumerate() {
                // No per-message wall-clock deadline: a run of unlucky iid
                // losses can exhaust the two tail-loss probes and leave the
                // packet to the 1 s `MIN_RTO`, so a correct repair legitimately
                // takes seconds (and longer under scheduler load). The test's
                // liveness is bounded once, by the 120 s timeout on the whole
                // echo exchange below; a fixed per-message deadline here only
                // turns that correct-but-slow repair into a spurious failure.
                let n = b_r.recv(&mut buf).await.unwrap();
                assert_eq!(&buf[..n], expected.as_slice());
                if crate::debug::debug_send() && idx % 50 == 0 {
                    eprintln!("[debug] b recv {idx}/{}", sent_for_server.len());
                }
                b_w.send(&buf[..n]).await.unwrap();
                if crate::debug::debug_send() {
                    eprintln!("[debug] b echoed {idx}");
                }
            }
            b_r.fec_recovered_symbols()
        });
        let exchange = async {
            for m in &sent {
                a_w.send(m).await.unwrap();
                let mut echo = vec![0u8; m.len()];
                let n = a_r.recv(&mut echo).await.unwrap();
                assert_eq!(&echo[..n], m.as_slice());
            }
            drop(a_w);
            drop(a_r);
        };
        tokio::time::timeout(Duration::from_secs(120), exchange)
            .await
            .expect("the FEC-under-loss echo exchange stalled");
        let recovered = server_tasks.join_next().await.unwrap().unwrap();
        assert!(recovered.is_some(), "FEC should be enabled on the receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols under 8% loss, got 0"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss_with_mss_8192() {
        use crate::socket::socket;
        use crate::traffic_shaping::redundancy::fec::gate::FecTuning;
        use crate::udp::testing::{BasisPoints, wrap_fec_lossy_with_mss_and_fec_tuning};
        // 8% loss: the FEC condition gate enables at 5% measured congestion
        // loss.  A wired-but-inert gate (configured yet never opened) would
        // emit no sender parity and fail the parity_sent assertion.  The
        // pinned loss evidence below presents that 8% to the gate: this lane
        // is a single-packet ping-pong whose delivery-rate loss-event window
        // never reaches its sample minimum, so without the pin the gate sees
        // no congestion loss at all and its openness would be a draw on when
        // retransmissions fell in the last 64 primary sends.
        let rate_a = BasisPoints::new(800);
        let rate_b = BasisPoints::new(800);
        let fec = true;
        let mss = 8192;
        let tuning = FecTuning::max_diversity();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let mut a_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(a.clone(), a, fec, mss, tuning, rate_a);
        let a_fec_counters = Arc::new(std::sync::Mutex::new(None));
        let observed_a_fec_counters = Arc::clone(&a_fec_counters);
        a_layer.metrics_observer = Some(crate::metrics::MetricsObserver::new(move |observation| {
            let Some(counters) = observation
                .snapshot
                .and_then(|snapshot| snapshot.fec_counters)
            else {
                return;
            };
            *observed_a_fec_counters.lock().unwrap() = Some(counters);
        }));
        let mut b_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(b.clone(), b, fec, mss, tuning, rate_b);
        let b_fec_counters = Arc::new(std::sync::Mutex::new(None));
        let observed_b_fec_counters = Arc::clone(&b_fec_counters);
        b_layer.metrics_observer = Some(crate::metrics::MetricsObserver::new(move |observation| {
            let Some(counters) = observation
                .snapshot
                .and_then(|snapshot| snapshot.fec_counters)
            else {
                return;
            };
            *observed_b_fec_counters.lock().unwrap() = Some(counters);
        }));
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (b_r, b_w, _b_supervisor) = socket(b_layer, None);
        pin_fec_loss_evidence(&a_w.transmission_layer);
        let mut b_r = b_r;
        let mut a_w = a_w;
        let mut a_r = a_r;
        let mut b_w = b_w;
        let msg_len = 256;
        let n_msgs = 512;
        let transfer_started = std::time::Instant::now();
        let mut sent = Vec::with_capacity(n_msgs);
        for i in 0..n_msgs {
            let mut m = vec![0u8; msg_len];
            for byte in &mut m {
                *byte = (i as u8).wrapping_add(rand::random());
            }
            sent.push(m.clone());
        }
        let sent_for_server = sent.clone();
        let mut server_tasks = tokio::task::JoinSet::new();
        server_tasks.spawn(async move {
            let mut buf = vec![0u8; msg_len];
            for (idx, expected) in sent_for_server.iter().enumerate() {
                // No per-message wall-clock deadline: a run of unlucky iid
                // losses can leave a correct repair to the 1 s `MIN_RTO`, so a
                // fixed deadline turns slow-but-correct recovery into a
                // spurious failure. Liveness is bounded once, by the 120 s
                // timeout on the whole echo exchange below.
                let n = b_r.recv(&mut buf).await.unwrap();
                assert_eq!(&buf[..n], expected.as_slice());
                if crate::debug::debug_send() && idx % 50 == 0 {
                    eprintln!("[debug] b recv {idx}/{}", sent_for_server.len());
                }
                b_w.send(&buf[..n]).await.unwrap();
                if crate::debug::debug_send() {
                    eprintln!("[debug] b echoed {idx}");
                }
            }
            b_r.fec_recovered_symbols()
        });
        // Keep the sender's connection handle so the observed sender-side
        // parity counter can be read after the transfer completes.
        let sender = Arc::clone(&a_w.transmission_layer);
        let exchange = async {
            for (idx, m) in sent.iter().enumerate() {
                if crate::debug::debug_send() && idx % 50 == 0 {
                    eprintln!("[debug] a sent {idx}/{}", sent.len());
                }
                a_w.send(m).await.unwrap();
                let mut echo = vec![0u8; m.len()];
                let n = a_r.recv(&mut echo).await.unwrap();
                assert_eq!(&echo[..n], m.as_slice());
            }
            drop(a_w);
            drop(a_r);
        };
        tokio::time::timeout(Duration::from_secs(120), exchange)
            .await
            .expect("the FEC-under-loss echo exchange stalled");
        let recovered = server_tasks.join_next().await.unwrap().unwrap();
        let sender_counters = a_fec_counters
            .lock()
            .unwrap()
            .expect("sender FEC counters must be observed");
        let receiver_counters = b_fec_counters
            .lock()
            .unwrap()
            .expect("receiver FEC counters must be observed");
        let parity_sent_direct = sender.fec_parity_sent_for_test();
        assert!(recovered.is_some(), "FEC should be enabled on the receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols under 8% loss, got 0; elapsed={:?} sender={sender_counters:?} receiver={receiver_counters:?} parity_sent_direct={parity_sent_direct:?}",
            transfer_started.elapsed()
        );
        assert!(
            sender_counters.parity_sent > 0,
            "loss-triggered FEC should emit parity through typed observations; counters={sender_counters:?}"
        );
        let parity_sent = sender.fec_parity_sent_for_test();
        assert!(
            parity_sent.is_some() && parity_sent.unwrap() > 0,
            "the sender must emit parity under 8% loss once the condition gate opens, got {parity_sent:?}"
        );
    }

    /// Focused, deterministic in-process probe for the single-symbol
    /// interactive repair path: 256-byte messages (one data symbol at MSS
    /// 8192) at a 25 ms cadence under 2% iid loss, for both the depth-1
    /// `interactive_prompt` preset (deployment) and depth-3 `max_diversity`.
    /// Prints the sender parity/gate counters and the receiver recovered
    /// count so the repair path is observable without the netem oracle.
    ///
    /// Self-validating report-only: prints the measurements and asserts the
    /// *instrument's* integrity (the arm ran its whole load, the far side
    /// observed it, and the counters it prints actually arrived) so a dead
    /// instrument fails instead of printing a table of zeros.  It asserts no
    /// bound from this crate's GATE.md.  The parity/recovery columns are
    /// reported rather than asserted: the iid loss stream's position depends on
    /// the sender's datagram count, so a correct run can show them as zero.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "in-process FEC-repair measurement probe; ~45 s; run with --ignored --nocapture"]
    async fn probe_single_symbol_interactive_fec_repair() {
        use crate::socket::socket;
        use crate::traffic_shaping::redundancy::fec::gate::FecTuning;
        use crate::udp::testing::{BasisPoints, wrap_fec_lossy_with_mss_and_fec_tuning};
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        let msg_len = 256usize;
        let n = 800usize;
        for (label, tuning) in [
            ("depth1_interactive_prompt", FecTuning::interactive_prompt()),
            ("depth3_max_diversity", FecTuning::max_diversity()),
        ] {
            let rate_a = BasisPoints::new(200);
            let rate_b = BasisPoints::new(200);
            let mss = 8192usize;
            let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            a.connect(b.local_addr().unwrap()).await.unwrap();
            b.connect(a.local_addr().unwrap()).await.unwrap();
            let mut a_layer =
                wrap_fec_lossy_with_mss_and_fec_tuning(a.clone(), a, true, mss, tuning, rate_a);
            let observed = Arc::new(Mutex::new(None));
            let sink = Arc::clone(&observed);
            a_layer.metrics_observer =
                Some(crate::metrics::MetricsObserver::new(move |observation| {
                    if let Some(counters) = observation
                        .snapshot
                        .and_then(|snapshot| snapshot.fec_counters)
                    {
                        *sink.lock().unwrap() = Some(counters);
                    }
                }));
            let b_layer =
                wrap_fec_lossy_with_mss_and_fec_tuning(b.clone(), b, true, mss, tuning, rate_b);
            let (mut a_r, mut a_w, _a_supervisor) = socket(a_layer, None);
            let (mut b_r, mut b_w, _b_supervisor) = socket(b_layer, None);
            let sent_ok = Arc::new(AtomicU64::new(0));
            let sent_ok_task = Arc::clone(&sent_ok);
            let mut echo_tasks = tokio::task::JoinSet::new();
            echo_tasks.spawn(async move {
                let mut buf = vec![0u8; msg_len];
                let mut delivered = 0usize;
                loop {
                    match tokio::time::timeout(Duration::from_millis(2000), b_r.recv(&mut buf))
                        .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                        Ok(Ok(len)) => {
                            delivered += 1;
                            let _ = b_w.send(&buf[..len]).await;
                        }
                    }
                }
                (delivered, b_r.fec_recovered_symbols())
            });
            for i in 0..n {
                let msg = vec![(i % 251) as u8; msg_len];
                if tokio::time::timeout(Duration::from_secs(2), a_w.send(&msg))
                    .await
                    .is_ok_and(|r| r.is_ok())
                {
                    sent_ok_task.fetch_add(1, AtomicOrdering::Relaxed);
                }
                let mut echo_buf = vec![0u8; msg_len];
                let _ =
                    tokio::time::timeout(Duration::from_millis(500), a_r.recv(&mut echo_buf)).await;
                tokio::time::sleep(Duration::from_millis(25)).await;
                if i == 99 || i == 399 || i == n - 1 {
                    eprintln!(
                        "[probe {label}] i={i} counters={:?}",
                        *observed.lock().unwrap()
                    );
                }
            }
            drop(a_w);
            drop(a_r);
            let (delivered, recovered) =
                tokio::time::timeout(Duration::from_secs(5), echo_tasks.join_next())
                    .await
                    .expect("echo task stalled")
                    .expect("echo task panicked")
                    .expect("echo task missing");
            let counters = *observed.lock().unwrap();
            eprintln!(
                "[probe {label}] sent={} delivered={} recovered={:?} counters={counters:?}",
                sent_ok.load(AtomicOrdering::Relaxed),
                delivered,
                recovered,
            );
            // Measurement integrity: the arm's load ran to completion, the far
            // side observed it, and the counters this probe exists to report
            // were actually delivered.  These assert the instrument, never the
            // transport: no bound from GATE.md appears here.
            let sent = sent_ok.load(AtomicOrdering::Relaxed);
            assert_eq!(
                sent, n as u64,
                "[probe {label}] only {sent} of the arm's {n} sends completed: the printed table is not the arm's load"
            );
            assert!(
                delivered > 0,
                "[probe {label}] the receiver observed 0 of the {sent} messages sent: nothing was measured"
            );
            assert!(
                counters.is_some(),
                "[probe {label}] no FEC-counter observation reached the probe: the counters the probe reports are missing, not zero"
            );
            assert!(
                recovered.is_some(),
                "[probe {label}] the receiver reports no FEC state (`recovered=None`): the repair path this arm measures is unobservable"
            );
            // No mechanism assertion is sound here: the two arms' iid loss is
            // drawn from a stream whose position depends on how many datagrams
            // the sender wrote, so whether the parity gate opens varies between
            // runs (a run has measured depth3 with `parity_sent` 771 and with
            // 0).  `parity_sent`/`recovered` may therefore legitimately be 0 on
            // either arm and the printed table is read as-is.
        }
    }

    /// Latency-focused companion to the single-symbol repair probe: 256-byte
    /// messages at a 25 ms cadence under 2% iid loss on the interactive
    /// preset, recording each echo round-trip latency and the sender's
    /// armor-duplicate count.  A lone loss repaired by same-round-trip
    /// redundancy shows as a sub-millisecond echo instead of the ~10 ms
    /// tail-loss-probe wait, so the p99 echo latency is the interactive
    /// repair tail.  Run with `--ignored --nocapture` to print the summary.
    ///
    /// Self-validating report-only: prints the measurements and asserts the
    /// *instrument's* integrity (the arm ran its whole load, no echo missed its
    /// deadline, the counters it prints actually arrived and agree with the
    /// arm's label) so a dead instrument fails instead of printing a table of
    /// zeros.  It asserts no bound from this crate's GATE.md.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "in-process interactive repair-latency probe; ~25 s; run with --ignored --nocapture"]
    async fn probe_fresh_tail_armor_latency() {
        use crate::socket::socket;
        use crate::traffic_shaping::redundancy::fec::gate::FecTuning;
        use crate::udp::testing::{BasisPoints, wrap_fec_lossy_with_mss_and_fec_tuning};
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        use std::time::Instant;

        let msg_len = 256usize;
        let n = 800usize;
        let rate_a = BasisPoints::new(200);
        let rate_b = BasisPoints::new(200);
        let mss = 8192usize;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let mut a_layer = wrap_fec_lossy_with_mss_and_fec_tuning(
            a.clone(),
            a,
            true,
            mss,
            FecTuning::interactive_prompt(),
            rate_a,
        );
        let observed = Arc::new(Mutex::new(None));
        let armor_duplicates = Arc::new(AtomicU64::new(0));
        let sink = Arc::clone(&observed);
        let armor_sink = Arc::clone(&armor_duplicates);
        a_layer.metrics_observer = Some(crate::metrics::MetricsObserver::new(move |observation| {
            if observation.event == crate::metrics::MetricsEvent::RetransmissionArmorDuplicate {
                armor_sink.fetch_add(1, AtomicOrdering::Relaxed);
            }
            if let Some(counters) = observation
                .snapshot
                .and_then(|snapshot| snapshot.fec_counters)
            {
                *sink.lock().unwrap() = Some(counters);
            }
        }));
        let b_layer = wrap_fec_lossy_with_mss_and_fec_tuning(
            b.clone(),
            b,
            true,
            mss,
            FecTuning::interactive_prompt(),
            rate_b,
        );
        let (mut a_r, mut a_w, _a_supervisor) = socket(a_layer, None);
        let (mut b_r, mut b_w, _b_supervisor) = socket(b_layer, None);
        let mut echo_tasks = tokio::task::JoinSet::new();
        echo_tasks.spawn(async move {
            let mut buf = vec![0u8; msg_len];
            loop {
                match tokio::time::timeout(Duration::from_millis(2000), b_r.recv(&mut buf)).await {
                    Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                    Ok(Ok(len)) => {
                        let _ = b_w.send(&buf[..len]).await;
                    }
                }
            }
        });
        let mut latencies = Vec::with_capacity(n);
        for i in 0..n {
            let msg = vec![(i % 251) as u8; msg_len];
            let started = Instant::now();
            let _ = tokio::time::timeout(Duration::from_secs(2), a_w.send(&msg)).await;
            let mut echo_buf = vec![0u8; msg_len];
            if tokio::time::timeout(Duration::from_millis(500), a_r.recv(&mut echo_buf))
                .await
                .is_ok_and(|r| r.is_ok())
            {
                latencies.push(started.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        drop(a_w);
        drop(a_r);
        let _ = tokio::time::timeout(Duration::from_secs(5), echo_tasks.join_next()).await;
        latencies.sort_unstable();
        let pick = |q: f64| -> Duration {
            if latencies.is_empty() {
                return Duration::ZERO;
            }
            let idx = ((latencies.len() - 1) as f64 * q).round() as usize;
            latencies[idx]
        };
        let repaired = latencies
            .iter()
            .filter(|latency| **latency > Duration::from_millis(2))
            .count();
        let counters = *observed.lock().unwrap();
        eprintln!(
            "[probe fresh-armor] samples={} repaired_gt_2ms={} p50={:?} p90={:?} p99={:?} max={:?} armor_duplicates={} counters={counters:?}",
            latencies.len(),
            repaired,
            pick(0.50),
            pick(0.90),
            pick(0.99),
            latencies.last().copied().unwrap_or_default(),
            armor_duplicates.load(AtomicOrdering::Relaxed),
        );
        // Measurement integrity: every one of the arm's `n` sends was awaited to
        // its own echo inside the 500 ms deadline, the fresh-tail armour this
        // probe is named for fired, the FEC counters arrived, and a real clock
        // measured the echoes.  Facts about the instrument's run, not bounds.
        assert_eq!(
            latencies.len(),
            n,
            "[probe fresh-armor] only {} of the arm's {n} messages produced a measured echo: a missing sample is an instrument failure, not a latency",
            latencies.len()
        );
        assert!(
            armor_duplicates.load(AtomicOrdering::Relaxed) > 0,
            "[probe fresh-armor] the sender re-sent no fresh-tail armour cover: the sub-millisecond echoes this probe attributes to armour were not armour"
        );
        assert!(
            counters.is_some(),
            "[probe fresh-armor] no FEC-counter observation reached the probe: the counters the probe reports are missing, not zero"
        );
        assert!(
            latencies.last().copied().unwrap_or_default() > Duration::ZERO,
            "[probe fresh-armor] every measured echo took zero time: the clock, not the link, produced these samples"
        );
    }

    /// Burst-loss companion to `probe_fresh_tail_armor_latency`: the same
    /// 256-byte, 25 ms interactive stream, but the sender-to-receiver direction
    /// drops runs of `burst` consecutive packets separated by a randomized
    /// quiet gap, so a burst can wipe a whole redundancy group (the primary,
    /// every fresh-tail armor copy, and the parity that trails the group).
    /// Sweeps bursts three through six: the six-datagram cover is expected to
    /// absorb a five-packet burst on the same round trip, while a six-packet
    /// burst is the remaining residual.  Classifies each echo by repair path:
    /// a same-round-trip recovery stays near the loopback floor, while a
    /// fall-through to the reorder-window ARQ repair costs at least one extra
    /// RTT. Run with `--ignored --nocapture`.
    ///
    /// The connection is built handshake-less (`initial_rtt: None`), so its
    /// first data packet carries no RTT sample and its first tail-loss probe
    /// waits the 1 s initial-RTO floor.  That cold start is an artefact of
    /// this layer's construction (a production connection seeds the RTT from
    /// the handshake, see `Connection`'s `initial_rtt` sampling), and it shows
    /// up in the reported tail as one isolated ~1 s echo — seed
    /// `a_layer.initial_rtt = Some(owd * 2)` to see the same arm without it.
    ///
    /// Each arm also prints its repair-path tail: every echo above 55 ms with
    /// its message index, so a repair is attributable to the parity path (a
    /// same-round-trip recovery) or to the tail-loss-probe path (roughly two
    /// smoothed RTTs plus a round trip) instead of being read as one opaque
    /// maximum.
    ///
    /// Self-validating report-only: prints the measurements and asserts the
    /// *instrument's* integrity (the arm ran its whole load, no echo missed its
    /// deadline, the counters it prints actually arrived and agree with the
    /// arm's label) so a dead instrument fails instead of printing a table of
    /// zeros.  It asserts no bound from this crate's GATE.md.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "in-process burst-loss interactive repair probe; ~145 s; run with --ignored --nocapture"]
    async fn probe_fresh_tail_burst_loss_latency() {
        use crate::socket::socket;
        use crate::traffic_shaping::redundancy::fec::gate::FecTuning;
        use crate::udp::testing::{
            BurstLoss, wrap_fec_burst_delayed_with_mss_and_fec_tuning,
            wrap_fec_delayed_with_mss_and_fec_tuning,
        };
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        use std::time::Instant;

        let msg_len = 256usize;
        let n = 400usize;
        let mss = 8192usize;
        // A 20 ms one-way delay puts the loopback link's round trip at ~40 ms,
        // the deployment's WAN-scale RTT, so an ARQ fall-through shows the
        // real `reorder_window + one round trip` tail rather than the
        // sub-millisecond loopback floor.
        let owd = Duration::from_millis(20);
        for (label, burst, quiet_min, quiet_max, seed) in [
            ("clean_delayed", 0usize, 1usize, 1usize, 0x1111_2222u64),
            ("burst3_gap8_12", 3usize, 8usize, 12usize, 0x1234_5678u64),
            ("burst4_gap18_26", 4, 18, 26, 0x0BAD_F00D),
            ("burst5_gap22_30", 5, 22, 30, 0x5EED_0005),
            ("burst6_gap26_34", 6, 26, 34, 0x5EED_0006),
        ] {
            let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            a.connect(b.local_addr().unwrap()).await.unwrap();
            b.connect(a.local_addr().unwrap()).await.unwrap();
            let loss = BurstLoss::new(burst, quiet_min, quiet_max, seed);
            let loss_sink = loss.clone();
            let mut a_layer = wrap_fec_burst_delayed_with_mss_and_fec_tuning(
                a.clone(),
                a,
                true,
                mss,
                FecTuning::interactive_prompt(),
                loss,
                owd,
            );
            let observed = Arc::new(Mutex::new(None));
            let armor_duplicates = Arc::new(AtomicU64::new(0));
            let sink = Arc::clone(&observed);
            let armor_sink = Arc::clone(&armor_duplicates);
            a_layer.metrics_observer =
                Some(crate::metrics::MetricsObserver::new(move |observation| {
                    if observation.event
                        == crate::metrics::MetricsEvent::RetransmissionArmorDuplicate
                    {
                        armor_sink.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    if let Some(counters) = observation
                        .snapshot
                        .and_then(|snapshot| snapshot.fec_counters)
                    {
                        *sink.lock().unwrap() = Some(counters);
                    }
                }));
            let b_layer = wrap_fec_delayed_with_mss_and_fec_tuning(
                b.clone(),
                b,
                true,
                mss,
                FecTuning::interactive_prompt(),
                owd,
            );
            let (mut a_r, mut a_w, _a_supervisor) = socket(a_layer, None);
            let (mut b_r, mut b_w, _b_supervisor) = socket(b_layer, None);
            let recovered = Arc::new(AtomicU64::new(0));
            let recovered_sink = Arc::clone(&recovered);
            let mut echo_tasks = tokio::task::JoinSet::new();
            echo_tasks.spawn(async move {
                let mut buf = vec![0u8; msg_len];
                loop {
                    match tokio::time::timeout(Duration::from_millis(2000), b_r.recv(&mut buf))
                        .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                        Ok(Ok(len)) => {
                            let _ = b_w.send(&buf[..len]).await;
                        }
                    }
                }
                if let Some(recovered) = b_r.fec_recovered_symbols() {
                    recovered_sink.store(recovered as u64, AtomicOrdering::Relaxed);
                }
            });
            let mut latencies = Vec::with_capacity(n);
            let mut tail_probe: Vec<(usize, Duration)> = Vec::with_capacity(n);
            let mut timeouts = 0usize;
            for i in 0..n {
                let mut msg = vec![(i % 251) as u8; msg_len];
                // Stamp the index so a stale echo left over from an earlier
                // timeout is detected and discarded instead of being counted
                // as this message's near-instant round trip.
                msg[..4].copy_from_slice(&(i as u32).to_le_bytes());
                let started = Instant::now();
                let _ = tokio::time::timeout(Duration::from_secs(2), a_w.send(&msg)).await;
                let mut echo_buf = vec![0u8; msg_len];
                let mut matched = None;
                loop {
                    match tokio::time::timeout(Duration::from_secs(2), a_r.recv(&mut echo_buf))
                        .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) => break,
                        Ok(Ok(_)) => {
                            if echo_buf[..4] == msg[..4] {
                                matched = Some(started.elapsed());
                                break;
                            }
                        }
                        Err(_) => {
                            timeouts += 1;
                            break;
                        }
                    }
                }
                if let Some(latency) = matched {
                    latencies.push(latency);
                    tail_probe.push((i, latency));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            drop(a_w);
            drop(a_r);
            let _ = tokio::time::timeout(Duration::from_secs(5), echo_tasks.join_next()).await;
            latencies.sort_unstable();
            let pick = |q: f64| -> Duration {
                if latencies.is_empty() {
                    return Duration::ZERO;
                }
                let idx = ((latencies.len() - 1) as f64 * q).round() as usize;
                latencies[idx]
            };
            let count_gt = |ms: u64| {
                latencies
                    .iter()
                    .filter(|latency| **latency > Duration::from_millis(ms))
                    .count()
            };
            let counters = *observed.lock().unwrap();
            let tail: Vec<(usize, u128)> = tail_probe
                .iter()
                .filter(|(_, latency)| *latency > Duration::from_millis(55))
                .map(|(i, latency)| (*i, latency.as_millis()))
                .collect();
            eprintln!("[probe burst tail {label}] (msg, ms) {tail:?}");
            eprintln!(
                "[probe burst {label}] samples={} timeouts={} gt60ms={} gt90ms={} gt150ms={} p50={:?} p90={:?} p99={:?} max={:?} armor_duplicates={} dropped={} recovered={} counters={counters:?}",
                latencies.len(),
                timeouts,
                count_gt(60),
                count_gt(90),
                count_gt(150),
                pick(0.50),
                pick(0.90),
                pick(0.99),
                latencies.last().copied().unwrap_or_default(),
                armor_duplicates.load(AtomicOrdering::Relaxed),
                loss_sink.dropped(),
                recovered.load(AtomicOrdering::Relaxed),
            );
            // Measurement integrity, per arm: the whole load was measured, no
            // echo missed its 2 s deadline, the counters arrived, the fresh-tail
            // armour fired, the impairment instrument agrees with the arm's
            // label, and the matched echoes are real round trips (a fixed `owd`
            // per direction makes `2*owd` the RTT floor).  No GATE.md bound is
            // asserted; `recovered` may legitimately be 0 on any burst arm.
            assert_eq!(
                latencies.len(),
                n,
                "[probe burst {label}] only {} of the arm's {n} messages produced a measured echo",
                latencies.len()
            );
            assert_eq!(
                timeouts, 0,
                "[probe burst {label}] {timeouts} echoes missed their 2 s deadline: the arm did not measure the {n} it claims"
            );
            assert!(
                counters.is_some(),
                "[probe burst {label}] no FEC-counter observation reached the probe: the counters the probe reports are missing, not zero"
            );
            assert!(
                armor_duplicates.load(AtomicOrdering::Relaxed) > 0,
                "[probe burst {label}] the sender re-sent no fresh-tail armour cover: the armour path this probe measures never fired"
            );
            if burst == 0 {
                assert_eq!(
                    loss_sink.dropped(),
                    0,
                    "[probe burst {label}] the `burst=0` clean arm dropped {} datagrams: its link is not the clean link its label claims",
                    loss_sink.dropped()
                );
            } else {
                assert!(
                    loss_sink.dropped() > 0,
                    "[probe burst {label}] the burst-{burst} arm dropped nothing: the impairment the arm is named for did not fire"
                );
            }
            assert!(
                pick(0.50) >= owd * 2,
                "[probe burst {label}] p50 {:?} is below the link's two-way floor {:?}: these samples are not round trips of this link",
                pick(0.50),
                owd * 2
            );
        }
    }

    /// Lone-tail repair-deadline probe: the *production* request/response
    /// shape.
    ///
    /// `probe_fresh_tail_burst_loss_latency` builds its connection
    /// handshake-less, so its first flight has no RTT sample and its one
    /// ~1 s echo is the initial `MIN_RTO`, not a lone-tail deadline.  This
    /// probe seeds `initial_rtt` from the round trip (the production shape,
    /// see `Connection`'s handshake sampling) so every repair it measures is
    /// an RTT-estimated deadline, and lengthens the sender-to-receiver burst
    /// past the six-datagram fresh-tail cover so a burst can wipe the cover
    /// *and* the first tail-loss probe.
    ///
    /// Two arms isolate the lone tail's repair deadline at the deployment's
    /// WAN scale (100 ms one way -> ~200 ms RTT):
    ///
    /// - `burst6`: the burst is exactly the cover, so the first tail-loss
    ///   probe (PTO = 2*srtt) survives and the echo lands at ~2*srtt + RTT.
    /// - `burst8`: the burst also eats that first probe, so the repair waits
    ///   the second PTO at ~4*srtt + RTT — the ~1 s lone-tail episode the
    ///   field reports.
    ///
    /// Prints the per-arm echo percentiles, the repair-path tail (every echo
    /// with its message index), the armour-duplicate count, and the sender's
    /// retransmission counters (`tail_probes` vs `rto_reason`) so the arm's
    /// repair is attributable to the tail-loss probe or to the RTO.  Also
    /// prints a `BURST_TRIANGLE` line naming the deadline the two arms imply:
    /// the arm-to-arm max delta is the first PTO, so the second PTO follows.
    /// Run with `--ignored --nocapture`.
    ///
    /// Self-validating report-only: prints the measurements and asserts the
    /// *instrument's* integrity (the arm ran its whole load, no echo missed its
    /// deadline, the counters it prints actually arrived and agree with the
    /// arm's label) so a dead instrument fails instead of printing a table of
    /// zeros.  It asserts no bound from this crate's GATE.md.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "in-process lone-tail repair-deadline probe; ~45 s; run with --ignored --nocapture"]
    async fn probe_lone_tail_repair_deadline_latency() {
        use crate::metrics::MetricsSnapshot;
        use crate::socket::socket;
        use crate::traffic_shaping::redundancy::fec::gate::FecTuning;
        use crate::udp::testing::{
            BurstLoss, wrap_fec_burst_delayed_with_mss_and_fec_tuning,
            wrap_fec_delayed_with_mss_and_fec_tuning,
        };
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        use std::time::Instant;

        let msg_len = 256usize;
        let n = 200usize;
        let mss = 8192usize;
        // A 100 ms one-way delay puts the round trip at the deployment's
        // WAN-scale RTT, so the first and second PTOs are 400 ms and 800 ms
        // and the double-PTO lone-tail episode lands at the ~1 s the field
        // reports.
        let owd = Duration::from_millis(100);
        for (label, burst, quiet_min, quiet_max, seed) in [
            ("burst6_owd100", 6usize, 26usize, 34usize, 0x5EED_0006u64),
            ("burst8_owd100", 8usize, 26usize, 34usize, 0x5EED_0008u64),
        ] {
            let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            a.connect(b.local_addr().unwrap()).await.unwrap();
            b.connect(a.local_addr().unwrap()).await.unwrap();
            let loss = BurstLoss::new(burst, quiet_min, quiet_max, seed);
            let loss_sink = loss.clone();
            let mut a_layer = wrap_fec_burst_delayed_with_mss_and_fec_tuning(
                a.clone(),
                a,
                true,
                mss,
                FecTuning::interactive_prompt(),
                loss,
                owd,
            );
            // The production shape: the handshake seeds the estimator, so no
            // arm here waits the initial `MIN_RTO` floor.
            a_layer.initial_rtt = Some(owd * 2);
            let observed: Arc<Mutex<Option<MetricsSnapshot>>> = Arc::new(Mutex::new(None));
            let armor_duplicates = Arc::new(AtomicU64::new(0));
            let sink = Arc::clone(&observed);
            let armor_sink = Arc::clone(&armor_duplicates);
            a_layer.metrics_observer =
                Some(crate::metrics::MetricsObserver::new(move |observation| {
                    if observation.event
                        == crate::metrics::MetricsEvent::RetransmissionArmorDuplicate
                    {
                        armor_sink.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    if let Some(snapshot) = observation.snapshot {
                        *sink.lock().unwrap() = Some(snapshot);
                    }
                }));
            let b_layer = wrap_fec_delayed_with_mss_and_fec_tuning(
                b.clone(),
                b,
                true,
                mss,
                FecTuning::interactive_prompt(),
                owd,
            );
            let (mut a_r, mut a_w, _a_supervisor) = socket(a_layer, None);
            let (mut b_r, mut b_w, _b_supervisor) = socket(b_layer, None);
            let mut echo_tasks = tokio::task::JoinSet::new();
            echo_tasks.spawn(async move {
                let mut buf = vec![0u8; msg_len];
                loop {
                    match tokio::time::timeout(Duration::from_millis(3000), b_r.recv(&mut buf))
                        .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                        Ok(Ok(len)) => {
                            let _ = b_w.send(&buf[..len]).await;
                        }
                    }
                }
            });
            let mut latencies = Vec::with_capacity(n);
            let mut tail: Vec<(usize, u128)> = Vec::new();
            let mut timeouts = 0usize;
            for i in 0..n {
                let mut msg = vec![(i % 251) as u8; msg_len];
                msg[..4].copy_from_slice(&(i as u32).to_le_bytes());
                let started = Instant::now();
                let _ = tokio::time::timeout(Duration::from_secs(3), a_w.send(&msg)).await;
                let mut echo_buf = vec![0u8; msg_len];
                let mut matched = None;
                loop {
                    match tokio::time::timeout(Duration::from_secs(3), a_r.recv(&mut echo_buf))
                        .await
                    {
                        Ok(Ok(0)) | Ok(Err(_)) => break,
                        Ok(Ok(_)) => {
                            if echo_buf[..4] == msg[..4] {
                                matched = Some(started.elapsed());
                                break;
                            }
                        }
                        Err(_) => {
                            timeouts += 1;
                            break;
                        }
                    }
                }
                if let Some(latency) = matched {
                    latencies.push(latency);
                    if latency > Duration::from_millis(250) {
                        tail.push((i, latency.as_millis()));
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            drop(a_w);
            drop(a_r);
            let _ = tokio::time::timeout(Duration::from_secs(5), echo_tasks.join_next()).await;
            latencies.sort_unstable();
            let pick = |q: f64| -> Duration {
                if latencies.is_empty() {
                    return Duration::ZERO;
                }
                let idx = ((latencies.len() - 1) as f64 * q).round() as usize;
                latencies[idx]
            };
            let count_gt = |ms: u64| {
                latencies
                    .iter()
                    .filter(|latency| **latency > Duration::from_millis(ms))
                    .count()
            };
            let snapshot = *observed.lock().unwrap();
            let counters = snapshot.map(|snapshot| snapshot.retransmission_counters);
            eprintln!("[probe lone-tail {label}] first-PTO tail (msg, ms) {tail:?}");
            eprintln!(
                "[probe lone-tail {label}] samples={} timeouts={} gt90ms={} gt150ms={} gt300ms={} gt600ms={} p50={:?} p90={:?} p99={:?} max={:?} armor_duplicates={} dropped={} counters={counters:?}",
                latencies.len(),
                timeouts,
                count_gt(90),
                count_gt(150),
                count_gt(300),
                count_gt(600),
                pick(0.50),
                pick(0.90),
                pick(0.99),
                latencies.last().copied().unwrap_or_default(),
                armor_duplicates.load(AtomicOrdering::Relaxed),
                loss_sink.dropped(),
            );
            // Measurement integrity, per arm: the whole load was measured, no
            // echo missed its 3 s deadline, the impairment fired, the fresh-tail
            // armour fired, the retransmission counters arrived, the repair this
            // arm attributes to the tail-loss probe really was a tail probe, and
            // the matched echoes clear the link's two-way floor.  No GATE.md
            // bound is asserted; `rto_reason` may legitimately be nonzero.
            assert_eq!(
                latencies.len(),
                n,
                "[probe lone-tail {label}] only {} of the arm's {n} messages produced a measured echo",
                latencies.len()
            );
            assert_eq!(
                timeouts, 0,
                "[probe lone-tail {label}] {timeouts} echoes missed their 3 s deadline: the arm did not measure the {n} it claims"
            );
            let counters = counters.unwrap_or_else(|| {
                panic!(
                    "[probe lone-tail {label}] no retransmission-counter observation reached the probe: the tail_probes/rto_reason attribution the probe prints is missing, not zero"
                )
            });
            assert!(
                counters.tail_probes > 0,
                "[probe lone-tail {label}] the sender issued no tail-loss probe: the repair this arm attributes to the {label} deadline was not a tail probe; counters={counters:?}"
            );
            assert!(
                loss_sink.dropped() > 0,
                "[probe lone-tail {label}] the burst-{burst} arm dropped nothing: the impairment the arm is named for did not fire"
            );
            assert!(
                armor_duplicates.load(AtomicOrdering::Relaxed) > 0,
                "[probe lone-tail {label}] the sender re-sent no fresh-tail armour cover: the armour path this probe measures never fired"
            );
            assert!(
                pick(0.50) >= owd * 2,
                "[probe lone-tail {label}] p50 {:?} is below the link's two-way floor {:?}: these samples are not round trips of this link",
                pick(0.50),
                owd * 2
            );
        }
        eprintln!(
            "[probe lone-tail BURST_TRIANGLE] burst6 (cover wiped, first PTO survives) vs burst8 \
             (cover and first PTO wiped): the max delta between the arms is the first PTO \
             (2*srtt); the burst8 max is the second (4*srtt) plus one RTT."
        );
    }

    /// Efficiency-frontier probe for the interactive fresh-tail armor.
    ///
    /// One request/response cell of the 256-byte / 25 ms interactive stream
    /// over a 20 ms one-way WAN delay, with the sender-to-receiver direction
    /// impaired by iid loss or a deterministic burst.  The armor copy count is
    /// forced through the test-only override so it can be swept independently
    /// of the loss-adaptive ladder; `ARMOR_COPIES=-1` keeps the production
    /// ladder (the trunk baseline).  Prints one `ARMOR_CELL` line with the
    /// percentile echo latencies and the forwarded datagrams/bytes per message.
    /// Configuration comes from the environment so a shell loop can sweep
    /// cells without recompiling: `ARMOR_COPIES` (-1 = trunk), `ARMOR_BPS`
    /// (iid basis points, 0 = off), `ARMOR_BURST`/`ARMOR_GAP` (burst length and
    /// fixed quiet gap, burst 0 = off), `ARMOR_N`, `ARMOR_SEED`.  Run with
    /// `--ignored --nocapture`.
    ///
    /// Self-validating report-only: prints the measurements and asserts the
    /// *instrument's* integrity (the cell collected samples, ran its whole
    /// load, and its wire and metrics counters reported) so a dead instrument
    /// fails instead of printing a table of zeros.  It asserts no bound from
    /// this crate's GATE.md.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "in-process armor-frontier cell; ~10 s per cell; run with --ignored --nocapture"]
    async fn probe_armor_copy_cell() {
        use crate::socket::socket;
        use crate::traffic_shaping::redundancy::fec::gate::FecTuning;
        use crate::udp::testing::{
            BasisPoints, BurstLoss, wrap_fec_burst_delayed_with_mss_and_fec_tuning,
            wrap_fec_delayed_with_mss_and_fec_tuning, wrap_fec_iid_delayed_with_mss_and_fec_tuning,
        };
        use std::sync::atomic::Ordering as AtomicOrdering;
        use std::time::Instant;

        let env_usize = |key: &str, default: usize| {
            std::env::var(key)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(default)
        };

        let msg_len = 256usize;
        let n = env_usize("ARMOR_N", 400);
        let copies_arg = std::env::var("ARMOR_COPIES")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(-1);
        let copies_override = (copies_arg >= 0).then_some(copies_arg as usize);
        let bps = env_usize("ARMOR_BPS", 0);
        let burst = env_usize("ARMOR_BURST", 0);
        let gap = env_usize("ARMOR_GAP", 0);
        let seed = env_usize("ARMOR_SEED", 0x1234_5678) as u64;
        let owd = Duration::from_millis(20);
        let cadence_ms = env_usize("ARMOR_CADENCE_MS", 25);
        let mss = 8192usize;

        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let (counting, datagrams, wire_bytes) = CountingWrite::new(a.clone());
        let mut a_layer = if burst > 0 {
            let loss = BurstLoss::new(burst, gap, gap, seed);
            wrap_fec_burst_delayed_with_mss_and_fec_tuning(
                a.clone(),
                counting,
                true,
                mss,
                FecTuning::interactive_prompt(),
                loss,
                owd,
            )
        } else {
            wrap_fec_iid_delayed_with_mss_and_fec_tuning(
                a.clone(),
                counting,
                true,
                mss,
                FecTuning::interactive_prompt(),
                BasisPoints::new(bps),
                owd,
            )
        };
        a_layer.fresh_tail_armor_copies_override = copies_override;
        let last_state = Arc::new(std::sync::Mutex::new((0.0f64, 0.0f64, 0usize)));
        let armor_copies = Arc::new(std::sync::atomic::AtomicU64::new(0));
        {
            let sink = Arc::clone(&last_state);
            let armor = Arc::clone(&armor_copies);
            a_layer.metrics_observer =
                Some(crate::metrics::MetricsObserver::new(move |observation| {
                    if observation.event
                        == crate::metrics::MetricsEvent::RetransmissionArmorDuplicate
                    {
                        armor.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    if let Some(snapshot) = observation.snapshot {
                        *sink.lock().unwrap() = (
                            snapshot.send_rate_packets_per_second,
                            snapshot.pacer_tokens_packets,
                            snapshot.congestion_window_packets,
                        );
                    }
                }));
        }

        let b_layer = wrap_fec_delayed_with_mss_and_fec_tuning(
            b.clone(),
            b,
            true,
            mss,
            FecTuning::interactive_prompt(),
            owd,
        );
        let (mut a_r, mut a_w, _a_supervisor) = socket(a_layer, None);
        let (mut b_r, mut b_w, _b_supervisor) = socket(b_layer, None);
        let mut echo_tasks = tokio::task::JoinSet::new();
        echo_tasks.spawn(async move {
            let mut buf = vec![0u8; msg_len];
            loop {
                match tokio::time::timeout(Duration::from_millis(2000), b_r.recv(&mut buf)).await {
                    Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                    Ok(Ok(len)) => {
                        let _ = b_w.send(&buf[..len]).await;
                    }
                }
            }
        });

        let send_times: Arc<std::sync::Mutex<std::collections::HashMap<u32, Instant>>> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        let latencies = Arc::new(std::sync::Mutex::new(Vec::with_capacity(n)));
        // Consumer: record the echo round-trip of each stamped message as it
        // arrives.  Running independently of the producer keeps the send
        // cadence fixed at 25 ms even when a lost head-of-line message delays
        // every later echo, so the measured latency is the true application
        // tail rather than an artifact of a synchronous request/response loop.
        let mut consumer_tasks = tokio::task::JoinSet::new();
        {
            let send_times = Arc::clone(&send_times);
            let latencies = Arc::clone(&latencies);
            consumer_tasks.spawn(async move {
                let mut buf = vec![0u8; msg_len];
                loop {
                    match tokio::time::timeout(Duration::from_secs(2), a_r.recv(&mut buf)).await {
                        Ok(Ok(len)) if len >= 4 => {
                            let id = u32::from_le_bytes(buf[..4].try_into().unwrap());
                            if let Some(start) = send_times.lock().unwrap().remove(&id) {
                                latencies.lock().unwrap().push(start.elapsed());
                            }
                        }
                        _ => break,
                    }
                }
            });
        }
        // Producer: a fixed 25 ms cadence of 256-byte stamped messages.
        let mut send_failures = 0usize;
        for i in 0..n {
            let mut msg = vec![(i % 251) as u8; msg_len];
            msg[..4].copy_from_slice(&(i as u32).to_le_bytes());
            send_times.lock().unwrap().insert(i as u32, Instant::now());
            if !tokio::time::timeout(Duration::from_secs(2), a_w.send(&msg))
                .await
                .is_ok_and(|r| r.is_ok())
            {
                send_failures += 1;
                send_times.lock().unwrap().remove(&(i as u32));
            }
            tokio::time::sleep(Duration::from_millis(cadence_ms as u64)).await;
        }
        drop(a_w);
        // Let the consumer drain the last echoes (it exits after 2 s of idle).
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            while consumer_tasks.join_next().await.is_some() {}
        })
        .await;
        let _ = tokio::time::timeout(Duration::from_secs(5), echo_tasks.join_next()).await;

        let mut latencies = latencies.lock().unwrap().clone();
        latencies.sort_unstable();
        let pick = |q: f64| -> f64 {
            if latencies.is_empty() {
                return 0.0;
            }
            let idx = ((latencies.len() - 1) as f64 * q).round() as usize;
            latencies[idx].as_secs_f64() * 1000.0
        };
        let dgrams = datagrams.load(AtomicOrdering::Relaxed);
        let bytes = wire_bytes.load(AtomicOrdering::Relaxed);
        let copies_label = if copies_arg >= 0 {
            copies_arg.to_string()
        } else {
            "trunk".to_string()
        };
        let (rate, tokens, cwnd) = *last_state.lock().unwrap();
        eprintln!(
            "ARMOR_CELL copies={} bps={} burst={} gap={} n={} cadence_ms={} samples={} undelivered={} send_failures={} p50_ms={:.2} p90_ms={:.2} p99_ms={:.2} max_ms={:.2} armor_copies={} armor_per_msg={:.3} dgrams={} dgrams_per_msg={:.3} bytes_per_msg={:.1} send_rate={:.1} tokens={:.2} cwnd={}",
            copies_label,
            bps,
            burst,
            gap,
            n,
            cadence_ms,
            latencies.len(),
            n - latencies.len(),
            send_failures,
            pick(0.50),
            pick(0.90),
            pick(0.99),
            latencies
                .last()
                .map(|d| d.as_secs_f64() * 1000.0)
                .unwrap_or(0.0),
            armor_copies.load(AtomicOrdering::Relaxed),
            armor_copies.load(AtomicOrdering::Relaxed) as f64 / n as f64,
            dgrams,
            dgrams as f64 / n as f64,
            bytes as f64 / n as f64,
            rate,
            tokens,
            cwnd,
        );
        // Measurement integrity: the cell produced samples, its whole load
        // completed, the wire counter and the metrics observer both reported,
        // and an echoed round trip cannot beat the link's two-way floor.  None
        // of these is an obligation bound; they assert only that the printed
        // cell is a measurement at all.  `armor_copies` is deliberately not
        // asserted: `ARMOR_COPIES=0` is a legitimate swept cell.
        assert!(
            !latencies.is_empty(),
            "ARMOR_CELL copies={copies_label} bps={bps} burst={burst} gap={gap} n={n}: no echo was measured, so every percentile above is a placeholder zero"
        );
        assert_eq!(
            send_failures, 0,
            "ARMOR_CELL copies={copies_label} bps={bps} burst={burst} gap={gap} n={n}: {send_failures} sends missed their 2 s deadline, so the cell did not run the load it claims"
        );
        assert!(
            dgrams > 0 && bytes > 0,
            "ARMOR_CELL copies={copies_label} bps={bps} burst={burst} gap={gap} n={n}: the wire counter observed {dgrams} datagrams / {bytes} bytes: dgrams_per_msg is a dead counter, not zero traffic"
        );
        assert!(
            dgrams >= latencies.len() as u64,
            "ARMOR_CELL copies={copies_label} bps={bps} burst={burst} gap={gap} n={n}: {} messages were delivered through only {dgrams} written datagrams: the two counters disagree",
            latencies.len()
        );
        assert!(
            cwnd > 0,
            "ARMOR_CELL copies={copies_label} bps={bps} burst={burst} gap={gap} n={n}: no metrics snapshot reached the probe (send_rate/tokens/cwnd all zero): those columns are missing, not measured"
        );
        let two_way_floor_ms = 2.0 * owd.as_secs_f64() * 1000.0;
        assert!(
            pick(0.99) >= two_way_floor_ms,
            "ARMOR_CELL copies={copies_label} bps={bps} burst={burst} gap={gap} n={n}: p99 {:.2} ms is below the link's two-way floor {two_way_floor_ms:.2} ms: these samples are not round trips of this link",
            pick(0.99)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_stream_max_write_bytes_scales_with_mss() {
        use crate::udp::wrap_fec;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let b_wrapped = wrap_fec(Box::new(b.clone()), Box::new(b), false);
        let (_b_r, b_w, _b_supervisor) = socket(b_wrapped, None);
        assert_eq!(
            b_w.into_async_write().max_write_bytes(),
            8 * 1024,
            "default-MSS staging buffer must be exactly 8 KiB"
        );
        let mss = 9_000;
        let a = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            false,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            crate::delivery::frame::mode::FrameMode::default(),
        )
        .unwrap();
        let (_a_r, a_w, _a_supervisor) = socket(a, None);
        let write_stream = a_w.into_async_write();
        let max_write_bytes = write_stream.max_write_bytes();
        let per_packet_payload = crate::udp::Mss::try_new(mss)
            .unwrap()
            .max_data_size_per_pkt();
        assert_eq!(
            max_write_bytes % per_packet_payload,
            0,
            "max_write_bytes {max_write_bytes} should be a multiple of per-packet payload {per_packet_payload}"
        );
        assert!(
            8 * 1024 < max_write_bytes,
            "max_write_bytes {max_write_bytes} should exceed the default staging buffer of 8 KiB"
        );
        drop(write_stream);
    }

    /// A write of EXACTLY `max_write_bytes` must be accepted whole (polled
    /// to completion in one poll), never truncated to `max_write_bytes - 1`:
    /// the oversize guard is strict (`buf.len() > max_write_bytes`), so the
    /// exact-size write is a partial-write boundary.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_at_exactly_max_write_bytes_is_not_truncated() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        // Frame-delivery mode: a write is exactly one wire frame, so the
        // exact-size frame is the acceptance boundary (the oversize arm
        // rejects with InvalidInput instead of truncating).
        let a = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            fec,
            crate::udp::Mss::try_new(9_000).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            crate::delivery::frame::mode::FrameMode::enabled(),
        )
        .unwrap();
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (_a_r, a_w, _a_supervisor) = socket(a, None);
        let (_b_r, _b_w, _b_supervisor) = socket(b, None);
        let mut write_stream = a_w.into_async_write();
        let max_write_bytes = write_stream.max_write_bytes();
        let exact = vec![0u8; max_write_bytes];
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let pinned = Pin::new(&mut write_stream);
        match pinned.poll_write(&mut cx, &exact) {
            Poll::Ready(Ok(n)) => assert_eq!(
                n, max_write_bytes,
                "a write of exactly max_write_bytes must not be truncated"
            ),
            Poll::Ready(Err(e)) => panic!("an exact-size write must not error: {e}"),
            Poll::Pending => panic!("an exact-size write must not park"),
        }
    }

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
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (_b_r, b_w, _b_supervisor) = socket(b, None);
        let _a_r = a_r;
        let capacity = a_w.transmission_layer.send_data_buf_capacity_for_test();
        let mut write_stream = a_w.into_async_write();
        let big = vec![0u8; capacity * 4];
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
        drop(write_stream);
        drop(b_w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_mode_async_write_produces_one_frame() {
        use crate::delivery::frame::mode::FrameMode;
        use tokio::io::AsyncWriteExt;
        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameMode::enabled();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            fd,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            fd,
        )
        .unwrap();
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (mut b_r, _b_w, _b_supervisor) = socket(b_layer, None);
        let frame_size = 16 * 1024;
        let payload: Vec<u8> = (0..frame_size).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let mut a_stream = a_w.into_async_write();
        let mut send_tasks = tokio::task::JoinSet::new();
        send_tasks.spawn(async move {
            a_stream.write_all(&payload).await.unwrap();
            a_stream.shutdown().await.ok();
            a_stream
        });
        let frame = tokio::time::timeout(Duration::from_secs(5), b_r.recv_frame())
            .await
            .expect("recv_frame timed out")
            .expect("recv_frame failed")
            .expect("expected a frame, got EOF");
        assert_eq!(
            frame.len(),
            frame_size,
            "receiver must get exactly one frame of the original size"
        );
        assert_eq!(frame, expected, "frame contents must match");
        drop(a_r);
        drop(send_tasks.join_next().await.unwrap().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_mode_oversized_write_errors_instead_of_splitting() {
        use crate::delivery::frame::mode::FrameMode;
        use tokio::io::AsyncWriteExt;
        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameMode::enabled();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            fd,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            fd,
        )
        .unwrap();
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (_b_r, _b_w, _b_supervisor) = socket(b_layer, None);
        let mut a_stream = a_w.into_async_write();
        let oversize = vec![0u8; a_stream.max_write_bytes() + 1];
        let error = a_stream
            .write(&oversize)
            .await
            .expect_err("an oversized frame-mode write must not be silently truncated");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("exceeds the maximum frame size"),
            "the error should explain the frame-size limit, got: {error}"
        );
        // A write within the limit still works (one write = one frame).
        assert_eq!(a_stream.write(b"ok").await.unwrap(), 2);
        drop(a_r);
        drop(a_stream);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn flush_waits_until_the_staging_buffer_drains() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncWriteExt;
        #[derive(Debug)]
        struct WriteState {
            calls: AtomicUsize,
            first_started: tokio::sync::Notify,
            release_first: tokio::sync::Notify,
        }
        #[derive(Debug)]
        struct BlockingFirstWrite(Arc<WriteState>);
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for BlockingFirstWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                if self.0.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.0.first_started.notify_one();
                    self.0.release_first.notified().await;
                }
                Ok(buf.len())
            }
        }
        let state = Arc::new(WriteState {
            calls: AtomicUsize::new(0),
            first_started: tokio::sync::Notify::new(),
            release_first: tokio::sync::Notify::new(),
        });
        let layer = wrap_fec(
            Box::new(PendingRead),
            Box::new(BlockingFirstWrite(Arc::clone(&state))),
            false,
        );
        let (_read, write, _supervisor) = socket(layer, None);
        let mut write_stream = write.into_async_write();
        // Stage a full staging buffer's worth of bytes. The driver picks the
        // staged bytes up and parks in its first underlay send after
        // consuming at most one packet, leaving the rest staged.
        let payload = vec![7u8; write_stream.max_write_bytes()];
        assert!(write_stream.write(&payload).await.unwrap() > 0);
        tokio::time::timeout(Duration::from_secs(1), state.first_started.notified())
            .await
            .expect("the driver must start its first unreliable send");
        // flush is a barrier: it must stay pending while staged bytes remain.
        let mut flush = Box::pin(write_stream.flush());
        tokio::select! {
            result = &mut flush => panic!("flush must wait for the stage to drain, returned {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => (),
        }
        // Release the driver: it drains the stage, and flush proceeds.
        state.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(2), &mut flush)
            .await
            .expect("flush must complete once the driver drains the stage")
            .expect("flush failed after the drain");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_delivery_io_conversion_rejects_stock_mode() {
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (_b_r, _b_w, _b_supervisor) = socket(b, None);
        let result = into_frame_io_parts(a_r, a_w);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not configured for frame delivery"),
            "stock-mode connections must be rejected"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_delivery_io_preserves_frames_across_async_io() {
        use crate::delivery::frame::mode::FrameMode;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameMode::enabled();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            fd,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            fd,
        )
        .unwrap();
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (b_r, b_w, _b_supervisor) = socket(b_layer, None);
        let mut a_io = into_frame_io_parts(a_r, a_w)
            .expect("frame delivery halves must convert")
            .into_parts();
        let mut b_io = into_frame_io_parts(b_r, b_w)
            .expect("frame delivery halves must convert")
            .into_parts();
        let first = b"first";
        let second = b"second-frame";
        a_io.1.write_all(first).await.unwrap();
        a_io.1.write_all(second).await.unwrap();
        a_io.1.flush().await.unwrap();
        a_io.1.shutdown().await.ok();
        let mut buf = vec![0u8; 256];
        let n1 = tokio::time::timeout(Duration::from_secs(5), b_io.0.read(&mut buf))
            .await
            .expect("first read timed out")
            .expect("first read failed");
        assert_eq!(&buf[..n1], first, "first frame must match");
        let n2 = tokio::time::timeout(Duration::from_secs(5), b_io.0.read(&mut buf))
            .await
            .expect("second read timed out")
            .expect("second read failed");
        assert_eq!(&buf[..n2], second, "second frame must match");
    }

    #[tokio::test]
    async fn shutdown_rejects_later_write_and_preserves_read_half() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (mut a_read, a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (mut b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
        let mut a_write = a_write.into_async_write();
        a_write.write_all(b"request").await.unwrap();
        a_write.shutdown().await.unwrap();
        assert_eq!(
            a_write.write(b"after FIN").await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            a_write.write(&[]).await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        let mut buf = [0; 64];
        let request_len = tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
            .await
            .expect("request receive timed out")
            .expect("request receive failed");
        assert_eq!(&buf[..request_len], b"request");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
                .await
                .expect("FIN receive timed out")
                .expect("FIN receive failed"),
            0
        );
        assert_eq!(b_write.send(b"response").await.unwrap(), 8);
        let response_len = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("response receive timed out")
            .expect("write shutdown must preserve the RTP read half");
        assert_eq!(&buf[..response_len], b"response");
    }

    #[tokio::test]
    async fn recv_frame_rejects_a_partially_consumed_frame_tail() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let frame_delivery = crate::delivery::frame::mode::FrameMode::enabled();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            false,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            false,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::traffic_shaping::redundancy::fec::gate::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let (mut a_read, _a_write, _a_supervisor) = socket(a_layer, None);
        let (_b_read, mut b_write, _b_supervisor) = socket(b_layer, None);
        assert_eq!(b_write.send_frame(b"abcdef").await.unwrap(), 6);
        assert_eq!(b_write.send_frame(b"XYZ").await.unwrap(), 3);
        let mut prefix = [0; 2];
        assert_eq!(a_read.recv(&mut prefix).await.unwrap(), 2);
        assert_eq!(&prefix, b"ab");
        // A frame read while the previous frame's tail is still buffered
        // must be surfaced as an error, not silently discard the tail.
        let error = a_read.recv_frame().await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("still buffered"),
            "the error should explain the mixed read modes, got: {error}"
        );
        // The tail is preserved: byte reads keep delivering the remainder,
        // then the next frame, byte-wise.
        let mut tail = [0; 4];
        assert_eq!(a_read.recv(&mut tail).await.unwrap(), 4);
        assert_eq!(&tail, b"cdef");
        let mut next = [0; 3];
        assert_eq!(a_read.recv(&mut next).await.unwrap(), 3);
        assert_eq!(&next, b"XYZ");
        // Once the tail is drained, frame reads work again.
        assert_eq!(b_write.send_frame(b"next").await.unwrap(), 4);
        let frame = tokio::time::timeout(Duration::from_secs(2), a_read.recv_frame())
            .await
            .expect("frame receive timed out")
            .expect("frame receive failed")
            .expect("unexpected EOF");
        assert_eq!(frame, b"next");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelling_public_send_does_not_cancel_driver_io() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        #[derive(Debug)]
        struct WriteState {
            calls: AtomicUsize,
            first_started: tokio::sync::Notify,
            release_first: tokio::sync::Notify,
            first_completed: tokio::sync::Notify,
            first_cancelled: AtomicBool,
        }
        struct CancelProbe {
            state: Arc<WriteState>,
            completed: bool,
        }
        impl Drop for CancelProbe {
            fn drop(&mut self) {
                if !self.completed {
                    self.state.first_cancelled.store(true, Ordering::SeqCst);
                }
            }
        }
        #[derive(Debug)]
        struct DriverWrite {
            state: Arc<WriteState>,
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for DriverWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                if self.state.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let mut probe = CancelProbe {
                        state: Arc::clone(&self.state),
                        completed: false,
                    };
                    self.state.first_started.notify_one();
                    self.state.release_first.notified().await;
                    probe.completed = true;
                    self.state.first_completed.notify_one();
                }
                Ok(buf.len())
            }
        }
        let state = Arc::new(WriteState {
            calls: AtomicUsize::new(0),
            first_started: tokio::sync::Notify::new(),
            release_first: tokio::sync::Notify::new(),
            first_completed: tokio::sync::Notify::new(),
            first_cancelled: AtomicBool::new(false),
        });
        let layer = wrap_fec(
            Box::new(PendingRead),
            Box::new(DriverWrite {
                state: Arc::clone(&state),
            }),
            false,
        );
        let (_read, mut write, _supervisor) = socket(layer, None);
        let payload = vec![7; 64 * 1024];
        assert!(write.send(&payload).await.unwrap() > 0);
        tokio::time::timeout(Duration::from_millis(100), state.first_started.notified())
            .await
            .expect("writer driver must start the first unreliable send");
        assert!(write.send(&payload).await.unwrap() > 0);
        let mut blocked_send = Box::pin(write.send(&payload));
        tokio::select! {
            result = &mut blocked_send => panic!("send queue unexpectedly had capacity: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
        drop(blocked_send);
        assert!(
            !state.first_cancelled.load(Ordering::SeqCst),
            "cancelling a public send waiter must not cancel driver-owned I/O"
        );
        state.release_first.notify_one();
        tokio::time::timeout(Duration::from_millis(100), state.first_completed.notified())
            .await
            .expect("released driver I/O must complete");
        assert!(!state.first_cancelled.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(1), write.send(b"after cancellation"))
            .await
            .expect("send queue must resume after driver progress")
            .expect("send after cancellation must succeed");
    }

    /// `send_buf_empty` is a staging barrier, not a delivery barrier: it
    /// returns as soon as the send driver has claimed the staged bytes, even
    /// while the underlay send is still parked. Tearing the session down at
    /// that point aborts the parked send, so a teardown that stops at
    /// `send_buf_empty` can drop a datagram the peer is waiting for.
    /// `all_sent_data_acked` must not return until the datagram has been
    /// transmitted and acknowledged. A parked underlay write pins both halves
    /// deterministically.
    #[tokio::test(flavor = "multi_thread")]
    async fn send_buf_empty_does_not_wait_for_the_underlay_but_all_sent_data_acked_does() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let cancelled = Arc::new(AtomicBool::new(false));
        let layer = wrap_fec(
            Box::new(PendingRead),
            Box::new(PendingWrite {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                cancelled: Arc::clone(&cancelled),
            }),
            false,
        );
        let (read, mut write, supervisor) = socket(layer, None);

        // Stage one datagram; the driver claims it out of the staging buffer
        // and parks inside the underlay send.
        write.send(b"payload").await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("the send driver must reach the parked underlay send");

        // The staging barrier returns while the datagram is still parked...
        tokio::time::timeout(Duration::from_millis(200), write.send_buf_empty())
            .await
            .expect("send_buf_empty must not wait for the underlay send")
            .unwrap();

        // ...and the acknowledged barrier must not.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), write.all_sent_data_acked())
                .await
                .is_err(),
            "all_sent_data_acked returned while the datagram was still parked"
        );

        // Tearing the session down at the staging barrier cancels the parked
        // send: the datagram never reaches the wire.
        drop(write);
        drop(read);
        drop(supervisor);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the session must cancel the parked underlay send");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_is_callable_while_poll_write_retains_the_write_socket() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct WriteState {
            calls: AtomicUsize,
            first_started: tokio::sync::Notify,
            release_first: tokio::sync::Notify,
            kill_started: tokio::sync::Notify,
        }
        #[derive(Debug)]
        struct BlockingFirstWrite(Arc<WriteState>);
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for BlockingFirstWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                let call = self.0.calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    self.0.first_started.notify_one();
                    self.0.release_first.notified().await;
                } else {
                    self.0.kill_started.notify_one();
                }
                Ok(buf.len())
            }
        }
        let state = Arc::new(WriteState {
            calls: AtomicUsize::new(0),
            first_started: tokio::sync::Notify::new(),
            release_first: tokio::sync::Notify::new(),
            kill_started: tokio::sync::Notify::new(),
        });
        let layer = wrap_fec(
            Box::new(PendingRead),
            Box::new(BlockingFirstWrite(Arc::clone(&state))),
            false,
        );
        let (_read, write, _supervisor) = socket(layer, None);
        let mut write = write.into_async_write();
        let payload = vec![7; write.max_write_bytes()];
        assert!(write.write(&payload).await.unwrap() > 0);
        tokio::time::timeout(Duration::from_secs(1), state.first_started.notified())
            .await
            .expect("driver did not start its first unreliable write");
        let mut retained_pending_write = false;
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_millis(20), write.write(&payload)).await {
                Ok(Ok(n)) => assert!(n > 0),
                Ok(Err(error)) => panic!("staging failed unexpectedly: {error}"),
                Err(_) => {
                    retained_pending_write = true;
                    break;
                }
            }
        }
        assert!(
            retained_pending_write,
            "test did not leave PollWrite owning a pending ConnWriter"
        );
        write.send_kill_and_abort().await;
        state.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), state.kill_started.notified())
            .await
            .expect("driver did not observe the out-of-band abort request");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_bulk_transfer_survives_loss_reorder_and_duplication() {
        use crate::udp::testing::{ImpairRate, wrap_fec_impaired};
        for fec in [false, true] {
            let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            a.connect(b.local_addr().unwrap()).await.unwrap();
            b.connect(a.local_addr().unwrap()).await.unwrap();
            let rate_a = ImpairRate::new(1500, 3000, 1000);
            let rate_b = ImpairRate::new(1500, 3000, 1000);
            let a = wrap_fec_impaired(a.clone(), a, fec, rate_a.clone());
            let b = wrap_fec_impaired(b.clone(), b, fec, rate_b.clone());
            let (a_r, a_w, _a_supervisor) = socket(a, None);
            let (b_r, b_w, _b_supervisor) = socket(b, None);
            let mut send_buf = vec![0u8; 1 << 20];
            for byte in &mut send_buf {
                *byte = rand::random();
            }
            let mut recv_buf = vec![0u8; send_buf.len()];
            let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
            let mut b_r = b_r.into_async_read();
            let b_w = b_w.into_async_write();
            let expected = send_buf.clone();
            let recv_done = Arc::new(tokio::sync::Notify::new());
            let mut send_tasks = tokio::task::JoinSet::new();
            send_tasks.spawn({
                let recv_done = recv_done.clone();
                async move {
                    let _b_w = b_w;
                    a.write_all(&send_buf).await.unwrap();
                    recv_done.notified().await;
                    a
                }
            });
            tokio::time::timeout(Duration::from_secs(120), b_r.read_exact(&mut recv_buf))
                .await
                .unwrap_or_else(|_| panic!("fec={fec}: the transfer stalled"))
                .unwrap();
            assert_eq!(expected, recv_buf, "fec={fec}: the stream arrived corrupt");
            recv_done.notify_waiters();
            send_tasks.join_next().await.unwrap().unwrap();
            let (dropped, reordered, duplicated) = rate_a.applied();
            assert!(
                dropped > 0 && reordered > 0 && duplicated > 0,
                "fec={fec}: the data path was not actually impaired (dropped {dropped}, reordered {reordered}, duplicated {duplicated})"
            );
            let (dropped, reordered, duplicated) = rate_b.applied();
            assert!(
                dropped + reordered + duplicated > 0,
                "fec={fec}: the acknowledgement path was not impaired at all"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_packet_arriving_twice_does_not_duplicate_a_byte() {
        use crate::udp::testing::{ImpairRate, wrap_fec_impaired};
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let rate_a = ImpairRate::new(0, 0, 10_000);
        let a = wrap_fec_impaired(a.clone(), a, fec, rate_a.clone());
        let b = wrap_fec_impaired(b.clone(), b, fec, ImpairRate::new(0, 0, 10_000));
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
        let mut send_buf = vec![0u8; 1 << 18];
        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut recv_buf = vec![0u8; send_buf.len()];
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b_r = b_r.into_async_read();
        let b_w = b_w.into_async_write();
        let expected = send_buf.clone();
        let recv_done = Arc::new(tokio::sync::Notify::new());
        let mut send_tasks = tokio::task::JoinSet::new();
        send_tasks.spawn({
            let recv_done = recv_done.clone();
            async move {
                let _b_w = b_w;
                a.write_all(&send_buf).await.unwrap();
                recv_done.notified().await;
                a
            }
        });
        tokio::time::timeout(Duration::from_secs(120), b_r.read_exact(&mut recv_buf))
            .await
            .expect("the transfer stalled with every packet duplicated")
            .unwrap();
        assert_eq!(expected, recv_buf);
        recv_done.notify_waiters();
        send_tasks.join_next().await.unwrap().unwrap();
        let (_, _, duplicated) = rate_a.applied();
        assert!(duplicated > 0, "no packet was actually duplicated");
    }
}
