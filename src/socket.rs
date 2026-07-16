use core::time::Duration;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
};
use rand::TryRng;
use tokio::task::JoinSet;

use crate::{
    codec::in_cmd_space,
    transmission::{
        shared::{Shared, build_parts, build_parts_with_watchdog_tuning},
        transmission_layer::{LogConfig, RecvBufs, SendBufs, SendKillPkt, UnreliableLayer},
        watchdog_tuning::WatchdogTuning,
        write_half::WriteHalf,
    },
};

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

    pub async fn send_kill_and_abort(&mut self) {
        self.inner_mut().send_kill_and_abort().await;
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

#[derive(Debug)]
pub struct FrameReader {
    inner: ReadStream,
}
impl tokio::io::AsyncRead for FrameReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[derive(Debug)]
pub struct FrameWriter {
    inner: WriteStream,
}
impl FrameWriter {
    pub async fn send_kill_and_abort(&mut self) {
        self.inner.send_kill_and_abort().await;
    }
}
impl tokio::io::AsyncWrite for FrameWriter {
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
    read: FrameReader,
    write: FrameWriter,
}
impl FrameIoParts {
    pub(crate) fn into_parts(self) -> (FrameReader, FrameWriter) {
        (self.read, self.write)
    }
}

const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<WriteStream>;
    let _ = assert_send::<ReadStream>;
    let _ = assert_send::<IoStream>;
    let _ = assert_send::<FrameReader>;
    let _ = assert_send::<FrameWriter>;
    let _ = assert_send::<FrameIoParts>;
};

pub fn socket(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (ReadSocket, WriteSocket) {
    let read_shutdown = tokio_util::sync::CancellationToken::new();
    let write_shutdown = tokio_util::sync::CancellationToken::new();
    let (shared, write_half, read_half) = build_parts(unreliable_layer, log_config);
    let mut events = JoinSet::new();

    // Send timer
    events.spawn({
        let write_half = Arc::clone(&write_half);
        async move {
            let mut send_bufs = SendBufs::new();
            loop {
                let resume_send = write_half.resume_send().notified();
                let deadline = write_half.next_poll_send_time();
                match deadline {
                    Some(t) => {
                        tokio::select! {
                            () = tokio::time::sleep_until(t.into()) => (),
                            () = resume_send => (),
                        }
                    }
                    None => {
                        resume_send.await;
                    }
                }
                if write_half.send_pkts(&mut send_bufs).await.is_err() {
                    return;
                }
            }
        }
    });

    // Recv
    events.spawn({
        let read_shutdown = read_shutdown.clone();
        let write_half = Arc::clone(&write_half);
        let mut read_half = read_half;
        async move {
            let mut recv_bufs = RecvBufs::new();
            loop {
                let is_read_shutdown = read_shutdown.is_cancelled();

                let recv_pkts = match read_half.recv_pkts(&mut recv_bufs).await {
                    Ok(recv_pkts) => recv_pkts,
                    Err((_e, should_send_kill_pkt)) => {
                        match should_send_kill_pkt {
                            SendKillPkt::Yes => {
                                let mut send_bufs = SendBufs::new();
                                let _ = write_half.send_kill_pkt(&mut send_bufs).await;
                            }
                            SendKillPkt::No => (),
                        }
                        return;
                    }
                };
                if is_read_shutdown && 0 < recv_pkts.num_payload_segments {
                    let mut send_bufs = SendBufs::new();
                    let _ = write_half.send_kill_pkt(&mut send_bufs).await;
                }
            }
        }
    });

    tokio::spawn({
        let read_shutdown = read_shutdown.clone();
        let write_shutdown = write_shutdown.clone();
        let shared = Arc::clone(&shared);
        async move {
            let _event_guard = events;

            tokio::select! {
                () = write_shutdown.cancelled() => {
                    shared.send_fin_buf();
                    shared.resume_send().notify_one();
                }
                () = shared.some_error().cancelled() => (),
            }
            tokio::select! {
                () = read_shutdown.cancelled() => (),
                () = shared.some_error().cancelled() => (),
            }

            tokio::select! {
                () = shared.recv_fin().cancelled() => (),
                () = shared.some_error().cancelled() => (),
                () = tokio::time::sleep(Duration::from_secs(675)) => (),
            }
        }
    });

    let read = ReadSocket {
        transmission_layer: Arc::clone(&shared),
        frame_buf: Mutex::new(Vec::new()),
        _shutdown_guard: read_shutdown.drop_guard(),
    };
    let write = WriteSocket {
        transmission_layer: Arc::clone(&write_half),
        send_bufs: SendBufs::new(),
        no_delay: true,
        _shutdown_guard: write_shutdown.drop_guard(),
    };
    (read, write)
}

pub fn socket_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (ReadSocket, WriteSocket) {
    let read_shutdown = tokio_util::sync::CancellationToken::new();
    let write_shutdown = tokio_util::sync::CancellationToken::new();
    let (shared, write_half, read_half) =
        build_parts_with_watchdog_tuning(unreliable_layer, log_config, tuning);
    let mut events = JoinSet::new();

    // Send timer
    events.spawn({
        let write_half = Arc::clone(&write_half);
        async move {
            let mut send_bufs = SendBufs::new();
            loop {
                let resume_send = write_half.resume_send().notified();
                let deadline = write_half.next_poll_send_time();
                match deadline {
                    Some(t) => {
                        tokio::select! {
                            () = tokio::time::sleep_until(t.into()) => (),
                            () = resume_send => (),
                        }
                    }
                    None => {
                        resume_send.await;
                    }
                }
                if write_half.send_pkts(&mut send_bufs).await.is_err() {
                    return;
                }
            }
        }
    });

    // Recv
    events.spawn({
        let read_shutdown = read_shutdown.clone();
        let write_half = Arc::clone(&write_half);
        let mut read_half = read_half;
        async move {
            let mut recv_bufs = RecvBufs::new();
            loop {
                let is_read_shutdown = read_shutdown.is_cancelled();

                let recv_pkts = match read_half.recv_pkts(&mut recv_bufs).await {
                    Ok(recv_pkts) => recv_pkts,
                    Err((_e, should_send_kill_pkt)) => {
                        match should_send_kill_pkt {
                            SendKillPkt::Yes => {
                                let mut send_bufs = SendBufs::new();
                                let _ = write_half.send_kill_pkt(&mut send_bufs).await;
                            }
                            SendKillPkt::No => (),
                        }
                        return;
                    }
                };
                if is_read_shutdown && 0 < recv_pkts.num_payload_segments {
                    let mut send_bufs = SendBufs::new();
                    let _ = write_half.send_kill_pkt(&mut send_bufs).await;
                }
            }
        }
    });

    tokio::spawn({
        let read_shutdown = read_shutdown.clone();
        let write_shutdown = write_shutdown.clone();
        let shared = Arc::clone(&shared);
        async move {
            let _event_guard = events;

            tokio::select! {
                () = write_shutdown.cancelled() => {
                    shared.send_fin_buf();
                    shared.resume_send().notify_one();
                }
                () = shared.some_error().cancelled() => (),
            }
            tokio::select! {
                () = read_shutdown.cancelled() => (),
                () = shared.some_error().cancelled() => (),
            }

            tokio::select! {
                () = shared.recv_fin().cancelled() => (),
                () = shared.some_error().cancelled() => (),
                () = tokio::time::sleep(Duration::from_secs(675)) => (),
            }
        }
    });

    let read = ReadSocket {
        transmission_layer: Arc::clone(&shared),
        frame_buf: Mutex::new(Vec::new()),
        _shutdown_guard: read_shutdown.drop_guard(),
    };
    let write = WriteSocket {
        transmission_layer: Arc::clone(&write_half),
        send_bufs: SendBufs::new(),
        no_delay: true,
        _shutdown_guard: write_shutdown.drop_guard(),
    };
    (read, write)
}

#[derive(Debug)]
pub struct ReadSocket {
    transmission_layer: Arc<Shared>,
    frame_buf: Mutex<Vec<u8>>,
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
        {
            let mut frame_buf = self.frame_buf.lock().unwrap();
            if !frame_buf.is_empty() {
                let n = frame_buf.len().min(data.len());
                data[..n].copy_from_slice(&frame_buf[..n]);
                frame_buf.drain(..n);
                return Ok(n);
            }
        }
        if self
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .frame_delivery_enabled()
        {
            match self.transmission_layer.recv_frame().await? {
                Some(frame) => {
                    let n = frame.len().min(data.len());
                    data[..n].copy_from_slice(&frame[..n]);
                    if n < frame.len() {
                        self.frame_buf
                            .lock()
                            .unwrap()
                            .extend_from_slice(&frame[n..]);
                    }
                    Ok(n)
                }
                None => Ok(0),
            }
        } else {
            self.transmission_layer.recv(data).await
        }
    }

    /// Receive one complete frame in frame-delivery mode.  Returns
    /// `Ok(Some(frame))` when a complete frame is available (possibly out of
    /// order past sequence holes), `Ok(None)` on EOF, or `Err(InvalidInput)`
    /// when frame-delivery mode is off.  When enabled, the byte-stream
    /// `recv()` returns `InvalidInput` — frame delivery and stock byte
    /// delivery are mutually exclusive per connection.
    pub async fn recv_frame(&self) -> Result<Option<Vec<u8>>, std::io::ErrorKind> {
        self.transmission_layer.recv_frame().await
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
    transmission_layer: Arc<WriteHalf>,
    send_bufs: SendBufs,
    no_delay: bool,
    _shutdown_guard: tokio_util::sync::DropGuard,
}
impl WriteSocket {
    /// This method may only send partial `data`.
    ///
    /// Return the number of bytes sent.
    ///
    /// In frame-delivery mode, every `send()` call maps to exactly one wire
    /// frame — one write = one frame.  This is what the `AsyncWrite` adapter
    /// and mux-over-rtp depend on.
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

    /// Send one complete frame in frame-delivery mode.  Returns
    /// `Ok(frame.len())` on success; `Err(InvalidInput)` when the mode is
    /// off or the frame is empty/oversize; `Err(WouldBlock)` when the
    /// staging cap is full (caller should retry after the send path drains).
    /// When enabled, `send()` delegates to `send_frame()` — one write = one
    /// frame.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.transmission_layer
            .send_frame(frame, self.no_delay, &mut self.send_bufs)
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

    pub async fn send_kill_and_abort(&mut self) {
        let _ = self
            .transmission_layer
            .send_kill_and_abort(&mut self.send_bufs)
            .await;
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
            .write_unit_capacity();
        WriteStream {
            inner: PollWrite::new(self),
            max_stage,
        }
    }
}

pub(crate) fn into_frame_io_parts(
    read: ReadSocket,
    write: WriteSocket,
) -> std::io::Result<FrameIoParts> {
    if !Arc::ptr_eq(&read.transmission_layer, &write.transmission_layer.shared) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RTP read and write halves belong to different connections",
        ));
    }
    let read_enabled = read
        .transmission_layer
        .reliable_layer()
        .lock()
        .unwrap()
        .frame_delivery_enabled();
    let write_enabled = write
        .transmission_layer
        .reliable_layer()
        .lock()
        .unwrap()
        .frame_delivery_enabled();
    if !read_enabled || !write_enabled {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RTP connection is not configured for frame delivery",
        ));
    }
    Ok(FrameIoParts {
        read: FrameReader {
            inner: read.into_async_read(),
        },
        write: FrameWriter {
            inner: write.into_async_write(),
        },
    })
}

pub fn unsplit(read: ReadStream, write: WriteStream) -> IoStream {
    IoStream { read, write }
}

impl AsyncAsyncRead for ReadSocket {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf)
            .await
            .map_err(|kind| self.transmission_layer.first_error.io_error(kind))
    }
}
impl AsyncAsyncWrite for WriteSocket {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf)
            .await
            .map_err(|kind| self.transmission_layer.first_error.io_error(kind))
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.transmission_layer
            .throw_error()
            .map_err(|kind| self.transmission_layer.first_error.io_error(kind))?;
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
/// How long a single raw send in the handshake may WouldBlock before we
/// give up and let the overall handshake deadline decide.  Kept short so
/// a transiently-blocked writer retries promptly within the handshake
/// timeout.
const HANDSHAKE_SEND_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const HANDSHAKE_SEND_TIMEOUT: Duration = Duration::from_millis(500);

async fn handshake_send(
    utp_write: &mut Box<dyn crate::transmission::transmission_layer::UnreliableWrite>,
    buf: &[u8],
    deadline: Instant,
) -> Result<(), std::io::Error> {
    let send_deadline = deadline.min(Instant::now() + HANDSHAKE_SEND_TIMEOUT);
    loop {
        match tokio::time::timeout_at(
            tokio::time::Instant::from_std(send_deadline),
            utp_write.send(buf),
        )
        .await
        {
            Ok(Ok(n)) if n == buf.len() => return Ok(()),
            Ok(Ok(_)) => {}
            Ok(Err(std::io::ErrorKind::WouldBlock)) => {}
            Ok(Err(e)) => return Err(std::io::Error::from(e)),
            Err(_) => return Err(std::io::Error::from(std::io::ErrorKind::TimedOut)),
        }
        if Instant::now() >= send_deadline {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
        }
        let retry_at = Instant::now()
            .checked_add(HANDSHAKE_SEND_RETRY_INTERVAL)
            .map(|t| t.min(send_deadline))
            .unwrap_or(send_deadline);
        tokio::time::sleep_until(tokio::time::Instant::from_std(retry_at)).await;
    }
}

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
    let deadline = Instant::now() + INIT_CONNECT_RTO.mul_f64((NUM_CONNECT_RETRIES + 1) as f64);
    let send_deadline = deadline
        .checked_sub(2 * INIT_CONNECT_RTO)
        .unwrap_or(deadline);
    for i in 0..=NUM_CONNECT_RETRIES {
        if i == NUM_CONNECT_RETRIES {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
        }
        handshake_send(&mut unreliable.utp_write, &challenge, send_deadline).await?;
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
    handshake_send(&mut unreliable.utp_write, &challenge, send_deadline).await?;
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
    let due = Instant::now() + INIT_CONNECT_RTO;
    let send_due = due.checked_sub(2 * INIT_CONNECT_RTO).unwrap_or(due);
    handshake_send(&mut unreliable.utp_write, &challenge, send_due).await?;

    neg_challenge(&mut challenge);
    let mut resp = [0; 1];
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

    /// FEC must recover lost data symbols at a large MSS (8192) under packet
    /// loss for the single-symbol interactive path.  At MSS 8192 a small
    /// message (< ~8175 bytes) is a single data symbol, so the stock
    /// depth-1 parity is one independent loss draw for the whole message.
    /// `FecTuning::mindiv()` (instream flush + depth 3) gives each
    /// single-symbol group three parity copies bypassing the budget gate,
    /// so a single loss is recoverable.  This sends many small interactive
    /// messages over a lossy large-MSS link and asserts
    /// `recovered_symbols > 0` on the receiver.
    ///
    /// Construction goes through `wrap_fec_lossy_with_mss_and_fec_tuning`,
    /// which delegates to the same `checked_mss_and_fec` /
    /// `wrap_fec_with_mss_and_fec_tuning` path production uses — so a
    /// regression that silently disables FEC at a non-default MSS (e.g.
    /// `if fec && mss == NO_FEC_MSS`) fails this test, not just the
    /// default-MSS one.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss_with_mss_8192() {
        use crate::socket::socket;
        use crate::transmission::fec_tuning::FecTuning;
        use crate::udp::testing::{LossRate, wrap_fec_lossy_with_mss_and_fec_tuning};

        // 3% loss on each side.
        let rate_a = LossRate::new(300);
        let rate_b = LossRate::new(300);

        let fec = true;
        let mss = 8192;
        let tuning = FecTuning::mindiv();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        // Construct both layers via the production FEC construction path
        // (checked_mss_and_fec / wrap_fec_with_mss_and_fec_tuning) with the
        // lossy read/write injected.  No post-construction overrides of
        // `.fec` or `.fec_tuning` — the test must exercise the real path so
        // a regression disabling FEC at non-default MSS fails here.
        let a_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(a.clone(), a, fec, mss, tuning, rate_a);
        let b_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(b.clone(), b, fec, mss, tuning, rate_b);
        let (a_r, a_w) = socket(a_layer, None);
        let (b_r, b_w) = socket(b_layer, None);
        let b_r = b_r;
        let mut a_w = a_w;
        let a_r = a_r;
        let mut b_w = b_w;

        // Send many small interactive messages, each forming a single data
        // symbol at MSS 8192.  512 messages of 256 bytes gives ~512
        // single-symbol groups; 3% loss drops ~15 data symbols, and mindiv's
        // depth-3 parity recovers the bulk of them.
        let msg_len = 256;
        let n_msgs = 512;
        let mut sent = Vec::with_capacity(n_msgs);
        for i in 0..n_msgs {
            let mut m = vec![0u8; msg_len];
            for byte in &mut m {
                *byte = (i as u8).wrapping_add(rand::random());
            }
            sent.push(m.clone());
        }

        // Echo server: read each message and echo it back so the connection
        // stays alive and the receiver drains.  Returns the receiver's FEC
        // recovered-symbol count for the assertion.
        let sent_for_server = sent.clone();
        let server_task = tokio::spawn(async move {
            let mut buf = vec![0u8; msg_len];
            for expected in &sent_for_server {
                let n = b_r.recv(&mut buf).await.unwrap();
                assert_eq!(&buf[..n], expected.as_slice());
                b_w.send(&buf[..n]).await.unwrap();
            }
            b_r.fec_recovered_symbols()
        });

        for m in &sent {
            a_w.send(m).await.unwrap();
            // Read the echo back to pace the stream and avoid overwhelming the
            // receiver window.  The echo is a single-symbol group in the
            // reverse direction, also protected by mindiv.
            let mut echo = vec![0u8; m.len()];
            let n = a_r.recv(&mut echo).await.unwrap();
            assert_eq!(&echo[..n], m.as_slice());
        }
        drop(a_w);
        drop(a_r);
        let recovered = server_task.await.unwrap();

        assert!(recovered.is_some(), "FEC should be enabled on the receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols at MSS 8192 under 3% loss with mindiv, got 0"
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

    /// Fix #12: `frame_mode_async_write_produces_one_frame` — in frame
    /// delivery mode, a single large AsyncWrite must produce exactly one
    /// frame on the wire (receiver reads one frame of that size).  Before
    /// the fix, `WriteStream` capped each write at the byte-stream staging
    /// capacity, splitting one large application frame into multiple wire
    /// frames.
    #[tokio::test(flavor = "multi_thread")]
    async fn frame_mode_async_write_produces_one_frame() {
        use crate::frame_delivery::FrameDelivery;
        use tokio::io::AsyncWriteExt;

        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameDelivery::enabled();

        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            a.clone(),
            a,
            fec,
            mss,
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        );
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            b.clone(),
            b,
            fec,
            mss,
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        );
        let (a_r, a_w) = socket(a_layer, None);
        let (b_r, _b_w) = socket(b_layer, None);

        // A frame larger than the byte-stream stage cap (8 KiB) but within
        // MAX_FRAME_LEN (64 KiB).  Before the fix, the WriteStream would
        // cap this at 8 KiB, producing multiple frames.
        let frame_size = 16 * 1024;
        let payload: Vec<u8> = (0..frame_size).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();

        let mut a_stream = a_w.into_async_write();
        let send_task = tokio::spawn(async move {
            a_stream.write_all(&payload).await.unwrap();
            a_stream.shutdown().await.ok();
            a_stream
        });

        // Receive one frame on the receiver.
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
        let _ = send_task.await;
    }

    /// `handshake_send_times_out_on_sustained_would_block` — an
    /// always-WouldBlock writer with a 5 ms deadline returns exactly
    /// `ErrorKind::TimedOut` and elapsed ~= the budget (not before, not
    /// unbounded).  Before the fix, `send` was awaited with no timeout, so
    /// a pending send future hung past the deadline indefinitely.
    #[tokio::test(flavor = "multi_thread")]
    async fn handshake_send_times_out_on_sustained_would_block() {
        use crate::transmission::transmission_layer::UnreliableWrite;
        use async_trait::async_trait;

        struct AlwaysWouldBlock;
        #[async_trait]
        impl UnreliableWrite for AlwaysWouldBlock {
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
        }
        impl std::fmt::Debug for AlwaysWouldBlock {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("AlwaysWouldBlock").finish_non_exhaustive()
            }
        }

        let mut writer: Box<dyn UnreliableWrite> = Box::new(AlwaysWouldBlock);
        let deadline = Instant::now() + Duration::from_millis(5);
        let start = Instant::now();
        let result = handshake_send(&mut writer, b"x", deadline).await;
        let elapsed = start.elapsed();

        match result {
            Err(e) => assert!(
                e.kind() == std::io::ErrorKind::TimedOut,
                "expected TimedOut, got {:?}",
                e.kind()
            ),
            Ok(_) => panic!("expected TimedOut, got Ok"),
        }
        // Must not return before the deadline (the between-iteration check
        // alone would fire immediately on the first WouldBlock → ~0 ms).
        assert!(
            elapsed >= Duration::from_millis(5),
            "elapsed {elapsed:?} must be >= 5ms budget"
        );
        // Must not hang unbounded — allow generous scheduling slack.
        assert!(
            elapsed < Duration::from_millis(500),
            "elapsed {elapsed:?} must be bounded (< 500ms)"
        );
    }

    /// `handshake_send_completes_on_late_writability` — a writer that
    /// WouldBlocks for most of the budget then becomes writable before the
    /// deadline returns `Ok(())`.  Before the fix, the fixed retry sleep was
    /// not clamped to the deadline, so a writer that became writable late in
    /// the budget was missed (sleep overshoots and returns TimedOut without
    /// retrying the now-ready send).
    #[tokio::test(flavor = "multi_thread")]
    async fn handshake_send_completes_on_late_writability() {
        use crate::transmission::transmission_layer::UnreliableWrite;
        use async_trait::async_trait;

        struct LateWritable {
            ready_at: Instant,
        }
        #[async_trait]
        impl UnreliableWrite for LateWritable {
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                if Instant::now() >= self.ready_at {
                    Ok(_buf.len())
                } else {
                    Err(std::io::ErrorKind::WouldBlock)
                }
            }
        }
        impl std::fmt::Debug for LateWritable {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("LateWritable").finish_non_exhaustive()
            }
        }

        let deadline = Instant::now() + Duration::from_millis(200);
        // Become writable 50 ms before the deadline — late in the budget but
        // before it expires.  The retry interval is 50 ms, so without
        // clamping the sleep to the deadline the last retry before the
        // writer is ready could overshoot and return TimedOut instead of
        // retrying the now-ready send.  With clamping, the sleep lands at
        // the deadline if the interval would overshoot, but a retry at
        // ~150 ms (when the writer is ready) must succeed before the 200 ms
        // deadline elapses.
        let ready_at = Instant::now() + Duration::from_millis(150);

        let mut writer: Box<dyn UnreliableWrite> = Box::new(LateWritable { ready_at });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            handshake_send(&mut writer, b"x", deadline),
        )
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!(
                "handshake_send should complete on late writability, got err: {:?}",
                e.kind()
            ),
            Err(_) => panic!("handshake_send hung (should have completed)"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_delivery_io_conversion_rejects_stock_mode() {
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
        let (a_r, a_w) = socket(a, None);
        let (_b_r, _b_w) = socket(b, None);
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
        use crate::frame_delivery::FrameDelivery;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameDelivery::enabled();

        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            a.clone(),
            a,
            fec,
            mss,
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        );
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            b.clone(),
            b,
            fec,
            mss,
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        );
        let (a_r, a_w) = socket(a_layer, None);
        let (b_r, b_w) = socket(b_layer, None);

        let mut a_io = into_frame_io_parts(a_r, a_w)
            .expect("frame delivery halves must convert")
            .into_parts();
        let mut b_io = into_frame_io_parts(b_r, b_w)
            .expect("frame delivery halves must convert")
            .into_parts();

        let first = b"first";
        let second = b"second-frame";

        // Write both frames through the sender's frame writer.
        a_io.1.write_all(first).await.unwrap();
        a_io.1.write_all(second).await.unwrap();
        a_io.1.flush().await.unwrap();
        a_io.1.shutdown().await.ok();

        // Two separately timed reads on the receiver must return exactly
        // those two frame payloads without coalescing.
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
}
