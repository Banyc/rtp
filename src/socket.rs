use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
};
use tokio::task::{JoinError, JoinHandle, JoinSet};

pub use crate::handshake::{client_opening_handshake, server_opening_handshake};

use crate::transmission::{
    read_half::ReadHalf,
    shared::{Shared, build_parts, build_parts_with_watchdog_tuning},
    termination::TerminationReaper,
    transmission_layer::{LogConfig, RecvBufs, SendBufs, SendKillPkt, UnreliableLayer},
    watchdog_tuning::WatchdogTuning,
    write_half::WriteHalf,
};

pub type ReadStream = PollRead<ReadSocket>;

#[derive(Debug)]
pub struct WriteStream {
    inner: PollWrite<WriteSocket>,
    max_stage: usize,
    abort_session: Arc<Shared>,
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
        self.abort_session.request_kill_and_abort();
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let max_stage = self.max_stage;
        let buf = if buf.len() > max_stage {
            &buf[..max_stage]
        } else {
            buf
        };
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl std::marker::Unpin for WriteStream {}

#[cfg(test)]
impl WriteStream {
    pub fn max_stage(&self) -> usize {
        self.max_stage
    }
}

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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for IoStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct FrameReader {
    inner: ReadStream,
}
impl tokio::io::AsyncRead for FrameReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
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
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
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
    let _ = assert_send::<SessionSupervisor>;
};

#[derive(Debug)]
#[must_use = "the RTP session supervisor must be retained and awaited"]
pub struct SessionSupervisor {
    join: JoinHandle<()>,
}

impl Future for SessionSupervisor {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.join).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) if error.is_panic() => {
                std::panic::resume_unwind(error.into_panic())
            }
            Poll::Ready(Err(error)) => {
                panic!("RTP session supervisor was unexpectedly cancelled: {error}")
            }
        }
    }
}

pub fn socket(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (ReadSocket, WriteSocket, SessionSupervisor) {
    build_socket(build_parts(unreliable_layer, log_config))
}

pub fn socket_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (ReadSocket, WriteSocket, SessionSupervisor) {
    build_socket(build_parts_with_watchdog_tuning(
        unreliable_layer,
        log_config,
        tuning,
    ))
}

type SocketParts = (Arc<Shared>, WriteHalf, ReadHalf, TerminationReaper);

fn build_socket(
    (shared, write_half, read_half, termination_reaper): SocketParts,
) -> (ReadSocket, WriteSocket, SessionSupervisor) {
    let read_shutdown = tokio_util::sync::CancellationToken::new();
    let write_shutdown = tokio_util::sync::CancellationToken::new();
    let stop_drivers = tokio_util::sync::CancellationToken::new();
    let mut events = JoinSet::new();

    events.spawn({
        let stop_drivers = stop_drivers.clone();
        async move {
            let mut write_half = write_half;
            let mut send_bufs = SendBufs::new();
            let kill_requested = write_half.kill_requested().clone();
            loop {
                let resume_send = write_half.resume_send().notified();
                let deadline = write_half.next_poll_send_time();
                match deadline {
                    Some(t) => {
                        tokio::select! {
                            () = tokio::time::sleep_until(t.into()) => (),
                            () = resume_send => (),
                            () = kill_requested.cancelled() => (),
                            () = stop_drivers.cancelled() => return,
                        }
                    }
                    None => {
                        tokio::select! {
                            () = resume_send => (),
                            () = kill_requested.cancelled() => (),
                            () = stop_drivers.cancelled() => return,
                        }
                    }
                }
                if write_half.send_pkts(&mut send_bufs).await.is_err() {
                    return;
                }
            }
        }
    });

    events.spawn({
        let read_shutdown = read_shutdown.clone();
        let stop_drivers = stop_drivers.clone();
        let shared = Arc::clone(&shared);
        let mut read_half = read_half;
        async move {
            let mut recv_bufs = RecvBufs::new();
            let mut read_closed = read_shutdown.is_cancelled();
            loop {
                let recv_result = if read_closed {
                    tokio::select! {
                        biased;
                        () = stop_drivers.cancelled() => return,
                        result = read_half.recv_pkts(&mut recv_bufs) => result,
                    }
                } else {
                    tokio::select! {
                        biased;
                        () = stop_drivers.cancelled() => return,
                        () = read_shutdown.cancelled() => {
                            read_closed = true;
                            continue;
                        }
                        result = read_half.recv_pkts(&mut recv_bufs) => result,
                    }
                };
                let recv_pkts = match recv_result {
                    Ok(recv_pkts) => recv_pkts,
                    Err((_error, should_send_kill_pkt)) => {
                        match should_send_kill_pkt {
                            SendKillPkt::Yes => shared.request_kill_and_abort(),
                            SendKillPkt::No => (),
                        }
                        return;
                    }
                };
                if read_closed && 0 < recv_pkts.num_payload_segments {
                    shared.request_kill_and_abort();
                    return;
                }
            }
        }
    });

    let supervisor = SessionSupervisor {
        join: tokio::spawn({
            let read_shutdown = read_shutdown.clone();
            let write_shutdown = write_shutdown.clone();
            let stop_drivers = stop_drivers.clone();
            let shared = Arc::clone(&shared);
            async move {
                let mut events = events;
                let first_exit = 'session: {
                    tokio::select! {
                        () = write_shutdown.cancelled() => {
                            shared.send_fin_buf();
                            shared.resume_send().notify_one();
                        }
                        () = termination_reaper.ready() => break 'session None,
                        result = next_event_exit(&mut events) => break 'session Some(result),
                    }
                    tokio::select! {
                        () = read_shutdown.cancelled() => (),
                        () = termination_reaper.ready() => break 'session None,
                        result = next_event_exit(&mut events) => break 'session Some(result),
                    }
                    tokio::select! {
                        () = termination_reaper.ready_or_graceful_close(shared.recv_fin(), shared.session_outbound_drained()) => break 'session None,
                        result = next_event_exit(&mut events) => break 'session Some(result),
                    }
                };
                stop_drivers.cancel();
                join_drivers(events, first_exit, &shared).await;
            }
        }),
    };

    let read = ReadSocket {
        transmission_layer: Arc::clone(&shared),
        frame_buf: Vec::new(),
        _shutdown_guard: read_shutdown.drop_guard(),
    };
    let write = WriteSocket {
        transmission_layer: Arc::clone(&shared),
        _shutdown_guard: write_shutdown.drop_guard(),
    };
    (read, write, supervisor)
}

async fn next_event_exit(events: &mut JoinSet<()>) -> Result<(), JoinError> {
    events
        .join_next()
        .await
        .expect("RTP event set became empty while the session was alive")
}

async fn join_drivers(
    mut events: JoinSet<()>,
    first_exit: Option<Result<(), JoinError>>,
    shared: &Shared,
) {
    let unexpected_clean_exit =
        first_exit.as_ref().is_some_and(Result::is_ok) && !shared.termination.has_error();
    let mut panic_payload = None;
    let mut cancelled = None;
    let mut result = first_exit;
    loop {
        if let Some(result) = result.take() {
            match result {
                Ok(()) => {}
                Err(error) if error.is_panic() => {
                    if panic_payload.is_none() {
                        panic_payload = Some(error.into_panic());
                    }
                }
                Err(error) => {
                    cancelled.get_or_insert_with(|| error.to_string());
                }
            }
        }
        result = events.join_next().await;
        if result.is_none() {
            break;
        }
    }
    if let Some(payload) = panic_payload {
        std::panic::resume_unwind(payload);
    }
    if let Some(error) = cancelled {
        panic!("RTP driver task was unexpectedly cancelled: {error}");
    }
    assert!(
        !unexpected_clean_exit,
        "RTP driver task exited without publishing a terminal session state"
    );
}

pub fn unsplit(read: ReadStream, write: WriteStream) -> IoStream {
    IoStream { read, write }
}

#[derive(Debug)]
pub struct ReadSocket {
    transmission_layer: Arc<Shared>,
    frame_buf: Vec<u8>,
    _shutdown_guard: tokio_util::sync::DropGuard,
}

impl ReadSocket {
    pub async fn recv(&mut self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        if data.is_empty() {
            return Ok(0);
        }
        if !self.frame_buf.is_empty() {
            let n = self.frame_buf.len().min(data.len());
            data[..n].copy_from_slice(&self.frame_buf[..n]);
            self.frame_buf.drain(..n);
            return Ok(n);
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

    pub async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>, std::io::ErrorKind> {
        self.frame_buf.clear();
        self.transmission_layer.recv_frame().await
    }

    pub fn into_async_read(self) -> ReadStream {
        PollRead::new(self)
    }

    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.transmission_layer.fec_recovered_symbols()
    }
}

#[derive(Debug)]
pub struct WriteSocket {
    transmission_layer: Arc<Shared>,
    _shutdown_guard: tokio_util::sync::DropGuard,
}

impl WriteSocket {
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.transmission_layer.send(data).await
    }

    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.transmission_layer.send_frame(frame).await
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
        self.transmission_layer.request_kill_and_abort();
    }

    pub fn into_async_write(self) -> WriteStream {
        let max_stage = self
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .write_unit_capacity();
        let abort_session = Arc::clone(&self.transmission_layer);
        WriteStream {
            inner: PollWrite::new(self),
            max_stage,
            abort_session,
        }
    }
}

pub(crate) fn into_frame_io_parts(
    read: ReadSocket,
    write: WriteSocket,
) -> std::io::Result<FrameIoParts> {
    if !Arc::ptr_eq(&read.transmission_layer, &write.transmission_layer) {
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

impl AsyncAsyncRead for ReadSocket {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf)
            .await
            .map_err(|kind| self.transmission_layer.io_error(kind))
    }
}

impl AsyncAsyncWrite for WriteSocket {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf)
            .await
            .map_err(|kind| self.transmission_layer.io_error(kind))
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.transmission_layer
            .throw_error()
            .map_err(|kind| self.transmission_layer.io_error(kind))?;
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.transmission_layer.send_fin_buf();
        self.transmission_layer.no_data_to_send().await?;
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

    use crate::udp::wrap_fec;

    use super::*;
    use core::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_stock_io_is_an_immediate_noop() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (mut a_read, mut a_write, _a_supervisor) = socket(wrap_fec(a.clone(), a, false), None);
        let (_b_read, mut b_write, _b_supervisor) = socket(wrap_fec(b.clone(), b, false), None);
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

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_generic_frame_io_is_a_noop_but_empty_frame_is_invalid() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let frame_delivery = crate::delivery::frame::FrameDelivery::enabled();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            a.clone(),
            a,
            false,
            crate::udp::NO_FEC_MSS,
            crate::transmission::fec_tuning::FecTuning::default(),
            frame_delivery,
        );
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            b.clone(),
            b,
            false,
            crate::udp::NO_FEC_MSS,
            crate::transmission::fec_tuning::FecTuning::default(),
            frame_delivery,
        );
        let (mut a_read, mut a_write, _a_supervisor) = socket(a_layer, None);
        let (_b_read, mut b_write, _b_supervisor) = socket(b_layer, None);
        assert_eq!(a_write.send(&[]).await.unwrap(), 0);
        assert_eq!(
            a_write.send_frame(&[]).await,
            Err(std::io::ErrorKind::InvalidInput)
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

        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
        let (mut a_r, mut a_w, _sup_a) = socket(a, None);
        let (mut b_r, mut b_w, _sup_b) = socket(b, None);
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
        let (a_r, a_w, _sup_a) = socket(a, None);
        let (b_r, b_w, _sup_b) = socket(b, None);

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
        use crate::udp::testing::{LossRate, wrap_fec_lossy};

        let rate_a = LossRate::new(300);
        let rate_b = LossRate::new(300);

        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec_lossy(a.clone(), a, fec, rate_a);
        let b = wrap_fec_lossy(b.clone(), b, fec, rate_b);
        let (a_r, a_w, _sup_a) = socket(a, None);
        let (b_r, b_w, _sup_b) = socket(b, None);

        let mut send_buf = vec![0; 4 << 20];
        let mut recv_buf = send_buf.clone();
        for byte in &mut send_buf {
            *byte = rand::random();
        }

        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b_r = b_r.into_async_read();
        let b_w = b_w.into_async_write();

        let send_buf_clone = send_buf.clone();
        let recv_done = Arc::new(tokio::sync::Notify::new());
        let recv_done_clone = recv_done.clone();
        let sender = tokio::spawn(async move {
            let _b_w = b_w;
            a.write_all(&send_buf_clone).await.unwrap();
            recv_done_clone.notified().await;
            a
        });

        b_r.read_exact(&mut recv_buf).await.unwrap();
        assert_eq!(send_buf, recv_buf);
        recv_done.notify_waiters();
        sender.await.unwrap();

        let recovered = b_r.inner().fec_recovered_symbols();
        assert!(recovered.is_some(), "FEC should be enabled on the receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols under 3% loss, got 0"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss_with_mss_8192() {
        use crate::socket::socket;
        use crate::transmission::fec_tuning::FecTuning;
        use crate::udp::testing::{LossRate, wrap_fec_lossy_with_mss_and_fec_tuning};

        let rate_a = LossRate::new(300);
        let rate_b = LossRate::new(300);

        let fec = true;
        let mss = 8192;
        let tuning = FecTuning::mindiv();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let a_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(a.clone(), a, fec, mss, tuning, rate_a);
        let b_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(b.clone(), b, fec, mss, tuning, rate_b);
        let (a_r, a_w, _sup_a) = socket(a_layer, None);
        let (b_r, b_w, _sup_b) = socket(b_layer, None);
        let mut a_r = a_r;
        let mut a_w = a_w;
        let mut b_r = b_r;
        let mut b_w = b_w;

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

    #[tokio::test(flavor = "multi_thread")]
    async fn write_stream_max_stage_scales_with_mss() {
        use crate::udp::{wrap_fec, wrap_fec_with_mss};

        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let b_wrapped = wrap_fec(b.clone(), b, false);
        let (_b_r, b_w, _sup_b) = socket(b_wrapped, None);
        assert_eq!(
            b_w.into_async_write().max_stage(),
            8 * 1024,
            "default-MSS staging buffer must be exactly 8 KiB"
        );

        let mss = 9_000;
        let a = wrap_fec_with_mss(a.clone(), a, false, mss);
        let (_a_r, a_w, _sup_a) = socket(a, None);
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

        drop(write_stream);
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
        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
        let (a_r, a_w, _sup_a) = socket(a, None);
        let (_b_r, b_w, _sup_b) = socket(b, None);

        let _a_r = a_r;

        let capacity = a_w
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .send_data_buf_capacity();

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
        use crate::delivery::frame::FrameDelivery;
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
        let (_a_r, a_w, _sup_a) = socket(a_layer, None);
        let (mut b_r, _b_w, _sup_b) = socket(b_layer, None);

        let frame_size = 16 * 1024;
        let payload: Vec<u8> = (0..frame_size).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();

        let mut a_stream = a_w.into_async_write();
        let send_task = tokio::spawn(async move {
            a_stream.write_all(&payload).await.unwrap();
            a_stream.shutdown().await.ok();
            a_stream
        });

        let frame = tokio::time::timeout(std::time::Duration::from_secs(5), b_r.recv_frame())
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

        drop(_b_w);
        let _ = send_task.await;
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
        let (a_r, a_w, _sup_a) = socket(a, None);
        let (_b_r, _b_w, _sup_b) = socket(b, None);
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
        use crate::delivery::frame::FrameDelivery;
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
        let (a_r, a_w, _sup_a) = socket(a_layer, None);
        let (b_r, b_w, _sup_b) = socket(b_layer, None);

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
        let n1 = tokio::time::timeout(std::time::Duration::from_secs(5), b_io.0.read(&mut buf))
            .await
            .expect("first read timed out")
            .expect("first read failed");
        assert_eq!(&buf[..n1], first, "first frame must match");

        let n2 = tokio::time::timeout(std::time::Duration::from_secs(5), b_io.0.read(&mut buf))
            .await
            .expect("second read timed out")
            .expect("second read failed");
        assert_eq!(&buf[..n2], second, "second frame must match");
    }
}
