use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin, task::Poll};

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

const ACTIVE_SEND_POLL_INTERVAL: Duration = Duration::from_millis(1);
fn active_send_poll_time(now: Instant, deadline: Instant, has_staged_data: bool) -> Instant {
    let fallback = now + ACTIVE_SEND_POLL_INTERVAL;
    if deadline <= now {
        fallback
    } else if has_staged_data {
        deadline.min(fallback)
    } else {
        deadline
    }
}

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
                        let has_staged_data = !write_half
                            .reliable_layer()
                            .lock()
                            .unwrap()
                            .is_send_buf_empty();
                        let poll_time = active_send_poll_time(Instant::now(), t, has_staged_data);
                        tokio::select! {
                            () = tokio::time::sleep_until(poll_time.into()) => (),
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
                        () = read_shutdown.cancelled() => { read_closed = true; continue; }
                        result = read_half.recv_pkts(&mut recv_bufs) => result,
                    }
                };
                let recv_pkts = match recv_result {
                    Ok(recv_pkts) => recv_pkts,
                    Err((_e, should_send_kill_pkt)) => {
                        match should_send_kill_pkt {
                            SendKillPkt::Yes => {
                                shared.request_kill_and_abort();
                            }
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
                        () = write_shutdown.cancelled() => { shared.send_fin_buf(); shared.resume_send().notify_one(); }
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
                join_drivers(events, first_exit, &shared).await
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
            .map_err(|kind| self.transmission_layer.termination.io_error(kind))
    }
}

impl AsyncAsyncWrite for WriteSocket {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf)
            .await
            .map_err(|kind| self.transmission_layer.termination.io_error(kind))
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.transmission_layer
            .throw_error()
            .map_err(|kind| self.transmission_layer.termination.io_error(kind))?;
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
    use std::sync::Mutex;

    #[test]
    fn active_send_poll_bounds_future_and_stale_deadlines() {
        let now = Instant::now();
        let near = now + Duration::from_micros(500);
        assert_eq!(active_send_poll_time(now, near, true), near);
        assert_eq!(
            active_send_poll_time(now, now + Duration::from_secs(1), true),
            now + ACTIVE_SEND_POLL_INTERVAL
        );
        assert_eq!(
            active_send_poll_time(now, now + Duration::from_secs(1), false),
            now + Duration::from_secs(1)
        );
        assert_eq!(
            active_send_poll_time(now, now - Duration::from_secs(1), false),
            now + ACTIVE_SEND_POLL_INTERVAL
        );
    }

    #[tokio::test]
    async fn supervisor_reaps_immediately_when_terminal_error_has_no_kill() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct FailedRead;

        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for FailedRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::ConnectionReset)
            }

            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::ConnectionReset)
            }
        }

        #[derive(Debug)]
        struct DropProbeWrite {
            sends: Arc<AtomicUsize>,
            dropped: Arc<tokio::sync::Notify>,
        }

        impl Drop for DropProbeWrite {
            fn drop(&mut self) {
                self.dropped.notify_one();
            }
        }

        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for DropProbeWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                self.sends.fetch_add(1, Ordering::SeqCst);
                Ok(buf.len())
            }
        }

        let sends = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(tokio::sync::Notify::new());
        let layer = wrap_fec(
            FailedRead,
            DropProbeWrite {
                sends: Arc::clone(&sends),
                dropped: Arc::clone(&dropped),
            },
            false,
        );
        let (_read, _write, _supervisor) = socket(layer, None);
        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("a terminal read error without KILL must reap the writer immediately");
        assert_eq!(sends.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn event_task_panic_reaps_the_rest_of_the_rtp_session() {
        #[derive(Debug)]
        struct PanickingRead;
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for PanickingRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                panic!("injected RTP read panic")
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                panic!("injected RTP read panic")
            }
        }
        #[derive(Debug)]
        struct DropProbeWrite(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for DropProbeWrite {
            fn drop(&mut self) {
                if let Some(dropped) = self.0.take() {
                    let _ = dropped.send(());
                }
            }
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for DropProbeWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                Ok(buf.len())
            }
        }
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let layer = wrap_fec(PanickingRead, DropProbeWrite(Some(dropped_tx)), false);
        let (_read, _write, supervisor) = socket(layer, None);
        let owner = tokio::spawn(supervisor);
        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("a panicked RTP event task left the peer task alive")
            .expect("writer drop probe was lost");
        let error = tokio::time::timeout(Duration::from_secs(1), owner)
            .await
            .expect("RTP supervisor did not finish joining its drivers")
            .expect_err("RTP driver panic did not cascade to the owning task");
        assert!(error.is_panic());
    }

    #[tokio::test]
    async fn supervisor_signals_then_joins_without_cancelling_an_in_flight_send() {
        use std::sync::atomic::{AtomicBool, Ordering};
        #[derive(Debug)]
        struct GatedFailedRead {
            fail: Arc<tokio::sync::Notify>,
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for GatedFailedRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                self.fail.notified().await;
                Err(std::io::ErrorKind::ConnectionReset)
            }
        }
        #[derive(Debug)]
        struct GatedWrite {
            started: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for GatedWrite {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for GatedWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                self.started.notify_one();
                self.release.notified().await;
                Ok(buf.len())
            }
        }
        let fail = Arc::new(tokio::sync::Notify::new());
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let layer = wrap_fec(
            GatedFailedRead {
                fail: Arc::clone(&fail),
            },
            GatedWrite {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                dropped: Arc::clone(&dropped),
            },
            false,
        );
        let (_read, mut write, supervisor) = socket(layer, None);
        let owner = tokio::spawn(supervisor);
        assert_eq!(write.send(b"payload").await.unwrap(), 7);
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("RTP writer did not start its unreliable send");
        fail.notify_one();
        tokio::task::yield_now().await;
        assert!(
            !owner.is_finished(),
            "the supervisor returned before joining the in-flight writer"
        );
        assert!(
            !dropped.load(Ordering::SeqCst),
            "the suicide signal cancelled an in-flight unreliable send"
        );
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), owner)
            .await
            .expect("the supervisor did not join the released writer")
            .expect("the supervisor failed after a clean cooperative stop");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn supervisor_waits_for_requested_kill_attempt_before_reaping() {
        use std::sync::atomic::{AtomicBool, Ordering};
        #[derive(Debug)]
        struct PendingRead;
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for PendingRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                std::future::pending().await
            }
        }
        #[derive(Debug)]
        struct KillGateWrite {
            kill_started: Arc<tokio::sync::Notify>,
            release_kill: Arc<tokio::sync::Notify>,
            dropped: Arc<tokio::sync::Notify>,
            was_dropped: Arc<AtomicBool>,
        }
        impl Drop for KillGateWrite {
            fn drop(&mut self) {
                self.was_dropped.store(true, Ordering::SeqCst);
                self.dropped.notify_one();
            }
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for KillGateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                assert_eq!(buf, [2], "the gated send must be the RTP KILL command");
                self.kill_started.notify_one();
                self.release_kill.notified().await;
                Ok(buf.len())
            }
        }
        let kill_started = Arc::new(tokio::sync::Notify::new());
        let release_kill = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let was_dropped = Arc::new(AtomicBool::new(false));
        let layer = wrap_fec(
            PendingRead,
            KillGateWrite {
                kill_started: Arc::clone(&kill_started),
                release_kill: Arc::clone(&release_kill),
                dropped: Arc::clone(&dropped),
                was_dropped: Arc::clone(&was_dropped),
            },
            false,
        );
        let (_read, mut write, _supervisor) = socket(layer, None);
        write.send_kill_and_abort().await;
        tokio::time::timeout(Duration::from_secs(1), kill_started.notified())
            .await
            .expect("the writer must claim and start the requested KILL");
        tokio::task::yield_now().await;
        assert!(
            !was_dropped.load(Ordering::SeqCst),
            "the supervisor must leave the writer alive during the KILL attempt"
        );
        release_kill.notify_one();
        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("the supervisor must reap after the KILL attempt completes");
        assert!(was_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn graceful_drop_drains_response_after_peer_fin() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let (mut a_read, mut a_write, _a_supervisor) = socket(wrap_fec(a.clone(), a, false), None);
        let (mut b_read, mut b_write, _b_supervisor) = socket(wrap_fec(b.clone(), b, false), None);

        let request = b"request";
        let response = b"response";

        assert_eq!(b_write.send(request).await.unwrap(), request.len());
        drop(b_write);

        let mut buf = [0; 64];
        let request_len = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("request receive timed out")
            .expect("request receive failed");
        assert_eq!(&buf[..request_len], request);

        tokio::time::timeout(
            Duration::from_secs(2),
            a_read.transmission_layer.recv_eof().cancelled(),
        )
        .await
        .expect("consuming the final payload did not publish receive EOF");

        assert_eq!(a_write.send(response).await.unwrap(), response.len());
        drop(a_write);
        drop(a_read);

        let response_len = tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
            .await
            .expect("response receive timed out")
            .expect("response receive failed");
        assert_eq!(&buf[..response_len], response);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
                .await
                .expect("local FIN receive timed out")
                .expect("local FIN receive failed"),
            0
        );
    }

    #[tokio::test]
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

    #[tokio::test]
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
        let a = wrap_fec(a.clone(), a, fec);
        let b = wrap_fec(b.clone(), b, fec);
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
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
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
        assert!(recovered.is_some(), "FEC should be enabled on receiver");
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
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (b_r, b_w, _b_supervisor) = socket(b_layer, None);
        let mut b_r = b_r;
        let mut a_w = a_w;
        let mut a_r = a_r;
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
            "FEC should recover >0 symbols under 3% loss, got 0"
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
        let (_b_r, b_w, _b_supervisor) = socket(b_wrapped, None);
        assert_eq!(
            b_w.into_async_write().max_stage(),
            8 * 1024,
            "default-MSS staging buffer must be exactly 8 KiB"
        );
        let mss = 9_000;
        let a = wrap_fec_with_mss(a.clone(), a, false, mss);
        let (_a_r, a_w, _a_supervisor) = socket(a, None);
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
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (_b_r, b_w, _b_supervisor) = socket(b, None);
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
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (mut b_r, _b_w, _b_supervisor) = socket(b_layer, None);
        let frame_size = 16 * 1024;
        let payload: Vec<u8> = (0..frame_size).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let mut a_stream = a_w.into_async_write();
        let send_task = tokio::spawn(async move {
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
        let (mut a_read, a_write, _a_supervisor) = socket(wrap_fec(a.clone(), a, false), None);
        let (mut b_read, mut b_write, _b_supervisor) = socket(wrap_fec(b.clone(), b, false), None);
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
    async fn recv_frame_discards_a_partially_consumed_frame_tail() {
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
        let (mut a_read, _a_write, _a_supervisor) = socket(a_layer, None);
        let (_b_read, mut b_write, _b_supervisor) = socket(b_layer, None);
        assert_eq!(b_write.send_frame(b"abcdef").await.unwrap(), 6);
        assert_eq!(b_write.send_frame(b"XYZ").await.unwrap(), 3);
        let mut prefix = [0; 2];
        assert_eq!(a_read.recv(&mut prefix).await.unwrap(), 2);
        assert_eq!(&prefix, b"ab");
        let next = tokio::time::timeout(Duration::from_secs(2), a_read.recv_frame())
            .await
            .expect("next frame receive timed out")
            .expect("frame receive failed")
            .expect("unexpected EOF");
        assert_eq!(next, b"XYZ");
        assert_eq!(b_write.send_frame(b"next").await.unwrap(), 4);
        let mut next = [0; 4];
        assert_eq!(a_read.recv(&mut next).await.unwrap(), 4);
        assert_eq!(&next, b"next");
    }

    #[tokio::test]
    async fn read_drop_with_unread_payload_reaps_after_peer_fin() {
        #[derive(Debug)]
        struct DropProbeUdpWrite {
            socket: Arc<UdpSocket>,
            dropped: Option<tokio::sync::oneshot::Sender<()>>,
        }
        impl Drop for DropProbeUdpWrite {
            fn drop(&mut self) {
                if let Some(dropped) = self.dropped.take() {
                    let _ = dropped.send(());
                }
            }
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for DropProbeUdpWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                UdpSocket::send(&self.socket, buf)
                    .await
                    .map_err(|error| error.kind())
            }
        }
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let (a_read, a_write, _a_supervisor) = socket(
            wrap_fec(
                Arc::clone(&a),
                DropProbeUdpWrite {
                    socket: a,
                    dropped: Some(dropped_tx),
                },
                false,
            ),
            None,
        );
        let (b_read, mut b_write, _b_supervisor) = socket(wrap_fec(b.clone(), b, false), None);
        assert_eq!(b_write.send(b"unread").await.unwrap(), 6);
        drop(b_write);
        tokio::time::timeout(
            Duration::from_secs(2),
            a_read.transmission_layer.recv_fin().cancelled(),
        )
        .await
        .expect("peer FIN was not published");
        assert!(
            !a_read.transmission_layer.recv_eof().is_cancelled(),
            "unread payload must prevent application EOF"
        );
        drop(a_write);
        drop(a_read);
        tokio::time::timeout(Duration::from_secs(2), dropped_rx)
            .await
            .expect("RTP session waited for application EOF after its read half was dropped")
            .expect("writer drop probe was lost");
        drop(b_read);
    }

    #[tokio::test]
    async fn first_payload_after_read_drop_resets_rtp_session() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (a_read, mut a_write, _a_supervisor) = socket(wrap_fec(a.clone(), a, false), None);
        let (mut b_read, mut b_write, _b_supervisor) = socket(wrap_fec(b.clone(), b, false), None);
        drop(a_read);
        tokio::task::yield_now().await;
        assert_eq!(b_write.send(b"late payload").await.unwrap(), 12);
        let mut buf = [0; 64];
        let peer_error = tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
            .await
            .expect("peer did not receive RTP KILL")
            .expect_err("post-close payload must reset the RTP session");
        assert_eq!(peer_error, std::io::ErrorKind::BrokenPipe);
        let local_error = a_write
            .send(b"after reset")
            .await
            .expect_err("the RTP KILL sender must also be locally aborted");
        assert_eq!(local_error, std::io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn payload_consumed_before_read_drop_does_not_reset_write_half() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (mut a_read, mut a_write, _a_supervisor) = socket(wrap_fec(a.clone(), a, false), None);
        let (mut b_read, mut b_write, _b_supervisor) = socket(wrap_fec(b.clone(), b, false), None);
        let request = b"request";
        let response = b"response";
        assert_eq!(b_write.send(request).await.unwrap(), request.len());
        let mut buf = [0; 64];
        let request_len = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("request receive timed out")
            .expect("request receive failed");
        assert_eq!(&buf[..request_len], request);
        drop(a_read);
        assert_eq!(a_write.send(response).await.unwrap(), response.len());
        let response_len = tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
            .await
            .expect("response receive timed out")
            .expect("read drop falsely reset the RTP write half");
        assert_eq!(&buf[..response_len], response);
    }

    #[tokio::test]
    async fn duplicate_payload_after_read_drop_is_acked_without_reset() {
        #[derive(Debug)]
        struct ProbeUdpWrite {
            socket: Arc<UdpSocket>,
            sent: Arc<Mutex<Vec<Vec<u8>>>>,
            sent_notify: Arc<tokio::sync::Notify>,
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for ProbeUdpWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                let written = UdpSocket::send(&self.socket, buf)
                    .await
                    .map_err(|error| error.kind())?;
                self.sent.lock().unwrap().push(buf.to_vec());
                self.sent_notify.notify_waiters();
                Ok(written)
            }
        }
        async fn wait_for_sends(
            sent: &Mutex<Vec<Vec<u8>>>,
            sent_notify: &tokio::sync::Notify,
            target: usize,
        ) {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let notified = sent_notify.notified();
                    if sent.lock().unwrap().len() >= target {
                        return;
                    }
                    notified.await;
                }
            })
            .await
            .expect("RTP writer did not emit the expected datagram");
        }
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_sent = Arc::new(Mutex::new(Vec::new()));
        let a_sent_notify = Arc::new(tokio::sync::Notify::new());
        let b_sent = Arc::new(Mutex::new(Vec::new()));
        let b_sent_notify = Arc::new(tokio::sync::Notify::new());
        let (mut a_read, mut a_write, _a_supervisor) = socket(
            wrap_fec(
                Arc::clone(&a),
                ProbeUdpWrite {
                    socket: Arc::clone(&a),
                    sent: Arc::clone(&a_sent),
                    sent_notify: Arc::clone(&a_sent_notify),
                },
                false,
            ),
            None,
        );
        let (mut b_read, mut b_write, _b_supervisor) = socket(
            wrap_fec(
                Arc::clone(&b),
                ProbeUdpWrite {
                    socket: Arc::clone(&b),
                    sent: Arc::clone(&b_sent),
                    sent_notify: Arc::clone(&b_sent_notify),
                },
                false,
            ),
            None,
        );
        let request = b"request";
        assert_eq!(b_write.send(request).await.unwrap(), request.len());
        wait_for_sends(&b_sent, &b_sent_notify, 1).await;
        let mut buf = [0; 64];
        let request_len = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("request receive timed out")
            .expect("request receive failed");
        assert_eq!(&buf[..request_len], request);
        wait_for_sends(&a_sent, &a_sent_notify, 1).await;
        drop(a_read);
        tokio::task::yield_now().await;
        let duplicate = b_sent.lock().unwrap()[0].clone();
        let sends_before_duplicate = a_sent.lock().unwrap().len();
        b.send(&duplicate).await.unwrap();
        wait_for_sends(&a_sent, &a_sent_notify, sends_before_duplicate + 1).await;
        let response = b"response";
        assert_eq!(a_write.send(response).await.unwrap(), response.len());
        let response_len = tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
            .await
            .expect("response receive timed out")
            .expect("duplicate payload falsely reset the RTP session");
        assert_eq!(&buf[..response_len], response);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelling_public_send_does_not_cancel_driver_io() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        #[derive(Debug)]
        struct PendingRead;

        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for PendingRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                std::future::pending().await
            }
        }

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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
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
            PendingRead,
            DriverWrite {
                state: Arc::clone(&state),
            },
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

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_is_callable_while_poll_write_retains_the_write_socket() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct PendingRead;
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for PendingRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                std::future::pending().await
            }
        }
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
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
        let layer = wrap_fec(PendingRead, BlockingFirstWrite(Arc::clone(&state)), false);
        let (_read, write, _supervisor) = socket(layer, None);
        let mut write = write.into_async_write();
        let payload = vec![7; write.max_stage()];
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
            "test did not leave PollWrite owning a pending WriteSocket"
        );
        write.send_kill_and_abort().await;
        state.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), state.kill_started.notified())
            .await
            .expect("driver did not observe the out-of-band abort request");
    }
}
