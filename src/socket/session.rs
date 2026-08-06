use std::sync::Arc;
use std::{future::Future, pin::Pin, task::Poll};

use tokio::task::{JoinError, JoinSet};

use super::stream::{ConnReader, ConnWriter};

use crate::transmission::{
    connection::{Connection, new_connection, new_connection_with_watchdog_tuning},
    read_half::ReadHalf,
    termination::TerminationReaper,
    transmission_layer::{LogConfig, RecvBufs, SendBufs, SendKillPkt, UnreliableLayer},
    watchdog_tuning::WatchdogTuning,
    write_half::WriteHalf,
};

#[derive(Debug)]
#[must_use = "the RTP session handle must be retained and awaited"]
pub struct SessionHandle {
    tasks: JoinSet<()>,
}

impl Future for SessionHandle {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.tasks).poll_join_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(()))) => Poll::Ready(()),
            Poll::Ready(Some(Err(error))) if error.is_panic() => {
                std::panic::resume_unwind(error.into_panic())
            }
            Poll::Ready(Some(Err(_))) => Poll::Ready(()),
            Poll::Ready(None) => Poll::Ready(()),
        }
    }
}

impl SessionHandle {
    /// An idle session handle with no driver tasks: holding it keeps nothing
    /// alive and dropping it aborts nothing. Used where a session-shaped value
    /// is required without a live transport (e.g. composing lane state in
    /// tests).
    pub fn idle() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }
}

pub fn socket(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (ConnReader, ConnWriter, SessionHandle) {
    build_socket(TransmissionLayer::new(unreliable_layer, log_config))
}

pub fn socket_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (ConnReader, ConnWriter, SessionHandle) {
    build_socket(TransmissionLayer::new_with_watchdog_tuning(
        unreliable_layer,
        log_config,
        tuning,
    ))
}

type SocketParts = (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper);

/// The composed session before the driver tasks are spawned.  Production
/// [`socket`] / [`socket_with_watchdog_tuning`] hand one to [`build_socket`],
/// which spawns the write/read driver tasks; the test facade holds the same
/// composition to poke the write/read halves directly without spawning them.
pub(crate) struct TransmissionLayer {
    pub(crate) shared: Arc<Connection>,
    pub(crate) write_half: WriteHalf,
    pub(crate) read_half: ReadHalf,
    pub(crate) termination_reaper: TerminationReaper,
}

impl std::ops::Deref for TransmissionLayer {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl TransmissionLayer {
    pub(crate) fn from_parts(parts: SocketParts) -> Self {
        let (shared, write_half, read_half, termination_reaper) = parts;
        Self {
            shared,
            write_half,
            read_half,
            termination_reaper,
        }
    }

    pub(crate) fn new(unreliable_layer: UnreliableLayer, log_config: Option<LogConfig>) -> Self {
        Self::from_parts(new_connection(unreliable_layer, log_config))
    }

    pub(crate) fn new_with_watchdog_tuning(
        unreliable_layer: UnreliableLayer,
        log_config: Option<LogConfig>,
        tuning: WatchdogTuning,
    ) -> Self {
        Self::from_parts(new_connection_with_watchdog_tuning(
            unreliable_layer,
            log_config,
            tuning,
        ))
    }

    pub(crate) fn into_parts(self) -> SocketParts {
        let Self {
            shared,
            write_half,
            read_half,
            termination_reaper,
        } = self;
        (shared, write_half, read_half, termination_reaper)
    }
}

fn build_socket(parts: TransmissionLayer) -> (ConnReader, ConnWriter, SessionHandle) {
    let (shared, write_half, read_half, termination_reaper) = parts.into_parts();
    let read_shutdown = tokio_util::sync::CancellationToken::new();
    let write_shutdown = tokio_util::sync::CancellationToken::new();
    let stop_drivers = tokio_util::sync::CancellationToken::new();
    let mut drivers = JoinSet::new();
    drivers.spawn({
        let stop_drivers = stop_drivers.clone();
        async move {
            let mut write_half = write_half;
            let mut send_bufs = SendBufs::new();
            let kill_requested = write_half.kill_requested().clone();
            loop {
                let pass = match write_half.send_pass(&mut send_bufs).await {
                    Ok(pass) => pass,
                    Err(_) => return,
                };
                let resume_send = write_half.resume_send().notified();
                match pass.wake.deadline() {
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
            }
        }
    });
    drivers.spawn({
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
                    Err((_e, SendKillPkt::No)) => {
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
    let mut tasks = JoinSet::new();
    tasks.spawn({
        let read_shutdown = read_shutdown.clone();
        let write_shutdown = write_shutdown.clone();
        let stop_drivers = stop_drivers.clone();
        let shared = Arc::clone(&shared);
        async move {
            let mut drivers = drivers;
            let first_exit = 'session: {
                tokio::select! {
                    () = write_shutdown.cancelled() => { shared.send_fin_buf(); shared.resume_send().notify_one(); }
                    () = termination_reaper.ready() => break 'session None,
                    result = next_driver_exit(&mut drivers) => break 'session Some(result),
                }
                tokio::select! {
                    () = read_shutdown.cancelled() => (),
                    () = termination_reaper.ready() => break 'session None,
                    result = next_driver_exit(&mut drivers) => break 'session Some(result),
                }
                tokio::select! {
                    () = termination_reaper.ready_or_graceful_close(shared.recv_fin(), shared.session_outbound_drained()) => break 'session None,
                    result = next_driver_exit(&mut drivers) => break 'session Some(result),
                }
            };
            stop_drivers.cancel();
            join_drivers(drivers, first_exit, &shared).await
        }
    });
    let supervisor = SessionHandle { tasks };
    let read = ConnReader {
        transmission_layer: Arc::clone(&shared),
        frame_buf: Vec::new(),
        _shutdown_guard: read_shutdown.drop_guard(),
    };
    let write = ConnWriter {
        transmission_layer: Arc::clone(&shared),
        _shutdown_guard: write_shutdown.drop_guard(),
    };
    (read, write, supervisor)
}

async fn next_driver_exit(drivers: &mut JoinSet<()>) -> Result<(), JoinError> {
    drivers.join_next().await.unwrap_or(Ok(()))
}

async fn join_drivers(
    mut drivers: JoinSet<()>,
    first_exit: Option<Result<(), JoinError>>,
    shared: &Connection,
) {
    let unexpected_clean_exit =
        first_exit.as_ref().is_some_and(Result::is_ok) && !shared.termination.has_error();
    let mut panic_payload = None;
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
                Err(_error) => {}
            }
        }
        result = drivers.join_next().await;
        if result.is_none() {
            break;
        }
    }
    if let Some(payload) = panic_payload {
        std::panic::resume_unwind(payload);
    }
    assert!(
        !unexpected_clean_exit,
        "RTP driver task exited without publishing a terminal session state"
    );
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use tokio::net::UdpSocket;

    use crate::io_err::IoErr;
    use crate::transmission::test_doubles::PendingRead;
    use crate::udp::wrap_fec;

    use super::*;
    use core::time::Duration;
    use std::sync::Mutex;

    #[tokio::test]
    async fn supervisor_reaps_immediately_when_terminal_error_has_no_kill() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct FailedRead;
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for FailedRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::ConnectionReset.into())
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                self.sends.fetch_add(1, Ordering::SeqCst);
                Ok(buf.len())
            }
        }
        let sends = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(tokio::sync::Notify::new());
        let layer = wrap_fec(
            Box::new(FailedRead),
            Box::new(DropProbeWrite {
                sends: Arc::clone(&sends),
                dropped: Arc::clone(&dropped),
            }),
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
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                panic!("injected RTP read panic")
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let layer = wrap_fec(
            Box::new(PanickingRead),
            Box::new(DropProbeWrite(Some(dropped_tx))),
            false,
        );
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
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                self.fail.notified().await;
                Err(std::io::ErrorKind::ConnectionReset.into())
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
            Box::new(GatedFailedRead {
                fail: Arc::clone(&fail),
            }),
            Box::new(GatedWrite {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
                dropped: Arc::clone(&dropped),
            }),
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
            Box::new(PendingRead),
            Box::new(KillGateWrite {
                kill_started: Arc::clone(&kill_started),
                release_kill: Arc::clone(&release_kill),
                dropped: Arc::clone(&dropped),
                was_dropped: Arc::clone(&was_dropped),
            }),
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

        let (mut a_read, mut a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (mut b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);

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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                UdpSocket::send(&self.socket, buf)
                    .await
                    .map_err(IoErr::from)
            }
        }
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let (a_read, a_write, _a_supervisor) = socket(
            wrap_fec(
                Box::new(Arc::clone(&a)),
                Box::new(DropProbeUdpWrite {
                    socket: a,
                    dropped: Some(dropped_tx),
                }),
                false,
            ),
            None,
        );
        let (b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
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
        let (a_read, mut a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (mut b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
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
        let (mut a_read, mut a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (mut b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                let written = UdpSocket::send(&self.socket, buf)
                    .await
                    .map_err(IoErr::from)?;
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
                Box::new(Arc::clone(&a)),
                Box::new(ProbeUdpWrite {
                    socket: Arc::clone(&a),
                    sent: Arc::clone(&a_sent),
                    sent_notify: Arc::clone(&a_sent_notify),
                }),
                false,
            ),
            None,
        );
        let (mut b_read, mut b_write, _b_supervisor) = socket(
            wrap_fec(
                Box::new(Arc::clone(&b)),
                Box::new(ProbeUdpWrite {
                    socket: Arc::clone(&b),
                    sent: Arc::clone(&b_sent),
                    sent_notify: Arc::clone(&b_sent_notify),
                }),
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

    #[tokio::test]
    async fn dropping_the_session_handle_aborts_the_driver_children() {
        use crate::transmission::test_doubles::PendingWrite;
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
        let (_read, mut write, supervisor) = socket(layer, None);
        assert_eq!(write.send(b"payload").await.unwrap(), 7);
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("the writer never blocked inside the unreliable send");
        drop(supervisor);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cancelled.load(Ordering::SeqCst) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the session handle did not abort the blocked writer");
    }

    #[tokio::test]
    async fn normal_shutdown_drains_all_driver_children() {
        #[derive(Debug)]
        struct FailedRead;
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableRead for FailedRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }
        }
        #[derive(Debug)]
        struct NoopWrite;
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for NoopWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        let layer = wrap_fec(Box::new(FailedRead), Box::new(NoopWrite), false);
        let (_read, _write, supervisor) = socket(layer, None);
        tokio::time::timeout(Duration::from_secs(1), supervisor)
            .await
            .expect("the supervisor did not drain its driver children on normal shutdown");
    }
}
