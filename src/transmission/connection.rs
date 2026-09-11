use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::ack_feedback::{AckFeedback, ReceivedAckWork};
use super::coordination::Signals;
use super::observability::ConnectionObservability;
use super::post_open_recovery::PostOpenRecovery;
use super::read_half::ReadHalf;
use super::termination::{KillPolicy, TerminationPresser, TerminationReaper, new_termination};
use super::transmission_layer::{LogConfig, PRINT_DEBUG_MSGS, UnreliableLayer};
use super::watchdog_tuning::WatchdogTuning;
use super::write_half::{WriteHalf, WriteHalfSettings};

use crate::io_err::IoErr;
use crate::metrics::{
    MetricsEvent, MetricsInterest, MetricsSendDriverResumeSource, MetricsTermination,
    MetricsTerminationCause,
};
use crate::reliable::reliable_layer::ReliableLayer;
use crate::traffic_shaping::control::handshake::{DueResponse, PostOpenVerdict};
use crate::traffic_shaping::core::SendWake;
use crate::traffic_shaping::redundancy::fec::FecStatsHandle;

#[derive(Debug, Default)]
pub(crate) struct ReceivedBatch {
    pending_acks: usize,
    fin_ack: bool,
    echo_ts: Option<u32>,
    recv_fin: bool,
    recv_eof: bool,
}

impl ReceivedBatch {
    pub(crate) fn record_ack(&mut self, fin_ack: bool, echo_ts: Option<u32>) {
        self.pending_acks += 1;
        self.fin_ack |= fin_ack;
        if echo_ts.is_some() {
            self.echo_ts = echo_ts;
        }
    }

    pub(crate) fn record_inserted_fin(&mut self) {
        self.recv_fin = true;
    }

    pub(crate) fn record_eof(&mut self, recv_eof: bool) {
        self.recv_eof |= recv_eof;
    }
}

#[derive(Debug)]
pub struct Connection {
    reliable_layer: Mutex<ReliableLayer>,
    /// `ReliableLayer::max_data_size_per_pkt`, captured at construction.  It
    /// is fixed for the connection's life (derived from the MSS), so the
    /// receive hot path reads it without taking the reliable-layer lock.
    max_data_size_per_pkt: usize,
    ack_feedback: Arc<AckFeedback>,
    post_open_recovery: PostOpenRecovery,
    /// Session tag authenticating codec control-plane datagrams; `None` on
    /// connections opened without the handshake.
    session_tag: Option<u64>,
    fec_stats: Option<FecStatsHandle>,
    termination: TerminationPresser,
    signals: Signals,
    clock_epoch: Instant,
    observability: ConnectionObservability,
}

pub fn new_connection(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper) {
    new_connection_inner(unreliable_layer, log_config, None)
}

pub fn new_connection_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper) {
    new_connection_inner(unreliable_layer, log_config, Some(tuning))
}

fn new_connection_inner(
    mut unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    watchdog_tuning: Option<WatchdogTuning>,
) -> (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper) {
    let now = Instant::now();
    let (fec_encoder, fec_decoder, fec_stats) = match unreliable_layer.fec.take() {
        Some(fec) => {
            let (encoder, decoder, stats) = fec.into_actor_parts();
            (Some(encoder), Some(decoder), Some(stats))
        }
        None => (None, None, None),
    };
    let frame_delivery = unreliable_layer.frame_delivery;
    let metrics_observer = unreliable_layer.metrics_observer.clone();
    let (mut reliable_layer, send_rate_limiter) = match watchdog_tuning {
        Some(tuning) => ReliableLayer::new_with_watchdog_tuning_at(
            unreliable_layer.mss,
            frame_delivery,
            now,
            unreliable_layer.initial_sequences,
            tuning,
        ),
        None => ReliableLayer::new_at(
            unreliable_layer.mss,
            frame_delivery,
            now,
            unreliable_layer.initial_sequences,
        ),
    };
    if let Some(initial_rtt) = unreliable_layer.initial_rtt {
        reliable_layer.sample_rtt(initial_rtt, now);
    }
    let observability = ConnectionObservability::new(now, log_config, metrics_observer);
    reliable_layer.set_congestion_metrics_enabled(observability.enabled());
    let max_data_size_per_pkt = reliable_layer.max_data_size_per_pkt();
    let (termination, termination_writer, termination_reaper) = new_termination();
    let post_open_recovery = PostOpenRecovery::new(unreliable_layer.post_open_handshake);
    let ack_feedback = Arc::new(AckFeedback::new());
    let shared = Arc::new(Connection {
        reliable_layer: Mutex::new(reliable_layer),
        max_data_size_per_pkt,
        ack_feedback: Arc::clone(&ack_feedback),
        post_open_recovery,
        session_tag: unreliable_layer.session_tag,
        fec_stats,
        termination,
        signals: Signals::new(),
        clock_epoch: now,
        observability,
    });
    let write_half = WriteHalf::new(
        unreliable_layer.utp_write,
        fec_encoder,
        send_rate_limiter,
        ack_feedback,
        Arc::clone(&shared),
        termination_writer,
        WriteHalfSettings {
            fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
            instream_group_fec_enabled: unreliable_layer.instream_group_fec,
            retransmission_armor: unreliable_layer.retransmission_armor,
            mss: unreliable_layer.mss,
            ack_padding: unreliable_layer.ack_padding,
        },
    );
    let read_half = ReadHalf::new(unreliable_layer.utp_read, fec_decoder, Arc::clone(&shared));
    (shared, write_half, read_half, termination_reaper)
}

impl Connection {
    /// Capture a transport snapshot under the reliable-layer lock, overlaying
    /// the connection-owned FEC counters (the reliable layer itself does not
    /// own FEC actor state, so `ReliableLayer::metrics_at` reports
    /// `fec_counters: None`).
    fn metrics_snapshot(
        &self,
        reliable_layer: &ReliableLayer,
        now: Instant,
    ) -> crate::metrics::MetricsSnapshot {
        let mut snapshot = reliable_layer.metrics_at(now);
        snapshot.fec_counters = self
            .fec_stats
            .as_ref()
            .map(FecStatsHandle::metrics_counters);
        snapshot
    }

    pub fn resume_send(&self) -> &tokio::sync::Notify {
        self.signals.resume_send()
    }

    /// Run `use_layer` against the reliable layer under its lock; the guard never
    /// escapes the closure, so it cannot survive an await. Logging never locks
    /// the reliable layer (see [`Self::log_at`]), so it is safe to call from
    /// inside this closure.
    pub(super) fn with_reliable_layer<R>(&self, use_layer: impl FnOnce(&ReliableLayer) -> R) -> R {
        use_layer(&self.reliable_layer.lock().unwrap())
    }

    /// Run `use_layer` against the reliable layer under its lock; the guard never
    /// escapes the closure, so it cannot survive an await. Logging never locks
    /// the reliable layer (see [`Self::log_at`]), so it is safe to call from
    /// inside this closure.
    pub(super) fn with_reliable_layer_mut<R>(
        &self,
        use_layer: impl FnOnce(&mut ReliableLayer) -> R,
    ) -> R {
        use_layer(&mut self.reliable_layer.lock().unwrap())
    }

    pub(crate) fn request_send_driver_resume(&self, source: MetricsSendDriverResumeSource) {
        self.log(MetricsEvent::SendDriverResumeRequest(source));
        self.signals.resume_send().notify_one();
    }

    /// ACK-flush owner shared between the read half (recording) and the write
    /// half (claims).  Test-only accessor; production flows stay behind the
    /// Connection facades.
    #[cfg(test)]
    pub(super) fn ack_feedback_for_test(&self) -> &AckFeedback {
        &self.ack_feedback
    }

    pub(crate) fn publish_data_sent(&self) {
        self.signals.sent_data_pkt().notify_waiters();
    }

    pub(crate) fn publish_data_received(&self) {
        self.signals.recv_data_pkt().notify_waiters();
    }

    pub(crate) fn publish_packet_acknowledged(&self) {
        self.signals.sent_pkt_acked().notify_waiters();
    }

    pub(crate) fn notify_session_outbound_progress(&self) {
        self.signals.session_outbound_progress().notify_one();
    }

    pub(crate) fn frame_delivery_enabled(&self) -> bool {
        self.reliable_layer.lock().unwrap().frame_delivery_enabled()
    }

    pub(crate) fn is_send_buf_empty(&self) -> bool {
        self.reliable_layer.lock().unwrap().is_send_buf_empty()
    }

    pub(crate) fn write_unit_capacity(&self) -> usize {
        self.reliable_layer.lock().unwrap().write_unit_capacity()
    }

    /// Whether the bounded receive window is at capacity; delegates to the
    /// reliable layer's recv-space occupancy predicate. The read-closed
    /// session half uses this as the memory-saturation signal: it keeps
    /// buffering and ACKing until the window fills, then terminates.
    pub(crate) fn recv_window_full(&self) -> bool {
        self.reliable_layer.lock().unwrap().recv_window_full()
    }

    pub(crate) fn max_data_size_per_pkt(&self) -> usize {
        self.max_data_size_per_pkt
    }
    #[cfg(test)]
    pub(crate) fn send_data_buf_capacity_for_test(&self) -> usize {
        self.reliable_layer.lock().unwrap().send_data_buf_capacity()
    }

    #[cfg(test)]
    pub(crate) fn reliable_layer_for_test(&self) -> &Mutex<ReliableLayer> {
        &self.reliable_layer
    }

    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.fec_stats
            .as_ref()
            .map(FecStatsHandle::recovered_symbols)
    }

    /// Test-only: the sender-side parity-sent counter from the shared FEC
    /// stats, so socket-level tests can assert that parity actually flowed
    /// (a wired-but-inert condition gate would fail such an assertion).
    #[cfg(test)]
    pub(crate) fn fec_parity_sent_for_test(&self) -> Option<u64> {
        self.fec_stats
            .as_ref()
            .map(|stats| stats.metrics_counters().parity_sent)
    }

    pub fn check_error(&self) -> Result<(), IoErr> {
        self.termination.check_error()
    }

    pub(crate) fn has_error(&self) -> bool {
        self.termination.has_error()
    }

    /// Render the first terminal error with its proactive-termination
    /// context (when it was the trigger).
    pub(crate) fn io_error(&self, error: IoErr) -> std::io::Error {
        self.termination.io_error(error)
    }

    #[cfg(test)]
    pub(crate) fn terminal_is_cancelled(&self) -> bool {
        self.termination.terminal().is_cancelled()
    }

    /// The terminal cancellation token: fires exactly when the first
    /// terminal error is pressed. Lets the socket-level session supervisor
    /// distinguish "no terminal event yet" (park indefinitely, a healthy
    /// session) from "terminal event, only the best-effort KILL datagram
    /// outstanding on possibly-stuck underlay" (bound the remaining wait).
    pub(crate) fn terminal(&self) -> &tokio_util::sync::CancellationToken {
        self.termination.terminal()
    }

    pub(crate) fn request_kill_and_abort(&self, cause: MetricsTerminationCause) {
        self.press_broken_pipe(KillPolicy::SendKill, None, cause);
    }

    pub(crate) fn press_error(&self, error: IoErr, cause: MetricsTerminationCause) -> bool {
        let inserted = self.termination.press_error(error);
        if inserted {
            let now = Instant::now();
            let event = MetricsEvent::SessionTermination(MetricsTermination {
                cause,
                error_kind: error.kind(),
                raw_os_error: error.raw_os_error(),
            });
            if self.wants_snapshot(event, now) {
                let snapshot = self.capture_snapshot(now);
                self.log_at_with_snapshot(event, now, snapshot);
            }
        }
        inserted
    }

    pub(crate) fn press_broken_pipe(
        &self,
        policy: KillPolicy,
        context: Option<super::transmission_layer::ProactiveTerminationContext>,
        cause: MetricsTerminationCause,
    ) -> bool {
        let inserted = self.termination.press_broken_pipe(policy, context);
        if inserted {
            let now = Instant::now();
            let event = MetricsEvent::SessionTermination(MetricsTermination {
                cause,
                error_kind: std::io::ErrorKind::BrokenPipe,
                raw_os_error: None,
            });
            if self.wants_snapshot(event, now) {
                let snapshot = self.capture_snapshot(now);
                self.log_at_with_snapshot(event, now, snapshot);
            }
        }
        inserted
    }

    pub async fn send(&self, data: &[u8]) -> Result<usize, IoErr> {
        self.termination.check_error()?;
        if data.is_empty() {
            self.reliable_layer.lock().unwrap().ensure_write_open()?;
            return Ok(0);
        }
        let result = if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            self.send_frame(data).await
        } else {
            self.send_bytes(data).await
        };
        self.termination.check_error()?;
        result
    }

    async fn send_bytes(&self, data: &[u8]) -> Result<usize, IoErr> {
        let now = Instant::now();
        let sent_data_pkt = self.signals.sent_data_pkt().notified();
        tokio::pin!(sent_data_pkt);
        loop {
            self.termination.check_error()?;
            sent_data_pkt.as_mut().enable();
            let (written_bytes, should_resume_send) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let stage_was_empty = reliable_layer.is_send_buf_empty();
                let written_bytes = reliable_layer.send_data_buf(data, now)?;
                if crate::debug::debug_send() {
                    eprintln!(
                        "[app-send] conn={:x} written={} data_len={}",
                        self as *const Connection as usize,
                        written_bytes,
                        data.len()
                    );
                }
                (written_bytes, stage_was_empty && written_bytes > 0)
            };
            let now = Instant::now();
            if self.wants_snapshot(MetricsEvent::SendDataBuffer, now) {
                let snapshot = self.capture_snapshot(now);
                self.log_at_with_snapshot(MetricsEvent::SendDataBuffer, now, snapshot);
            }
            if 0 < written_bytes {
                if should_resume_send {
                    self.request_send_driver_resume(MetricsSendDriverResumeSource::ApplicationData);
                }
                return Ok(written_bytes);
            }
            self.termination.check_error()?;
            {
                let _waiter = self
                    .reliable_layer
                    .lock()
                    .unwrap()
                    .application_write_waiter();
                tokio::select! {
                    () = &mut sent_data_pkt => (),
                    () = self.termination.terminal().cancelled() => (),
                }
            }
            self.termination.check_error()?;
            sent_data_pkt.set(self.signals.sent_data_pkt().notified());
        }
    }

    pub async fn send_frame(&self, frame: &[u8]) -> Result<usize, IoErr> {
        let now = Instant::now();
        let frame_len = frame.len();
        let sent_data_pkt = self.signals.sent_data_pkt().notified();
        tokio::pin!(sent_data_pkt);
        loop {
            self.termination.check_error()?;
            // Arm the notification before inspecting capacity so a wake
            // between the check and the await is never lost.
            sent_data_pkt.as_mut().enable();
            let result = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_frame_buf(frame, now)
            };
            match result {
                Ok(()) => {
                    let now = Instant::now();
                    if self.wants_snapshot(MetricsEvent::SendFrameBuffer, now) {
                        let snapshot = self.capture_snapshot(now);
                        self.log_at_with_snapshot(MetricsEvent::SendFrameBuffer, now, snapshot);
                    }
                    self.request_send_driver_resume(
                        MetricsSendDriverResumeSource::ApplicationFrame,
                    );
                    return Ok(frame_len);
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    self.termination.check_error()?;
                    {
                        // Register the blocked writer for the duration of the
                        // wait (drop-scoped; see `send_bytes`).
                        let _waiter = self
                            .reliable_layer
                            .lock()
                            .unwrap()
                            .application_write_waiter();
                        tokio::select! {
                            () = &mut sent_data_pkt => (),
                            () = self.termination.terminal().cancelled() => (),
                        }
                    }
                    self.termination.check_error()?;
                    sent_data_pkt.set(self.signals.sent_data_pkt().notified());
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn send_fin_buf(&self) {
        self.reliable_layer.lock().unwrap().send_fin_buf();
        self.request_send_driver_resume(MetricsSendDriverResumeSource::ApplicationFinish);
    }

    pub fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        self.signals.recv_fin()
    }

    #[cfg(test)]
    pub fn recv_eof(&self) -> &tokio_util::sync::CancellationToken {
        self.signals.recv_eof()
    }

    pub(crate) fn session_tag(&self) -> Option<u64> {
        self.session_tag
    }

    pub(crate) fn observe_post_open_handshake(
        &self,
        datagram: &[u8],
        now: Instant,
    ) -> PostOpenVerdict {
        self.post_open_recovery.observe(datagram, now)
    }

    pub(crate) fn claim_post_open_response(&self, now: Instant) -> Option<DueResponse> {
        self.post_open_recovery.claim_response(now)
    }

    pub(crate) fn retry_post_open_response(&self, now: Instant) {
        self.post_open_recovery.retry_response(now);
    }

    pub(crate) fn next_send_wake(&self, now: Instant) -> SendWake {
        let (mut protocol_deadline, pacing_deadline) = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            (
                reliable_layer
                    .pkt_send_space()
                    .next_poll_time(now, reliable_layer.is_send_buf_empty()),
                reliable_layer.next_pacing_deadline(now),
            )
        };
        if let Some(handshake_deadline) = self.post_open_recovery.next_send_time(now) {
            protocol_deadline = Some(protocol_deadline.map_or(handshake_deadline, |current| {
                current.min(handshake_deadline)
            }));
        }
        SendWake::after_send_pass(now, pacing_deadline, protocol_deadline)
    }

    pub(crate) fn wire_ts(&self, now: Instant) -> u32 {
        let us = now.duration_since(self.clock_epoch).as_micros();
        us as u32
    }

    pub async fn no_data_to_send(&self) -> Result<(), IoErr> {
        let sent_pkt_acked = self.signals.sent_pkt_acked().notified();
        tokio::pin!(sent_pkt_acked);
        loop {
            self.termination.check_error()?;
            // Arm the notification before inspecting the send buffer so an
            // ack between the check and the await is never lost.
            sent_pkt_acked.as_mut().enable();
            if self.reliable_layer.lock().unwrap().is_no_data_to_send() {
                return Ok(());
            }
            tokio::select! {
                () = &mut sent_pkt_acked => (),
                () = self.termination.terminal().cancelled() => (),
            }
            sent_pkt_acked.set(self.signals.sent_pkt_acked().notified());
        }
    }

    pub(crate) async fn session_outbound_drained(&self) -> Result<(), IoErr> {
        loop {
            let progress = self.signals.session_outbound_progress().notified();
            self.termination.check_error()?;
            let reliable_drained = self.reliable_layer.lock().unwrap().is_no_data_to_send();
            let ack_drained = self.ack_feedback.is_drained();
            if reliable_drained && ack_drained {
                return Ok(());
            }
            tokio::select! {
                () = progress => (),
                () = self.termination.terminal().cancelled() => (),
            }
        }
    }

    pub async fn send_buf_empty(&self) -> Result<(), IoErr> {
        let sent_data_pkt = self.signals.sent_data_pkt().notified();
        tokio::pin!(sent_data_pkt);
        loop {
            self.termination.check_error()?;
            // Arm the notification before inspecting the buffer so a wake
            // between the check and the await is never lost.
            sent_data_pkt.as_mut().enable();
            if self.reliable_layer.lock().unwrap().is_send_buf_empty() {
                return Ok(());
            }
            tokio::select! {
                () = &mut sent_data_pkt => (),
                () = self.termination.terminal().cancelled() => (),
            }
            sent_data_pkt.set(self.signals.sent_data_pkt().notified());
        }
    }

    pub(crate) fn commit_received_batch(&self, batch: ReceivedBatch) {
        let ack_work_added = batch.pending_acks > 0 || batch.fin_ack;
        let schedule_changed = if ack_work_added {
            self.ack_feedback.record(ReceivedAckWork {
                pending_acks: batch.pending_acks,
                fin_ack: batch.fin_ack,
                echo_ts: batch.echo_ts,
            })
        } else {
            false
        };
        if schedule_changed {
            self.log(MetricsEvent::SendDriverResumeRequest(
                MetricsSendDriverResumeSource::AckFlush,
            ));
        }
        if batch.recv_fin {
            self.signals.recv_fin().cancel();
        }
        self.publish_recv_eof(batch.recv_eof);
        self.signals.session_outbound_progress().notify_one();
    }

    fn publish_recv_eof(&self, recv_eof: bool) {
        if recv_eof && !self.signals.recv_eof().is_cancelled() {
            self.signals.recv_eof().cancel();
            if let Some(fec_stats) = self.fec_stats.as_ref() {
                fec_stats.debug_print();
            }
        }
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, IoErr> {
        if data.is_empty() {
            return Ok(0);
        }
        if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let recv_data_pkt = self.signals.recv_data_pkt().notified();
        tokio::pin!(recv_data_pkt);
        let read_bytes = loop {
            self.termination.check_error()?;
            if self.signals.recv_eof().is_cancelled() {
                return Ok(0);
            }
            // Arm the notification before inspecting the receive buffer so
            // a delivery between the check and the await is never lost.
            recv_data_pkt.as_mut().enable();
            let (read_bytes, recv_eof) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let read_bytes = reliable_layer.recv_data_buf(data);
                if crate::debug::debug_send() {
                    eprintln!(
                        "[app-recv] conn={:x} read_bytes={}",
                        self as *const Connection as usize, read_bytes
                    );
                }
                (read_bytes, reliable_layer.recv_eof_ready())
            };
            self.publish_recv_eof(recv_eof);
            let now = Instant::now();
            if self.wants_snapshot(MetricsEvent::ReceiveDataBuffer, now) {
                let snapshot = self.capture_snapshot(now);
                self.log_at_with_snapshot(MetricsEvent::ReceiveDataBuffer, now, snapshot);
            }
            if PRINT_DEBUG_MSGS {
                println!("recv: data: {read_bytes}");
            }
            if 0 < read_bytes {
                break read_bytes;
            }
            if recv_eof {
                continue;
            }
            tokio::select! {
                () = &mut recv_data_pkt => (),
                () = self.termination.terminal().cancelled() => (),
            }
            recv_data_pkt.set(self.signals.recv_data_pkt().notified());
        };
        Ok(read_bytes)
    }

    pub async fn recv_frame(&self) -> Result<Option<Vec<u8>>, IoErr> {
        let recv_data_pkt = self.signals.recv_data_pkt().notified();
        tokio::pin!(recv_data_pkt);
        loop {
            self.termination.check_error()?;
            // Arm the notification before inspecting the frame buffer so a
            // delivery between the check and the await is never lost.
            recv_data_pkt.as_mut().enable();
            let (res, recv_eof) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let res = reliable_layer.recv_frame_buf();
                (res, reliable_layer.recv_eof_ready())
            };
            self.publish_recv_eof(recv_eof);
            match res {
                Ok(Some(frame)) => {
                    let now = Instant::now();
                    if self.wants_snapshot(MetricsEvent::ReceiveFrameBuffer, now) {
                        let snapshot = self.capture_snapshot(now);
                        self.log_at_with_snapshot(MetricsEvent::ReceiveFrameBuffer, now, snapshot);
                    }
                    return Ok(Some(frame));
                }
                Ok(None) => {
                    return Ok(None);
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    tokio::select! {
                        () = &mut recv_data_pkt => (),
                        () = self.termination.terminal().cancelled() => (),
                    }
                    recv_data_pkt.set(self.signals.recv_data_pkt().notified());
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn sample_rtt(&self, rtt: std::time::Duration, now: Instant) {
        let elapsed = self
            .observability
            .enabled()
            .then(|| self.observability.elapsed_since(now));
        let observer_interest = elapsed
            .map(|elapsed| {
                self.observability
                    .interest(MetricsEvent::RttSample, elapsed)
            })
            .unwrap_or(MetricsInterest::Skip);
        let enabled = observer_interest != MetricsInterest::Skip || self.observability.has_logger();
        let captured = {
            let mut reliable_layer = self.reliable_layer.lock().unwrap();
            reliable_layer.sample_rtt(rtt, now);
            enabled.then(|| {
                let snapshot = (observer_interest == MetricsInterest::Snapshot
                    || self.observability.has_logger())
                .then(|| self.metrics_snapshot(&reliable_layer, now));
                let event_index = self.observability.next_event_index();
                (event_index, snapshot)
            })
        };
        if let Some((event_index, snapshot)) = captured {
            self.observability.publish(
                event_index,
                MetricsEvent::RttSample,
                Some(rtt),
                elapsed.expect("enabled metrics capture includes elapsed time"),
                snapshot,
                observer_interest,
            );
        }
    }

    pub(crate) fn log(&self, event: MetricsEvent) {
        self.log_at(event, Instant::now());
    }

    /// Log an event against a caller-supplied decision time: the clock is
    /// never re-sampled for a decision already made at the pass time, so
    /// `elapsed` (and every deadline derived from it) matches the moment the
    /// decision was made, not the moment the metrics call happened to run.
    ///
    /// Never locks the reliable layer: the metrics snapshot is caller-supplied
    /// (see [`Self::log_at_with_snapshot`]), so this is safe to call from
    /// inside a `with_reliable_layer` closure. The deadlock class where
    /// logging re-locked the reliable layer is eliminated by construction.
    pub(crate) fn log_at(&self, event: MetricsEvent, now: Instant) {
        self.publish_event(event, now, None);
    }

    /// Whether `event` at `now` warrants a reliable-layer snapshot. Consults
    /// the observer's filter exactly once (its event counters and
    /// state-sample claim are side effects) and claims the state-sample slot
    /// if one is due. Returns true iff the caller should capture a snapshot
    /// and publish it via [`Self::log_at_with_snapshot`].
    ///
    /// Never locks the reliable layer, so this is safe to call from inside a
    /// `with_reliable_layer` closure. The snapshot itself is captured by the
    /// caller (from the held `&ReliableLayer` inside the lock, or via
    /// [`Self::capture_snapshot`] outside it).
    pub(crate) fn wants_snapshot(&self, event: MetricsEvent, now: Instant) -> bool {
        if !self.observability.enabled() {
            return false;
        }
        let elapsed = self.observability.elapsed_since(now);
        self.observability.wants_snapshot(event, elapsed)
    }

    /// Publish a metrics event with a caller-supplied reliable-layer snapshot.
    /// Never locks the reliable layer, so it is safe to call from inside a
    /// `with_reliable_layer` closure — the caller captures the snapshot from
    /// the `&ReliableLayer` it already holds (or via [`Self::capture_snapshot`]
    /// when outside the lock).
    pub(crate) fn log_at_with_snapshot(
        &self,
        event: MetricsEvent,
        now: Instant,
        snapshot: crate::metrics::MetricsSnapshot,
    ) {
        self.publish_event(event, now, Some(snapshot));
    }

    /// Capture a reliable-layer metrics snapshot at `now`. Locks the reliable
    /// layer — call this only OUTSIDE a `with_reliable_layer` closure (inside
    /// the lock, capture from the held `&ReliableLayer` via
    /// [`Self::metrics_snapshot`] instead).
    pub(crate) fn capture_snapshot(&self, now: Instant) -> crate::metrics::MetricsSnapshot {
        let reliable_layer = self.reliable_layer.lock().unwrap();
        self.metrics_snapshot(&reliable_layer, now)
    }

    fn publish_event(
        &self,
        event: MetricsEvent,
        now: Instant,
        snapshot: Option<crate::metrics::MetricsSnapshot>,
    ) {
        if !self.observability.enabled() {
            return;
        }
        let elapsed = self.observability.elapsed_since(now);
        let observer_interest = if snapshot.is_some() {
            // The caller captured a snapshot because the observer or logger
            // wanted one; the observer's filter was already consulted (via
            // [`Self::wants_snapshot`]), so do not re-consult it — that would
            // re-claim the observer's state-sample slot and double-count its
            // event counters.
            MetricsInterest::Snapshot
        } else {
            let interest = self.observability.interest(event, elapsed);
            if interest == MetricsInterest::Snapshot {
                // The observer wants a snapshot but none was supplied: the
                // logging path never locks, so one cannot be conjured here.
                // Downgrade to event-only rather than drop the event.
                MetricsInterest::EventOnly
            } else {
                interest
            }
        };
        if observer_interest == MetricsInterest::Skip && !self.observability.has_logger() {
            return;
        }
        if snapshot.is_none() && self.observability.has_logger() {
            // The CSV logger needs a snapshot for every row, and the logging
            // path never locks, so one cannot be conjured here. Drop the
            // event rather than panic the logger; snapshot-worthy call sites
            // capture via [`Self::wants_snapshot`] + [`Self::capture_snapshot`]
            // and publish through [`Self::log_at_with_snapshot`].
            return;
        }
        let event_index = self.observability.next_event_index();
        self.observability.publish(
            event_index,
            event,
            None,
            elapsed,
            snapshot,
            observer_interest,
        );
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use crate::delivery::frame::FrameMode;
    use crate::delivery::frame::send::MAX_FRAME_LEN;
    use crate::metrics::{
        MetricsEvent, MetricsInterest, MetricsObserver, MetricsSendDriverResumeSource,
        MetricsTerminationCause, SCHEMA_VERSION,
    };
    use crate::obfuscate::padding::AckPaddingMode;
    use crate::traffic_shaping::core::SendWake;
    use crate::traffic_shaping::redundancy::RetransmissionArmorConfig;
    use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
    use crate::transmission::test_doubles::{BlockingWrite, PendingRead};
    use crate::transmission::transmission_layer::UnreliableLayer;

    use super::{new_connection, new_connection_inner};

    fn pending_layer(frame_delivery: FrameMode) -> UnreliableLayer {
        UnreliableLayer {
            utp_read: Box::new(PendingRead),
            utp_write: Box::new(BlockingWrite::new()),
            post_open_handshake: None,
            session_tag: None,
            initial_sequences: crate::sequence::InitialSequences::ZERO,
            initial_rtt: None,
            metrics_observer: None,
            mss: crate::mss::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            fec: None,
            fec_tuning: FecTuning::default(),
            frame_delivery,
            retransmission_armor: RetransmissionArmorConfig::disabled(),
            instream_group_fec: false,
            ack_padding: AckPaddingMode::None,
        }
    }

    /// The receive hot path reads `max_data_size_per_pkt` once per payload.
    /// It must not take the reliable-layer lock for a value fixed at
    /// construction.  Hold the lock on this thread and call the accessor from
    /// another thread: the cached read returns, the locking version blocks.
    #[test]
    fn max_data_size_per_pkt_reads_without_taking_the_reliable_layer_lock() {
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        let expected = shared
            .reliable_layer
            .lock()
            .unwrap()
            .max_data_size_per_pkt();
        let guard = shared.reliable_layer.lock().unwrap();

        let shared_for_thread = Arc::clone(&shared);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(shared_for_thread.max_data_size_per_pkt());
        });
        let got = rx.recv_timeout(std::time::Duration::from_secs(2)).expect(
            "max_data_size_per_pkt must not take the reliable-layer lock on the receive hot path",
        );
        assert_eq!(got, expected);
        drop(guard);
    }

    #[test]
    fn opening_rtt_seeds_reliable_recovery() {
        let mut layer = pending_layer(FrameMode::default());
        layer.initial_rtt = Some(std::time::Duration::from_millis(42));
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        assert_eq!(
            shared
                .reliable_layer
                .lock()
                .unwrap()
                .pkt_send_space()
                .smooth_rtt(),
            std::time::Duration::from_millis(42),
            "an opening-handshake RTT sample must seed the sender's recovery timing"
        );
    }

    #[test]
    fn metrics_observer_gets_ordered_state_and_raw_rtt() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let raw_rtt = std::time::Duration::from_millis(42);
        shared.sample_rtt(raw_rtt, Instant::now());
        shared.log(MetricsEvent::SendDataBuffer);
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].schema_version, SCHEMA_VERSION);
        assert_eq!(observations[0].event_index, 0);
        assert_eq!(observations[0].event, MetricsEvent::RttSample);
        assert_eq!(observations[0].raw_rtt_sample, Some(raw_rtt));
        let snapshot = observations[0].snapshot.unwrap();
        assert_eq!(snapshot.smoothed_rtt, raw_rtt);
        assert_eq!(snapshot.congestion_loss_ratio, None);
        assert_eq!(snapshot.congestion_action, None);
        assert!(
            snapshot.retransmission_timeout >= raw_rtt,
            "the RTO must cover the raw RTT sample"
        );
        assert_eq!(
            snapshot.oldest_pipe_packet_age, None,
            "no pipe packet timing on an idle connection"
        );
        assert_eq!(snapshot.maximum_packet_rto_overdue, None);
        assert_eq!(snapshot.rto_deadline_postponements, 0);
        assert_eq!(snapshot.retransmission_active_packets, 0);
        assert_eq!(snapshot.retransmission_ready_packets, 0);
        assert_eq!(
            snapshot.congestion_rtt_floor, None,
            "no controller RTT floor before the first rate sample"
        );
        assert_eq!(
            snapshot.congestion_queue_tolerance, None,
            "no controller queue gate before the first rate sample"
        );
        assert_eq!(
            snapshot.congestion_persistent_queue_for, None,
            "no persistent-queue signal before the first rate sample"
        );
        assert_eq!(snapshot.congestion_persistent_queue_resets, 0);
        assert_eq!(
            snapshot.congestion_delivery_peak_packets_per_second, None,
            "no delivery peak before the first rate sample"
        );
        assert!(snapshot.slow_start);
        assert!(!snapshot.gentle_mode);
        assert!(!snapshot.gentle_draining);
        assert!(!snapshot.queue_building);
        assert!(!snapshot.drain_floor_binding);
        assert!(!snapshot.outage_recovery);
        assert_eq!(snapshot.stall_reason, None);
        assert_eq!(observations[1].event_index, 1);
        assert_eq!(observations[1].event, MetricsEvent::SendDataBuffer);
        assert_eq!(observations[1].raw_rtt_sample, None);
    }

    #[test]
    fn filtered_metrics_observer_skips_snapshot_and_callback() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::filtered(
                |event, _| event == MetricsEvent::RttSample,
                move |observation| observations.lock().unwrap().push(observation),
            )
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        shared.log(MetricsEvent::SendDataBuffer);
        shared.sample_rtt(std::time::Duration::from_millis(20), Instant::now());
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].event_index, 0);
        assert_eq!(observations[0].event, MetricsEvent::RttSample);
    }

    #[test]
    fn event_only_observation_avoids_a_state_snapshot() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::selective(
                |event, _| {
                    if event == MetricsEvent::RttSample {
                        MetricsInterest::EventOnly
                    } else {
                        MetricsInterest::Skip
                    }
                },
                move |observation| observations.lock().unwrap().push(observation),
            )
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let raw_rtt = std::time::Duration::from_millis(20);
        shared.sample_rtt(raw_rtt, Instant::now());
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].raw_rtt_sample, Some(raw_rtt));
        assert_eq!(observations[0].snapshot, None);
    }

    #[test]
    fn log_at_preserves_the_transport_decision_time() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::selective(
                |_, _| MetricsInterest::EventOnly,
                move |observation| observations.lock().unwrap().push(observation),
            )
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let decision_time = shared.clock_epoch + std::time::Duration::from_millis(7);
        shared.log_at(MetricsEvent::SendDataPacketAttempt, decision_time);
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].elapsed, std::time::Duration::from_millis(7));
        assert_eq!(observations[0].event, MetricsEvent::SendDataPacketAttempt);
        assert_eq!(observations[0].snapshot, None);
    }

    #[test]
    fn log_at_inside_the_reliable_layer_lock_never_deadlocks() {
        // The deadlock this guards against: `log_at` used to re-lock the
        // reliable layer for the metrics snapshot, and the mutex is not
        // reentrant, so calling it inside a `with_reliable_layer` closure
        // hung forever whenever observability was enabled. The structural
        // fix removed the lock from the logging path entirely: `log_at` and
        // `log_at_with_snapshot` never lock, so this call completes.
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::selective(
                |_, _| MetricsInterest::Snapshot,
                move |observation| observations.lock().unwrap().push(observation),
            )
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let now = Instant::now();
        // Event-only logging inside the lock: completes, publishes without a
        // snapshot.
        shared.with_reliable_layer(|_| {
            shared.log_at(MetricsEvent::SendDataPacketAttempt, now);
        });
        // Snapshot logging inside the lock: the caller captures the snapshot
        // from the `&ReliableLayer` it already holds and passes it in — no
        // re-lock, so this also completes.
        shared.with_reliable_layer(|reliable_layer| {
            let snapshot = shared.metrics_snapshot(reliable_layer, now);
            shared.log_at_with_snapshot(MetricsEvent::SendDataPacketAttempt, now, snapshot);
        });
        let observations = observations.lock().unwrap();
        assert_eq!(
            observations.len(),
            2,
            "both log calls inside the lock must publish"
        );
        assert!(
            observations[1].snapshot.is_some(),
            "the caller-supplied snapshot must be published"
        );
    }

    #[test]
    fn first_terminal_path_is_observed_once_with_its_owner() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        assert!(shared.press_error(
            crate::io_err::IoErr::new(std::io::ErrorKind::ConnectionReset, Some(54)),
            MetricsTerminationCause::DataWrite,
        ));
        assert!(!shared.press_error(
            std::io::ErrorKind::BrokenPipe.into(),
            MetricsTerminationCause::PeerKill,
        ));
        let observations = observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        let MetricsEvent::SessionTermination(termination) = observations[0].event else {
            panic!("the first terminal path must emit a termination event");
        };
        assert_eq!(termination.cause, MetricsTerminationCause::DataWrite);
        assert_eq!(termination.error_kind, std::io::ErrorKind::ConnectionReset);
        assert_eq!(termination.raw_os_error, Some(54));
        assert!(observations[0].snapshot.is_some());
    }

    #[tokio::test]
    async fn broken_pipe_outranks_full_frame_queue() {
        let layer = pending_layer(FrameMode::enabled());
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let full_frame = vec![0; MAX_FRAME_LEN];
        shared
            .reliable_layer
            .lock()
            .unwrap()
            .send_frame_buf(&full_frame, Instant::now())
            .unwrap();
        let one_byte_frame = [1];
        let mut blocked_send = Box::pin(shared.send_frame(&one_byte_frame));
        tokio::select! {
            result = &mut blocked_send => panic!("full frame queue unexpectedly accepted data: {result:?}"),
            () = tokio::task::yield_now() => (),
        }
        shared
            .termination
            .press_error(std::io::ErrorKind::BrokenPipe.into());
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), blocked_send)
            .await
            .expect("BrokenPipe must wake a sender waiting for frame-queue capacity");
        assert_eq!(result, Err(std::io::ErrorKind::BrokenPipe.into()));
    }

    #[test]
    fn idle_sender_waits_for_an_event_without_a_timer() {
        let now = Instant::now();
        let (shared, _write_half, _read_half, _reaper) =
            new_connection_inner(pending_layer(FrameMode::default()), None, None);
        assert_eq!(shared.next_send_wake(now), SendWake::Event);
    }

    #[test]
    fn pacing_block_uses_a_one_shot_batch_deadline() {
        let now = Instant::now();
        let (shared, write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        let payload = vec![0; crate::udp::NO_FEC_MSS * 2];
        assert!(
            shared
                .reliable_layer
                .lock()
                .unwrap()
                .send_data_buf(&payload, now)
                .unwrap()
                > 0
        );
        write_half.drain_pacer_for_test(usize::MAX, now);
        let SendWake::Pacing(deadline) = shared.next_send_wake(now) else {
            panic!("staged, sendable data must wait on pacing");
        };
        assert!(deadline > now);
    }

    #[test]
    fn congestion_window_block_waits_for_ack_or_protocol_deadline() {
        let now = Instant::now();
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        let mut reliable = shared.reliable_layer.lock().unwrap();
        reliable.set_cwnd_for_test(NonZeroUsize::new(1).unwrap());
        let payload = vec![0; crate::udp::NO_FEC_MSS * 2];
        assert!(reliable.send_data_buf(&payload, now).unwrap() > 0);
        let mut packet = vec![0; crate::udp::NO_FEC_MSS];
        assert!(reliable.send_data_pkt(&mut packet, now).is_some());
        assert!(!reliable.is_send_buf_empty());
        assert!(!reliable.pkt_send_space().accepts_new_pkt());
        drop(reliable);
        assert!(matches!(shared.next_send_wake(now), SendWake::Protocol(_)));
    }

    /// Every application data write must wake the send driver, not only the
    /// stage's empty -> non-empty edge.  A write arriving while the stage is
    /// already non-empty (e.g. between the driver draining the stage and
    /// parking) must still wake the driver; `Notify` coalesces, so the
    /// redundant wake is free.
    #[tokio::test]
    async fn every_application_write_resumes_the_send_driver() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let application_data_requests = || {
            observations
                .lock()
                .unwrap()
                .iter()
                .filter(|observation| {
                    matches!(
                        observation.event,
                        MetricsEvent::SendDriverResumeRequest(
                            MetricsSendDriverResumeSource::ApplicationData
                        )
                    )
                })
                .count()
        };
        assert_eq!(shared.send(b"first").await.unwrap(), b"first".len());
        assert_eq!(
            application_data_requests(),
            1,
            "the first write must resume the send driver"
        );
        assert_eq!(shared.send(b"second").await.unwrap(), b"second".len());
        assert_eq!(
            application_data_requests(),
            2,
            "a write into a nonempty stage must still resume the send driver"
        );
        assert_eq!(shared.send(b"third").await.unwrap(), b"third".len());
        assert_eq!(
            application_data_requests(),
            3,
            "every application write must resume the send driver"
        );
    }

    #[tokio::test]
    async fn application_data_resumes_only_on_empty_to_nonempty_stage() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let mut layer = pending_layer(FrameMode::default());
        layer.metrics_observer = Some(observer);
        let (shared, _write_half, _read_half, _reaper) = new_connection(layer, None);
        let application_data_requests = || {
            observations
                .lock()
                .unwrap()
                .iter()
                .filter(|observation| {
                    matches!(
                        observation.event,
                        MetricsEvent::SendDriverResumeRequest(
                            MetricsSendDriverResumeSource::ApplicationData
                        )
                    )
                })
                .count()
        };
        assert_eq!(shared.send(b"first").await.unwrap(), b"first".len());
        assert_eq!(
            application_data_requests(),
            1,
            "the empty-to-nonempty stage edge must resume the send driver exactly once"
        );
        assert_eq!(shared.send(b"second").await.unwrap(), b"second".len());
        assert_eq!(
            application_data_requests(),
            1,
            "a write into an already-nonempty stage must not resume the send driver again"
        );
        assert_eq!(shared.send(b"third").await.unwrap(), b"third".len());
        assert_eq!(
            application_data_requests(),
            1,
            "further nonempty-stage writes stay silent on the resume request"
        );
    }

    #[tokio::test]
    async fn recv_wakes_when_payload_arrives_after_it_starts_waiting() {
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        let mut buf = [0u8; 64];
        let mut recv = Box::pin(shared.recv(&mut buf));
        // First poll: the buffer is empty, so `recv` arms the notification
        // and parks. (Previously the future was not registered until the
        // first poll inside the select; a payload published in between the
        // empty-buffer check and that poll was a lost wakeup.)
        assert!(
            tokio::select! {
                result = &mut recv => panic!("recv returned before any payload: {result:?}"),
                () = tokio::task::yield_now() => true,
            },
            "recv must park on an empty receive buffer"
        );
        // The read driver delivers a payload, then publishes the receive
        // wake (read_half's `publish_data_received`).
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(0), None, b"hi");
        }
        shared.publish_data_received();
        let n = tokio::time::timeout(std::time::Duration::from_secs(1), &mut recv)
            .await
            .expect("a published payload must wake the parked receiver")
            .unwrap();
        assert_eq!(n, 2);
        drop(recv);
        assert_eq!(&buf[..2], b"hi");
    }

    #[tokio::test]
    async fn recv_returns_payload_already_buffered_without_waiting() {
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(0), None, b"pre");
        }
        let mut buf = [0u8; 64];
        // No publish at all: the buffer-check-first path must satisfy the
        // call without any notification.
        let n = shared.recv(&mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..3], b"pre");
    }

    #[tokio::test]
    async fn recv_returns_zero_after_eof_is_published() {
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        let mut buf = [0u8; 64];
        let mut recv = Box::pin(shared.recv(&mut buf));
        assert!(
            tokio::select! {
                result = &mut recv => panic!("recv returned before any FIN: {result:?}"),
                () = tokio::task::yield_now() => true,
            },
            "recv must park on an empty receive buffer"
        );
        // Deliver the peer FIN (empty payload at the contiguous seq), then
        // publish the receive wake exactly as the read driver would.
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(0), None, b"");
        }
        shared.publish_data_received();
        let read = tokio::time::timeout(std::time::Duration::from_secs(1), &mut recv)
            .await
            .expect("a published FIN must wake the parked receiver");
        assert_eq!(read.unwrap(), 0, "recv must return Ok(0) at EOF");
    }

    #[tokio::test]
    async fn recv_frame_wakes_when_a_frame_arrives_after_it_starts_waiting() {
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::enabled()), None);
        let mut recv_frame = Box::pin(shared.recv_frame());
        assert!(
            tokio::select! {
                result = &mut recv_frame => panic!("recv_frame returned before any frame: {result:?}"),
                () = tokio::task::yield_now() => true,
            },
            "recv_frame must park on an empty frame buffer"
        );
        // Deliver the whole frame in one packet (frame_len == payload), then
        // publish the receive wake exactly as the read driver would.
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            reliable.recv_data_pkt(
                crate::sequence::SequenceNumber::from_wire(0),
                Some(2),
                b"hi",
            );
        }
        shared.publish_data_received();
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), &mut recv_frame)
            .await
            .expect("a published frame must wake the parked frame receiver");
        assert_eq!(frame.unwrap(), Some(b"hi".to_vec()));
    }

    #[tokio::test]
    async fn no_data_to_send_wakes_when_the_last_in_flight_packet_is_acked() {
        let (shared, _write_half, _read_half, _reaper) =
            new_connection(pending_layer(FrameMode::default()), None);
        let now = Instant::now();
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            assert_eq!(
                reliable.send_data_buf(&[0u8; 100], now).unwrap(),
                100,
                "staging must succeed"
            );
            let mut pkt = vec![0u8; crate::udp::NO_FEC_MSS];
            let sent = reliable
                .send_data_pkt(&mut pkt, now)
                .expect("a packet must go out");
            match sent.data_written {
                crate::reliable::reliable_layer::DataPktPayload::Data(_) => (),
                _ => panic!("expected a data packet"),
            }
            assert!(
                !reliable.is_no_data_to_send(),
                "an in-flight packet must prevent the drain wait from completing"
            );
        }
        let mut drained = Box::pin(shared.no_data_to_send());
        assert!(
            tokio::select! {
                result = &mut drained => panic!("no_data_to_send returned while a packet is in flight: {result:?}"),
                () = tokio::task::yield_now() => true,
            },
            "no_data_to_send must park while a packet is in flight"
        );
        // The read driver acks the in-flight packet and publishes the ack
        // wake exactly as `read_half` does after processing a peer ACK.
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            let next_seq = reliable.pkt_send_space().next_seq();
            let acks = [crate::ack::AckInterval {
                start: crate::sequence::SequenceNumber::ZERO,
                size: core::num::NonZeroU64::new(next_seq.to_wire()).unwrap(),
            }];
            reliable.recv_ack_pkt(crate::ack::AckBlocks::new(next_seq, &acks), now);
        }
        shared.publish_packet_acknowledged();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut drained)
            .await
            .expect("an ack must wake the parked drain wait")
            .expect("no_data_to_send must succeed after the in-flight packet is acked");
    }
}
