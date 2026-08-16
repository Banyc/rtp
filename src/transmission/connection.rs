use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::coordination::Signals;
use super::read_half::ReadHalf;
use super::termination::{KillPolicy, TerminationPresser, TerminationReaper, new_termination};
use super::transmission_layer::{
    LogConfig, MetricsRow, PRINT_DEBUG_MSGS, ReliableLayerLogger, UnreliableLayer,
};
use super::ts_echo::RecentEchoes;
use super::watchdog_tuning::WatchdogTuning;
use super::write_half::WriteHalf;

use crate::io_err::IoErr;
use crate::metrics::{
    MetricsEvent, MetricsInterest, MetricsObservation, MetricsObserver,
    MetricsSendDriverResumeSource, MetricsTermination, MetricsTerminationCause, SCHEMA_VERSION,
};
use crate::reliable::reliable_layer::ReliableLayer;
use crate::traffic_shaping::control::ack_flush::AckFlushState;
use crate::traffic_shaping::control::handshake::{DueResponse, PostOpenHandshake, PostOpenVerdict};
use crate::traffic_shaping::core::{SendPacer, SendWake};
use crate::traffic_shaping::redundancy::fec::FecState;

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
    pub(crate) reliable_layer: Mutex<ReliableLayer>,
    pub(crate) ack_flush: Mutex<AckFlushState>,
    post_open_handshake: Option<Mutex<PostOpenHandshake>>,
    post_open_handshake_active: AtomicBool,
    /// Session tag authenticating codec control-plane datagrams; `None` on
    /// connections opened without the handshake.
    pub(crate) session_tag: Option<u64>,
    pub(crate) fec: Option<Mutex<FecState>>,
    pub(crate) send_rate_limiter: Arc<Mutex<SendPacer>>,
    pub(crate) termination: TerminationPresser,
    pub(crate) signals: Signals,
    pub(crate) rtx_dup: std::sync::atomic::AtomicBool,
    pub(crate) fec_instream_flush: bool,
    pub(crate) instream_group_fec_enabled: std::sync::atomic::AtomicBool,
    pub(crate) clock_epoch: Instant,
    pub(crate) reliable_layer_logger: Option<ReliableLayerLogger>,
    metrics_observer: Option<MetricsObserver>,
    metrics_event_index: AtomicU64,
}

pub fn new_connection(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper) {
    let now = Instant::now();
    let frame_delivery = unreliable_layer.frame_delivery;
    let metrics_observer = unreliable_layer.metrics_observer.clone();
    let (mut reliable_layer, send_rate_limiter) = ReliableLayer::new_at(
        unreliable_layer.mss,
        frame_delivery,
        now,
        unreliable_layer.initial_sequences,
    );
    if let Some(initial_rtt) = unreliable_layer.initial_rtt {
        reliable_layer.sample_rtt(initial_rtt, now);
    }
    // Controller interval accounting is opt-in: it runs only when a metrics
    // observer or a reliable-layer logger exists, so a bare connection pays
    // exactly one predictable branch per controller decision point.
    reliable_layer.congestion_metrics_enabled = metrics_observer.is_some() || log_config.is_some();
    let reliable_layer_logger = log_config.as_ref().map(|c| {
        let file = std::fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&c.reliable_layer_log_path)
            .expect("open log file");
        Mutex::new(csv::WriterBuilder::new().from_writer(file))
    });
    let (termination, termination_writer, termination_reaper) = new_termination();
    let post_open_handshake_active = unreliable_layer.post_open_handshake.is_some();
    let shared = Arc::new(Connection {
        reliable_layer: Mutex::new(reliable_layer),
        ack_flush: Mutex::new(AckFlushState::new()),
        post_open_handshake: unreliable_layer.post_open_handshake.map(Mutex::new),
        post_open_handshake_active: AtomicBool::new(post_open_handshake_active),
        session_tag: unreliable_layer.session_tag,
        fec: unreliable_layer.fec.map(Mutex::new),
        send_rate_limiter,
        termination,
        signals: Signals::new(),
        rtx_dup: std::sync::atomic::AtomicBool::new(unreliable_layer.rtx_dup),
        fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
        instream_group_fec_enabled: std::sync::atomic::AtomicBool::new(
            unreliable_layer.instream_group_fec,
        ),
        clock_epoch: now,
        reliable_layer_logger,
        metrics_observer,
        metrics_event_index: AtomicU64::new(0),
    });
    let write_half = WriteHalf {
        utp_write: unreliable_layer.utp_write,
        shared: Arc::clone(&shared),
        termination_writer,
    };
    let read_half = ReadHalf {
        utp_read: unreliable_layer.utp_read,
        recent_echoes: RecentEchoes::new(),
        shared: Arc::clone(&shared),
    };
    (shared, write_half, read_half, termination_reaper)
}

pub fn new_connection_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper) {
    let now = Instant::now();
    let frame_delivery = unreliable_layer.frame_delivery;
    let metrics_observer = unreliable_layer.metrics_observer.clone();
    let (mut reliable_layer, send_rate_limiter) = ReliableLayer::new_with_watchdog_tuning_at(
        unreliable_layer.mss,
        frame_delivery,
        now,
        unreliable_layer.initial_sequences,
        tuning,
    );
    if let Some(initial_rtt) = unreliable_layer.initial_rtt {
        reliable_layer.sample_rtt(initial_rtt, now);
    }
    // Controller interval accounting is opt-in: it runs only when a metrics
    // observer or a reliable-layer logger exists, so a bare connection pays
    // exactly one predictable branch per controller decision point.
    reliable_layer.congestion_metrics_enabled = metrics_observer.is_some() || log_config.is_some();
    let reliable_layer_logger = log_config.as_ref().map(|c| {
        let file = std::fs::File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&c.reliable_layer_log_path)
            .expect("open log file");
        Mutex::new(csv::WriterBuilder::new().from_writer(file))
    });
    let (termination, termination_writer, termination_reaper) = new_termination();
    let post_open_handshake_active = unreliable_layer.post_open_handshake.is_some();
    let shared = Arc::new(Connection {
        reliable_layer: Mutex::new(reliable_layer),
        ack_flush: Mutex::new(AckFlushState::new()),
        post_open_handshake: unreliable_layer.post_open_handshake.map(Mutex::new),
        post_open_handshake_active: AtomicBool::new(post_open_handshake_active),
        session_tag: unreliable_layer.session_tag,
        fec: unreliable_layer.fec.map(Mutex::new),
        send_rate_limiter,
        termination,
        signals: Signals::new(),
        rtx_dup: std::sync::atomic::AtomicBool::new(unreliable_layer.rtx_dup),
        fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
        instream_group_fec_enabled: std::sync::atomic::AtomicBool::new(
            unreliable_layer.instream_group_fec,
        ),
        clock_epoch: now,
        reliable_layer_logger,
        metrics_observer,
        metrics_event_index: AtomicU64::new(0),
    });
    let write_half = WriteHalf {
        utp_write: unreliable_layer.utp_write,
        shared: Arc::clone(&shared),
        termination_writer,
    };
    let read_half = ReadHalf {
        utp_read: unreliable_layer.utp_read,
        recent_echoes: RecentEchoes::new(),
        shared: Arc::clone(&shared),
    };
    (shared, write_half, read_half, termination_reaper)
}

impl Connection {
    pub fn resume_send(&self) -> &tokio::sync::Notify {
        &self.signals.resume_send
    }
    pub(crate) fn request_send_driver_resume(&self, source: MetricsSendDriverResumeSource) {
        self.log(MetricsEvent::SendDriverResumeRequest(source));
        self.signals.resume_send.notify_one();
    }

    pub fn reliable_layer(&self) -> &Mutex<ReliableLayer> {
        &self.reliable_layer
    }

    pub fn rtx_dup(&self) -> bool {
        self.rtx_dup.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn instream_group_fec_enabled(&self) -> bool {
        self.instream_group_fec_enabled
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.fec
            .as_ref()
            .map(|fec| fec.lock().unwrap().recovered_symbols())
    }

    pub fn check_error(&self) -> Result<(), IoErr> {
        self.termination.check_error()
    }

    pub(crate) fn request_kill_and_abort(&self, cause: MetricsTerminationCause) {
        self.press_broken_pipe(KillPolicy::SendKill, None, cause);
    }

    pub(crate) fn press_error(&self, error: IoErr, cause: MetricsTerminationCause) -> bool {
        let inserted = self.termination.press_error(error);
        if inserted {
            self.log(MetricsEvent::SessionTermination(MetricsTermination {
                cause,
                error_kind: error.kind(),
                raw_os_error: error.raw_os_error(),
            }));
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
            self.log(MetricsEvent::SessionTermination(MetricsTermination {
                cause,
                error_kind: std::io::ErrorKind::BrokenPipe,
                raw_os_error: None,
            }));
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
        let sent_data_pkt = self.signals.sent_data_pkt.notified();
        tokio::pin!(sent_data_pkt);
        loop {
            self.termination.check_error()?;
            // Arm the notification before inspecting capacity so a wake
            // between the check and the await is never lost.
            sent_data_pkt.as_mut().enable();
            let written_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_buf(data, now)
            }?;
            self.log(MetricsEvent::SendDataBuffer);
            if 0 < written_bytes {
                self.request_send_driver_resume(MetricsSendDriverResumeSource::ApplicationData);
                return Ok(written_bytes);
            }
            self.termination.check_error()?;
            {
                // Register the blocked writer for the duration of the wait:
                // the counter is drop-scoped, so a cancelled writer always
                // releases its slot.  Waiting never suppresses
                // application-limited classification (the exported
                // suppression counter stays zero).
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
            sent_data_pkt.set(self.signals.sent_data_pkt.notified());
        }
    }

    pub async fn send_frame(&self, frame: &[u8]) -> Result<usize, IoErr> {
        let now = Instant::now();
        let frame_len = frame.len();
        let sent_data_pkt = self.signals.sent_data_pkt.notified();
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
                    self.log(MetricsEvent::SendFrameBuffer);
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
                    sent_data_pkt.set(self.signals.sent_data_pkt.notified());
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
        &self.signals.recv_fin
    }

    #[cfg(test)]
    pub fn recv_eof(&self) -> &tokio_util::sync::CancellationToken {
        &self.signals.recv_eof
    }

    pub(crate) fn session_tag(&self) -> Option<u64> {
        self.session_tag
    }

    pub(crate) fn observe_post_open_handshake(
        &self,
        datagram: &[u8],
        now: Instant,
    ) -> PostOpenVerdict {
        if !self.post_open_handshake_active.load(Ordering::Acquire) {
            return PostOpenVerdict::NotHandshake;
        }
        let Some(handshake) = &self.post_open_handshake else {
            return PostOpenVerdict::NotHandshake;
        };
        let mut handshake = handshake.lock().unwrap();
        let observation = handshake.observe(datagram, now);
        if observation == PostOpenVerdict::Complete || handshake.expired(now) {
            self.post_open_handshake_active
                .store(false, Ordering::Release);
        }
        observation
    }

    pub(crate) fn claim_post_open_response(&self, now: Instant) -> Option<DueResponse> {
        if !self.post_open_handshake_active.load(Ordering::Acquire) {
            return None;
        }
        let handshake = self.post_open_handshake.as_ref()?;
        let mut handshake = handshake.lock().unwrap();
        let response = handshake.take_due_response(now);
        if handshake.expired(now) {
            self.post_open_handshake_active
                .store(false, Ordering::Release);
        }
        response
    }

    pub(crate) fn retry_post_open_response(&self, now: Instant) {
        if self.post_open_handshake_active.load(Ordering::Acquire)
            && let Some(handshake) = &self.post_open_handshake
        {
            handshake.lock().unwrap().retry_response(now);
        }
    }

    pub(crate) fn next_send_wake_with_ack_deadline(
        &self,
        now: Instant,
        ack_deadline: Option<Instant>,
    ) -> SendWake {
        let (mut protocol_deadline, pacing_deadline) = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            (
                reliable_layer.pkt_send_space().next_poll_time(now),
                reliable_layer.next_pacing_deadline(now),
            )
        };
        if let Some(ack_deadline) = ack_deadline {
            protocol_deadline =
                Some(protocol_deadline.map_or(ack_deadline, |current| current.min(ack_deadline)));
        }
        if self.post_open_handshake_active.load(Ordering::Acquire)
            && let Some(handshake) = &self.post_open_handshake
        {
            let handshake = handshake.lock().unwrap();
            if handshake.expired(now) {
                self.post_open_handshake_active
                    .store(false, Ordering::Release);
            } else if let Some(handshake_deadline) = handshake.next_send_time(now) {
                protocol_deadline = Some(protocol_deadline.map_or(handshake_deadline, |current| {
                    current.min(handshake_deadline)
                }));
            }
        }
        SendWake::after_send_pass(now, pacing_deadline, protocol_deadline)
    }

    /// The next-send wake with the ACK deadline supplied by the caller's
    /// single `AckFlushState::check` (one lock in the send pass, not two).
    pub(crate) fn next_send_wake_after_ack_check(
        &self,
        now: Instant,
        ack_deadline: Option<Instant>,
    ) -> SendWake {
        self.next_send_wake_with_ack_deadline(now, ack_deadline)
    }

    #[cfg(test)]
    pub(crate) fn next_send_wake(&self, now: Instant) -> SendWake {
        let ack_deadline = self.ack_flush.lock().unwrap().next_deadline(now);
        self.next_send_wake_with_ack_deadline(now, ack_deadline)
    }

    pub(crate) fn wire_ts(&self, now: Instant) -> u32 {
        let us = now.duration_since(self.clock_epoch).as_micros();
        us as u32
    }

    pub async fn no_data_to_send(&self) -> Result<(), IoErr> {
        let mut sent_pkt_acked = self.signals.sent_pkt_acked.notified();
        loop {
            self.termination.check_error()?;
            if self.reliable_layer.lock().unwrap().is_no_data_to_send() {
                return Ok(());
            }
            tokio::select! {
                () = sent_pkt_acked => (),
                () = self.termination.terminal().cancelled() => (),
            }
            sent_pkt_acked = self.signals.sent_pkt_acked.notified();
        }
    }

    pub(crate) async fn session_outbound_drained(&self) -> Result<(), IoErr> {
        loop {
            let progress = self.signals.session_outbound_progress.notified();
            self.termination.check_error()?;
            let reliable_drained = self.reliable_layer.lock().unwrap().is_no_data_to_send();
            let ack_drained = {
                let ack_flush = self.ack_flush.lock().unwrap();
                !ack_flush.has_pending()
            };
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
        let sent_data_pkt = self.signals.sent_data_pkt.notified();
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
            sent_data_pkt.set(self.signals.sent_data_pkt.notified());
        }
    }

    pub(crate) fn commit_received_batch(&self, batch: ReceivedBatch) {
        let ack_work_added = batch.pending_acks > 0 || batch.fin_ack;
        let should_resume_send = if ack_work_added {
            let mut ack_flush = self.ack_flush.lock().unwrap();
            ack_flush.record(batch.pending_acks, batch.fin_ack, batch.echo_ts)
        } else {
            false
        };
        if should_resume_send {
            self.request_send_driver_resume(MetricsSendDriverResumeSource::AckFlush);
        }
        if batch.recv_fin {
            self.signals.recv_fin.cancel();
        }
        self.publish_recv_eof(batch.recv_eof);
        self.signals.session_outbound_progress.notify_one();
    }

    fn publish_recv_eof(&self, recv_eof: bool) {
        if recv_eof && !self.signals.recv_eof.is_cancelled() {
            self.signals.recv_eof.cancel();
            if let Some(fec) = self.fec.as_ref() {
                fec.lock().unwrap().debug_print_stats();
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
        let mut recv_data_pkt = self.signals.recv_data_pkt.notified();
        let read_bytes = loop {
            self.termination.check_error()?;
            if self.signals.recv_eof.is_cancelled() {
                return Ok(0);
            }
            let (read_bytes, recv_eof) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let read_bytes = reliable_layer.recv_data_buf(data);
                (read_bytes, reliable_layer.recv_eof_ready())
            };
            self.publish_recv_eof(recv_eof);
            self.log(MetricsEvent::ReceiveDataBuffer);
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
                () = recv_data_pkt => (),
                () = self.termination.terminal().cancelled() => (),
            }
            recv_data_pkt = self.signals.recv_data_pkt.notified();
        };
        Ok(read_bytes)
    }

    pub async fn recv_frame(&self) -> Result<Option<Vec<u8>>, IoErr> {
        let mut recv_data_pkt = self.signals.recv_data_pkt.notified();
        loop {
            self.termination.check_error()?;
            let (res, recv_eof) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let res = reliable_layer.recv_frame_buf();
                (res, reliable_layer.recv_eof_ready())
            };
            self.publish_recv_eof(recv_eof);
            match res {
                Ok(Some(frame)) => {
                    self.log(MetricsEvent::ReceiveFrameBuffer);
                    return Ok(Some(frame));
                }
                Ok(None) => {
                    return Ok(None);
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    tokio::select! {
                        () = recv_data_pkt => (),
                        () = self.termination.terminal().cancelled() => (),
                    }
                    recv_data_pkt = self.signals.recv_data_pkt.notified();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub(crate) fn sample_rtt(&self, rtt: std::time::Duration, now: Instant) {
        let elapsed = (self.metrics_observer.is_some() || self.reliable_layer_logger.is_some())
            .then(|| now.saturating_duration_since(self.clock_epoch));
        let observer_interest = self
            .metrics_observer
            .as_ref()
            .map(|observer| {
                observer.interest(
                    MetricsEvent::RttSample,
                    elapsed.expect("metrics observer capture includes elapsed time"),
                )
            })
            .unwrap_or(MetricsInterest::Skip);
        let enabled =
            observer_interest != MetricsInterest::Skip || self.reliable_layer_logger.is_some();
        let captured = {
            let mut reliable_layer = self.reliable_layer.lock().unwrap();
            reliable_layer.sample_rtt(rtt, now);
            enabled.then(|| {
                let snapshot = (observer_interest == MetricsInterest::Snapshot
                    || self.reliable_layer_logger.is_some())
                .then(|| reliable_layer.metrics_at(now));
                let event_index = self.metrics_event_index.fetch_add(1, Ordering::Relaxed);
                (event_index, snapshot)
            })
        };
        if let Some((event_index, snapshot)) = captured {
            self.publish_metrics(
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
        if self.metrics_observer.is_none() && self.reliable_layer_logger.is_none() {
            return;
        }
        self.log_enabled_at(event, Instant::now());
    }

    /// Log an event against a caller-supplied decision time: the clock is
    /// never re-sampled for a decision already made at the pass time, so
    /// `elapsed` (and every deadline derived from it) matches the moment the
    /// decision was made, not the moment the metrics call happened to run.
    pub(crate) fn log_at(&self, event: MetricsEvent, now: Instant) {
        if self.metrics_observer.is_none() && self.reliable_layer_logger.is_none() {
            return;
        }
        self.log_enabled_at(event, now);
    }

    fn log_enabled_at(&self, event: MetricsEvent, now: Instant) {
        let elapsed = now.saturating_duration_since(self.clock_epoch);
        let observer_interest = self
            .metrics_observer
            .as_ref()
            .map(|observer| observer.interest(event, elapsed))
            .unwrap_or(MetricsInterest::Skip);
        if observer_interest == MetricsInterest::Skip && self.reliable_layer_logger.is_none() {
            return;
        }
        let capture_snapshot =
            observer_interest == MetricsInterest::Snapshot || self.reliable_layer_logger.is_some();
        let (event_index, snapshot) = if capture_snapshot {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let snapshot = reliable_layer.metrics_at(now);
            let event_index = self.metrics_event_index.fetch_add(1, Ordering::Relaxed);
            (event_index, Some(snapshot))
        } else {
            (
                self.metrics_event_index.fetch_add(1, Ordering::Relaxed),
                None,
            )
        };
        self.publish_metrics(
            event_index,
            event,
            None,
            elapsed,
            snapshot,
            observer_interest,
        );
    }

    fn publish_metrics(
        &self,
        event_index: u64,
        event: MetricsEvent,
        raw_rtt_sample: Option<std::time::Duration>,
        elapsed: std::time::Duration,
        snapshot: Option<crate::metrics::MetricsSnapshot>,
        observer_interest: MetricsInterest,
    ) {
        let observation = MetricsObservation {
            schema_version: SCHEMA_VERSION,
            event_index,
            elapsed,
            event,
            raw_rtt_sample,
            snapshot,
        };
        if observer_interest != MetricsInterest::Skip
            && let Some(observer) = &self.metrics_observer
        {
            observer.observe(observation);
        }
        let Some(logger) = &self.reliable_layer_logger else {
            return;
        };
        let snapshot = snapshot.expect("logger capture includes a snapshot");
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let log = MetricsRow {
            schema_version: SCHEMA_VERSION,
            event_index,
            op: event.as_str(),
            time: time.as_micros(),
            elapsed_micros: elapsed.as_micros(),
            raw_rtt_micros: raw_rtt_sample.map(|sample| sample.as_micros()),
            tokens: snapshot.pacer_tokens_packets,
            send_rate: snapshot.send_rate_packets_per_second,
            loss_rate: snapshot.loss_ratio,
            congestion_loss_rate: snapshot.congestion_loss_ratio,
            congestion_action: snapshot.congestion_action.map(|action| action.as_str()),
            num_in_flight_pkts: snapshot.in_flight_packets,
            num_pkts_in_pipe: snapshot.packets_in_pipe,
            num_rtx_active_pkts: snapshot.retransmission_active_packets,
            num_rtx_ready_pkts: snapshot.retransmission_ready_packets,
            num_rtx_pkts: snapshot.retransmitted_packets,
            send_seq: snapshot.next_send_sequence,
            min_rtt: snapshot.minimum_rtt.map(|rtt| rtt.as_millis()),
            rtt: snapshot.smoothed_rtt.as_millis(),
            retransmission_timeout_micros: snapshot.retransmission_timeout.as_micros(),
            oldest_pipe_packet_age_micros: snapshot
                .oldest_pipe_packet_age
                .map(|value| value.as_micros()),
            maximum_packet_rto_overdue_micros: snapshot
                .maximum_packet_rto_overdue
                .map(|value| value.as_micros()),
            rto_deadline_postponements: snapshot.rto_deadline_postponements,
            cwnd: snapshot.congestion_window_packets,
            num_rx_pkts: snapshot.received_packets,
            recv_seq: snapshot.next_receive_sequence,
            delivery_rate: snapshot.delivery_rate_packets_per_second,
            delivery_sample_app_limited: snapshot.delivery_sample_app_limited,
            application_write_waiters: snapshot.application_write_waiters,
            application_limited_detections: snapshot.application_limited_detections,
            application_limited_detections_suppressed_by_waiting_writer: snapshot
                .application_limited_detections_suppressed_by_waiting_writer,
            congestion_control_rtt_micros: snapshot
                .congestion_control_rtt
                .map(|value| value.as_micros()),
            congestion_rtt_floor_micros: snapshot
                .congestion_rtt_floor
                .map(|value| value.as_micros()),
            congestion_queue_tolerance_micros: snapshot
                .congestion_queue_tolerance
                .map(|value| value.as_micros()),
            congestion_persistent_queue_for_micros: snapshot
                .congestion_persistent_queue_for
                .map(|value| value.as_micros()),
            congestion_persistent_queue_resets: snapshot.congestion_persistent_queue_resets,
            congestion_delivery_peak_packets_per_second: snapshot
                .congestion_delivery_peak_packets_per_second,
            congestion_drain_floor_packets_per_second: snapshot
                .congestion_drain_floor_packets_per_second,
            congestion_drain_target_packets_per_second: snapshot
                .congestion_drain_target_packets_per_second,
            congestion_rate_samples: snapshot.congestion_rate_samples,
            congestion_bandwidth_probe_decisions: snapshot.congestion_bandwidth_probe_decisions,
            congestion_bandwidth_probe_increases: snapshot.congestion_bandwidth_probe_increases,
            congestion_bandwidth_probe_before_feedback: snapshot
                .congestion_bandwidth_probe_before_feedback,
            congestion_last_bandwidth_probe_interval_micros: snapshot
                .congestion_last_bandwidth_probe_interval
                .map(|value| value.as_micros()),
            congestion_delay_drains: snapshot.congestion_delay_drains,
            pending_send_bytes: snapshot.pending_send_bytes,
            send_stage_capacity_bytes: snapshot.send_stage_capacity_bytes,
            accepts_new_packet: snapshot.accepts_new_packet,
            slow_start: snapshot.slow_start,
            gentle_mode: snapshot.gentle_mode,
            gentle_draining: snapshot.gentle_draining,
            queue_building: snapshot.queue_building,
            drain_floor_binding: snapshot.drain_floor_binding,
            outage_recovery: snapshot.outage_recovery,
            no_response_for_micros: snapshot.no_response_for.map(|value| value.as_micros()),
            no_progress_for_micros: snapshot.no_progress_for.map(|value| value.as_micros()),
            stall_reason: snapshot.stall_reason.map(|reason| reason.as_str()),
        };
        logger
            .lock()
            .unwrap()
            .serialize(&log)
            .expect("write CSV log");
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
        MetricsEvent, MetricsInterest, MetricsObserver, MetricsTerminationCause, SCHEMA_VERSION,
    };
    use crate::traffic_shaping::core::SendWake;
    use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
    use crate::transmission::test_doubles::{BlockingWrite, PendingRead};
    use crate::transmission::transmission_layer::UnreliableLayer;

    use super::new_connection;

    fn pending_layer(frame_delivery: FrameMode) -> UnreliableLayer {
        UnreliableLayer {
            utp_read: Box::new(PendingRead),
            utp_write: Box::new(BlockingWrite::new()),
            post_open_handshake: None,
            session_tag: None,
            initial_sequences: crate::sequence::InitialSequences::ZERO,
            initial_rtt: None,
            metrics_observer: None,
            mss: NonZeroUsize::new(crate::udp::NO_FEC_MSS).unwrap(),
            fec: None,
            fec_tuning: FecTuning::default(),
            frame_delivery,
            rtx_dup: false,
            instream_group_fec: false,
        }
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
            new_connection(pending_layer(FrameMode::default()), None);
        assert_eq!(shared.next_send_wake(now), SendWake::Event);
    }

    #[test]
    fn pacing_block_uses_a_one_shot_batch_deadline() {
        let now = Instant::now();
        let (shared, _write_half, _read_half, _reaper) =
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
        shared
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_at_most_tokens(usize::MAX, now);
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
}
