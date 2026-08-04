use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::ack_flush::AckFlushState;
use super::coordination::Signals;
use super::fec::FecState;
use super::read_half::ReadHalf;
use super::termination::{KillPolicy, TerminationPresser, TerminationReaper, new_termination};
use super::transmission_layer::{
    LogConfig, MetricsRow, PRINT_DEBUG_MSGS, ReliableLayerLogger, UnreliableLayer,
};
use super::ts_echo::RecentEchoes;
use super::watchdog_tuning::WatchdogTuning;
use super::write_half::WriteHalf;

use crate::handshake::{DueResponse, PostOpenHandshake, PostOpenVerdict};
use crate::io_err::IoErr;
use crate::pacer::{SendPacer, SendWake};
use crate::reliable::reliable_layer::ReliableLayer;

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
    pub(crate) fec: Option<Mutex<FecState>>,
    pub(crate) send_rate_limiter: Arc<Mutex<SendPacer>>,
    pub(crate) termination: TerminationPresser,
    pub(crate) signals: Signals,
    pub(crate) rtx_dup: std::sync::atomic::AtomicBool,
    pub(crate) fec_instream_flush: bool,
    pub(crate) instream_group_fec_enabled: std::sync::atomic::AtomicBool,
    pub(crate) clock_epoch: Instant,
    pub(crate) reliable_layer_logger: Option<ReliableLayerLogger>,
}

pub fn new_connection(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (Arc<Connection>, WriteHalf, ReadHalf, TerminationReaper) {
    let now = Instant::now();
    let frame_delivery = unreliable_layer.frame_delivery;
    let (reliable_layer, send_rate_limiter) =
        ReliableLayer::new(unreliable_layer.mss, frame_delivery, now);
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
    let (reliable_layer, send_rate_limiter) =
        ReliableLayer::new_with_watchdog_tuning(unreliable_layer.mss, frame_delivery, now, tuning);
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

    pub fn request_kill_and_abort(&self) {
        self.termination
            .press_broken_pipe(KillPolicy::SendKill, None);
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
        let mut sent_data_pkt = self.signals.sent_data_pkt.notified();
        loop {
            self.termination.check_error()?;
            let written_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_buf(data, now)
            }?;
            self.log("send_data_buf");
            if 0 < written_bytes {
                self.signals.resume_send.notify_one();
                return Ok(written_bytes);
            }
            self.termination.check_error()?;
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                () = self.termination.terminal().cancelled() => (),
            }
            self.termination.check_error()?;
            sent_data_pkt = self.signals.sent_data_pkt.notified();
        }
    }

    pub async fn send_frame(&self, frame: &[u8]) -> Result<usize, IoErr> {
        let now = Instant::now();
        let frame_len = frame.len();
        let mut sent_data_pkt = self.signals.sent_data_pkt.notified();
        loop {
            self.termination.check_error()?;
            let result = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_frame_buf(frame, now)
            };
            match result {
                Ok(()) => {
                    self.log("send_frame_buf");
                    self.signals.resume_send.notify_one();
                    return Ok(frame_len);
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    self.termination.check_error()?;
                    tokio::select! {
                        _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                        () = self.termination.terminal().cancelled() => (),
                    }
                    self.termination.check_error()?;
                    sent_data_pkt = self.signals.sent_data_pkt.notified();
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn send_fin_buf(&self) {
        self.reliable_layer.lock().unwrap().send_fin_buf();
        self.signals.resume_send.notify_one();
    }

    pub fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        &self.signals.recv_fin
    }

    #[cfg(test)]
    pub fn recv_eof(&self) -> &tokio_util::sync::CancellationToken {
        &self.signals.recv_eof
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

    pub(crate) fn next_send_wake(&self, now: Instant) -> SendWake {
        let (mut protocol_deadline, pacing_deadline) = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            (
                reliable_layer.pkt_send_space().next_poll_time(),
                reliable_layer.next_pacing_deadline(now),
            )
        };
        let ack_deadline = {
            let ack = self.ack_flush.lock().unwrap();
            ack.next_deadline(now)
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
        let mut sent_data_pkt = self.signals.sent_data_pkt.notified();
        loop {
            self.termination.check_error()?;
            if self.reliable_layer.lock().unwrap().is_send_buf_empty() {
                return Ok(());
            }
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                () = self.termination.terminal().cancelled() => (),
            }
            sent_data_pkt = self.signals.sent_data_pkt.notified();
        }
    }

    pub(crate) fn commit_received_batch(&self, batch: ReceivedBatch) {
        let ack_work_added = batch.pending_acks > 0 || batch.fin_ack;
        if ack_work_added {
            let mut ack_flush = self.ack_flush.lock().unwrap();
            ack_flush.record(batch.pending_acks, batch.fin_ack, batch.echo_ts);
        }
        if ack_work_added {
            self.signals.resume_send.notify_one();
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
            self.log("recv_data_buf");
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
                    self.log("recv_frame_buf");
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

    pub(crate) fn log(&self, op: &str) {
        let Some(logger) = &self.reliable_layer_logger else {
            return;
        };
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let log = self.reliable_layer.lock().unwrap().log();
        let log = MetricsRow {
            op,
            time: time.as_micros(),
            tokens: log.tokens,
            send_rate: log.send_rate,
            loss_rate: log.loss_rate,
            num_in_flight_pkts: log.num_in_flight_pkts,
            num_pkts_in_pipe: log.num_pkts_in_pipe,
            num_rtx_pkts: log.num_rtx_pkts,
            send_seq: log.send_seq,
            min_rtt: log.min_rtt,
            rtt: log.rtt,
            cwnd: log.cwnd,
            num_rx_pkts: log.num_rx_pkts,
            recv_seq: log.recv_seq,
            delivery_rate: log.delivery_rate,
            app_limited: log.app_limited,
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
    use std::time::Instant;

    use crate::delivery::frame::FrameMode;
    use crate::delivery::frame::send::MAX_FRAME_LEN;
    use crate::pacer::SendWake;
    use crate::transmission::fec_tuning::FecTuning;
    use crate::transmission::test_doubles::{BlockingWrite, PendingRead};
    use crate::transmission::transmission_layer::UnreliableLayer;

    use super::new_connection;

    fn pending_layer(frame_delivery: FrameMode) -> UnreliableLayer {
        UnreliableLayer {
            utp_read: Box::new(PendingRead),
            utp_write: Box::new(BlockingWrite::new()),
            post_open_handshake: None,
            mss: NonZeroUsize::new(crate::udp::NO_FEC_MSS).unwrap(),
            fec: None,
            fec_tuning: FecTuning::default(),
            frame_delivery,
            rtx_dup: false,
            instream_group_fec: false,
        }
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
