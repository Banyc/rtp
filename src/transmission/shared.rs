use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::coordination::Coordination;
use super::fec::FecState;
use super::read_half::ReadHalf;
use super::termination::{
    PeerReset, TerminationPresser, TerminationReaper, channel as termination_channel,
};
use super::transmission_layer::{
    ACK_FLUSH_AGE, ACK_FLUSH_COUNT, AckFlushState, Log, LogConfig, PRINT_DEBUG_MSGS,
    ReliableLayerLogger, UnreliableLayer, instream_group_fec_from_env, rtx_dup_from_env,
};
use super::ts_echo::RecentEchoes;
use super::watchdog_tuning::WatchdogTuning;
use super::write_half::WriteHalf;
use crate::handshake::{ClaimedResponse, Observation, PostOpenHandshake};

use crate::reliable::reliable_layer::{ReliableLayer, SharedTokenBucket};

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
pub struct Shared {
    pub(crate) reliable_layer: Mutex<ReliableLayer>,
    pub(crate) ack_flush: Mutex<AckFlushState>,
    post_open_handshake: Option<Mutex<PostOpenHandshake>>,
    post_open_handshake_active: AtomicBool,
    pub(crate) fec: Option<Mutex<FecState>>,
    pub(crate) send_rate_limiter: Arc<Mutex<SharedTokenBucket>>,
    pub(crate) termination: TerminationPresser,
    pub(crate) coord: Coordination,
    pub(crate) rtx_dup: std::sync::atomic::AtomicBool,
    pub(crate) fec_instream_flush: bool,
    pub(crate) instream_group_fec_enabled: std::sync::atomic::AtomicBool,
    pub(crate) clock_epoch: Instant,
    pub(crate) reliable_layer_logger: Option<ReliableLayerLogger>,
}

pub fn build_parts(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (Arc<Shared>, WriteHalf, ReadHalf, TerminationReaper) {
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
    let (termination, termination_writer, termination_reaper) = termination_channel();
    let post_open_handshake_active = unreliable_layer.post_open_handshake.is_some();
    let shared = Arc::new(Shared {
        reliable_layer: Mutex::new(reliable_layer),
        ack_flush: Mutex::new(AckFlushState::new()),
        post_open_handshake: unreliable_layer.post_open_handshake.map(Mutex::new),
        post_open_handshake_active: AtomicBool::new(post_open_handshake_active),
        fec: unreliable_layer.fec.map(Mutex::new),
        send_rate_limiter,
        termination,
        coord: Coordination::new(),
        rtx_dup: std::sync::atomic::AtomicBool::new(rtx_dup_from_env()),
        fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
        instream_group_fec_enabled: std::sync::atomic::AtomicBool::new(
            instream_group_fec_from_env(),
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

pub fn build_parts_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (Arc<Shared>, WriteHalf, ReadHalf, TerminationReaper) {
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
    let (termination, termination_writer, termination_reaper) = termination_channel();
    let post_open_handshake_active = unreliable_layer.post_open_handshake.is_some();
    let shared = Arc::new(Shared {
        reliable_layer: Mutex::new(reliable_layer),
        ack_flush: Mutex::new(AckFlushState::new()),
        post_open_handshake: unreliable_layer.post_open_handshake.map(Mutex::new),
        post_open_handshake_active: AtomicBool::new(post_open_handshake_active),
        fec: unreliable_layer.fec.map(Mutex::new),
        send_rate_limiter,
        termination,
        coord: Coordination::new(),
        rtx_dup: std::sync::atomic::AtomicBool::new(rtx_dup_from_env()),
        fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
        instream_group_fec_enabled: std::sync::atomic::AtomicBool::new(
            instream_group_fec_from_env(),
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

impl Shared {
    pub fn resume_send(&self) -> &tokio::sync::Notify {
        &self.coord.resume_send
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

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        self.termination.throw_error()
    }

    pub fn some_error(&self) -> &tokio_util::sync::CancellationToken {
        self.termination.terminal()
    }

    pub fn has_error(&self) -> bool {
        self.termination.has_error()
    }

    pub fn io_error(&self, kind: std::io::ErrorKind) -> std::io::Error {
        self.termination.io_error(kind)
    }

    pub fn request_kill_and_abort(&self) {
        self.termination
            .press_broken_pipe(PeerReset::SendKill, None);
    }

    pub async fn send(&self, data: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.termination.throw_error()?;
        if data.is_empty() {
            self.reliable_layer.lock().unwrap().ensure_write_open()?;
            return Ok(0);
        }
        let result = if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            self.send_frame(data).await
        } else {
            self.send_stock(data).await
        };
        self.termination.throw_error()?;
        result
    }

    async fn send_stock(&self, data: &[u8]) -> Result<usize, std::io::ErrorKind> {
        let mut sent_data_pkt = self.coord.sent_data_pkt.notified();
        loop {
            self.termination.throw_error()?;
            let written_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_buf(data, Instant::now())
            }?;
            self.log("send_data_buf");
            if 0 < written_bytes {
                self.coord.resume_send.notify_one();
                return Ok(written_bytes);
            }
            self.termination.throw_error()?;
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                () = self.termination.terminal().cancelled() => (),
            }
            self.termination.throw_error()?;
            sent_data_pkt = self.coord.sent_data_pkt.notified();
        }
    }

    pub async fn send_frame(&self, frame: &[u8]) -> Result<usize, std::io::ErrorKind> {
        let frame_len = frame.len();
        let mut sent_data_pkt = self.coord.sent_data_pkt.notified();
        loop {
            self.termination.throw_error()?;
            let result = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_frame_buf(frame, Instant::now())
            };
            match result {
                Ok(()) => {
                    self.log("send_frame_buf");
                    self.coord.resume_send.notify_one();
                    return Ok(frame_len);
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    self.termination.throw_error()?;
                    tokio::select! {
                        _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                        () = self.termination.terminal().cancelled() => (),
                    }
                    self.termination.throw_error()?;
                    sent_data_pkt = self.coord.sent_data_pkt.notified();
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn set_error(&self, kind: std::io::ErrorKind) {
        self.termination.press_error(kind);
    }

    pub fn send_fin_buf(&self) {
        self.reliable_layer.lock().unwrap().send_fin_buf();
        self.coord.resume_send.notify_one();
    }

    pub fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        &self.coord.recv_fin
    }

    pub fn recv_eof(&self) -> &tokio_util::sync::CancellationToken {
        &self.coord.recv_eof
    }

    pub(crate) fn observe_post_open_handshake(&self, datagram: &[u8], now: Instant) -> Observation {
        if !self.post_open_handshake_active.load(Ordering::Acquire) {
            return Observation::NotHandshake;
        }
        let Some(handshake) = &self.post_open_handshake else {
            return Observation::NotHandshake;
        };
        let mut handshake = handshake.lock().unwrap();
        let observation = handshake.observe(datagram, now);
        if observation == Observation::Complete || handshake.expired(now) {
            self.post_open_handshake_active
                .store(false, Ordering::Release);
        }
        observation
    }

    pub(crate) fn claim_post_open_response(&self, now: Instant) -> Option<ClaimedResponse> {
        if !self.post_open_handshake_active.load(Ordering::Acquire) {
            return None;
        }
        let handshake = self.post_open_handshake.as_ref()?;
        let mut handshake = handshake.lock().unwrap();
        let response = handshake.claim_response(now);
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

    pub fn next_poll_send_time(&self) -> Option<Instant> {
        let now = Instant::now();
        let mut deadline = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let mut deadline = reliable_layer.pkt_send_space().next_poll_time();
            if !reliable_layer.is_send_buf_empty()
                && reliable_layer.pkt_send_space().accepts_new_pkt()
            {
                let token = self.send_rate_limiter.lock().unwrap().next_token_time();
                deadline = Some(deadline.map_or(token, |current| current.min(token)));
            }
            deadline
        };
        let ack_deadline = {
            let ack = self.ack_flush.lock().unwrap();
            if ack.pending_acks >= ACK_FLUSH_COUNT || ack.fin_pending {
                Some(now)
            } else if ack.pending_acks > 0 {
                ack.last_ack_flush
                    .map_or(Some(now), |last| Some(last + ACK_FLUSH_AGE))
            } else {
                None
            }
        };
        if let Some(ack_deadline) = ack_deadline {
            deadline = Some(deadline.map_or(ack_deadline, |current| current.min(ack_deadline)));
        }
        if self.post_open_handshake_active.load(Ordering::Acquire)
            && let Some(handshake) = &self.post_open_handshake
        {
            let handshake = handshake.lock().unwrap();
            if handshake.expired(now) {
                self.post_open_handshake_active
                    .store(false, Ordering::Release);
            } else if let Some(handshake_deadline) = handshake.next_send_time(now) {
                deadline = Some(deadline.map_or(handshake_deadline, |current| {
                    current.min(handshake_deadline)
                }));
            }
        }
        deadline
    }

    pub(crate) fn wire_ts(&self, now: Instant) -> u32 {
        let us = now.duration_since(self.clock_epoch).as_micros();
        us as u32
    }

    pub async fn no_data_to_send(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_pkt_acked = self.coord.sent_pkt_acked.notified();
        loop {
            self.termination.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_no_data_to_send() {
                return Ok(());
            }
            tokio::select! {
                () = sent_pkt_acked => (),
                () = self.termination.terminal().cancelled() => (),
            }
            sent_pkt_acked = self.coord.sent_pkt_acked.notified();
        }
    }

    pub(crate) async fn session_outbound_drained(&self) -> Result<(), std::io::ErrorKind> {
        loop {
            let progress = self.coord.session_outbound_progress.notified();
            self.termination.throw_error()?;
            let reliable_drained = self.reliable_layer.lock().unwrap().is_no_data_to_send();
            let ack_drained = {
                let ack_flush = self.ack_flush.lock().unwrap();
                ack_flush.pending_acks == 0 && !ack_flush.fin_pending
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

    pub async fn send_buf_empty(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_data_pkt = self.coord.sent_data_pkt.notified();
        loop {
            self.termination.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_send_buf_empty() {
                return Ok(());
            }
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                () = self.termination.terminal().cancelled() => (),
            }
            sent_data_pkt = self.coord.sent_data_pkt.notified();
        }
    }

    pub(crate) fn commit_received_batch(&self, batch: ReceivedBatch) {
        let ack_work_added = batch.pending_acks > 0 || batch.fin_ack;
        if ack_work_added {
            let mut ack_flush = self.ack_flush.lock().unwrap();
            ack_flush.pending_acks += batch.pending_acks;
            ack_flush.fin_pending |= batch.fin_ack;
            if let Some(echo_ts) = batch.echo_ts {
                ack_flush.ts_echo.set(echo_ts);
            }
        }
        if ack_work_added {
            self.coord.resume_send.notify_one();
        }
        if batch.recv_fin {
            self.coord.recv_fin.cancel();
        }
        self.publish_recv_eof(batch.recv_eof);
        self.coord.session_outbound_progress.notify_one();
    }

    fn publish_recv_eof(&self, recv_eof: bool) {
        if recv_eof && !self.coord.recv_eof.is_cancelled() {
            self.coord.recv_eof.cancel();
            if let Some(fec) = self.fec.as_ref() {
                fec.lock().unwrap().debug_print_stats();
            }
        }
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        if data.is_empty() {
            return Ok(0);
        }
        if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            return Err(std::io::ErrorKind::InvalidInput);
        }
        let mut recv_data_pkt = self.coord.recv_data_pkt.notified();
        let read_bytes = loop {
            self.termination.throw_error()?;
            if self.coord.recv_eof.is_cancelled() {
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
                println!("recv:data:{read_bytes}");
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
            recv_data_pkt = self.coord.recv_data_pkt.notified();
        };
        Ok(read_bytes)
    }

    pub async fn recv_frame(&self) -> Result<Option<Vec<u8>>, std::io::ErrorKind> {
        let mut recv_data_pkt = self.coord.recv_data_pkt.notified();
        loop {
            self.termination.throw_error()?;
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
                Ok(None) => return Ok(None),
                Err(std::io::ErrorKind::WouldBlock) => {
                    tokio::select! {
                        () = recv_data_pkt => (),
                        () = self.termination.terminal().cancelled() => (),
                    }
                    recv_data_pkt = self.coord.recv_data_pkt.notified();
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
            .expect("unix timestamp");
        let log = self.reliable_layer.lock().unwrap().log();
        let log = Log {
            op,
            time: time.as_micros(),
            tokens: log.tokens,
            send_rate: log.send_rate,
            loss_rate: log.loss_rate,
            num_tx_pkts: log.num_tx_pkts,
            num_pkts_in_pipe: log.num_pkts_in_pipe,
            num_rt_pkts: log.num_rt_pkts,
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

    use async_trait::async_trait;

    use crate::delivery::frame::FrameDelivery;
    use crate::delivery::frame::send::MAX_FRAME_LEN;
    use crate::transmission::fec_tuning::FecTuning;
    use crate::transmission::transmission_layer::{
        UnreliableLayer, UnreliableRead, UnreliableWrite,
    };

    use super::build_parts;

    #[derive(Debug)]
    struct PendingRead;

    #[derive(Debug)]
    struct PendingWrite;

    #[async_trait]
    impl UnreliableRead for PendingRead {
        fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            Err(std::io::ErrorKind::WouldBlock)
        }

        async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            std::future::pending().await
        }
    }

    #[async_trait]
    impl UnreliableWrite for PendingWrite {
        async fn send(&mut self, _buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn broken_pipe_outranks_full_frame_queue() {
        let layer = UnreliableLayer {
            utp_read: Box::new(PendingRead),
            utp_write: Box::new(PendingWrite),
            post_open_handshake: None,
            mss: NonZeroUsize::new(crate::udp::NO_FEC_MSS).unwrap(),
            fec: None,
            fec_tuning: FecTuning::default(),
            frame_delivery: FrameDelivery::enabled(),
        };
        let (shared, _write_half, _read_half, _reaper) = build_parts(layer, None);
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
            .press_error(std::io::ErrorKind::BrokenPipe);
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), blocked_send)
            .await
            .expect("BrokenPipe must wake a sender waiting for frame-queue capacity");
        assert_eq!(result, Err(std::io::ErrorKind::BrokenPipe));
    }
}
