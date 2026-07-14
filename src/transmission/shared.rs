use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::coordination::Coordination;
use super::fec::FecState;
use super::read_half::ReadHalf;
use super::transmission_layer::{
    ACK_FLUSH_AGE, ACK_FLUSH_COUNT, AckFlushState, FirstError, Log, LogConfig, PRINT_DEBUG_MSGS,
    ProactiveTerminationContext, ReliableLayerLogger, UnreliableLayer,
    instream_group_fec_from_env, rtx_dup_from_env,
};
use super::ts_echo::RecentEchoes;
use super::write_half::WriteHalf;
use super::watchdog_tuning::WatchdogTuning;
use crate::reliable::reliable_layer::{ReliableLayer, SharedTokenBucket};

#[derive(Debug)]
pub struct Shared {
    pub(crate) reliable_layer: Mutex<ReliableLayer>,
    pub(crate) ack_flush: Mutex<AckFlushState>,
    pub(crate) ack_flush_gate: tokio::sync::Mutex<()>,
    pub(crate) fec: Option<Mutex<FecState>>,
    pub(crate) send_rate_limiter: Arc<Mutex<SharedTokenBucket>>,
    pub(crate) first_error: FirstError,
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
) -> (Arc<Shared>, Arc<WriteHalf>, ReadHalf) {
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
    let shared = Arc::new(Shared {
        reliable_layer: Mutex::new(reliable_layer),
        ack_flush: Mutex::new(AckFlushState::new()),
        ack_flush_gate: tokio::sync::Mutex::new(()),
        fec: unreliable_layer.fec.map(Mutex::new),
        send_rate_limiter,
        first_error: FirstError::new(),
        coord: Coordination::new(),
        rtx_dup: std::sync::atomic::AtomicBool::new(rtx_dup_from_env()),
        fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
        instream_group_fec_enabled: std::sync::atomic::AtomicBool::new(
            instream_group_fec_from_env(),
        ),
        clock_epoch: now,
        reliable_layer_logger,
    });
    let write_half = Arc::new(WriteHalf {
        utp_write: tokio::sync::Mutex::new(unreliable_layer.utp_write),
        shared: Arc::clone(&shared),
    });
    let read_half = ReadHalf {
        utp_read: unreliable_layer.utp_read,
        recent_echoes: RecentEchoes::new(),
        shared: Arc::clone(&shared),
    };
    (shared, write_half, read_half)
}

pub fn build_parts_with_watchdog_tuning(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
    tuning: WatchdogTuning,
) -> (Arc<Shared>, Arc<WriteHalf>, ReadHalf) {
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
    let shared = Arc::new(Shared {
        reliable_layer: Mutex::new(reliable_layer),
        ack_flush: Mutex::new(AckFlushState::new()),
        ack_flush_gate: tokio::sync::Mutex::new(()),
        fec: unreliable_layer.fec.map(Mutex::new),
        send_rate_limiter,
        first_error: FirstError::new(),
        coord: Coordination::new(),
        rtx_dup: std::sync::atomic::AtomicBool::new(rtx_dup_from_env()),
        fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
        instream_group_fec_enabled: std::sync::atomic::AtomicBool::new(
            instream_group_fec_from_env(),
        ),
        clock_epoch: now,
        reliable_layer_logger,
    });
    let write_half = Arc::new(WriteHalf {
        utp_write: tokio::sync::Mutex::new(unreliable_layer.utp_write),
        shared: Arc::clone(&shared),
    });
    let read_half = ReadHalf {
        utp_read: unreliable_layer.utp_read,
        recent_echoes: RecentEchoes::new(),
        shared: Arc::clone(&shared),
    };
    (shared, write_half, read_half)
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

    pub fn next_poll_send_time(&self) -> Option<Instant> {
        let now = Instant::now();
        let mut deadline = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let mut deadline = reliable_layer.pkt_send_space().next_poll_time();
            if !reliable_layer.is_send_buf_empty() && reliable_layer.pkt_send_space().accepts_new_pkt() {
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
                ack.last_ack_flush.map_or(Some(now), |last| Some(last + ACK_FLUSH_AGE))
            } else {
                None
            }
        };
        if let Some(ack_deadline) = ack_deadline {
            deadline = Some(deadline.map_or(ack_deadline, |current| current.min(ack_deadline)));
        }
        deadline
    }

    pub fn set_with_context_if_empty(
        &self,
        kind: std::io::ErrorKind,
        context: Option<ProactiveTerminationContext>,
    ) -> bool {
        self.first_error.set_with_context_if_empty(kind, context)
    }

    pub fn next_send_deadline(&self) -> Option<Instant> {
        let pacing = self.send_rate_limiter.lock().unwrap().next_token_time();
        let now = Instant::now();
        let rl = self.reliable_layer.lock().unwrap();
        let stock_deadline = rl.pkt_send_space().next_poll_time();
        let deadlines: [Option<Instant>; 2] = [
            if pacing > now { Some(pacing) } else { None },
            stock_deadline,
        ];
        deadlines.into_iter().flatten().min()
    }

    pub(crate) fn wire_ts(&self, now: Instant) -> u32 {
        let us = now.duration_since(self.clock_epoch).as_micros();
        us as u32
    }

    pub async fn no_data_to_send(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_pkt_acked = self.coord.sent_pkt_acked.notified();
        loop {
            self.first_error.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_no_data_to_send() {
                return Ok(());
            }
            tokio::select! {
                () = sent_pkt_acked => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_pkt_acked = self.coord.sent_pkt_acked.notified();
        }
    }

    pub async fn send_buf_empty(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_data_pkt = self.coord.sent_data_pkt.notified();
        loop {
            self.first_error.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_send_buf_empty() {
                return Ok(());
            }
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_data_pkt = self.coord.sent_data_pkt.notified();
        }
    }

    pub fn send_fin_buf(&self) {
        self.reliable_layer.lock().unwrap().send_fin_buf();
    }

    pub fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        &self.coord.recv_fin
    }

    pub fn some_error(&self) -> &tokio_util::sync::CancellationToken {
        &self.first_error.some
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        self.first_error.throw_error()
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            return Err(std::io::ErrorKind::InvalidInput);
        }
        let mut recv_data_pkt = self.coord.recv_data_pkt.notified();
        let read_bytes = loop {
            self.first_error.throw_error()?;

            if self.coord.recv_fin.is_cancelled() {
                return Ok(0);
            }

            let (read_bytes, read_fin) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                (
                    reliable_layer.recv_data_buf(data),
                    reliable_layer.recv_fin_buf(),
                )
            };
            self.log("recv_data_buf");
            if PRINT_DEBUG_MSGS {
                println!("recv: data: {read_bytes}");
            }
            if 0 < read_bytes {
                break read_bytes;
            }
            if read_fin {
                self.coord.recv_fin.cancel();
                if let Some(fec) = self.fec.as_ref() {
                    fec.lock().unwrap().debug_print_stats();
                }
                continue;
            }
            tokio::select! {
                () = recv_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            recv_data_pkt = self.coord.recv_data_pkt.notified();
        };
        Ok(read_bytes)
    }

    pub async fn recv_frame(&self) -> Result<Option<Vec<u8>>, std::io::ErrorKind> {
        let mut recv_data_pkt = self.coord.recv_data_pkt.notified();
        loop {
            self.first_error.throw_error()?;

            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.recv_frame_buf()
            };
            match res {
                Ok(Some(frame)) => {
                    self.log("recv_frame_buf");
                    return Ok(Some(frame));
                }
                Ok(None) => {
                    self.coord.recv_fin.cancel();
                    if let Some(fec) = self.fec.as_ref() {
                        fec.lock().unwrap().debug_print_stats();
                    }
                    return Ok(None);
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    tokio::select! {
                        () = recv_data_pkt => (),
                        () = self.first_error.some().cancelled() => (),
                    }
                    recv_data_pkt = self.coord.recv_data_pkt.notified();
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
