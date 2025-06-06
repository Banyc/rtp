use core::{num::NonZeroUsize, time::Duration};
use std::{
    path::PathBuf,
    sync::{Mutex, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    codec::{EncodeAck, EncodeData, decode, encode_ack_data, encode_kill},
    reliable_layer::ReliableLayer,
    sack::{AckBall, AckBallSequence},
};

const PRINT_DEBUG_MSGS: bool = false;
const MAX_NUM_ACK: usize = 64;
const MIN_NO_RESP_FOR: Duration = Duration::from_secs(1);

type ReliableLayerLogger = Mutex<csv::Writer<std::fs::File>>;

#[derive(Debug)]
pub struct TransmissionLayer {
    utp_read: tokio::sync::Mutex<Box<dyn UnreliableRead>>,
    utp_write: tokio::sync::Mutex<Box<dyn UnreliableWrite>>,
    reliable_layer: Mutex<ReliableLayer>,
    sent_data_pkt: tokio::sync::Notify,
    recv_data_pkt: tokio::sync::Notify,
    sent_pkt_acked: tokio::sync::Notify,
    recv_fin: tokio_util::sync::CancellationToken,
    first_error: FirstError,
    reliable_layer_logger: Option<ReliableLayerLogger>,
}
impl TransmissionLayer {
    pub fn new(unreliable_layer: UnreliableLayer, log_config: Option<LogConfig>) -> Self {
        let now = Instant::now();
        let reliable_layer = Mutex::new(ReliableLayer::new(unreliable_layer.mss, now));
        let sent_data_pkt = tokio::sync::Notify::new();
        let recv_data_pkt = tokio::sync::Notify::new();
        let sent_pkt_acked = tokio::sync::Notify::new();
        let recv_fin = tokio_util::sync::CancellationToken::new();
        let first_error = FirstError::new();
        let reliable_layer_logger = log_config.as_ref().map(|c| {
            let file = std::fs::File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&c.reliable_layer_log_path)
                .expect("open log file");
            Mutex::new(csv::WriterBuilder::new().from_writer(file))
        });
        Self {
            utp_read: tokio::sync::Mutex::new(unreliable_layer.utp_read),
            utp_write: tokio::sync::Mutex::new(unreliable_layer.utp_write),
            reliable_layer,
            sent_data_pkt,
            recv_data_pkt,
            sent_pkt_acked,
            recv_fin,
            first_error,
            reliable_layer_logger,
        }
    }

    pub fn reliable_layer(&self) -> &Mutex<ReliableLayer> {
        &self.reliable_layer
    }

    /// # Cancel safety
    ///
    /// It is cancel safe.
    pub async fn no_data_to_send(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_pkt_acked = self.sent_pkt_acked.notified();
        loop {
            self.first_error.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_no_data_to_send() {
                return Ok(());
            }
            tokio::select! {
                () = sent_pkt_acked => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_pkt_acked = self.sent_pkt_acked.notified();
        }
    }

    /// # Cancel safety
    ///
    /// It is cancel safe.
    pub async fn send_buf_empty(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_data_pkt = self.sent_data_pkt.notified();
        loop {
            self.first_error.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_send_buf_empty() {
                return Ok(());
            }
            tokio::select! {
                () = sent_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_data_pkt = self.sent_data_pkt.notified();
        }
    }

    pub fn send_fin_buf(&self) {
        self.reliable_layer.lock().unwrap().send_fin_buf();
    }

    pub fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        &self.recv_fin
    }

    pub fn some_error(&self) -> &tokio_util::sync::CancellationToken {
        &self.first_error.some
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        self.first_error.throw_error()
    }

    pub async fn send_kill_pkt(&self) -> Result<(), std::io::ErrorKind> {
        let mut buf = [0; 1];
        encode_kill(&mut buf).unwrap();
        self.utp_write.lock().await.send(&buf).await?;
        Ok(())
    }

    pub async fn send_pkts(
        &self,
        data_buf: &mut [u8],
        utp_buf: &mut [u8],
    ) -> Result<(), std::io::ErrorKind> {
        let detect_broken_pipe_proactively = || {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let Some(no_resp_for) = reliable_layer.pkt_send_space().no_resp_for(Instant::now())
            else {
                return;
            };
            if no_resp_for < reliable_layer.pkt_send_space().rto_duration().mul_f64(16.0) {
                return;
            }
            // Avoid triggering broken pipe errors during inter-process data transfer.
            if no_resp_for < MIN_NO_RESP_FOR {
                return;
            }
            self.first_error.set(std::io::ErrorKind::BrokenPipe);
        };
        detect_broken_pipe_proactively();

        let mut written_bytes = 0;
        let mut written_fin = false;
        loop {
            self.first_error.throw_error()?;
            // reliable -{data}> UDP remote
            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_pkt(data_buf, Instant::now())
            };
            self.log("send_data_pkt");
            let Some(p) = res else {
                break;
            };
            let data_written = match p.data_written {
                crate::reliable_layer::DataPktPayload::Data(data_written) => {
                    written_bytes += data_written.get();
                    data_written.get()
                }
                crate::reliable_layer::DataPktPayload::Fin => {
                    written_fin = true;
                    0
                }
            };
            let data = EncodeData {
                seq: p.seq,
                data: &data_buf[..data_written],
            };
            let n = encode_ack_data(None, Some(data), utp_buf).unwrap();
            let Err(e) = self.utp_write.lock().await.send(&utp_buf[..n]).await else {
                continue;
            };
            self.first_error.set(e);
            return Err(e);
        }
        if 0 < written_bytes || written_fin {
            if PRINT_DEBUG_MSGS {
                println!("send_pkts: {{ data: {written_bytes}; fin: {written_fin} }}");
            }
            self.sent_data_pkt.notify_waiters();
        }
        Ok(())
    }

    pub async fn recv_pkts(
        &self,
        utp_buf: &mut [u8],
        ack_from_peer_buf: &mut Vec<AckBall>,
        ack_to_peer_buf: &mut Vec<u64>,
    ) -> Result<RecvPkts, (std::io::ErrorKind, SendKillPkt)> {
        let throw_error = |e: std::io::ErrorKind| {
            self.first_error.set(e);
            e
        };
        let mut recv_pkts = RecvPkts {
            num_ack_segments: 0,
            num_payload_segments: 0,
            num_fin_segments: 0,
        };

        ack_to_peer_buf.clear();
        for _ in 0..MAX_NUM_ACK {
            self.first_error
                .throw_error()
                .map_err(|e| (e, SendKillPkt::No))?;
            let res = {
                let mut utp_read = self.utp_read.lock().await;
                match ack_to_peer_buf.is_empty() {
                    true => utp_read.recv(utp_buf).await,
                    false => {
                        let res = utp_read.try_recv(utp_buf);
                        if let Err(e) = &res {
                            if *e == std::io::ErrorKind::WouldBlock {
                                break;
                            }
                        }
                        res
                    }
                }
            };
            let read_bytes = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err((throw_error(e), SendKillPkt::No));
                }
            };
            if PRINT_DEBUG_MSGS {
                println!("recv_pkts: recv: {read_bytes}");
            }

            ack_from_peer_buf.clear();
            let data = match decode(&utp_buf[..read_bytes], ack_from_peer_buf) {
                Ok(x) => x,
                Err(_) => continue,
            };

            if data.killed {
                let e = std::io::ErrorKind::BrokenPipe;
                throw_error(e);
                return Err((e, SendKillPkt::No));
            }

            let now = Instant::now();
            let mut reliable_layer = self.reliable_layer.lock().unwrap();

            // UDP local -{ACK}> reliable
            reliable_layer.recv_ack_pkt(AckBallSequence::new(ack_from_peer_buf), now);
            recv_pkts.num_ack_segments += 1;
            self.sent_pkt_acked.notify_waiters();

            let Some(data) = data.data else {
                drop(reliable_layer);
                self.log("recv_ack_pkt");
                continue;
            };

            // UDP local -{data}> reliable
            let ack = reliable_layer.recv_data_pkt(data.seq, &utp_buf[data.buf_range.clone()]);
            drop(reliable_layer);
            if data.buf_range.is_empty() {
                recv_pkts.num_fin_segments += 1;
            } else {
                recv_pkts.num_payload_segments += 1;
            }
            self.log("recv_data_pkt");
            match ack {
                true => {
                    ack_to_peer_buf.push(data.seq);
                }
                false => break,
            }
        }

        // No new data received in the reliable layer
        if ack_to_peer_buf.is_empty() {
            return Ok(recv_pkts);
        }

        self.recv_data_pkt.notify_waiters();

        // reliable -{ACK}> UDP remote
        let written_bytes = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let pkt_recv_space = reliable_layer.pkt_recv_space();
            let ack = EncodeAck {
                queue: pkt_recv_space.ack_history(),
                skip: 0,
                max_take: MAX_NUM_ACK,
            };
            encode_ack_data(Some(ack), None, utp_buf).unwrap()
        };
        self.utp_write
            .lock()
            .await
            .send(&utp_buf[..written_bytes])
            .await
            .map_err(throw_error)
            .map_err(|e| (e, SendKillPkt::No))?;
        if PRINT_DEBUG_MSGS {
            println!("recv_pkts: ack: {ack_to_peer_buf:?}");
        }

        Ok(recv_pkts)
    }

    pub async fn send(
        &self,
        data: &[u8],
        no_delay: bool,
        data_buf: &mut [u8],
        utp_buf: &mut [u8],
    ) -> Result<usize, std::io::ErrorKind> {
        let mut sent_data_pkt = self.sent_data_pkt.notified();
        let written_bytes = loop {
            self.first_error.throw_error()?;
            let written_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_buf(data, Instant::now())
            };
            self.log("send_data_buf");

            if no_delay {
                self.send_pkts(data_buf, utp_buf).await?;
            }

            if 0 < written_bytes {
                break written_bytes;
            }
            tokio::select! {
                () = sent_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_data_pkt = self.sent_data_pkt.notified();
        };
        Ok(written_bytes)
    }

    /// Return `Ok(0)` when the read stream hits EOF
    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let mut recv_data_pkt = self.recv_data_pkt.notified();
        let read_bytes = loop {
            self.first_error.throw_error()?;

            if self.recv_fin.is_cancelled() {
                return Ok(0);
            }

            // reliable -{data}> app
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
                self.recv_fin.cancel();
                continue;
            }
            tokio::select! {
                () = recv_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            recv_data_pkt = self.recv_data_pkt.notified();
        };
        Ok(read_bytes)
    }

    fn log(&self, op: &str) {
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

#[derive(Debug)]
pub struct UnreliableLayer {
    pub utp_read: Box<dyn UnreliableRead>,
    pub utp_write: Box<dyn UnreliableWrite>,
    pub mss: NonZeroUsize,
}

#[derive(Debug, Clone)]
pub struct RecvPkts {
    pub num_ack_segments: usize,
    pub num_payload_segments: usize,
    pub num_fin_segments: usize,
}

#[derive(Debug, Clone)]
pub enum SendKillPkt {
    Yes,
    No,
}

#[async_trait]
pub trait UnreliableRead: core::fmt::Debug + Sync + Send + 'static {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;
}
#[async_trait]
pub trait UnreliableWrite: core::fmt::Debug + Sync + Send + 'static {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind>;
}

#[derive(Debug)]
struct FirstError {
    first_error: RwLock<Option<std::io::ErrorKind>>,
    some: tokio_util::sync::CancellationToken,
}
impl FirstError {
    pub fn new() -> Self {
        let first_error = RwLock::new(None);
        let some = tokio_util::sync::CancellationToken::new();
        Self { first_error, some }
    }

    pub fn set(&self, err: std::io::ErrorKind) {
        {
            let mut first_error = self.first_error.write().unwrap();
            if first_error.is_none() {
                *first_error = Some(err);
            }
        }
        self.some.cancel();
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        let first_error = self.first_error.read().unwrap();
        if let Some(e) = &*first_error {
            return Err(*e);
        }
        Ok(())
    }

    pub fn some(&self) -> &tokio_util::sync::CancellationToken {
        &self.some
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub reliable_layer_log_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log<'a> {
    pub time: u128,
    pub op: &'a str,

    pub tokens: f64,
    pub send_rate: f64,
    pub delivery_rate: Option<f64>,
    pub loss_rate: Option<f64>,
    pub num_tx_pkts: usize,
    pub num_pkts_in_pipe: usize,
    pub num_rt_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub cwnd: usize,
    pub num_rx_pkts: usize,
    pub recv_seq: Option<u64>,
    pub app_limited: Option<bool>,
}
