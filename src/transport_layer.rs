use std::{
    path::PathBuf,
    sync::{Mutex, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    codec::{decode, encode, EncodeData},
    reliable_layer::ReliableLayer,
};

const MAX_ACK_BATCH_SIZE: usize = 64;
const PRINT_DEBUG_MESSAGES: bool = false;

type ReliableLayerLogger = Mutex<csv::Writer<std::fs::File>>;

#[derive(Debug)]
pub struct TransportLayer {
    utp_read: tokio::sync::Mutex<Box<dyn UnreliableRead>>,
    utp_write: Box<dyn UnreliableWrite>,
    reliable_layer: Mutex<ReliableLayer>,
    sent_data_packet: tokio::sync::Notify,
    recv_data_packet: tokio::sync::Notify,
    first_error: FirstError,
    reliable_layer_logger: Option<ReliableLayerLogger>,
}
impl TransportLayer {
    pub fn new(
        utp_read: Box<dyn UnreliableRead>,
        utp_write: Box<dyn UnreliableWrite>,
        log_config: Option<LogConfig>,
    ) -> Self {
        let now = Instant::now();
        let reliable_layer = Mutex::new(ReliableLayer::new(now));
        let sent_data_packet = tokio::sync::Notify::new();
        let recv_data_packet = tokio::sync::Notify::new();
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
            utp_read: tokio::sync::Mutex::new(utp_read),
            utp_write,
            reliable_layer,
            sent_data_packet,
            recv_data_packet,
            first_error,
            reliable_layer_logger,
        }
    }

    pub fn reliable_layer(&self) -> &Mutex<ReliableLayer> {
        &self.reliable_layer
    }

    pub async fn send_packets(
        &self,
        data_buf: &mut [u8],
        utp_buf: &mut [u8],
    ) -> Result<(), std::io::ErrorKind> {
        let mut written_bytes = 0;
        loop {
            self.first_error.throw_error()?;
            // reliable -{data}> UDP remote
            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_packet(data_buf, Instant::now())
            };
            self.log("send_data_packet");
            let Some(p) = res else {
                break;
            };
            written_bytes += p.data_written.get();
            let data = EncodeData {
                seq: p.seq,
                data: &data_buf[..p.data_written.get()],
            };
            let n = encode(&[], Some(data), utp_buf).unwrap();
            let Err(e) = self.utp_write.send(&utp_buf[..n]).await else {
                continue;
            };
            self.first_error.set(e);
            self.sent_data_packet.notify_waiters();
            return Err(e);
        }
        if 0 < written_bytes {
            if PRINT_DEBUG_MESSAGES {
                println!("send_packets: data: {written_bytes}");
            }
            self.sent_data_packet.notify_waiters();
        }
        Ok(())
    }

    pub async fn recv_packets(
        &self,
        utp_buf: &mut [u8],
        ack_from_peer_buf: &mut Vec<u64>,
        ack_to_peer_buf: &mut Vec<u64>,
    ) -> Result<(), std::io::ErrorKind> {
        let throw_error = |e: std::io::ErrorKind| {
            self.first_error.set(e);
            self.recv_data_packet.notify_waiters();
            e
        };

        ack_to_peer_buf.clear();
        for _ in 0..MAX_ACK_BATCH_SIZE {
            self.first_error.throw_error()?;
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
                    return Err(throw_error(e));
                }
            };
            if PRINT_DEBUG_MESSAGES {
                println!("recv_packets: recv: {read_bytes}");
            }

            ack_from_peer_buf.clear();
            let data = match decode(&utp_buf[..read_bytes], ack_from_peer_buf) {
                Ok(x) => x,
                Err(_) => continue,
            };

            let now = Instant::now();
            let mut reliable_layer = self.reliable_layer.lock().unwrap();

            // UDP local -{ACK}> reliable
            reliable_layer.recv_ack_packet(ack_from_peer_buf, now);

            let Some(data) = data else {
                drop(reliable_layer);
                self.log("recv_ack_packet");
                continue;
            };

            // UDP local -{data}> reliable
            let ack = reliable_layer.recv_data_packet(data.seq, &utp_buf[data.buf_range]);
            drop(reliable_layer);
            self.log("recv_data_packet");
            match ack {
                true => {
                    ack_to_peer_buf.push(data.seq);
                }
                false => break,
            }
        }

        // No new data received in the reliable layer
        if ack_to_peer_buf.is_empty() {
            return Ok(());
        }

        // reliable -{ACK}> UDP remote
        let written_bytes = encode(ack_to_peer_buf, None, utp_buf).unwrap();
        self.utp_write
            .send(&utp_buf[..written_bytes])
            .await
            .map_err(throw_error)?;
        if PRINT_DEBUG_MESSAGES {
            println!("recv_packets: ack: {ack_to_peer_buf:?}");
        }

        self.recv_data_packet.notify_waiters();
        Ok(())
    }

    pub async fn send(
        &self,
        data: &[u8],
        no_delay: bool,
        data_buf: &mut [u8],
        utp_buf: &mut [u8],
    ) -> Result<usize, std::io::ErrorKind> {
        let mut sent_data_packet = self.sent_data_packet.notified();
        let written_bytes = loop {
            self.first_error.throw_error()?;
            let written_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_buf(data, Instant::now())
            };
            self.log("send_data_buf");

            if no_delay {
                self.send_packets(data_buf, utp_buf).await?;
            }

            if 0 < written_bytes {
                break written_bytes;
            }
            sent_data_packet.await;
            sent_data_packet = self.sent_data_packet.notified();
        };
        Ok(written_bytes)
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let mut recv_data_packet = self.recv_data_packet.notified();
        let read_bytes = loop {
            self.first_error.throw_error()?;

            // reliable -{data}> app
            let read_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.recv_data_buf(data)
            };
            self.log("recv_data_buf");
            if PRINT_DEBUG_MESSAGES {
                println!("recv: data: {read_bytes}");
            }
            if 0 < read_bytes {
                break read_bytes;
            }
            recv_data_packet.await;
            recv_data_packet = self.recv_data_packet.notified();
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
            num_rt_pkts: log.num_rt_pkts,
            send_seq: log.send_seq,
            min_rtt: log.min_rtt,
            rtt: log.rtt,
            num_rx_pkts: log.num_rx_pkts,
            recv_seq: log.recv_seq,
        };
        logger
            .lock()
            .unwrap()
            .serialize(&log)
            .expect("write CSV log");
    }
}

#[async_trait]
pub trait UnreliableRead: core::fmt::Debug + Sync + Send + 'static {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;
}
#[async_trait]
pub trait UnreliableWrite: core::fmt::Debug + Sync + Send + 'static {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind>;
}

#[derive(Debug)]
struct FirstError {
    first_error: RwLock<Option<std::io::ErrorKind>>,
}
impl FirstError {
    pub fn new() -> Self {
        let first_error = RwLock::new(None);
        Self { first_error }
    }

    pub fn set(&self, err: std::io::ErrorKind) {
        let mut first_error = self.first_error.write().unwrap();
        if first_error.is_none() {
            *first_error = Some(err);
        }
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        let first_error = self.first_error.read().unwrap();
        if let Some(e) = &*first_error {
            return Err(*e);
        }
        Ok(())
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
    pub loss_rate: Option<f64>,
    pub num_tx_pkts: usize,
    pub num_rt_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub num_rx_pkts: usize,
    pub recv_seq: u64,
}
