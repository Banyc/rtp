use std::{
    sync::{Mutex, RwLock},
    time::Instant,
};

use async_trait::async_trait;

use crate::{
    codec::{decode, encode, EncodeData},
    reliable_layer::ReliableLayer,
};

const MAX_ACK_BATCH_SIZE: usize = 64;
const PRINT_DEBUG_MESSAGES: bool = false;

#[derive(Debug)]
pub struct TransportLayer {
    utp_read: tokio::sync::Mutex<Box<dyn UnreliableRead>>,
    utp_write: Box<dyn UnreliableWrite>,
    reliable_layer: Mutex<ReliableLayer>,
    sent_data_packet: tokio::sync::Notify,
    recv_data_packet: tokio::sync::Notify,
    first_error: FirstError,
}
impl TransportLayer {
    pub fn new(utp_read: Box<dyn UnreliableRead>, utp_write: Box<dyn UnreliableWrite>) -> Self {
        let now = Instant::now();
        let reliable_layer = Mutex::new(ReliableLayer::new(now));
        let sent_data_packet = tokio::sync::Notify::new();
        let recv_data_packet = tokio::sync::Notify::new();
        let first_error = FirstError::new();
        Self {
            utp_read: tokio::sync::Mutex::new(utp_read),
            utp_write,
            reliable_layer,
            sent_data_packet,
            recv_data_packet,
            first_error,
        }
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
            let p = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let Some(p) = reliable_layer.send_data_packet(data_buf, Instant::now()) else {
                    break;
                };
                p
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

            {
                let now = Instant::now();
                let mut reliable_layer = self.reliable_layer.lock().unwrap();

                // UDP local -{ACK}> reliable
                reliable_layer.recv_ack_packet(ack_from_peer_buf, now);

                // UDP local -{data}> reliable
                if let Some(data) = data {
                    let ack = reliable_layer.recv_data_packet(data.seq, &utp_buf[data.buf_range]);
                    match ack {
                        true => {
                            ack_to_peer_buf.push(data.seq);
                        }
                        false => break,
                    }
                }
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
            {
                // reliable -{data}> app
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let read_bytes = reliable_layer.recv_data_buf(data);
                if PRINT_DEBUG_MESSAGES {
                    println!("recv: data: {read_bytes}");
                }
                if 0 < read_bytes {
                    break read_bytes;
                }
            }
            recv_data_packet.await;
            recv_data_packet = self.recv_data_packet.notified();
        };
        Ok(read_bytes)
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
