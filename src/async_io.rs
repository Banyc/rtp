use std::{
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use tokio::{net::UdpSocket, task::JoinSet};

use crate::{
    codec::{decode, encode, EncodeData},
    reliable_layer::ReliableLayer,
};

const TIMER_INTERVAL: Duration = Duration::from_millis(10);
const BUFFER_SIZE: usize = 1500;

#[derive(Debug)]
pub struct AsyncIo {
    transport_layer: Arc<TransportLayer>,
    data_buf: [u8; BUFFER_SIZE],
    udp_buf: [u8; BUFFER_SIZE],
    _events: Arc<JoinSet<()>>,
}
impl AsyncIo {
    pub fn new(udp: UdpSocket) -> Self {
        let transport_layer = Arc::new(TransportLayer::new(udp));
        let mut events = JoinSet::new();

        // Send timer
        events.spawn({
            let transport_layer = Arc::clone(&transport_layer);
            async move {
                let mut data_buf = [0; BUFFER_SIZE];
                let mut udp_buf = [0; BUFFER_SIZE];
                loop {
                    tokio::time::sleep(TIMER_INTERVAL).await;
                    if transport_layer
                        .send_packets(&mut data_buf, &mut udp_buf)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        // Recv
        events.spawn({
            let transport_layer = Arc::clone(&transport_layer);
            async move {
                let mut udp_buf = [0; BUFFER_SIZE];
                let mut ack_from_peer_buf = vec![];
                let mut ack_to_peer_buf = vec![];
                loop {
                    if transport_layer
                        .recv_packets(&mut udp_buf, &mut ack_from_peer_buf, &mut ack_to_peer_buf)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        Self {
            transport_layer,
            data_buf: [0; BUFFER_SIZE],
            udp_buf: [0; BUFFER_SIZE],
            _events: Arc::new(events),
        }
    }

    pub async fn send(&mut self, data: &[u8], no_delay: bool) -> Result<(), std::io::ErrorKind> {
        self.transport_layer
            .send(data, no_delay, &mut self.data_buf, &mut self.udp_buf)
            .await
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer.recv(data).await
    }
}

#[derive(Debug)]
struct TransportLayer {
    udp: UdpSocket,
    reliable_layer: Mutex<ReliableLayer>,
    sent_data_packet: tokio::sync::Notify,
    recv_data_packet: tokio::sync::Notify,
    first_error: FirstError,
}
impl TransportLayer {
    pub fn new(udp: UdpSocket) -> Self {
        let now = Instant::now();
        let reliable_layer = Mutex::new(ReliableLayer::new(now));
        let udp = udp;
        let sent_data_packet = tokio::sync::Notify::new();
        let recv_data_packet = tokio::sync::Notify::new();
        let first_error = FirstError::new();
        Self {
            udp,
            reliable_layer,
            sent_data_packet,
            recv_data_packet,
            first_error,
        }
    }

    pub async fn send_packets(
        &self,
        data_buf: &mut [u8],
        udp_buf: &mut [u8],
    ) -> Result<(), std::io::ErrorKind> {
        let mut written = 0;
        loop {
            self.first_error.throw_error()?;
            let p = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let Some(p) = reliable_layer.send_data_packet(data_buf, Instant::now()) else {
                    break;
                };
                p
            };
            written += p.data_written.get();
            let data = EncodeData {
                seq: p.seq,
                data: &data_buf[..p.data_written.get()],
            };
            let n = encode(&[], Some(data), udp_buf).unwrap();
            let Err(e) = self.udp.send(&udp_buf[..n]).await else {
                continue;
            };
            let kind = e.kind();
            self.first_error.set(e);
            self.sent_data_packet.notify_waiters();
            return Err(kind);
        }
        if 0 < written {
            self.sent_data_packet.notify_waiters();
        }
        Ok(())
    }

    pub async fn recv_packets(
        &self,
        udp_buf: &mut [u8],
        ack_from_peer_buf: &mut Vec<u64>,
        ack_to_peer_buf: &mut Vec<u64>,
    ) -> Result<(), std::io::ErrorKind> {
        let throw_error = |e: std::io::Error| {
            let kind = e.kind();
            self.first_error.set(e);
            self.recv_data_packet.notify_waiters();
            kind
        };

        ack_to_peer_buf.clear();
        loop {
            self.first_error.throw_error()?;
            let res = match ack_to_peer_buf.is_empty() {
                true => self.udp.recv(udp_buf).await,
                false => {
                    let res = self.udp.try_recv(udp_buf);
                    if let Err(e) = &res {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            break;
                        }
                    }
                    res
                }
            };
            let read_bytes = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err(throw_error(e));
                }
            };

            ack_from_peer_buf.clear();
            let decoded = match decode(&udp_buf[..read_bytes], ack_from_peer_buf) {
                Ok(x) => x,
                Err(_) => return Ok(()),
            };

            {
                let now = Instant::now();
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.recv_ack_packet(ack_from_peer_buf, now);

                let Some(decoded) = decoded else {
                    return Ok(());
                };
                let ack = reliable_layer.recv_data_packet(&udp_buf[decoded.buf_range]);
                if ack {
                    ack_to_peer_buf.push(decoded.seq);
                }
            }
        }

        assert!(!ack_to_peer_buf.is_empty());

        let written_bytes = encode(ack_to_peer_buf, None, udp_buf).unwrap();
        self.udp
            .send(&udp_buf[..written_bytes])
            .await
            .map_err(throw_error)?;

        self.recv_data_packet.notify_waiters();
        Ok(())
    }

    pub async fn send(
        &self,
        data: &[u8],
        no_delay: bool,
        data_buf: &mut [u8],
        udp_buf: &mut [u8],
    ) -> Result<(), std::io::ErrorKind> {
        let mut sent_data_packet = self.sent_data_packet.notified();
        self.first_error.throw_error()?;
        let mut written_bytes = 0;
        loop {
            {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                written_bytes +=
                    reliable_layer.send_data_buf(&data[written_bytes..], Instant::now());
            }

            if no_delay {
                self.send_packets(data_buf, udp_buf).await?;
            }

            if data.len() == written_bytes {
                break;
            }
            sent_data_packet.await;
            sent_data_packet = self.sent_data_packet.notified();
        }
        Ok(())
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let mut recv_data_packet = self.recv_data_packet.notified();
        self.first_error.throw_error()?;
        let read_bytes = loop {
            {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                let n = reliable_layer.recv_data_buf(data);
                if 0 < n {
                    break n;
                }
            }
            recv_data_packet.await;
            recv_data_packet = self.recv_data_packet.notified();
        };
        Ok(read_bytes)
    }
}

#[derive(Debug)]
struct FirstError {
    first_error: RwLock<Option<std::io::Error>>,
}
impl FirstError {
    pub fn new() -> Self {
        let first_error = RwLock::new(None);
        Self { first_error }
    }

    pub fn set(&self, err: std::io::Error) {
        let mut first_error = self.first_error.write().unwrap();
        if first_error.is_none() {
            *first_error = Some(err);
        }
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        let first_error = self.first_error.read().unwrap();
        if let Some(e) = &*first_error {
            return Err(e.kind());
        }
        Ok(())
    }
}
