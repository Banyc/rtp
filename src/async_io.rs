use std::{
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use async_async_io::{read::AsyncAsyncRead, write::AsyncAsyncWrite};
use async_trait::async_trait;
use tokio::{net::UdpSocket, task::JoinSet};

use crate::{
    codec::{decode, encode, EncodeData},
    reliable_layer::ReliableLayer,
};

const TIMER_INTERVAL: Duration = Duration::from_millis(10);
const BUFFER_SIZE: usize = 1500;
const MAX_ACK_BATCH_SIZE: usize = 64;
const PRINT_DEBUG_MESSAGES: bool = false;

#[derive(Debug)]
pub struct AsyncIo {
    transport_layer: Arc<TransportLayer>,
    data_buf: [u8; BUFFER_SIZE],
    utp_buf: [u8; BUFFER_SIZE],
    _events: Arc<JoinSet<()>>,
}
impl AsyncIo {
    pub fn new(utp: Box<dyn UnreliableSocket>) -> Self {
        let transport_layer = Arc::new(TransportLayer::new(utp));
        let mut events = JoinSet::new();

        // Send timer
        events.spawn({
            let transport_layer = Arc::clone(&transport_layer);
            async move {
                let mut data_buf = [0; BUFFER_SIZE];
                let mut utp_buf = [0; BUFFER_SIZE];
                loop {
                    tokio::time::sleep(TIMER_INTERVAL).await;
                    if transport_layer
                        .send_packets(&mut data_buf, &mut utp_buf)
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
                let mut utp_buf = [0; BUFFER_SIZE];
                let mut ack_from_peer_buf = vec![];
                let mut ack_to_peer_buf = vec![];
                loop {
                    if transport_layer
                        .recv_packets(&mut utp_buf, &mut ack_from_peer_buf, &mut ack_to_peer_buf)
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
            utp_buf: [0; BUFFER_SIZE],
            _events: Arc::new(events),
        }
    }

    pub async fn send(&mut self, data: &[u8], no_delay: bool) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer
            .send(data, no_delay, &mut self.data_buf, &mut self.utp_buf)
            .await
    }

    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer.recv(data).await
    }
}

#[derive(Debug)]
struct TransportLayer {
    utp: Box<dyn UnreliableSocket>,
    reliable_layer: Mutex<ReliableLayer>,
    sent_data_packet: tokio::sync::Notify,
    recv_data_packet: tokio::sync::Notify,
    first_error: FirstError,
}
impl TransportLayer {
    pub fn new(utp: Box<dyn UnreliableSocket>) -> Self {
        let now = Instant::now();
        let reliable_layer = Mutex::new(ReliableLayer::new(now));
        let sent_data_packet = tokio::sync::Notify::new();
        let recv_data_packet = tokio::sync::Notify::new();
        let first_error = FirstError::new();
        Self {
            utp,
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
            let Err(e) = self.utp.send(&utp_buf[..n]).await else {
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
            let res = match ack_to_peer_buf.is_empty() {
                true => self.utp.recv(utp_buf).await,
                false => {
                    let res = self.utp.try_recv(utp_buf);
                    if let Err(e) = &res {
                        if *e == std::io::ErrorKind::WouldBlock {
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
        self.utp
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
                let n = reliable_layer.recv_data_buf(data);
                if PRINT_DEBUG_MESSAGES {
                    println!("recv: data: {n}");
                }
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

#[async_trait]
pub trait UnreliableSocket: core::fmt::Debug + Sync + Send + 'static {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind>;

    fn try_recv(&self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;

    async fn recv(&self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;
}
#[async_trait]
impl UnreliableSocket for UdpSocket {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.send(buf).await.map_err(|e| e.kind())
    }

    fn try_recv(&self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.try_recv(buf).map_err(|e| e.kind())
    }

    async fn recv(&self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.recv(buf).await.map_err(|e| e.kind())
    }
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

#[derive(Debug)]
pub struct AsyncAsyncIo {
    io: AsyncIo,
    no_delay: bool,
}
impl AsyncAsyncIo {
    pub fn new(no_delay: bool, io: AsyncIo) -> Self {
        Self { io, no_delay }
    }
}
impl AsyncAsyncRead for AsyncAsyncIo {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.io.recv(buf).await.map_err(std::io::Error::from)
    }
}
impl AsyncAsyncWrite for AsyncAsyncIo {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.io
            .send(buf, self.no_delay)
            .await
            .map_err(std::io::Error::from)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use async_async_io::{read::PollRead, write::PollWrite};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_io() {
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let hello = b"hello";
        let world = b"world";

        let mut a = AsyncIo::new(Box::new(a));
        let mut b = AsyncIo::new(Box::new(b));
        a.send(hello, true).await.unwrap();
        b.send(world, true).await.unwrap();

        let mut recv_buf = [0; BUFFER_SIZE];
        a.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..world.len()], world);
        b.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..hello.len()], hello);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_async_io() {
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = AsyncIo::new(Box::new(a));
        let b = AsyncIo::new(Box::new(b));
        let a = AsyncAsyncIo::new(true, a);
        let b = AsyncAsyncIo::new(true, b);

        let mut send_buf = vec![0; 2 << 17];
        let mut recv_buf = send_buf.clone();

        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut a = PollWrite::new(a);
        let mut b = PollRead::new(b);
        let mut transport = JoinSet::new();
        let recv_all = Arc::new(tokio::sync::Notify::new());
        transport.spawn({
            let send_buf = send_buf.clone();
            let recv_all = recv_all.clone();
            async move {
                let recv_all = recv_all.notified();
                a.write_all(&send_buf).await.unwrap();
                print!("{:?}", &a);
                recv_all.await;
            }
        });
        transport.spawn(async move {
            b.read_exact(&mut recv_buf).await.unwrap();
            assert_eq!(send_buf, recv_buf);
            recv_all.notify_waiters();
        });
        while let Some(res) = transport.join_next().await {
            res.unwrap();
        }
    }
}
