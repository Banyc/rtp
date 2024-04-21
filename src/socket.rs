use std::{sync::Arc, time::Duration};

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
    PollIo,
};
use tokio::task::JoinSet;

use crate::transport_layer::{TransportLayer, UnreliableRead, UnreliableWrite};

const TIMER_INTERVAL: Duration = Duration::from_millis(10);
const BUFFER_SIZE: usize = 1500;

pub fn socket(
    utp_read: Box<dyn UnreliableRead>,
    utp_write: Box<dyn UnreliableWrite>,
) -> (ReadSocket, WriteSocket) {
    let transport_layer = Arc::new(TransportLayer::new(utp_read, utp_write));
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

    let events = Arc::new(events);

    let read = ReadSocket {
        transport_layer: Arc::clone(&transport_layer),
        _events: Arc::clone(&events),
    };
    let write = WriteSocket {
        transport_layer: Arc::clone(&transport_layer),
        data_buf: [0; BUFFER_SIZE],
        utp_buf: [0; BUFFER_SIZE],
        _events: Arc::clone(&events),
    };
    (read, write)
}

#[derive(Debug)]
pub struct ReadSocket {
    transport_layer: Arc<TransportLayer>,
    _events: Arc<JoinSet<()>>,
}
impl ReadSocket {
    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer.recv(data).await
    }

    pub fn into_async_read(self) -> PollRead<Self> {
        PollRead::new(self)
    }
}

#[derive(Debug)]
pub struct WriteSocket {
    transport_layer: Arc<TransportLayer>,
    data_buf: [u8; BUFFER_SIZE],
    utp_buf: [u8; BUFFER_SIZE],
    _events: Arc<JoinSet<()>>,
}
impl WriteSocket {
    pub async fn send(&mut self, data: &[u8], no_delay: bool) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer
            .send(data, no_delay, &mut self.data_buf, &mut self.utp_buf)
            .await
    }

    pub fn into_async_write(self, no_delay: bool) -> PollWrite<WriteSocket2> {
        let write = WriteSocket2 {
            write: self,
            no_delay,
        };
        PollWrite::new(write)
    }
}

pub fn unsplit(
    read: PollRead<ReadSocket>,
    write: PollWrite<WriteSocket2>,
) -> PollIo<ReadSocket, WriteSocket2> {
    PollIo::new(read, write)
}

#[derive(Debug)]
pub struct WriteSocket2 {
    write: WriteSocket,
    no_delay: bool,
}
impl WriteSocket2 {
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.write.send(data, self.no_delay).await
    }
}

impl AsyncAsyncRead for ReadSocket {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf).await.map_err(std::io::Error::from)
    }
}
impl AsyncAsyncWrite for WriteSocket2 {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write
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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UdpSocket,
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_io() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let hello = b"hello";
        let world = b"world";

        let (a_r, mut a_w) = socket(Box::new(a.clone()), Box::new(a));
        let (b_r, mut b_w) = socket(Box::new(b.clone()), Box::new(b));
        a_w.send(hello, true).await.unwrap();
        b_w.send(world, true).await.unwrap();

        let mut recv_buf = [0; BUFFER_SIZE];
        a_r.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..world.len()], world);
        b_r.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..hello.len()], hello);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_async_io() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (a_r, a_w) = socket(Box::new(a.clone()), Box::new(a));
        let (b_r, b_w) = socket(Box::new(b.clone()), Box::new(b));

        let mut send_buf = vec![0; 2 << 17];
        let mut recv_buf = send_buf.clone();

        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write(true));
        let mut b = unsplit(b_r.into_async_read(), b_w.into_async_write(true));
        let mut transport = JoinSet::new();
        let recv_all = Arc::new(tokio::sync::Notify::new());
        transport.spawn({
            let send_buf = send_buf.clone();
            let recv_all = recv_all.clone();
            async move {
                let recv_all = recv_all.notified();
                a.write_all(&send_buf).await.unwrap();
                println!("{:?}", &a);
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
