use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
    PollIo,
};
use rand::Rng;
use tokio::task::JoinSet;

use crate::{
    codec::in_cmd_space,
    transport_layer::{LogConfig, TransportLayer, UnreliableLayer},
};

const TIMER_INTERVAL: Duration = Duration::from_millis(1);
const BUFFER_SIZE: usize = 1024 * 64;

pub type WriteStream = PollWrite<WriteSocket>;
pub type ReadStream = PollRead<ReadSocket>;
pub type IoStream = PollIo<ReadSocket, WriteSocket>;

const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<WriteStream>;
    let _ = assert_send::<ReadStream>;
};

pub fn socket(
    unreliable_layer: UnreliableLayer,
    log_config: Option<LogConfig>,
) -> (ReadSocket, WriteSocket) {
    let read_shutdown = tokio_util::sync::CancellationToken::new();
    let write_shutdown = tokio_util::sync::CancellationToken::new();
    let transport_layer = Arc::new(TransportLayer::new(unreliable_layer, log_config));
    let mut events = JoinSet::new();

    // Send timer
    events.spawn({
        let transport_layer = Arc::clone(&transport_layer);
        async move {
            let mut data_buf = vec![0; BUFFER_SIZE];
            let mut utp_buf = vec![0; BUFFER_SIZE];
            loop {
                // tokio::time::sleep(TIMER_INTERVAL).await;
                let next_poll_time = {
                    let reliable_layer = transport_layer.reliable_layer().lock().unwrap();
                    reliable_layer.token_bucket().next_token_time()
                };
                let fast_poll_time = Instant::now() + TIMER_INTERVAL;
                let poll_time = next_poll_time.min(fast_poll_time);
                tokio::time::sleep_until(poll_time.into()).await;
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
        let read_shutdown = read_shutdown.clone();
        let transport_layer = Arc::clone(&transport_layer);
        async move {
            let mut utp_buf = vec![0; BUFFER_SIZE];
            let mut ack_from_peer_buf = vec![];
            let mut ack_to_peer_buf = vec![];
            loop {
                // Read shutdown status must be checked before moving packets to the read buf
                //   to prevent triggering sending kill packet when the read shuts down right after the packet moving.
                let is_read_shutdown = read_shutdown.is_cancelled();

                let Ok(recv_packets) = transport_layer
                    .recv_packets(&mut utp_buf, &mut ack_from_peer_buf, &mut ack_to_peer_buf)
                    .await
                else {
                    return;
                };
                if is_read_shutdown && 0 < recv_packets.num_payload_segments {
                    let _ = transport_layer.send_kill_packet().await;
                }
            }
        }
    });

    tokio::spawn({
        let read_shutdown = read_shutdown.clone();
        let write_shutdown = write_shutdown.clone();
        let transport_layer = Arc::clone(&transport_layer);
        async move {
            let _event_guard = events;

            tokio::select! {
                () = write_shutdown.cancelled() => {
                    transport_layer.send_fin_buf();
                }
                () = transport_layer.some_error().cancelled() => (),
            }
            tokio::select! {
                () = read_shutdown.cancelled() => (),
                () = transport_layer.some_error().cancelled() => (),
            }

            tokio::select! {
                () = transport_layer.recv_fin().cancelled() => (),
                () = transport_layer.some_error().cancelled() => (),
                () = tokio::time::sleep(Duration::from_secs(675)) => (),
            }
        }
    });

    let read = ReadSocket {
        transport_layer: Arc::clone(&transport_layer),
        _shutdown_guard: read_shutdown.drop_guard(),
    };
    let write = WriteSocket {
        transport_layer: Arc::clone(&transport_layer),
        data_buf: vec![0; BUFFER_SIZE],
        utp_buf: vec![0; BUFFER_SIZE],
        no_delay: true,
        _shutdown_guard: write_shutdown.drop_guard(),
    };
    (read, write)
}

#[derive(Debug)]
pub struct ReadSocket {
    transport_layer: Arc<TransportLayer>,
    _shutdown_guard: tokio_util::sync::DropGuard,
}
impl ReadSocket {
    /// Return the number of bytes sent.
    ///
    /// You probably want to receive a fix-sized message like this:
    ///
    /// ```rust
    /// use tokio::io::AsyncReadExt;
    ///
    /// async fn f(read_socket: rtp::socket::ReadSocket, message: &mut [u8]) -> std::io::Result<()> {
    ///     let mut read_stream = read_socket.into_async_read();
    ///     read_stream.read_exact(message).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer.recv(data).await
    }

    /// Undo this method:
    ///
    /// ```rust
    /// fn f(read_stream: rtp::socket::ReadStream) -> rtp::socket::ReadSocket {
    ///     read_stream.into_inner()
    /// }
    /// ```
    pub fn into_async_read(self) -> ReadStream {
        PollRead::new(self)
    }
}

#[derive(Debug)]
pub struct WriteSocket {
    transport_layer: Arc<TransportLayer>,
    data_buf: Vec<u8>,
    utp_buf: Vec<u8>,
    no_delay: bool,
    _shutdown_guard: tokio_util::sync::DropGuard,
}
impl WriteSocket {
    /// This method may only send partial `data`.
    ///
    /// Return the number of bytes sent.
    ///
    /// You probably want to send a message like this:
    ///
    /// ```rust
    /// use tokio::io::AsyncWriteExt;
    ///
    /// async fn f(write_socket: rtp::socket::WriteSocket, message: &[u8]) -> std::io::Result<()> {
    ///     let mut write_stream = write_socket.into_async_write();
    ///     write_stream.write_all(message).await
    /// }
    /// ```
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, std::io::ErrorKind> {
        self.transport_layer
            .send(data, self.no_delay, &mut self.data_buf, &mut self.utp_buf)
            .await
    }

    pub fn is_send_buf_empty(&self) -> bool {
        self.transport_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .is_send_buf_empty()
    }

    pub async fn send_buf_empty(&self) -> Result<(), std::io::ErrorKind> {
        self.transport_layer.send_buf_empty().await
    }

    /// Undo this method:
    ///
    /// ```rust
    /// fn f(write_stream: rtp::socket::WriteStream) -> rtp::socket::WriteSocket {
    ///     write_stream.into_inner()
    /// }
    /// ```
    pub fn into_async_write(self) -> WriteStream {
        PollWrite::new(self)
    }
}

pub fn unsplit(read: ReadStream, write: WriteStream) -> IoStream {
    PollIo::new(read, write)
}

impl AsyncAsyncRead for ReadSocket {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf).await.map_err(std::io::Error::from)
    }
}
impl AsyncAsyncWrite for WriteSocket {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf).await.map_err(std::io::Error::from)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.transport_layer.throw_error()?;
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.transport_layer.send_fin_buf();
        self.transport_layer.no_data_to_send().await?;
        Ok(())
    }
}

const NUM_CONNECT_RETRIES: usize = 2;
const INIT_CONNECT_RTO: Duration = Duration::from_secs(1);

pub async fn client_opening_handshake(
    unreliable: &mut UnreliableLayer,
) -> Result<(), std::io::Error> {
    let mut challenge = [0; 1];
    let mut rng = rand::rngs::OsRng;
    loop {
        rng.fill(&mut challenge);
        if !in_cmd_space(challenge[0]) {
            break;
        }
    }
    for i in 0..=NUM_CONNECT_RETRIES {
        if i == NUM_CONNECT_RETRIES {
            return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
        }
        let _ = unreliable.utp_write.send(&challenge).await?;
        let mut response = [0; 1];
        tokio::select! {
            res = unreliable.utp_read.recv(&mut response) => {
                res?;
            }
            () = tokio::time::sleep(INIT_CONNECT_RTO.mul_f64((i + 1) as f64)) => continue,
        }
        if challenge == response {
            break;
        }
    }

    neg_challenge(&mut challenge);
    let _ = unreliable.utp_write.send(&challenge).await?;
    Ok(())
}

pub async fn server_opening_handshake(
    unreliable: &mut UnreliableLayer,
) -> Result<(), std::io::Error> {
    let mut challenge = [0; 1];
    let n = unreliable.utp_read.try_recv(&mut challenge)?;
    if n != challenge.len() {
        return Err(std::io::ErrorKind::InvalidInput)?;
    }
    unreliable.utp_write.send(&challenge).await?;

    neg_challenge(&mut challenge);
    let mut response = [0; 1];
    let due = Instant::now() + INIT_CONNECT_RTO;
    loop {
        tokio::select! {
            res = unreliable.utp_read.recv(&mut response) => {
                res?;
            }
            () = tokio::time::sleep_until(due.into()) => {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut));
            }
        }
        if challenge == response {
            break;
        }
    }
    Ok(())
}

fn neg_challenge(challenge: &mut [u8]) {
    for c in challenge.iter_mut() {
        *c ^= u8::MAX;
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UdpSocket,
    };

    use crate::udp::MSS;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_io() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();

        let hello = b"hello";
        let world = b"world";

        let a = UnreliableLayer {
            utp_read: Box::new(a.clone()),
            utp_write: Box::new(a),
            mss: NonZeroUsize::new(MSS).unwrap(),
        };
        let b = UnreliableLayer {
            utp_read: Box::new(b.clone()),
            utp_write: Box::new(b),
            mss: NonZeroUsize::new(MSS).unwrap(),
        };
        let (a_r, mut a_w) = socket(a, None);
        let (b_r, mut b_w) = socket(b, None);
        a_w.send(hello).await.unwrap();
        b_w.send(world).await.unwrap();

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
        let a = UnreliableLayer {
            utp_read: Box::new(a.clone()),
            utp_write: Box::new(a),
            mss: NonZeroUsize::new(MSS).unwrap(),
        };
        let b = UnreliableLayer {
            utp_read: Box::new(b.clone()),
            utp_write: Box::new(b),
            mss: NonZeroUsize::new(MSS).unwrap(),
        };
        let (a_r, a_w) = socket(a, None);
        let (b_r, b_w) = socket(b, None);

        let mut send_buf = vec![0; 2 << 17];
        let mut recv_buf = send_buf.clone();

        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b = unsplit(b_r.into_async_read(), b_w.into_async_write());
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
