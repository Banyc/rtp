use std::sync::Arc;

use async_async_io::{
    read::{AsyncAsyncRead, PollRead},
    write::{AsyncAsyncWrite, PollWrite},
};

use super::session::SessionHandle;

use crate::io_err::IoErr;
use crate::transmission::connection::Connection;

pub type AsyncReadAdapter = PollRead<ConnReader>;

#[derive(Debug)]
pub struct AsyncWriteAdapter {
    inner: PollWrite<ConnWriter>,
    max_write_bytes: usize,
    abort_session: Arc<Connection>,
}

impl AsyncWriteAdapter {
    pub fn into_inner(self) -> ConnWriter {
        self.inner.into_inner()
    }

    pub fn inner(&self) -> &ConnWriter {
        self.inner.inner()
    }

    pub fn inner_mut(&mut self) -> &mut ConnWriter {
        self.inner.inner_mut()
    }

    pub async fn send_kill_and_abort(&mut self) {
        self.abort_session.request_kill_and_abort();
    }
}

impl std::convert::AsRef<ConnWriter> for AsyncWriteAdapter {
    fn as_ref(&self) -> &ConnWriter {
        self.inner()
    }
}

impl std::convert::AsMut<ConnWriter> for AsyncWriteAdapter {
    fn as_mut(&mut self) -> &mut ConnWriter {
        self.inner_mut()
    }
}

impl tokio::io::AsyncWrite for AsyncWriteAdapter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        let max_write_bytes = self.max_write_bytes;
        let buf = if buf.len() > max_write_bytes {
            &buf[..max_write_bytes]
        } else {
            buf
        };
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl std::marker::Unpin for AsyncWriteAdapter {}

#[cfg(test)]
impl AsyncWriteAdapter {
    pub fn max_write_bytes(&self) -> usize {
        self.max_write_bytes
    }
}

#[derive(Debug)]
pub struct IoStream {
    read: AsyncReadAdapter,
    write: AsyncWriteAdapter,
}

impl IoStream {
    pub fn into_split(self) -> (ConnReader, ConnWriter) {
        (self.read.into_inner(), self.write.into_inner())
    }

    pub fn split(&self) -> (&ConnReader, &ConnWriter) {
        (self.read.inner(), self.write.inner())
    }

    pub fn split_mut(&mut self) -> (&mut ConnReader, &mut ConnWriter) {
        (self.read.inner_mut(), self.write.inner_mut())
    }
}

impl tokio::io::AsyncRead for IoStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.read).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for IoStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.write).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.write).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.write).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub struct FrameByteReader {
    inner: AsyncReadAdapter,
}
impl tokio::io::AsyncRead for FrameByteReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

#[derive(Debug)]
pub struct FrameByteWriter {
    inner: AsyncWriteAdapter,
}
impl FrameByteWriter {
    pub async fn send_kill_and_abort(&mut self) {
        self.inner.send_kill_and_abort().await;
    }
}
impl tokio::io::AsyncWrite for FrameByteWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub(crate) struct FrameIoParts {
    read: FrameByteReader,
    write: FrameByteWriter,
}
impl FrameIoParts {
    pub(crate) fn into_parts(self) -> (FrameByteReader, FrameByteWriter) {
        (self.read, self.write)
    }
}

const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<AsyncWriteAdapter>;
    let _ = assert_send::<AsyncReadAdapter>;
    let _ = assert_send::<IoStream>;
    let _ = assert_send::<FrameByteReader>;
    let _ = assert_send::<FrameByteWriter>;
    let _ = assert_send::<FrameIoParts>;
    let _ = assert_send::<SessionHandle>;
};

pub fn unsplit(read: AsyncReadAdapter, write: AsyncWriteAdapter) -> IoStream {
    IoStream { read, write }
}

#[derive(Debug)]
pub struct ConnReader {
    pub(crate) transmission_layer: Arc<Connection>,
    pub(crate) frame_buf: Vec<u8>,
    pub(crate) _shutdown_guard: tokio_util::sync::DropGuard,
}

impl ConnReader {
    pub async fn recv(&mut self, data: &mut [u8]) -> Result<usize, IoErr> {
        if data.is_empty() {
            return Ok(0);
        }
        if !self.frame_buf.is_empty() {
            let n = self.frame_buf.len().min(data.len());
            data[..n].copy_from_slice(&self.frame_buf[..n]);
            self.frame_buf.drain(..n);
            return Ok(n);
        }
        if self
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .frame_delivery_enabled()
        {
            match self.transmission_layer.recv_frame().await? {
                Some(frame) => {
                    let n = frame.len().min(data.len());
                    data[..n].copy_from_slice(&frame[..n]);
                    if n < frame.len() {
                        self.frame_buf.extend_from_slice(&frame[n..]);
                    }
                    Ok(n)
                }
                None => Ok(0),
            }
        } else {
            self.transmission_layer.recv(data).await
        }
    }

    pub async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>, IoErr> {
        self.frame_buf.clear();
        self.transmission_layer.recv_frame().await
    }

    pub fn into_async_read(self) -> AsyncReadAdapter {
        PollRead::new(self)
    }

    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.transmission_layer.fec_recovered_symbols()
    }
}

#[derive(Debug)]
pub struct ConnWriter {
    pub(crate) transmission_layer: Arc<Connection>,
    pub(crate) _shutdown_guard: tokio_util::sync::DropGuard,
}

impl ConnWriter {
    pub async fn send(&mut self, data: &[u8]) -> Result<usize, IoErr> {
        self.transmission_layer.send(data).await
    }

    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<usize, IoErr> {
        self.transmission_layer.send_frame(frame).await
    }

    pub fn is_send_buf_empty(&self) -> bool {
        self.transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .is_send_buf_empty()
    }

    pub async fn send_buf_empty(&self) -> Result<(), IoErr> {
        self.transmission_layer.send_buf_empty().await
    }

    pub async fn send_kill_and_abort(&mut self) {
        self.transmission_layer.request_kill_and_abort();
    }

    pub fn into_async_write(self) -> AsyncWriteAdapter {
        let max_write_bytes = self
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .write_unit_capacity();
        let abort_session = Arc::clone(&self.transmission_layer);
        AsyncWriteAdapter {
            inner: PollWrite::new(self),
            max_write_bytes,
            abort_session,
        }
    }
}

pub(crate) fn into_frame_io_parts(
    read: ConnReader,
    write: ConnWriter,
) -> std::io::Result<FrameIoParts> {
    if !Arc::ptr_eq(&read.transmission_layer, &write.transmission_layer) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RTP read and write halves belong to different connections",
        ));
    }
    let read_enabled = read
        .transmission_layer
        .reliable_layer()
        .lock()
        .unwrap()
        .frame_delivery_enabled();
    let write_enabled = write
        .transmission_layer
        .reliable_layer()
        .lock()
        .unwrap()
        .frame_delivery_enabled();
    if !read_enabled || !write_enabled {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "RTP connection is not configured for frame delivery",
        ));
    }
    Ok(FrameIoParts {
        read: FrameByteReader {
            inner: read.into_async_read(),
        },
        write: FrameByteWriter {
            inner: write.into_async_write(),
        },
    })
}

impl AsyncAsyncRead for ConnReader {
    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.recv(buf)
            .await
            .map_err(|kind| self.transmission_layer.termination.io_error(kind))
    }
}

impl AsyncAsyncWrite for ConnWriter {
    async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.send(buf)
            .await
            .map_err(|kind| self.transmission_layer.termination.io_error(kind))
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.transmission_layer
            .check_error()
            .map_err(|kind| self.transmission_layer.termination.io_error(kind))?;
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        self.transmission_layer.send_fin_buf();
        self.transmission_layer.no_data_to_send().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UdpSocket,
        task::JoinSet,
    };

    use super::super::session::socket;
    use crate::transmission::test_doubles::PendingRead;
    use crate::udp::wrap_fec;

    use super::*;
    use core::time::Duration;

    #[tokio::test]
    async fn empty_stock_io_is_an_immediate_noop() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (mut a_read, mut a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (_b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), a_write.send(&[]))
                .await
                .expect("empty stock write waited")
                .unwrap(),
            0
        );
        let mut empty = [];
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(100), a_read.recv(&mut empty))
                .await
                .expect("empty stock read waited")
                .unwrap(),
            0
        );
        assert_eq!(b_write.send(b"payload").await.unwrap(), 7);
        let mut buf = [0; 16];
        let n = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("payload receive timed out")
            .expect("empty read disturbed the RTP receive path");
        assert_eq!(&buf[..n], b"payload");
    }

    #[tokio::test]
    async fn empty_generic_frame_io_is_a_noop_but_empty_frame_is_invalid() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let frame_delivery = crate::delivery::frame::FrameMode::enabled();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            false,
            crate::udp::ValidMss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            false,
            crate::udp::ValidMss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let (mut a_read, mut a_write, _a_supervisor) = socket(a_layer, None);
        let (_b_read, mut b_write, _b_supervisor) = socket(b_layer, None);
        assert_eq!(a_write.send(&[]).await.unwrap(), 0);
        assert_eq!(
            a_write.send_frame(&[]).await,
            Err(std::io::ErrorKind::InvalidInput.into())
        );
        let mut empty = [];
        assert_eq!(a_read.recv(&mut empty).await.unwrap(), 0);
        assert_eq!(b_write.send_frame(b"frame").await.unwrap(), 5);
        let frame = tokio::time::timeout(Duration::from_secs(2), a_read.recv_frame())
            .await
            .expect("frame receive timed out")
            .expect("empty generic read disturbed frame delivery")
            .expect("empty generic read falsely exposed EOF");
        assert_eq!(frame, b"frame");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_io() {
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let hello = b"hello";
        let world = b"world";
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (mut a_r, mut a_w, _a_supervisor) = socket(a, None);
        let (mut b_r, mut b_w, _b_supervisor) = socket(b, None);
        a_w.send(hello).await.unwrap();
        b_w.send(world).await.unwrap();
        let mut recv_buf = [0; 1024 * 64];
        a_r.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..world.len()], world);
        b_r.recv(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..hello.len()], hello);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_async_async_io() {
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
        let mut send_buf = vec![0; 2 << 17];
        let mut recv_buf = send_buf.clone();
        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b = unsplit(b_r.into_async_read(), b_w.into_async_write());
        let mut transmission = JoinSet::new();
        let recv_all = Arc::new(tokio::sync::Notify::new());
        transmission.spawn({
            let send_buf = send_buf.clone();
            let recv_all = recv_all.clone();
            async move {
                let recv_all = recv_all.notified();
                a.write_all(&send_buf).await.unwrap();
                println!("{a:?}");
                recv_all.await;
            }
        });
        transmission.spawn(async move {
            b.read_exact(&mut recv_buf).await.unwrap();
            assert_eq!(send_buf, recv_buf);
            recv_all.notify_waiters();
        });
        while let Some(res) = transmission.join_next().await {
            res.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss() {
        use crate::udp::testing::{BasisPoints, wrap_fec_lossy};
        let rate_a = BasisPoints::new(300);
        let rate_b = BasisPoints::new(300);
        let fec = true;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec_lossy(a.clone(), a, fec, rate_a);
        let b = wrap_fec_lossy(b.clone(), b, fec, rate_b);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
        let mut send_buf = vec![0; 4 << 20];
        let mut recv_buf = send_buf.clone();
        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b_r = b_r.into_async_read();
        let b_w = b_w.into_async_write();
        let send_buf_clone = send_buf.clone();
        let recv_done = Arc::new(tokio::sync::Notify::new());
        let recv_done_clone = recv_done.clone();
        let mut send_tasks = tokio::task::JoinSet::new();
        send_tasks.spawn(async move {
            let _b_w = b_w;
            a.write_all(&send_buf_clone).await.unwrap();
            recv_done_clone.notified().await;
            a
        });
        b_r.read_exact(&mut recv_buf).await.unwrap();
        assert_eq!(send_buf, recv_buf);
        recv_done.notify_waiters();
        send_tasks.join_next().await.unwrap().unwrap();
        let recovered = b_r.inner().fec_recovered_symbols();
        assert!(recovered.is_some(), "FEC should be enabled on receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols under 3% loss, got 0"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fec_recovers_under_loss_with_mss_8192() {
        use crate::socket::socket;
        use crate::transmission::fec_tuning::FecTuning;
        use crate::udp::testing::{BasisPoints, wrap_fec_lossy_with_mss_and_fec_tuning};
        let rate_a = BasisPoints::new(300);
        let rate_b = BasisPoints::new(300);
        let fec = true;
        let mss = 8192;
        let tuning = FecTuning::max_diversity();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(a.clone(), a, fec, mss, tuning, rate_a);
        let b_layer =
            wrap_fec_lossy_with_mss_and_fec_tuning(b.clone(), b, fec, mss, tuning, rate_b);
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (b_r, b_w, _b_supervisor) = socket(b_layer, None);
        let mut b_r = b_r;
        let mut a_w = a_w;
        let mut a_r = a_r;
        let mut b_w = b_w;
        let msg_len = 256;
        let n_msgs = 512;
        let mut sent = Vec::with_capacity(n_msgs);
        for i in 0..n_msgs {
            let mut m = vec![0u8; msg_len];
            for byte in &mut m {
                *byte = (i as u8).wrapping_add(rand::random());
            }
            sent.push(m.clone());
        }
        let sent_for_server = sent.clone();
        let mut server_tasks = tokio::task::JoinSet::new();
        server_tasks.spawn(async move {
            let mut buf = vec![0u8; msg_len];
            for expected in &sent_for_server {
                let n = b_r.recv(&mut buf).await.unwrap();
                assert_eq!(&buf[..n], expected.as_slice());
                b_w.send(&buf[..n]).await.unwrap();
            }
            b_r.fec_recovered_symbols()
        });
        for m in &sent {
            a_w.send(m).await.unwrap();
            let mut echo = vec![0u8; m.len()];
            let n = a_r.recv(&mut echo).await.unwrap();
            assert_eq!(&echo[..n], m.as_slice());
        }
        drop(a_w);
        drop(a_r);
        let recovered = server_tasks.join_next().await.unwrap().unwrap();
        assert!(recovered.is_some(), "FEC should be enabled on the receiver");
        assert!(
            recovered.unwrap() > 0,
            "FEC should recover >0 symbols under 3% loss, got 0"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_stream_max_write_bytes_scales_with_mss() {
        use crate::udp::wrap_fec;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let b_wrapped = wrap_fec(Box::new(b.clone()), Box::new(b), false);
        let (_b_r, b_w, _b_supervisor) = socket(b_wrapped, None);
        assert_eq!(
            b_w.into_async_write().max_write_bytes(),
            8 * 1024,
            "default-MSS staging buffer must be exactly 8 KiB"
        );
        let mss = 9_000;
        let a = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            false,
            crate::udp::ValidMss::try_new(mss).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            crate::delivery::frame::FrameMode::default(),
        )
        .unwrap();
        let (_a_r, a_w, _a_supervisor) = socket(a, None);
        let write_stream = a_w.into_async_write();
        let max_write_bytes = write_stream.max_write_bytes();
        let per_packet_payload = mss - crate::codec::data_overhead();
        assert_eq!(
            max_write_bytes % per_packet_payload,
            0,
            "max_write_bytes {max_write_bytes} should be a multiple of per-packet payload {per_packet_payload}"
        );
        assert!(
            8 * 1024 < max_write_bytes,
            "max_write_bytes {max_write_bytes} should exceed the default staging buffer of 8 KiB"
        );
        drop(write_stream);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_stream_stages_at_most_the_send_buf_capacity() {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (_b_r, b_w, _b_supervisor) = socket(b, None);
        let _a_r = a_r;
        let capacity = a_w
            .transmission_layer
            .reliable_layer()
            .lock()
            .unwrap()
            .send_data_buf_capacity();
        let mut write_stream = a_w.into_async_write();
        let big = vec![0u8; capacity * 4];
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let pinned = Pin::new(&mut write_stream);
        let poll = pinned.poll_write(&mut cx, &big);
        let n = match poll {
            Poll::Ready(Ok(n)) => n,
            Poll::Pending => 0,
            Poll::Ready(Err(e)) => panic!("poll_write failed: {e:?}"),
        };
        assert!(
            n <= capacity,
            "poll_write consumed {n} bytes, but capacity is {capacity}"
        );
        drop(write_stream);
        drop(b_w);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_mode_async_write_produces_one_frame() {
        use crate::delivery::frame::FrameMode;
        use tokio::io::AsyncWriteExt;
        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameMode::enabled();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            fec,
            crate::udp::ValidMss::try_new(mss).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            fec,
            crate::udp::ValidMss::try_new(mss).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        )
        .unwrap();
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (mut b_r, _b_w, _b_supervisor) = socket(b_layer, None);
        let frame_size = 16 * 1024;
        let payload: Vec<u8> = (0..frame_size).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let mut a_stream = a_w.into_async_write();
        let mut send_tasks = tokio::task::JoinSet::new();
        send_tasks.spawn(async move {
            a_stream.write_all(&payload).await.unwrap();
            a_stream.shutdown().await.ok();
            a_stream
        });
        let frame = tokio::time::timeout(Duration::from_secs(5), b_r.recv_frame())
            .await
            .expect("recv_frame timed out")
            .expect("recv_frame failed")
            .expect("expected a frame, got EOF");
        assert_eq!(
            frame.len(),
            frame_size,
            "receiver must get exactly one frame of the original size"
        );
        assert_eq!(frame, expected, "frame contents must match");
        drop(a_r);
        drop(send_tasks.join_next().await.unwrap().unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_delivery_io_conversion_rejects_stock_mode() {
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a = wrap_fec(Box::new(a.clone()), Box::new(a), fec);
        let b = wrap_fec(Box::new(b.clone()), Box::new(b), fec);
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (_b_r, _b_w, _b_supervisor) = socket(b, None);
        let result = into_frame_io_parts(a_r, a_w);
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not configured for frame delivery"),
            "stock-mode connections must be rejected"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn frame_delivery_io_preserves_frames_across_async_io() {
        use crate::delivery::frame::FrameMode;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let fec = false;
        let mss = crate::udp::NO_FEC_MSS;
        let fd = FrameMode::enabled();
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            fec,
            crate::udp::ValidMss::try_new(mss).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            fec,
            crate::udp::ValidMss::try_new(mss).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            fd,
        )
        .unwrap();
        let (a_r, a_w, _a_supervisor) = socket(a_layer, None);
        let (b_r, b_w, _b_supervisor) = socket(b_layer, None);
        let mut a_io = into_frame_io_parts(a_r, a_w)
            .expect("frame delivery halves must convert")
            .into_parts();
        let mut b_io = into_frame_io_parts(b_r, b_w)
            .expect("frame delivery halves must convert")
            .into_parts();
        let first = b"first";
        let second = b"second-frame";
        a_io.1.write_all(first).await.unwrap();
        a_io.1.write_all(second).await.unwrap();
        a_io.1.flush().await.unwrap();
        a_io.1.shutdown().await.ok();
        let mut buf = vec![0u8; 256];
        let n1 = tokio::time::timeout(Duration::from_secs(5), b_io.0.read(&mut buf))
            .await
            .expect("first read timed out")
            .expect("first read failed");
        assert_eq!(&buf[..n1], first, "first frame must match");
        let n2 = tokio::time::timeout(Duration::from_secs(5), b_io.0.read(&mut buf))
            .await
            .expect("second read timed out")
            .expect("second read failed");
        assert_eq!(&buf[..n2], second, "second frame must match");
    }

    #[tokio::test]
    async fn shutdown_rejects_later_write_and_preserves_read_half() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let (mut a_read, a_write, _a_supervisor) =
            socket(wrap_fec(Box::new(a.clone()), Box::new(a), false), None);
        let (mut b_read, mut b_write, _b_supervisor) =
            socket(wrap_fec(Box::new(b.clone()), Box::new(b), false), None);
        let mut a_write = a_write.into_async_write();
        a_write.write_all(b"request").await.unwrap();
        a_write.shutdown().await.unwrap();
        assert_eq!(
            a_write.write(b"after FIN").await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            a_write.write(&[]).await.unwrap_err().kind(),
            std::io::ErrorKind::BrokenPipe
        );
        let mut buf = [0; 64];
        let request_len = tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
            .await
            .expect("request receive timed out")
            .expect("request receive failed");
        assert_eq!(&buf[..request_len], b"request");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), b_read.recv(&mut buf))
                .await
                .expect("FIN receive timed out")
                .expect("FIN receive failed"),
            0
        );
        assert_eq!(b_write.send(b"response").await.unwrap(), 8);
        let response_len = tokio::time::timeout(Duration::from_secs(2), a_read.recv(&mut buf))
            .await
            .expect("response receive timed out")
            .expect("write shutdown must preserve the RTP read half");
        assert_eq!(&buf[..response_len], b"response");
    }

    #[tokio::test]
    async fn recv_frame_discards_a_partially_consumed_frame_tail() {
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let frame_delivery = crate::delivery::frame::FrameMode::enabled();
        let a_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(a.clone()),
            Box::new(a),
            false,
            crate::udp::ValidMss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let b_layer = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(b.clone()),
            Box::new(b),
            false,
            crate::udp::ValidMss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::transmission::fec_tuning::FecTuning::default(),
            frame_delivery,
        )
        .unwrap();
        let (mut a_read, _a_write, _a_supervisor) = socket(a_layer, None);
        let (_b_read, mut b_write, _b_supervisor) = socket(b_layer, None);
        assert_eq!(b_write.send_frame(b"abcdef").await.unwrap(), 6);
        assert_eq!(b_write.send_frame(b"XYZ").await.unwrap(), 3);
        let mut prefix = [0; 2];
        assert_eq!(a_read.recv(&mut prefix).await.unwrap(), 2);
        assert_eq!(&prefix, b"ab");
        let next = tokio::time::timeout(Duration::from_secs(2), a_read.recv_frame())
            .await
            .expect("next frame receive timed out")
            .expect("frame receive failed")
            .expect("unexpected EOF");
        assert_eq!(next, b"XYZ");
        assert_eq!(b_write.send_frame(b"next").await.unwrap(), 4);
        let mut next = [0; 4];
        assert_eq!(a_read.recv(&mut next).await.unwrap(), 4);
        assert_eq!(&next, b"next");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cancelling_public_send_does_not_cancel_driver_io() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        #[derive(Debug)]
        struct WriteState {
            calls: AtomicUsize,
            first_started: tokio::sync::Notify,
            release_first: tokio::sync::Notify,
            first_completed: tokio::sync::Notify,
            first_cancelled: AtomicBool,
        }
        struct CancelProbe {
            state: Arc<WriteState>,
            completed: bool,
        }
        impl Drop for CancelProbe {
            fn drop(&mut self) {
                if !self.completed {
                    self.state.first_cancelled.store(true, Ordering::SeqCst);
                }
            }
        }
        #[derive(Debug)]
        struct DriverWrite {
            state: Arc<WriteState>,
        }
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for DriverWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                if self.state.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let mut probe = CancelProbe {
                        state: Arc::clone(&self.state),
                        completed: false,
                    };
                    self.state.first_started.notify_one();
                    self.state.release_first.notified().await;
                    probe.completed = true;
                    self.state.first_completed.notify_one();
                }
                Ok(buf.len())
            }
        }
        let state = Arc::new(WriteState {
            calls: AtomicUsize::new(0),
            first_started: tokio::sync::Notify::new(),
            release_first: tokio::sync::Notify::new(),
            first_completed: tokio::sync::Notify::new(),
            first_cancelled: AtomicBool::new(false),
        });
        let layer = wrap_fec(
            Box::new(PendingRead),
            Box::new(DriverWrite {
                state: Arc::clone(&state),
            }),
            false,
        );
        let (_read, mut write, _supervisor) = socket(layer, None);
        let payload = vec![7; 64 * 1024];
        assert!(write.send(&payload).await.unwrap() > 0);
        tokio::time::timeout(Duration::from_millis(100), state.first_started.notified())
            .await
            .expect("writer driver must start the first unreliable send");
        assert!(write.send(&payload).await.unwrap() > 0);
        let mut blocked_send = Box::pin(write.send(&payload));
        tokio::select! {
            result = &mut blocked_send => panic!("send queue unexpectedly had capacity: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
        drop(blocked_send);
        assert!(
            !state.first_cancelled.load(Ordering::SeqCst),
            "cancelling a public send waiter must not cancel driver-owned I/O"
        );
        state.release_first.notify_one();
        tokio::time::timeout(Duration::from_millis(100), state.first_completed.notified())
            .await
            .expect("released driver I/O must complete");
        assert!(!state.first_cancelled.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(1), write.send(b"after cancellation"))
            .await
            .expect("send queue must resume after driver progress")
            .expect("send after cancellation must succeed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn abort_is_callable_while_poll_write_retains_the_write_socket() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct WriteState {
            calls: AtomicUsize,
            first_started: tokio::sync::Notify,
            release_first: tokio::sync::Notify,
            kill_started: tokio::sync::Notify,
        }
        #[derive(Debug)]
        struct BlockingFirstWrite(Arc<WriteState>);
        #[async_trait::async_trait]
        impl crate::transmission::transmission_layer::UnreliableWrite for BlockingFirstWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                let call = self.0.calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    self.0.first_started.notify_one();
                    self.0.release_first.notified().await;
                } else {
                    self.0.kill_started.notify_one();
                }
                Ok(buf.len())
            }
        }
        let state = Arc::new(WriteState {
            calls: AtomicUsize::new(0),
            first_started: tokio::sync::Notify::new(),
            release_first: tokio::sync::Notify::new(),
            kill_started: tokio::sync::Notify::new(),
        });
        let layer = wrap_fec(
            Box::new(PendingRead),
            Box::new(BlockingFirstWrite(Arc::clone(&state))),
            false,
        );
        let (_read, write, _supervisor) = socket(layer, None);
        let mut write = write.into_async_write();
        let payload = vec![7; write.max_write_bytes()];
        assert!(write.write(&payload).await.unwrap() > 0);
        tokio::time::timeout(Duration::from_secs(1), state.first_started.notified())
            .await
            .expect("driver did not start its first unreliable write");
        let mut retained_pending_write = false;
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_millis(20), write.write(&payload)).await {
                Ok(Ok(n)) => assert!(n > 0),
                Ok(Err(error)) => panic!("staging failed unexpectedly: {error}"),
                Err(_) => {
                    retained_pending_write = true;
                    break;
                }
            }
        }
        assert!(
            retained_pending_write,
            "test did not leave PollWrite owning a pending ConnWriter"
        );
        write.send_kill_and_abort().await;
        state.release_first.notify_one();
        tokio::time::timeout(Duration::from_secs(1), state.kill_started.notified())
            .await
            .expect("driver did not observe the out-of-band abort request");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_bulk_transfer_survives_loss_reorder_and_duplication() {
        use crate::udp::testing::{ImpairRate, wrap_fec_impaired};
        for fec in [false, true] {
            let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            a.connect(b.local_addr().unwrap()).await.unwrap();
            b.connect(a.local_addr().unwrap()).await.unwrap();
            let rate_a = ImpairRate::new(1500, 3000, 1000);
            let rate_b = ImpairRate::new(1500, 3000, 1000);
            let a = wrap_fec_impaired(a.clone(), a, fec, rate_a.clone());
            let b = wrap_fec_impaired(b.clone(), b, fec, rate_b.clone());
            let (a_r, a_w, _a_supervisor) = socket(a, None);
            let (b_r, b_w, _b_supervisor) = socket(b, None);
            let mut send_buf = vec![0u8; 1 << 20];
            for byte in &mut send_buf {
                *byte = rand::random();
            }
            let mut recv_buf = vec![0u8; send_buf.len()];
            let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
            let mut b_r = b_r.into_async_read();
            let b_w = b_w.into_async_write();
            let expected = send_buf.clone();
            let recv_done = Arc::new(tokio::sync::Notify::new());
            let mut send_tasks = tokio::task::JoinSet::new();
            send_tasks.spawn({
                let recv_done = recv_done.clone();
                async move {
                    let _b_w = b_w;
                    a.write_all(&send_buf).await.unwrap();
                    recv_done.notified().await;
                    a
                }
            });
            tokio::time::timeout(Duration::from_secs(120), b_r.read_exact(&mut recv_buf))
                .await
                .unwrap_or_else(|_| panic!("fec={fec}: the transfer stalled"))
                .unwrap();
            assert_eq!(expected, recv_buf, "fec={fec}: the stream arrived corrupt");
            recv_done.notify_waiters();
            send_tasks.join_next().await.unwrap().unwrap();
            let (dropped, reordered, duplicated) = rate_a.applied();
            assert!(
                dropped > 0 && reordered > 0 && duplicated > 0,
                "fec={fec}: the data path was not actually impaired (dropped {dropped}, reordered {reordered}, duplicated {duplicated})"
            );
            let (dropped, reordered, duplicated) = rate_b.applied();
            assert!(
                dropped + reordered + duplicated > 0,
                "fec={fec}: the acknowledgement path was not impaired at all"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_packet_arriving_twice_does_not_duplicate_a_byte() {
        use crate::udp::testing::{ImpairRate, wrap_fec_impaired};
        let fec = false;
        let a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        a.connect(b.local_addr().unwrap()).await.unwrap();
        b.connect(a.local_addr().unwrap()).await.unwrap();
        let rate_a = ImpairRate::new(0, 0, 10_000);
        let a = wrap_fec_impaired(a.clone(), a, fec, rate_a.clone());
        let b = wrap_fec_impaired(b.clone(), b, fec, ImpairRate::new(0, 0, 10_000));
        let (a_r, a_w, _a_supervisor) = socket(a, None);
        let (b_r, b_w, _b_supervisor) = socket(b, None);
        let mut send_buf = vec![0u8; 1 << 18];
        for byte in &mut send_buf {
            *byte = rand::random();
        }
        let mut recv_buf = vec![0u8; send_buf.len()];
        let mut a = unsplit(a_r.into_async_read(), a_w.into_async_write());
        let mut b_r = b_r.into_async_read();
        let b_w = b_w.into_async_write();
        let expected = send_buf.clone();
        let recv_done = Arc::new(tokio::sync::Notify::new());
        let mut send_tasks = tokio::task::JoinSet::new();
        send_tasks.spawn({
            let recv_done = recv_done.clone();
            async move {
                let _b_w = b_w;
                a.write_all(&send_buf).await.unwrap();
                recv_done.notified().await;
                a
            }
        });
        tokio::time::timeout(Duration::from_secs(120), b_r.read_exact(&mut recv_buf))
            .await
            .expect("the transfer stalled with every packet duplicated")
            .unwrap();
        assert_eq!(expected, recv_buf);
        recv_done.notify_waiters();
        send_tasks.join_next().await.unwrap().unwrap();
        let (_, _, duplicated) = rate_a.applied();
        assert!(duplicated > 0, "no packet was actually duplicated");
    }
}
