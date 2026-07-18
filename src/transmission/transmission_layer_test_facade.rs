use core::time::Duration;
use std::sync::Arc;
use std::time::Instant;

use super::read_half::ReadHalf;
use super::shared::Shared;
use super::shared::{build_parts, build_parts_with_watchdog_tuning};
use super::transmission_layer::{
    LogConfig, RecvBufs, RecvPkts, SendBufs, SendKillPkt, UnreliableLayer, UnreliableRead,
    UnreliableWrite,
};
use super::write_half::WriteHalf;
use async_trait::async_trait;

#[cfg(test)]
pub struct TransmissionLayer {
    pub(crate) shared: Arc<Shared>,
    pub(crate) write_half: WriteHalf,
    pub(crate) read_half: ReadHalf,
    pub(crate) _termination_reaper: super::termination::TerminationReaper,
}

#[cfg(test)]
impl std::ops::Deref for TransmissionLayer {
    type Target = Shared;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

#[cfg(test)]
impl TransmissionLayer {
    pub fn new(unreliable_layer: UnreliableLayer, log_config: Option<LogConfig>) -> Self {
        let (shared, write_half, read_half, termination_reaper) =
            build_parts(unreliable_layer, log_config);
        Self {
            shared,
            write_half,
            read_half,
            _termination_reaper: termination_reaper,
        }
    }

    pub fn new_with_watchdog_tuning(
        unreliable_layer: UnreliableLayer,
        tuning: crate::transmission::watchdog_tuning::WatchdogTuning,
    ) -> Self {
        let (shared, write_half, read_half, termination_reaper) =
            build_parts_with_watchdog_tuning(unreliable_layer, None, tuning);
        Self {
            shared,
            write_half,
            read_half,
            _termination_reaper: termination_reaper,
        }
    }

    /// Test-only: force the `RTP_RTX_DUP` toggle to a fixed value regardless
    /// of the process environment, so parallel tests in the same binary do not
    /// race on the env var.
    pub(crate) fn set_rtx_dup_for_test(&mut self, enabled: bool) {
        self.shared
            .rtx_dup
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test-only: force the `RTP_INSTREAM_GROUP_FEC` toggle to a fixed value
    /// regardless of the process environment, so parallel tests in the same
    /// binary do not race on the env var.
    pub(crate) fn set_instream_group_fec_for_test(&mut self, enabled: bool) {
        self.shared
            .instream_group_fec_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test-only: take up to `n` tokens from the shared send-rate limiter so
    /// the retransmission-armor duplicate-copy token gate can be exercised
    /// (the primary rtx bypasses the bucket; the dup needs a token).
    pub(crate) fn drain_rate_limiter_for_test(&self, n: usize, now: Instant) -> usize {
        self.shared
            .send_rate_limiter
            .lock()
            .unwrap()
            .take_at_most_tokens(n, now)
    }

    pub async fn send_pkts(&mut self, bufs: &mut SendBufs) -> Result<bool, std::io::ErrorKind> {
        self.write_half.send_pkts(bufs).await
    }

    pub async fn flush_acks(&mut self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        self.write_half.flush_acks(bufs).await
    }

    pub fn has_pending_acks(&self) -> bool {
        self.write_half.has_pending_acks()
    }

    pub async fn send_kill_pkt(&mut self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        self.write_half.send_kill_pkt(bufs).await
    }

    pub async fn send_kill_and_abort(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Result<(), std::io::ErrorKind> {
        self.write_half.send_kill_and_abort(bufs).await
    }

    pub async fn recv_pkts(
        &mut self,
        bufs: &mut RecvBufs,
    ) -> Result<RecvPkts, (std::io::ErrorKind, SendKillPkt)> {
        self.read_half.recv_pkts(bufs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    struct RecordingWrite {
        sent: Mutex<Vec<Vec<u8>>>,
    }
    impl RecordingWrite {
        fn push(&self, b: Vec<u8>) {
            self.sent.lock().unwrap().push(b);
        }
        fn count(&self) -> usize {
            self.sent.lock().unwrap().len()
        }
        fn datagrams(&self) -> Vec<Vec<u8>> {
            self.sent.lock().unwrap().clone()
        }
    }

    #[derive(Debug)]
    struct BlackholeRead;
    #[async_trait]
    impl UnreliableRead for BlackholeRead {
        fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            Err(std::io::ErrorKind::WouldBlock)
        }
        async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
            Err(std::io::ErrorKind::WouldBlock)
        }
    }

    fn settle_rtt(tl: &TransmissionLayer, rtt: Duration, n: usize) {
        let rl = tl.reliable_layer();
        let mut rl = rl.lock().unwrap();
        let mut t = Instant::now();
        for _ in 0..n {
            rl.sample_rtt(rtt, t);
            t += Duration::from_micros(100);
        }
    }

    fn send_one_packet(tl: &TransmissionLayer, now: Instant) -> u64 {
        let rl = tl.reliable_layer();
        let mut rl = rl.lock().unwrap();
        let payload = vec![0u8; 100];
        assert_eq!(
            rl.send_data_buf(&payload, now).unwrap(),
            payload.len(),
            "send_data_buf must accept the payload"
        );
        let mut pkt = vec![0u8; crate::udp::NO_FEC_MSS];
        let p = rl
            .send_data_pkt(&mut pkt, now)
            .expect("send_data_pkt must send a packet");
        match p.data_written {
            crate::reliable::reliable_layer::DataPktPayload::Data(_) => p.seq,
            _ => panic!("expected data packet"),
        }
    }

    #[tokio::test]
    async fn accepted_out_of_order_fin_publishes_fin_before_eof() {
        #[derive(Debug)]
        struct OneDatagramRead(Option<Vec<u8>>);
        impl OneDatagramRead {
            fn take(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let Some(datagram) = self.0.take() else {
                    return Err(std::io::ErrorKind::WouldBlock);
                };
                buf[..datagram.len()].copy_from_slice(&datagram);
                Ok(datagram.len())
            }
        }
        #[async_trait]
        impl UnreliableRead for OneDatagramRead {
            fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                self.take(buf)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                self.take(buf)
            }
        }
        #[derive(Debug)]
        struct ImmediateWrite;
        #[async_trait]
        impl UnreliableWrite for ImmediateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                Ok(buf.len())
            }
        }
        let mut datagram = vec![0; 64];
        let fin = crate::codec::EncodeData {
            seq: 1,
            send_ts: None,
            frame_len: None,
            data: &[],
        };
        let len = crate::codec::encode_ack_data(None, None, Some(fin), &mut datagram).unwrap();
        datagram.truncate(len);
        let layer = crate::udp::wrap_fec(OneDatagramRead(Some(datagram)), ImmediateWrite, false);
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        assert!(
            transmission.recv_fin().is_cancelled(),
            "an accepted FIN must be published even while an earlier sequence is missing"
        );
        assert!(
            !transmission.recv_eof().is_cancelled(),
            "an out-of-order FIN must not publish application EOF"
        );
    }

    #[tokio::test]
    async fn duplicate_payload_sequence_reshaped_as_fin_does_not_publish_fin() {
        #[derive(Debug)]
        struct DatagramQueue(std::collections::VecDeque<Vec<u8>>);
        impl DatagramQueue {
            fn take(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let Some(datagram) = self.0.pop_front() else {
                    return Err(std::io::ErrorKind::WouldBlock);
                };
                buf[..datagram.len()].copy_from_slice(&datagram);
                Ok(datagram.len())
            }
        }
        #[async_trait]
        impl UnreliableRead for DatagramQueue {
            fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                self.take(buf)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                self.take(buf)
            }
        }
        #[derive(Debug)]
        struct ImmediateWrite;
        #[async_trait]
        impl UnreliableWrite for ImmediateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                Ok(buf.len())
            }
        }
        let encode = |seq, data: &[u8]| {
            let mut datagram = vec![0; 64];
            let data = crate::codec::EncodeData {
                seq,
                send_ts: None,
                frame_len: None,
                data,
            };
            let len = crate::codec::encode_ack_data(None, None, Some(data), &mut datagram).unwrap();
            datagram.truncate(len);
            datagram
        };
        let datagrams = std::collections::VecDeque::from([encode(0, b"payload"), encode(0, b"")]);
        let layer = crate::udp::wrap_fec(DatagramQueue(datagrams), ImmediateWrite, false);
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        assert!(
            !transmission.recv_fin().is_cancelled(),
            "a duplicate sequence is not proof that the peer sent FIN"
        );
    }

    #[tokio::test]
    async fn graceful_reap_waits_for_fin_ack_send() {
        #[derive(Debug)]
        struct OneFinRead(Option<Vec<u8>>);
        #[async_trait]
        impl UnreliableRead for OneFinRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let Some(datagram) = self.0.take() else {
                    return Err(std::io::ErrorKind::WouldBlock);
                };
                buf[..datagram.len()].copy_from_slice(&datagram);
                Ok(datagram.len())
            }
        }
        #[derive(Debug)]
        struct BlockingWrite {
            started: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }
        #[async_trait]
        impl UnreliableWrite for BlockingWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                self.started.notify_one();
                self.release.notified().await;
                Ok(buf.len())
            }
        }
        let mut fin = vec![0; 64];
        let fin_data = crate::codec::EncodeData {
            seq: 0,
            send_ts: None,
            frame_len: None,
            data: &[],
        };
        let len = crate::codec::encode_ack_data(None, None, Some(fin_data), &mut fin).unwrap();
        fin.truncate(len);
        let send_started = Arc::new(tokio::sync::Notify::new());
        let release_send = Arc::new(tokio::sync::Notify::new());
        let layer = crate::udp::wrap_fec(
            OneFinRead(Some(fin)),
            BlockingWrite {
                started: Arc::clone(&send_started),
                release: Arc::clone(&release_send),
            },
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        assert!(transmission.recv_fin().is_cancelled());
        assert!(transmission.has_pending_acks());
        let shared = Arc::clone(&transmission.shared);
        let reaper = transmission._termination_reaper.clone();
        let mut reap = Box::pin(async move {
            reaper
                .ready_or_graceful_close(shared.recv_fin(), shared.session_outbound_drained())
                .await;
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut reap)
                .await
                .is_err(),
            "peer FIN must not reap while its ACK is pending"
        );
        let mut send_bufs = SendBufs::new();
        let mut flush = Box::pin(transmission.flush_acks(&mut send_bufs));
        tokio::select! {
            result = &mut flush => panic!("FIN ACK send completed before release: {result:?}"),
            () = send_started.notified() => (),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut reap)
                .await
                .is_err(),
            "an in-flight FIN ACK must still hold the session open"
        );
        release_send.notify_one();
        tokio::time::timeout(Duration::from_millis(100), flush)
            .await
            .expect("released FIN ACK send did not finish")
            .expect("FIN ACK send failed");
        tokio::time::timeout(Duration::from_millis(100), reap)
            .await
            .expect("successful FIN ACK did not release graceful reaping");
    }

    async fn wait_for_rtx_window() {
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    fn harness(fec: bool, enabled: bool) -> (TransmissionLayer, Arc<Mutex<RecordingWrite>>) {
        harness_with_tuning(
            fec,
            enabled,
            crate::transmission::fec_tuning::FecTuning::default(),
        )
    }
    fn harness_with_tuning(
        fec: bool,
        enabled: bool,
        tuning: crate::transmission::fec_tuning::FecTuning,
    ) -> (TransmissionLayer, Arc<Mutex<RecordingWrite>>) {
        let recorder = Arc::new(Mutex::new(RecordingWrite::default()));
        struct SharedWrite(Arc<Mutex<RecordingWrite>>);
        #[async_trait]
        impl UnreliableWrite for SharedWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                self.0.lock().unwrap().push(buf.to_vec());
                Ok(buf.len())
            }
        }
        impl std::fmt::Debug for SharedWrite {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SharedWrite").finish_non_exhaustive()
            }
        }
        let write = SharedWrite(recorder.clone());
        let read = BlackholeRead;
        let ul = crate::udp::wrap_fec_with_mss_and_fec_tuning(
            read,
            write,
            fec,
            crate::udp::NO_FEC_MSS,
            tuning,
        );
        let mut tl = TransmissionLayer::new(ul, None);
        tl.set_rtx_dup_for_test(enabled);
        (tl, recorder)
    }

    #[derive(Debug)]
    struct FailAfterFirstWrite {
        attempts: Arc<AtomicUsize>,
        error: std::io::ErrorKind,
    }
    #[async_trait]
    impl UnreliableWrite for FailAfterFirstWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(buf.len())
            } else {
                Err(self.error)
            }
        }
    }

    fn parity_error_harness(error: std::io::ErrorKind) -> (TransmissionLayer, Arc<AtomicUsize>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let unreliable = crate::udp::wrap_fec_with_mss_and_fec_tuning(
            BlackholeRead,
            FailAfterFirstWrite {
                attempts: Arc::clone(&attempts),
                error,
            },
            true,
            crate::udp::NO_FEC_MSS,
            crate::transmission::fec_tuning::FecTuning::mindiv(),
        );
        (TransmissionLayer::new(unreliable, None), attempts)
    }

    fn stage_small_message(tl: &TransmissionLayer) {
        let payload = [0; 100];
        let now = Instant::now();
        let reliable = tl.reliable_layer();
        assert_eq!(
            reliable
                .lock()
                .unwrap()
                .send_data_buf(&payload, now)
                .unwrap(),
            payload.len()
        );
    }

    #[tokio::test]
    async fn fatal_regular_fec_parity_send_terminates_session() {
        let (mut tl, attempts) = parity_error_harness(std::io::ErrorKind::ConnectionReset);
        stage_small_message(&tl);
        let mut bufs = SendBufs::new();
        assert_eq!(
            tl.send_pkts(&mut bufs).await,
            Err(std::io::ErrorKind::ConnectionReset)
        );
        assert_eq!(tl.throw_error(), Err(std::io::ErrorKind::ConnectionReset));
        assert!(tl.termination.terminal().is_cancelled());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            tl.send_pkts(&mut bufs).await,
            Err(std::io::ErrorKind::ConnectionReset)
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "terminal RTP state must prevent another underlay send"
        );
    }

    #[tokio::test]
    async fn would_block_fec_parity_send_remains_non_terminal() {
        let (mut tl, attempts) = parity_error_harness(std::io::ErrorKind::WouldBlock);
        stage_small_message(&tl);
        let mut bufs = SendBufs::new();
        assert_eq!(tl.send_pkts(&mut bufs).await, Ok(true));
        assert_eq!(tl.throw_error(), Ok(()));
        assert!(!tl.termination.terminal().is_cancelled());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "the primary data and first parity datagrams should be attempted"
        );
    }

    #[tokio::test]
    async fn kill_tail_parity_error_preserves_original_terminal_error() {
        let (mut tl, attempts) = parity_error_harness(std::io::ErrorKind::ConnectionReset);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let mut bufs = SendBufs::new();
        assert_eq!(tl.send_kill_and_abort(&mut bufs).await, Ok(()));
        assert_eq!(tl.throw_error(), Err(std::io::ErrorKind::BrokenPipe));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "the KILL datagram and its first parity datagram should be attempted"
        );
    }

    #[tokio::test]
    async fn rtx_dup_queue_building_gate_suppresses_extra_copy() {
        let (mut tl, recorder) = harness(false, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        tl.reliable_layer()
            .lock()
            .unwrap()
            .set_queue_building_for_test(true);
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        assert_eq!(
            recorder.lock().unwrap().count(),
            1,
            "queue-building must suppress the duplicate copy"
        );
    }
    #[tokio::test]
    async fn fec_rtx_dup_reuses_identical_encoded_symbol() {
        let (mut tl, recorder) = harness(true, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let dg = recorder.lock().unwrap().datagrams();
        assert!(dg.len() >= 2, "primary + duplicate (got {})", dg.len());
        assert_eq!(
            dg[0], dg[1],
            "dup must reuse the exact encoded symbol bytes (no re-encode)"
        );
    }
    #[tokio::test]
    async fn primary_rtx_bypasses_empty_bucket_and_dup_is_skipped() {
        let (mut tl, recorder) = harness(false, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        let drained = tl.drain_rate_limiter_for_test(usize::MAX, Instant::now());
        assert!(drained > 0, "bucket should have started with tokens");
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        assert_eq!(
            recorder.lock().unwrap().count(),
            1,
            "primary sends from empty bucket; dup is skipped (no token)"
        );
    }
    #[tokio::test]
    async fn rtx_dup_disabled_sends_one_datagram() {
        let (mut tl, recorder) = harness(false, false);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        assert_eq!(
            recorder.lock().unwrap().count(),
            1,
            "toggle off must send exactly one datagram (stock)"
        );
    }
}
