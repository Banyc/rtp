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

    #[tokio::test]
    async fn single_symbol_depth_is_ungated_but_bulk_keeps_budget() {
        use crate::transmission::fec_tuning::FecTuning;
        let (mut tl, recorder) = harness_with_tuning(true, false, FecTuning::mindiv());
        let payload = vec![0u8; 100];
        let now = Instant::now();
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            assert_eq!(rl.send_data_buf(&payload, now).unwrap(), payload.len());
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 4,
            "mindiv single-symbol burst must emit 1 data + 3 parity = 4 datagrams, got {n}"
        );
    }

    fn stage_n_packets(tl: &TransmissionLayer, n: usize) -> usize {
        let mss = 8192usize;
        let post_fec_mss = mss - 11 - 2;
        let payload_len = post_fec_mss - crate::codec::data_overhead();
        let rl = tl.reliable_layer();
        let mut rl = rl.lock().unwrap();
        for _ in 0..n {
            let payload = vec![0u8; payload_len];
            rl.enqueue_send_data_for_test(&payload);
        }
        payload_len
    }

    fn harness_with_mss(
        fec: bool,
        enabled: bool,
        mss: usize,
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
            mss,
            crate::transmission::fec_tuning::FecTuning::default(),
        );
        let mut tl = TransmissionLayer::new(ul, None);
        tl.set_rtx_dup_for_test(enabled);
        (tl, recorder)
    }

    #[tokio::test]
    async fn full_group_flushes_four_parities_inline_mid_burst() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        tl.set_instream_group_fec_for_test(true);
        stage_n_packets(&tl, 8);
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 12,
            "full in-stream group must emit 8 data + 4 parity = 12 datagrams, got {n}"
        );
    }

    #[tokio::test]
    async fn partial_data_burst_force_flushes_when_tail_gate_closed() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        tl.set_instream_group_fec_for_test(true);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.set_cwnd_for_test(std::num::NonZeroUsize::new(3).unwrap());
        }
        stage_n_packets(&tl, 3);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.enqueue_send_data_for_test(&[0u8; 100]);
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 7,
            "partial data burst with toggle on and stock gate closed must flush 3 data + 4 parity = 7 datagrams, got {n}"
        );
    }

    #[tokio::test]
    async fn partial_data_burst_skipped_when_toggle_off_and_gate_closed() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.set_cwnd_for_test(std::num::NonZeroUsize::new(3).unwrap());
        }
        stage_n_packets(&tl, 3);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.enqueue_send_data_for_test(&[0u8; 100]);
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 3,
            "partial data burst with toggle off and stock gate closed must emit 3 data + 0 parity = 3 datagrams, got {n}"
        );
    }

    #[tokio::test]
    async fn ack_burst_keeps_stock_tail_gate_when_blocked() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        tl.set_instream_group_fec_for_test(true);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.enqueue_send_data_for_test(&[0u8; 100]);
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_kill_pkt(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 1,
            "ACK/kill burst with stock tail gate blocked must emit 1 datagram (no parity), got {n}"
        );
    }

    #[tokio::test]
    async fn toggle_off_wire_byte_identical_to_stock() {
        let (mut tl_off, recorder_off) = harness_with_mss(true, false, 8192);
        stage_n_packets(&tl_off, 8);
        let mut bufs = SendBufs::new();
        let _ = tl_off.send_pkts(&mut bufs).await;
        let n_off = recorder_off.lock().unwrap().count();
        let (mut tl_stock, recorder_stock) = harness_with_mss(true, false, 8192);
        stage_n_packets(&tl_stock, 8);
        let mut bufs2 = SendBufs::new();
        let _ = tl_stock.send_pkts(&mut bufs2).await;
        let n_stock = recorder_stock.lock().unwrap().count();
        assert_eq!(
            n_off, n_stock,
            "toggle off must produce identical datagram count to stock (got {n_off} vs {n_stock})"
        );
        assert!(
            n_off <= 8 + 5,
            "toggle off must not emit inline mid-burst parity (got {n_off} > 13)"
        );
        assert_ne!(
            n_off, 12,
            "toggle off must NOT emit 8 data + 4 inline parity = 12 (inline flush must not fire)"
        );
    }

    #[tokio::test]
    async fn recv_drains_with_blocked_utp_write() {
        use async_trait::async_trait;
        #[derive(Debug)]
        struct OnePktRead {
            sent: Mutex<bool>,
        }
        #[async_trait]
        impl UnreliableRead for OnePktRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let mut sent = self.sent.lock().unwrap();
                if *sent {
                    return Err(std::io::ErrorKind::UnexpectedEof);
                }
                *sent = true;
                let payload = b"hi";
                let mut pkt = vec![0u8; 1 + 8 + 4 + 2 + payload.len()];
                pkt[0] = 3;
                pkt[1..9].copy_from_slice(&0u64.to_be_bytes());
                pkt[9..13].copy_from_slice(&100u32.to_be_bytes());
                pkt[13..15].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                pkt[15..].copy_from_slice(payload);
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                Ok(n)
            }
        }
        #[derive(Debug)]
        struct BlockedWrite;
        #[async_trait]
        impl UnreliableWrite for BlockedWrite {
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
        }
        let read = OnePktRead {
            sent: Mutex::new(false),
        };
        let write = BlockedWrite;
        let ul = crate::udp::wrap_fec(read, write, false);
        let mut tl = TransmissionLayer::new(ul, None);
        let mut recv_bufs = RecvBufs::new();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tl.recv_pkts(&mut recv_bufs),
        )
        .await;
        match result {
            Ok(Ok(pkts)) => assert!(pkts.num_payload_segments > 0 || pkts.num_ack_segments > 0),
            Ok(Err((e, _))) => panic!("recv_pkts failed with blocked write: {e:?}"),
            Err(_) => panic!("recv_pkts hung with blocked utp_write (deadlock)"),
        }
    }

    #[tokio::test]
    async fn ack_deadline_does_not_cancel_or_reuse_async_read() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        #[derive(Debug)]
        struct ReadState {
            calls: AtomicUsize,
            third_started: tokio::sync::Notify,
            release_third: tokio::sync::Notify,
            third_cancelled: AtomicBool,
        }
        struct CancelProbe {
            state: Arc<ReadState>,
            completed: bool,
        }
        impl Drop for CancelProbe {
            fn drop(&mut self) {
                if !self.completed {
                    self.state.third_cancelled.store(true, Ordering::SeqCst);
                }
            }
        }
        #[derive(Debug)]
        struct CancellationSensitiveRead {
            state: Arc<ReadState>,
        }
        #[async_trait]
        impl UnreliableRead for CancellationSensitiveRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let call = self.state.calls.fetch_add(1, Ordering::SeqCst);
                let mut probe = (call == 2).then(|| CancelProbe {
                    state: Arc::clone(&self.state),
                    completed: false,
                });
                if call == 2 {
                    self.state.third_started.notify_one();
                    self.state.release_third.notified().await;
                }
                let payload = [call as u8];
                let data = crate::codec::EncodeData {
                    seq: call as u64,
                    send_ts: Some(100 + call as u32),
                    frame_len: None,
                    data: &payload,
                };
                let n = crate::codec::encode_ack_data(None, None, Some(data), buf).unwrap();
                if let Some(probe) = probe.as_mut() {
                    probe.completed = true;
                }
                Ok(n)
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
        let state = Arc::new(ReadState {
            calls: AtomicUsize::new(0),
            third_started: tokio::sync::Notify::new(),
            release_third: tokio::sync::Notify::new(),
            third_cancelled: AtomicBool::new(false),
        });
        let layer = crate::udp::wrap_fec(
            CancellationSensitiveRead {
                state: Arc::clone(&state),
            },
            ImmediateWrite,
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        let mut send_bufs = SendBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        transmission.resume_send().notified().await;
        transmission.flush_acks(&mut send_bufs).await.unwrap();
        assert!(!transmission.has_pending_acks());
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            transmission.resume_send().notified(),
        )
        .await
        .expect("new ACK work must wake the send timer");
        assert!(transmission.has_pending_acks());
        let mut third_recv = Box::pin(transmission.recv_pkts(&mut recv_bufs));
        tokio::select! {
            result = &mut third_recv => panic!("receive returned before the test released it: {result:?}"),
            () = state.third_started.notified() => (),
        }
        tokio::select! {
            result = &mut third_recv => panic!("ACK deadline cancelled the asynchronous receive: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
        assert!(
            !state.third_cancelled.load(Ordering::SeqCst),
            "ACK timing must not cancel the asynchronous reader"
        );
        state.release_third.notify_one();
        tokio::time::timeout(Duration::from_millis(100), &mut third_recv)
            .await
            .expect("released receive must finish")
            .expect("released receive must succeed");
        assert!(!state.third_cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn ack_flush_survives_wouldblock() {
        use async_trait::async_trait;
        use std::sync::Mutex;
        #[derive(Debug)]
        struct WouldBlockWrite {
            call_count: Mutex<usize>,
        }
        #[async_trait]
        impl UnreliableWrite for WouldBlockWrite {
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                let mut c = self.call_count.lock().unwrap();
                *c += 1;
                Err(std::io::ErrorKind::WouldBlock)
            }
        }
        #[derive(Debug)]
        struct OnePktRead2 {
            sent: Mutex<bool>,
        }
        #[async_trait]
        impl UnreliableRead for OnePktRead2 {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let mut sent = self.sent.lock().unwrap();
                if *sent {
                    return Err(std::io::ErrorKind::UnexpectedEof);
                }
                *sent = true;
                let payload = b"x";
                let mut pkt = vec![0u8; 1 + 8 + 4 + 2 + payload.len()];
                pkt[0] = 3;
                pkt[1..9].copy_from_slice(&0u64.to_be_bytes());
                pkt[9..13].copy_from_slice(&100u32.to_be_bytes());
                pkt[13..15].copy_from_slice(&(payload.len() as u16).to_be_bytes());
                pkt[15..].copy_from_slice(payload);
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                Ok(n)
            }
        }
        let read = OnePktRead2 {
            sent: Mutex::new(false),
        };
        let write = WouldBlockWrite {
            call_count: Mutex::new(0),
        };
        let ul = crate::udp::wrap_fec(read, write, false);
        let mut tl = TransmissionLayer::new(ul, None);
        let mut recv_bufs = RecvBufs::new();
        let _ = tl.recv_pkts(&mut recv_bufs).await;
        assert!(
            tl.has_pending_acks(),
            "ACK work must be recorded after recv"
        );
        let mut send_bufs = SendBufs::new();
        let _ = tl.flush_acks(&mut send_bufs).await;
        assert!(
            tl.has_pending_acks(),
            "ACK work must survive a WouldBlock flush (still pending for retry)"
        );
    }

    #[tokio::test]
    async fn proactive_watchdog_aborts_locally_before_best_effort_kill_completes() {
        use crate::transmission::fec_tuning::FecTuning;
        use crate::transmission::watchdog_tuning::WatchdogTuning;
        let recorder = Arc::new(Mutex::new(RecordingWrite::default()));
        let kill_started = Arc::new(tokio::sync::Notify::new());
        struct PendingKillWrite {
            recorder: Arc<Mutex<RecordingWrite>>,
            kill_started: Arc<tokio::sync::Notify>,
        }
        impl std::fmt::Debug for PendingKillWrite {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("PendingKillWrite").finish_non_exhaustive()
            }
        }
        #[async_trait]
        impl UnreliableWrite for PendingKillWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                self.recorder.lock().unwrap().push(buf.to_vec());
                if buf == [2] {
                    self.kill_started.notify_one();
                    std::future::pending().await
                } else {
                    Ok(buf.len())
                }
            }
        }
        let tuning = WatchdogTuning::new(
            1,
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_secs(2),
        );
        let ul = crate::udp::wrap_fec_with_mss_and_fec_tuning(
            BlackholeRead,
            PendingKillWrite {
                recorder: Arc::clone(&recorder),
                kill_started: Arc::clone(&kill_started),
            },
            false,
            crate::udp::NO_FEC_MSS,
            FecTuning::default(),
        );
        let mut tl = TransmissionLayer::new_with_watchdog_tuning(ul, tuning);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.send_data_buf(&[0u8; 100], Instant::now()).unwrap();
        }
        let mut bufs = SendBufs::new();
        assert!(tl.send_pkts(&mut bufs).await.is_ok());
        tokio::time::sleep(Duration::from_secs(2)).await;
        let shared = Arc::clone(&tl.shared);
        let send = tokio::spawn(async move {
            let mut bufs = SendBufs::new();
            tl.send_pkts(&mut bufs).await
        });
        kill_started.notified().await;
        assert_eq!(
            shared.throw_error(),
            Err(std::io::ErrorKind::BrokenPipe),
            "local fatal state must be visible while KILL delivery is blocked"
        );
        assert!(
            shared.termination.terminal().is_cancelled(),
            "session cancellation must be published before KILL delivery completes"
        );
        assert!(!send.is_finished(), "KILL delivery must still be pending");
        let err = shared.termination.io_error(std::io::ErrorKind::BrokenPipe);
        let msg = err.to_string();
        assert!(
            msg.contains("trigger=proactive_stall"),
            "error message must contain trigger=proactive_stall, got: {msg}"
        );
        assert!(
            msg.contains("reason=no_response"),
            "error message must contain reason=no_response, got: {msg}"
        );
        let datagrams = recorder.lock().unwrap().datagrams();
        assert!(
            datagrams.iter().any(|datagram| datagram.as_slice() == [2]),
            "a KILL datagram must be attempted, got {datagrams:?}"
        );
        send.abort();
        assert!(send.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn send_kill_and_abort_publishes_error_before_stalled_fec_tail() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct KillThenStuckTail {
            sends: Arc<AtomicUsize>,
            tail_started: Arc<tokio::sync::Notify>,
        }
        #[async_trait]
        impl UnreliableWrite for KillThenStuckTail {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                if self.sends.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(buf.len())
                } else {
                    self.tail_started.notify_one();
                    std::future::pending().await
                }
            }
        }
        let sends = Arc::new(AtomicUsize::new(0));
        let tail_started = Arc::new(tokio::sync::Notify::new());
        let unreliable = crate::udp::wrap_fec(
            BlackholeRead,
            KillThenStuckTail {
                sends: Arc::clone(&sends),
                tail_started: Arc::clone(&tail_started),
            },
            true,
        );
        let mut transmission = TransmissionLayer::new(unreliable, None);
        let shared = Arc::clone(&transmission.shared);
        let send = tokio::spawn(async move {
            let mut bufs = SendBufs::new();
            transmission.send_kill_and_abort(&mut bufs).await
        });
        tail_started.notified().await;
        assert_eq!(sends.load(Ordering::SeqCst), 2);
        assert_eq!(shared.throw_error(), Err(std::io::ErrorKind::BrokenPipe));
        assert!(shared.termination.terminal().is_cancelled());
        assert!(!send.is_finished());
        send.abort();
        assert!(send.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn duplicate_echo_updates_rtt_once() {
        use async_trait::async_trait;
        use std::sync::Mutex;
        #[derive(Debug)]
        struct DupEchoRead {
            sent: Mutex<usize>,
        }
        #[async_trait]
        impl UnreliableRead for DupEchoRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                Err(std::io::ErrorKind::WouldBlock)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
                let mut sent = self.sent.lock().unwrap();
                if *sent >= 2 {
                    return Err(std::io::ErrorKind::UnexpectedEof);
                }
                *sent += 1;
                let mut pkt = [0u8; 1 + 4];
                pkt[0] = 4;
                pkt[1..5].copy_from_slice(&1000u32.to_be_bytes());
                let n = pkt.len().min(buf.len());
                buf[..n].copy_from_slice(&pkt[..n]);
                Ok(n)
            }
        }
        #[derive(Debug)]
        struct OkWrite;
        #[async_trait]
        impl UnreliableWrite for OkWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
                Ok(buf.len())
            }
        }
        let read = DupEchoRead {
            sent: Mutex::new(0),
        };
        let write = OkWrite;
        let ul = crate::udp::wrap_fec(read, write, false);
        let mut tl = TransmissionLayer::new(ul, None);
        let mut recv_bufs = RecvBufs::new();
        let _ = tl.recv_pkts(&mut recv_bufs).await;
        let rtt = tl
            .reliable_layer
            .lock()
            .unwrap()
            .pkt_send_space()
            .smooth_rtt();
        let _ = rtt;
    }
}
