use core::time::Duration;
use std::sync::Arc;
use std::time::Instant;

use super::transmission_layer::{
    RecvBufs, RecvPkts, SendBufs, SendKillPkt, UnreliableRead, UnreliableWrite,
};
use crate::io_err::IoErr;
use crate::socket::session::TransmissionLayer;
use async_trait::async_trait;

impl TransmissionLayer {
    /// Test-only: take up to `n` tokens from the shared send-rate limiter so
    /// the retransmission-armor duplicate-copy token gate can be exercised
    /// (the primary rtx bypasses the bucket; the dup needs a token).
    pub(crate) fn drain_rate_limiter_for_test(&self, n: usize, now: Instant) -> usize {
        self.write_half_for_test().drain_pacer_for_test(n, now)
    }

    pub async fn send_pkts(&mut self, bufs: &mut SendBufs) -> Result<bool, IoErr> {
        self.write_half_mut_for_test().send_pkts(bufs).await
    }

    pub async fn flush_acks(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.write_half_mut_for_test().flush_acks(bufs).await
    }

    pub fn has_pending_acks(&self) -> bool {
        self.write_half_for_test().has_pending_acks()
    }

    pub async fn send_kill_pkt(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.write_half_mut_for_test().send_kill_pkt(bufs).await
    }

    pub async fn send_kill_and_abort(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.write_half_mut_for_test()
            .send_kill_and_abort(bufs)
            .await
    }

    pub async fn recv_pkts(
        &mut self,
        bufs: &mut RecvBufs,
    ) -> Result<RecvPkts, (IoErr, SendKillPkt)> {
        self.read_half_for_test().recv_pkts(bufs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traffic_shaping::redundancy::RetransmissionArmorConfig;
    use crate::transmission::test_doubles::BlockingWrite;
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
        fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
        async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
            Err(std::io::ErrorKind::WouldBlock.into())
        }
    }

    fn settle_rtt(tl: &TransmissionLayer, rtt: Duration, n: usize) {
        let rl = tl.shared_for_test().reliable_layer_for_test();
        let mut rl = rl.lock().unwrap();
        let mut t = Instant::now();
        for _ in 0..n {
            rl.sample_rtt(rtt, t);
            t += Duration::from_micros(100);
        }
    }

    fn send_one_packet(tl: &TransmissionLayer, now: Instant) -> crate::sequence::SequenceNumber {
        let rl = tl.shared_for_test().reliable_layer_for_test();
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
            fn take(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let Some(datagram) = self.0.take() else {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                };
                buf[..datagram.len()].copy_from_slice(&datagram);
                Ok(datagram.len())
            }
        }
        #[async_trait]
        impl UnreliableRead for OneDatagramRead {
            fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                self.take(buf)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                self.take(buf)
            }
        }
        #[derive(Debug)]
        struct ImmediateWrite;
        #[async_trait]
        impl UnreliableWrite for ImmediateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        let mut datagram = vec![0; 64];
        let fin = crate::codec::EncodeData {
            seq: crate::sequence::SequenceNumber::from_wire(1),
            send_ts: None,
            frame_len: None,
            data: &[],
        };
        let len =
            crate::codec::encode_ack_data(None, None, None, Some(fin), &mut datagram).unwrap();
        datagram.truncate(len);
        let layer = crate::udp::wrap_fec(
            Box::new(OneDatagramRead(Some(datagram))),
            Box::new(ImmediateWrite),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        assert!(
            transmission.shared_for_test().recv_fin().is_cancelled(),
            "an accepted FIN must be published even while an earlier sequence is missing"
        );
        assert!(
            !transmission.shared_for_test().recv_eof().is_cancelled(),
            "an out-of-order FIN must not publish application EOF"
        );
    }

    #[tokio::test]
    async fn duplicate_payload_sequence_reshaped_as_fin_does_not_publish_fin() {
        #[derive(Debug)]
        struct DatagramQueue(std::collections::VecDeque<Vec<u8>>);
        impl DatagramQueue {
            fn take(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let Some(datagram) = self.0.pop_front() else {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                };
                buf[..datagram.len()].copy_from_slice(&datagram);
                Ok(datagram.len())
            }
        }
        #[async_trait]
        impl UnreliableRead for DatagramQueue {
            fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                self.take(buf)
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                self.take(buf)
            }
        }
        #[derive(Debug)]
        struct ImmediateWrite;
        #[async_trait]
        impl UnreliableWrite for ImmediateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        let encode = |seq: crate::sequence::SequenceNumber, data: &[u8]| {
            let mut datagram = vec![0; 64];
            let data = crate::codec::EncodeData {
                seq,
                send_ts: None,
                frame_len: None,
                data,
            };
            let len =
                crate::codec::encode_ack_data(None, None, None, Some(data), &mut datagram).unwrap();
            datagram.truncate(len);
            datagram
        };
        let datagrams = std::collections::VecDeque::from([
            encode(crate::sequence::SequenceNumber::from_wire(0), b"payload"),
            encode(crate::sequence::SequenceNumber::from_wire(0), b""),
        ]);
        let layer = crate::udp::wrap_fec(
            Box::new(DatagramQueue(datagrams)),
            Box::new(ImmediateWrite),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        assert!(
            !transmission.shared_for_test().recv_fin().is_cancelled(),
            "a duplicate sequence is not proof that the peer sent FIN"
        );
    }

    #[tokio::test]
    async fn graceful_reap_waits_for_fin_ack_send() {
        #[derive(Debug)]
        struct OneFinRead(Option<Vec<u8>>);
        #[async_trait]
        impl UnreliableRead for OneFinRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let Some(datagram) = self.0.take() else {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                };
                buf[..datagram.len()].copy_from_slice(&datagram);
                Ok(datagram.len())
            }
        }
        let mut fin = vec![0; 64];
        let fin_data = crate::codec::EncodeData {
            seq: crate::sequence::SequenceNumber::from_wire(0),
            send_ts: None,
            frame_len: None,
            data: &[],
        };
        let len =
            crate::codec::encode_ack_data(None, None, None, Some(fin_data), &mut fin).unwrap();
        fin.truncate(len);
        let send_started = Arc::new(tokio::sync::Notify::new());
        let release_send = Arc::new(tokio::sync::Notify::new());
        let layer = crate::udp::wrap_fec(
            Box::new(OneFinRead(Some(fin))),
            Box::new(BlockingWrite {
                started: Arc::clone(&send_started),
                release: Arc::clone(&release_send),
            }),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        assert!(transmission.shared_for_test().recv_fin().is_cancelled());
        assert!(transmission.has_pending_acks());
        let shared = Arc::clone(transmission.shared_for_test());
        let reaper = transmission.termination_reaper_for_test().clone();
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
            crate::traffic_shaping::redundancy::fec_tuning::FecTuning::default(),
        )
    }
    fn harness_with_tuning(
        fec: bool,
        enabled: bool,
        tuning: crate::traffic_shaping::redundancy::fec_tuning::FecTuning,
    ) -> (TransmissionLayer, Arc<Mutex<RecordingWrite>>) {
        harness_with_tuning_and_ack_padding(fec, enabled, tuning, false)
    }
    fn harness_with_tuning_and_ack_padding(
        fec: bool,
        enabled: bool,
        tuning: crate::traffic_shaping::redundancy::fec_tuning::FecTuning,
        ack_padding: bool,
    ) -> (TransmissionLayer, Arc<Mutex<RecordingWrite>>) {
        let recorder = Arc::new(Mutex::new(RecordingWrite::default()));
        struct SharedWrite(Arc<Mutex<RecordingWrite>>);
        #[async_trait]
        impl UnreliableWrite for SharedWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
        let mut ul = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(read),
            Box::new(write),
            fec,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            tuning,
            crate::delivery::frame::FrameMode::default(),
        )
        .unwrap();
        ul.retransmission_armor = RetransmissionArmorConfig::from(enabled);
        ul.instream_group_fec = false;
        ul.ack_padding = ack_padding;
        let tl = TransmissionLayer::new(ul, None);
        (tl, recorder)
    }

    #[derive(Debug)]
    struct FailAfterFirstWrite {
        attempts: Arc<AtomicUsize>,
        error: std::io::ErrorKind,
    }
    #[async_trait]
    impl UnreliableWrite for FailAfterFirstWrite {
        async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(buf.len())
            } else {
                Err(self.error.into())
            }
        }
    }

    fn parity_error_harness(error: std::io::ErrorKind) -> (TransmissionLayer, Arc<AtomicUsize>) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let unreliable = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(BlackholeRead),
            Box::new(FailAfterFirstWrite {
                attempts: Arc::clone(&attempts),
                error,
            }),
            true,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            crate::traffic_shaping::redundancy::fec_tuning::FecTuning::max_diversity(),
            crate::delivery::frame::FrameMode::default(),
        )
        .unwrap();
        (TransmissionLayer::new(unreliable, None), attempts)
    }

    fn stage_small_message(tl: &TransmissionLayer) {
        let payload = [0; 100];
        let now = Instant::now();
        let reliable = tl.shared_for_test().reliable_layer_for_test();
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
    async fn fresh_non_fec_data_uses_the_canonical_contiguous_encoding() {
        let (mut tl, recorder) = harness(false, false);
        let mut bufs = SendBufs::new();
        stage_small_message(&tl);
        assert!(
            tl.send_pkts(&mut bufs).await.unwrap(),
            "a data packet must go out"
        );
        let datagrams = recorder.lock().unwrap().datagrams();
        assert_eq!(datagrams.len(), 1);
        let encoded = bufs.parts_mut().1;
        assert_eq!(
            datagrams[0],
            encoded[..datagrams[0].len()],
            "fresh non-FEC data must use the canonical contiguous codec buffer"
        );
    }

    #[tokio::test]
    async fn fatal_regular_fec_parity_send_terminates_session() {
        let (mut tl, attempts) = parity_error_harness(std::io::ErrorKind::ConnectionReset);
        // Measured congestion loss opens the condition gate so a parity burst
        // is actually attempted and its fatal error surfaces.
        tl.shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
        stage_small_message(&tl);
        let mut bufs = SendBufs::new();
        assert_eq!(
            tl.send_pkts(&mut bufs).await,
            Err(std::io::ErrorKind::ConnectionReset.into())
        );
        assert_eq!(
            tl.shared_for_test().check_error(),
            Err(std::io::ErrorKind::ConnectionReset.into())
        );
        assert!(tl.shared_for_test().terminal_is_cancelled());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            tl.send_pkts(&mut bufs).await,
            Err(std::io::ErrorKind::ConnectionReset.into())
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
        tl.shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
        stage_small_message(&tl);
        let mut bufs = SendBufs::new();
        assert_eq!(tl.send_pkts(&mut bufs).await, Ok(true));
        assert_eq!(tl.shared_for_test().check_error(), Ok(()));
        assert!(!tl.shared_for_test().terminal_is_cancelled());
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "the primary data and first parity datagrams should be attempted"
        );
    }

    #[tokio::test]
    async fn kill_tail_parity_error_preserves_original_terminal_error() {
        let (mut tl, attempts) = parity_error_harness(std::io::ErrorKind::ConnectionReset);
        tl.shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let mut bufs = SendBufs::new();
        assert_eq!(tl.send_kill_and_abort(&mut bufs).await, Ok(()));
        assert_eq!(
            tl.shared_for_test().check_error(),
            Err(std::io::ErrorKind::BrokenPipe.into())
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "the KILL datagram and its first parity datagram should be attempted"
        );
    }

    #[tokio::test]
    async fn retransmission_armor_queue_building_gate_suppresses_extra_copy() {
        let (mut tl, recorder) = harness(false, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        tl.shared_for_test()
            .reliable_layer_for_test()
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
    async fn fec_retransmission_armor_reuses_identical_encoded_symbol() {
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
    async fn tail_probe_bypasses_empty_bucket_and_dup_is_skipped() {
        let (mut tl, recorder) = harness(false, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        // Wait out the tail-loss-probe window (PTO = max(2*srtt, 10 ms) at
        // the 1 ms settled RTT) so the *tail probe* — not a data packet — is
        // the next recovery send.  The regular RTO stays far off (MIN_RTO
        // floor), so a regular retransmit cannot preempt it.
        wait_for_rtx_window().await;
        // Drain the ordinary pacer: no token is available for the armor
        // duplicate-copy gate.
        let drained = tl.drain_rate_limiter_for_test(usize::MAX, Instant::now());
        assert!(drained > 0, "bucket should have started with tokens");
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        assert_eq!(
            recorder.lock().unwrap().count(),
            1,
            "the tail probe still sends once from an empty bucket; the armor duplicate is skipped (no token)"
        );
    }
    #[tokio::test]
    async fn recovery_non_fec_data_uses_the_canonical_contiguous_encoding() {
        let (mut tl, recorder) = harness(false, false);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let datagrams = recorder.lock().unwrap().datagrams();
        assert_eq!(
            datagrams.len(),
            1,
            "a non-FEC recovery packet without armor sends exactly one datagram"
        );
        let encoded = bufs.parts_mut().1;
        assert_eq!(
            datagrams[0],
            encoded[..datagrams[0].len()],
            "non-FEC recovery must use the same canonical contiguous codec buffer as fresh data (no vectored header)"
        );
    }

    #[tokio::test]
    async fn single_symbol_depth_uses_spare_capacity_after_loss_gate_opens() {
        use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
        let (mut tl, recorder) = harness_with_tuning(true, false, FecTuning::max_diversity());
        // Measured congestion loss above the enable threshold opens the loss
        // gate; the single-symbol depth-3 parity then flows because the
        // capacity is genuinely spare.
        tl.shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
        let payload = vec![0u8; 100];
        let now = Instant::now();
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            assert_eq!(rl.send_data_buf(&payload, now).unwrap(), payload.len());
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 4,
            "max_diversity single-symbol burst with the loss gate open must emit 1 data + 3 parity = 4 datagrams, got {n}"
        );
    }

    fn stage_n_packets(tl: &TransmissionLayer, n: usize) -> usize {
        let mss = 8192usize;
        let post_fec_mss = mss - 11 - 2;
        let payload_len = post_fec_mss - crate::codec::data_overhead();
        let rl = tl.shared_for_test().reliable_layer_for_test();
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
        instream_group_fec: bool,
        mss: usize,
    ) -> (TransmissionLayer, Arc<Mutex<RecordingWrite>>) {
        let recorder = Arc::new(Mutex::new(RecordingWrite::default()));
        struct SharedWrite(Arc<Mutex<RecordingWrite>>);
        #[async_trait]
        impl UnreliableWrite for SharedWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
        let mut ul = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(read),
            Box::new(write),
            fec,
            crate::udp::Mss::try_new(mss).unwrap(),
            crate::traffic_shaping::redundancy::fec_tuning::FecTuning::default(),
            crate::delivery::frame::FrameMode::default(),
        )
        .unwrap();
        ul.retransmission_armor = RetransmissionArmorConfig::from(enabled);
        ul.instream_group_fec = instream_group_fec;
        let tl = TransmissionLayer::new(ul, None);
        (tl, recorder)
    }

    #[tokio::test]
    async fn full_group_flushes_four_parities_inline_mid_burst() {
        let (mut tl, recorder) = harness_with_mss(true, false, true, 8192);
        // Measured congestion loss opens the condition gate so the full
        // in-stream group may flush its parity inline mid-burst.
        tl.shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
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
    async fn saturated_data_burst_does_not_spend_capacity_on_parity() {
        let (mut tl, recorder) = harness_with_mss(true, false, true, 8192);
        // Open the loss gate so the only blocking condition is capacity: a
        // saturated burst (cwnd pressure + queued application work) must not
        // spend capacity on parity.
        tl.shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            rl.set_cwnd_for_test(std::num::NonZeroUsize::new(3).unwrap());
        }
        stage_n_packets(&tl, 3);
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            rl.enqueue_send_data_for_test(&[0u8; 100]);
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 3,
            "saturated data burst must emit 3 data + 0 parity = 3 datagrams (cwnd pressure wins over parity), got {n}"
        );
    }

    #[tokio::test]
    async fn fec_waits_for_loss_feedback_even_with_spare_capacity() {
        use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
        let (mut tl, recorder) = harness_with_tuning(true, false, FecTuning::max_diversity());
        // Fresh connection: no congestion feedback and no recovery samples
        // yet, so the loss gate stays closed even though capacity is spare
        // and the instream_flush tail policy requests a flush.
        let payload = vec![0u8; 100];
        let now = Instant::now();
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            assert_eq!(rl.send_data_buf(&payload, now).unwrap(), payload.len());
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 1,
            "startup without measured congestion loss must emit 1 data + 0 parity = 1 datagram, got {n}"
        );
    }

    #[tokio::test]
    async fn queue_growth_closes_fec_spare_capacity_gate() {
        use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
        let (mut tl, recorder) = harness_with_tuning(true, false, FecTuning::max_diversity());
        // Loss evidence is present (gate open) but the bottleneck queue is
        // building: parity must not spend capacity on a growing queue.
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            rl.set_congestion_loss_ratio_for_test(Some(0.08));
            rl.set_queue_building_for_test(true);
        }
        let payload = vec![0u8; 100];
        let now = Instant::now();
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            assert_eq!(rl.send_data_buf(&payload, now).unwrap(), payload.len());
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        assert_eq!(
            n, 1,
            "queue growth must close the spare-capacity gate: 1 data + 0 parity = 1 datagram, got {n}"
        );
    }

    #[tokio::test]
    async fn partial_data_burst_skipped_when_toggle_off_and_gate_closed() {
        let (mut tl, recorder) = harness_with_mss(true, false, false, 8192);
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            rl.set_cwnd_for_test(std::num::NonZeroUsize::new(3).unwrap());
        }
        stage_n_packets(&tl, 3);
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
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
        let (mut tl, recorder) = harness_with_mss(true, false, true, 8192);
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
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
        let (mut tl_off, recorder_off) = harness_with_mss(true, false, false, 8192);
        stage_n_packets(&tl_off, 8);
        let mut bufs = SendBufs::new();
        let _ = tl_off.send_pkts(&mut bufs).await;
        let n_off = recorder_off.lock().unwrap().count();
        let (mut tl_stock, recorder_stock) = harness_with_mss(true, false, false, 8192);
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
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let mut sent = self.sent.lock().unwrap();
                if *sent {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
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
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
        }
        let read = OnePktRead {
            sent: Mutex::new(false),
        };
        let write = BlockedWrite;
        let ul = crate::udp::wrap_fec(Box::new(read), Box::new(write), false);
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
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
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
                    seq: crate::sequence::SequenceNumber::from_wire(call as u64),
                    send_ts: Some(100 + call as u32),
                    frame_len: None,
                    data: &payload,
                };
                let n = crate::codec::encode_ack_data(None, None, None, Some(data), buf).unwrap();
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
            Box::new(CancellationSensitiveRead {
                state: Arc::clone(&state),
            }),
            Box::new(ImmediateWrite),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut recv_bufs = RecvBufs::new();
        let mut send_bufs = SendBufs::new();
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        transmission
            .write_half_for_test()
            .ack_schedule_changed()
            .notified()
            .await;
        transmission.flush_acks(&mut send_bufs).await.unwrap();
        assert!(!transmission.has_pending_acks());
        transmission.recv_pkts(&mut recv_bufs).await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            transmission
                .write_half_for_test()
                .ack_schedule_changed()
                .notified(),
        )
        .await
        .expect("new ACK work must rearm the send schedule");
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
    async fn ack_flush_never_sends_an_empty_page() {
        use crate::transmission::transmission_layer::MAX_NUM_ACK;
        let (mut transmission, recorder) = harness(false, false);
        {
            let mut reliable = transmission
                .shared_for_test()
                .reliable_layer_for_test()
                .lock()
                .unwrap();
            // (seq 0 would fold into the cumulative front, so start at 2 to
            // keep exactly one full page of selective blocks.)
            for seq in (2..=128).step_by(2) {
                reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(seq), None, b"x");
            }
            assert_eq!(
                reliable.pkt_recv_space().ack_history().blocks().count(),
                MAX_NUM_ACK,
                "the history must hold exactly one page of balls"
            );
        }
        transmission
            .shared_for_test()
            .ack_feedback_for_test()
            .record(crate::transmission::ack_feedback::ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            });
        let mut send_bufs = SendBufs::new();
        transmission
            .flush_acks(&mut send_bufs)
            .await
            .expect("flush must succeed");
        let sent = recorder.lock().unwrap().datagrams();
        assert!(!sent.is_empty(), "the flush must send the one real page");
        assert!(
            sent.iter().all(|datagram| !datagram.is_empty()),
            "an ACK flush sent {} datagrams, one of them empty",
            sent.len()
        );
    }

    #[tokio::test]
    async fn fitted_ack_padding_pads_acks_into_the_observed_data_band() {
        use crate::transmission::ack_feedback::ReceivedAckWork;
        let (mut transmission, recorder) = harness_with_tuning_and_ack_padding(
            false,
            false,
            crate::traffic_shaping::redundancy::fec_tuning::FecTuning::default(),
            true,
        );
        let now = Instant::now();
        // Populate the sampler with >= MIN_SAMPLES data packets (each ~111 B
        // on the wire: cmd + seq + len + 100 B payload), so the fitted band
        // is anchored at the data size.
        let mut send_bufs = SendBufs::new();
        for _ in 0..16 {
            transmission
                .shared_for_test()
                .reliable_layer_for_test()
                .lock()
                .unwrap()
                .send_data_buf(&[0u8; 100], now)
                .unwrap();
            assert!(
                transmission.send_pkts(&mut send_bufs).await.unwrap(),
                "each staged data packet must send (and be sampled)"
            );
        }
        // Give the flush a claimable page: a small recv history plus pending
        // ack work.
        {
            let mut reliable = transmission
                .shared_for_test()
                .reliable_layer_for_test()
                .lock()
                .unwrap();
            for seq in [2u64, 4, 6] {
                reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(seq), None, b"x");
            }
        }
        transmission
            .shared_for_test()
            .ack_feedback_for_test()
            .record(ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            });
        transmission.flush_acks(&mut send_bufs).await.unwrap();
        let sent = recorder.lock().unwrap().datagrams();
        let ack_wire = sent.last().expect("the flush must send the padded ACK");
        // The padded ACK decodes: the codec's zero-tail rule strips the fill
        // and the ack content survives.
        let mut acks = Vec::new();
        let decoded = crate::codec::decode(ack_wire, &mut acks, None).unwrap();
        assert!(
            decoded.ack_next.is_some(),
            "the padded ACK must decode, got {ack_wire:?}"
        );
        // The padded ACK is data-sized: at least as large as the observed
        // data packets, so it is hidden among them by wire size.
        assert!(
            ack_wire.len() >= 100,
            "the padded ACK must be data-sized, got {} bytes",
            ack_wire.len()
        );
    }

    #[tokio::test]
    async fn ack_flush_emits_the_reason_of_the_transactional_claim() {
        use crate::metrics::{MetricsAckFlushReason, MetricsEvent, MetricsObserver};
        use crate::transmission::ack_feedback::ReceivedAckWork;
        #[derive(Debug)]
        struct ImmediateWrite;
        #[async_trait]
        impl UnreliableWrite for ImmediateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let mut layer =
            crate::udp::wrap_fec(Box::new(BlackholeRead), Box::new(ImmediateWrite), false);
        layer.metrics_observer = Some(observer);
        let mut transmission = TransmissionLayer::new(layer, None);
        // Sparse work with no prior flush: the first successful transactional
        // claim is due for the Initial reason.
        transmission
            .shared_for_test()
            .ack_feedback_for_test()
            .record(ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            });
        let mut send_bufs = SendBufs::new();
        transmission
            .flush_acks(&mut send_bufs)
            .await
            .expect("flush must succeed");
        let observations = observations.lock().unwrap();
        let flush = observations
            .iter()
            .find(|observation| matches!(observation.event, MetricsEvent::AckFlush(_)))
            .expect("a successful claim must emit an AckFlush event");
        assert_eq!(
            flush.event,
            MetricsEvent::AckFlush(MetricsAckFlushReason::Initial),
            "the first claim without a prior flush is due for the Initial reason"
        );
    }

    #[tokio::test]
    async fn ack_flush_does_not_claim_a_fin_that_arrived_mid_flush() {
        #[derive(Debug)]
        struct SilentRead;
        #[async_trait]
        impl UnreliableRead for SilentRead {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                std::future::pending().await
            }
        }
        let send_started = Arc::new(tokio::sync::Notify::new());
        let release_send = Arc::new(tokio::sync::Notify::new());
        let layer = crate::udp::wrap_fec(
            Box::new(SilentRead),
            Box::new(BlockingWrite {
                started: Arc::clone(&send_started),
                release: Arc::clone(&release_send),
            }),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let shared = Arc::clone(transmission.shared_for_test());
        shared
            .ack_feedback_for_test()
            .record(crate::transmission::ack_feedback::ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            });
        let mut send_bufs = SendBufs::new();
        let mut flush = Box::pin(transmission.flush_acks(&mut send_bufs));
        tokio::select! {
            result = &mut flush => panic!("ACK send completed before release: {result:?}"),
            () = send_started.notified() => (),
        }
        shared
            .ack_feedback_for_test()
            .record(crate::transmission::ack_feedback::ReceivedAckWork {
                pending_acks: 1,
                fin_ack: true,
                echo_ts: None,
            });
        release_send.notify_one();
        tokio::time::timeout(Duration::from_millis(100), flush)
            .await
            .expect("released ACK send did not finish")
            .expect("ACK send failed");
        let (pending_acks, fin_pending) = shared.ack_feedback_for_test().pending_work();
        assert!(
            fin_pending,
            "a FIN that arrived mid-flush was not acked by it and must stay pending"
        );
        assert_eq!(
            pending_acks, 1,
            "only the acks claimed before the send may be subtracted"
        );
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
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, IoErr> {
                let mut c = self.call_count.lock().unwrap();
                *c += 1;
                Err(std::io::ErrorKind::WouldBlock.into())
            }
        }
        #[derive(Debug)]
        struct OnePktRead2 {
            sent: Mutex<bool>,
        }
        #[async_trait]
        impl UnreliableRead for OnePktRead2 {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let mut sent = self.sent.lock().unwrap();
                if *sent {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
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
        let ul = crate::udp::wrap_fec(Box::new(read), Box::new(write), false);
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
        use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
        let ul = crate::udp::wrap_fec_with_mss_and_fec_tuning_and_frame_delivery(
            Box::new(BlackholeRead),
            Box::new(PendingKillWrite {
                recorder: Arc::clone(&recorder),
                kill_started: Arc::clone(&kill_started),
            }),
            false,
            crate::udp::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            FecTuning::default(),
            crate::delivery::frame::FrameMode::default(),
        )
        .unwrap();
        let mut tl = TransmissionLayer::new_with_watchdog_tuning(ul, None, tuning);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        {
            let rl = tl.shared_for_test().reliable_layer_for_test();
            let mut rl = rl.lock().unwrap();
            rl.send_data_buf(&[0u8; 100], Instant::now()).unwrap();
        }
        let mut bufs = SendBufs::new();
        assert!(tl.send_pkts(&mut bufs).await.is_ok());
        tokio::time::sleep(Duration::from_secs(2)).await;
        let shared = Arc::clone(tl.shared_for_test());
        let mut send_tasks = tokio::task::JoinSet::new();
        // The parked KILL delivery is cancelled through a watch inside the
        // task, so the task exits normally instead of being aborted (no
        // cancelled JoinError to tolerate); the pending operation is dropped.
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        send_tasks.spawn(async move {
            let mut bufs = SendBufs::new();
            tokio::select! {
                result = tl.send_pkts(&mut bufs) => Some(result),
                _ = stop_rx.changed() => None,
            }
        });
        kill_started.notified().await;
        assert_eq!(
            shared.check_error(),
            Err(std::io::ErrorKind::BrokenPipe.into()),
            "local fatal state must be visible while KILL delivery is blocked"
        );
        assert!(
            shared.terminal_is_cancelled(),
            "session cancellation must be published before KILL delivery completes"
        );
        assert!(
            send_tasks.try_join_next().is_none(),
            "KILL delivery must still be pending"
        );
        let err = shared.io_error(std::io::ErrorKind::BrokenPipe.into());
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
        stop_tx.send(true).unwrap();
        let exit = send_tasks.join_next().await.unwrap().unwrap();
        assert!(
            exit.is_none(),
            "KILL delivery must be cancelled, not completed"
        );
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
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
            Box::new(BlackholeRead),
            Box::new(KillThenStuckTail {
                sends: Arc::clone(&sends),
                tail_started: Arc::clone(&tail_started),
            }),
            true,
        );
        let mut transmission = TransmissionLayer::new(unreliable, None);
        let shared = Arc::clone(transmission.shared_for_test());
        // Measured congestion loss opens the condition gate so the KILL tail
        // actually attempts its parity (the second stalled send).
        shared
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .set_congestion_loss_ratio_for_test(Some(0.08));
        let mut send_tasks = tokio::task::JoinSet::new();
        // The parked operation is cancelled through a watch inside the task,
        // so the task exits normally instead of being aborted (no cancelled
        // JoinError to tolerate); the pending operation is dropped.
        let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
        send_tasks.spawn(async move {
            let mut bufs = SendBufs::new();
            tokio::select! {
                result = transmission.send_kill_and_abort(&mut bufs) => Some(result),
                _ = stop_rx.changed() => None,
            }
        });
        tail_started.notified().await;
        assert_eq!(sends.load(Ordering::SeqCst), 2);
        assert_eq!(
            shared.check_error(),
            Err(std::io::ErrorKind::BrokenPipe.into())
        );
        assert!(shared.terminal_is_cancelled());
        assert!(send_tasks.try_join_next().is_none());
        stop_tx.send(true).unwrap();
        let exit = send_tasks.join_next().await.unwrap().unwrap();
        assert!(
            exit.is_none(),
            "the stalled operation must be cancelled, not completed"
        );
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
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let mut sent = self.sent.lock().unwrap();
                if *sent >= 2 {
                    return Err(std::io::ErrorKind::UnexpectedEof.into());
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
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        let read = DupEchoRead {
            sent: Mutex::new(0),
        };
        let write = OkWrite;
        let ul = crate::udp::wrap_fec(Box::new(read), Box::new(write), false);
        let mut tl = TransmissionLayer::new(ul, None);
        let mut recv_bufs = RecvBufs::new();
        let _ = tl.recv_pkts(&mut recv_bufs).await;
        let rtt = tl
            .shared_for_test()
            .reliable_layer_for_test()
            .lock()
            .unwrap()
            .pkt_send_space()
            .smooth_rtt();
        let _ = rtt;
    }

    #[tokio::test]
    async fn ack_only_datagram_wakes_writer_before_receive_waits_again() {
        use async_trait::async_trait;
        use std::sync::Mutex;
        #[derive(Debug)]
        struct AckThenWait {
            datagram: Vec<u8>,
            sent: Mutex<bool>,
        }
        #[async_trait]
        impl UnreliableRead for AckThenWait {
            fn try_recv(&mut self, _buf: &mut [u8]) -> Result<usize, IoErr> {
                Err(std::io::ErrorKind::WouldBlock.into())
            }
            async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, IoErr> {
                let already_sent = *self.sent.lock().unwrap();
                if already_sent {
                    std::future::pending().await
                }
                *self.sent.lock().unwrap() = true;
                let n = self.datagram.len();
                buf[..n].copy_from_slice(&self.datagram);
                Ok(n)
            }
        }
        #[derive(Debug)]
        struct ImmediateWrite;
        #[async_trait]
        impl UnreliableWrite for ImmediateWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                Ok(buf.len())
            }
        }
        // The peer received the first packet (seq 0): cumulative next 1 with
        // no selective blocks — an ACK-only datagram.
        let history = crate::ack::AckHistory::new_at(crate::sequence::SequenceNumber::from_wire(1));
        let ack = crate::ack::EncodeAck {
            queue: &history,
            first_block_index: 0,
            max_blocks: crate::ack::MAX_ACK_BLOCKS,
        };
        let mut datagram = vec![0u8; 64];
        let len =
            crate::codec::encode_ack_data(None, Some(ack), None, None, &mut datagram).unwrap();
        datagram.truncate(len);
        let layer = crate::udp::wrap_fec(
            Box::new(AckThenWait {
                datagram,
                sent: Mutex::new(false),
            }),
            Box::new(ImmediateWrite),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let mut send_bufs = SendBufs::new();
        let mut recv_bufs = RecvBufs::new();
        // Put one packet in flight so the ACK genuinely updates the send
        // window when it lands.
        stage_small_message(&transmission);
        assert!(transmission.send_pkts(&mut send_bufs).await.unwrap());
        let shared = Arc::clone(transmission.shared_for_test());
        let mut recv = Box::pin(transmission.recv_pkts(&mut recv_bufs));
        tokio::select! {
            result = &mut recv => panic!("receive returned before the ACK-only wake: {result:?}"),
            () = shared.resume_send().notified() => (),
        }
        // recv_pkts must now be parked waiting for the next datagram again;
        // the writer was woken by the ACK, not by the receive loop exiting.
        tokio::select! {
            result = &mut recv => panic!("receive returned while the writer was being woken: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(10)) => (),
        }
    }

    #[tokio::test]
    async fn ack_flush_pages_share_one_history_snapshot_across_await() {
        use crate::transmission::transmission_layer::MAX_NUM_ACK;
        let recorder = Arc::new(Mutex::new(Vec::new()));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        #[derive(Debug)]
        struct GatedRecordingWrite {
            recorder: Arc<Mutex<Vec<Vec<u8>>>>,
            first_started: Arc<tokio::sync::Notify>,
            release_first: Arc<tokio::sync::Notify>,
        }
        #[async_trait]
        impl UnreliableWrite for GatedRecordingWrite {
            async fn send(&mut self, buf: &[u8]) -> Result<usize, IoErr> {
                let is_first = {
                    let mut sent = self.recorder.lock().unwrap();
                    sent.push(buf.to_vec());
                    sent.len() == 1
                };
                if is_first {
                    self.first_started.notify_one();
                    self.release_first.notified().await;
                }
                Ok(buf.len())
            }
        }
        let layer = crate::udp::wrap_fec(
            Box::new(BlackholeRead),
            Box::new(GatedRecordingWrite {
                recorder: Arc::clone(&recorder),
                first_started: Arc::clone(&first_started),
                release_first: Arc::clone(&release_first),
            }),
            false,
        );
        let mut transmission = TransmissionLayer::new(layer, None);
        let shared = Arc::clone(transmission.shared_for_test());
        // Seed a history spanning two full pages (head + deep page).
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            for seq in (2..=258).step_by(2) {
                reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(seq), None, b"x");
            }
            assert!(
                reliable.pkt_recv_space().ack_history().len() > MAX_NUM_ACK,
                "the history must span two pages"
            );
        }
        // Snapshot the expected deep page from the pre-mutation history.
        let expected_deep = {
            let mut buf = vec![0u8; 8192];
            let reliable = shared.reliable_layer_for_test().lock().unwrap();
            let history = reliable.pkt_recv_space().ack_history();
            let ack = crate::ack::EncodeAck {
                queue: history,
                first_block_index: MAX_NUM_ACK,
                max_blocks: MAX_NUM_ACK,
            };
            let n = crate::codec::encode_ack_data(None, Some(ack), None, None, &mut buf).unwrap();
            buf.truncate(n);
            buf
        };
        transmission
            .shared_for_test()
            .ack_feedback_for_test()
            .record(crate::transmission::ack_feedback::ReceivedAckWork {
                pending_acks: 1,
                fin_ack: false,
                echo_ts: None,
            });
        let mut send_bufs = SendBufs::new();
        let mut flush = Box::pin(transmission.flush_acks(&mut send_bufs));
        tokio::select! {
            result = &mut flush => panic!("page 0 send completed before release: {result:?}"),
            () = first_started.notified() => (),
        }
        // Page 0 is in flight; mutate the history before page 1 is encoded.
        {
            let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
            for seq in (260..=300).step_by(2) {
                reliable.recv_data_pkt(crate::sequence::SequenceNumber::from_wire(seq), None, b"x");
            }
        }
        release_first.notify_one();
        tokio::time::timeout(Duration::from_millis(100), flush)
            .await
            .expect("released ACK flush did not finish")
            .expect("ACK flush failed");
        let sent = recorder.lock().unwrap();
        assert_eq!(
            sent.len(),
            2,
            "the flush must send both claimed pages (head + deep)"
        );
        assert_eq!(
            sent[1], expected_deep,
            "the deep page must encode from the one pre-await history snapshot"
        );
    }

    #[tokio::test]
    async fn exhausted_data_send_writability_emits_one_typed_metric() {
        use crate::metrics::{MetricsEvent, MetricsObserver};
        #[derive(Debug)]
        struct ExhaustedWrite {
            call_count: Mutex<usize>,
        }
        #[async_trait]
        impl UnreliableWrite for ExhaustedWrite {
            async fn send(&mut self, _buf: &[u8]) -> Result<usize, IoErr> {
                let mut c = self.call_count.lock().unwrap();
                *c += 1;
                Err(std::io::ErrorKind::WouldBlock.into())
            }
        }
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let mut layer = crate::udp::wrap_fec(
            Box::new(BlackholeRead),
            Box::new(ExhaustedWrite {
                call_count: Mutex::new(0),
            }),
            false,
        );
        layer.metrics_observer = Some(observer);
        let mut transmission = TransmissionLayer::new(layer, None);
        stage_small_message(&transmission);
        let mut send_bufs = SendBufs::new();
        assert!(
            transmission.send_pkts(&mut send_bufs).await.is_ok(),
            "an exhausted underlay write must not surface as a terminal error"
        );
        assert!(
            !transmission.shared_for_test().has_error(),
            "data-send WouldBlock is an I/O-pressure outcome, not a terminal error"
        );
        let would_blocks = observations
            .lock()
            .unwrap()
            .iter()
            .filter(|observation| observation.event == MetricsEvent::DataSendWouldBlock)
            .count();
        assert_eq!(
            would_blocks, 1,
            "one send pass hitting the exhausted underlay must emit exactly one DataSendWouldBlock"
        );
    }
}
