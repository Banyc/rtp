use std::sync::Arc;
use std::time::Instant;

use super::connection::Connection;
use super::termination::{KillPolicy, TerminationWriter};
use super::transmission_layer::{
    FEC_DEBUG, PRINT_DEBUG_MSGS, ProactiveTerminationContext, SendBufs, UnreliableWrite,
};
use crate::codec::{EncodeData, encode_ack_data, encode_kill};
use crate::io_err::IoErr;
use crate::metrics::MetricsTerminationCause;
use crate::traffic_shaping::core::SendWake;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SendLoopResult {
    pub(crate) made_progress: bool,
    pub(crate) wake: SendWake,
}

#[derive(Debug)]
pub struct WriteHalf {
    pub(crate) utp_write: Box<dyn UnreliableWrite>,
    pub(crate) shared: Arc<Connection>,
    pub(crate) termination_writer: TerminationWriter,
}

impl std::ops::Deref for WriteHalf {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl WriteHalf {
    pub(crate) fn kill_requested(&self) -> &tokio_util::sync::CancellationToken {
        self.termination_writer.kill_requested()
    }

    async fn try_send_requested_kill(&mut self, bufs: &mut SendBufs) -> Option<Result<(), IoErr>> {
        let attempt = self.termination_writer.take_kill_attempt()?;
        let result = self.send_kill_pkt(bufs).await;
        drop(attempt);
        Some(result)
    }

    async fn check_error_after_requested_kill(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        match self.termination.check_error() {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self.try_send_requested_kill(bufs).await;
                Err(error)
            }
        }
    }

    pub(crate) fn proactively_terminate_stalled_session(&self) {
        let now = Instant::now();
        let context = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let send_space = reliable_layer.pkt_send_space();
            let Some(reason) = send_space.stall_reason(now) else {
                return;
            };
            ProactiveTerminationContext {
                reason: match reason {
                    crate::traffic_shaping::recovery::liveness::PeerStall::NoResponse => {
                        "no_response"
                    }
                    crate::traffic_shaping::recovery::liveness::PeerStall::NoProgress => {
                        "no_progress"
                    }
                },
                no_response_for_ms: send_space
                    .no_resp_for(now)
                    .map(|duration| duration.as_millis()),
                no_progress_for_ms: send_space
                    .no_progress_for(now)
                    .map(|duration| duration.as_millis()),
                snapshot: format!("{:?}", reliable_layer.log()),
            }
        };
        if self.termination.has_error() {
            return;
        }
        self.press_broken_pipe(
            KillPolicy::SendKill,
            Some(context),
            MetricsTerminationCause::ProactiveStall,
        );
    }

    #[cfg(test)]
    pub async fn send_pkts(&mut self, bufs: &mut SendBufs) -> Result<bool, IoErr> {
        self.send_pkts_inner(bufs, Instant::now()).await
    }

    pub(crate) async fn send_pass(&mut self, bufs: &mut SendBufs) -> Result<SendLoopResult, IoErr> {
        let now = Instant::now();
        let made_progress = self.send_pkts_inner(bufs, now).await?;
        Ok(SendLoopResult {
            made_progress,
            wake: self.next_send_wake(now),
        })
    }

    async fn send_pkts_inner(&mut self, bufs: &mut SendBufs, now: Instant) -> Result<bool, IoErr> {
        if self.try_send_requested_kill(bufs).await.is_some() {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.proactively_terminate_stalled_session();
        self.check_error_after_requested_kill(bufs).await?;
        self.send_due_post_open_response().await?;
        // `now` is already fixed for one send pass: compute the wire timestamp
        // once and reuse it for every packet encoded by this pass.
        let wire_ts = self.wire_ts(now);
        let mut written_bytes = 0;
        let mut written_fin = false;
        loop {
            if self.try_send_requested_kill(bufs).await.is_some() {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            self.check_error_after_requested_kill(bufs).await?;
            let (payload, codec_pkt, wire_pkt) = bufs.parts_mut();
            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_pkt(payload, now)
            };
            self.log(crate::metrics::MetricsEvent::SendDataPacketAttempt);
            let Some(p) = res else {
                if FEC_DEBUG {
                    eprintln!("send_data_pkt: no pkt to send (rtx=None, cwnd full or no tokens)");
                }
                break;
            };
            let data_written = match p.data_written {
                crate::reliable::reliable_layer::DataPktPayload::Data(data_written) => {
                    written_bytes += data_written.get();
                    data_written.get()
                }
                crate::reliable::reliable_layer::DataPktPayload::Fin => {
                    written_fin = true;
                    0
                }
            };
            let is_recovery = p.is_recovery;
            let data = EncodeData {
                seq: p.seq,
                send_ts: Some(wire_ts),
                frame_len: p.frame_len,
                data: &payload[..data_written],
            };
            if FEC_DEBUG {
                eprintln!("send_data_pkt seq={} data_len={}", p.seq, data_written);
            }
            let instream = self.instream_group_fec_enabled();
            let has_fec = self.fec.is_some() || instream;
            // The mutex-backed queue_building read is kept cold behind the
            // rtx_dup/is_recovery short-circuit: it is needed only when
            // retransmission duplication is enabled for a recovery packet.
            #[rustfmt::skip]
            let wants_dup = self.rtx_dup() && is_recovery && !self.reliable_layer.lock().unwrap().queue_building();
            let (primary_res, send_buf): (_, Option<&[u8]>) = if !has_fec {
                let ts = data.send_ts.unwrap_or(0);
                let cmd: u8 = match data.frame_len {
                    Some(_) => crate::delivery::frame::wire::FRAME_DATA_TS_CMD,
                    None => 3,
                };
                let mut hdr = [0u8; 19];
                let hdr_len = if let Some(frame_len) = data.frame_len {
                    hdr[0] = cmd;
                    hdr[1..9].copy_from_slice(&data.seq.to_wire().to_be_bytes());
                    hdr[9..13].copy_from_slice(&ts.to_be_bytes());
                    hdr[13..17].copy_from_slice(&frame_len.to_be_bytes());
                    hdr[17..19].copy_from_slice(&(data.data.len() as u16).to_be_bytes());
                    19
                } else {
                    hdr[0] = cmd;
                    hdr[1..9].copy_from_slice(&data.seq.to_wire().to_be_bytes());
                    hdr[9..13].copy_from_slice(&ts.to_be_bytes());
                    hdr[13..15].copy_from_slice(&(data.data.len() as u16).to_be_bytes());
                    15
                };
                let payload_slice = &payload[..data_written];
                let iov = [
                    std::io::IoSlice::new(&hdr[..hdr_len]),
                    std::io::IoSlice::new(payload_slice),
                ];
                let res = self.utp_write.send_vectored(&iov).await;
                let dup_buf = if wants_dup {
                    let n = encode_ack_data(None, None, None, Some(data), codec_pkt).unwrap();
                    Some(&codec_pkt[..n])
                } else {
                    None
                };
                (res, dup_buf)
            } else {
                let n = encode_ack_data(None, None, None, Some(data), codec_pkt).unwrap();
                let utp_pkt = &codec_pkt[..n];
                let send_buf: &[u8] = match self.fec.as_ref() {
                    Some(fec) => {
                        let mut fec = fec.lock().unwrap();
                        let fec_n = fec.encode_data(utp_pkt, wire_pkt, instream);
                        &wire_pkt[..fec_n]
                    }
                    None => utp_pkt,
                };
                (self.utp_write.send(send_buf).await, Some(send_buf))
            };
            match primary_res {
                Ok(_) => {
                    if self.fec.is_some() && instream {
                        self.maybe_flush_full_fec_group(now).await?;
                    }
                    if wants_dup && let Some(send_buf) = send_buf {
                        let token_taken = self
                            .send_rate_limiter
                            .lock()
                            .unwrap()
                            .take_exact_tokens(1, now);
                        if token_taken {
                            match self.utp_write.send(send_buf).await {
                                Ok(_) => {}
                                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                                    if FEC_DEBUG {
                                        eprintln!("send_pkts: dup WouldBlock (transient)");
                                    }
                                }
                                Err(e) => {
                                    self.press_error(e, MetricsTerminationCause::DataWrite);
                                    return Err(e);
                                }
                            }
                        }
                    }
                    continue;
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    if FEC_DEBUG {
                        eprintln!("send_pkts: WouldBlock on data send (transient)");
                    }
                    continue;
                }
                Err(e) => {
                    self.press_error(e, MetricsTerminationCause::DataWrite);
                    return Err(e);
                }
            }
        }
        if 0 < written_bytes || written_fin {
            if PRINT_DEBUG_MSGS {
                println!("send_pkts: {{ data: {written_bytes}; fin: {written_fin} }}");
            }
            self.signals.sent_data_pkt.notify_waiters();
        }
        if self.fec.is_some() {
            let baseline_can_send_tail_fec =
                { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
            let data_path = true;
            let can_send_tail_fec = self.fec_instream_flush
                || baseline_can_send_tail_fec
                || (data_path && self.instream_group_fec_enabled());
            self.close_fec_burst(now, can_send_tail_fec).await?;
        }
        if self.ack_flush_is_due() {
            self.flush_acks(bufs).await?;
        }
        Ok(0 < written_bytes || written_fin)
    }

    async fn maybe_flush_full_fec_group(&mut self, now: Instant) -> Result<(), IoErr> {
        let Some(fec) = self.fec.as_ref() else {
            return Ok(());
        };
        let should_flush = {
            let fec = fec.lock().unwrap();
            fec.group_data_full(self.instream_group_fec_enabled())
        };
        if !should_flush {
            return Ok(());
        }
        self.flush_fec_parities(now).await
    }

    pub(crate) async fn close_fec_burst(
        &mut self,
        now: Instant,
        can_send_tail_fec: bool,
    ) -> Result<(), IoErr> {
        let Some(fec) = self.fec.as_ref() else {
            return Ok(());
        };
        if !can_send_tail_fec {
            fec.lock().unwrap().skip_open_group();
            return Ok(());
        }
        self.flush_fec_parities(now).await
    }

    fn skip_open_fec_group(&self) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        fec.lock().unwrap().skip_open_group();
    }

    pub(crate) async fn flush_fec_parities(&mut self, now: Instant) -> Result<(), IoErr> {
        let Some(fec) = self.fec.as_ref() else {
            return Ok(());
        };
        let parity_pkts = {
            let mut fec = fec.lock().unwrap();
            let mut tb = self.send_rate_limiter.lock().unwrap();
            fec.maybe_flush_parities(
                tb.token_bucket_mut(),
                now,
                self.instream_group_fec_enabled(),
            )
        };
        for pkt in parity_pkts {
            match self.utp_write.send(&pkt).await {
                Ok(_) => (),
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    if FEC_DEBUG {
                        eprintln!("flush_fec_parities: WouldBlock (transient)");
                    }
                    return Ok(());
                }
                Err(e) => {
                    self.press_error(e, MetricsTerminationCause::FecParityWrite);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn send_due_post_open_response(&mut self) -> Result<(), IoErr> {
        let Some(response) = self.claim_post_open_response(Instant::now()) else {
            return Ok(());
        };
        match self.utp_write.send(&response.bytes).await {
            Ok(len) if len == response.bytes.len() => Ok(()),
            Ok(_) => {
                self.retry_post_open_response(Instant::now());
                Ok(())
            }
            Err(error) if error == std::io::ErrorKind::WouldBlock => {
                self.retry_post_open_response(Instant::now());
                Ok(())
            }
            Err(error) => {
                self.press_error(error, MetricsTerminationCause::HandshakeWrite);
                Err(error)
            }
        }
    }

    pub async fn send_kill_pkt(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        let fec_enabled = self.send_kill_data_pkt(bufs).await?;
        if fec_enabled {
            self.flush_kill_fec_tail().await;
        }
        Ok(())
    }

    async fn send_kill_data_pkt(&mut self, bufs: &mut SendBufs) -> Result<bool, IoErr> {
        // Session tag prefix (9 bytes) + KILL_CMD byte, when a tag exists.
        let mut buf = [0; 1 + 1 + 8];
        let n = encode_kill(self.session_tag(), &mut buf).unwrap();
        let fec_enabled = self.fec.is_some();
        let res = self.send_with_fec(&buf[..n], bufs.wire_pkt_mut()).await;
        if res.is_err() && fec_enabled {
            self.skip_open_fec_group();
        }
        res?;
        Ok(fec_enabled)
    }

    async fn flush_kill_fec_tail(&mut self) {
        let now = Instant::now();
        let can_send_tail_fec = { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
        let _ = self.close_fec_burst(now, can_send_tail_fec).await;
    }

    #[cfg(test)]
    pub async fn send_kill_and_abort(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.press_broken_pipe(
            KillPolicy::SendKill,
            None,
            MetricsTerminationCause::LocalAbort,
        );
        match self.try_send_requested_kill(bufs).await {
            Some(result) => result,
            None => self.termination.check_error(),
        }
    }

    #[cfg(test)]
    pub fn has_pending_acks(&self) -> bool {
        self.ack_flush.lock().unwrap().has_pending()
    }

    pub fn ack_flush_is_due(&self) -> bool {
        let now = Instant::now();
        self.ack_flush.lock().unwrap().is_due(now)
    }

    pub async fn flush_acks(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        {
            let s = self.ack_flush.lock().unwrap();
            if !s.has_pending() {
                return Ok(());
            }
        }
        crate::traffic_shaping::control::ack_flush::flush(self, bufs).await
    }

    pub(crate) async fn send_with_fec(
        &mut self,
        codec_pkt: &[u8],
        fec_buf: &mut [u8],
    ) -> Result<usize, IoErr> {
        let send_buf: &[u8] = {
            match self.fec.as_ref() {
                Some(fec) => {
                    let mut fec = fec.lock().unwrap();
                    let n = fec.encode_data(codec_pkt, fec_buf, false);
                    &fec_buf[..n]
                }
                None => codec_pkt,
            }
        };
        self.utp_write.send(send_buf).await
    }
}

#[cfg(test)]
mod tests {
    use crate::delivery::frame::FrameMode;
    use crate::metrics::{MetricsEvent, MetricsObserver, MetricsTerminationCause};
    use crate::traffic_shaping::recovery::liveness::PeerLiveness;
    use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
    use crate::transmission::connection::new_connection_with_watchdog_tuning;
    use crate::transmission::test_doubles::{BlockingWrite, PendingRead};
    use crate::transmission::transmission_layer::UnreliableLayer;
    use crate::transmission::watchdog_tuning::WatchdogTuning;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    fn term(l: &PeerLiveness, now: Instant, has_in_flight: bool) -> bool {
        l.should_terminate_session(now, has_in_flight)
    }

    #[test]
    fn inert_catcher() {
        let now = Instant::now();
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(200);
        let long_ago = now - Duration::from_secs(31);
        l.on_send(long_ago, rto);
        l.record_progress();
        l.on_send(long_ago, rto);
        l.refresh_waits(now - Duration::from_millis(1), rto);
        assert!(term(&l, now, true), "progress branch must fire");
        assert!(
            !term(&l, now, false),
            "idle safety: must not fire with empty window"
        );
    }

    #[test]
    fn idle_empty_queue_not_killed() {
        let rto = Duration::from_millis(100);
        let l = PeerLiveness::new();
        assert!(!term(&l, Instant::now(), false));
        let mut l2 = PeerLiveness::new();
        l2.refresh_waits(Instant::now() - Duration::from_millis(1), rto);
        l2.on_send(Instant::now() - Duration::from_secs(999), rto);
        assert!(!term(&l2, Instant::now(), false));
    }

    #[test]
    fn slow_but_progressing_not_killed() {
        let rto = Duration::from_millis(100);
        let mut l = PeerLiveness::new();
        l.refresh_waits(Instant::now() - Duration::from_millis(50), rto);
        l.record_progress();
        assert!(!term(&l, Instant::now(), true));
        let mut l2 = PeerLiveness::new();
        l2.refresh_waits(Instant::now() - Duration::from_millis(50), rto);
        l2.record_progress();
        l2.on_send(Instant::now() - Duration::from_secs(1), rto);
        assert!(!term(&l2, Instant::now(), true));
    }

    #[test]
    fn response_watchdog_still_fires() {
        let rto = Duration::from_millis(100);
        let mut l = PeerLiveness::new();
        l.on_send(Instant::now() - Duration::from_secs(31), rto);
        assert!(term(&l, Instant::now(), false));
        let mut l2 = PeerLiveness::new();
        l2.on_send(Instant::now() - Duration::from_secs(10), rto);
        assert!(!term(&l2, Instant::now(), false));
    }

    #[test]
    fn idle_connection_has_no_send_timer_deadline() {
        let l = PeerLiveness::new();
        assert!(
            l.next_deadline(false).is_none(),
            "idle connection has no deadline"
        );
    }

    #[test]
    fn next_deadline_uses_only_active_watchdogs() {
        let now = Instant::now();
        let mut l = PeerLiveness::new();
        let rto = Duration::from_millis(100);
        l.on_send(now, rto);
        let dl = l.next_deadline(false).unwrap();
        assert!(dl > now, "deadline must be in the future");
    }

    #[test]
    fn proactive_termination_emits_a_snapshot_event() {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let observer = {
            let observations = Arc::clone(&observations);
            MetricsObserver::new(move |observation| {
                observations.lock().unwrap().push(observation);
            })
        };
        let layer = UnreliableLayer {
            utp_read: Box::new(PendingRead),
            utp_write: Box::new(BlockingWrite::new()),
            post_open_handshake: None,
            session_tag: None,
            initial_sequences: crate::sequence::InitialSequences::ZERO,
            initial_rtt: None,
            metrics_observer: Some(observer),
            mss: NonZeroUsize::new(crate::udp::NO_FEC_MSS).unwrap(),
            fec: None,
            fec_tuning: FecTuning::default(),
            frame_delivery: FrameMode::default(),
            rtx_dup: false,
            instream_group_fec: false,
        };
        let watchdog = WatchdogTuning::new(1, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let (shared, write_half, _read_half, _reaper) =
            new_connection_with_watchdog_tuning(layer, None, watchdog);
        let now = Instant::now();
        let mut reliable = shared.reliable_layer.lock().unwrap();
        reliable.send_data_buf(b"x", now).unwrap();
        let mut packet = vec![0; crate::udp::NO_FEC_MSS];
        assert!(reliable.send_data_pkt(&mut packet, now).is_some());
        drop(reliable);
        write_half.proactively_terminate_stalled_session();
        let observations = observations.lock().unwrap();
        let event = observations
            .iter()
            .find(|observation| {
                matches!(
                    observation.event,
                    MetricsEvent::SessionTermination(termination)
                        if termination.cause == MetricsTerminationCause::ProactiveStall
                )
            })
            .expect("watchdog termination must be observable");
        assert_eq!(
            event.snapshot.unwrap().stall_reason,
            Some(crate::metrics::MetricsStallReason::NoResponse)
        );
    }
}
