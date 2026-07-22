use std::sync::Arc;
use std::time::Instant;

use super::shared::Shared;
use super::termination::{PeerReset, TerminationWriter};
use super::transmission_layer::{
    ACK_FLUSH_AGE, ACK_FLUSH_COUNT, FEC_DEBUG, MAX_NUM_ACK, PRINT_DEBUG_MSGS,
    ProactiveTerminationContext, SendBufs, UnreliableWrite,
};
use crate::codec::{EncodeAck, EncodeData, encode_ack_data, encode_kill};
use crate::pacer::SendWake;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SendPass {
    pub(crate) made_progress: bool,
    pub(crate) wake: SendWake,
}

#[derive(Debug)]
pub struct WriteHalf {
    pub(crate) utp_write: Box<dyn UnreliableWrite>,
    pub(crate) shared: Arc<Shared>,
    pub(crate) termination_writer: TerminationWriter,
}

impl std::ops::Deref for WriteHalf {
    type Target = Shared;
    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl WriteHalf {
    pub(crate) fn kill_requested(&self) -> &tokio_util::sync::CancellationToken {
        self.termination_writer.kill_requested()
    }

    async fn try_send_requested_kill(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Option<Result<(), std::io::ErrorKind>> {
        let attempt = self.termination_writer.take_kill_attempt()?;
        let result = self.send_kill_pkt(bufs).await;
        drop(attempt);
        Some(result)
    }

    async fn throw_error_after_requested_kill(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Result<(), std::io::ErrorKind> {
        match self.termination.throw_error() {
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
                    crate::send_queue::liveness::PeerStall::NoResponse => "no_response",
                    crate::send_queue::liveness::PeerStall::NoProgress => "no_progress",
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
        self.termination
            .press_broken_pipe(PeerReset::SendKill, Some(context));
    }

    #[cfg(test)]
    pub async fn send_pkts(&mut self, bufs: &mut SendBufs) -> Result<bool, std::io::ErrorKind> {
        self.send_pkts_inner(bufs).await
    }

    pub(crate) async fn send_pass(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Result<SendPass, std::io::ErrorKind> {
        let made_progress = self.send_pkts_inner(bufs).await?;
        Ok(SendPass {
            made_progress,
            wake: self.next_send_wake(Instant::now()),
        })
    }

    async fn send_pkts_inner(&mut self, bufs: &mut SendBufs) -> Result<bool, std::io::ErrorKind> {
        if self.try_send_requested_kill(bufs).await.is_some() {
            return Err(std::io::ErrorKind::BrokenPipe);
        }
        self.proactively_terminate_stalled_session();
        self.throw_error_after_requested_kill(bufs).await?;
        self.send_due_post_open_response().await?;
        let mut written_bytes = 0;
        let mut written_fin = false;
        loop {
            if self.try_send_requested_kill(bufs).await.is_some() {
                return Err(std::io::ErrorKind::BrokenPipe);
            }
            self.throw_error_after_requested_kill(bufs).await?;
            let now = Instant::now();
            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_pkt(&mut bufs.data, now)
            };
            self.log("send_data_pkt");
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
            let was_repair = p.was_repair;
            let queue_building = self.reliable_layer.lock().unwrap().queue_building();
            let data = EncodeData {
                seq: p.seq,
                send_ts: Some(self.wire_ts(now)),
                frame_len: p.frame_len,
                data: &bufs.data[..data_written],
            };
            if FEC_DEBUG {
                eprintln!("send_data_pkt seq={} data_len={}", p.seq, data_written);
            }
            let instream = self.instream_group_fec_enabled();
            let has_fec = self.fec.is_some() || instream;

            let (primary_res, send_buf): (_, &[u8]) = if !has_fec {
                // Fast path: non-FEC, data-only packet — avoid copying the
                // payload by encoding just the protocol header on the stack
                // and issuing a single vectored send.
                let ts = data.send_ts.unwrap_or(0);
                let cmd: u8 = match data.frame_len {
                    Some(_) => crate::delivery::frame::wire::FRAME_DATA_TS_CMD,
                    None => 3, // DATA_TS_CMD
                };
                let mut hdr = [0u8; 19];
                let hdr_len = if let Some(frame_len) = data.frame_len {
                    hdr[0] = cmd;
                    hdr[1..9].copy_from_slice(&data.seq.to_be_bytes());
                    hdr[9..13].copy_from_slice(&ts.to_be_bytes());
                    hdr[13..17].copy_from_slice(&frame_len.to_be_bytes());
                    hdr[17..19].copy_from_slice(&(data.data.len() as u16).to_be_bytes());
                    19
                } else {
                    hdr[0] = cmd;
                    hdr[1..9].copy_from_slice(&data.seq.to_be_bytes());
                    hdr[9..13].copy_from_slice(&ts.to_be_bytes());
                    hdr[13..15].copy_from_slice(&(data.data.len() as u16).to_be_bytes());
                    15
                };
                let payload = &bufs.data[..data_written];
                let iov = [std::io::IoSlice::new(&hdr[..hdr_len]), std::io::IoSlice::new(payload)];
                let res = self.utp_write.send_vectored(&iov).await;
                // For the rtx_dup path the caller needs the raw wire bytes.
                // Concatenate into bufs.utp on demand (rare).
                let dup_buf = {
                    let n = encode_ack_data(None, None, Some(data), &mut bufs.utp).unwrap();
                    &bufs.utp[..n]
                };
                (res, dup_buf)
            } else {
                let n = encode_ack_data(None, None, Some(data), &mut bufs.utp).unwrap();
                let utp_pkt = &bufs.utp[..n];
                let send_buf: &[u8] = match self.fec.as_ref() {
                    Some(fec) => {
                        let mut fec = fec.lock().unwrap();
                        let fec_n = fec.encode_data(utp_pkt, &mut bufs.fec, instream);
                        &bufs.fec[..fec_n]
                    }
                    None => utp_pkt,
                };
                (self.utp_write.send(send_buf).await, send_buf)
            };
            match primary_res {
                Ok(_) => {
                    if self.fec.is_some() && instream {
                        self.maybe_flush_full_fec_group(Instant::now()).await?;
                    }
                    if self.rtx_dup() && was_repair && !queue_building {
                        let now = Instant::now();
                        let token_taken = self
                            .send_rate_limiter
                            .lock()
                            .unwrap()
                            .take_exact_tokens(1, now);
                        if token_taken {
                            match self.utp_write.send(send_buf).await {
                                Ok(_) => {}
                                Err(std::io::ErrorKind::WouldBlock) => {
                                    if FEC_DEBUG {
                                        eprintln!("send_pkts: dup WouldBlock (transient)");
                                    }
                                }
                                Err(e) => {
                                    self.termination.press_error(e);
                                    return Err(e);
                                }
                            }
                        }
                    }
                    continue;
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    if FEC_DEBUG {
                        eprintln!("send_pkts: WouldBlock on data send (transient)");
                    }
                    continue;
                }
                Err(e) => {
                    self.termination.press_error(e);
                    return Err(e);
                }
            }
        }
        if 0 < written_bytes || written_fin {
            if PRINT_DEBUG_MSGS {
                println!("send_pkts: {{ data: {written_bytes}; fin: {written_fin} }}");
            }
            self.coord.sent_data_pkt.notify_waiters();
        }
        if self.fec.is_some() {
            let now = Instant::now();
            let stock_can_send_tail_fec =
                { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
            let data_path = true;
            let can_send_tail_fec = self.fec_instream_flush
                || stock_can_send_tail_fec
                || (data_path && self.instream_group_fec_enabled());
            self.close_fec_burst(now, can_send_tail_fec).await?;
        }
        if self.ack_flush_is_due() {
            self.flush_acks(bufs).await?;
        }
        Ok(0 < written_bytes || written_fin)
    }

    async fn maybe_flush_full_fec_group(&mut self, now: Instant) -> Result<(), std::io::ErrorKind> {
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

    async fn close_fec_burst(
        &mut self,
        now: Instant,
        can_send_tail_fec: bool,
    ) -> Result<(), std::io::ErrorKind> {
        let Some(fec) = self.fec.as_ref() else {
            return Ok(());
        };
        {
            let mut fec = fec.lock().unwrap();
            if can_send_tail_fec {
                fec.note_tail_flush_allowed();
            } else {
                fec.note_tail_flush_blocked();
                fec.skip_open_group();
                return Ok(());
            }
        }
        self.flush_fec_parities(now).await
    }

    fn skip_open_fec_group(&self) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        fec.lock().unwrap().skip_open_group();
    }

    async fn flush_fec_parities(&mut self, now: Instant) -> Result<(), std::io::ErrorKind> {
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
                Err(std::io::ErrorKind::WouldBlock) => {
                    if FEC_DEBUG {
                        eprintln!("flush_fec_parities: WouldBlock (transient)");
                    }
                    return Ok(());
                }
                Err(e) => {
                    self.termination.press_error(e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    async fn send_due_post_open_response(&mut self) -> Result<(), std::io::ErrorKind> {
        let Some(response) = self.claim_post_open_response(Instant::now()) else {
            return Ok(());
        };
        match self.utp_write.send(&response.bytes).await {
            Ok(len) if len == response.bytes.len() => Ok(()),
            Ok(_) | Err(std::io::ErrorKind::WouldBlock) => {
                self.retry_post_open_response(Instant::now());
                Ok(())
            }
            Err(error) => {
                self.termination.press_error(error);
                Err(error)
            }
        }
    }

    pub async fn send_kill_pkt(&mut self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        let fec_enabled = self.send_kill_data_pkt(bufs).await?;
        if fec_enabled {
            self.flush_kill_fec_tail().await;
        }
        Ok(())
    }

    async fn send_kill_data_pkt(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Result<bool, std::io::ErrorKind> {
        let mut buf = [0; 1];
        encode_kill(&mut buf).unwrap();
        let fec_enabled = self.fec.is_some();
        let res = self.send_with_fec(&buf, &mut bufs.fec).await;
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
    pub async fn send_kill_and_abort(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Result<(), std::io::ErrorKind> {
        self.termination
            .press_broken_pipe(PeerReset::SendKill, None);
        match self.try_send_requested_kill(bufs).await {
            Some(result) => result,
            None => self.termination.throw_error(),
        }
    }

    #[cfg(test)]
    pub fn has_pending_acks(&self) -> bool {
        let s = self.ack_flush.lock().unwrap();
        0 < s.pending_acks || s.fin_pending
    }

    pub fn ack_flush_is_due(&self) -> bool {
        let s = self.ack_flush.lock().unwrap();
        if s.pending_acks == 0 && !s.fin_pending {
            return false;
        }
        let now = Instant::now();
        s.fin_pending
            || ACK_FLUSH_COUNT <= s.pending_acks
            || s.last_ack_flush
                .is_none_or(|last| ACK_FLUSH_AGE <= now.duration_since(last))
    }

    pub async fn flush_acks(&mut self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        {
            let s = self.ack_flush.lock().unwrap();
            if s.pending_acks == 0 && !s.fin_pending {
                return Ok(());
            }
        }
        self.flush_acks_inner(bufs).await
    }

    async fn flush_acks_inner(&mut self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        let now = Instant::now();
        let (cursor, history_count) = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let queue = reliable_layer.pkt_recv_space().ack_history();
            let count = queue.balls().count();
            let s = self.ack_flush.lock().unwrap();
            (s.ack_page_cursor.max(MAX_NUM_ACK).min(count), count)
        };
        let mut echo_ts = self.ack_flush.lock().unwrap().ts_echo.take();
        let echo_backup = echo_ts;
        let claimed_acks = self.ack_flush.lock().unwrap().pending_acks;
        let mut page_0 = true;
        let mut skip = 0;
        let fec_enabled = self.fec.is_some();
        let mut pages_sent: usize = 0;
        'ack_pages: loop {
            let written_bytes = {
                let reliable_layer = self.reliable_layer.lock().unwrap();
                let queue = reliable_layer.pkt_recv_space().ack_history();
                let ack = EncodeAck {
                    queue,
                    skip,
                    max_take: MAX_NUM_ACK,
                };
                let this_echo = echo_ts.take();
                encode_ack_data(Some(ack), this_echo, None, &mut bufs.utp).unwrap()
            };
            let res = self
                .send_with_fec(&bufs.utp[..written_bytes], &mut bufs.fec)
                .await;
            match res {
                Ok(_) => {
                    pages_sent += 1;
                    if fec_enabled {
                        let now = Instant::now();
                        let can_send_tail_fec =
                            { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
                        self.close_fec_burst(now, can_send_tail_fec).await?;
                    }
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    if let Some(ts) = echo_ts.take().or(echo_backup) {
                        self.ack_flush.lock().unwrap().ts_echo.restore(ts);
                    }
                    let mut s = self.ack_flush.lock().unwrap();
                    s.complete_claim(pages_sent * MAX_NUM_ACK, false);
                    drop(s);
                    self.coord.session_outbound_progress.notify_one();
                    break 'ack_pages;
                }
                Err(e) => {
                    self.termination.press_error(e);
                    if let Some(ts) = echo_ts.take().or(echo_backup) {
                        self.ack_flush.lock().unwrap().ts_echo.restore(ts);
                    }
                    return Err(e);
                }
            }
            if page_0 {
                page_0 = false;
                if history_count < MAX_NUM_ACK {
                    let mut s = self.ack_flush.lock().unwrap();
                    s.ack_page_cursor = MAX_NUM_ACK;
                    s.complete_claim(claimed_acks, true);
                    s.last_ack_flush = Some(now);
                    drop(s);
                    self.coord.session_outbound_progress.notify_one();
                    break;
                }
                skip = cursor;
                if skip > history_count {
                    let mut s = self.ack_flush.lock().unwrap();
                    s.ack_page_cursor = MAX_NUM_ACK;
                    s.complete_claim(claimed_acks, true);
                    s.last_ack_flush = Some(now);
                    drop(s);
                    self.coord.session_outbound_progress.notify_one();
                    break;
                }
            } else {
                let mut s = self.ack_flush.lock().unwrap();
                if cursor + MAX_NUM_ACK < history_count {
                    s.ack_page_cursor = cursor + MAX_NUM_ACK;
                } else {
                    s.ack_page_cursor = MAX_NUM_ACK;
                }
                s.complete_claim(claimed_acks, true);
                s.last_ack_flush = Some(now);
                drop(s);
                self.coord.session_outbound_progress.notify_one();
                break;
            }
        }
        Ok(())
    }

    async fn send_with_fec(
        &mut self,
        codec_pkt: &[u8],
        fec_buf: &mut [u8],
    ) -> Result<usize, std::io::ErrorKind> {
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
    use crate::send_queue::liveness::PeerLiveness;
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
}
