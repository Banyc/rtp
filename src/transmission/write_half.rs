use std::sync::Arc;
use std::time::Instant;

use crate::codec::{EncodeAck, EncodeData, encode_ack_data, encode_kill};
use super::shared::Shared;
use super::transmission_layer::{
    SendBufs, ACK_FLUSH_AGE, ACK_FLUSH_COUNT, FEC_DEBUG, MAX_NUM_ACK, MIN_NO_RESP_FOR,
    PRINT_DEBUG_MSGS, UnreliableWrite,
};

#[derive(Debug)]
pub struct WriteHalf {
    pub(crate) utp_write: tokio::sync::Mutex<Box<dyn UnreliableWrite>>,
    pub(crate) shared: Arc<Shared>,
}

impl std::ops::Deref for WriteHalf {
    type Target = Shared;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl WriteHalf {
    pub async fn send_pkts(&self, bufs: &mut SendBufs) -> Result<bool, std::io::ErrorKind> {
        let detect_broken_pipe_proactively = || {
            let now = Instant::now();
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let Some(no_resp_for) = reliable_layer.pkt_send_space().no_resp_for(now) else {
                return;
            };
            if no_resp_for < reliable_layer.pkt_send_space().rto_duration().mul_f64(16.0) {
                return;
            }
            if no_resp_for < MIN_NO_RESP_FOR {
                return;
            }
            self.first_error.set(std::io::ErrorKind::BrokenPipe);
        };
        detect_broken_pipe_proactively();

        let mut written_bytes = 0;
        let mut written_fin = false;
        loop {
            self.first_error.throw_error()?;
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
            let n = encode_ack_data(None, None, Some(data), &mut bufs.utp).unwrap();
            let utp_pkt = &bufs.utp[..n];
            if FEC_DEBUG {
                eprintln!("send_data_pkt seq={} data_len={}", p.seq, data_written);
            }
            let instream = self.instream_group_fec_enabled();
            let send_buf: &[u8] = {
                match self.fec.as_ref() {
                    Some(fec) => {
                        let mut fec = fec.lock().unwrap();
                        let fec_n = fec.encode_data(utp_pkt, &mut bufs.fec, instream);
                        &bufs.fec[..fec_n]
                    }
                    None => utp_pkt,
                }
            };
            let primary_res = {
                let mut guard = self.utp_write.lock().await;
                guard.send(send_buf).await
            };
            match primary_res {
                Ok(_) => {
                    if self.fec.is_some() && instream {
                        self.maybe_flush_full_fec_group(Instant::now()).await;
                    }
                    if self.rtx_dup() && was_repair && !queue_building {
                        let now = Instant::now();
                        let token_taken = self
                            .send_rate_limiter
                            .lock()
                            .unwrap()
                            .take_exact_tokens(1, now);
                        if token_taken {
                            match self.utp_write.lock().await.send(send_buf).await {
                                Ok(_) => {}
                                Err(std::io::ErrorKind::WouldBlock) => {
                                    if FEC_DEBUG {
                                        eprintln!("send_pkts: dup WouldBlock (transient)");
                                    }
                                }
                                Err(e) => {
                                    self.first_error.set(e);
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
                    self.first_error.set(e);
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
            self.close_fec_burst(now, can_send_tail_fec).await;
        }
        if self.ack_flush_is_due() {
            let _ = self.flush_acks(bufs).await;
        }
        Ok(0 < written_bytes || written_fin)
    }

    async fn maybe_flush_full_fec_group(&self, now: Instant) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        let should_flush = {
            let fec = fec.lock().unwrap();
            fec.group_data_full(self.instream_group_fec_enabled())
        };
        if !should_flush {
            return;
        }
        self.flush_fec_parities(now).await;
    }

    async fn close_fec_burst(&self, now: Instant, can_send_tail_fec: bool) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        {
            let mut fec = fec.lock().unwrap();
            if can_send_tail_fec {
                fec.note_tail_flush_allowed();
            } else {
                fec.note_tail_flush_blocked();
                fec.skip_open_group();
                return;
            }
        }
        self.flush_fec_parities(now).await;
    }

    fn skip_open_fec_group(&self) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        fec.lock().unwrap().skip_open_group();
    }

    async fn flush_fec_parities(&self, now: Instant) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        let parity_pkts = {
            let mut fec = fec.lock().unwrap();
            let mut tb = self.send_rate_limiter.lock().unwrap();
            fec.maybe_flush_parities(&mut tb, now, self.instream_group_fec_enabled())
        };
        for pkt in parity_pkts {
            match self.utp_write.lock().await.send(&pkt).await {
                Ok(_) => (),
                Err(std::io::ErrorKind::WouldBlock) => {
                    if FEC_DEBUG {
                        eprintln!("flush_fec_parities: WouldBlock (transient)");
                    }
                    return;
                }
                Err(_) => return,
            }
        }
    }

    pub async fn send_kill_pkt(&self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        let mut buf = [0; 1];
        encode_kill(&mut buf).unwrap();
        let fec_enabled = self.fec.is_some();
        let res = self.send_with_fec(&buf, &mut bufs.fec).await;
        if fec_enabled {
            match res {
                Ok(_) => {
                    let now = Instant::now();
                    let can_send_tail_fec =
                        { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
                    self.close_fec_burst(now, can_send_tail_fec).await;
                }
                Err(_) => self.skip_open_fec_group(),
            }
        }
        res?;
        Ok(())
    }

    pub async fn send_kill_and_abort(&self, bufs: &mut SendBufs) {
        let _ = self.send_kill_pkt(bufs).await;
        self.first_error.set(std::io::ErrorKind::BrokenPipe);
    }

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

    pub async fn flush_acks(&self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
        let _gate = self.ack_flush_gate.try_lock();
        if _gate.is_err() {
            return Ok(());
        }
        {
            let s = self.ack_flush.lock().unwrap();
            if s.pending_acks == 0 && !s.fin_pending {
                return Ok(());
            }
        }
        self.flush_acks_inner(bufs).await
    }

    async fn flush_acks_inner(&self, bufs: &mut SendBufs) -> Result<(), std::io::ErrorKind> {
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
                        self.close_fec_burst(now, can_send_tail_fec).await;
                    }
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    if let Some(ts) = echo_ts.take().or(echo_backup) {
                        self.ack_flush.lock().unwrap().ts_echo.restore(ts);
                    }
                    let mut s = self.ack_flush.lock().unwrap();
                    s.complete_claim(pages_sent * MAX_NUM_ACK, false);
                    break 'ack_pages;
                }
                Err(e) => {
                    self.first_error.set(e);
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
                    break;
                }
                skip = cursor;
                if skip > history_count {
                    let mut s = self.ack_flush.lock().unwrap();
                    s.ack_page_cursor = MAX_NUM_ACK;
                    s.complete_claim(claimed_acks, true);
                    s.last_ack_flush = Some(now);
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
                break;
            }
        }
        Ok(())
    }

    async fn send_with_fec(
        &self,
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
        self.utp_write.lock().await.send(send_buf).await
    }

    pub async fn send(
        &self,
        data: &[u8],
        no_delay: bool,
        bufs: &mut SendBufs,
    ) -> Result<usize, std::io::ErrorKind> {
        if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            self.send_frame(data, no_delay, bufs).await
        } else {
            self.send_stock(data, no_delay, bufs).await
        }
    }

    async fn send_stock(
        &self,
        data: &[u8],
        no_delay: bool,
        bufs: &mut SendBufs,
    ) -> Result<usize, std::io::ErrorKind> {
        let mut sent_data_pkt = self.coord.sent_data_pkt.notified();
        let written_bytes = loop {
            self.first_error.throw_error()?;
            let now = Instant::now();
            let written_bytes = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_data_buf(data, now)
            };
            self.log("send_data_buf");
            if no_delay {
                let made_progress = self.send_pkts(bufs).await?;
                if !made_progress && 0 < written_bytes {
                    self.resume_send().notify_one();
                }
            }
            if 0 < written_bytes {
                break written_bytes;
            }
            tokio::select! {
                _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_data_pkt = self.coord.sent_data_pkt.notified();
        };
        Ok(written_bytes)
    }

    pub async fn send_frame(
        &self,
        frame: &[u8],
        no_delay: bool,
        bufs: &mut SendBufs,
    ) -> Result<usize, std::io::ErrorKind> {
        let frame_len = frame.len();
        let mut sent_data_pkt = self.coord.sent_data_pkt.notified();
        loop {
            self.first_error.throw_error()?;
            let now = Instant::now();
            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.send_frame_buf(frame, now)
            };
            match res {
                Ok(()) => {
                    self.log("send_frame_buf");
                    if no_delay {
                        let made_progress = self.send_pkts(bufs).await?;
                        if !made_progress {
                            self.resume_send().notify_one();
                        }
                    }
                    return Ok(frame_len);
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    if no_delay {
                        self.send_pkts(bufs).await?;
                    }
                    tokio::select! {
                        _ = tokio::time::timeout(std::time::Duration::from_millis(10), sent_data_pkt) => (),
                        () = self.first_error.some().cancelled() => (),
                    }
                    sent_data_pkt = self.coord.sent_data_pkt.notified();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
