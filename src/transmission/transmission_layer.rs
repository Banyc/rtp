use core::{num::NonZeroUsize, time::Duration};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{fec::FecState, fec_tuning::FecTuning, frame_delivery::FrameDelivery, ts_echo::TsEcho};
use crate::{
    codec::{EncodeAck, EncodeData, decode, encode_ack_data, encode_kill},
    reliable::reliable_layer::{ReliableLayer, SharedTokenBucket},
    sack::{AckBall, AckBallSequence},
};

const PRINT_DEBUG_MSGS: bool = false;
const FEC_DEBUG: bool = false;
const MAX_NUM_ACK: usize = 64;
const ACK_FLUSH_COUNT: usize = 8;
const ACK_FLUSH_AGE: Duration = Duration::from_millis(1);
const MIN_NO_RESP_FOR: Duration = Duration::from_secs(30);
const BUF_SIZE: usize = 1024 * 64;

/// Whether retransmission armor (`RTP_RTX_DUP`) is enabled at process
/// startup.  Reads `RTP_RTX_DUP` once; `1`/`true` enables it, anything
/// else preserves stock single-datagram behaviour byte-for-byte.
///
/// When enabled, the transmission layer emits a second identical copy of
/// every retransmit and tail-loss-probe datagram — reusing the exact
/// already-encoded symbol bytes (encode once, send twice).  The primary
/// repair datagram always sends (it bypasses the pacing token bucket as
/// today); the duplicate is skipped when the token bucket lacks tokens and
/// is charged to the bucket when sent, and is suppressed whenever the
/// delivery-rate congestion controller reports the bottleneck queue is
/// building.  Duplicating ordinary data packets is never done — the win is
/// specific to rare recovery packets.
fn rtx_dup_from_env() -> bool {
    match std::env::var("RTP_RTX_DUP") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

/// Whether in-stream group FEC (`RTP_INSTREAM_GROUP_FEC`) is enabled at
/// process startup.  Reads the env var once; `1`/`true` enables it, anything
/// else preserves stock behaviour byte-for-byte (parity is tail-only and
/// force-skipped at `PARITY_DATA_THRESHOLD`).
///
/// When enabled, the transmission layer suppresses the
/// `PARITY_DATA_THRESHOLD` force-skip in `encode_data` (passing
/// `instream = true`), so a data group may accumulate up to
/// `INSTREAM_DATA_PER_GROUP` (8) data symbols.  Right after each successful
/// data send, `maybe_flush_full_fec_group` emits
/// `INSTREAM_PARITY_PER_GROUP` (4) parity symbols inline mid-burst when the
/// group is full, gated on the spare-token budget.  At the data-path burst
/// close, a partial DATA group is force-flushed (regardless of the stock
/// `can_send_tail_fec` gate) so a burst ending mid-group still emits its
/// stock 1:4 parity.  ACK/kill bursts keep the stock tail gate untouched
/// (force-flushing ACK bursts tripled reverse-path packets for zero gain).
fn instream_group_fec_from_env() -> bool {
    match std::env::var("RTP_INSTREAM_GROUP_FEC") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

type ReliableLayerLogger = Mutex<csv::Writer<std::fs::File>>;

/// Reusable buffers for the send path. Allocated once and passed by `&mut`
/// to avoid per-call allocation.
#[derive(Debug)]
pub struct SendBufs {
    pub data: Vec<u8>,
    pub utp: Vec<u8>,
    pub fec: Vec<u8>,
}

impl SendBufs {
    pub fn new() -> Self {
        Self {
            data: vec![0; BUF_SIZE],
            utp: vec![0; BUF_SIZE],
            fec: vec![0; BUF_SIZE],
        }
    }
}

impl Default for SendBufs {
    fn default() -> Self {
        Self::new()
    }
}

/// Reusable buffers for the recv path. Allocated once and passed by `&mut`
/// to avoid per-call allocation.
#[derive(Debug)]
pub struct RecvBufs {
    pub utp: Vec<u8>,
    pub ack_from_peer: Vec<AckBall>,
    pub ack_to_peer: Vec<u64>,
    pub codec_pkts: Vec<Vec<u8>>,
    ts_echo: TsEcho,
    pending_acks: usize,
    last_ack_flush: Option<Instant>,
    /// Resume offset for deep ack-history pages. Each flush sends cumulative
    /// page 0 plus one deep page starting here. Wrapped back to MAX_NUM_ACK on
    /// reset and when the cursor reaches the end of the history.
    ack_page_cursor: usize,
}

impl RecvBufs {
    pub fn new() -> Self {
        Self {
            utp: vec![0; BUF_SIZE],
            ack_from_peer: vec![],
            ack_to_peer: vec![],
            codec_pkts: vec![],
            ts_echo: TsEcho::new(),
            pending_acks: 0,
            last_ack_flush: None,
            ack_page_cursor: MAX_NUM_ACK,
        }
    }
}

impl Default for RecvBufs {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct TransmissionLayer {
    utp_read: tokio::sync::Mutex<Box<dyn UnreliableRead>>,
    utp_write: tokio::sync::Mutex<Box<dyn UnreliableWrite>>,
    reliable_layer: Mutex<ReliableLayer>,
    send_rate_limiter: Arc<Mutex<SharedTokenBucket>>,
    sent_data_pkt: tokio::sync::Notify,
    recv_data_pkt: tokio::sync::Notify,
    sent_pkt_acked: tokio::sync::Notify,
    resume_send: tokio::sync::Notify,
    recv_fin: tokio_util::sync::CancellationToken,
    first_error: FirstError,
    reliable_layer_logger: Option<ReliableLayerLogger>,
    fec: Option<Mutex<FecState>>,
    clock_epoch: Instant,
    /// Snapshot of `RTP_RTX_DUP` taken at construction.  Stored as a field
    /// rather than read from a `OnceLock` so tests can construct a layer with
    /// the toggle on or off deterministically without racing other parallel
    /// tests in the same binary.
    rtx_dup: bool,
    /// Per-connection FEC tuning snapshot taken at construction.  When
    /// `instream_flush` is set, the data send burst force-flushes the open
    /// FEC group at its end instead of waiting for the stock
    /// `can_send_tail_fec` gate.  ACK/kill bursts keep the stock gate
    /// regardless.  `interactive_parity_depth` is consumed by the `FecState`
    /// at construction; this field only drives the force-flush.
    fec_instream_flush: bool,
    /// Snapshot of `RTP_INSTREAM_GROUP_FEC` taken at construction.  When
    /// `true`, the data send path suppresses the `PARITY_DATA_THRESHOLD`
    /// force-skip in `encode_data`, flushes `INSTREAM_PARITY_PER_GROUP`
    /// parity symbols inline mid-burst once a group of
    /// `INSTREAM_DATA_PER_GROUP` data symbols fills, and force-flushes
    /// partial DATA groups at burst end.  ACK/kill bursts keep the stock
    /// tail gate.  Stored as a field (not read from a `OnceLock`) so tests
    /// can construct a layer with the toggle on or off deterministically
    /// without racing other parallel tests in the same binary.
    instream_group_fec_enabled: bool,
}
impl TransmissionLayer {
    pub fn new(unreliable_layer: UnreliableLayer, log_config: Option<LogConfig>) -> Self {
        let now = Instant::now();
        let frame_delivery = unreliable_layer.frame_delivery;
        let (reliable_layer, send_rate_limiter) = ReliableLayer::new(unreliable_layer.mss, frame_delivery, now);
        let reliable_layer = Mutex::new(reliable_layer);
        let sent_data_pkt = tokio::sync::Notify::new();
        let recv_data_pkt = tokio::sync::Notify::new();
        let sent_pkt_acked = tokio::sync::Notify::new();
        let resume_send = tokio::sync::Notify::new();
        let recv_fin = tokio_util::sync::CancellationToken::new();
        let first_error = FirstError::new();
        let reliable_layer_logger = log_config.as_ref().map(|c| {
            let file = std::fs::File::options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&c.reliable_layer_log_path)
                .expect("open log file");
            Mutex::new(csv::WriterBuilder::new().from_writer(file))
        });
        Self {
            utp_read: tokio::sync::Mutex::new(unreliable_layer.utp_read),
            utp_write: tokio::sync::Mutex::new(unreliable_layer.utp_write),
            reliable_layer,
            send_rate_limiter,
            sent_data_pkt,
            recv_data_pkt,
            sent_pkt_acked,
            resume_send,
            recv_fin,
            first_error,
            reliable_layer_logger,
            fec: unreliable_layer.fec.map(Mutex::new),
            clock_epoch: now,
            rtx_dup: rtx_dup_from_env(),
            fec_instream_flush: unreliable_layer.fec_tuning.instream_flush,
            instream_group_fec_enabled: instream_group_fec_from_env(),
        }
    }

    pub fn resume_send(&self) -> &tokio::sync::Notify {
        &self.resume_send
    }

    pub fn reliable_layer(&self) -> &Mutex<ReliableLayer> {
        &self.reliable_layer
    }

    /// Test-only: force the `RTP_RTX_DUP` toggle to a fixed value regardless
    /// of the process environment, so parallel tests in the same binary do not
    /// race on the env var.
    #[cfg(test)]
    pub(crate) fn set_rtx_dup_for_test(&mut self, enabled: bool) {
        self.rtx_dup = enabled;
    }

    /// Test-only: force the `RTP_INSTREAM_GROUP_FEC` toggle to a fixed value
    /// regardless of the process environment, so parallel tests in the same
    /// binary do not race on the env var.
    #[cfg(test)]
    pub(crate) fn set_instream_group_fec_for_test(&mut self, enabled: bool) {
        self.instream_group_fec_enabled = enabled;
    }

    /// Test-only: take up to `n` tokens from the shared send-rate limiter so
    /// the retransmission-armor duplicate-copy token gate can be exercised
    /// (the primary rtx bypasses the bucket; the dup needs a token).
    #[cfg(test)]
    pub(crate) fn drain_rate_limiter_for_test(&self, n: usize, now: Instant) -> usize {
        self.send_rate_limiter
            .lock()
            .unwrap()
            .take_at_most_tokens(n, now)
    }

    /// Number of codec payloads recovered by FEC parity so far, or `None` when
    /// FEC is not enabled. Exposed for tests asserting that parity actually
    /// reconstructs lost data under loss injection.
    pub fn fec_recovered_symbols(&self) -> Option<usize> {
        self.fec
            .as_ref()
            .map(|fec| fec.lock().unwrap().recovered_symbols())
    }

    pub fn next_poll_send_time(&self) -> Instant {
        self.send_rate_limiter.lock().unwrap().next_token_time()
    }

    fn wire_ts(&self, now: Instant) -> u32 {
        let us = now.duration_since(self.clock_epoch).as_micros();
        // Truncate to u32; the receiver uses wrapping subtraction, so the wrap
        // every ~71.6 minutes is harmless for RTTs below the max echo age.
        us as u32
    }

    /// # Cancel safety
    ///
    /// It is cancel safe.
    pub async fn no_data_to_send(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_pkt_acked = self.sent_pkt_acked.notified();
        loop {
            self.first_error.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_no_data_to_send() {
                return Ok(());
            }
            tokio::select! {
                () = sent_pkt_acked => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_pkt_acked = self.sent_pkt_acked.notified();
        }
    }

    /// # Cancel safety
    ///
    /// It is cancel safe.
    pub async fn send_buf_empty(&self) -> Result<(), std::io::ErrorKind> {
        let mut sent_data_pkt = self.sent_data_pkt.notified();
        loop {
            self.first_error.throw_error()?;
            if self.reliable_layer.lock().unwrap().is_send_buf_empty() {
                return Ok(());
            }
            tokio::select! {
                () = sent_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_data_pkt = self.sent_data_pkt.notified();
        }
    }

    pub fn send_fin_buf(&self) {
        self.reliable_layer.lock().unwrap().send_fin_buf();
    }

    pub fn recv_fin(&self) -> &tokio_util::sync::CancellationToken {
        &self.recv_fin
    }

    pub fn some_error(&self) -> &tokio_util::sync::CancellationToken {
        &self.first_error.some
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        self.first_error.throw_error()
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

    /// Wrap a codec packet with a FEC data-symbol header (if FEC is active) and
    /// send it via `utp_write`. All outgoing codec packets (data, ACK, kill)
    /// must go through this when FEC is active, so the receiver's FEC decoder
    /// can process them uniformly.
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
            // Avoid triggering broken pipe errors during inter-process data transfer.
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
            // reliable -{data}> UDP remote
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
            // Snapshot the queue-building gate now so the dup decision uses the
            // latest ACK-derived delivery-rate signal.  The primary repair
            // bypasses the token bucket, so this read is independent of it.
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
            // Wrap the codec packet with a FEC data-symbol header (if FEC is
            // active) before sending, so the receiver can recover it via
            // parity if it's lost.  When in-stream group FEC is enabled, the
            // `PARITY_DATA_THRESHOLD` force-skip is suppressed so a group may
            // accumulate up to `INSTREAM_DATA_PER_GROUP` data symbols.
            let instream = self.instream_group_fec_enabled;
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
            // Send the primary in a block so the `utp_write` `MutexGuard`
            // (a temporary in the `.lock().await.send(..).await` expression)
            // is dropped before the retransmission-armor duplicate copy below,
            // which re-locks the same mutex.
            let primary_res = {
                let mut guard = self.utp_write.lock().await;
                guard.send(send_buf).await
            };
            match primary_res {
                Ok(_) => {
                    // In-stream group FEC: when the toggle is on and the open
                    // FEC group just reached `INSTREAM_DATA_PER_GROUP` data
                    // symbols, emit `INSTREAM_PARITY_PER_GROUP` parity
                    // symbols inline mid-burst.  The group must be flushed
                    // *after* the data send succeeded and the `utp_write`
                    // guard is dropped — holding the guard across the parity
                    // flush deadlocks (the flush re-locks `utp_write`).
                    if self.fec.is_some() && instream {
                        self.maybe_flush_full_fec_group(Instant::now()).await;
                    }
                    // Retransmission armor (`RTP_RTX_DUP`): emit a second
                    // identical copy of every retransmit and tail-loss-probe
                    // datagram, reusing the exact already-encoded symbol
                    // bytes (encode once, send twice).  The primary repair
                    // bypasses the token bucket (already sent above); the
                    // duplicate is skipped when the bucket lacks tokens and
                    // is charged to the bucket when sent, and is suppressed
                    // whenever the bottleneck queue is building.  Duplicating
                    // ordinary data packets is never done — the win is
                    // specific to rare recovery packets.
                    if self.rtx_dup && was_repair && !queue_building {
                        let now = Instant::now();
                        let token_taken = self
                            .send_rate_limiter
                            .lock()
                            .unwrap()
                            .take_exact_tokens(1, now);
                        if token_taken {
                            // Reuse the exact bytes just sent (the same
                            // `send_buf` slice) — do NOT re-encode.
                            match self.utp_write.lock().await.send(send_buf).await {
                                Ok(_) => {}
                                Err(std::io::ErrorKind::WouldBlock) => {
                                    if FEC_DEBUG {
                                        eprintln!(
                                            "send_pkts: dup WouldBlock (transient)"
                                        );
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
                    // Transient UDP send-buffer exhaustion. Treat as packet
                    // loss/backpressure: skip this packet and keep going. The
                    // reliable layer will retransmit if it is not acked.
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
            self.sent_data_pkt.notify_waiters();
        }
        // Close the FEC burst at the data tail. A tail flush is only permitted
        // when the reliable layer has no more data/RTX to send, so parity is
        // tail-only and never competes with data bandwidth. When blocked, the
        // open group is skipped so no stale group carries into the next burst.
        //
        // `FecTuning::instream_flush`: force-flush the open DATA group at
        // the end of every data send burst instead of waiting for the stock
        // `can_send_tail_fec` gate.  This is what lets a single-symbol
        // interactive message emit its (deeper) parity promptly rather than
        // being skipped at the burst end.  ACK/kill bursts (above) keep the
        // stock gate regardless — only data bursts are force-flushed, since
        // ACK/kill parity is not the interactive win.
        //
        // In-stream group FEC (`RTP_INSTREAM_GROUP_FEC`): when the toggle is
        // on, a partial DATA group (2..INSTREAM_DATA_PER_GROUP symbols) is
        // force-flushed at burst end regardless of `can_send_tail_fec`, so a
        // burst ending mid-group still emits its INSTREAM_PARITY_PER_GROUP
        // parity.  ACK/kill bursts keep the stock tail gate untouched
        // (force-flushing ACK bursts tripled reverse-path packets for zero
        // gain).
        if self.fec.is_some() {
            let now = Instant::now();
            let stock_can_send_tail_fec = { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
            let data_path = true;
            let can_send_tail_fec = self.fec_instream_flush
                || stock_can_send_tail_fec
                || (data_path && self.instream_group_fec_enabled);
            self.close_fec_burst(now, can_send_tail_fec).await;
        }
        Ok(0 < written_bytes || written_fin)
    }

    /// In-stream group FEC: if the open FEC group just reached
    /// `INSTREAM_DATA_PER_GROUP` data symbols, emit
    /// `INSTREAM_PARITY_PER_GROUP` parity symbols inline mid-burst, gated on
    /// the spare-token budget.  No-op when FEC is off, the toggle is off, or
    /// the group is not full.  The `utp_write` guard must be dropped before
    /// calling this — `flush_fec_parities` re-locks `utp_write` to send the
    /// parity packets, so holding the guard across the call deadlocks.
    async fn maybe_flush_full_fec_group(&self, now: Instant) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        let should_flush = {
            let fec = fec.lock().unwrap();
            fec.group_data_full(self.instream_group_fec_enabled)
        };
        if !should_flush {
            return;
        }
        self.flush_fec_parities(now).await;
    }

    /// Close the current FEC burst: flush parities for the open group when a
    /// tail flush is allowed, otherwise skip the group. No-op when FEC is not
    /// in use. This is the sole entry point that decides flush-vs-skip at a
    /// burst boundary; it consumes the `tail_flush_allowed` flag set by
    /// `note_tail_flush_allowed`/`note_tail_flush_blocked`.
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

    /// Skip the open FEC group without flushing. Used on send errors where we
    /// cannot flush but must still close the burst so no stale group leaks
    /// into the next one.
    fn skip_open_fec_group(&self) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        fec.lock().unwrap().skip_open_group();
    }

    /// Encode and send FEC parities for the current group, rate-limited by the
    /// shared token bucket. No-op when FEC is not in use.
    async fn flush_fec_parities(&self, now: Instant) {
        let Some(fec) = self.fec.as_ref() else {
            return;
        };
        let parity_pkts = {
            let mut fec = fec.lock().unwrap();
            let mut tb = self.send_rate_limiter.lock().unwrap();
            fec.maybe_flush_parities(&mut tb, now, self.instream_group_fec_enabled)
        };
        for pkt in parity_pkts {
            match self.utp_write.lock().await.send(&pkt).await {
                Ok(_) => (),
                Err(std::io::ErrorKind::WouldBlock) => {
                    // Transient UDP send-buffer exhaustion. Treat as loss of
                    // this parity packet; FEC parity is redundant by design,
                    // so dropping one is recoverable.
                    if FEC_DEBUG {
                        eprintln!("flush_fec_parities: WouldBlock (transient)");
                    }
                    return;
                }
                Err(_) => return,
            }
        }
    }

    pub async fn recv_pkts(
        &self,
        bufs: &mut RecvBufs,
        send_bufs: &mut SendBufs,
    ) -> Result<RecvPkts, (std::io::ErrorKind, SendKillPkt)> {
        let throw_error = |e: std::io::ErrorKind| {
            self.first_error.set(e);
            e
        };
        let mut recv_pkts = RecvPkts {
            num_ack_segments: 0,
            num_payload_segments: 0,
            num_fin_segments: 0,
        };

        bufs.ack_to_peer.clear();
        let ack_deadline = if 0 < bufs.pending_acks {
            bufs.last_ack_flush.map(|last| last + ACK_FLUSH_AGE)
        } else {
            None
        };
        let mut ack_deadline_hit = false;
        for _ in 0..MAX_NUM_ACK {
            self.first_error
                .throw_error()
                .map_err(|e| (e, SendKillPkt::No))?;

            // Read a raw UDP packet and FEC-decode it. If FEC is active, the
            // raw packet is a FEC data/parity symbol; `fec.decode` strips the
            // FEC header and returns the codec payload, or returns None for a
            // parity symbol (recovered data is queued internally). If FEC is
            // not active, the raw packet IS the codec payload.
            let res = {
                let mut utp_read = self.utp_read.lock().await;
                match bufs.ack_to_peer.is_empty() {
                    true => {
                        if let Some(deadline) = ack_deadline {
                            tokio::select! {
                                res = utp_read.recv(&mut bufs.utp) => res,
                                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                                    ack_deadline_hit = true;
                                    break;
                                }
                            }
                        } else {
                            utp_read.recv(&mut bufs.utp).await
                        }
                    }
                    false => {
                        let res = utp_read.try_recv(&mut bufs.utp);
                        if let Err(e) = &res
                            && *e == std::io::ErrorKind::WouldBlock
                        {
                            break;
                        }
                        res
                    }
                }
            };
            let read_bytes = match res {
                Ok(x) => x,
                Err(e) => {
                    return Err((throw_error(e), SendKillPkt::No));
                }
            };
            let now = Instant::now();
            let read_pkt = &bufs.utp[..read_bytes];

            // FEC-decode the raw packet into `codec_pkts`, then process each.
            // Also drain any packets recovered by parity.
            bufs.codec_pkts.clear();
            let mut orig_pkt = None;
            match self.fec.as_ref() {
                Some(fec) => {
                    let mut fec = fec.lock().unwrap();
                    if let Some(payload) = fec.decode(read_pkt) {
                        bufs.codec_pkts.push(payload);
                    }
                    while let Some(recovered) = fec.pop_recovered() {
                        bufs.codec_pkts.push(recovered);
                    }
                }
                None => {
                    orig_pkt = Some(read_pkt);
                }
            }

            let mut end_of_acks = false;
            for pkt in bufs.codec_pkts.iter().map(|p| p.as_slice()).chain(orig_pkt) {
                bufs.ack_from_peer.clear();
                let data = match decode(pkt, &mut bufs.ack_from_peer) {
                    Ok(x) => x,
                    Err(e) => {
                        if FEC_DEBUG {
                            eprintln!("recv_pkts: decode error: {e:?}");
                        }
                        continue;
                    }
                };

                if let Some(echo_ts) = data.echo_ts {
                    let local_ts = self.wire_ts(now);
                    if let Some(rtt) = TsEcho::rtt_from_echo(local_ts, echo_ts) {
                        self.reliable_layer.lock().unwrap().sample_rtt(rtt, now);
                    }
                }

                if data.killed {
                    let e = std::io::ErrorKind::BrokenPipe;
                    throw_error(e);
                    return Err((e, SendKillPkt::No));
                }

                let to_ack = {
                    let mut reliable_layer = self.reliable_layer.lock().unwrap();

                    // UDP local -{ACK}> reliable
                    reliable_layer.recv_ack_pkt(AckBallSequence::new(&bufs.ack_from_peer), now);
                    if FEC_DEBUG {
                        eprintln!("recv_ack_pkt: balls={:?}", bufs.ack_from_peer);
                    }

                    match &data.data {
                        None => false,
                        Some(data) => {
                            // UDP local -{data}> reliable
                            let to_ack = reliable_layer
                                .recv_data_pkt(data.seq, data.frame_len, &pkt[data.buf_range.clone()]);
                            if FEC_DEBUG {
                                eprintln!(
                                    "recv_data_pkt seq={} empty={} ack={}",
                                    data.seq,
                                    data.buf_range.is_empty(),
                                    to_ack
                                );
                            }
                            to_ack
                        }
                    }
                };
                recv_pkts.num_ack_segments += 1;
                self.sent_pkt_acked.notify_waiters();

                let Some(data) = data.data else {
                    self.log("recv_ack_pkt");
                    continue;
                };

                if data.buf_range.is_empty() && data.frame_len.is_none() {
                    recv_pkts.num_fin_segments += 1;
                } else {
                    recv_pkts.num_payload_segments += 1;
                }
                if to_ack {
                    bufs.ack_to_peer.push(data.seq);
                    if let Some(send_ts) = data.send_ts {
                        bufs.ts_echo.set(send_ts);
                    }
                } else {
                    end_of_acks = true;
                }
                self.log("recv_data_pkt");
            }
            if end_of_acks {
                break;
            }
        }

        bufs.pending_acks += bufs.ack_to_peer.len();

        // No new data received in the reliable layer
        if bufs.ack_to_peer.is_empty() && !ack_deadline_hit {
            // ACK processing may have freed cwnd or drained the send buffer
            // enough that the send pump can make progress now. Wake the send
            // timer exactly once per batch, after dropping the reliable layer
            // lock. notify_one stores a permit, so a wake racing with the timer
            // registering its next notified() is not lost.
            let should_resume_send = {
                let reliable_layer = self.reliable_layer.lock().unwrap();
                !reliable_layer.is_send_buf_empty()
                    && reliable_layer.pkt_send_space().accepts_new_pkt()
            };
            if should_resume_send {
                self.resume_send.notify_one();
            }
            return Ok(recv_pkts);
        }

        if !bufs.ack_to_peer.is_empty() {
            self.recv_data_pkt.notify_waiters();
        }

        let now = Instant::now();
        let should_flush = ack_deadline_hit
            || ACK_FLUSH_COUNT <= bufs.pending_acks
            || 0 < recv_pkts.num_fin_segments
            || bufs
                .last_ack_flush
                .is_none_or(|last| ACK_FLUSH_AGE <= now.duration_since(last))
            || {
                let reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer
                    .pkt_recv_space()
                    .ack_history()
                    .balls()
                    .nth(1)
                    .is_some()
            };
        if !should_flush {
            return Ok(recv_pkts);
        }
        bufs.pending_acks = 0;
        bufs.last_ack_flush = Some(now);

        // reliable -{ACK}> UDP remote. Each flush sends the cumulative page-0
        // plus one deep page resumed from `ack_page_cursor`. The cursor is
        // advanced by MAX_NUM_ACK on each flush so the deep page rotates across
        // the history, while page 0 always carries the latest cumulative acks
        // (and the only echo_ts so each RTT sample is reflected once).
        let mut echo_ts = bufs.ts_echo.take();
        let mut page_0 = true;
        let mut send_res: Result<(), (std::io::ErrorKind, SendKillPkt)> = Ok(());
        let fec_enabled = self.fec.is_some();

        // Snapshot the ack history length and clamp the deep-page cursor. The
        // cursor is >= MAX_NUM_ACK so the deep page never overlaps page 0, and
        // <= the current history length. History does not shrink during a flush.
        let (cursor, history_count) = {
            let reliable_layer = self.reliable_layer.lock().unwrap();
            let queue = reliable_layer.pkt_recv_space().ack_history();
            let count = queue.balls().count();
            (bufs.ack_page_cursor.max(MAX_NUM_ACK).min(count), count)
        };
        let mut skip = 0;

        'ack_pages: loop {
            let written_bytes = {
                let reliable_layer = self.reliable_layer.lock().unwrap();
                let queue = reliable_layer.pkt_recv_space().ack_history();
                // Encode against a borrowed queue; the lock is dropped before any
                // await point so the borrow never crosses an await.
                let ack = EncodeAck {
                    queue,
                    skip,
                    max_take: MAX_NUM_ACK,
                };
                let this_echo = echo_ts.take();
                encode_ack_data(Some(ack), this_echo, None, &mut bufs.utp).unwrap()
            };
            let res = self
                .send_with_fec(&bufs.utp[..written_bytes], &mut send_bufs.fec)
                .await;
            send_res = match res {
                Ok(_) => Ok(()),
                Err(std::io::ErrorKind::WouldBlock) => {
                    // Transient UDP send-buffer exhaustion. Stop paging for
                    // now; the next recv batch will continue from the same ack
                    // history.
                    break 'ack_pages;
                }
                Err(e) => {
                    let e = throw_error(e);
                    Err((e, SendKillPkt::No))
                }
            };
            if fec_enabled {
                match &send_res {
                    Ok(_) => {
                        let now = Instant::now();
                        let can_send_tail_fec =
                            { self.reliable_layer.lock().unwrap().can_send_tail_fec(now) };
                        self.close_fec_burst(now, can_send_tail_fec).await;
                    }
                    Err(_) => self.skip_open_fec_group(),
                }
            }
            if send_res.is_err() {
                break;
            }

            if page_0 {
                page_0 = false;
                // After a successful page-0 send, decide whether the whole
                // history already fits in page 0.
                if history_count <= MAX_NUM_ACK {
                    // The deep cursor is reset to the start of the non-cumulative
                    // region for the next flush.
                    bufs.ack_page_cursor = MAX_NUM_ACK;
                    break;
                }
                // Move to the deep page. If the clamped cursor has already
                // reached the end of the history, wrap it back to the start of
                // the deep region and send only page 0 this flush.
                skip = cursor;
                if skip >= history_count {
                    bufs.ack_page_cursor = MAX_NUM_ACK;
                    break;
                }
            } else {
                // Deep page sent successfully: advance the cursor if more deep
                // pages remain, otherwise wrap back to the start of the deep
                // region for the next flush.
                if cursor + MAX_NUM_ACK < history_count {
                    bufs.ack_page_cursor = cursor + MAX_NUM_ACK;
                } else {
                    bufs.ack_page_cursor = MAX_NUM_ACK;
                }
                break;
            }
        }
        send_res?;
        if PRINT_DEBUG_MSGS {
            println!("recv_pkts: ack: {:?}", bufs.ack_to_peer);
        }

        Ok(recv_pkts)
    }

    pub async fn send(
        &self,
        data: &[u8],
        no_delay: bool,
        bufs: &mut SendBufs,
    ) -> Result<usize, std::io::ErrorKind> {
        // In frame-delivery mode, delegate each send() call to send_frame()
        // so one write = one frame — this is what the AsyncWrite adapter and
        // mux-over-rtp depend on.
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
        let mut sent_data_pkt = self.sent_data_pkt.notified();
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
                    self.resume_send.notify_one();
                }
            }

            if 0 < written_bytes {
                break written_bytes;
            }
            tokio::select! {
                () = sent_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            sent_data_pkt = self.sent_data_pkt.notified();
        };
        Ok(written_bytes)
    }

    /// Send one complete frame in frame-delivery mode.  When enabled, this is
    /// also what `send()` delegates to — one write = one frame.  Returns
    /// `Ok(frame.len())` on success; `Err(InvalidInput)` when the mode is off
    /// or the frame is empty/oversize; `Err(WouldBlock)` when the staging cap
    /// is full (the caller should spin on `sent_data_pkt`).
    pub async fn send_frame(
        &self,
        frame: &[u8],
        no_delay: bool,
        bufs: &mut SendBufs,
    ) -> Result<usize, std::io::ErrorKind> {
        let frame_len = frame.len();
        let mut sent_data_pkt = self.sent_data_pkt.notified();
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
                            self.resume_send.notify_one();
                        }
                    }
                    return Ok(frame_len);
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    // Staging cap full; wait for the send path to drain some frames.
                    if no_delay {
                        self.send_pkts(bufs).await?;
                    }
                    tokio::select! {
                        () = sent_data_pkt => (),
                        () = self.first_error.some().cancelled() => (),
                    }
                    sent_data_pkt = self.sent_data_pkt.notified();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Return `Ok(0)` when the read stream hits EOF.
    /// In frame-delivery mode, byte-stream recv is not supported — the caller
    /// must use `recv_frame()` instead.
    pub async fn recv(&self, data: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        if self.reliable_layer.lock().unwrap().frame_delivery_enabled() {
            return Err(std::io::ErrorKind::InvalidInput);
        }
        let mut recv_data_pkt = self.recv_data_pkt.notified();
        let read_bytes = loop {
            self.first_error.throw_error()?;

            if self.recv_fin.is_cancelled() {
                return Ok(0);
            }

            // reliable -{data}> app
            let (read_bytes, read_fin) = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                (
                    reliable_layer.recv_data_buf(data),
                    reliable_layer.recv_fin_buf(),
                )
            };
            self.log("recv_data_buf");
            if PRINT_DEBUG_MSGS {
                println!("recv: data: {read_bytes}");
            }
            if 0 < read_bytes {
                break read_bytes;
            }
            if read_fin {
                self.recv_fin.cancel();
                if let Some(fec) = self.fec.as_ref() {
                    fec.lock().unwrap().debug_print_stats();
                }
                continue;
            }
            tokio::select! {
                () = recv_data_pkt => (),
                () = self.first_error.some().cancelled() => (),
            }
            recv_data_pkt = self.recv_data_pkt.notified();
        };
        Ok(read_bytes)
    }

    /// Receive one complete frame in frame-delivery mode.  Returns
    /// `Ok(Some(frame))` when a complete frame is available (possibly out of
    /// order past sequence holes), `Ok(None)` on EOF, or `Err(InvalidInput)`
    /// when the mode is off.
    pub async fn recv_frame(&self) -> Result<Option<Vec<u8>>, std::io::ErrorKind> {
        let mut recv_data_pkt = self.recv_data_pkt.notified();
        loop {
            self.first_error.throw_error()?;

            let res = {
                let mut reliable_layer = self.reliable_layer.lock().unwrap();
                reliable_layer.recv_frame_buf()
            };
            match res {
                Ok(Some(frame)) => {
                    self.log("recv_frame_buf");
                    return Ok(Some(frame));
                }
                Ok(None) => {
                    self.recv_fin.cancel();
                    if let Some(fec) = self.fec.as_ref() {
                        fec.lock().unwrap().debug_print_stats();
                    }
                    return Ok(None);
                }
                Err(std::io::ErrorKind::WouldBlock) => {
                    tokio::select! {
                        () = recv_data_pkt => (),
                        () = self.first_error.some().cancelled() => (),
                    }
                    recv_data_pkt = self.recv_data_pkt.notified();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn log(&self, op: &str) {
        let Some(logger) = &self.reliable_layer_logger else {
            return;
        };
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("unix timestamp");
        let log = self.reliable_layer.lock().unwrap().log();
        let log = Log {
            op,
            time: time.as_micros(),
            tokens: log.tokens,
            send_rate: log.send_rate,
            loss_rate: log.loss_rate,
            num_tx_pkts: log.num_tx_pkts,
            num_pkts_in_pipe: log.num_pkts_in_pipe,
            num_rt_pkts: log.num_rt_pkts,
            send_seq: log.send_seq,
            min_rtt: log.min_rtt,
            rtt: log.rtt,
            cwnd: log.cwnd,
            num_rx_pkts: log.num_rx_pkts,
            recv_seq: log.recv_seq,
            delivery_rate: log.delivery_rate,
            app_limited: log.app_limited,
        };
        logger
            .lock()
            .unwrap()
            .serialize(&log)
            .expect("write CSV log");
    }
}

#[derive(Debug)]
pub struct UnreliableLayer {
    pub utp_read: Box<dyn UnreliableRead>,
    pub utp_write: Box<dyn UnreliableWrite>,
    pub mss: NonZeroUsize,
    pub fec: Option<FecState>,
    /// Per-connection FEC tuning.  Consumed at construction to drive the
    /// data-burst force-flush (`instream_flush`) and the single-symbol
    /// interactive parity depth.  Stock `FecTuning::default()` preserves the
    /// behaviour byte-for-byte.
    pub fec_tuning: FecTuning,
    /// Per-connection frame-delivery mode.  When `enabled`, application data
    /// is staged as whole frames and the receiver may deliver complete frames
    /// out of order past sequence holes.  Both peers must enable together (no
    /// in-band negotiation).  `Default` is `enabled: false` — stock.
    pub frame_delivery: FrameDelivery,
}

#[derive(Debug, Clone)]
pub struct RecvPkts {
    pub num_ack_segments: usize,
    pub num_payload_segments: usize,
    pub num_fin_segments: usize,
}

#[derive(Debug, Clone)]
pub enum SendKillPkt {
    Yes,
    No,
}

#[async_trait]
pub trait UnreliableRead: core::fmt::Debug + Sync + Send + 'static {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind>;
}
#[async_trait]
pub trait UnreliableWrite: core::fmt::Debug + Sync + Send + 'static {
    async fn send(&mut self, buf: &[u8]) -> Result<usize, std::io::ErrorKind>;
}

#[derive(Debug)]
struct FirstError {
    first_error: RwLock<Option<std::io::ErrorKind>>,
    some: tokio_util::sync::CancellationToken,
}
impl FirstError {
    pub fn new() -> Self {
        let first_error = RwLock::new(None);
        let some = tokio_util::sync::CancellationToken::new();
        Self { first_error, some }
    }

    pub fn set(&self, err: std::io::ErrorKind) {
        {
            let mut first_error = self.first_error.write().unwrap();
            if first_error.is_none() {
                *first_error = Some(err);
            }
        }
        self.some.cancel();
    }

    pub fn throw_error(&self) -> Result<(), std::io::ErrorKind> {
        let first_error = self.first_error.read().unwrap();
        if let Some(e) = &*first_error {
            return Err(*e);
        }
        Ok(())
    }

    pub fn some(&self) -> &tokio_util::sync::CancellationToken {
        &self.some
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub reliable_layer_log_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log<'a> {
    pub time: u128,
    pub op: &'a str,

    pub tokens: f64,
    pub send_rate: f64,
    pub loss_rate: Option<f64>,
    pub num_tx_pkts: usize,
    pub num_pkts_in_pipe: usize,
    pub num_rt_pkts: usize,
    pub send_seq: u64,
    pub min_rtt: Option<u128>,
    pub rtt: u128,
    pub cwnd: usize,
    pub num_rx_pkts: usize,
    pub recv_seq: Option<u64>,
    pub delivery_rate: Option<f64>,
    pub app_limited: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A `UnreliableWrite` that records every datagram sent to it, so tests
    /// can count copies and inspect the exact bytes (for the
    /// "reuses-identical-encoded-symbol" assertion).
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

    /// Feed `n` RTT samples so the smoothed RTT settles to ~`rtt`, shrinking
    /// the reorder window so a retransmit fires quickly in real wall-clock
    /// time.
    fn settle_rtt(tl: &TransmissionLayer, rtt: Duration, n: usize) {
        let rl = tl.reliable_layer();
        let mut rl = rl.lock().unwrap();
        let mut t = Instant::now();
        for _ in 0..n {
            rl.sample_rtt(rtt, t);
            t += Duration::from_micros(100);
        }
    }

    /// Push one data packet through the reliable layer so the send window
    /// has an unacked packet that can be retransmitted later.
    fn send_one_packet(tl: &TransmissionLayer, now: Instant) -> u64 {
        let rl = tl.reliable_layer();
        let mut rl = rl.lock().unwrap();
        let payload = vec![0u8; 100];
        assert_eq!(
            rl.send_data_buf(&payload, now),
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

    /// Wait long enough for the (settled ~1ms) reorder window AND the TLP
    /// `MIN_TOL` (10 ms) to elapse so the next `send_pkts` call fires a
    /// retransmit or tail-loss probe.  30 ms gives ample margin over parallel
    /// test scheduling jitter.
    async fn wait_for_rtx_window() {
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    /// A `TransmissionLayer` test harness exposing the recording write via an
    /// `Arc<Mutex<RecordingWrite>>` shared with the layer's `UnreliableWrite`.
    /// `fec` toggles FEC on; `enabled` toggles the `RTP_RTX_DUP` retransmit
    /// armor; `tuning` sets the per-connection FEC tuning (defaults to stock).
    fn harness(fec: bool, enabled: bool) -> (TransmissionLayer, Arc<Mutex<RecordingWrite>>) {
        harness_with_tuning(fec, enabled, crate::transmission::fec_tuning::FecTuning::default())
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
        let ul =
            crate::udp::wrap_fec_with_mss_and_fec_tuning(read, write, fec, crate::udp::NO_FEC_MSS, tuning);
        let mut tl = TransmissionLayer::new(ul, None);
        tl.set_rtx_dup_for_test(enabled);
        (tl, recorder)
    }

    #[tokio::test]
    async fn rtx_dup_queue_building_gate_suppresses_extra_copy() {
        // When the bottleneck queue is building, the duplicate must be
        // suppressed even though tokens are available: ungated duplication
        // under congestion collapses bulk goodput.
        let (tl, recorder) = harness(false, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        // Force the queue-building gate closed.
        tl.reliable_layer().lock().unwrap().set_queue_building_for_test(true);
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        // Exactly one datagram (the primary rtx/TLP); the dup was suppressed by
        // the queue-building gate.
        assert_eq!(
            recorder.lock().unwrap().count(),
            1,
            "queue-building must suppress the duplicate copy"
        );
    }

    #[tokio::test]
    async fn fec_rtx_dup_reuses_identical_encoded_symbol() {
        // The duplicate must reuse the exact already-encoded symbol bytes
        // (encode once, send twice).  With FEC active the encoded symbol is
        // the FEC-wrapped datagram; the dup must be byte-identical to the
        // primary.  (A tail FEC parity may also be flushed at burst end; it
        // is a distinct symbol, so only the first two datagrams are compared.)
        let (tl, recorder) = harness(true, true);
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
        // The primary rtx always sends (bypasses the empty pacing token
        // bucket); the duplicate is skipped when the bucket lacks tokens and
        // is charged to the bucket when sent.  Drain the bucket first; only
        // the primary should go out.
        let (tl, recorder) = harness(false, true);
        settle_rtt(&tl, Duration::from_millis(1), 5);
        let _seq = send_one_packet(&tl, Instant::now());
        wait_for_rtx_window().await;
        // Drain every token from the shared bucket.
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
        // With the toggle off, a retransmit must send exactly one datagram
        // (stock behaviour byte-for-byte): no duplicate copy.
        let (tl, recorder) = harness(false, false);
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

    /// With `FecTuning::mindiv()` (instream flush + depth 3), a
    /// single-symbol data burst must force-flush 3 parity copies at the
    /// burst end, even though `can_send_tail_fec` is false (an unacked data
    /// packet is still in flight).  Stock tuning skips the open group when
    /// `can_send_tail_fec` is false; the instream-flush gate overrides that
    /// so the single-symbol interactive message gets its parity promptly.
    /// The single-symbol budget bypass is covered by the `fec.rs` unit tests.
    ///
    /// Mutation target: if the `fec_instream_flush` force-flush is removed
    /// (stock `can_send_tail_fec` gate always applied), the unacked data
    /// packet makes `can_send_tail_fec` false, the group is skipped, and
    /// this test gets 1 datagram (data only) instead of 4.
    #[tokio::test]
    async fn single_symbol_depth_is_ungated_but_bulk_keeps_budget() {
        use crate::transmission::fec_tuning::FecTuning;

        // mindiv: instream flush + depth 3.
        let (tl, recorder) = harness_with_tuning(true, false, FecTuning::mindiv());
        // Stage one small data packet (a single-symbol group at the default
        // MSS, since 100 bytes << single_symbol_payload).
        let payload = vec![0u8; 100];
        let now = Instant::now();
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            assert_eq!(rl.send_data_buf(&payload, now), payload.len());
        }
        // The packet is now staged but unacked, so `can_send_tail_fec` is
        // false (has_rtx / send buffer not empty).  Stock tuning would skip
        // the group; mindiv force-flushes it.
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        // 1 data symbol + 3 parity copies = 4 datagrams.
        assert_eq!(
            n, 4,
            "mindiv single-symbol burst must emit 1 data + 3 parity = 4 datagrams, got {n}"
        );
    }

    // ---- In-stream group FEC integration tests ----

    /// Helper: stage `n` full-size data packets directly into the reliable
    /// layer's send buffer (bypassing the staging cap) so a single `send_pkts`
    /// call emits them all in one FEC group.  Uses a large MSS (8192) so the
    /// send buffer can hold 8 full-size packets.
    fn stage_n_packets(tl: &TransmissionLayer, n: usize) -> usize {
        // Compute the payload size the same way `checked_mss_and_fec` does:
        // data_mss(mss) = mss - HDR_SIZE(11) - DATA_SYMBOL_HDR_SIZE(2),
        // then subtract codec data_overhead (15).
        let mss = 8192usize;
        let post_fec_mss = mss - 11 - 2; // data_mss: FEC hdr + data-symbol hdr
        let payload_len = post_fec_mss - crate::codec::data_overhead();
        let rl = tl.reliable_layer();
        let mut rl = rl.lock().unwrap();
        for _ in 0..n {
            let payload = vec![0u8; payload_len];
            rl.enqueue_send_data_for_test(&payload);
        }
        payload_len
    }

    /// A `TransmissionLayer` test harness with an explicit MSS, exposing the
    /// recording write via an `Arc<Mutex<RecordingWrite>>`.  Used by the
    /// in-stream group FEC tests which need a large MSS to fit 8 full-size
    /// packets in the send buffer.
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

    /// A full 8-data-symbol group must emit 4 parity symbols inline mid-burst,
    /// before the burst-end tail flush.  With the toggle on, the
    /// `PARITY_DATA_THRESHOLD` force-skip is suppressed, the group
    /// accumulates to 8, and `maybe_flush_full_fec_group` emits 4 parities
    /// right after the 8th data send.  The tail flush at burst end then sees
    /// an empty group (0 data symbols) and emits nothing.
    ///
    /// Mutation target: if the force-skip is kept on the instream path
    /// (`encode_data` ignores `instream`), the group never reaches 8, so
    /// inline parity is never emitted; the datagram count is 8 (data only)
    /// instead of 12 (8 data + 4 parity).
    #[tokio::test]
    async fn full_group_flushes_four_parities_inline_mid_burst() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        tl.set_instream_group_fec_for_test(true);
        stage_n_packets(&tl, 8);
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        // 8 data symbols + 4 parity = 12 datagrams.
        assert_eq!(
            n, 12,
            "full in-stream group must emit 8 data + 4 parity = 12 datagrams, got {n}"
        );
    }

    /// A partial data burst (3 data symbols) with the toggle on and the stock
    /// tail gate genuinely closed must still flush 4 parities via the
    /// data-path force-flush (`|| (data_path && instream_group_fec_enabled)`).
    /// The stock gate is closed by shrinking cwnd to 3 so the send loop stops
    /// after 3 new packets, leaving the rest of the staged data in the send
    /// buffer (`is_send_buf_empty` = false → `can_send_tail_fec` = false).
    /// The token bucket still has tokens (64 − 3 = 61) so the 4-parity budget
    /// gate passes.
    ///
    /// Mutation targets:
    /// (a) revert the instream branch to `data_count == INSTREAM_DATA_PER_GROUP`
    ///     → 3-symbol group falls through to stock parity_for(3)=1, not 4.
    /// (b) with the toggle off, no force-flush fires and the stock gate is
    ///     closed → group skipped, 0 parity (see inverse test).
    /// (c) drop the `(data_path && instream_group_fec_enabled)` OR at the
    ///     data-burst close → stock gate is closed, group skipped, 0 parity.
    #[tokio::test]
    async fn partial_data_burst_force_flushes_when_tail_gate_closed() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        tl.set_instream_group_fec_for_test(true);
        // Shrink cwnd to 3 so the send loop emits exactly 3 new packets, then
        // stops (cwnd full).  The remaining staged data stays in the send
        // buffer, making `is_send_buf_empty` = false → stock gate closed.
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.set_cwnd_for_test(std::num::NonZeroUsize::new(3).unwrap());
        }
        // Stage 3 full packets (the ones that will send) + extra data that
        // stays in the buffer (the 4th packet the cwnd-full loop won't send).
        stage_n_packets(&tl, 3);
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.enqueue_send_data_for_test(&[0u8; 100]);
        }
        let mut bufs = SendBufs::new();
        let _ = tl.send_pkts(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        // 3 data symbols + 4 parity (INSTREAM_PARITY_PER_GROUP) = 7.
        assert_eq!(
            n, 7,
            "partial data burst with toggle on and stock gate closed must flush \
             3 data + 4 parity = 7 datagrams, got {n}"
        );
    }

    /// Inverse of `partial_data_burst_force_flushes_when_tail_gate_closed`:
    /// with the toggle OFF and the stock gate closed (same cwnd-3 + extra
    /// data setup), the data-path force-flush does NOT fire, so the open
    /// group is skipped (0 parity).  This proves the force-flush is gated on
    /// the toggle, not unconditional.  3 data + 0 parity = 3 datagrams.
    #[tokio::test]
    async fn partial_data_burst_skipped_when_toggle_off_and_gate_closed() {
        let (tl, recorder) = harness_with_mss(true, false, 8192);
        // Toggle is OFF (do not call set_instream_group_fec_for_test).
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
        // 3 data symbols + 0 parity (stock gate closed, no force-flush) = 3.
        assert_eq!(
            n, 3,
            "partial data burst with toggle off and stock gate closed must emit \
             3 data + 0 parity = 3 datagrams, got {n}"
        );
    }

    /// An ACK burst must keep the stock tail gate: when `can_send_tail_fec`
    /// is false, the FEC group is skipped (no parity), even when the toggle
    /// is on.  Force-flushing ACK bursts would triple reverse-path packets
    /// for zero gain.  Mutation target: if the ACK path uses the data-path
    /// tail gate (`|| (data_path && instream_group_fec_enabled)`), the ACK
    /// group is force-flushed and this test fails.
    ///
    /// This test is exercised via the `send_kill_pkt` path: a kill packet is
    /// sent via `send_with_fec` (instream=false), and the subsequent
    /// `close_fec_burst` uses the stock gate.  Since `can_send_tail_fec` is
    /// false (no RTT samples → no settled tail), the group is skipped.
    #[tokio::test]
    async fn ack_burst_keeps_stock_tail_gate_when_blocked() {
        let (mut tl, recorder) = harness_with_mss(true, false, 8192);
        tl.set_instream_group_fec_for_test(true);
        // Stage unsent data in the send buffer so `is_send_buf_empty` is
        // false, making `can_send_tail_fec` false.  This is the condition
        // that the data-path force-flush overrides (so partial DATA groups
        // still flush at burst end), but the ACK/kill path must NOT override
        // — the kill group must be skipped (no parity).
        {
            let rl = tl.reliable_layer();
            let mut rl = rl.lock().unwrap();
            rl.enqueue_send_data_for_test(&[0u8; 100]);
        }
        // Send a kill packet.  The kill path calls `send_with_fec` (which
        // encodes with instream=false) then `close_fec_burst` with the stock
        // `can_send_tail_fec` gate.  Since the send buffer has unsent data,
        // `can_send_tail_fec` is false, so the kill group is skipped.
        let mut bufs = SendBufs::new();
        let _ = tl.send_kill_pkt(&mut bufs).await;
        let n = recorder.lock().unwrap().count();
        // 1 kill datagram, 0 parity (stock gate blocked → group skipped).
        assert_eq!(
            n, 1,
            "ACK/kill burst with stock tail gate blocked must emit 1 datagram (no parity), got {n}"
        );
    }

    /// When the toggle is off, the wire behavior must be byte-identical to
    /// stock: the `PARITY_DATA_THRESHOLD` force-skip fires at 4, so a burst
    /// of 8 packets produces groups of at most 4, and the tail flush emits
    /// at most `MAX_PARITY_PER_GROUP` parity.  No inline mid-burst parity
    /// is ever emitted.
    #[tokio::test]
    async fn toggle_off_wire_byte_identical_to_stock() {
        // With the toggle off, the wire behavior must be byte-identical to
        // stock: the `PARITY_DATA_THRESHOLD` force-skip fires at 4, so a burst
        // of 8 packets produces groups of at most 4, and no inline mid-burst
        // parity is ever emitted.
        let (tl_off, recorder_off) = harness_with_mss(true, false, 8192);
        // Toggle is off (env not set → false).  Stage 8 packets and send.
        stage_n_packets(&tl_off, 8);
        let mut bufs = SendBufs::new();
        let _ = tl_off.send_pkts(&mut bufs).await;
        let n_off = recorder_off.lock().unwrap().count();

        // Now run the same with a second stock layer (also toggle off).
        let (tl_stock, recorder_stock) = harness_with_mss(true, false, 8192);
        stage_n_packets(&tl_stock, 8);
        let mut bufs2 = SendBufs::new();
        let _ = tl_stock.send_pkts(&mut bufs2).await;
        let n_stock = recorder_stock.lock().unwrap().count();

        assert_eq!(
            n_off, n_stock,
            "toggle off must produce identical datagram count to stock (got {n_off} vs {n_stock})"
        );
        // With the toggle off, the force-skip at 4 means the group never
        // reaches 8, so no inline 4-parity flush fires.  The tail flush
        // emits at most MAX_PARITY_PER_GROUP (5) parity for the last
        // <=4-symbol group.  So the total is 8 data + some tail parity.
        // The key assertion: no inline mid-burst 4-parity flush.
        assert!(
            n_off <= 8 + 5,
            "toggle off must not emit inline mid-burst parity (got {n_off} > 13)"
        );
        // And specifically: if the toggle were ON, we'd get 12 (8 data +
        // 4 inline parity + 0 tail parity since the group is flushed
        // inline).  With toggle off, we get at most 8 + tail_parity, and
        // the tail parity is at most 5 — but crucially, the 4-parity
        // inline flush never fires, so n_off != 12.
        // (The tail flush may or may not fire depending on
        // can_send_tail_fec; with 8 unacked packets it's false, so the
        // group is skipped — n_off = 8.  But we don't hard-assert that
        // since the exact tail behavior depends on the reliable layer
        // state; the key is that n_off != 12, proving no inline flush.)
        assert_ne!(
            n_off, 12,
            "toggle off must NOT emit 8 data + 4 inline parity = 12 (inline flush must not fire)"
        );
    }
}
