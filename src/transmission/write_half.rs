use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::ack_feedback::{AckClaim, AckFeedback, AckFlushOutcome, AckPage, AckSchedule};
use super::connection::Connection;
use super::termination::{KillPolicy, TerminationWriter};
use super::transmission_layer::{
    FEC_DEBUG, PRINT_DEBUG_MSGS, ProactiveTerminationContext, SendBufs, UnreliableWrite,
};
use crate::ack::{AckHistory, EncodeAck};
use crate::codec::{EncodeData, EncodeError, encode_ack_data, encode_kill};
use crate::io_err::IoErr;
use crate::metrics::{MetricsEvent, MetricsTerminationCause};
use crate::mss::TRUNCATION_DETECTION_BYTES;
use crate::obfuscate::padding::AckPaddingMode;
use crate::obfuscate::sampler::DataSizeSampler;
use crate::reliable::reliable_layer::DataPkt;
use crate::traffic_shaping::control::handshake::padding::pad_handshake;
use crate::traffic_shaping::core::{SendPacer, SendWake};
use crate::traffic_shaping::redundancy::{
    ArmorDecision, RetransmissionArmor, RetransmissionArmorConfig,
    fec::FecEncoderState,
    fec_gate::{FecConditionGate, FecGateDecision},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SendLoopResult {
    pub(crate) made_progress: bool,
    pub(crate) wake: SendWake,
}

/// The write-actor half: owns the unreliable write transport, the FEC encoder
/// state (sole mutator), retransmission armor, the send pacer, the ACK
/// feedback owner (shared with the read half for recording), and the
/// termination writer.  All remaining shared state lives behind the
/// [`Connection`] facades, so no mutex guard survives an await.
#[derive(Debug)]
pub struct WriteHalf {
    utp_write: Box<dyn UnreliableWrite>,
    fec: Option<FecEncoderState>,
    fec_instream_flush: bool,
    instream_group_fec_enabled: bool,
    fec_gate: FecConditionGate,
    retransmission_armor: RetransmissionArmor,
    send_pacer: SendPacer,
    ack_feedback: Arc<AckFeedback>,
    shared: Arc<Connection>,
    termination_writer: TerminationWriter,
    /// Connection MSS, used to derive the handshake padding bound (see
    /// [`crate::traffic_shaping::control::handshake::padding`]).
    mss: crate::mss::Mss,
    /// ACK-padding mode, resolved from the connect/accept config's
    /// [`crate::udp::HarmfulPaddingPolicy`]: `Fitted` zero-fills ACK flush
    /// pages to a target drawn from the observed data-packet sizes so they
    /// are indistinguishable from data by wire size; `Jitter` appends a
    /// uniform `[0, ACK_INTERVAL_WIRE_SIZE)` pad so natural-size ACKs are
    /// not readable by size-slot analysis; `None` adds nothing. The
    /// receiver's codec strips the all-zero tail after the ACK command.
    ack_padding: AckPaddingMode,
    /// Sampler over recent sent data-packet sizes feeding fitted ACK
    /// padding (only read in `Fitted` mode).
    data_size_sampler: DataSizeSampler,
}

/// FEC and retransmission-armor settings for the write half, bundled so
/// the constructor stays data-first with a single settings argument.
#[derive(Debug, Clone, Copy)]
pub(super) struct WriteHalfSettings {
    pub(super) fec_instream_flush: bool,
    pub(super) instream_group_fec_enabled: bool,
    pub(super) retransmission_armor: RetransmissionArmorConfig,
    /// Connection MSS, used to derive the handshake padding bound (see
    /// [`crate::traffic_shaping::control::handshake::padding`]).
    pub(super) mss: crate::mss::Mss,
    /// ACK-padding mode (see [`crate::transmission::transmission_layer::UnreliableLayer`]).
    pub(super) ack_padding: AckPaddingMode,
}

/// A due ACK claimed for piggybacking on a data packet: page 0 rides on the
/// data datagram (encoded ahead of the data), page 1 (if any) is sent
/// standalone after the data packet.
struct PiggybackAck {
    claim: AckClaim,
    page1: Option<AckPage>,
}

/// A blocked underlay must not become a send-pass spin: after this many
/// consecutive WouldBlocked data sends the pass breaks so the write driver
/// parks on the next wake (resume signal / pacing / protocol deadline)
/// instead of re-minting packets that never traverse the wire.
const MAX_CONSECUTIVE_WOULD_BLOCK: u32 = 16;

/// Convert a codec [`EncodeError`] into the session [`IoErr`] surfaced by the
/// terminal-error paths. An encode failure is a wire-format invariant
/// violation (an oversized payload or an undersized envelope) and reads
/// `InvalidData`.
fn encode_to_io(error: EncodeError) -> IoErr {
    IoErr::from(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// Per-pass cache of FEC-encoded wire bytes, keyed by sequence.
///
/// A WouldBlocked data send retries the same seq within the pass; the retry
/// must restore the already-minted symbol instead of re-encoding a second
/// group slot (the phantom symbol that never traversed the wire, which also
/// breaks the single-symbol interactive classification).  The cache is
/// populated only when a send actually blocks, so a pass with no
/// backpressure neither allocates nor copies.
#[derive(Default)]
struct FecSymbolCache {
    by_seq: HashMap<u64, Vec<u8>>,
}

impl FecSymbolCache {
    /// Return the wire length for `key`, restoring cached bytes into
    /// `wire_pkt` when the seq was cached, or minting the symbol in place via
    /// `encode` on the first attempt.  A miss does not populate the cache.
    fn restore_or_encode(
        &self,
        key: u64,
        wire_pkt: &mut [u8],
        encode: impl FnOnce(&mut [u8]) -> usize,
    ) -> usize {
        match self.by_seq.get(&key) {
            Some(cached) => {
                wire_pkt[..cached.len()].copy_from_slice(cached);
                cached.len()
            }
            None => encode(wire_pkt),
        }
    }

    /// Remember the wire bytes for `key` after a WouldBlock so the within-pass
    /// retry restores them.
    fn remember(&mut self, key: u64, wire: &[u8]) {
        self.by_seq.entry(key).or_insert_with(|| wire.to_vec());
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_seq.len()
    }
}

impl WriteHalf {
    /// Encode one ACK page into `output`, optionally with piggybacked data
    /// appended after the ACK command (one ACK_CMD per datagram). Consumes
    /// the claim's pending echo on the first page. Returns the encoded
    /// length, or the codec error when the page cannot fit its envelope (a
    /// sizing-invariant violation — the caller must surface it, never
    /// panic). Shared by the standalone flush and the piggyback paths so the
    /// page-encode shape (history → EncodeAck → echo → wire bytes) lives in
    /// exactly one place.
    fn encode_ack_page(
        &self,
        claim: &mut AckClaim,
        history: &AckHistory,
        page: AckPage,
        data: Option<EncodeData<'_>>,
        output: &mut [u8],
    ) -> Result<usize, EncodeError> {
        let ack = EncodeAck {
            queue: history,
            first_block_index: page.first_block_index,
            max_blocks: page.max_blocks,
        };
        let echo = claim.take_echo();
        encode_ack_data(self.shared.session_tag(), Some(ack), echo, data, output)
    }

    /// Claim a due ACK for piggybacking, size the data payload to leave room
    /// for it, and encode the ACK + data in one pass under the reliable-layer
    /// lock (so the ACK size and the encode use the same history). Returns
    /// the data packet and the piggyback: `None` when no ACK is due or
    /// claimable, when the data packet is a retransmission too large to fit
    /// alongside the ACK, or when no packet was produced — the claim is
    /// released in those cases and the datagram is data-only, exactly as
    /// before. The combined datagram never exceeds the FEC symbol / MSS.
    fn claim_piggyback_ack(
        &self,
        now: Instant,
        wire_ts: u32,
        payload: &mut [u8],
        codec_pkt: &mut [u8],
    ) -> (Option<DataPkt>, Option<(usize, PiggybackAck)>) {
        self.shared.with_reliable_layer_mut(|reliable_layer| {
            let history = reliable_layer.pkt_recv_space().ack_history();
            let claim = if self.ack_feedback.schedule(now).is_due() {
                self.ack_feedback.claim(now, history.len())
            } else {
                None
            };
            // Measure the encoded page-0 size (no data yet) so the data
            // payload can be sized to leave room for it. The echo is peeked,
            // not consumed — the final encode below consumes it. A failed
            // measure must never panic or fabricate a size: release the
            // claim and continue with a data-only datagram (the standalone
            // flush at the end of the pass retries the ACK work).
            let mut reserved = 0;
            let mut measure_failed = false;
            if let Some(c) = claim.as_ref() {
                let page0 = c.pages()[0].expect("a claim always carries page 0");
                let ack = EncodeAck {
                    queue: history,
                    first_block_index: page0.first_block_index,
                    max_blocks: page0.max_blocks,
                };
                match encode_ack_data(
                    self.shared.session_tag(),
                    Some(ack),
                    c.peek_echo(),
                    None,
                    codec_pkt,
                ) {
                    Ok(encoded) => reserved = encoded,
                    Err(_) => measure_failed = true,
                }
            }
            let res = reliable_layer.send_data_pkt_bounded(payload, now, reserved);
            let Some(p) = res.as_ref() else {
                // No packet to send: release the claim (if any) so the
                // standalone flush can pick up the ACK work.
                if let Some(claim) = claim {
                    self.ack_feedback.complete(
                        claim,
                        AckFlushOutcome::WouldBlock {
                            pages_sent: 0,
                            rearm: false,
                        },
                    );
                }
                return (res, None);
            };
            let data_written = match &p.data_written {
                crate::reliable::reliable_layer::DataPktPayload::Data(data_written) => {
                    data_written.get()
                }
                crate::reliable::reliable_layer::DataPktPayload::Fin => 0,
            };
            let Some(claim) = claim else {
                return (res, None);
            };
            if measure_failed {
                // The ACK page could not be encoded into the codec envelope
                // (unreachable with an MSS-sized buffer): release the claim so
                // the flush path keeps the ACK work and send the datagram
                // data-only — no piggyback, no fabricated size.
                self.ack_feedback.complete(
                    claim,
                    AckFlushOutcome::WouldBlock {
                        pages_sent: 0,
                        rearm: false,
                    },
                );
                return (res, None);
            }
            // The combined datagram must stay within the MSS: the data
            // packet's own wire overhead is `data_overhead()` for a stock
            // packet but `frame_data_overhead()` (4 bytes more) for a
            // first-frame packet, so the release check must use the packet's
            // actual overhead — the stock bound would let a retransmitted
            // first-frame packet exceed the MSS. The check also deducts the
            // truncated-datagram detection headroom (matching
            // `Mss::max_data_size_per_pkt()`), so the combined datagram can
            // never be exactly the MSS — a full-size datagram would be
            // indistinguishable from a truncated larger one at the receiver
            // (and would exceed the FEC symbol ceiling).
            let overhead = if p.frame_len.is_some() {
                crate::delivery::frame::wire::frame_data_overhead()
            } else {
                crate::codec::data_overhead()
            };
            if reserved + data_written
                > self
                    .mss
                    .get()
                    .saturating_sub(TRUNCATION_DETECTION_BYTES)
                    .saturating_sub(overhead)
            {
                // The data packet (a retransmission) is too large to fit
                // alongside the ACK: release the claim and send the data
                // alone.
                self.ack_feedback.complete(
                    claim,
                    AckFlushOutcome::WouldBlock {
                        pages_sent: 0,
                        rearm: false,
                    },
                );
                return (res, None);
            }
            // Re-read the history for the encode: the reliable-layer lock is
            // held for the whole closure, so the history is unchanged since
            // the claim and the size were computed.
            let history = reliable_layer.pkt_recv_space().ack_history();
            // Encode the ACK + data: page 0 rides ahead of the data (one
            // ACK_CMD per datagram, so at most one page piggybacks); page 1,
            // if any, is sent standalone after the data packet.
            let data = EncodeData {
                seq: p.seq,
                send_ts: Some(wire_ts),
                frame_len: p.frame_len,
                data: &payload[..data_written],
            };
            let mut claim = claim;
            let page0 = claim.pages()[0].expect("a claim always carries page 0");
            let n = match self.encode_ack_page(&mut claim, history, page0, Some(data), codec_pkt) {
                Ok(n) => n,
                // Unreachable with the sizing check above, but never panic:
                // release the claim and send the datagram data-only.
                Err(_) => {
                    self.ack_feedback.complete(
                        claim,
                        AckFlushOutcome::WouldBlock {
                            pages_sent: 0,
                            rearm: false,
                        },
                    );
                    return (res, None);
                }
            };
            let page1 = claim.pages()[1];
            (res, Some((n, PiggybackAck { claim, page1 })))
        })
    }

    /// Apply the resolved ack-padding mode to an encoded ACK page in place:
    /// `Fitted` zero-fills to a target drawn from the observed data-packet
    /// sizes, `Jitter` appends a uniform `[0, ACK_INTERVAL_WIRE_SIZE)` pad,
    /// `None` adds nothing. Returns the padded page length.
    fn pad_ack_page(&mut self, page_buf: &mut [u8], written_bytes: usize) -> usize {
        match self.ack_padding {
            AckPaddingMode::Fitted => {
                match self
                    .data_size_sampler
                    .draw_target(written_bytes, Instant::now())
                {
                    Some(target) => {
                        let target = target.min(page_buf.len());
                        page_buf[written_bytes..target].fill(0);
                        target
                    }
                    // No fit yet, or the data envelope too small: the page
                    // goes out unpadded exactly as before.
                    None => written_bytes,
                }
            }
            AckPaddingMode::Jitter => {
                let pad = rand::random_range(0..crate::codec::ACK_INTERVAL_WIRE_SIZE);
                page_buf[written_bytes..written_bytes + pad].fill(0);
                written_bytes + pad
            }
            AckPaddingMode::None => written_bytes,
        }
    }

    /// Finish a piggybacked ACK claim: send page 1 (if any) standalone with
    /// the ack-padding mode, then complete the claim. Page 0 was already
    /// delivered on the data datagram.
    async fn finish_piggyback_claim(
        &mut self,
        piggyback: PiggybackAck,
        bufs: &mut SendBufs,
    ) -> Result<(), IoErr> {
        let PiggybackAck { mut claim, page1 } = piggyback;
        let Some(page1) = page1 else {
            self.ack_feedback
                .complete(claim, AckFlushOutcome::Sent { pages_sent: 1 });
            self.shared.notify_session_outbound_progress();
            return Ok(());
        };
        // Re-encode page 1 with the current history (the claim's page params
        // are fixed; the queue may have grown, which is harmless for a
        // selective ack).  An encode failure abandons the claim (page 0
        // already rode the data datagram) and surfaces as session-fatal.
        let (page1_len, page1_encode_error) =
            match self.shared.with_reliable_layer(|reliable_layer| {
                let history = reliable_layer.pkt_recv_space().ack_history();
                let (payload, _codec_pkt, _wire_pkt) = bufs.parts_mut();
                self.encode_ack_page(&mut claim, history, page1, None, payload)
            }) {
                Ok(page1_len) => (page1_len, None),
                Err(error) => (0, Some(error)),
            };
        if let Some(error) = page1_encode_error {
            // The page-1 encode failed (a sizing-invariant violation): page 0
            // already rode the data datagram, so abandon the claim and
            // surface the error as session-fatal — the ACK work is stuck.
            let error = encode_to_io(error);
            self.ack_feedback
                .complete(claim, AckFlushOutcome::Fatal { pages_sent: 1 });
            self.shared
                .press_error(error, MetricsTerminationCause::AckWrite);
            return Err(error);
        }
        let (payload, _codec_pkt, wire_pkt) = bufs.parts_mut();
        let page_len = self.pad_ack_page(payload, page1_len);
        match self.send_with_fec(&payload[..page_len], wire_pkt).await {
            Ok(_) => {
                self.ack_feedback
                    .complete(claim, AckFlushOutcome::Sent { pages_sent: 2 });
                self.shared.notify_session_outbound_progress();
                Ok(())
            }
            Err(error) if error == std::io::ErrorKind::WouldBlock => {
                // Page 0 (piggybacked) was delivered; page 1's work remains
                // for a later flush.
                self.ack_feedback.complete(
                    claim,
                    AckFlushOutcome::WouldBlock {
                        pages_sent: 1,
                        rearm: false,
                    },
                );
                self.shared.notify_session_outbound_progress();
                Ok(())
            }
            Err(error) => {
                self.ack_feedback
                    .complete(claim, AckFlushOutcome::Fatal { pages_sent: 1 });
                self.shared
                    .press_error(error, MetricsTerminationCause::AckWrite);
                Err(error)
            }
        }
    }

    pub(super) fn new(
        utp_write: Box<dyn UnreliableWrite>,
        fec: Option<FecEncoderState>,
        send_pacer: SendPacer,
        ack_feedback: Arc<AckFeedback>,
        shared: Arc<Connection>,
        termination_writer: TerminationWriter,
        settings: WriteHalfSettings,
    ) -> Self {
        let WriteHalfSettings {
            fec_instream_flush,
            instream_group_fec_enabled,
            retransmission_armor,
            mss,
            ack_padding,
        } = settings;
        Self {
            utp_write,
            fec,
            fec_instream_flush,
            instream_group_fec_enabled,
            fec_gate: FecConditionGate::default(),
            retransmission_armor: RetransmissionArmor::new(retransmission_armor),
            send_pacer,
            ack_feedback,
            shared,
            termination_writer,
            mss,
            ack_padding,
            data_size_sampler: DataSizeSampler::new(),
        }
    }

    pub(crate) fn kill_requested(&self) -> &tokio_util::sync::CancellationToken {
        self.termination_writer.kill_requested()
    }

    /// In-stream group FEC is only live while the condition gate's loss
    /// evidence is active: startup without measured congestion loss or enough
    /// primary recovery samples emits no parity, so the in-stream group path
    /// must not accumulate full groups while the gate is closed.
    fn instream_group_fec_enabled(&self) -> bool {
        self.instream_group_fec_enabled && self.fec_gate.loss_active()
    }

    /// Refresh the condition gate's loss evidence from the reliable layer's
    /// latest measured congestion-loss ratio before making any gate decision.
    ///
    /// With FEC disabled the gate is never consulted, so skip the reliable-
    /// layer lock entirely and leave `loss_active` clear.
    fn refresh_fec_loss_mode(&mut self) {
        if self.fec.is_none() {
            return;
        }
        let loss = self
            .shared
            .with_reliable_layer(|layer| layer.congestion_loss_ratio());
        self.fec_gate.refresh_loss(true, loss);
    }

    /// Evaluate the condition gate for a flush decision at `now`: spare
    /// capacity comes from the reliable layer (tail gate + zero write waiters
    /// + no queue building); the tail policy is the caller's request.
    fn fec_gate_decision(&self, now: Instant, tail_requested: bool) -> FecGateDecision {
        let spare = self
            .shared
            .with_reliable_layer(|layer| layer.fec_has_spare_capacity(now));
        self.fec_gate.decide(spare, tail_requested)
    }

    pub(crate) fn resume_send(&self) -> &tokio::sync::Notify {
        self.shared.resume_send()
    }

    pub(crate) fn ack_schedule_changed(&self) -> &tokio::sync::Notify {
        self.ack_feedback.schedule_changed()
    }

    pub(crate) fn log(&self, event: MetricsEvent) {
        self.shared.log(event);
    }

    #[cfg(test)]
    pub(crate) fn drain_pacer_for_test(&self, n: usize, now: Instant) -> usize {
        self.send_pacer.take_at_most_tokens(n, now)
    }

    async fn try_send_requested_kill(&mut self, bufs: &mut SendBufs) -> Option<Result<(), IoErr>> {
        let attempt = self.termination_writer.take_kill_attempt()?;
        let result = self.send_kill_pkt(bufs).await;
        drop(attempt);
        Some(result)
    }

    async fn return_error_after_requested_kill(
        &mut self,
        bufs: &mut SendBufs,
    ) -> Result<(), IoErr> {
        let error = self
            .shared
            .check_error()
            .expect_err("error-present flag must publish the first terminal error");
        let _ = self.try_send_requested_kill(bufs).await;
        Err(error)
    }

    /// Proactively terminate the session when the peer-liveness watchdog
    /// considers it stalled, evaluated at the caller-supplied decision time
    /// (the send pass's fixed `now`, never a fresh clock sampled after the
    /// stall was observed).  Emits the termination snapshot event exactly once.
    pub(crate) fn proactively_terminate_stalled_session_at(&self, now: Instant) {
        let context = self.shared.with_reliable_layer(|reliable_layer| {
            let send_space = reliable_layer.pkt_send_space();
            let reason = send_space.stall_reason(now)?;
            Some(ProactiveTerminationContext {
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
            })
        });
        let Some(context) = context else {
            return;
        };
        if self.shared.has_error() {
            return;
        }
        self.shared.press_broken_pipe(
            KillPolicy::SendKill,
            Some(context),
            MetricsTerminationCause::ProactiveStall,
        );
    }

    #[cfg(test)]
    pub async fn send_pkts(&mut self, bufs: &mut SendBufs) -> Result<bool, IoErr> {
        Ok(self.send_pkts_inner(bufs, Instant::now()).await?.0)
    }

    pub(crate) async fn send_pass(&mut self, bufs: &mut SendBufs) -> Result<SendLoopResult, IoErr> {
        let now = Instant::now();
        let (made_progress, completed_at, wouldblock_retry_at) =
            self.send_pkts_inner(bufs, now).await?;
        // A WouldBlock-bounded pass must retry promptly: wake the driver at
        // the short retry deadline instead of parking until the next peer
        // event (which on a backpressured link could stall the send path
        // for up to an RTT and inflate tail latency).
        let wake = match wouldblock_retry_at {
            Some(retry_at) => SendWake::Pacing(retry_at),
            None => self.shared.next_send_wake(completed_at),
        };
        Ok(SendLoopResult {
            made_progress,
            wake,
        })
    }

    pub(crate) fn ack_schedule(&self, now: Instant) -> AckSchedule {
        self.ack_feedback.schedule(now)
    }

    pub(crate) fn debug_conn_id(&self) -> usize {
        Arc::as_ptr(&self.shared) as usize
    }

    /// How long the write driver waits after a bounded WouldBlock streak
    /// before retrying the send pass: long enough to yield the CPU (no busy
    /// spin on a blocked underlay) yet far below an RTT (so the retry is
    /// prompt on a rate-limited link instead of parking until the next peer
    /// event arrives).
    const WOULDBLOCK_RETRY_DELAY: Duration = Duration::from_millis(1);

    async fn send_pkts_inner(
        &mut self,
        bufs: &mut SendBufs,
        mut now: Instant,
    ) -> Result<(bool, Instant, Option<Instant>), IoErr> {
        if self.try_send_requested_kill(bufs).await.is_some() {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.proactively_terminate_stalled_session_at(now);
        if self.shared.has_error() {
            self.return_error_after_requested_kill(bufs).await?;
        }
        self.send_due_post_open_response(now).await?;
        self.refresh_fec_loss_mode();
        // `now` is already fixed for one send pass: compute the wire timestamp
        // once and reuse it for every packet encoded by this pass.  A blocked
        // underlay send refreshes the clock for deadline/token math, but the
        // wire timestamp stays stable for the whole pass.
        let wire_ts = self.shared.wire_ts(now);
        let mut written_bytes = 0;
        let mut written_fin = false;
        let mut piggyback_attempted = false;
        // FEC symbols are minted into the open group BEFORE the underlay
        // send, so a WouldBlocked send must not retry via a fresh encode:
        // the retry re-encodes the same seq into a second group slot (a
        // phantom symbol that never traversed the wire, which also breaks
        // the single-symbol interactive classification).  Cache the encoded
        // wire bytes keyed by seq for the duration of the pass so a retry
        // reuses the one symbol the group already holds.  The cache is a
        // local, so it dies when the pass ends (no stale group_id can leak
        // into a later pass).
        let mut fec_symbol_cache = FecSymbolCache::default();
        let mut wouldblock_retry_at: Option<Instant> = None;
        // A blocked underlay must not become a spin: after this many
        // consecutive WouldBlocked sends the pass breaks so the write driver
        // parks on the next wake (resume signal / pacing / protocol deadline)
        // instead of re-minting packets that never traverse the wire.
        let mut consecutive_wouldblock = 0u32;
        loop {
            // Observe the stop/cancel token between packets as well as the
            // error flag: a kill request cancels the token only after
            // pressing the error, but checking both keeps the loop responsive
            // even if the flag races the cancellation.
            if self.shared.has_error() || self.termination_writer.kill_requested().is_cancelled() {
                self.return_error_after_requested_kill(bufs).await?;
            }
            let (payload, codec_pkt, wire_pkt) = bufs.parts_mut();
            // Claim a due ACK for piggybacking BEFORE the payload is filled,
            // so the data can be sized to leave room for the ACK (the
            // combined datagram must stay within the FEC symbol / MSS). The
            // claim, the size, and the encode share one reliable-layer lock,
            // so the ACK size and the encode use the same history.
            let (res, piggyback) = if !piggyback_attempted {
                piggyback_attempted = true;
                self.claim_piggyback_ack(now, wire_ts, payload, codec_pkt)
            } else {
                let res = self.shared.with_reliable_layer_mut(|reliable_layer| {
                    reliable_layer.send_data_pkt(payload, now)
                });
                (res, None)
            };
            if crate::debug::debug_send() {
                eprintln!(
                    "[sdp-res] conn={:x} result={}",
                    Arc::as_ptr(&self.shared) as usize,
                    if res.is_some() { "pkt" } else { "none" }
                );
            }
            if self
                .shared
                .wants_snapshot(crate::metrics::MetricsEvent::SendDataPacketAttempt, now)
            {
                let snapshot = self.shared.capture_snapshot(now);
                self.shared.log_at_with_snapshot(
                    crate::metrics::MetricsEvent::SendDataPacketAttempt,
                    now,
                    snapshot,
                );
            }
            if let Some((_, piggyback)) = &piggyback {
                // A successful transactional claim names why it became due;
                // the observation is emitted before the datagram is sent so
                // the claim event is never confused with the resume wake
                // that rearmed us. `log_at` snapshots the reliable layer, so
                // it must run outside the `with_reliable_layer` lock above
                // (the same discipline `flush_acks` follows) — calling it
                // inside would deadlock on the non-reentrant mutex whenever
                // observability is enabled.
                self.shared
                    .log_at(MetricsEvent::AckFlush(piggyback.claim.reason()), now);
            }
            let Some(p) = res else {
                if FEC_DEBUG {
                    eprintln!("send_data_pkt: no pkt to send (rtx=None, cwnd full or no tokens)");
                }
                break;
            };
            let data_written = match p.data_written {
                // Progress accounting (bytes/FIN) is deferred to the
                // successful-send arm below: a packet minted but refused by
                // the underlay never traversed the wire, and the same
                // retransmission retried on the next iteration must not be
                // double-counted.
                crate::reliable::reliable_layer::DataPktPayload::Data(data_written) => {
                    data_written.get()
                }
                crate::reliable::reliable_layer::DataPktPayload::Fin => 0,
            };
            let is_fin = matches!(
                &p.data_written,
                crate::reliable::reliable_layer::DataPktPayload::Fin
            );
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
            // The loss-gated method (not the raw field): while the condition
            // gate is closed (no loss evidence), the in-stream path must not
            // accumulate full groups — `encode_data` force-skips groups past
            // PARITY_DATA_THRESHOLD when `instream` is false, matching the
            // flush paths' gated decisions.
            let instream = self.instream_group_fec_enabled();
            let armor_decision = self.retransmission_armor.decide(is_recovery, || {
                self.shared
                    .with_reliable_layer(|reliable_layer| reliable_layer.queue_building())
            });
            // The first data packet of the pass piggybacks a due ACK on the
            // data datagram (page 0 rides ahead of the data; page 1, if any,
            // is sent standalone after). This hides ACKs among data packets
            // at zero wire cost — no standalone ACK datagram — and the wire
            // format already carries ack+data in one datagram. The claim was
            // made (and the data sized) before the payload was filled; when
            // no ACK was due or claimable, the datagram is data-only.
            let (n, piggyback) = match piggyback {
                Some((n, piggyback)) => (n, Some(piggyback)),
                None => match encode_ack_data(None, None, None, Some(data), codec_pkt) {
                    Ok(n) => (n, None),
                    Err(error) => {
                        // The codec envelope cannot hold this data packet (a
                        // sizing-invariant violation): session-fatal.  Drop
                        // the open FEC group so an error return cannot leak
                        // a group across passes.
                        self.skip_open_fec_group();
                        return Err(encode_to_io(error));
                    }
                },
            };
            if self.ack_padding == AckPaddingMode::Fitted {
                // Sample the DATA path only: the encoded codec packet length
                // (before FEC/obfuscation envelopes, which padded ACKs add
                // identically) is the wire-size distribution ACKs must
                // blend into.
                self.data_size_sampler.observe(n);
            }
            let utp_pkt = &codec_pkt[..n];
            let send_buf: &[u8] = match self.fec.as_mut() {
                Some(fec) => {
                    let key = p.seq.to_wire();
                    // Mint the FEC symbol into the open group exactly once
                    // per seq per pass.  On the first attempt the symbol is
                    // encoded in place and NOT cached; only a WouldBlock (see
                    // the send-result arm below) caches the bytes, so the
                    // happy path neither allocates nor copies.  A retry
                    // restores the cached symbol instead of re-encoding a
                    // second group slot.
                    let len = fec_symbol_cache.restore_or_encode(key, wire_pkt, |buf| {
                        fec.encode_data(utp_pkt, buf, instream)
                    });
                    &wire_pkt[..len]
                }
                None => utp_pkt,
            };
            if crate::debug::debug_send() {
                eprintln!(
                    "[send] conn={:x} seq={} len={} recovery={} piggyback={}",
                    Arc::as_ptr(&self.shared) as usize,
                    p.seq.to_wire(),
                    n,
                    is_recovery,
                    piggyback.is_some()
                );
            }
            let primary_res = self.utp_write.send(send_buf).await;
            if crate::debug::debug_send() {
                eprintln!(
                    "[send-res] seq={} result={:?}",
                    p.seq.to_wire(),
                    primary_res.as_ref().map(|_| "ok").map_err(|e| e.kind())
                );
            }
            match primary_res {
                Ok(_) => {
                    // The send succeeded: only now does the packet's bytes
                    // (or FIN) count as progress, and the WouldBlock streak
                    // resets.
                    if is_fin {
                        written_fin = true;
                    } else {
                        written_bytes += data_written;
                    }
                    consecutive_wouldblock = 0;
                    self.fec_gate.record_data_send(is_recovery);
                    if self.fec.is_some()
                        && instream
                        && let Err(error) = self.maybe_flush_full_fec_group(now).await
                    {
                        // The data send (with the piggybacked ACK) already
                        // succeeded, but the FEC flush failed fatally:
                        // abandon the claim so the ACK state machine is
                        // not left with a stuck in-flight claim (a later
                        // claim would panic on the in-flight assert). The
                        // error is already pressed by the flush path; drop
                        // any open group so the error return cannot leak it
                        // into the next pass.
                        if let Some(piggyback) = piggyback {
                            self.ack_feedback.complete(
                                piggyback.claim,
                                AckFlushOutcome::Fatal { pages_sent: 1 },
                            );
                        }
                        self.skip_open_fec_group();
                        return Err(error);
                    }
                    if armor_decision == ArmorDecision::Duplicate {
                        let token_taken = self.send_pacer.take_exact_tokens(1, now);
                        if token_taken {
                            match self.utp_write.send(send_buf).await {
                                Ok(_) => self.shared.log_at(
                                    crate::metrics::MetricsEvent::RetransmissionArmorDuplicate,
                                    now,
                                ),
                                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                                    if FEC_DEBUG {
                                        eprintln!("send_pkts: dup WouldBlock (transient)");
                                    }
                                }
                                Err(e) => {
                                    // The primary data send (with the
                                    // piggybacked ACK) already succeeded, but
                                    // the armor duplicate failed fatally:
                                    // abandon the claim so the next flush is
                                    // not blocked by a stuck in-flight claim.
                                    if let Some(piggyback) = piggyback {
                                        self.ack_feedback.complete(
                                            piggyback.claim,
                                            AckFlushOutcome::Fatal { pages_sent: 1 },
                                        );
                                    }
                                    self.shared
                                        .press_error(e, MetricsTerminationCause::DataWrite);
                                    // Do not leak the open FEC group on the
                                    // error return.
                                    self.skip_open_fec_group();
                                    return Err(e);
                                }
                            }
                        }
                    }
                    // The piggybacked ACK rode on the data datagram; finish
                    // the claim (send page 1 standalone if any) now that the
                    // data send and its duplicate are done.  Any error path
                    // drops the open FEC group (the flush paths may have
                    // re-opened it for the standalone page).
                    if let Some(piggyback) = piggyback
                        && let Err(error) = self.finish_piggyback_claim(piggyback, bufs).await
                    {
                        self.skip_open_fec_group();
                        return Err(error);
                    }
                    continue;
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    // A piggybacked ACK was not delivered: restore its work
                    // for a later flush and retry the data packet without it.
                    if let Some(piggyback) = piggyback {
                        self.ack_feedback.complete(
                            piggyback.claim,
                            AckFlushOutcome::WouldBlock {
                                pages_sent: 0,
                                rearm: false,
                            },
                        );
                    }
                    // Cache this seq's already-minted symbol so the within-pass
                    // retry restores it instead of re-encoding a second group
                    // slot.  Only a WouldBlock populates the cache.
                    if self.fec.is_some() {
                        fec_symbol_cache.remember(p.seq.to_wire(), send_buf);
                    }
                    let blocked_at = Instant::now();
                    self.shared
                        .log_at(crate::metrics::MetricsEvent::DataSendWouldBlock, blocked_at);
                    if FEC_DEBUG {
                        eprintln!("send_pkts: WouldBlock on data send (transient)");
                    }
                    // A blocked underlay send may have consumed real time:
                    // refresh the clock so the next pacing/token decision is
                    // computed against the fresh instant.
                    now = blocked_at;
                    // Bound the spin: a persistently-blocked underlay must
                    // not keep re-minting packets that never traverse the
                    // wire.  Break so the driver parks on the next wake; the
                    // open FEC group is closed at the bottom of the pass.
                    // (The cache keeps the retried seq's symbol minted once,
                    // so the group never holds a phantom duplicate.)
                    consecutive_wouldblock += 1;
                    if consecutive_wouldblock >= MAX_CONSECUTIVE_WOULD_BLOCK {
                        // Park for a bounded, RTT-sub-epsilon delay before
                        // retrying: the driver sleeps until
                        // `wouldblock_retry_at` (a `Pacing` wake) instead of
                        // parking until the next peer event, which on a
                        // backpressured link could stall the send path for
                        // up to an RTT.
                        wouldblock_retry_at = Some(blocked_at + Self::WOULDBLOCK_RETRY_DELAY);
                        break;
                    }
                    continue;
                }
                Err(e) => {
                    // The data send failed fatally before the piggybacked ACK
                    // could be delivered: abandon the claim (restores the
                    // echo, clears in-flight) so a later flush is not blocked
                    // by a stuck claim.
                    if let Some(piggyback) = piggyback {
                        self.ack_feedback
                            .complete(piggyback.claim, AckFlushOutcome::Fatal { pages_sent: 0 });
                    }
                    self.shared
                        .press_error(e, MetricsTerminationCause::DataWrite);
                    // Do not leak the open FEC group on the error return.
                    self.skip_open_fec_group();
                    return Err(e);
                }
            }
        }
        if 0 < written_bytes || written_fin {
            if PRINT_DEBUG_MSGS {
                println!("send_pkts: {{ data: {written_bytes}; fin: {written_fin} }}");
            }
            self.shared.publish_data_sent();
        }
        if self.fec.is_some() {
            let baseline_can_send_tail_fec = self
                .shared
                .with_reliable_layer(|reliable_layer| reliable_layer.can_send_tail_fec(now));
            let tail_requested = self.fec_instream_flush
                || baseline_can_send_tail_fec
                || self.instream_group_fec_enabled();
            self.close_fec_burst(
                now,
                self.fec_gate_decision(now, tail_requested),
                self.instream_group_fec_enabled(),
            )
            .await?;
        }
        let made_progress = 0 < written_bytes || written_fin;
        let mut completed_at = Instant::now();
        if self.ack_feedback.schedule(completed_at).is_due() {
            self.flush_acks(bufs).await?;
            completed_at = Instant::now();
        }
        Ok((made_progress, completed_at, wouldblock_retry_at))
    }

    async fn maybe_flush_full_fec_group(&mut self, now: Instant) -> Result<(), IoErr> {
        let instream = self.instream_group_fec_enabled();
        let Some(fec) = self.fec.as_ref() else {
            return Ok(());
        };
        if !fec.group_data_full(instream) {
            return Ok(());
        }
        let gate = self.fec_gate_decision(now, true);
        self.close_fec_burst(now, gate, instream).await
    }

    pub(crate) async fn close_fec_burst(
        &mut self,
        now: Instant,
        gate: FecGateDecision,
        instream: bool,
    ) -> Result<(), IoErr> {
        let Some(fec) = self.fec.as_mut() else {
            return Ok(());
        };
        match gate {
            FecGateDecision::Flush => {}
            FecGateDecision::LossNotWarranted => {
                fec.skip_open_group_loss_gate();
                return Ok(());
            }
            FecGateDecision::NoSpareCapacity => {
                fec.skip_open_group_no_spare_capacity();
                return Ok(());
            }
            FecGateDecision::TailNotRequested => {
                fec.skip_open_group();
                return Ok(());
            }
        }
        self.flush_fec_parities(now, instream).await
    }

    fn skip_open_fec_group(&mut self) {
        let Some(fec) = self.fec.as_mut() else {
            return;
        };
        fec.skip_open_group();
    }

    pub(crate) async fn flush_fec_parities(
        &mut self,
        now: Instant,
        instream: bool,
    ) -> Result<(), IoErr> {
        let send_pacer = self.send_pacer.clone();
        let Some(fec) = self.fec.as_mut() else {
            return Ok(());
        };
        let parity_pkts =
            send_pacer.with_token_bucket(|bucket| fec.maybe_flush_parities(bucket, now, instream));
        if crate::debug::debug_send() {
            eprintln!("[fec] flush_parities: {} pkts", parity_pkts.len());
        }
        for pkt in parity_pkts {
            match self.utp_write.send(&pkt).await {
                Ok(_) => (),
                Err(error) if error == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    self.shared
                        .press_error(error, MetricsTerminationCause::FecParityWrite);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    async fn send_due_post_open_response(&mut self, now: Instant) -> Result<(), IoErr> {
        let Some(response) = self.shared.claim_post_open_response(now) else {
            return Ok(());
        };
        let mut padded = vec![0u8; self.mss.get()];
        let n = pad_handshake(&response.bytes, &mut padded, self.mss);
        match self.utp_write.send(&padded[..n]).await {
            Ok(len) if len == n => Ok(()),
            Ok(_) => {
                self.shared.retry_post_open_response(Instant::now());
                Ok(())
            }
            Err(error) if error == std::io::ErrorKind::WouldBlock => {
                self.shared.retry_post_open_response(Instant::now());
                Ok(())
            }
            Err(error) => {
                self.shared
                    .press_error(error, MetricsTerminationCause::HandshakeWrite);
                Err(error)
            }
        }
    }

    pub async fn send_kill_pkt(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.refresh_fec_loss_mode();
        let fec_enabled = self.send_kill_data_pkt(bufs).await?;
        if fec_enabled {
            self.flush_kill_fec_tail().await;
        }
        Ok(())
    }

    async fn send_kill_data_pkt(&mut self, bufs: &mut SendBufs) -> Result<bool, IoErr> {
        // The kill buffer `[1+1+8]` (KILL_CMD + optional TAG_CMD/tag prefix)
        // is exactly `encode_kill`'s maximum footprint (9 tag bytes + 1 kill
        // byte when tagged, 1 byte otherwise), so the encode cannot fail —
        // but never unwrap a Result blindly: name the sizing invariant.
        let mut buf = [0; 1 + 1 + 8];
        let n = encode_kill(self.shared.session_tag(), &mut buf)
            .expect("the [1+1+8] kill buffer exactly fits TAG+tag (9 bytes) + KILL_CMD (1 byte)");
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
        let tail_requested = self
            .shared
            .with_reliable_layer(|reliable_layer| reliable_layer.can_send_tail_fec(now));
        let gate = self.fec_gate_decision(now, tail_requested);
        let _ = self.close_fec_burst(now, gate, false).await;
    }

    #[cfg(test)]
    pub async fn send_kill_and_abort(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.shared.press_broken_pipe(
            KillPolicy::SendKill,
            None,
            MetricsTerminationCause::LocalAbort,
        );
        match self.try_send_requested_kill(bufs).await {
            Some(result) => result,
            None => self.shared.check_error(),
        }
    }

    #[cfg(test)]
    pub fn has_pending_acks(&self) -> bool {
        !self.ack_feedback.is_drained()
    }

    pub async fn flush_acks(&mut self, bufs: &mut SendBufs) -> Result<(), IoErr> {
        self.refresh_fec_loss_mode();
        let now = Instant::now();
        let (claim, encoded_page_lengths) = {
            let (claim, encoded_page_lengths, encode_error) =
                self.shared.with_reliable_layer(|reliable_layer| {
                    let history = reliable_layer.pkt_recv_space().ack_history();
                    let Some(mut claim) = self.ack_feedback.claim(now, history.len()) else {
                        return (None, [None; 2], None);
                    };
                    let (payload, codec_pkt, _) = bufs.parts_mut();
                    let mut encoded_page_lengths = [None; 2];
                    let mut encode_error = None;
                    for (index, page) in claim.pages().into_iter().enumerate() {
                        let Some(page) = page else {
                            continue;
                        };
                        let output = if index == 0 {
                            &mut codec_pkt[..]
                        } else {
                            &mut payload[..]
                        };
                        match self.encode_ack_page(&mut claim, history, page, None, output) {
                            Ok(len) => encoded_page_lengths[index] = Some(len),
                            Err(error) => {
                                encode_error = Some(error);
                                break;
                            }
                        }
                    }
                    (Some(claim), encoded_page_lengths, encode_error)
                });
            let Some(claim) = claim else {
                return Ok(());
            };
            if let Some(error) = encode_error {
                // The ACK page cannot be encoded (a sizing-invariant
                // violation): abandon the claim so the ACK state machine is
                // not left with a stuck in-flight claim, and surface the
                // error as session-fatal.
                let error = encode_to_io(error);
                self.ack_feedback
                    .complete(claim, AckFlushOutcome::Fatal { pages_sent: 0 });
                self.shared
                    .press_error(error, MetricsTerminationCause::AckWrite);
                return Err(error);
            }
            // A successful transactional claim names why it became due; the
            // observation is emitted before any page is sent so the claim
            // event is never confused with the resume wake that rearmed us.
            self.shared
                .log_at(MetricsEvent::AckFlush(claim.reason()), now);
            (claim, encoded_page_lengths)
        };
        let fec_enabled = self.fec.is_some();
        let mut pages_sent = 0;
        for (index, written_bytes) in encoded_page_lengths.into_iter().enumerate() {
            let Some(written_bytes) = written_bytes else {
                continue;
            };
            let (payload, codec_pkt, wire_pkt) = bufs.parts_mut();
            // ACK-page padding, resolved from the HarmfulPaddingPolicy (see
            // [`Self::pad_ack_page`]): the pad rides INSIDE the FEC envelope
            // when FEC is on and is stripped by the codec's zero-tail rule at
            // the receiver, so padded and unpadded ACKs decode identically.
            let page_buf = if index == 0 { codec_pkt } else { payload };
            let page_len = self.pad_ack_page(page_buf, written_bytes);
            let encoded_page = &page_buf[..page_len];
            match self.send_with_fec(encoded_page, wire_pkt).await {
                Ok(_) => {
                    pages_sent += 1;
                    if fec_enabled {
                        let fec_now = Instant::now();
                        let tail_requested = self.shared.with_reliable_layer(|reliable_layer| {
                            reliable_layer.can_send_tail_fec(fec_now)
                        });
                        let gate = self.fec_gate_decision(fec_now, tail_requested);
                        if let Err(error) = self.close_fec_burst(fec_now, gate, false).await {
                            self.ack_feedback
                                .complete(claim, AckFlushOutcome::Fatal { pages_sent });
                            return Err(error);
                        }
                    }
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    // The underlay refused the ACK datagram: rearm the flush
                    // schedule so the write driver parks for ACK_FLUSH_AGE
                    // instead of busy-looping on an immediately-due schedule
                    // (a WouldBlock flush leaves the pending work untouched).
                    self.ack_feedback.complete(
                        claim,
                        AckFlushOutcome::WouldBlock {
                            pages_sent,
                            rearm: true,
                        },
                    );
                    self.shared.notify_session_outbound_progress();
                    return Ok(());
                }
                Err(error) => {
                    self.ack_feedback
                        .complete(claim, AckFlushOutcome::Fatal { pages_sent });
                    self.shared
                        .press_error(error, MetricsTerminationCause::AckWrite);
                    // The ACK symbol was already encoded into the open group
                    // by `send_with_fec`; drop the group so a fatal return
                    // cannot leak it into the next pass.
                    self.skip_open_fec_group();
                    return Err(error);
                }
            }
        }
        self.ack_feedback
            .complete(claim, AckFlushOutcome::Sent { pages_sent });
        self.shared.notify_session_outbound_progress();
        Ok(())
    }

    pub(crate) async fn send_with_fec(
        &mut self,
        codec_pkt: &[u8],
        fec_buf: &mut [u8],
    ) -> Result<usize, IoErr> {
        let send_buf: &[u8] = match self.fec.as_mut() {
            Some(fec) => {
                let n = fec.encode_data(codec_pkt, fec_buf, false);
                &fec_buf[..n]
            }
            None => codec_pkt,
        };
        self.utp_write.send(send_buf).await
    }
}

#[cfg(test)]
mod tests {
    use crate::delivery::frame::FrameMode;
    use crate::metrics::{MetricsEvent, MetricsObserver, MetricsTerminationCause};
    use crate::obfuscate::padding::AckPaddingMode;
    use crate::traffic_shaping::recovery::liveness::PeerLiveness;
    use crate::traffic_shaping::redundancy::RetransmissionArmorConfig;
    use crate::traffic_shaping::redundancy::fec_tuning::FecTuning;
    use crate::transmission::connection::new_connection_with_watchdog_tuning;
    use crate::transmission::test_doubles::{BlockingWrite, PendingRead};
    use crate::transmission::transmission_layer::UnreliableLayer;
    use crate::transmission::watchdog_tuning::WatchdogTuning;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::FecSymbolCache;

    fn term(l: &PeerLiveness, now: Instant, has_in_flight: bool) -> bool {
        l.should_terminate_session(now, has_in_flight)
    }

    /// The FEC symbol cache must not allocate or copy on the happy path: a
    /// first attempt mints the symbol in place and caches nothing; only a
    /// WouldBlock remembers the bytes, and the within-pass retry restores
    /// them without re-encoding a second group slot.
    #[test]
    fn fec_symbol_cache_mints_in_place_and_only_caches_on_would_block() {
        let mut cache = FecSymbolCache::default();
        let mut encode_calls = 0;
        let mut wire = [0u8; 16];
        let len = cache.restore_or_encode(7, &mut wire, |buf| {
            encode_calls += 1;
            buf[..3].copy_from_slice(b"abc");
            3
        });
        assert_eq!(len, 3);
        assert_eq!(&wire[..3], b"abc");
        assert_eq!(
            cache.len(),
            0,
            "the first attempt must mint in place and cache nothing (no allocation on the happy path)"
        );

        // The send would block: remember the bytes for the retry.
        cache.remember(7, &wire[..len]);
        assert_eq!(cache.len(), 1);

        // The retry restores the cached symbol without re-encoding.
        let mut retry_wire = [0u8; 16];
        let retry_len = cache.restore_or_encode(7, &mut retry_wire, |_| {
            encode_calls += 1;
            0
        });
        assert_eq!(retry_len, 3);
        assert_eq!(&retry_wire[..3], b"abc");
        assert_eq!(
            encode_calls, 1,
            "the retry must reuse the cached symbol, not re-encode a second group slot"
        );
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
            mss: crate::mss::Mss::try_new(crate::udp::NO_FEC_MSS).unwrap(),
            fec: None,
            fec_tuning: FecTuning::default(),
            frame_delivery: FrameMode::default(),
            retransmission_armor: RetransmissionArmorConfig::disabled(),
            instream_group_fec: false,
            ack_padding: AckPaddingMode::None,
        };
        let watchdog = WatchdogTuning::new(1, Duration::ZERO, Duration::ZERO, Duration::ZERO);
        let (shared, write_half, _read_half, _reaper) =
            new_connection_with_watchdog_tuning(layer, None, watchdog);
        let now = Instant::now();
        let mut reliable = shared.reliable_layer_for_test().lock().unwrap();
        reliable.send_data_buf(b"x", now).unwrap();
        let mut packet = vec![0; crate::udp::NO_FEC_MSS];
        assert!(reliable.send_data_pkt(&mut packet, now).is_some());
        drop(reliable);
        write_half.proactively_terminate_stalled_session_at(now + Duration::from_nanos(1));
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
