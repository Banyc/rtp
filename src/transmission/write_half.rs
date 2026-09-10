use std::sync::Arc;
use std::time::Instant;

use super::ack_feedback::{AckClaim, AckFeedback, AckFlushOutcome, AckPage, AckSchedule};
use super::connection::Connection;
use super::termination::{KillPolicy, TerminationWriter};
use super::transmission_layer::{
    FEC_DEBUG, PRINT_DEBUG_MSGS, ProactiveTerminationContext, SendBufs, UnreliableWrite,
};
use crate::ack::{AckHistory, EncodeAck};
use crate::codec::{EncodeData, encode_ack_data, encode_kill};
use crate::io_err::IoErr;
use crate::metrics::{MetricsEvent, MetricsTerminationCause};
use crate::obfuscate::padding::AckPaddingMode;
use crate::obfuscate::sampler::DataSizeSampler;
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

impl WriteHalf {
    /// Encode one ACK page into `output`, optionally with piggybacked data
    /// appended after the ACK command (one ACK_CMD per datagram). Consumes
    /// the claim's pending echo on the first page. Returns the encoded
    /// length. Shared by the standalone flush and the piggyback paths so the
    /// page-encode shape (history → EncodeAck → echo → wire bytes) lives in
    /// exactly one place.
    fn encode_ack_page(
        &self,
        claim: &mut AckClaim,
        history: &AckHistory,
        page: AckPage,
        data: Option<EncodeData<'_>>,
        output: &mut [u8],
    ) -> usize {
        let ack = EncodeAck {
            queue: history,
            first_block_index: page.first_block_index,
            max_blocks: page.max_blocks,
        };
        let echo = claim.take_echo();
        encode_ack_data(self.shared.session_tag(), Some(ack), echo, data, output).unwrap()
    }

    /// Claim a due ACK and encode it piggybacked on the data packet: page 0
    /// rides in `codec_pkt` ahead of the data (one ACK_CMD per datagram, so
    /// at most one page piggybacks). Returns the combined datagram length
    /// and the piggyback (None when no ACK is due or none is claimable — the
    /// datagram is then data-only, exactly as before).
    fn claim_piggyback_ack(
        &self,
        now: Instant,
        codec_pkt: &mut [u8],
        data: EncodeData<'_>,
    ) -> (usize, Option<PiggybackAck>) {
        let (n, claimed) = self.shared.with_reliable_layer(|reliable_layer| {
            let history = reliable_layer.pkt_recv_space().ack_history();
            if !self.ack_feedback.schedule(now).is_due() {
                return (
                    encode_ack_data(None, None, None, Some(data), codec_pkt).unwrap(),
                    None,
                );
            }
            let Some(mut claim) = self.ack_feedback.claim(now, history.len()) else {
                return (
                    encode_ack_data(None, None, None, Some(data), codec_pkt).unwrap(),
                    None,
                );
            };
            let page0 = claim.pages()[0].expect("a claim always carries page 0");
            let n = self.encode_ack_page(&mut claim, history, page0, Some(data), codec_pkt);
            let page1 = claim.pages()[1];
            (n, Some((claim, page1)))
        });
        let Some((claim, page1)) = claimed else {
            return (n, None);
        };
        // A successful transactional claim names why it became due; the
        // observation is emitted before the datagram is sent so the claim
        // event is never confused with the resume wake that rearmed us.
        // `log_at` snapshots the reliable layer, so it must run outside the
        // `with_reliable_layer` lock above (the same discipline `flush_acks`
        // follows) — calling it inside would deadlock on the non-reentrant
        // mutex whenever observability is enabled.
        self.shared
            .log_at(MetricsEvent::AckFlush(claim.reason()), now);
        (n, Some(PiggybackAck { claim, page1 }))
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
        // selective ack).
        let page1_len = self.shared.with_reliable_layer(|reliable_layer| {
            let history = reliable_layer.pkt_recv_space().ack_history();
            let (payload, _codec_pkt, _wire_pkt) = bufs.parts_mut();
            self.encode_ack_page(&mut claim, history, page1, None, payload)
        });
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
                self.ack_feedback
                    .complete(claim, AckFlushOutcome::WouldBlock { pages_sent: 1 });
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
    fn refresh_fec_loss_mode(&mut self) {
        let loss = self
            .shared
            .with_reliable_layer(|layer| layer.congestion_loss_ratio());
        self.fec_gate.refresh_loss(self.fec.is_some(), loss);
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
        let (made_progress, completed_at) = self.send_pkts_inner(bufs, now).await?;
        Ok(SendLoopResult {
            made_progress,
            wake: self.shared.next_send_wake(completed_at),
        })
    }

    pub(crate) fn ack_schedule(&self, now: Instant) -> AckSchedule {
        self.ack_feedback.schedule(now)
    }

    pub(crate) fn debug_conn_id(&self) -> usize {
        Arc::as_ptr(&self.shared) as usize
    }

    async fn send_pkts_inner(
        &mut self,
        bufs: &mut SendBufs,
        mut now: Instant,
    ) -> Result<(bool, Instant), IoErr> {
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
        loop {
            if self.shared.has_error() {
                self.return_error_after_requested_kill(bufs).await?;
            }
            let (payload, codec_pkt, wire_pkt) = bufs.parts_mut();
            let res = self.shared.with_reliable_layer_mut(|reliable_layer| {
                reliable_layer.send_data_pkt(payload, now)
            });
            if std::env::var("RTP_DEBUG_SEND").is_ok() {
                eprintln!(
                    "[sdp-res] conn={:x} result={}",
                    Arc::as_ptr(&self.shared) as usize,
                    if res.is_some() { "pkt" } else { "none" }
                );
            }
            self.shared
                .log_at(crate::metrics::MetricsEvent::SendDataPacketAttempt, now);
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
            let instream = self.instream_group_fec_enabled;
            let armor_decision = self.retransmission_armor.decide(is_recovery, || {
                self.shared
                    .with_reliable_layer(|reliable_layer| reliable_layer.queue_building())
            });
            // On the first data packet of the pass, piggyback a due ACK on
            // the data datagram (page 0 rides ahead of the data; page 1, if
            // any, is sent standalone after). This hides ACKs among data
            // packets at zero wire cost — no standalone ACK datagram — and
            // the wire format already carries ack+data in one datagram.
            let (n, piggyback) = if !piggyback_attempted {
                piggyback_attempted = true;
                self.claim_piggyback_ack(now, codec_pkt, data)
            } else {
                (
                    encode_ack_data(None, None, None, Some(data), codec_pkt).unwrap(),
                    None,
                )
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
                    let fec_n = fec.encode_data(utp_pkt, wire_pkt, instream);
                    &wire_pkt[..fec_n]
                }
                None => utp_pkt,
            };
            if std::env::var("RTP_DEBUG_SEND").is_ok() {
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
            if std::env::var("RTP_DEBUG_SEND").is_ok() {
                eprintln!(
                    "[send-res] seq={} result={:?}",
                    p.seq.to_wire(),
                    primary_res.as_ref().map(|_| "ok").map_err(|e| e.kind())
                );
            }
            match primary_res {
                Ok(_) => {
                    self.fec_gate.record_data_send(is_recovery);
                    if self.fec.is_some() && instream {
                        self.maybe_flush_full_fec_group(now).await?;
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
                                    return Err(e);
                                }
                            }
                        }
                    }
                    // The piggybacked ACK rode on the data datagram; finish
                    // the claim (send page 1 standalone if any) now that the
                    // data send and its duplicate are done.
                    if let Some(piggyback) = piggyback {
                        self.finish_piggyback_claim(piggyback, bufs).await?;
                    }
                    continue;
                }
                Err(error) if error == std::io::ErrorKind::WouldBlock => {
                    // A piggybacked ACK was not delivered: restore its work
                    // for a later flush and retry the data packet without it.
                    if let Some(piggyback) = piggyback {
                        self.ack_feedback.complete(
                            piggyback.claim,
                            AckFlushOutcome::WouldBlock { pages_sent: 0 },
                        );
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
        Ok((made_progress, completed_at))
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
        if std::env::var("RTP_DEBUG_SEND").is_ok() {
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
        // Session tag prefix (9 bytes) + KILL_CMD byte, when a tag exists.
        let mut buf = [0; 1 + 1 + 8];
        let n = encode_kill(self.shared.session_tag(), &mut buf).unwrap();
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
            let (claim, encoded_page_lengths) = self.shared.with_reliable_layer(|reliable_layer| {
                let history = reliable_layer.pkt_recv_space().ack_history();
                let Some(mut claim) = self.ack_feedback.claim(now, history.len()) else {
                    return (None, [None; 2]);
                };
                let (payload, codec_pkt, _) = bufs.parts_mut();
                let mut encoded_page_lengths = [None; 2];
                for (index, page) in claim.pages().into_iter().enumerate() {
                    let Some(page) = page else {
                        continue;
                    };
                    let output = if index == 0 {
                        &mut codec_pkt[..]
                    } else {
                        &mut payload[..]
                    };
                    encoded_page_lengths[index] =
                        Some(self.encode_ack_page(&mut claim, history, page, None, output));
                }
                (Some(claim), encoded_page_lengths)
            });
            let Some(claim) = claim else {
                return Ok(());
            };
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
                    self.ack_feedback
                        .complete(claim, AckFlushOutcome::WouldBlock { pages_sent });
                    self.shared.notify_session_outbound_progress();
                    return Ok(());
                }
                Err(error) => {
                    self.ack_feedback
                        .complete(claim, AckFlushOutcome::Fatal { pages_sent });
                    self.shared
                        .press_error(error, MetricsTerminationCause::AckWrite);
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
