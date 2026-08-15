use core::{num::NonZeroUsize, time::Duration};
use std::time::Instant;

use dre::PacketState;
use primitive::{
    arena::obj_pool::{ObjPool, buf_pool},
    ops::float::{PosR, UnitR},
};

use crate::{
    ack::{AckBlocks, MAX_ACK_BLOCKS},
    recv_queue::pkt_recv_space::MAX_NUM_RECVING_PKTS,
    sequence::{SendWindow, SequenceNumber, le, lt},
    traffic_shaping::recovery::{
        liveness::PeerLiveness,
        loss_event_window::LossEventWindow,
        outage::{OutageDetection, OutageEpoch},
        rtt_stats::RttStats,
        rtx_index::{
            DeferredLossIndex, ReadyReason, RetransmissionActivation, RetransmissionIndex,
        },
        tlp::TailLossProber,
    },
    transmission::watchdog_tuning::WatchdogTuning,
};

pub const INIT_CWND: usize = 32;
pub(crate) const OUTAGE_RECOVERY_CWND: usize = 16;
pub(super) const LOSS_RATE_MIN_SAMPLES: usize = 16;
pub(crate) const CWND_SEND_RATE_SCALE: usize = 8;

/// Number of newer in-flight packets that must be SACKed past an unacked
/// packet before the evidence-gated fast-loss path declares it lost.  Mirrors
/// the classic dup-ACK threshold of 3.
pub(crate) const FAST_LOSS_SACK_THRESHOLD: u32 = 3;

/// Whether the jitter-tolerant fast-retransmit ("jitter cap") path is
/// enabled at process startup.  Reads `RTP_JITTER_CAP` once; `1`/`true`
/// enables it, anything else preserves stock behaviour byte-for-byte.
///
/// When enabled, the retransmission of an out-of-order-passed packet is
/// scheduled on a fast reorder window `srtt + max(rttvar, srtt/4)` (rttvar
/// NOT multiplied by `K`), well below the stock `srtt + max(K*rttvar,
/// srtt/4)` on high-jitter links.  The congestion-control loss event is NOT
/// recorded at the fast retransmit time: it is deferred to the stock
/// reorder-window deadline and recorded only if the packet is still unacked
/// then (genuine loss).  If the original is acked before the stock deadline
/// (it was reordering, not loss) no loss event is recorded and no later
/// repair is double-counted.
fn jitter_cap_enabled(value: Option<&str>) -> bool {
    value.is_none_or(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
}

fn jitter_cap_from_env() -> bool {
    jitter_cap_enabled(std::env::var("RTP_JITTER_CAP").ok().as_deref())
}

#[derive(Debug)]
pub struct PktSendSpace {
    send_wnd: SendWindow<Option<InFlightPkt>>,
    /// Unacked occupancy, aligned slot-for-slot with `send_wnd`: `true`
    /// while the slot holds an in-flight packet, `false` once the packet is
    /// acked (the slot becomes a `None` data-window hole).  Both windows
    /// push and pop together, so a data-window hole can never desynchronize
    /// the occupancy view used to bound ACK analysis.
    occupied_wnd: SendWindow<bool>,
    /// The newest sequence still holding an in-flight packet (the logical
    /// tail of the window), updated on send and only rescanned when that
    /// exact packet is acked.
    newest_unacked: Option<SequenceNumber>,
    num_in_flight: usize,
    reused_buf: ObjPool<Vec<u8>>,
    cwnd: NonZeroUsize,
    out_of_order_seq_end: Option<SequenceNumber>,
    /// Any sequence after this does not participate in data loss analysis.
    max_pipe_seq: Option<SequenceNumber>,
    loss_event_window: LossEventWindow,
    /// Count of RTO deadlines postponed by the lazy live-estimator floor
    /// (see [`crate::traffic_shaping::recovery::rtx_index::RetransmissionIndex::promote_due`]).
    rto_deadline_postponements: u64,
    /// RFC 8985 tail-loss-probe engine.
    tlp: TailLossProber,

    /// Peer-liveness monitor: progress tracking.
    liveness: PeerLiveness,
    /// Outage-recovery epoch state machine.
    outage: OutageEpoch,
    /// RTT statistics: smoothed RTT, RTO, and lifetime minimum.
    rtt_stats: RttStats,

    /// Hard-disable flag for evidence-gated fast loss.  Set when reordering
    /// is actually observed (a fast-loss-retransmitted packet's original
    /// later arrives — detected by an ACK arriving for such a packet faster
    /// than the retransmit could have been acknowledged).  Once set it stays
    /// set for the life of the connection; the structural jitter gate alone
    /// is not enough to re-arm after a real reordering event.
    fast_loss_disabled: bool,

    /// Pending deferred loss-event recordings for the jitter-tolerant fast-
    /// retransmit path (`RTP_JITTER_CAP`).  Each entry is a packet that was
    /// retransmitted at the fast reorder window; its CC loss event is deferred
    /// to the stock reorder-window deadline and recorded only if the packet
    /// is still unacked then.  An ACK for the seq cancels the entry
    /// (reordering, not loss).  Indexed by sequence for O(1) cancellation
    /// with a deadline-ordered set for the next-due selection.
    deferred_losses: DeferredLossIndex,

    /// Snapshot of `RTP_JITTER_CAP` taken at construction.  Stored as a
    /// field rather than read from a `OnceLock` so tests can construct a
    /// `PktSendSpace` with the toggle on or off deterministically without
    /// racing other parallel tests in the same binary.
    jitter_cap: bool,

    /// Wrap-safe eligibility and deadline index for retransmission: tracks
    /// the exact `min(num_in_flight, cwnd)` in-flight prefix and drives
    /// `has_rtx`, retransmit selection, and the retransmission part of
    /// `next_poll_time` without per-poll send-window scans.
    rtx_index: RetransmissionIndex,

    // reused buffers
    unacked_buf: Vec<SequenceNumber>,
    ack_buf: Vec<SequenceNumber>,
    ack_block_buf: Vec<(u64, u64)>,
    sacked_above_buf: Vec<u32>,
    fast_loss_buf: Vec<SequenceNumber>,
}
impl PktSendSpace {
    pub fn new() -> Self {
        Self::new_at(SequenceNumber::ZERO)
    }

    /// Send space seeded at `initial_seq` (handshake-derived directional
    /// start); `new()` is the zero-seeded form used by connections opened
    /// without the handshake.
    pub(crate) fn new_at(initial_seq: SequenceNumber) -> Self {
        Self {
            send_wnd: SendWindow::new(initial_seq),
            occupied_wnd: SendWindow::new(initial_seq),
            newest_unacked: None,
            num_in_flight: 0,
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            out_of_order_seq_end: None,
            max_pipe_seq: None,
            loss_event_window: LossEventWindow::new(),
            rto_deadline_postponements: 0,
            tlp: TailLossProber::new(),
            liveness: PeerLiveness::new(),
            outage: OutageEpoch::new(),
            rtt_stats: RttStats::new(),
            fast_loss_disabled: false,
            deferred_losses: DeferredLossIndex::new(initial_seq),
            jitter_cap: jitter_cap_from_env(),
            rtx_index: RetransmissionIndex::new(initial_seq),
            unacked_buf: vec![],
            ack_buf: vec![],
            ack_block_buf: Vec::with_capacity(MAX_ACK_BLOCKS),
            sacked_above_buf: vec![],
            fast_loss_buf: vec![],
        }
    }

    pub fn smooth_rtt(&self) -> Duration {
        self.rtt_stats.smooth_rtt()
    }

    pub fn smooth_rtt_var(&self) -> Duration {
        self.rtt_stats.smooth_rtt_var()
    }

    /// Whether the evidence-gated fast-loss path is currently armed.  Arming
    /// requires the structural low-jitter gate (`K*rttvar < srtt/4`) AND no
    /// observed reordering on the connection.  Always-on when armed; there is
    /// no env toggle — the structural gate is the safety.
    pub fn fast_loss_armed(&self) -> bool {
        !self.fast_loss_disabled && self.rtt_stats.fast_loss_armed()
    }

    #[cfg(test)]
    pub(crate) fn fast_loss_disabled(&self) -> bool {
        self.fast_loss_disabled
    }

    /// Test-only constructor that forces the `RTP_JITTER_CAP` toggle to a
    /// fixed value regardless of the process environment, so parallel tests
    /// in the same binary do not race on the env var.
    #[cfg(test)]
    pub(crate) fn with_jitter_cap(enabled: bool) -> Self {
        let mut s = Self::new();
        s.jitter_cap = enabled;
        s
    }

    /// The sequences currently tracked by the retransmission index (the
    /// exact `min(num_in_flight, cwnd)` in-flight prefix), in logical order.
    #[cfg(test)]
    pub(crate) fn active_rtx_seqs(&self) -> Vec<SequenceNumber> {
        self.rtx_index.active_entries().collect()
    }

    pub fn with_watchdog_tuning(mut self, tuning: WatchdogTuning) -> Self {
        self.liveness = PeerLiveness::with_tuning(tuning);
        self
    }

    fn sync_rtx_index(&mut self) {
        self.sync_rtx_index_inner(true, None);
    }

    /// The ACK-path form of [`Self::sync_rtx_index`]: the ack delivery loop
    /// already deactivated delivered entries, so no stale-entries pass runs;
    /// `reorder_extension` is the `[start, end)` range of sequences the ack's
    /// selective evidence newly brought below the reorder boundary, and only
    /// those sequences get a fresh reorder deadline key.
    fn sync_rtx_index_after_ack(
        &mut self,
        reorder_extension: Option<(SequenceNumber, SequenceNumber)>,
    ) {
        self.sync_rtx_index_inner(false, reorder_extension);
    }

    /// Resize the retransmission index to the exact
    /// `min(num_in_flight, cwnd)` in-flight prefix: deactivate active entries
    /// that fell outside the prefix and activate in-flight packets inside it
    /// that are not yet active.  Ready reasons of packets that stay active
    /// are preserved (resize never touches readiness).  Used when the cwnd
    /// changes and after a plain cumulative-only ACK, where no evidence was
    /// re-derived.
    fn resize_rtx_index_to_active_target(&mut self) {
        let active_target = self.active_target();
        while self.rtx_index.active_count() > active_target {
            let seq = self
                .rtx_index
                .last_active()
                .expect("a non-empty retransmission prefix must have a logical tail");
            assert!(self.rtx_index.deactivate(seq));
        }

        let missing = active_target - self.rtx_index.active_count();
        let start = self
            .rtx_index
            .last_active()
            .map_or_else(|| self.send_wnd.start(), |seq| seq.advance(1));
        let out_of_order_seq_end = self.out_of_order_seq_end;
        let fast_loss_armed = self.fast_loss_armed();
        let send_wnd = &self.send_wnd;
        let outage = &self.outage;
        let rtx_index = &mut self.rtx_index;
        for (seq, p) in send_wnd
            .iter_from(start)
            .filter_map(|(seq, p)| p.as_ref().map(|p| (seq, p)))
            .take(missing)
        {
            let pre_outage = outage.is_pre_outage_loss(p.sent_time);
            rtx_index.activate(RetransmissionActivation {
                seq,
                rto_at: p.sent_time + p.rto,
                sent_at: p.sent_time,
                apply_live_rto_floor: !p.rto_from_tail_probe,
                reorder_eligible: out_of_order_seq_end.is_some_and(|end| lt(seq, end)),
                fast_loss_eligible: fast_loss_armed && p.is_fast_loss(),
                pre_outage_eligible: pre_outage,
            });
        }
        debug_assert_eq!(self.rtx_index.active_count(), active_target);
    }

    /// The plain cumulative-only ACK path: resize the index to the exact
    /// active prefix (a plain ACK carries no selective evidence and delivers
    /// no new packets, so nothing else changed), then advance every anchor to
    /// the send-window front so wrap safety holds even if the window slid
    /// since the last sync.
    fn refill_rtx_index_after_plain_ack(&mut self) {
        self.resize_rtx_index_to_active_target();
        let anchor = if self.send_wnd.is_empty() {
            self.send_wnd.next()
        } else {
            self.send_wnd.start()
        };
        self.rtx_index.advance_anchor(anchor);
        self.deferred_losses.advance_anchor(anchor);
    }

    /// Reconcile the retransmission index with the send window's exact
    /// `min(num_in_flight, cwnd)` in-flight prefix — or, while an
    /// outage-recovery epoch is open, the whole pre-outage window.  Centralizes,
    /// in one place, active-prefix updates, anchor advancement, reorder-boundary
    /// synchronization, fast-loss gate synchronization, and outage
    /// readiness.  Runs only on state transitions (send/ack/cwnd change/
    /// outage detection/tail probe), never on the per-poll paths
    /// (`has_rtx`/`next_poll_time`), which read the index directly.
    fn sync_rtx_index_inner(
        &mut self,
        stale_entries_may_exist: bool,
        reorder_extension: Option<(SequenceNumber, SequenceNumber)>,
    ) {
        let active_target = self.active_target();
        let out_of_order_seq_end = self.out_of_order_seq_end;
        let fast_loss_armed = self.fast_loss_armed();
        let anchor = if self.send_wnd.is_empty() {
            self.send_wnd.next()
        } else {
            self.send_wnd.start()
        };
        // Snapshot the exact in-flight active prefix.
        self.unacked_buf.clear();
        self.unacked_buf.extend(
            Self::unacked(&self.send_wnd)
                .take(active_target)
                .map(|(seq, _)| seq),
        );
        // Deactivate entries that left the prefix.  The stale set is small
        // (bounded by the index size) and only recomputed when the caller
        // expects stale entries: the ACK delivery loop already deactivated
        // everything it delivered.
        if stale_entries_may_exist {
            self.fast_loss_buf.clear();
            {
                let unacked = &self.unacked_buf;
                let rtx_index = &self.rtx_index;
                let stale = &mut self.fast_loss_buf;
                stale.extend(rtx_index.active_entries().filter(|seq| {
                    let offset = anchor.forward_distance_to(*seq);
                    unacked
                        .binary_search_by(|candidate| {
                            anchor.forward_distance_to(*candidate).cmp(&offset)
                        })
                        .is_err()
                }));
            }
            for index in 0..self.fast_loss_buf.len() {
                let seq = self.fast_loss_buf[index];
                self.rtx_index.deactivate(seq);
            }
        }
        // Activate the entries that entered the prefix, sync the reorder
        // boundary for already-active entries, and re-sync the independent
        // ready reasons on every prefix sync: clear the transient reorder and
        // fast-loss reasons (re-armed below only when their evidence still
        // holds), then set pre-outage from the packet's own send time.  The
        // RTO reason survives — a packet whose RTO already expired is
        // re-promoted (and lazily re-floored) on the next `promote_due`.
        self.fast_loss_buf.clear();
        for &seq in &self.unacked_buf {
            let Some(Some(p)) = self.send_wnd.get(&seq) else {
                continue;
            };
            let out_of_order = out_of_order_seq_end.is_some_and(|end| lt(seq, end));
            let preserve_fast_loss = fast_loss_armed && p.is_fast_loss();
            if preserve_fast_loss {
                self.fast_loss_buf.push(seq);
            }
            let was_active = self.rtx_index.is_active(&seq);
            if !was_active {
                self.rtx_index.activate(RetransmissionActivation {
                    seq,
                    rto_at: p.sent_time + p.rto,
                    sent_at: p.sent_time,
                    apply_live_rto_floor: !p.rto_from_tail_probe,
                    reorder_eligible: out_of_order,
                    fast_loss_eligible: false,
                    pre_outage_eligible: false,
                });
            }
            let pre_outage = self
                .outage
                .is_pre_outage_loss(p.sent_time)
                .then_some(p.sent_time);
            let rearm_reorder =
                self.rtx_index
                    .sync_active_evidence_reasons(seq, preserve_fast_loss, pre_outage);
            let entered_reorder_range =
                reorder_extension.is_some_and(|(start, end)| !lt(seq, start) && lt(seq, end));
            if was_active && out_of_order && (entered_reorder_range || rearm_reorder) {
                self.rtx_index.add_reorder_candidate(seq, p.sent_time);
            }
        }
        for &seq in &self.fast_loss_buf {
            let sent_time = self.send_wnd.get(&seq).unwrap().as_ref().unwrap().sent_time;
            self.rtx_index
                .set_reason(seq, ReadyReason::FastLoss, Some(sent_time));
        }
        // Advance the anchor to the send-window front (or the next sequence
        // when the window is empty) so wrap safety holds as the window
        // advances.
        self.rtx_index.advance_anchor(anchor);
        self.deferred_losses.advance_anchor(anchor);
    }

    fn unacked(
        send_wnd: &SendWindow<Option<InFlightPkt>>,
    ) -> impl Iterator<Item = (SequenceNumber, &InFlightPkt)> {
        send_wnd
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
    }
    fn unacked_mut(
        send_wnd: &mut SendWindow<Option<InFlightPkt>>,
    ) -> impl Iterator<Item = (SequenceNumber, &mut InFlightPkt)> {
        send_wnd
            .iter_mut()
            .filter_map(|(k, v)| v.as_mut().map(|v| (k, v)))
    }

    pub fn cwnd(&self) -> NonZeroUsize {
        self.cwnd
    }

    /// Test-only: shrink the congestion window to `cwnd` so the send loop
    /// stops after that many new packets, leaving the rest of staged data in
    /// the send buffer.  Used by in-stream group FEC tests that need the stock
    /// `can_send_tail_fec` gate closed (send buffer not empty) while still
    /// having a partial FEC group open.  This is the single state-mutating
    /// seam behind [`crate::reliable::reliable_layer::ReliableLayer`]'s
    /// `set_cwnd_for_test`; only that method is test-facing.
    #[cfg(test)]
    pub(crate) fn set_cwnd(&mut self, cwnd: NonZeroUsize) {
        self.cwnd = cwnd;
        self.sync_rtx_index();
    }

    pub fn next_seq(&self) -> SequenceNumber {
        self.send_wnd.next()
    }

    /// Single-accessor form of [`Self::send_window_metrics`]; retained as
    /// public API for external consumers of the send window.
    #[allow(dead_code)]
    pub fn num_rtxed_pkts(&self) -> usize {
        let mut n = 0;
        for (_, p) in Self::unacked(&self.send_wnd) {
            if !p.rtxed {
                continue;
            }
            n += 1;
        }
        n
    }

    pub fn reused_buf(&mut self) -> &mut ObjPool<Vec<u8>> {
        &mut self.reused_buf
    }

    pub fn no_resp_for(&self, now: Instant) -> Option<Duration> {
        self.liveness.no_resp_for(now)
    }

    pub fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        self.liveness.no_progress_for(now)
    }

    pub fn stall_reason(&self, now: Instant) -> Option<super::liveness::PeerStall> {
        self.liveness.stall_reason(now, !self.no_pkts_in_flight())
    }

    #[cfg(test)]
    pub fn should_terminate_session(&self, now: Instant) -> bool {
        self.liveness
            .stall_reason(now, !self.no_pkts_in_flight())
            .is_some()
    }

    pub fn ack(&mut self, recved: AckBlocks<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        let send_start = self.send_wnd.start();
        let sent_span = self.send_wnd.len() as u64;
        let previous_out_of_order_seq_end = self.out_of_order_seq_end;
        let fast_loss_was_armed = self.fast_loss_armed();
        // This ACK can only deliver sequences below the relevant end offset
        // (cumulative front ∪ highest clipped SACK block): walk only the
        // occupied prefix of that bound instead of scanning the whole send
        // window.
        let relevant_end = recved.relevant_unacked_end_offset(send_start, sent_span);
        self.unacked_buf.clear();
        for (seq, &occupied) in self
            .occupied_wnd
            .iter()
            .take_while(|(seq, _)| send_start.forward_distance_to(*seq) < relevant_end)
        {
            if occupied {
                self.unacked_buf.push(seq);
            }
        }
        self.ack_buf.clear();
        let analysis = recved.analyze(
            send_start,
            sent_span,
            &self.unacked_buf,
            &mut self.ack_block_buf,
            &mut self.ack_buf,
            &mut self.sacked_above_buf,
        );
        let peer_response = analysis.cumulative_is_current;
        if let Some(seq) = analysis.highest_sacked {
            let replace = self.out_of_order_seq_end.is_none_or(|current| {
                send_start.forward_distance_to(current) < send_start.forward_distance_to(seq)
            });
            if replace {
                self.out_of_order_seq_end = Some(seq);
            }
        }
        let delivered = self.ack_buf.len();
        let peer_response = peer_response || delivered > 0;
        if delivered > 0 {
            self.tlp.reset();
        }
        if !self.fast_loss_disabled
            && let Some(min_rtt) = self.rtt_stats.min_rtt()
        {
            for &s in &self.ack_buf {
                let Some(Some(p)) = self.send_wnd.get(&s).map(|o| o.as_ref()) else {
                    continue;
                };
                if let Some(rtx_t) = p.fast_loss_rtx_time
                    && now < rtx_t + min_rtt
                {
                    self.fast_loss_disabled = true;
                    break;
                }
            }
        }
        let mut cumulative_advance = 0;
        let newest_unacked = self.newest_unacked;
        let mut newest_unacked_delivered = false;
        for &s in &self.ack_buf {
            newest_unacked_delivered |= newest_unacked == Some(s);
            self.rtx_index.deactivate(s);
            let p = self.send_wnd.get_mut(&s).unwrap();
            let p = p.take().unwrap();
            *self.occupied_wnd.get_mut(&s).unwrap() = false;
            self.num_in_flight -= 1;
            if self.send_wnd.start() == s {
                // Cumulative release: pop the physical head plus the None
                // prefix behind it, mirroring every pop on the occupancy
                // window so the two stay aligned slot-for-slot.
                self.send_wnd.pop().unwrap();
                let released = 1 + self.send_wnd.pop_none();
                for _ in 0..released {
                    self.occupied_wnd.pop().unwrap();
                }
                cumulative_advance += released;
            }
            self.deferred_losses.cancel(s);
            self.reused_buf.put(p.data);
            acked.push(p.stats);
        }
        if cumulative_advance > 0 {
            self.liveness.record_progress();
        }
        // The newest unacked packet was acked: rescan for the new tail.  No
        // other ACK ever moves it, so no other ACK rescans.
        if newest_unacked_delivered {
            self.newest_unacked = self.send_wnd.last_present().map(|(seq, _)| seq);
        }
        let current_start = self.send_wnd.start();
        let current_span = self.send_wnd.len() as u64;
        if self
            .out_of_order_seq_end
            .is_some_and(|seq| current_start.forward_distance_to(seq) > current_span)
        {
            self.out_of_order_seq_end = None;
        }
        let mut fast_loss_eligibility_changed = false;
        if analysis.has_sack_evidence {
            for (&sequence, &sacked_above) in self.unacked_buf.iter().zip(&self.sacked_above_buf) {
                let Some(Some(packet)) = self.send_wnd.get_mut(&sequence) else {
                    continue;
                };
                let was_fast_loss = packet.is_fast_loss();
                packet.sacked_above = packet.sacked_above.max(sacked_above);
                fast_loss_eligibility_changed |= was_fast_loss != packet.is_fast_loss();
            }
        }
        let reorder_extension = match (previous_out_of_order_seq_end, self.out_of_order_seq_end) {
            (None, Some(end)) => Some((current_start, end)),
            (Some(start), Some(end)) if lt(start, end) => Some((start, end)),
            _ => None,
        };
        let evidence_changed = previous_out_of_order_seq_end != self.out_of_order_seq_end
            || fast_loss_was_armed != self.fast_loss_armed()
            || fast_loss_eligibility_changed;
        if evidence_changed {
            self.sync_rtx_index_after_ack(reorder_extension);
        } else if delivered > 0 {
            self.refill_rtx_index_after_plain_ack();
        }
        self.loss_event_window
            .record_delivered(delivered, now, self.smooth_rtt());
        if !peer_response {
            return;
        }
        if self.send_wnd.is_empty() {
            self.liveness.reset_waits();
            return;
        }
        let rto = self.rtt_stats.rto_duration();
        self.liveness.refresh_waits(now, rto);
    }

    pub fn sample_rtt(&mut self, rtt: Duration, now: Instant) -> bool {
        if self.outage.should_censor_rtt_sample(rtt, now) {
            return false;
        }
        let fast_loss_was_armed = self.fast_loss_armed();
        if self.outage.try_close_epoch_with_fresh_sample() {
            self.rtt_stats.record_min_and_reseed_rto(rtt);
            self.sync_rtx_index();
            return true;
        }

        self.rtt_stats.record_rtt(rtt);
        if self.fast_loss_armed() != fast_loss_was_armed {
            self.sync_rtx_index();
        }
        false
    }

    pub fn accepts_new_pkt(&self) -> bool {
        self.num_in_flight < self.cwnd.get()
    }

    /// Sequence number of the current tail packet, if any: the newest
    /// sequence still holding an in-flight packet (tracked directly and
    /// rescanned only when that packet is acked).  `None` when the window
    /// is empty.
    fn tail_seq(&self) -> Option<SequenceNumber> {
        self.newest_unacked
    }

    /// Whether the tail packet is still unacked and enough time has passed for
    /// the next probe to fire.
    pub fn has_tail_probe(&self, now: Instant) -> bool {
        if !self.tlp.can_probe() {
            return false;
        }
        let Some(seq) = self.tail_seq() else {
            return false;
        };
        let Some(p) = self.send_wnd.get(&seq) else {
            return false;
        };
        let Some(p) = p.as_ref() else {
            return false;
        };
        self.tlp.is_due(p.sent_time, &self.rtt_stats, now)
    }

    /// Produce a tail-loss probe if it is time for one. The probe retransmits
    /// the current tail packet with a fresh timestamp and RTO without marking
    /// it as a loss event or clearing its congestion state. `packet_state`
    /// refreshes the DRE packet state on the probe so recovered delivery-rate
    /// samples inherit fresh prior_delivered/prior_time instead of stale
    /// censored state.
    pub fn tail_probe_with_state(
        &mut self,
        now: Instant,
        packet_state: impl FnOnce() -> PacketState,
    ) -> Option<Pkt<'_>> {
        if !self.has_tail_probe(now) {
            return None;
        }
        let seq = self.tail_seq()?;
        self.tlp.sent();
        let rto = self.tlp.rto(&self.rtt_stats);
        let p = self.send_wnd.get_mut(&seq)?.as_mut()?;
        // Whether the packet was a pre-outage loss is captured from its
        // ORIGINAL send time before the probe refreshes it below.
        let pre_outage = self.outage.is_pre_outage_loss(p.sent_time);
        p.stats = packet_state();

        // Refresh the timestamp/RTO so the probe is tracked as a fresh packet
        // for RTO calculation (the RTO fallback covers a lost probe).
        self_assign::self_assign! {
            p = InFlightPkt {
                stats: _,
                sent_time: now,
                rtxed: _,
                considered_new_in_cwnd: _,
                data: _,
                frame_len: _,
                rto,
                rto_from_tail_probe: true,
                sacked_above: _,
                fast_loss_rtx_time: _,
                deferred_loss_baseline_deadline: _,
            };
        }
        // The probe refreshed the packet's send time and RTO: reflect both
        // in the retransmission index so its RTO/reorder deadlines stay
        // current.  A tail-probe-derived RTO is never re-floored by the live
        // estimator, and pre-outage readiness survives the refresh.
        if self.rtx_index.is_active(&seq) {
            let out_of_order = self.out_of_order_seq_end.is_some_and(|end| lt(seq, end));
            self.rtx_index.deactivate(seq);
            self.rtx_index.activate(RetransmissionActivation {
                seq,
                rto_at: now + rto,
                sent_at: now,
                apply_live_rto_floor: false,
                reorder_eligible: out_of_order,
                fast_loss_eligible: false,
                pre_outage_eligible: pre_outage,
            });
        }
        Some(Pkt {
            seq,
            data: &p.data,
            frame_len: p.frame_len,
        })
    }

    #[cfg(test)]
    pub fn tail_probe(&mut self, now: Instant) -> Option<Pkt<'_>> {
        let no_packets_in_flight = self.no_pkts_in_flight();
        let mut connection_state = dre::ConnectionState::new(now);
        self.tail_probe_with_state(now, || {
            connection_state.send_packet_2(now, no_packets_in_flight)
        })
    }

    pub fn send(
        &mut self,
        data: Vec<u8>,
        stats: PacketState,
        frame_len: Option<u32>,
        now: Instant,
    ) -> Pkt<'_> {
        let s = self.send_wnd.next();

        self.max_pipe_seq = Some(s);

        let rto = self.rtt_stats.rto_duration();
        self.liveness.on_send(now, rto);

        let p = InFlightPkt {
            stats,
            sent_time: now,
            rtxed: false,
            considered_new_in_cwnd: false,
            data,
            frame_len,
            rto,
            rto_from_tail_probe: false,
            sacked_above: 0,
            fast_loss_rtx_time: None,
            deferred_loss_baseline_deadline: None,
        };

        self.send_wnd.push(Some(p));
        self.occupied_wnd.push(true);
        self.newest_unacked = Some(s);
        self.num_in_flight += 1;
        self.tlp.reset();

        // The new packet is indexed iff it lands inside the exact active
        // prefix of the send window: the `min(num_in_flight, cwnd)` prefix in
        // ordinary mode, the whole pre-outage window during outage recovery.
        if self.num_in_flight <= self.active_target() {
            let out_of_order = self.out_of_order_seq_end.is_some_and(|end| lt(s, end));
            self.rtx_index.activate(RetransmissionActivation {
                seq: s,
                rto_at: now + rto,
                sent_at: now,
                apply_live_rto_floor: true,
                reorder_eligible: out_of_order,
                fast_loss_eligible: false,
                pre_outage_eligible: false,
            });
        }

        Pkt {
            seq: s,
            data: &self.send_wnd.get(&s).unwrap().as_ref().unwrap().data,
            frame_len,
        }
    }

    pub fn has_rtx(&self, now: Instant) -> bool {
        let rtx_window = if self.jitter_cap {
            self.rtt_stats.fast_reorder_window()
        } else {
            self.rtt_stats.reorder_window()
        };
        self.rtx_index.has_due(now, rtx_window)
    }

    pub fn rtx_with_state(
        &mut self,
        now: Instant,
        packet_state: impl FnOnce() -> PacketState,
    ) -> Option<Pkt<'_>> {
        let stock_window = self.rtt_stats.reorder_window();
        let rtx_window = if self.jitter_cap {
            self.rtt_stats.fast_reorder_window()
        } else {
            stock_window
        };
        // Promote due retransmissions, lazily flooring stale non-tail-probe
        // RTO deadlines with the current live estimator.
        let live_rto = self.rtt_stats.rto_duration();
        self.rto_deadline_postponements = self.rto_deadline_postponements.saturating_add(
            u64::try_from(self.rtx_index.promote_due(now, rtx_window, live_rto))
                .unwrap_or(u64::MAX),
        );
        let Some((s, reasons)) = self.rtx_index.first_ready() else {
            return None;
        };
        let reasons = *reasons;
        let p = self.send_wnd.get_mut(&s)?.as_mut()?;

        // Count one loss event per packet the first time it is retransmitted,
        // unless the loss happened before the outage-recovery cut (those losses
        // are caused by the link going away) or the packet was already sent as
        // a tail-loss probe (the probe itself owns the tail-latency signal).
        let already_rtxed = p.rtxed;
        let pre_outage_loss = !already_rtxed && self.outage.is_pre_outage_loss(p.sent_time);
        let tail_probe_loss = p.rto_from_tail_probe;
        // A fast-loss retransmit is a genuine loss declaration (not a TLP
        // probe), so it records a loss event exactly as a window-expiry
        // loss would — no TLP probe accounting.
        let is_fast_loss_rtx = reasons.fast_loss_at().is_some() && !already_rtxed;

        // Jitter-tolerant fast-retransmit deferred-loss accounting.  When
        // the jitter-cap toggle is on and this retransmit fired purely on the fast
        // reorder window (i.e. the stock window has NOT yet expired for
        // the original send), the CC loss event is deferred to the stock
        // deadline (`original_sent_time + stock_window`).  If the original
        // is acked before that deadline the deferred entry is cancelled
        // (reordering, not loss); otherwise it is recorded exactly once
        // by `poll_deferred_loss`.  This is what keeps goodput from
        // collapsing on high-jitter lossy links: the rtx happens early
        // (recovery) but the loss-rate signal seen by delivery-rate CC
        // only counts genuine losses.
        let original_sent_time = p.sent_time;
        let defer_loss = self.jitter_cap
            && !already_rtxed
            && !pre_outage_loss
            && !tail_probe_loss
            && !is_fast_loss_rtx
            && now < original_sent_time + stock_window;
        let baseline_deadline_opt = if defer_loss {
            Some(original_sent_time + stock_window)
        } else {
            None
        };

        // Refresh the DRE packet state on this retransmit so recovered
        // delivery-rate samples inherit fresh prior_delivered/prior_time
        // instead of the stale pre-outage censored state.
        p.stats = packet_state();

        // fresh pkt for this cwnd
        let considered_new_in_cwnd = if self.max_pipe_seq.is_some_and(|m| lt(m, s)) {
            self.max_pipe_seq = Some(s);
            true
        } else {
            false
        };

        let fresh_rto = self.rtt_stats.rto_duration();
        self_assign::self_assign! {
            p = InFlightPkt {
                stats: _,
                sent_time: now,
                rtxed: true,
                considered_new_in_cwnd,
                data: _,
                frame_len: _,
                rto: fresh_rto,
                rto_from_tail_probe: false,
                sacked_above: _,
                fast_loss_rtx_time: if is_fast_loss_rtx { Some(now) } else { None },
                deferred_loss_baseline_deadline: baseline_deadline_opt,
            };
        }
        if defer_loss {
            self.deferred_losses
                .insert(s, baseline_deadline_opt.unwrap());
        } else if !already_rtxed && !pre_outage_loss && !tail_probe_loss {
            let smooth_rtt = self.rtt_stats.smooth_rtt();
            self.loss_event_window.record_lost(1, now, smooth_rtt);
        }
        // After retransmission, deactivate the selected sequence so unchanged
        // SACK evidence cannot rearm fast loss, then re-activate it with the
        // fresh send time/RTO so the retransmitted copy's own deadlines are
        // still tracked (and no stale ready reason survives).
        let out_of_order = self.out_of_order_seq_end.is_some_and(|end| lt(s, end));
        self.rtx_index.deactivate(s);
        self.rtx_index.activate(RetransmissionActivation {
            seq: s,
            rto_at: now + fresh_rto,
            sent_at: now,
            apply_live_rto_floor: true,
            reorder_eligible: out_of_order,
            fast_loss_eligible: false,
            pre_outage_eligible: false,
        });
        let p = Pkt {
            seq: s,
            data: &p.data,
            frame_len: p.frame_len,
        };
        Some(p)
    }

    #[cfg(test)]
    pub fn rtx(&mut self, now: Instant) -> Option<Pkt<'_>> {
        let no_packets_in_flight = self.no_pkts_in_flight();
        let mut connection_state = dre::ConnectionState::new(now);
        self.rtx_with_state(now, || {
            connection_state.send_packet_2(now, no_packets_in_flight)
        })
    }

    /// Record any deferred CC loss-events whose stock reorder-window deadline
    /// has now elapsed and whose packet is still unacked (genuine loss).
    /// Cancelled automatically when the seq is acked (see `ack`).  This must
    /// be polled every send tick so deadlines are honoured promptly.
    pub fn poll_deferred_loss(&mut self, now: Instant) {
        if self.deferred_losses.is_empty() {
            return;
        }
        let smooth_rtt = self.rtt_stats.smooth_rtt();
        while let Some(seq) = self.deferred_losses.pop_due(now) {
            // Deadline elapsed: is the seq still in flight (genuine loss)?
            // `send_wnd.get(&seq)` returning Some(Some(_)) means the packet is
            // still unacked; if it is None or Some(None) the packet was acked
            // and `ack` already cancelled this entry — but be defensive and
            // double-check here too.
            let still_in_flight = self.send_wnd.get(&seq).and_then(|o| o.as_ref()).is_some();
            if still_in_flight {
                self.loss_event_window.record_lost(1, now, smooth_rtt);
            }
        }
    }

    pub fn min_rtt(&self) -> Option<Duration> {
        self.rtt_stats.min_rtt()
    }

    pub fn set_send_rate(&mut self, send_rate: PosR<f64>) {
        let previous_cwnd = self.cwnd;
        let cwnd = self.rtt_stats.smooth_rtt().as_secs_f64() * send_rate.get();
        let cwnd = cwnd.round() as usize;
        let cwnd = cwnd * CWND_SEND_RATE_SCALE;
        let cwnd = 1.max(cwnd);
        // While an outage-recovery epoch is open, clamp cwnd to
        // OUTAGE_RECOVERY_CWND so a just-restored path is not flooded before
        // fresh RTT samples can seed the congestion state.
        let cwnd = if self.outage.in_outage_recovery() {
            cwnd.min(OUTAGE_RECOVERY_CWND)
        } else {
            cwnd
        };
        self.cwnd = NonZeroUsize::new(cwnd).unwrap();
        if self.cwnd != previous_cwnd {
            self.resize_rtx_index_to_active_target();
        }

        let last_seq_in_cwnd = if self.outage.in_outage_recovery() {
            Self::unacked(&self.send_wnd)
                .map(|(seq, _)| seq)
                .take(cwnd.saturating_add(1))
                .last()
        } else {
            let active_tail = self.rtx_index.last_active();
            if self.rtx_index.active_count() == cwnd {
                active_tail.and_then(|tail| {
                    self.send_wnd
                        .iter_from(tail.advance(1))
                        .find_map(|(seq, packet)| packet.as_ref().map(|_| seq))
                        .or(Some(tail))
                })
            } else {
                active_tail
            }
        };

        // Retract max sequence in pipe
        if let Some(last) = last_seq_in_cwnd
            && self.max_pipe_seq.is_some_and(|m| lt(last, m))
        {
            self.max_pipe_seq = Some(last);
        }
    }

    pub fn no_pkts_in_flight(&self) -> bool {
        self.send_wnd.is_empty()
    }

    /// Number of in-flight packets eligible for the retransmission index:
    /// the whole pre-outage window while an outage-recovery epoch is open
    /// (every in-flight packet may be retransmitted without waiting for ack
    /// rounds), otherwise the exact `min(num_in_flight, cwnd)` prefix.
    pub fn active_target(&self) -> usize {
        if self.outage.in_outage_recovery() {
            self.num_in_flight
        } else {
            self.num_in_flight.min(self.cwnd.get())
        }
    }

    pub fn num_rtx_active_pkts(&self) -> usize {
        self.rtx_index.active_count()
    }

    pub fn num_rtx_ready_pkts(&self) -> usize {
        self.rtx_index.ready_count()
    }

    pub fn rto_deadline_postponements(&self) -> u64 {
        self.rto_deadline_postponements
    }

    /// One send-window traversal computing the loss ratio, the pipe depth,
    /// the retransmission count, the oldest pipe-packet age, and the maximum
    /// RTO overdue for the metrics snapshot.  Mirrors the semantics of
    /// `data_loss_rate` / `num_pkts_in_pipe` / `num_rtxed_pkts` and
    /// `pipe_rto_timing` without walking the window repeatedly.
    pub(crate) fn send_window_observation(&self, now: Instant) -> SendWindowObservation {
        let mut lost = 0;
        let mut pipe_len = 0;
        let mut retransmitted = 0;
        let mut oldest_sent: Option<Instant> = None;
        let mut max_overdue: Option<Duration> = None;
        let live_rto = self.rtt_stats.rto_duration();
        for (seq, p) in Self::unacked(&self.send_wnd) {
            let in_pipe = self.max_pipe_seq.is_some_and(|m| le(seq, m));
            if in_pipe {
                pipe_len += 1;
                let rtxed = !p.considered_new_in_cwnd && p.rtxed;
                if rtxed || p.hits_rto(now, live_rto) {
                    lost += 1;
                }
                oldest_sent =
                    Some(oldest_sent.map_or(p.sent_time, |oldest| oldest.min(p.sent_time)));
                let deadline = p.sent_time + p.rto;
                if now >= deadline {
                    let overdue = now.duration_since(deadline);
                    max_overdue = Some(max_overdue.map_or(overdue, |max| max.max(overdue)));
                }
            }
            if p.rtxed {
                retransmitted += 1;
            }
        }
        let loss_ratio = (pipe_len > 0).then_some(lost as f64 / pipe_len as f64);
        SendWindowObservation {
            loss_ratio,
            packets_in_pipe: pipe_len,
            retransmitted_packets: retransmitted,
            oldest_pipe_packet_age: oldest_sent.map(|sent| now.duration_since(sent)),
            maximum_packet_rto_overdue: max_overdue,
        }
    }

    pub fn in_outage_recovery(&self) -> bool {
        self.outage.in_outage_recovery()
    }

    // Only the outage-recovery tests read the cut; gate it to test builds
    // instead of carrying an `#[allow(dead_code)]` on the production build.
    #[cfg(test)]
    pub(crate) fn outage_cut(&self) -> Option<Instant> {
        self.outage.outage_cut()
    }

    pub(crate) fn should_censor_rate_sample(&self, prior_time: Instant) -> bool {
        self.outage.should_censor_rate_sample(prior_time)
    }

    pub fn detect_outage_recovery(&mut self, now: Instant) -> bool {
        let no_progress_for = self.liveness.no_progress_for(now);
        let rto = self.rtt_stats.rto_duration();
        let has_loss_event = self.loss_event_window.raw_has_loss_event();

        let detected = match self.outage.detect(
            now,
            self.liveness.ever_progressed,
            no_progress_for,
            rto,
            has_loss_event,
        ) {
            OutageDetection::NewEpoch => {
                self.rtt_stats.reset_rto(rto);
                self.loss_event_window
                    .reset(now, self.rtt_stats.smooth_rtt());
                true
            }
            OutageDetection::Rearming => true,
            OutageDetection::None => false,
        };
        if detected {
            // Pre-outage packets are immediately eligible for retransmission
            // once the epoch opens (or re-arms): reflect that in the index.
            self.sync_rtx_index();
        }
        detected
    }

    #[cfg(test)]
    pub fn clear_outage_recovery(&mut self) {
        self.outage.clear();
    }

    pub fn cwnd_stats(&self, now: Instant) -> CwndStats {
        let mut not_lost = 0;
        let mut all_lost_pkts_rtxed = true;
        let live_rto = self.rtt_stats.rto_duration();
        if !self.rtx_index.has_rto_due(now, live_rto) {
            return CwndStats {
                all_lost_pkts_rtxed,
                num_not_lost_in_flight_pkts: self.num_in_flight.min(self.cwnd.get()),
            };
        }
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now, live_rto) {
                all_lost_pkts_rtxed = false;
            } else {
                not_lost += 1;
            }
        }
        CwndStats {
            all_lost_pkts_rtxed,
            num_not_lost_in_flight_pkts: not_lost,
        }
    }
    pub fn num_in_flight_pkts(&self) -> usize {
        self.num_in_flight
    }

    pub fn huge_data_loss(&self, tolerant_loss_rate: UnitR<f64>, now: Instant) -> bool {
        let Some((samples, data_loss_rate)) = self.data_loss_stats(now) else {
            return false;
        };
        let enough_samples_for_stats = LOSS_RATE_MIN_SAMPLES < samples;
        enough_samples_for_stats && tolerant_loss_rate.get() < data_loss_rate
    }

    /// Single-accessor form of [`Self::send_window_metrics`]; retained as
    /// public API for external consumers of the send window.
    #[allow(dead_code)]
    pub fn data_loss_rate(&self, now: Instant) -> Option<f64> {
        self.data_loss_stats(now).map(|(_, rate)| rate)
    }

    /// One send-window traversal computing both the sample count and the loss
    /// ratio, so `huge_data_loss` no longer walks `pkts_in_pipe` a second
    /// time for its sample count.
    fn data_loss_stats(&self, now: Instant) -> Option<(usize, f64)> {
        let mut lost = 0;
        let mut len = 0;
        let live_rto = self.rtt_stats.rto_duration();
        for (_, p) in self.pkts_in_pipe() {
            len += 1;
            let rtxed = !p.considered_new_in_cwnd && p.rtxed;
            if rtxed || p.hits_rto(now, live_rto) {
                lost += 1;
            }
        }
        if len == 0 {
            return None;
        }
        Some((len, lost as f64 / len as f64))
    }

    pub fn loss_event_rate(&mut self, now: Instant) -> Option<f64> {
        self.loss_event_window.rate(now, self.smooth_rtt())
    }

    #[cfg(test)]
    pub(crate) fn inject_loss_event(&mut self, now: Instant) {
        // Use the current smooth-RTT bucket so the loss entry does not age out
        // before the next round of deliveries can accumulate.  A 1 ms bucket
        // causes a ≥2-bucket gap on the very next ack when sRTT ≫ 2 ms,
        // resetting the entire loss window and making loss_event_rate always None.
        self.loss_event_window
            .record_lost(1, now, self.smooth_rtt());
    }

    /// Single-accessor form of [`Self::send_window_metrics`]; retained as
    /// public API for external consumers of the send window.
    #[allow(dead_code)]
    pub fn num_pkts_in_pipe(&self) -> usize {
        self.pkts_in_pipe().count()
    }

    fn pkts_in_pipe(&self) -> impl Iterator<Item = (SequenceNumber, &InFlightPkt)> + '_ {
        Self::unacked(&self.send_wnd)
            .take_while(|(s, _)| self.max_pipe_seq.is_some_and(|m| le(*s, m)))
    }

    pub(crate) fn next_poll_time(&self, now: Instant) -> Option<Instant> {
        let wd_dl = self.liveness.next_deadline(!self.no_pkts_in_flight());
        let mut min_next_poll_time: Option<Instant> = wd_dl;
        if min_next_poll_time.is_some_and(|deadline| deadline <= now) {
            return min_next_poll_time;
        }
        let rtx_window = if self.jitter_cap {
            self.rtt_stats.fast_reorder_window()
        } else {
            self.rtt_stats.reorder_window()
        };
        if let Some(t) = self.rtx_index.next_deadline(rtx_window) {
            min_next_poll_time = Some(min_next_poll_time.map(|min| min.min(t)).unwrap_or(t));
        }
        if min_next_poll_time.is_some_and(|deadline| deadline <= now) {
            return min_next_poll_time;
        }
        if let Some(deadline) = self.deferred_losses.next_deadline() {
            min_next_poll_time = Some(min_next_poll_time.map_or(deadline, |min| min.min(deadline)));
        }
        if min_next_poll_time.is_some_and(|deadline| deadline <= now) {
            return min_next_poll_time;
        }
        if let Some(seq) = self.tail_seq()
            && let Some(Some(p)) = self.send_wnd.get(&seq)
            && let Some(t) = self.tlp.next_probe_time(p.sent_time, &self.rtt_stats)
        {
            min_next_poll_time = Some(min_next_poll_time.map(|min| min.min(t)).unwrap_or(t));
        }
        min_next_poll_time
    }

    pub fn rto_duration(&self) -> Duration {
        self.rtt_stats.rto_duration()
    }
}
impl Default for PktSendSpace {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SendWindowObservation {
    pub(crate) loss_ratio: Option<f64>,
    pub(crate) packets_in_pipe: usize,
    pub(crate) retransmitted_packets: usize,
    pub(crate) oldest_pipe_packet_age: Option<Duration>,
    pub(crate) maximum_packet_rto_overdue: Option<Duration>,
}

pub struct CwndStats {
    pub all_lost_pkts_rtxed: bool,
    pub num_not_lost_in_flight_pkts: usize,
}

#[derive(Debug, Clone)]
struct InFlightPkt {
    pub stats: PacketState,
    pub sent_time: Instant,
    pub rtxed: bool,
    pub considered_new_in_cwnd: bool,
    pub data: Vec<u8>,
    /// Application frame length this packet belongs to, in bytes.  `Some` only
    /// for the *first* packet of a frame in frame-delivery mode; `None` for
    /// continuation packets and for all packets when frame-delivery mode is
    /// off.  Stored so retransmissions re-encode the same framing on the wire
    /// (the first packet of a frame keeps `FRAME_DATA_TS`; a retransmit of a
    /// continuation packet keeps `DATA_TS`).
    pub frame_len: Option<u32>,
    pub rto: Duration,
    /// True only if this packet's RTO was last re-armed by a tail-loss probe
    /// using the tightened post-probe floor.  A subsequent full-RTO retransmit
    /// still fires, but it must not record a congestion loss event because the
    /// probe itself already signalled the tail episode.
    pub rto_from_tail_probe: bool,
    /// Number of newer in-flight packets that have been SACKed past this
    /// packet.  Used by the evidence-gated fast-loss path: once this reaches
    /// [`FAST_LOSS_SACK_THRESHOLD`], the packet is declared lost without
    /// waiting for the time-based reorder window to expire.
    pub sacked_above: u32,
    /// `Some(t)` if this packet was retransmitted by the evidence-gated
    /// fast-loss path at time `t`.  Used to detect observed reordering: if an
    /// ACK for this packet arrives before `t + min_rtt` could plausibly have
    /// elapsed, the original (not the retransmit) must have been delivered.
    pub fast_loss_rtx_time: Option<Instant>,
    /// `Some(t)` if this packet was retransmitted at time `t` by the
    /// jitter-tolerant fast-retransmit path (`RTP_JITTER_CAP`).  The
    /// congestion-control loss event for that retransmit is *deferred* to the
    /// stock reorder-window deadline (`sent_time_of_original + reorder_window`
    /// computed against the original send time stored in
    /// `deferred_loss_baseline_deadline`).  If the original is acked before that
    /// deadline the loss event is cancelled (it was reordering, not loss);
    /// otherwise it is recorded exactly once at the deadline.
    pub deferred_loss_baseline_deadline: Option<Instant>,
}
impl InFlightPkt {
    pub fn hits_rto(&self, now: Instant, live_rto: Duration) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        let effective_rto = if self.rto_from_tail_probe {
            self.rto
        } else {
            self.rto.max(live_rto)
        };
        effective_rto <= sent_elapsed
    }

    #[allow(dead_code)]
    pub fn next_rto_time(&self) -> Instant {
        self.sent_time + self.rto
    }

    /// Whether this packet should be declared lost by the evidence-gated
    /// fast-loss path: it has not yet been retransmitted and at least
    /// [`FAST_LOSS_SACK_THRESHOLD`] newer in-flight packets have been SACKed
    /// past it.  The structural arming gate and the observed-reordering
    /// hard-disable are checked by the caller (`PktSendSpace::fast_loss_armed`).
    pub fn is_fast_loss(&self) -> bool {
        !self.rtxed && self.sacked_above >= FAST_LOSS_SACK_THRESHOLD
    }
}

/// A pending deferred CC loss-event recording for the jitter-tolerant fast-
/// retransmit path.  The loss event for `seq` was deferred at `rtx_time` to
/// `baseline_deadline` (the original send time + stock reorder window).  When
/// [`PktSendSpace::poll_deferred_loss`] runs at `now >= baseline_deadline` it
/// records one loss event iff `seq` is still in flight (genuine loss); if the
/// seq was acked in the meantime the entry was already cancelled by `ack`.
/// Stored in a [`DeferredLossIndex`] so ACK cancellation and next-deadline
/// selection stay O(1) without rescanning the pending set.
#[derive(Debug, Clone)]
pub struct Pkt<'a> {
    pub seq: SequenceNumber,
    pub data: &'a [u8],
    /// Application frame length this packet belongs to.  `Some` only for the
    /// first packet of a frame in frame-delivery mode; the transmission layer
    /// uses this to choose `FRAME_DATA_TS` vs `DATA_TS` on the wire.  Retrans-
    /// mits carry the same value so the framing is preserved across repairs.
    pub frame_len: Option<u32>,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        CWND_SEND_RATE_SCALE, INIT_CWND, MAX_ACK_BLOCKS, OUTAGE_RECOVERY_CWND, PktSendSpace,
    };
    use crate::sequence::SequenceNumber;
    use primitive::ops::float::PosR;

    fn sq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn settle_rtt_at(space: &mut PktSendSpace, now: Instant) {
        // Saturate the SRTT filter so the RTO is stable and close to 100 ms.
        for i in 0..20 {
            space.sample_rtt(ms(100), now + ms(i));
        }
    }

    fn send_packet(space: &mut PktSendSpace, now: Instant) -> SequenceNumber {
        use dre::ConnectionState;
        let data = vec![0u8; 1];
        let stats = ConnectionState::new(now).send_packet_2(now, space.no_pkts_in_flight());
        let seq = space.next_seq();
        space.send(data, stats, None, now);
        seq
    }

    fn ack_one(space: &mut PktSendSpace, seq: u64, now: Instant) -> usize {
        // The peer received `seq` in order: its cumulative front is one past.
        let ball = crate::ack::AckInterval {
            start: SequenceNumber::from_wire(seq),
            size: std::num::NonZeroU64::new(1).unwrap(),
        };
        let balls = [ball];
        let recved = crate::ack::AckBlocks::new(SequenceNumber::from_wire(seq).advance(1), &balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, now);
        acked.len()
    }

    /// SACK a single out-of-order sequence: the peer has received `seq` but
    /// the cumulative ACK point is still below it.  This removes `seq` from
    /// the send window (it is delivered) and increments `sacked_above` on
    /// every older in-flight packet.
    fn sack_one(space: &mut PktSendSpace, seq: u64, now: Instant) -> usize {
        let mut peer = crate::ack::AckHistory::new();
        for s in 0..space.next_seq().to_wire() {
            if space
                .send_wnd
                .get(&SequenceNumber::from_wire(s))
                .and_then(|o| o.as_ref())
                .is_none()
            {
                peer.insert(SequenceNumber::from_wire(s));
            }
        }
        peer.insert(SequenceNumber::from_wire(seq));
        let balls = peer.blocks().collect::<Vec<_>>();
        let recved = crate::ack::AckBlocks::new(peer.next(), &balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, now);
        acked.len()
    }

    fn resack(space: &mut PktSendSpace, seq: u64, now: Instant) {
        let ball = crate::ack::AckInterval {
            start: SequenceNumber::from_wire(seq),
            size: std::num::NonZeroU64::new(1).unwrap(),
        };
        let balls = [ball];
        let mut acked = Vec::new();
        // The peer re-acks `seq` without advancing its cumulative front.
        space.ack(crate::ack::AckBlocks::new(sq(0), &balls), &mut acked, now);
    }

    fn sacked_above(space: &PktSendSpace, seq: u64) -> u32 {
        space
            .send_wnd
            .get(&SequenceNumber::from_wire(seq))
            .and_then(|o| o.as_ref())
            .map(|p| p.sacked_above)
            .unwrap_or(0)
    }

    fn settle_high_jitter(space: &mut PktSendSpace, now: Instant) {
        // Alternating 100 ms / 900 ms samples grow rttvar so that K*rttvar
        // dominates srtt/4 — the structural fast-loss gate stays disarmed.
        for i in 0..20 {
            space.sample_rtt(ms(100), now + ms(i * 2));
            space.sample_rtt(ms(900), now + ms(i * 2 + 1));
        }
    }

    fn scanned_cwnd_stats(space: &PktSendSpace, now: Instant) -> (bool, usize) {
        let live_rto = space.rtt_stats.rto_duration();
        let mut not_lost = 0;
        let mut all_lost_pkts_rtxed = true;
        for packet in PktSendSpace::unacked(&space.send_wnd)
            .map(|(_, packet)| packet)
            .take(space.cwnd.get())
        {
            if packet.hits_rto(now, live_rto) {
                all_lost_pkts_rtxed = false;
            } else {
                not_lost += 1;
            }
        }
        (all_lost_pkts_rtxed, not_lost)
    }

    fn assert_cwnd_stats_match_scan(space: &PktSendSpace, now: Instant) {
        let expected = scanned_cwnd_stats(space, now);
        let actual = space.cwnd_stats(now);
        assert_eq!(
            (
                actual.all_lost_pkts_rtxed,
                actual.num_not_lost_in_flight_pkts
            ),
            expected
        );
    }

    fn scanned_pipe_boundary(space: &PktSendSpace) -> Option<SequenceNumber> {
        PktSendSpace::unacked(&space.send_wnd)
            .map(|(seq, _)| seq)
            .take(space.cwnd.get().saturating_add(1))
            .last()
    }

    #[test]
    fn indexed_rto_gate_matches_the_exact_cwnd_scan() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        let sent_at = t0 + ms(1000);
        for _ in 0..INIT_CWND + 3 {
            send_packet(&mut space, sent_at);
        }
        let live_rto = space.rtt_stats.rto_duration();
        assert!(!space.rtx_index.has_rto_due(sent_at, live_rto));
        assert_cwnd_stats_match_scan(&space, sent_at);
        let due = sent_at + live_rto;
        assert!(space.rtx_index.has_rto_due(due, live_rto));
        assert_cwnd_stats_match_scan(&space, due);
        space
            .rtx_index
            .promote_due(due, space.rtt_stats.reorder_window(), live_rto);
        assert!(space.rtx_index.has_rto_due(due, live_rto));
        assert_cwnd_stats_match_scan(&space, due);
    }

    #[test]
    fn indexed_pipe_boundary_matches_the_wrapping_scan_across_a_hole() {
        let t0 = Instant::now();
        let start = sq(u64::MAX - 32);
        let mut space = PktSendSpace::new_at(start);
        settle_rtt_at(&mut space, t0);
        let rate = PosR::new(80.0).unwrap();
        space.set_send_rate(rate);
        let cwnd = space.cwnd.get();
        for _ in 0..cwnd + 3 {
            send_packet(&mut space, t0 + ms(1000));
        }
        let expected = scanned_pipe_boundary(&space);
        space.set_send_rate(rate);
        assert_eq!(space.max_pipe_seq, expected);
        assert_eq!(space.num_pkts_in_pipe(), cwnd + 1);
        let boundary = expected.unwrap();
        let block = crate::ack::AckInterval {
            start: boundary,
            size: std::num::NonZeroU64::new(1).unwrap(),
        };
        let mut acked = Vec::new();
        space.ack(
            crate::ack::AckBlocks::new(start, &[block]),
            &mut acked,
            t0 + ms(1100),
        );
        assert_eq!(acked.len(), 1);
        send_packet(&mut space, t0 + ms(1200));
        let expected = scanned_pipe_boundary(&space);
        space.set_send_rate(rate);
        assert_eq!(space.max_pipe_seq, expected);
        assert_eq!(space.num_pkts_in_pipe(), cwnd + 1);
    }

    fn lose_packet(space: &mut PktSendSpace, seq: u64, now: Instant) {
        // Record a loss event for the packet without actually transmitting it.
        // We do this by pulling the packet out, marking it as already retransmitted,
        // and then putting it back as a retransmitted copy.  The original send time
        // is preserved so it still looks like a pre-outage packet.
        let seq = SequenceNumber::from_wire(seq);
        let p = space.send_wnd.get_mut(&seq).unwrap().take().unwrap();
        let mut retransmitted = p.clone();
        retransmitted.rtxed = true;
        *space.send_wnd.get_mut(&seq).unwrap() = Some(retransmitted);
        // Record the loss event for congestion accounting.  Use a 1 ms floor for
        // the bucket length so the loss is visible immediately regardless of the
        // current (possibly 1 s seed) sRTT.
        space
            .loss_event_window
            .record_lost(1, now, Duration::from_millis(1));
    }

    fn ack_up_to(space: &mut PktSendSpace, seq: u64, now: Instant) {
        let ball = crate::ack::AckInterval {
            start: sq(0),
            size: std::num::NonZeroU64::new(seq + 1).unwrap(),
        };
        let balls = [ball];
        let recved = crate::ack::AckBlocks::new(SequenceNumber::from_wire(seq).advance(1), &balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, now);
    }

    #[test]
    fn tail_probe_fires_before_rto_and_respects_budget() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Send two packets; the second is the tail.
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));

        // No probe immediately after sending.
        assert!(!space.has_tail_probe(t0 + ms(1)));

        // First probe fires around the PTO (2*srtt = 200 ms), well before RTO.
        let t1 = t0 + ms(150);
        assert!(!space.has_tail_probe(t1), "probe should not fire at 150 ms");
        let t2 = t0 + ms(250);
        assert!(space.has_tail_probe(t2), "probe should fire by 250 ms");

        let p = space.tail_probe(t2).unwrap();
        assert_eq!(p.seq, sq(1));

        // Second probe requires another full PTO window.
        let t3 = t2 + ms(150);
        assert!(!space.has_tail_probe(t3), "second probe too early");
        let t4 = t2 + ms(250);
        assert!(space.has_tail_probe(t4), "second probe should fire");
        let p = space.tail_probe(t4).unwrap();
        assert_eq!(p.seq, sq(1));

        // Budget is exhausted after two probes.
        let t5 = t4 + ms(500);
        assert!(!space.has_tail_probe(t5), "no third probe");
        assert!(space.tail_probe(t5).is_none());
    }

    #[test]
    fn tail_probe_budget_resets_on_ack_progress_and_new_send() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));

        let t1 = t0 + ms(250);
        let _ = space.tail_probe(t1);

        // ACK progress resets the budget.
        ack_up_to(&mut space, 1, t1 + ms(1));
        // Window is now empty; no tail, so has_tail_probe is false.
        assert!(!space.has_tail_probe(t1 + ms(1)));

        // New tail after sending again starts fresh.
        send_packet(&mut space, t1 + ms(2));
        assert!(!space.has_tail_probe(t1 + ms(2)));
        assert!(space.has_tail_probe(t1 + ms(252)));
    }

    #[test]
    fn tail_probe_abstains_when_all_acked_or_before_first_rtt_sample() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();

        // Without any RTT sample the RTO defaults to 1 s, so the PTO (capped at
        // RTO) does not fire at 900 ms.
        send_packet(&mut space, t0);
        assert!(!space.has_tail_probe(t0 + ms(900)));
        assert!(space.tail_probe(t0 + ms(900)).is_none());

        // After all packets are acked there is no tail and no probe.
        ack_up_to(&mut space, 0, t0 + ms(1));
        assert!(!space.has_tail_probe(t0 + ms(900)));
        assert!(space.tail_probe(t0 + ms(900)).is_none());
    }

    #[test]
    fn the_outage_cut_censors_late_rate_samples_after_the_epoch_closes() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        let pre_outage = t0 + ms(5);
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(10));
        send_packet(&mut space, t0 + ms(11));
        lose_packet(&mut space, 1, t0 + ms(50));
        let detect_at = t0 + ms(10) + space.rto_duration() + ms(1);
        assert!(space.detect_outage_recovery(detect_at), "epoch must open");
        assert!(
            space.should_censor_rate_sample(pre_outage),
            "a blackout-spanning sample must be censored while the epoch is open"
        );
        space.sample_rtt(ms(200), detect_at + ms(300));
        assert!(!space.in_outage_recovery(), "epoch must have closed");
        assert!(
            space.should_censor_rate_sample(pre_outage),
            "a blackout-spanning sample must stay censored after the epoch closes"
        );
        assert!(
            !space.should_censor_rate_sample(detect_at + ms(1)),
            "a post-cut sample must not be censored"
        );
    }

    #[test]
    fn outage_recovery_resets_state_and_censors_stale_samples() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Send 3 packets in flight, make one packet's worth of progress, and
        // mark another as lost so the epoch has a loss event to trigger on.
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        ack_one(&mut space, 0, t0 + ms(10));
        lose_packet(&mut space, 2, t0 + ms(50));

        // Advance at least one RTO past the last forward-progress so the epoch
        // can be detected.
        let detect_at = t0 + ms(10) + space.rto_duration() + ms(1);
        assert!(space.detect_outage_recovery(detect_at));
        assert!(space.in_outage_recovery());
        // Recovery starts by clamping cwnd to OUTAGE_RECOVERY_CWND once
        // set_send_rate is recomputed with the epoch active.
        space.set_send_rate(PosR::new(crate::reliable::reliable_layer::INIT_SEND_RATE).unwrap());
        assert_eq!(space.cwnd().get(), OUTAGE_RECOVERY_CWND);
        // RTO is reset to the prior RTO value (acts as the seed).
        let seed_rto = space.rto_duration();

        // The unacked pre-outage packets are now immediately eligible for rtx
        // and are not counted as congestion losses.  seq 1 was marked lost by the
        // helper but is at the front of the window; it should still be exempt.
        let rtx = space.rtx(t0 + ms(600)).unwrap();
        assert!(
            rtx.seq == sq(1) || rtx.seq == sq(2),
            "expected pre-outage rtx, got {rtx_seq}",
            rtx_seq = rtx.seq
        );
        let after_rtx = space.loss_event_rate(t0 + ms(600));
        assert!(
            after_rtx.is_none() || after_rtx.unwrap() == 0.0,
            "pre-outage loss should not count"
        );

        // A stale RTT sample (computed sent_at would be before recovery_start)
        // is ignored and the epoch stays open.
        space.sample_rtt(ms(300), detect_at + ms(100));
        assert!(space.in_outage_recovery());
        // The stale sample did not shrink the RTO.
        assert_eq!(space.rto_duration(), seed_rto);

        // A fresh post-outage sample (sent_at after outage_cut) ends the epoch
        // and seeds sRTT.  It must be measured at or after outage_cut + rtt so
        // the computed original send time is within the post-outage window.
        let fresh = detect_at + ms(300);
        space.sample_rtt(ms(200), fresh);
        assert!(!space.in_outage_recovery());
        assert_eq!(space.smooth_rtt(), ms(200));

        // The outage_loss_cut remains after the epoch closes so that late echoes
        // of pre-outage packets are still censored.
        space.sample_rtt(Duration::from_secs(11), fresh + ms(100));
        assert_eq!(
            space.smooth_rtt(),
            ms(200),
            "late pre-outage echo must be censored"
        );

        // CWND is no longer forced to OUTAGE_RECOVERY_CWND; set_send_rate
        // recomputes it.
        space.set_send_rate(PosR::new(128.0).unwrap());
        assert!(
            space.cwnd().get() > OUTAGE_RECOVERY_CWND,
            "fresh RTT must release the outage-recovery CWND clamp"
        );
    }

    #[test]
    fn outage_recovery_clamps_cwnd_until_fresh_sample() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Establish a large cwnd by setting a high send rate.
        space.set_send_rate(PosR::new(1_000_000.0).unwrap());
        let large_cwnd = space.cwnd().get();
        assert!(large_cwnd > OUTAGE_RECOVERY_CWND, "cwnd={large_cwnd}");

        // Trigger outage recovery: make progress, send a second packet, then
        // mark that second packet as lost so a loss event exists while the
        // first packet remains acked.  Wait one RTO from the last progress (the
        // ack at t0 + 10 ms).
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(10));
        send_packet(&mut space, t0 + ms(11));
        lose_packet(&mut space, 1, t0 + ms(50));
        let detect_at = t0 + ms(10) + space.rto_duration() + ms(1);
        let no_progress = space.no_progress_for(detect_at);
        assert!(
            space.detect_outage_recovery(detect_at),
            "detection should fire; no_progress={no_progress:?}"
        );
        space.set_send_rate(PosR::new(crate::reliable::reliable_layer::INIT_SEND_RATE).unwrap());
        assert_eq!(
            space.cwnd().get(),
            OUTAGE_RECOVERY_CWND,
            "cwnd should clamp to OUTAGE_RECOVERY_CWND"
        );

        // After a fresh sample, cwnd returns to the rate-based formula using the
        // newly seeded sRTT (200 ms), not the pre-outage 100 ms baseline.
        space.sample_rtt(ms(200), detect_at + ms(300));
        assert!(!space.in_outage_recovery());
        space.set_send_rate(PosR::new(1_000_000.0).unwrap());
        let expected_cwnd =
            (ms(200).as_secs_f64() * 1_000_000.0).round() as usize * CWND_SEND_RATE_SCALE;
        assert_eq!(
            space.cwnd().get(),
            expected_cwnd,
            "cwnd should recompute with fresh sRTT"
        );
    }

    #[test]
    fn outage_epoch_refreshes_while_still_open() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Enter an epoch via loss + one-RTO stall, then make a brief ack-progress
        // blip that is shorter than one RTT.  The epoch stays open because no fresh
        // post-cut RTT sample has arrived.
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(10));
        send_packet(&mut space, t0 + ms(11));
        lose_packet(&mut space, 1, t0 + ms(50));
        let detect_at = t0 + ms(10) + space.rto_duration() + ms(1);
        assert!(space.detect_outage_recovery(detect_at));
        assert!(space.in_outage_recovery());
        let first_cut = space.outage_cut().unwrap();

        // Short progress blip: an immediate re-check must still require the full
        // stall conditions, so it returns false.
        ack_one(&mut space, 1, detect_at + ms(1));
        assert!(
            !space.detect_outage_recovery(detect_at + ms(2)),
            "re-check right after progress must require full stall conditions"
        );
        assert!(space.in_outage_recovery());

        // A second outage-length stall re-arms the epoch and refreshes the cut.
        send_packet(&mut space, detect_at + ms(2));
        let detect_at2 = detect_at + ms(2) + space.rto_duration() * 2 + ms(1);
        assert!(
            space.detect_outage_recovery(detect_at2),
            "flapping outage should refresh while still open"
        );
        assert!(space.in_outage_recovery());
        assert_eq!(
            space.outage_cut(),
            Some(detect_at2),
            "cut must move to the new detect time, not the first cut"
        );
        assert_ne!(
            space.outage_cut(),
            Some(first_cut),
            "cut must have been refreshed"
        );
    }

    #[test]
    fn outage_epoch_requires_prior_progress() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Send a packet and mark it as lost, but never ack anything: no forward
        // progress has been made, so the epoch should not fire.
        send_packet(&mut space, t0);
        lose_packet(&mut space, 0, t0 + ms(50));
        let detect_at = t0 + space.rto_duration() + ms(1);
        assert!(
            !space.detect_outage_recovery(detect_at),
            "outage detection should require prior progress"
        );

        // After a single ACK creates progress, detection can fire after another
        // RTO of stall.  Send a new packet, ack the original packet so progress
        // is recorded on the new flight, then wait two RTOs (no loss event).
        send_packet(&mut space, detect_at);
        ack_one(&mut space, 0, detect_at + ms(1));
        let detect_at2 = detect_at + ms(1) + space.rto_duration() * 2 + ms(1);
        assert!(
            space.detect_outage_recovery(detect_at2),
            "progress should allow detection"
        );
    }

    #[test]
    fn outage_epoch_is_reentrant_for_flapping_links() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Establish progress and enter the first epoch.  Keep seq 1 in flight
        // by only acking seq 0.  The loss event needs to be recorded after
        // progress so it is not reset by the progress ack.
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(10));
        send_packet(&mut space, t0 + ms(11));
        lose_packet(&mut space, 1, t0 + ms(50));
        let detect_at = t0 + ms(10) + space.rto_duration() + ms(1);
        assert!(
            space.detect_outage_recovery(detect_at),
            "first epoch should fire"
        );
        space.clear_outage_recovery();
        assert!(!space.in_outage_recovery());
        assert!(space.outage_cut().is_some());

        // A second outage-length stall re-enters recovery with a refreshed cut.
        // There must be a packet in flight for progress_wait_start to stay set.
        // For the second outage to be detected there must be a packet in flight
        // so progress_wait_start stays set after clear_outage_recovery.
        send_packet(&mut space, detect_at + ms(1));
        let detect_at2 = detect_at + ms(1) + space.rto_duration() * 2 + ms(1);
        assert!(
            space.detect_outage_recovery(detect_at2),
            "flapping outage should re-enter recovery"
        );
        assert!(space.in_outage_recovery());
        assert_eq!(space.outage_cut(), Some(detect_at2));
    }

    #[test]
    fn outage_detected_despite_zero_progress_acks() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Make progress with one packet, then send two more so there are
        // packets in flight when the duplicate ACKs arrive.
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(10));
        send_packet(&mut space, t0 + ms(11));
        send_packet(&mut space, t0 + ms(12));

        // Feed 11 s of duplicate zero-progress ACKs (acks only seq 0, which is
        // already delivered).  The resp_wait_start resets on every ACK, but
        // progress_wait_start stays at the last real progress.
        let mut t = t0 + ms(13);
        let rto = space.rto_duration();
        for _ in 0..110 {
            ack_one(&mut space, 0, t);
            t += rto / 10;
        }

        // Forward progress stalled since t0 + 10 ms; detection should fire.
        // Because the window is non-empty, resp_wait_start is still advancing.
        let no_resp = space.no_resp_for(t).unwrap();
        assert!(no_resp < rto, "zero-progress ACKs keep resp_wait alive");
        let no_progress = space.no_progress_for(t).unwrap();
        assert!(no_progress >= rto, "no_progress={no_progress:?}");
        assert!(
            space.detect_outage_recovery(t),
            "zero-progress ACKs should not prevent outage detection"
        );
    }

    #[test]
    fn post_tlp_rtx_does_not_arm_spurious_outage_on_delay_spike() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Make forward progress so outage detection is eligible, then send a
        // new tail packet whose episode we will probe and eventually retransmit.
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));

        // First tail-loss probe fires around 2*srtt (~200 ms) and rearms with
        // the tightened post-probe RTO floor.
        let t1 = t0 + ms(210);
        assert!(space.has_tail_probe(t1), "first probe should be due");
        let p1 = space.tail_probe(t1).unwrap();
        assert_eq!(p1.seq, sq(1));

        // Second probe fires after another PTO window.
        let t2 = t1 + ms(210);
        assert!(space.has_tail_probe(t2), "second probe should be due");
        let p2 = space.tail_probe(t2).unwrap();
        assert_eq!(p2.seq, sq(1));

        // Full-RTO retransmit off the lowered floor fires around 300 ms after the
        // second probe.  It must not record a congestion loss event because the
        // tail probe already owns the tail-latency signal.
        let t3 = t2 + ms(330);
        let rtx = space
            .rtx(t3)
            .expect("full RTO should fire after tail probes");
        assert_eq!(rtx.seq, sq(1));
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "post-TLP full-RTO rtx must not arm a loss event"
        );

        // A single RTO of stall is not enough to declare outage because there is
        // no loss event and the silence has not yet reached 2*RTO.
        let t4 = t0 + ms(1200);
        assert!(
            !space.detect_outage_recovery(t4),
            "delay spike after tail probes must not trigger spurious outage"
        );

        // After two RTOs of silence the clean-link path still declares outage.
        let t5 = t0 + ms(2010);
        assert!(
            space.detect_outage_recovery(t5),
            "genuine long stall should still declare outage"
        );
    }

    #[test]
    fn full_rto_rtx_still_records_loss_event() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Make progress so outage detection is eligible, then let the next packet
        // time out and retransmit on the full RTO.
        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));

        // Wait for the full RTO (1 s floor on steady low-RTT path) and retransmit.
        let rtx_t = t0 + ms(2) + space.rto_duration() + ms(1);
        let rtx = space.rtx(rtx_t).expect("RTO should fire");
        assert_eq!(rtx.seq, sq(1));

        // A non-tail-probe retransmit must record the loss event for congestion
        // accounting and outage detection.
        assert!(
            space.loss_event_window.raw_has_loss_event(),
            "plain RTO rtx must record a loss event"
        );
    }

    #[test]
    fn fast_loss_declares_before_reorder_window_with_sack_count_evidence() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Low-jitter link: K*rttvar < srtt/4, so the structural gate is armed.
        assert!(
            space.fast_loss_armed(),
            "gate should be armed on a low-jitter link"
        );

        // Send four packets; seq 0 will be the one we starve of ACKs.
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        send_packet(&mut space, t0 + ms(3));

        // SACK seqs 1, 2, 3 — each is newer than seq 0, so each SACK
        // increments seq 0's sacked_above by 1.  After three SACKs the
        // threshold (3) is reached.
        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));
        assert_eq!(
            sacked_above(&space, 0),
            3,
            "seq 0 should have 3 sack passes"
        );

        // At t0 + 30 ms the time-based reorder window (~125 ms) has NOT
        // expired, so a stock time-only declaration would not fire.  The
        // evidence-gated fast-loss path must declare seq 0 lost anyway.
        let early = t0 + ms(30);
        assert!(
            early.duration_since(t0) < space.smooth_rtt(),
            "test must run before the reorder window expires"
        );
        assert!(
            space.has_rtx(early),
            "fast loss should make has_rtx true before the reorder window"
        );
        let rtx = space
            .rtx(early)
            .expect("fast loss should retransmit seq 0 before the reorder window");
        assert_eq!(rtx.seq, sq(0), "fast loss should target the starved seq 0");
    }

    #[test]
    fn duplicate_acks_carrying_no_new_information_do_not_earn_sacked_above() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        assert!(space.fast_loss_armed(), "gate should be armed");
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        send_packet(&mut space, t0 + ms(3));
        sack_one(&mut space, 1, t0 + ms(10));
        resack(&mut space, 1, t0 + ms(11));
        resack(&mut space, 1, t0 + ms(12));
        assert_eq!(
            sacked_above(&space, 0),
            1,
            "one newer packet delivered is one sack pass, however often it is re-acked"
        );
        let early = t0 + ms(30);
        assert!(
            early.duration_since(t0) < space.smooth_rtt(),
            "test must run before the reorder window expires"
        );
        assert!(
            !space.has_rtx(early),
            "duplicate acks must not declare a fast loss"
        );
        assert!(
            space.rtx(early).is_none(),
            "duplicate acks must not retransmit a merely reordered packet"
        );
    }

    #[test]
    fn fast_loss_stays_off_under_structural_jitter_gate() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_high_jitter(&mut space, t0);

        // High-jitter link: K*rttvar dominates srtt/4, so the structural gate
        // is disarmed — reordering can mimic loss, so the fast path must stay
        // off and only the stock time-based declaration is allowed.
        assert!(
            !space.fast_loss_armed(),
            "gate should be disarmed on a high-jitter link"
        );

        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        send_packet(&mut space, t0 + ms(3));

        // Same SACK evidence as the low-jitter case: three newer packets
        // SACKed past seq 0.  The sacked_above counter still reaches 3
        // (tracking is unconditional), but the declaration must not fire.
        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));
        assert_eq!(sacked_above(&space, 0), 3, "sack passes are always tracked");

        // Well before the (large, jitter-dominated) reorder window expires.
        let early = t0 + ms(30);
        assert!(
            !space.has_rtx(early),
            "fast loss must stay off when the structural gate is disarmed"
        );
        assert!(
            space.rtx(early).is_none(),
            "no retransmit should fire under high jitter before the reorder window"
        );
    }

    #[test]
    fn rtt_gate_transition_resyncs_existing_fast_loss_evidence() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_high_jitter(&mut space, t0);
        assert!(!space.fast_loss_armed(), "high-jitter gate starts disarmed");
        let sent_at = t0 + ms(1_000);
        for offset in 0..4 {
            send_packet(&mut space, sent_at + ms(offset));
        }
        sack_one(&mut space, 1, sent_at + ms(10));
        sack_one(&mut space, 2, sent_at + ms(11));
        sack_one(&mut space, 3, sent_at + ms(12));
        assert_eq!(sacked_above(&space, 0), 3);
        let early = sent_at + ms(30);
        assert!(
            !space.has_rtx(early),
            "fast-loss evidence must stay dormant while the structural gate is disarmed"
        );
        for _ in 0..256 {
            space.sample_rtt(ms(100), early);
            if space.fast_loss_armed() {
                break;
            }
        }
        assert!(
            space.fast_loss_armed(),
            "stable RTT samples must re-arm the gate"
        );
        assert!(
            space.has_rtx(early),
            "the gate transition must publish existing fast-loss evidence"
        );
        assert_eq!(
            space.rtx(early).expect("fast loss becomes ready").seq,
            sq(0)
        );
    }

    #[test]
    fn observed_reordering_hard_disables_fast_loss() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        // min_rtt is established by settle_rtt_at (100 ms) — needed for the
        // observed-reordering detection (original arrives before the
        // retransmit could plausibly be acked).
        assert_eq!(space.min_rtt(), Some(ms(100)));
        assert!(space.fast_loss_armed(), "gate armed on low-jitter link");

        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        send_packet(&mut space, t0 + ms(3));

        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));

        // Fast-loss retransmit of seq 0 fires before the reorder window.
        let rtx_t = t0 + ms(30);
        let rtx = space.rtx(rtx_t).expect("fast loss should fire for seq 0");
        assert_eq!(rtx.seq, sq(0));
        // The retransmit is recorded as a fast-loss rtx for reordering检测.
        assert_eq!(
            space
                .send_wnd
                .get(&sq(0))
                .and_then(|o| o.as_ref())
                .unwrap()
                .fast_loss_rtx_time,
            Some(rtx_t),
            "fast-loss rtx must stamp fast_loss_rtx_time"
        );

        // The original (not the retransmit) arrives: an ACK for seq 0 lands
        // at rtx_t + 1 ms, which is before rtx_t + min_rtt (100 ms) — the
        // retransmit could not have made a round trip that fast, so the peer
        // must have received the original.  Reordering is observed: hard-
        // disable fast loss for the rest of the connection.
        let original_arrives = rtx_t + ms(1);
        assert!(original_arrives < rtx_t + space.min_rtt().unwrap());
        ack_one(&mut space, 0, original_arrives);
        assert!(
            space.fast_loss_disabled(),
            "observed reordering must hard-disable fast loss"
        );
        assert!(
            !space.fast_loss_armed(),
            "fast loss must be disarmed after observed reordering"
        );

        // A subsequent flight with identical SACK evidence must NOT trigger a
        // fast-loss retransmit, even though the structural jitter gate alone
        // would still be armed.
        send_packet(&mut space, original_arrives + ms(1));
        send_packet(&mut space, original_arrives + ms(2));
        send_packet(&mut space, original_arrives + ms(3));
        send_packet(&mut space, original_arrives + ms(4));
        // seq 4 is the new starved packet (front of the new flight).
        let new_front = 4u64;
        sack_one(&mut space, new_front + 1, original_arrives + ms(6));
        sack_one(&mut space, new_front + 2, original_arrives + ms(7));
        sack_one(&mut space, new_front + 3, original_arrives + ms(8));
        assert_eq!(sacked_above(&space, new_front), 3);

        let early2 = original_arrives + ms(9);
        assert!(
            !space.has_rtx(early2),
            "fast loss must not fire after observed reordering disabled it"
        );
        assert!(
            space.rtx(early2).is_none(),
            "no fast-loss retransmit after observed reordering"
        );
    }

    #[test]
    fn fast_loss_rtx_records_loss_event_without_tlp_probe_accounting() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        assert!(space.fast_loss_armed());

        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        send_packet(&mut space, t0 + ms(3));

        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));

        // No loss event yet — only SACK evidence, no declaration so far.
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event before the fast-loss rtx"
        );

        let rtx_t = t0 + ms(30);
        let rtx = space.rtx(rtx_t).expect("fast loss should fire for seq 0");
        assert_eq!(rtx.seq, sq(0));

        // A fast-loss retransmit is a genuine loss declaration, not a TLP
        // probe, so it must record a congestion loss event exactly as a
        // window-expiry loss would — feeding delivery-rate CC the true loss
        // rate.
        assert!(
            space.loss_event_window.raw_has_loss_event(),
            "fast-loss rtx must record a loss event for CC"
        );

        // And it must not be accounted as a tail-loss probe: the retransmitted
        // packet carries no TLP marker.
        let p = space
            .send_wnd
            .get(&sq(0))
            .and_then(|o| o.as_ref())
            .expect("seq 0 still in flight after rtx");
        assert!(
            !p.rto_from_tail_probe,
            "fast-loss rtx must not carry TLP probe accounting"
        );
        assert!(p.rtxed, "fast-loss rtx must mark the packet rtxed");
        assert_eq!(
            p.fast_loss_rtx_time,
            Some(rtx_t),
            "fast-loss rtx must stamp fast_loss_rtx_time"
        );
    }

    #[test]
    fn jitter_cap_defaults_on_and_accepts_an_explicit_diagnostic_disable() {
        assert!(super::jitter_cap_enabled(None));
        assert!(super::jitter_cap_enabled(Some("1")));
        assert!(super::jitter_cap_enabled(Some("true")));
        assert!(!super::jitter_cap_enabled(Some("0")));
        assert!(!super::jitter_cap_enabled(Some("false")));
        assert!(!super::jitter_cap_enabled(Some("FALSE")));
    }

    // ---- jitter-tolerant fast-retransmit (RTP_JITTER_CAP) tests ----
    //
    // These tests force the jitter-cap toggle to a fixed value via `with_jitter_cap`
    // so they do not race on the process env var with parallel tests.

    /// High-jitter link where the fast reorder window is strictly below the
    /// stock window.  Returns the `(fast_window, stock_window)` pair so tests
    /// can pick deadlines on either side.
    fn settle_high_jitter_f2(space: &mut PktSendSpace, t0: Instant) -> (Duration, Duration) {
        settle_high_jitter(space, t0);
        let fast = space.rtt_stats.fast_reorder_window();
        let stock = space.rtt_stats.reorder_window();
        assert!(
            fast < stock,
            "test requires fast < stock window; fast={fast:?} stock={stock:?}"
        );
        (fast, stock)
    }

    /// Send seq 0..3 and SACK seqs 1, 2, 3 so seq 0 is out-of-order-passed
    /// and the reorder-window path is eligible for it.
    fn starve_seq0_with_sacks(space: &mut PktSendSpace, t0: Instant) {
        send_packet(space, t0);
        send_packet(space, t0 + ms(1));
        send_packet(space, t0 + ms(2));
        send_packet(space, t0 + ms(3));
        sack_one(space, 1, t0 + ms(10));
        sack_one(space, 2, t0 + ms(11));
        sack_one(space, 3, t0 + ms(12));
        // seq 0 is below out_of_order_seq_end (= 1, the highest SACKed
        // block start), so the reorder-window path applies to it.
        assert!(space.out_of_order_seq_end.is_some());
    }

    #[test]
    fn a_peer_cannot_report_a_gap_past_what_was_sent() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        for _ in 0..3 {
            send_packet(&mut space, t0);
        }
        let balls = [crate::ack::AckInterval {
            start: sq(u64::MAX),
            size: std::num::NonZeroU64::new(1).unwrap(),
        }];
        let mut acked = Vec::new();
        space.ack(
            crate::ack::AckBlocks::new(sq(0), &balls),
            &mut acked,
            t0 + ms(1),
        );
        assert!(
            space
                .out_of_order_seq_end
                .is_none_or(|end| crate::sequence::le(end, space.next_seq())),
            "the peer moved the gap bound to {:?} with only {} sequences sent",
            space.out_of_order_seq_end,
            space.next_seq(),
        );
        assert!(
            !space.has_rtx(t0 + ms(300)),
            "the whole send window went into fast retransmit on one bogus ack"
        );
    }

    #[test]
    fn a_ball_spanning_the_whole_space_is_no_evidence_of_loss() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        for _ in 0..3 {
            send_packet(&mut space, t0);
        }
        let balls = [crate::ack::AckInterval {
            start: sq(3),
            size: std::num::NonZeroU64::new(u64::MAX).unwrap(),
        }];
        let mut acked = Vec::new();
        space.ack(
            crate::ack::AckBlocks::new(sq(0), &balls),
            &mut acked,
            t0 + ms(1),
        );
        for s in 0..3 {
            assert_eq!(
                sacked_above(&space, s),
                0,
                "seq {s} was credited with newer deliveries the peer never received"
            );
        }
    }

    #[test]
    fn jitter_cap_early_reorder_rtx_skips_loss_event_at_rtx_time() {
        let t0 = Instant::now();
        // Jitter-cap toggle ON, high-jitter link: fast window < stock window.
        let mut space = PktSendSpace::with_jitter_cap(true);
        let (fast, stock) = settle_high_jitter_f2(&mut space, t0);
        assert!(!space.fast_loss_armed(), "high-jitter gate disarmed");

        starve_seq0_with_sacks(&mut space, t0);

        // Pick a time strictly after the fast window but strictly before the
        // stock window for the original send of seq 0 (sent at t0).
        let fast_deadline = t0 + fast;
        let baseline_deadline = t0 + stock;
        let rtx_t = fast_deadline + ms(50);
        assert!(
            rtx_t < baseline_deadline,
            "rtx_t must be before stock deadline"
        );

        // No loss event before the rtx.
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event before rtx"
        );

        // The fast rtx must fire (toggle on → fast window governs rtx timing).
        assert!(
            space.has_rtx(rtx_t),
            "jitter-cap fast window must make has_rtx true before stock deadline"
        );
        let rtx = space
            .rtx(rtx_t)
            .expect("jitter-cap fast rtx must fire before the stock reorder window");
        assert_eq!(rtx.seq, sq(0));

        // *** The loss event must NOT be recorded at rtx time — it is deferred
        // to the stock deadline.  Recording it here is the goodput-collapse
        // bug. ***
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "jitter-cap fast rtx must NOT record a loss event at rtx time (deferred)"
        );

        // The deferred entry must be pending with the stock deadline.
        assert_eq!(
            space.deferred_losses.len(),
            1,
            "one deferred loss entry pending"
        );
        assert_eq!(
            space.deferred_losses.deadline(sq(0)),
            Some(baseline_deadline)
        );
    }

    #[test]
    fn jitter_cap_deferred_loss_event_records_at_baseline_deadline_if_still_unacked() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::with_jitter_cap(true);
        let (fast, stock) = settle_high_jitter_f2(&mut space, t0);

        starve_seq0_with_sacks(&mut space, t0);

        let fast_deadline = t0 + fast;
        let baseline_deadline = t0 + stock;
        let rtx_t = fast_deadline + ms(50);
        let rtx = space.rtx(rtx_t).expect("jitter-cap fast rtx fires");
        assert_eq!(rtx.seq, sq(0));
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event at rtx time (deferred)"
        );

        // Still before the stock deadline: polling must not record anything.
        let before_deadline = baseline_deadline - ms(10);
        space.poll_deferred_loss(before_deadline);
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event before stock deadline"
        );
        assert_eq!(
            space.deferred_losses.len(),
            1,
            "deferred entry still pending before stock deadline"
        );

        // At the stock deadline, the packet is still unacked (genuine loss):
        // the deferred loss event must now be recorded exactly once.
        space.poll_deferred_loss(baseline_deadline + ms(1));
        assert!(
            space.loss_event_window.raw_has_loss_event(),
            "deferred loss event must record at stock deadline if still unacked"
        );
        assert_eq!(
            space.deferred_losses.len(),
            0,
            "deferred entry cleared after recording"
        );
    }

    #[test]
    fn jitter_cap_repaired_before_baseline_deadline_is_not_double_counted() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::with_jitter_cap(true);
        let (fast, stock) = settle_high_jitter_f2(&mut space, t0);

        starve_seq0_with_sacks(&mut space, t0);

        let fast_deadline = t0 + fast;
        let baseline_deadline = t0 + stock;
        let rtx_t = fast_deadline + ms(50);
        let rtx = space.rtx(rtx_t).expect("jitter-cap fast rtx fires");
        assert_eq!(rtx.seq, sq(0));
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event at rtx time (deferred)"
        );
        assert_eq!(space.deferred_losses.len(), 1);

        // The original (or the repair) is acked before the stock deadline:
        // this was reordering, not loss.  The deferred entry must be cancelled
        // and no loss event recorded.  Polling after the deadline must NOT
        // double-count a later repair either.
        let ack_t = baseline_deadline - ms(100);
        assert!(ack_t > rtx_t);
        let acked = ack_one(&mut space, 0, ack_t);
        assert_eq!(acked, 1, "seq 0 must be acked");

        // The ack cancels the deferred entry.
        assert_eq!(
            space.deferred_losses.len(),
            0,
            "deferred entry cancelled by ack before stock deadline"
        );
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "reordering must not record a loss event"
        );

        // Polling after the stock deadline must not record anything (the
        // entry is gone, no double-count).
        space.poll_deferred_loss(baseline_deadline + ms(1));
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no double-count after repair before stock deadline"
        );
        assert_eq!(space.deferred_losses.len(), 0);
    }

    #[test]
    fn jitter_cap_toggle_off_preserves_stock_out_of_order_window() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::with_jitter_cap(false);
        let (_fast, stock) = settle_high_jitter_f2(&mut space, t0);
        let stock_window = space.rtt_stats.reorder_window();
        assert_eq!(stock_window, stock);

        starve_seq0_with_sacks(&mut space, t0);

        let fast = space.rtt_stats.fast_reorder_window();
        let fast_deadline = t0 + fast;
        let baseline_deadline = t0 + stock;
        let between = fast_deadline + ms(50);
        assert!(between < baseline_deadline);
        assert!(
            !space.has_rtx(between),
            "toggle off: no rtx before the stock reorder window"
        );
        assert!(
            space.rtx(between).is_none(),
            "toggle off: no retransmit before the stock reorder window"
        );

        assert_eq!(
            space.deferred_losses.len(),
            0,
            "toggle off: no deferred loss entries"
        );

        let rtx_t = baseline_deadline + ms(1);
        assert!(
            space.has_rtx(rtx_t),
            "toggle off: stock rtx fires at stock deadline"
        );
        let rtx = space.rtx(rtx_t).expect("stock rtx at stock deadline");
        assert_eq!(rtx.seq, sq(0));
        assert!(
            space.loss_event_window.raw_has_loss_event(),
            "toggle off: stock rtx records loss event immediately (no deferral)"
        );
        assert_eq!(
            space.deferred_losses.len(),
            0,
            "toggle off: no deferred entries created"
        );
    }

    #[test]
    fn zero_progress_stall_declares_broken_pipe() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        let rto = space.rto_duration();

        send_packet(&mut space, t0);
        ack_one(&mut space, 0, t0 + ms(10));

        let send_start = t0 + ms(11);
        for i in 0..22 {
            send_packet(&mut space, send_start + ms(i));
        }

        let mut t = send_start + ms(22 + 1);
        for _ in 0..320 {
            ack_one(&mut space, 0, t);
            t += ms(100);
        }

        let no_resp = space.no_resp_for(t);
        let no_progress = space.no_progress_for(t);
        assert!(
            no_resp.is_some_and(|d| d < rto.mul_f64(16.0)),
            "duplicate ACKs keep resp_wait short; no_resp={no_resp:?}"
        );
        assert!(
            no_progress.is_some_and(|d| d >= Duration::from_secs(30)),
            "zero-progress stall must exceed 30s; no_progress={no_progress:?}"
        );

        assert!(
            space.should_terminate_session(t),
            "stalled-but-responding peer must be declared broken pipe"
        );

        for seq in 1..23 {
            ack_one(&mut space, seq, t + ms(seq));
        }
        assert_eq!(
            space.no_progress_for(t + Duration::from_secs(600)),
            None,
            "drained idle window must report no progress wait"
        );
    }

    #[test]
    fn fresh_sacks_behind_permanent_hole_do_not_reset_cumulative_progress() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        assert_eq!(send_packet(&mut space, t0), sq(0));
        assert_eq!(ack_one(&mut space, 0, t0 + ms(10)), 1);
        assert_eq!(send_packet(&mut space, t0 + ms(11)), sq(1));
        let mut last_ack = t0 + ms(11);
        for i in 0..8 {
            let send_at = t0 + ms(12) + Duration::from_secs(i * 5);
            let seq = send_packet(&mut space, send_at);
            let balls = [
                crate::ack::AckInterval {
                    start: sq(0),
                    size: std::num::NonZeroU64::new(1).unwrap(),
                },
                crate::ack::AckInterval {
                    start: sq(2),
                    size: std::num::NonZeroU64::new(seq.to_wire() - 1).unwrap(),
                },
            ];
            let mut acked = Vec::new();
            last_ack = send_at + ms(1);
            space.ack(
                crate::ack::AckBlocks::new(sq(1), &balls),
                &mut acked,
                last_ack,
            );
            assert_eq!(acked.len(), 1, "each heartbeat SACK must be fresh");
            assert_eq!(space.send_wnd.start(), sq(1));
        }
        let no_resp = space.no_resp_for(last_ack);
        let no_progress = space.no_progress_for(last_ack);
        assert_eq!(no_resp, Some(Duration::ZERO));
        assert!(
            no_progress.is_some_and(|d| d > Duration::from_secs(30)),
            "fresh SACKs beyond the hole must not reset cumulative progress; no_progress={no_progress:?}"
        );
        assert!(space.should_terminate_session(last_ack));
    }

    #[test]
    fn fresh_sacks_behind_initial_hole_age_cumulative_progress() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        assert_eq!(send_packet(&mut space, t0), sq(0));
        let mut last_ack = t0;
        for i in 0..8 {
            let send_at = t0 + ms(1) + Duration::from_secs(i * 5);
            let seq = send_packet(&mut space, send_at);
            let balls = [crate::ack::AckInterval {
                start: sq(1),
                size: std::num::NonZeroU64::new(seq.to_wire()).unwrap(),
            }];
            let mut acked = Vec::new();
            last_ack = send_at + ms(1);
            space.ack(
                crate::ack::AckBlocks::new(sq(0), &balls),
                &mut acked,
                last_ack,
            );
            assert_eq!(acked.len(), 1, "each heartbeat SACK must be fresh");
            assert_eq!(space.send_wnd.start(), sq(0));
        }
        let no_resp = space.no_resp_for(last_ack);
        let no_progress = space.no_progress_for(last_ack);
        assert_eq!(no_resp, Some(Duration::ZERO));
        assert!(
            no_progress.is_some_and(|d| d > Duration::from_secs(30)),
            "the initial cumulative hole must age even before first progress; no_progress={no_progress:?}"
        );
        assert!(space.should_terminate_session(last_ack));
    }

    #[test]
    fn cumulative_ack_releases_a_send_window_across_u64_wrap() {
        let t0 = Instant::now();
        // Seed the send window just before the wrap so the three sent
        // packets straddle u64::MAX → 0.
        let mut space = PktSendSpace::new_at(sq(u64::MAX - 2));
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        assert_eq!(space.send_wnd.start(), sq(u64::MAX - 2));
        assert_eq!(space.next_seq(), sq(0), "the next sequence wrapped to 0");
        assert_eq!(space.num_in_flight_pkts(), 3);
        // The peer received all three: its cumulative next is one past the
        // last packet (seq 0, past the wrap).  The whole window must release.
        let acked = ack_one(&mut space, u64::MAX, t0 + ms(3));
        assert_eq!(
            acked, 3,
            "a cumulative ack must release the whole window across the wrap"
        );
        assert!(space.no_pkts_in_flight());
        assert_eq!(space.send_wnd.start(), space.send_wnd.next());
    }

    #[test]
    fn selective_responses_across_wrap_do_not_fake_cumulative_progress() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new_at(sq(u64::MAX - 2));
        settle_rtt_at(&mut space, t0);
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        send_packet(&mut space, t0 + ms(2));
        assert!(!space.liveness.ever_progressed);

        // A selective-only response: the peer's cumulative front stays at
        // MAX-2 while it SACKs the wrapped tail (seq MAX).
        let mut peer = crate::ack::AckHistory::new_at(sq(u64::MAX - 2));
        peer.insert(sq(u64::MAX));
        let balls = peer.blocks().collect::<Vec<_>>();
        let recved = crate::ack::AckBlocks::new(peer.next(), &balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, t0 + ms(10));
        assert_eq!(acked.len(), 1, "only the SACKed tail is released");
        assert_eq!(
            space.send_wnd.start(),
            sq(u64::MAX - 2),
            "the cumulative front must not advance on a selective-only response"
        );
        assert_eq!(space.num_in_flight_pkts(), 2);
        assert!(
            !space.liveness.ever_progressed,
            "selective-only responses must not fake cumulative progress"
        );

        // SACKing the middle packet too: still selective-only.
        let mut peer = crate::ack::AckHistory::new_at(sq(u64::MAX - 2));
        peer.insert(sq(u64::MAX));
        peer.insert(sq(u64::MAX - 1));
        let balls = peer.blocks().collect::<Vec<_>>();
        let recved = crate::ack::AckBlocks::new(peer.next(), &balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, t0 + ms(11));
        assert_eq!(
            acked.len(),
            1,
            "only the newly SACKed middle packet is released"
        );
        assert_eq!(space.send_wnd.start(), sq(u64::MAX - 2));
        assert_eq!(space.num_in_flight_pkts(), 1);
        assert!(!space.liveness.ever_progressed);

        // Only the in-order ack of the head advances the front (pop_none
        // count is nonzero) and records progress.
        let acked = ack_one(&mut space, u64::MAX - 2, t0 + ms(12));
        assert_eq!(acked, 1, "the head is the last in-flight packet");
        assert!(space.no_pkts_in_flight());
        assert!(
            space.liveness.ever_progressed,
            "an in-order cumulative ack must record progress"
        );
    }

    fn sack_apply_cost(num_blocks: usize) -> f64 {
        // One timing pass: a full send window, then one ack carrying
        // `num_blocks` adjacent SACK blocks over the upper half of the
        // window (the same total covered span regardless of block count).
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t0 = Instant::now();
            let mut space = PktSendSpace::new();
            for _ in 0..crate::recv_queue::pkt_recv_space::MAX_NUM_RECVING_PKTS {
                send_packet(&mut space, t0);
            }
            let send_start = space.send_wnd.start();
            let sent_span = space.send_wnd.len() as u64;
            let region_start = sent_span / 2;
            let region_len = sent_span - region_start;
            let block_size = region_len / num_blocks as u64;
            let mut balls = Vec::with_capacity(num_blocks);
            for i in 0..num_blocks as u64 {
                let start = send_start.advance(region_start + i * block_size);
                balls.push(crate::ack::AckInterval {
                    start,
                    size: std::num::NonZeroU64::new(block_size.max(1)).unwrap(),
                });
            }
            let recved = crate::ack::AckBlocks::new(send_start, &balls);
            let mut acked = Vec::new();
            let start = Instant::now();
            space.ack(recved, &mut acked, t0);
            best = best.min(start.elapsed().as_nanos() as f64);
        }
        best
    }

    #[test]
    #[ignore = "perf lane: wall-clock ns/ack ratio; run with cargo test --release -- --ignored"]
    fn applying_many_sacks_remains_linear_in_the_send_window() {
        let one = sack_apply_cost(1);
        let many = sack_apply_cost(MAX_ACK_BLOCKS);
        assert!(
            many < one * 16.0,
            "{many:.1} ns/ack with {MAX_ACK_BLOCKS} SACK blocks against {one:.1} ns with one over a full {} packet window: the per-ack cost grows with the number of blocks",
            crate::recv_queue::pkt_recv_space::MAX_NUM_RECVING_PKTS,
        );
    }

    #[test]
    fn retransmit_index_moves_the_exact_cwnd_prefix_after_ack() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();

        // A full cwnd of packets: the whole window is the indexed prefix.
        for i in 0..INIT_CWND * 2 {
            send_packet(&mut space, t0 + ms(i as u64));
        }
        let seqs = space.active_rtx_seqs();
        assert_eq!(seqs.len(), INIT_CWND);
        for (i, seq) in seqs.iter().enumerate() {
            assert_eq!(*seq, sq(i as u64));
        }

        // Ack the first half: the prefix must move exactly to the remainder.
        ack_up_to(&mut space, INIT_CWND as u64 / 2 - 1, t0 + ms(1000));
        let seqs = space.active_rtx_seqs();
        assert_eq!(seqs.len(), INIT_CWND);
        for (i, seq) in seqs.iter().enumerate() {
            assert_eq!(*seq, sq((INIT_CWND / 2 + i) as u64));
        }

        // Acking the rest empties the index along with the window.
        ack_up_to(
            &mut space,
            (INIT_CWND * 2) as u64 - 1,
            t0 + ms(1000) + ms(1),
        );
        assert!(space.active_rtx_seqs().is_empty());
    }

    #[test]
    fn plain_ack_refill_preserves_surviving_reorder_readiness() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_high_jitter(&mut space, t0);
        let sent_at = t0 + ms(1000);
        for i in 0..INIT_CWND * 2 {
            send_packet(&mut space, sent_at + ms(i as u64));
        }
        assert_eq!(
            sack_one(&mut space, INIT_CWND as u64 - 1, sent_at + ms(100)),
            1
        );
        let reorder_window = space.rtt_stats.reorder_window();
        let promote_at = sent_at + ms(INIT_CWND as u64) + reorder_window;
        space
            .rtx_index
            .promote_due(promote_at, reorder_window, space.rtt_stats.rto_duration());
        assert_eq!(space.rtx_index.first_ready().unwrap().0, sq(0));
        assert_eq!(
            sack_one(&mut space, INIT_CWND as u64 - 2, promote_at + ms(1)),
            1
        );
        assert_eq!(space.rtx_index.first_ready().unwrap().0, sq(0));
        let active = space.active_rtx_seqs();
        assert_eq!(active.len(), INIT_CWND);
        assert_eq!(active[INIT_CWND - 2], sq(INIT_CWND as u64));
        assert_eq!(active[INIT_CWND - 1], sq(INIT_CWND as u64 + 1));
    }

    #[test]
    fn cwnd_resize_preserves_surviving_reorder_readiness() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_high_jitter(&mut space, t0);
        let sent_at = t0 + ms(1000);
        for i in 0..INIT_CWND * 2 {
            send_packet(&mut space, sent_at + ms(i as u64));
        }
        assert_eq!(
            sack_one(&mut space, INIT_CWND as u64 - 1, sent_at + ms(100)),
            1
        );
        let reorder_window = space.rtt_stats.reorder_window();
        let promote_at = sent_at + ms(INIT_CWND as u64) + reorder_window;
        space
            .rtx_index
            .promote_due(promote_at, reorder_window, space.rtt_stats.rto_duration());
        assert_eq!(space.rtx_index.first_ready().unwrap().0, sq(0));
        let srtt_seconds = space.rtt_stats.smooth_rtt().as_secs_f64();
        let rate_for_cwnd = |cwnd: usize| {
            PosR::new(cwnd as f64 / (CWND_SEND_RATE_SCALE as f64 * srtt_seconds)).unwrap()
        };
        let shrunken = INIT_CWND / 2;
        space.set_send_rate(rate_for_cwnd(shrunken));
        assert_eq!(space.cwnd.get(), shrunken);
        assert_eq!(space.rtx_index.first_ready().unwrap().0, sq(0));
        assert_eq!(
            space.active_rtx_seqs(),
            (0..shrunken as u64).map(sq).collect::<Vec<_>>()
        );
        let grown = INIT_CWND - CWND_SEND_RATE_SCALE;
        space.set_send_rate(rate_for_cwnd(grown));
        assert_eq!(space.cwnd.get(), grown);
        assert_eq!(space.rtx_index.first_ready().unwrap().0, sq(0));
        assert_eq!(
            space.active_rtx_seqs(),
            (0..grown as u64).map(sq).collect::<Vec<_>>()
        );
    }

    #[test]
    fn indexed_next_poll_tracks_retransmit_and_reorder_deadlines() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        let stock = space.rtt_stats.reorder_window();

        // A starved packet below the SACK boundary polls at its reorder-
        // window expiry — earlier than the tail probe or the RTO.
        send_packet(&mut space, t0);
        send_packet(&mut space, t0 + ms(1));
        sack_one(&mut space, 1, t0 + ms(10));
        let reorder_deadline = t0 + stock;
        assert_eq!(space.next_poll_time(t0 + ms(10)), Some(reorder_deadline));
        assert_eq!(
            space.next_poll_time(reorder_deadline),
            Some(reorder_deadline)
        );

        // The reorder rtx fires at that deadline; the retransmitted copy
        // then polls at its own fresh reorder deadline.
        let first_rtx_t = reorder_deadline + ms(1);
        let rtx = space
            .rtx(first_rtx_t)
            .expect("reorder rtx fires at the tracked deadline");
        assert_eq!(rtx.seq, sq(0));
        assert_eq!(space.next_poll_time(first_rtx_t), Some(first_rtx_t + stock));

        // With the tail-probe budget then exhausted, the next poll tracks
        // the retransmitted copy's RTO deadline instead of the reorder
        // path (the new flight sits past the old SACK boundary).
        ack_up_to(&mut space, 0, first_rtx_t + ms(1));
        let send_t = first_rtx_t + ms(2);
        send_packet(&mut space, send_t);
        assert!(
            space.tail_probe(send_t + ms(210)).is_some(),
            "first probe fires"
        );
        assert!(
            space.tail_probe(send_t + ms(420)).is_some(),
            "second probe fires"
        );
        assert!(
            space.tail_probe(send_t + ms(630)).is_none(),
            "probe budget exhausted"
        );
        let rto_deadline = space
            .next_poll_time(send_t + ms(630))
            .expect("RTO deadline tracked");
        let rtx = space
            .rtx(rto_deadline)
            .expect("full-RTO rtx fires at the tracked deadline");
        assert_eq!(rtx.seq, sq(2));
    }

    #[test]
    fn ack_without_new_sack_evidence_does_not_rearm_fast_loss_after_rtx() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        assert!(space.fast_loss_armed());

        // Six packets; 1, 2, 3 will be SACKed (arming fast loss for seq 0)
        // while 4, 5 stay in flight so the later re-acks still carry real
        // SACK blocks within the sent span.
        for i in 0..6u64 {
            send_packet(&mut space, t0 + ms(i as u64));
        }
        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));

        // The fast-loss retransmit of seq 0 fires before the reorder window
        // and deactivates the sequence in the index.
        let rtx_t = t0 + ms(30);
        let rtx = space.rtx(rtx_t).expect("fast loss should retransmit seq 0");
        assert_eq!(rtx.seq, sq(0));
        assert!(!space.has_rtx(rtx_t + ms(1)));

        // Re-ack the same SACK evidence without any new delivery: the
        // unchanged evidence must not rearm fast loss for the already-
        // retransmitted packet.
        resack(&mut space, 1, rtx_t + ms(1));
        resack(&mut space, 2, rtx_t + ms(2));
        resack(&mut space, 3, rtx_t + ms(3));
        assert_eq!(sacked_above(&space, 0), 3, "evidence is unchanged");
        assert!(
            !space.has_rtx(rtx_t + ms(5)),
            "unchanged SACK evidence must not rearm fast loss after rtx"
        );
        assert!(
            space.rtx(rtx_t + ms(5)).is_none(),
            "no second fast-loss retransmit of the same sequence"
        );
    }

    /// SACK `seq` with a peer history seeded at `start`, so the wrap-seeded
    /// sender and the peer share a coordinate system (the zero-seeded
    /// `sack_one` helper mismatches wrap-seeded send windows).
    fn sack_seq(space: &mut PktSendSpace, start: u64, seq: u64, now: Instant) -> usize {
        let mut peer = crate::ack::AckHistory::new_at(SequenceNumber::from_wire(start));
        peer.insert(SequenceNumber::from_wire(seq));
        let balls = peer.blocks().collect::<Vec<_>>();
        let recved = crate::ack::AckBlocks::new(peer.next(), &balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, now);
        acked.len()
    }

    #[test]
    fn tail_probe_targets_newest_unacked_across_sequence_wrap() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new_at(sq(u64::MAX - 2));
        settle_rtt_at(&mut space, t0);
        // Window [MAX-2, MAX-1, MAX, 0]; the newest sent is seq 0.
        for i in 0..4u64 {
            send_packet(&mut space, t0 + ms(i));
        }
        assert_eq!(space.next_seq(), sq(1));
        // The peer receives the newest packet (seq 0) out of order: its slot
        // becomes a None hole, but the physical head (MAX-2) is still unacked
        // so the hole stays in the window.
        assert_eq!(sack_seq(&mut space, u64::MAX - 2, 0, t0 + ms(10)), 1);
        assert!(
            space.send_wnd.get(&sq(0)).is_some_and(|o| o.is_none()),
            "seq 0 must be acked-but-unpopped"
        );
        // The tail is now the newest *unacked* packet (MAX), not the physical
        // newest slot (seq 0's None).
        let p = space
            .tail_probe(t0 + ms(250))
            .expect("the tail probe fires for the newest unacked packet");
        assert_eq!(
            p.seq,
            sq(u64::MAX),
            "the probe must target the newest unacked packet across the wrap"
        );
    }

    #[test]
    fn retransmit_index_and_deadline_order_cross_u64_wrap() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new_at(sq(u64::MAX - 1));
        settle_rtt_at(&mut space, t0);
        for i in 0..4u64 {
            send_packet(&mut space, t0 + ms(i));
        }
        assert_eq!(space.next_seq(), sq(2), "the window straddles the wrap");
        // Once every RTO deadline has elapsed, retransmission order follows
        // wrap-aware serial sequence order: the logical head (MAX-1) first,
        // then MAX, 0, 1 — never raw u64 value order.
        let rto = space.rto_duration();
        let mut due = t0 + rto + ms(1);
        for expected in [u64::MAX - 1, u64::MAX, 0, 1] {
            let p = space
                .rtx(due)
                .expect("RTO retransmits fire in serial order");
            assert_eq!(p.seq, sq(expected));
            due = due + ms(1);
        }
        assert!(space.rtx(due).is_none());
    }

    #[test]
    fn cumulative_only_ack_releases_prefix_and_keeps_wrapped_tail() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new_at(sq(u64::MAX - 2));
        settle_rtt_at(&mut space, t0);
        for i in 0..4u64 {
            send_packet(&mut space, t0 + ms(i));
        }
        // The peer received the first two packets in order, so its cumulative
        // front is one past MAX-1 (i.e. MAX).  No SACK blocks at all.
        let recved = crate::ack::AckBlocks::new(sq(u64::MAX), &[]);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, t0 + ms(10));
        assert_eq!(acked.len(), 2, "the cumulative prefix is released");
        assert_eq!(space.send_wnd.start(), sq(u64::MAX));
        assert_eq!(space.num_in_flight_pkts(), 2);
        // A cumulative-only ACK carries no selective evidence at all.
        assert_eq!(sacked_above(&space, u64::MAX), 0);
        assert_eq!(sacked_above(&space, 0), 0);
        // The wrapped tail [MAX, 0] releases on the next cumulative ACK.
        let recved = crate::ack::AckBlocks::new(sq(1), &[]);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, t0 + ms(11));
        assert_eq!(acked.len(), 2);
        assert!(space.no_pkts_in_flight());
        assert_eq!(space.send_wnd.start(), space.send_wnd.next());
        assert_eq!(space.newest_unacked, None);
    }

    #[test]
    fn outage_recovery_schedules_the_whole_pre_outage_window_without_ack_rounds() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Raise cwnd so a flight larger than OUTAGE_RECOVERY_CWND can be in
        // flight before the outage clamps it: 0.1 s * 80 pkt/s * scale = 64.
        space.set_send_rate(PosR::new(80.0).unwrap());
        let pre_outage_flight = space.cwnd().get();
        assert!(
            pre_outage_flight > OUTAGE_RECOVERY_CWND,
            "cwnd={pre_outage_flight}"
        );
        for i in 0..pre_outage_flight {
            send_packet(&mut space, t0 + ms(i as u64));
        }
        assert_eq!(space.num_in_flight_pkts(), pre_outage_flight);

        // Make forward progress, arm a loss event, then wait one RTO so the
        // epoch can be detected.
        ack_one(&mut space, 0, t0 + ms(pre_outage_flight as u64));
        lose_packet(&mut space, 1, t0 + ms(pre_outage_flight as u64 + 40));
        let detect_at = t0 + ms(pre_outage_flight as u64) + space.rto_duration() + ms(1);
        assert!(space.detect_outage_recovery(detect_at));
        assert!(space.in_outage_recovery());
        space.set_send_rate(PosR::new(crate::reliable::reliable_layer::INIT_SEND_RATE).unwrap());
        assert!(
            space.cwnd().get() < pre_outage_flight,
            "recovery cwnd must clamp below the pre-outage flight"
        );

        // Outage mode retains the cwnd-bounded pipe scan even while the whole
        // pre-outage window is indexed for retransmission.
        assert_eq!(
            space.max_pipe_seq,
            scanned_pipe_boundary(&space),
            "outage mode must retain the cwnd-bounded pipe scan"
        );
        assert_eq!(
            space.active_rtx_seqs().len(),
            space.num_in_flight_pkts(),
            "the whole pre-outage window must be indexed, not the cwnd prefix"
        );
        assert!(
            space.has_rtx(detect_at),
            "pre-outage packets must be immediately retransmission-ready"
        );

        // Retransmit everything in one sweep.  Production paces these through
        // the token bucket, but the scheduler itself must offer the whole
        // pre-outage window without any ack rounds in between.
        let mut t = detect_at;
        let mut retransmitted = 0;
        while let Some(_p) = space.rtx(t) {
            retransmitted += 1;
            t += ms(1);
        }
        assert_eq!(
            retransmitted,
            pre_outage_flight - 1,
            "every pre-outage in-flight packet must be scheduled without ack rounds"
        );
        assert!(!space.has_rtx(t), "the window is exhausted after one sweep");
    }

    #[test]
    fn live_rto_growth_postpones_stale_deadline_and_loss_classification() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);
        // Send a packet whose stored RTO comes from the settled estimator,
        // then grow rttvar so the live estimator's RTO exceeds the stored
        // deadline.
        send_packet(&mut space, t0);
        let stored_rto = space.rto_duration();
        for i in 0..10 {
            space.sample_rtt(ms(100), t0 + ms(2000) + ms(i * 2));
            space.sample_rtt(ms(900), t0 + ms(2000) + ms(i * 2 + 1));
        }
        let live_rto = space.rto_duration();
        assert!(live_rto > stored_rto, "test requires live RTO growth");

        // At the stored deadline the poll wakes, but the retransmission is
        // lazily floored to the live deadline: no loss is classified yet.
        let stale_deadline = t0 + stored_rto;
        assert!(
            space.has_rtx(stale_deadline),
            "the stored deadline wakes the poll"
        );
        assert!(
            space.rtx(stale_deadline).is_none(),
            "the live floor postpones the stale deadline"
        );
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss classified at the stale deadline"
        );
        assert_eq!(space.rto_deadline_postponements, 1);

        // Between the stale and the live deadline nothing is due.
        let between = t0 + stored_rto + ms(500);
        assert!(between < t0 + live_rto);
        assert!(!space.has_rtx(between));
        assert!(space.rtx(between).is_none());
        assert_eq!(space.rto_deadline_postponements, 1);

        // At the live deadline the packet is retransmitted and the loss is
        // classified exactly once.
        let live_deadline = t0 + live_rto + ms(1);
        let p = space
            .rtx(live_deadline)
            .expect("the live-floored RTO deadline fires");
        assert_eq!(p.seq, sq(0));
        assert!(
            space.loss_event_window.raw_has_loss_event(),
            "the postponed loss is classified at the live deadline"
        );
    }
}
