use core::{num::NonZeroUsize, time::Duration};
use std::time::Instant;

use dre::PacketState;
use primitive::{
    arena::obj_pool::{ObjPool, buf_pool},
    ops::{
        float::{PosR, UnitR},
        len::LenExt,
        opt_cmp::MinNoneOptCmp,
    },
    queue::send_wnd::SendWnd,
};

use crate::{
    recv_queue::pkt_recv_space::MAX_NUM_RECVING_PKTS,
    sack::AckBallSequence,
    send_queue::{
        liveness::PeerLiveness,
        outage::{OutageDetection, OutageEpoch},
        rtt_stats::RttStats,
        tlp::TailLossProber,
    },
};

pub const INIT_CWND: usize = 16;
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
fn jitter_cap_from_env() -> bool {
    match std::env::var("RTP_JITTER_CAP") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

#[derive(Debug)]
pub struct PktSendSpace {
    send_wnd: SendWnd<u64, Option<TxingPkt>>,
    num_txing: usize,
    reused_buf: ObjPool<Vec<u8>>,
    cwnd: NonZeroUsize,
    out_of_order_seq_end: u64,
    /// Any sequence after this does not participate in data loss analysis.
    max_pipe_seq: MinNoneOptCmp<u64>,
    loss_event_window: LossEventWindow,
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
    /// (reordering, not loss).
    deferred_losses: Vec<DeferredLoss>,

    /// Snapshot of `RTP_JITTER_CAP` taken at construction.  Stored as a
    /// field rather than read from a `OnceLock` so tests can construct a
    /// `PktSendSpace` with the toggle on or off deterministically without
    /// racing other parallel tests in the same binary.
    jitter_cap: bool,

    // reused buffers
    unacked_buf: Vec<u64>,
    ack_buf: Vec<u64>,
}
impl PktSendSpace {
    pub fn new() -> Self {
        Self {
            send_wnd: SendWnd::new(0),
            num_txing: 0,
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            out_of_order_seq_end: 0,
            max_pipe_seq: MinNoneOptCmp(None),
            loss_event_window: LossEventWindow::new(),
            tlp: TailLossProber::new(),
            liveness: PeerLiveness::new(),
            outage: OutageEpoch::new(),
            rtt_stats: RttStats::new(),
            fast_loss_disabled: false,
            deferred_losses: vec![],
            jitter_cap: jitter_cap_from_env(),
            unacked_buf: vec![],
            ack_buf: vec![],
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

    fn unacked(
        send_wnd: &SendWnd<u64, Option<TxingPkt>>,
    ) -> impl Iterator<Item = (u64, &TxingPkt)> {
        send_wnd
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
    }
    fn unacked_mut(
        send_wnd: &mut SendWnd<u64, Option<TxingPkt>>,
    ) -> impl Iterator<Item = (u64, &mut TxingPkt)> {
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
    /// having a partial FEC group open.
    #[cfg(test)]
    pub(crate) fn set_cwnd_for_test(&mut self, cwnd: NonZeroUsize) {
        self.cwnd = cwnd;
    }

    pub fn next_seq(&self) -> u64 {
        *self.send_wnd.next().unwrap()
    }

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

    pub fn ack(&mut self, recved: AckBallSequence<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        let cumulative_front_before = self.send_wnd.start().or(self.send_wnd.next()).copied();
        let peer_waiting_for_acked_pkts =
            cumulative_front_before.is_some_and(|front| recved.first_unacked() < front);
        if let Some(seq) = recved.out_of_order_seq_end() {
            self.out_of_order_seq_end = self.out_of_order_seq_end.max(seq);
        }
        self.unacked_buf.clear();
        self.unacked_buf
            .extend(Self::unacked(&self.send_wnd).map(|(k, _)| k));
        self.ack_buf.clear();
        recved.ack(&self.unacked_buf, &mut self.ack_buf);
        let delivered = self.ack_buf.len();
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

        for &s in &self.ack_buf {
            let p = self.send_wnd.get_mut(&s).unwrap();
            let p = p.take().unwrap();
            self.num_txing -= 1;
            if s == *self.send_wnd.start().unwrap() {
                self.send_wnd.pop().unwrap();
                self.send_wnd.pop_none();
            }
            if !self.deferred_losses.is_empty() {
                self.deferred_losses.retain(|dl| dl.seq != s);
            }
            self.reused_buf.put(p.data);
            acked.push(p.stats);
        }

        let cumulative_front_after = self.send_wnd.start().or(self.send_wnd.next()).copied();
        if cumulative_front_before
            .zip(cumulative_front_after)
            .is_some_and(|(before, after)| before < after)
        {
            self.liveness.record_progress();
        }

        for ball in recved.balls() {
            let ball_end = ball.start + ball.size.get();
            for (s, p) in Self::unacked_mut(&mut self.send_wnd) {
                if s < ball.start {
                    p.sack_passes = p.sack_passes.saturating_add(ball.size.get() as u32);
                } else if s < ball_end {
                }
            }
        }

        self.loss_event_window
            .record_delivered(delivered, now, self.smooth_rtt());
        if peer_waiting_for_acked_pkts && delivered == 0 {
            return;
        }
        if self.send_wnd.is_empty() {
            self.liveness.reset_waits();
            return;
        }
        let rto = self.rtt_stats.rto_duration();
        self.liveness.refresh_waits(now, rto);
    }

    pub fn sample_rtt(&mut self, rtt: Duration, now: Instant) {
        if self.outage.should_censor_rtt_sample(rtt, now) {
            return;
        }
        if self.outage.try_close_epoch_with_fresh_sample() {
            self.rtt_stats.record_min_and_reseed_rto(rtt);
            return;
        }

        self.rtt_stats.record_rtt(rtt);
    }

    pub fn accepts_new_pkt(&self) -> bool {
        self.num_txing < self.cwnd.get()
    }

    /// Sequence number of the current tail packet, if any.
    fn tail_seq(&self) -> Option<u64> {
        let next = self.send_wnd.next().copied()?;
        if next == 0 {
            return None;
        }
        Some(next - 1)
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
    /// it as a loss event or clearing its congestion state.
    pub fn tail_probe(&mut self, now: Instant) -> Option<Pkt<'_>> {
        if !self.has_tail_probe(now) {
            return None;
        }
        let seq = self.tail_seq()?;
        self.tlp.sent();
        let rto = self.tlp.rto(&self.rtt_stats);
        let p = self.send_wnd.get_mut(&seq)?.as_mut()?;

        // Refresh the timestamp/RTO so the probe is tracked as a fresh packet
        // for RTO calculation (the RTO fallback covers a lost probe).
        self_assign::self_assign! {
            p = TxingPkt {
                stats: _,
                sent_time: now,
                rtxed: _,
                considered_new_in_cwnd: _,
                data: _,
                frame_len: _,
                rto,
                rto_from_tail_probe: true,
                sack_passes: _,
                fast_loss_rtx_time: _,
                deferred_loss_stock_deadline: _,
            };
        }
        Some(Pkt {
            seq,
            data: &p.data,
            frame_len: p.frame_len,
        })
    }

    pub fn send(
        &mut self,
        data: Vec<u8>,
        stats: PacketState,
        frame_len: Option<u32>,
        now: Instant,
    ) -> Pkt<'_> {
        let s = *self.send_wnd.next().unwrap();

        self.max_pipe_seq = MinNoneOptCmp(Some(s));

        let rto = self.rtt_stats.rto_duration();
        self.liveness.on_send(now, rto);

        let p = TxingPkt {
            stats,
            sent_time: now,
            rtxed: false,
            considered_new_in_cwnd: false,
            data,
            frame_len,
            rto: self.rtt_stats.rto_duration(),
            rto_from_tail_probe: false,
            sack_passes: 0,
            fast_loss_rtx_time: None,
            deferred_loss_stock_deadline: None,
        };

        self.send_wnd.push(Some(p));
        self.num_txing += 1;
        self.tlp.reset();

        Pkt {
            seq: s,
            data: &self.send_wnd.get(&s).unwrap().as_ref().unwrap().data,
            frame_len,
        }
    }

    pub fn has_rtx(&self, now: Instant) -> bool {
        let out_of_order_seq_end = self.out_of_order_seq_end;
        let fast_loss_armed = self.fast_loss_armed();
        let stock_window = self.rtt_stats.reorder_window();
        let jcap = self.jitter_cap;
        let rtx_window = if jcap {
            self.rtt_stats.fast_reorder_window()
        } else {
            stock_window
        };
        Self::unacked(&self.send_wnd)
            .take(self.cwnd.get())
            .any(|(s, p)| {
                let is_seq_out_of_order = SeqOutOfOrder(s < out_of_order_seq_end);
                let is_pre_outage_loss = self.outage.is_pre_outage_loss(p.sent_time);
                let time_based = p.is_rtx(is_seq_out_of_order, is_pre_outage_loss, rtx_window, now);
                let fast_loss = fast_loss_armed && p.is_fast_loss();
                time_based || fast_loss
            })
    }

    pub fn rtx(&mut self, now: Instant) -> Option<Pkt<'_>> {
        let out_of_order_seq_end = self.out_of_order_seq_end;
        let stock_window = self.rtt_stats.reorder_window();
        let jcap = self.jitter_cap;
        let rtx_window = if jcap {
            self.rtt_stats.fast_reorder_window()
        } else {
            stock_window
        };
        let fast_loss_armed = self.fast_loss_armed();
        for (s, p) in Self::unacked_mut(&mut self.send_wnd).take(self.cwnd.get()) {
            let is_seq_out_of_order = SeqOutOfOrder(s < out_of_order_seq_end);
            let is_pre_outage_loss = self.outage.is_pre_outage_loss(p.sent_time);
            let time_based = p.is_rtx(is_seq_out_of_order, is_pre_outage_loss, rtx_window, now);
            let fast_loss = fast_loss_armed && p.is_fast_loss();
            let should_rtx = time_based || fast_loss;
            if !should_rtx {
                continue;
            }

            // Count one loss event per packet the first time it is retransmitted,
            // unless the loss happened before the outage-recovery cut (those losses
            // are caused by the link going away) or the packet was already sent as
            // a tail-loss probe (the probe itself owns the tail-latency signal).
            let already_rtxed = p.rtxed;
            let pre_outage_loss = !already_rtxed && is_pre_outage_loss;
            let tail_probe_loss = p.rto_from_tail_probe;
            // A fast-loss retransmit is a genuine loss declaration (not a TLP
            // probe), so it records a loss event exactly as a window-expiry
            // loss would — no TLP probe accounting.
            let is_fast_loss_rtx = fast_loss && !already_rtxed;

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
            let defer_loss = jcap
                && !already_rtxed
                && !pre_outage_loss
                && !tail_probe_loss
                && !is_fast_loss_rtx
                && now < original_sent_time + stock_window;
            let stock_deadline_opt = if defer_loss {
                Some(original_sent_time + stock_window)
            } else {
                None
            };

            // fresh pkt for this cwnd
            let considered_new_in_cwnd = if self.max_pipe_seq < MinNoneOptCmp(Some(s)) {
                self.max_pipe_seq = MinNoneOptCmp(Some(s));
                true
            } else {
                false
            };

            self_assign::self_assign! {
                p = TxingPkt {
                    stats: _,
                    sent_time: now,
                    rtxed: true,
                    considered_new_in_cwnd,
                    data: _,
                    frame_len: _,
                    rto: self.rtt_stats.rto_duration(),
                    rto_from_tail_probe: false,
                    sack_passes: _,
                    fast_loss_rtx_time: if is_fast_loss_rtx { Some(now) } else { None },
                    deferred_loss_stock_deadline: stock_deadline_opt,
                };
            }
            if defer_loss {
                self.deferred_losses.push(DeferredLoss {
                    seq: s,
                    stock_deadline: stock_deadline_opt.unwrap(),
                });
            } else if !already_rtxed && !pre_outage_loss && !tail_probe_loss {
                let smooth_rtt = self.rtt_stats.smooth_rtt();
                self.loss_event_window.record_lost(1, now, smooth_rtt);
            }
            let p = Pkt {
                seq: s,
                data: &p.data,
                frame_len: p.frame_len,
            };
            return Some(p);
        }
        None
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
        let mut i = 0;
        while i < self.deferred_losses.len() {
            let dl = &self.deferred_losses[i];
            if now < dl.stock_deadline {
                i += 1;
                continue;
            }
            // Deadline elapsed: is the seq still in flight (genuine loss)?
            // `send_wnd.get(&seq)` returning Some(Some(_)) means the packet is
            // still unacked; if it is None or Some(None) the packet was acked
            // and `ack` already cancelled this entry — but be defensive and
            // double-check here too.
            let still_in_flight = self
                .send_wnd
                .get(&dl.seq)
                .and_then(|o| o.as_ref())
                .is_some();
            if still_in_flight {
                self.loss_event_window.record_lost(1, now, smooth_rtt);
            }
            self.deferred_losses.swap_remove(i);
        }
    }

    pub fn min_rtt(&self) -> Option<Duration> {
        self.rtt_stats.min_rtt()
    }

    pub fn set_send_rate(&mut self, send_rate: PosR<f64>) {
        let cwnd = self.rtt_stats.smooth_rtt().as_secs_f64() * send_rate.get();
        let cwnd = cwnd.round() as usize;
        let cwnd = cwnd * CWND_SEND_RATE_SCALE;
        let cwnd = 1.max(cwnd);
        // While an outage-recovery epoch is open, clamp cwnd to INIT_CWND so a
        // just-restored path is not flooded before fresh RTT samples can seed
        // the congestion state.
        let cwnd = if self.outage.in_outage_recovery() {
            cwnd.min(INIT_CWND)
        } else {
            cwnd
        };
        self.cwnd = NonZeroUsize::new(cwnd).unwrap();

        let last_seq_in_cwnd = || {
            if let Some(s) = Self::unacked(&self.send_wnd).map(|(k, _)| k).nth(cwnd) {
                return Some(s);
            }
            if let Some(s) = Self::unacked(&self.send_wnd).map(|(k, _)| k).last() {
                return Some(s);
            }
            None
        };
        let last_seq_in_cwnd = last_seq_in_cwnd();

        // Retract max sequence in pipe
        if MinNoneOptCmp(last_seq_in_cwnd) < self.max_pipe_seq {
            self.max_pipe_seq = MinNoneOptCmp(last_seq_in_cwnd);
        }
    }

    pub fn no_pkts_in_flight(&self) -> bool {
        self.send_wnd.is_empty()
    }

    pub fn in_outage_recovery(&self) -> bool {
        self.outage.in_outage_recovery()
    }

    #[allow(dead_code)]
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

        match self.outage.detect(
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
        }
    }

    pub fn clear_outage_recovery(&mut self) {
        self.outage.clear();
    }

    pub fn cwnd_stats(&self, now: Instant) -> CwndStats {
        let mut not_lost = 0;
        let mut all_lost_pkts_rtxed = true;
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now) {
                all_lost_pkts_rtxed = false;
            } else {
                not_lost += 1;
            }
        }
        CwndStats {
            all_lost_pkts_rtxed,
            num_not_lost_txing_pkts: not_lost,
        }
    }
    pub fn all_lost_pkts_rtxed(&self, now: Instant) -> bool {
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now) {
                return false;
            }
        }
        true
    }
    pub fn num_not_lost_txing_pkts(&self, now: Instant) -> usize {
        let mut not_lost = 0;
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            if p.hits_rto(now) {
                continue;
            }
            not_lost += 1;
        }
        not_lost
    }

    pub fn num_txing_pkts(&self) -> usize {
        self.num_txing
    }

    pub fn huge_data_loss(&self, tolerant_loss_rate: UnitR<f64>, now: Instant) -> bool {
        let Some(data_loss_rate) = self.data_loss_rate(now) else {
            return false;
        };
        let enough_samples_for_stats = INIT_CWND < self.pkts_in_pipe().count();
        enough_samples_for_stats && tolerant_loss_rate.get() < data_loss_rate
    }

    pub fn data_loss_rate(&self, now: Instant) -> Option<f64> {
        let mut lost = 0;
        let mut len = 0;
        for (_, p) in self.pkts_in_pipe() {
            len += 1;
            let rtxed = !p.considered_new_in_cwnd && p.rtxed;
            if rtxed || p.hits_rto(now) {
                lost += 1;
            }
        }
        if len == 0 {
            return None;
        }
        Some(lost as f64 / len as f64)
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

    pub fn num_pkts_in_pipe(&self) -> usize {
        self.pkts_in_pipe().count()
    }

    fn pkts_in_pipe(&self) -> impl Iterator<Item = (u64, &TxingPkt)> + '_ {
        Self::unacked(&self.send_wnd)
            .take_while(|(s, _)| MinNoneOptCmp(Some(*s)) <= self.max_pipe_seq)
    }

    pub fn next_poll_time(&self) -> Option<Instant> {
        let mut min_next_poll_time: Option<Instant> = None;
        let rtx_window = if self.jitter_cap {
            self.rtt_stats.fast_reorder_window()
        } else {
            self.rtt_stats.reorder_window()
        };
        for (s, p) in Self::unacked(&self.send_wnd).take(self.cwnd.get()) {
            let t = p.next_rto_time();
            let t = min_next_poll_time.map(|min| min.min(t)).unwrap_or(t);
            min_next_poll_time = Some(t);
            if s < self.out_of_order_seq_end {
                let rw_t = p.sent_time + rtx_window;
                min_next_poll_time =
                    Some(min_next_poll_time.map(|min| min.min(rw_t)).unwrap_or(rw_t));
            }
        }
        if let Some(seq) = self.tail_seq()
            && let Some(Some(p)) = self.send_wnd.get(&seq)
            && let Some(t) = self.tlp.next_probe_time(p.sent_time, &self.rtt_stats)
        {
            min_next_poll_time = Some(min_next_poll_time.map(|min| min.min(t)).unwrap_or(t));
        }
        for dl in &self.deferred_losses {
            min_next_poll_time = Some(
                min_next_poll_time
                    .map(|min| min.min(dl.stock_deadline))
                    .unwrap_or(dl.stock_deadline),
            );
        }
        if let Some(wd) = self.liveness.next_watchdog_deadline(Instant::now()) {
            min_next_poll_time = Some(
                min_next_poll_time
                    .map(|min| min.min(wd))
                    .unwrap_or(wd),
            );
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
pub struct CwndStats {
    pub all_lost_pkts_rtxed: bool,
    pub num_not_lost_txing_pkts: usize,
}

#[derive(Debug, Clone)]
struct TxingPkt {
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
    pub sack_passes: u32,
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
    /// `deferred_loss_stock_deadline`).  If the original is acked before that
    /// deadline the loss event is cancelled (it was reordering, not loss);
    /// otherwise it is recorded exactly once at the deadline.
    pub deferred_loss_stock_deadline: Option<Instant>,
}
impl TxingPkt {
    pub fn hits_rto(&self, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        self.rto <= sent_elapsed
    }

    pub fn hits_custom_rto(&self, rto: Duration, now: Instant) -> bool {
        let sent_elapsed = now.duration_since(self.sent_time);
        rto <= sent_elapsed
    }

    pub fn next_rto_time(&self) -> Instant {
        self.sent_time + self.rto
    }

    pub fn is_rtx(
        &self,
        seq_out_of_order: SeqOutOfOrder,
        pre_outage_loss: bool,
        custom_rto: Duration,
        now: Instant,
    ) -> bool {
        let out_of_order_rt = seq_out_of_order.0 && self.hits_custom_rto(custom_rto, now);
        // Pre-outage packets that are still unacked after the link recovers are
        // treated as already lost: retransmit them immediately so the first ACK
        // after the outage can make progress.
        let pre_outage = pre_outage_loss;
        self.hits_rto(now) || out_of_order_rt || pre_outage
    }

    /// Whether this packet should be declared lost by the evidence-gated
    /// fast-loss path: it has not yet been retransmitted and at least
    /// [`FAST_LOSS_SACK_THRESHOLD`] newer in-flight packets have been SACKed
    /// past it.  The structural arming gate and the observed-reordering
    /// hard-disable are checked by the caller (`PktSendSpace::fast_loss_armed`).
    pub fn is_fast_loss(&self) -> bool {
        !self.rtxed && self.sack_passes >= FAST_LOSS_SACK_THRESHOLD
    }
}

struct SeqOutOfOrder(pub bool);

/// A pending deferred CC loss-event recording for the jitter-tolerant fast-
/// retransmit path.  The loss event for `seq` was deferred at `rtx_time` to
/// `stock_deadline` (the original send time + stock reorder window).  When
/// [`PktSendSpace::poll_deferred_loss`] runs at `now >= stock_deadline` it
/// records one loss event iff `seq` is still in flight (genuine loss); if the
/// seq was acked in the meantime the entry was already cancelled by `ack`.
#[derive(Debug, Clone, Copy)]
struct DeferredLoss {
    seq: u64,
    stock_deadline: Instant,
}

#[derive(Debug, Clone)]
pub struct Pkt<'a> {
    pub seq: u64,
    pub data: &'a [u8],
    /// Application frame length this packet belongs to.  `Some` only for the
    /// first packet of a frame in frame-delivery mode; the transmission layer
    /// uses this to choose `FRAME_DATA_TS` vs `DATA_TS` on the wire.  Retrans-
    /// mits carry the same value so the framing is preserved across repairs.
    pub frame_len: Option<u32>,
}

#[derive(Debug, Clone)]
struct LossEventBucket {
    lost: usize,
    delivered: usize,
    start: Option<Instant>,
}

impl LossEventBucket {
    fn new() -> Self {
        Self {
            lost: 0,
            delivered: 0,
            start: None,
        }
    }

    fn reset(&mut self, start: Instant) {
        self.lost = 0;
        self.delivered = 0;
        self.start = Some(start);
    }
}

#[derive(Debug, Clone)]
struct LossEventWindow {
    curr: LossEventBucket,
    prev: LossEventBucket,
}

impl LossEventWindow {
    fn new() -> Self {
        Self {
            curr: LossEventBucket::new(),
            prev: LossEventBucket::new(),
        }
    }

    fn bucket_len(smooth_rtt: Duration) -> Duration {
        smooth_rtt.max(Duration::from_millis(1))
    }

    fn rotate(&mut self, now: Instant, smooth_rtt: Duration) {
        let bucket_len = Self::bucket_len(smooth_rtt);
        let curr_start = self.curr.start.unwrap_or(now);
        let elapsed = now.duration_since(curr_start);
        if elapsed < bucket_len {
            return;
        }

        // Gap of 2 or more empty buckets: reset both buckets (loss event has aged out).
        let gap_buckets = elapsed.as_millis() / bucket_len.as_millis();
        if gap_buckets >= 2 {
            self.curr.reset(now);
            self.prev.reset(now);
            return;
        }

        // Single-bucket rotation.
        self.prev = self.curr.clone();
        self.curr.reset(now);
    }

    fn record_delivered(&mut self, delivered: usize, now: Instant, smooth_rtt: Duration) {
        self.rotate(now, smooth_rtt);
        if self.curr.start.is_none() {
            self.curr.reset(now);
        }
        self.curr.delivered += delivered;
    }

    fn record_lost(&mut self, lost: usize, now: Instant, smooth_rtt: Duration) {
        self.rotate(now, smooth_rtt);
        if self.curr.start.is_none() {
            self.curr.reset(now);
        }
        self.curr.lost += lost;
    }

    fn rate(&mut self, now: Instant, smooth_rtt: Duration) -> Option<f64> {
        self.rotate(now, smooth_rtt);
        let total = self.curr.lost + self.curr.delivered + self.prev.lost + self.prev.delivered;
        if total < INIT_CWND {
            return None;
        }
        let lost = self.curr.lost + self.prev.lost;
        Some(lost as f64 / total as f64)
    }

    /// Directly inspect the buckets without rotating.
    fn raw_has_loss_event(&self) -> bool {
        self.curr.lost > 0 || self.prev.lost > 0
    }

    fn reset(&mut self, now: Instant, smooth_rtt: Duration) {
        self.curr.reset(now);
        self.prev.reset(now + Self::bucket_len(smooth_rtt));
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{CWND_SEND_RATE_SCALE, INIT_CWND, LossEventWindow, PktSendSpace};
    use primitive::ops::float::PosR;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn settle_rtt_at(space: &mut PktSendSpace, now: Instant) {
        // Saturate the SRTT filter so the RTO is stable and close to 100 ms.
        for i in 0..20 {
            space.sample_rtt(ms(100), now + ms(i));
        }
    }

    fn send_packet(space: &mut PktSendSpace, now: Instant) -> u64 {
        use dre::ConnectionState;
        let data = vec![0u8; 1];
        let stats = ConnectionState::new(now).send_packet_2(now, space.no_pkts_in_flight());
        let seq = space.next_seq();
        space.send(data, stats, None, now);
        seq
    }

    fn ack_one(space: &mut PktSendSpace, seq: u64, now: Instant) -> usize {
        let ball = crate::sack::AckBall {
            start: seq,
            size: std::num::NonZeroU64::new(1).unwrap(),
        };
        let balls = [ball];
        let seq = crate::sack::AckBallSequence::new(&balls);
        let mut acked = Vec::new();
        space.ack(seq, &mut acked, now);
        acked.len()
    }

    /// SACK a single out-of-order sequence: the peer has received `seq` but
    /// the cumulative ACK point is still below it.  This removes `seq` from
    /// the send window (it is delivered) and increments `sack_passes` on
    /// every older in-flight packet.
    fn sack_one(space: &mut PktSendSpace, seq: u64, now: Instant) -> usize {
        let ball = crate::sack::AckBall {
            start: seq,
            size: std::num::NonZeroU64::new(1).unwrap(),
        };
        let balls = [ball];
        let recved = crate::sack::AckBallSequence::new(&balls);
        let mut acked = Vec::new();
        space.ack(recved, &mut acked, now);
        acked.len()
    }

    fn sack_passes(space: &PktSendSpace, seq: u64) -> u32 {
        space
            .send_wnd
            .get(&seq)
            .and_then(|o| o.as_ref())
            .map(|p| p.sack_passes)
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

    fn lose_packet(space: &mut PktSendSpace, seq: u64, now: Instant) {
        // Record a loss event for the packet without actually transmitting it.
        // We do this by pulling the packet out, marking it as already retransmitted,
        // and then putting it back as a retransmitted copy.  The original send time
        // is preserved so it still looks like a pre-outage packet.
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
        let ball = crate::sack::AckBall {
            start: 0,
            size: std::num::NonZeroU64::new(seq + 1).unwrap(),
        };
        let balls = [ball];
        let seq = crate::sack::AckBallSequence::new(&balls);
        let mut acked = Vec::new();
        space.ack(seq, &mut acked, now);
    }

    #[test]
    fn loss_event_rate_reads_true_loss() {
        let now = Instant::now();
        let smooth = ms(100);
        let mut w = LossEventWindow::new();
        // Deliver 85, lose 15 over a single bucket worth of samples.
        for _ in 0..85 {
            w.record_delivered(1, now, smooth);
        }
        for _ in 0..15 {
            w.record_lost(1, now, smooth);
        }
        let rate = w.rate(now, smooth).unwrap();
        assert!((rate - 0.15).abs() < 0.001, "rate={rate}");
    }

    #[test]
    fn loss_event_rate_needs_min_samples() {
        let now = Instant::now();
        let smooth = ms(100);
        let mut w = LossEventWindow::new();
        for _ in 0..15 {
            w.record_lost(1, now, smooth);
        }
        assert!(w.rate(now, smooth).is_none());
        for _ in 0..16 {
            w.record_delivered(1, now, smooth);
        }
        assert!(w.rate(now, smooth).is_some());
    }

    #[test]
    fn loss_events_age_out() {
        let t0 = Instant::now();
        let smooth = ms(100);
        let mut w = LossEventWindow::new();
        // Fill current bucket with losses.
        for _ in 0..100 {
            w.record_lost(1, t0, smooth);
        }
        let r = w.rate(t0, smooth).unwrap();
        assert!(r > 0.9, "r={r}");

        // After one bucket rotation losses move to prev and still count.
        let t1 = t0 + smooth + ms(1);
        for _ in 0..100 {
            w.record_delivered(1, t1, smooth);
        }
        let r = w.rate(t1, smooth).unwrap();
        assert!(r > 0.4 && r < 0.6, "r={r}");

        // After a second rotation the old losses are gone.
        let t2 = t1 + smooth + ms(1);
        for _ in 0..100 {
            w.record_delivered(1, t2, smooth);
        }
        let r = w.rate(t2, smooth).unwrap();
        assert!(r < 0.01, "r={r}");

        // After a 10-bucket idle gap the window is empty and abstains.
        let t3 = t2 + smooth * 10;
        assert!(w.rate(t3, smooth).is_none());
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
        assert_eq!(p.seq, 1);

        // Second probe requires another full PTO window.
        let t3 = t2 + ms(150);
        assert!(!space.has_tail_probe(t3), "second probe too early");
        let t4 = t2 + ms(250);
        assert!(space.has_tail_probe(t4), "second probe should fire");
        let p = space.tail_probe(t4).unwrap();
        assert_eq!(p.seq, 1);

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
        // Recovery starts by clamping cwnd to INIT_CWND once set_send_rate is
        // recomputed with the epoch active.
        space.set_send_rate(PosR::new(crate::reliable::reliable_layer::INIT_SEND_RATE).unwrap());
        assert_eq!(space.cwnd().get(), INIT_CWND);
        // RTO is reset to the prior RTO value (acts as the seed).
        let seed_rto = space.rto_duration();

        // The unacked pre-outage packets are now immediately eligible for rtx
        // and are not counted as congestion losses.  seq 1 was marked lost by the
        // helper but is at the front of the window; it should still be exempt.
        let rtx = space.rtx(t0 + ms(600)).unwrap();
        assert!(
            rtx.seq == 1 || rtx.seq == 2,
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

        // CWND is no longer forced to INIT_CWND; set_send_rate recomputes it.
        space.set_send_rate(PosR::new(128.0).unwrap());
        assert!(space.cwnd().get() >= INIT_CWND);
    }

    #[test]
    fn outage_recovery_clamps_cwnd_until_fresh_sample() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        settle_rtt_at(&mut space, t0);

        // Establish a large cwnd by setting a high send rate.
        space.set_send_rate(PosR::new(1_000_000.0).unwrap());
        let large_cwnd = space.cwnd().get();
        assert!(large_cwnd > INIT_CWND, "cwnd={large_cwnd}");

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
            INIT_CWND,
            "cwnd should clamp to INIT_CWND"
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
        assert_eq!(p1.seq, 1);

        // Second probe fires after another PTO window.
        let t2 = t1 + ms(210);
        assert!(space.has_tail_probe(t2), "second probe should be due");
        let p2 = space.tail_probe(t2).unwrap();
        assert_eq!(p2.seq, 1);

        // Full-RTO retransmit off the lowered floor fires around 300 ms after the
        // second probe.  It must not record a congestion loss event because the
        // tail probe already owns the tail-latency signal.
        let t3 = t2 + ms(330);
        let rtx = space
            .rtx(t3)
            .expect("full RTO should fire after tail probes");
        assert_eq!(rtx.seq, 1);
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
        assert_eq!(rtx.seq, 1);

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
        // increments seq 0's sack_passes by 1.  After three SACKs the
        // threshold (3) is reached.
        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));
        assert_eq!(sack_passes(&space, 0), 3, "seq 0 should have 3 sack passes");

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
        assert_eq!(rtx.seq, 0, "fast loss should target the starved seq 0");
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
        // SACKed past seq 0.  The sack_passes counter still reaches 3
        // (tracking is unconditional), but the declaration must not fire.
        sack_one(&mut space, 1, t0 + ms(10));
        sack_one(&mut space, 2, t0 + ms(11));
        sack_one(&mut space, 3, t0 + ms(12));
        assert_eq!(sack_passes(&space, 0), 3, "sack passes are always tracked");

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
        assert_eq!(rtx.seq, 0);
        // The retransmit is recorded as a fast-loss rtx for reordering检测.
        assert_eq!(
            space
                .send_wnd
                .get(&0)
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
        assert_eq!(sack_passes(&space, new_front), 3);

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
        assert_eq!(rtx.seq, 0);

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
            .get(&0)
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
        // seq 0 is below out_of_order_seq_end (= 3), so the reorder-window
        // path applies to it.
        assert!(space.out_of_order_seq_end > 0);
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
        let stock_deadline = t0 + stock;
        let rtx_t = fast_deadline + ms(50);
        assert!(
            rtx_t < stock_deadline,
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
        assert_eq!(rtx.seq, 0);

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
        assert_eq!(space.deferred_losses[0].seq, 0);
        assert_eq!(space.deferred_losses[0].stock_deadline, stock_deadline);
    }

    #[test]
    fn jitter_cap_deferred_loss_event_records_at_stock_deadline_if_still_unacked() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::with_jitter_cap(true);
        let (fast, stock) = settle_high_jitter_f2(&mut space, t0);

        starve_seq0_with_sacks(&mut space, t0);

        let fast_deadline = t0 + fast;
        let stock_deadline = t0 + stock;
        let rtx_t = fast_deadline + ms(50);
        let rtx = space.rtx(rtx_t).expect("jitter-cap fast rtx fires");
        assert_eq!(rtx.seq, 0);
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event at rtx time (deferred)"
        );

        // Still before the stock deadline: polling must not record anything.
        let before_deadline = stock_deadline - ms(10);
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
        space.poll_deferred_loss(stock_deadline + ms(1));
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
    fn jitter_cap_repaired_before_stock_deadline_is_not_double_counted() {
        let t0 = Instant::now();
        let mut space = PktSendSpace::with_jitter_cap(true);
        let (fast, stock) = settle_high_jitter_f2(&mut space, t0);

        starve_seq0_with_sacks(&mut space, t0);

        let fast_deadline = t0 + fast;
        let stock_deadline = t0 + stock;
        let rtx_t = fast_deadline + ms(50);
        let rtx = space.rtx(rtx_t).expect("jitter-cap fast rtx fires");
        assert_eq!(rtx.seq, 0);
        assert!(
            !space.loss_event_window.raw_has_loss_event(),
            "no loss event at rtx time (deferred)"
        );
        assert_eq!(space.deferred_losses.len(), 1);

        // The original (or the repair) is acked before the stock deadline:
        // this was reordering, not loss.  The deferred entry must be cancelled
        // and no loss event recorded.  Polling after the deadline must NOT
        // double-count a later repair either.
        let ack_t = stock_deadline - ms(100);
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
        space.poll_deferred_loss(stock_deadline + ms(1));
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
        let stock_deadline = t0 + stock;
        let between = fast_deadline + ms(50);
        assert!(between < stock_deadline);
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

        let rtx_t = stock_deadline + ms(1);
        assert!(
            space.has_rtx(rtx_t),
            "toggle off: stock rtx fires at stock deadline"
        );
        let rtx = space.rtx(rtx_t).expect("stock rtx at stock deadline");
        assert_eq!(rtx.seq, 0);
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
            send_packet(&mut space, send_start + ms(i * 1));
        }

        let mut t = send_start + ms(22 * 1 + 1);
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
            ack_one(&mut space, seq, t + ms((seq * 1) as u64));
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
        assert_eq!(send_packet(&mut space, t0), 0);
        assert_eq!(ack_one(&mut space, 0, t0 + ms(10)), 1);
        assert_eq!(send_packet(&mut space, t0 + ms(11)), 1);
        let mut last_ack = t0 + ms(11);
        for i in 0..8 {
            let send_at = t0 + ms(12) + Duration::from_secs(i * 5);
            let seq = send_packet(&mut space, send_at);
            let balls = [
                crate::sack::AckBall {
                    start: 0,
                    size: std::num::NonZeroU64::new(1).unwrap(),
                },
                crate::sack::AckBall {
                    start: 2,
                    size: std::num::NonZeroU64::new(seq - 1).unwrap(),
                },
            ];
            let mut acked = Vec::new();
            last_ack = send_at + ms(1);
            space.ack(
                crate::sack::AckBallSequence::new(&balls),
                &mut acked,
                last_ack,
            );
            assert_eq!(acked.len(), 1, "each heartbeat SACK must be fresh");
            assert_eq!(space.send_wnd.start().copied(), Some(1));
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
        assert_eq!(send_packet(&mut space, t0), 0);
        let mut last_ack = t0;
        for i in 0..8 {
            let send_at = t0 + ms(1) + Duration::from_secs(i * 5);
            let seq = send_packet(&mut space, send_at);
            let balls = [crate::sack::AckBall {
                start: 1,
                size: std::num::NonZeroU64::new(seq).unwrap(),
            }];
            let mut acked = Vec::new();
            last_ack = send_at + ms(1);
            space.ack(
                crate::sack::AckBallSequence::new(&balls),
                &mut acked,
                last_ack,
            );
            assert_eq!(acked.len(), 1, "each heartbeat SACK must be fresh");
            assert_eq!(space.send_wnd.start().copied(), Some(0));
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
}
