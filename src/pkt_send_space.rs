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

use crate::{pkt_recv_space::MAX_NUM_RECVING_PKTS, rto::RtxTimer, sack::AckBallSequence, tlp::TailLossProber};

pub const INIT_CWND: usize = 16;
pub(crate) const CWND_SEND_RATE_SCALE: usize = 8;

#[derive(Debug)]
pub struct PktSendSpace {
    send_wnd: SendWnd<u64, Option<TxingPkt>>,
    num_txing: usize,
    min_rtt: Option<Duration>,
    rto: RtxTimer,
    reused_buf: ObjPool<Vec<u8>>,
    cwnd: NonZeroUsize,
    /// Detect if the peer has died
    resp_wait_start: Option<Instant>,
    /// Has any packet ever been acked or have we ever had to wait?
    ever_progressed: bool,
    /// Start of the latest interval during which no forward progress was made.
    progress_wait_start: Option<Instant>,
    out_of_order_seq_end: u64,
    /// Any sequence after this does not participate in data loss analysis.
    max_pipe_seq: MinNoneOptCmp<u64>,
    loss_event_window: LossEventWindow,
    /// RFC 8985 tail-loss-probe engine.
    tlp: TailLossProber,

    // outage recovery epoch
    /// Start of the current outage-recovery epoch, if one is open.
    recovery_start: Option<Instant>,
    /// Packets originally sent before this instant are considered pre-outage.
    /// Their retransmission losses do not mark a congestion event.
    outage_loss_cut: Option<Instant>,

    // reused buffers
    unacked_buf: Vec<u64>,
    ack_buf: Vec<u64>,
}
impl PktSendSpace {
    pub fn new() -> Self {
        Self {
            send_wnd: SendWnd::new(0),
            num_txing: 0,
            min_rtt: None,
            rto: RtxTimer::new(),
            reused_buf: buf_pool(Some(MAX_NUM_RECVING_PKTS)),
            cwnd: NonZeroUsize::new(INIT_CWND).unwrap(),
            resp_wait_start: None,
            out_of_order_seq_end: 0,
            max_pipe_seq: MinNoneOptCmp(None),
            loss_event_window: LossEventWindow::new(),
            tlp: TailLossProber::new(),
            recovery_start: None,
            outage_loss_cut: None,
            ever_progressed: false,
            progress_wait_start: None,
            unacked_buf: vec![],
            ack_buf: vec![],
        }
    }

    fn no_progress_for(&self, now: Instant) -> Option<Duration> {
        Some(now.duration_since(self.progress_wait_start?))
    }

    fn update_min_rtt(&mut self, rtt: Duration) {
        self.min_rtt = Some(match self.min_rtt {
            Some(min_rtt) => min_rtt.min(rtt),
            None => rtt,
        });
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

    pub fn next_seq(&self) -> u64 {
        *self.send_wnd.next().unwrap()
    }

    pub fn smooth_rtt(&self) -> Duration {
        self.rto.smooth_rtt()
    }

    pub fn smooth_rtt_var(&self) -> Duration {
        self.rto.smooth_rtt_var()
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
        Some(now.duration_since(self.resp_wait_start?))
    }

    pub fn ack(&mut self, recved: AckBallSequence<'_>, acked: &mut Vec<PacketState>, now: Instant) {
        let next_unacked = self.send_wnd.start().or(self.send_wnd.next()).copied();
        let peer_waiting_for_acked_pkts =
            next_unacked.is_some_and(|next_unacked| recved.first_unacked() < next_unacked);
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
            // Forward progress: we just delivered packets to the peer.
            self.ever_progressed = true;
            self.progress_wait_start = None;
            self.tlp.reset();
        }
        for &s in &self.ack_buf {
            let p = self.send_wnd.get_mut(&s).unwrap();
            let p = p.take().unwrap();
            self.num_txing -= 1;
            if s == *self.send_wnd.start().unwrap() {
                self.send_wnd.pop().unwrap();
                self.send_wnd.pop_none();
            }

            // Do not use the ACK arrival time as an RTT sample: `sent_time` is
            // refreshed on every retransmission, so an ACK for a retransmitted
            // packet would be Karn-ambiguous; ACK decimation can hold ACKs for
            // up to ACK_FLUSH_AGE, skewing the sample; and this path bypasses
            // the outage-recovery censoring in `sample_rtt`.  All RTT samples
            // come from peer-echoed send timestamps via `sample_rtt`.
            self.reused_buf.put(p.data);
            acked.push(p.stats);
        }
        self.loss_event_window
            .record_delivered(delivered, now, self.smooth_rtt());
        if peer_waiting_for_acked_pkts && delivered == 0 {
            return;
        }
        if self.send_wnd.is_empty() {
            self.resp_wait_start = None;
            self.progress_wait_start = None;
            return;
        }
        self.resp_wait_start = Some(now);
        // If we have made progress before and the window is not empty, we are
        // now waiting for the next round of progress.  If progress_wait_start is
        // None because the ACK just made progress, this sets it to the end of
        // this ACK; subsequent zero-progress ACKs will keep resp_wait alive but
        // will not reset progress_wait_start.
        if self.ever_progressed && self.progress_wait_start.is_none() {
            self.progress_wait_start = Some(now);
        }
    }

    pub fn sample_rtt(&mut self, rtt: Duration, now: Instant) {
        // During an outage-recovery epoch, censor stale RTT samples taken across
        // the outage.  A sample is stale if it was sent before the outage cut.
        // The cut persists after recovery_start is cleared so that late echoes
        // of pre-outage packets are still discarded.
        if let Some(cut) = self.outage_loss_cut {
            let sent_at = now.checked_sub(rtt);
            if sent_at.is_none_or(|sent_at| sent_at < cut) {
                // Echoes from before the cut keep the recovery_start epoch open
                // if it is still active, otherwise they are simply dropped.
                return;
            }
            // A fresh post-outage sample ends the epoch and re-seeds sRTT.
            // outage_loss_cut intentionally stays in place until a new outage
            // re-arms it; this prevents late echoes of pre-outage packets from
            // corrupting the freshly seeded sRTT.
            if self.recovery_start.take().is_some() {
                self.update_min_rtt(rtt);
                self.rto.reset_to(rtt);
                return;
            }
        }

        self.update_min_rtt(rtt);
        self.rto.set(rtt);
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
        self.tlp.is_due(p.sent_time, &self.rto, self.min_rtt, now)
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
        let rto = self.tlp.rto(&self.rto);
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
                rto,
                rto_from_tail_probe: true,
            };
        }
        Some(Pkt { seq, data: &p.data })
    }

    pub fn send(&mut self, data: Vec<u8>, stats: PacketState, now: Instant) -> Pkt<'_> {
        let s = *self.send_wnd.next().unwrap();

        self.max_pipe_seq = MinNoneOptCmp(Some(s));

        let p = TxingPkt {
            stats,
            sent_time: now,
            rtxed: false,
            considered_new_in_cwnd: false,
            data,
            rto: self.rto.rto(),
            rto_from_tail_probe: false,
        };

        self.send_wnd.push(Some(p));
        self.num_txing += 1;
        self.tlp.reset();
        if self.resp_wait_start.is_none() {
            self.resp_wait_start = Some(now);
        }
        // Sending a new packet does not by itself demonstrate forward progress;
        // progress is measured by ACK delivery.  However, once we have made prior
        // progress and are sending into an empty pipe, start the stall clock so a
        // subsequent outage can be detected.
        if self.ever_progressed && self.progress_wait_start.is_none() {
            self.progress_wait_start = Some(now);
        }

        Pkt {
            seq: s,
            data: &self.send_wnd.get(&s).unwrap().as_ref().unwrap().data,
        }
    }

    pub fn has_rtx(&self, now: Instant) -> bool {
        let out_of_order_seq_end = self.out_of_order_seq_end;
        let outage_loss_cut = self.outage_loss_cut;
        Self::unacked(&self.send_wnd)
            .take(self.cwnd.get())
            .any(|(s, p)| {
                let is_seq_out_of_order = SeqOutOfOrder(s < out_of_order_seq_end);
                let is_pre_outage_loss = Self::is_pre_outage_loss(p, outage_loss_cut);
                p.is_rtx(
                    is_seq_out_of_order,
                    is_pre_outage_loss,
                    self.rto.reorder_window(),
                    now,
                )
            })
    }

    pub fn rtx(&mut self, now: Instant) -> Option<Pkt<'_>> {
        let out_of_order_seq_end = self.out_of_order_seq_end;
        let reorder_window = self.rto.reorder_window();
        let outage_loss_cut = self.outage_loss_cut;
        for (s, p) in Self::unacked_mut(&mut self.send_wnd).take(self.cwnd.get()) {
            let is_seq_out_of_order = SeqOutOfOrder(s < out_of_order_seq_end);
            let is_pre_outage_loss = Self::is_pre_outage_loss(p, outage_loss_cut);
            let should_rtx = p.is_rtx(is_seq_out_of_order, is_pre_outage_loss, reorder_window, now);
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
                    rto: self.rto.rto(),
                    rto_from_tail_probe: false,
                };
            }
            if !already_rtxed && !pre_outage_loss && !tail_probe_loss {
                let smooth_rtt = self.rto.smooth_rtt();
                self.loss_event_window.record_lost(1, now, smooth_rtt);
            }
            let p = Pkt {
                seq: s,
                data: &p.data,
            };
            return Some(p);
        }
        None
    }

    fn is_pre_outage_loss(p: &TxingPkt, outage_loss_cut: Option<Instant>) -> bool {
        let Some(cut) = outage_loss_cut else {
            return false;
        };
        // The original transmission must have happened before the cut.  A later
        // retransmission naturally expires the exemption because `sent_time` is
        // overwritten each time we rtx.
        p.sent_time < cut
    }

    pub fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    pub fn set_send_rate(&mut self, send_rate: PosR<f64>) {
        let cwnd = self.rto.smooth_rtt().as_secs_f64() * send_rate.get();
        let cwnd = cwnd.round() as usize;
        let cwnd = cwnd * CWND_SEND_RATE_SCALE;
        let cwnd = 1.max(cwnd);
        // While an outage-recovery epoch is open, clamp cwnd to INIT_CWND so a
        // just-restored path is not flooded before fresh RTT samples can seed
        // the congestion state.
        let cwnd = if self.recovery_start.is_some() {
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
        self.recovery_start.is_some()
    }

    pub(crate) fn outage_cut(&self) -> Option<Instant> {
        self.outage_loss_cut
    }

    pub fn detect_outage_recovery(&mut self, now: Instant) -> bool {
        // Outage detection is keyed on forward progress stalls, not merely the
        // absence of any ACK datagram.  Zero-progress duplicate ACKs can keep the
        // total-silence clock alive without delivering data; we require that the
        // sender has made prior progress and has now stopped making progress.
        if !self.ever_progressed {
            return false;
        }

        let no_progress_for = match self.no_progress_for(now) {
            Some(d) => d,
            None => return false,
        };
        let rto = self.rto.rto();
        if no_progress_for < rto {
            return false;
        }

        // Conditions that detect a sustained outage:
        //  - we have been waiting for forward progress for at least one RTO,
        //  - and either there has been a loss event or the silence is long enough
        //    that a clean link would have produced an ACK by now.
        let has_loss_event = self.loss_event_window.raw_has_loss_event();
        let loss_or_long_silence = has_loss_event || no_progress_for >= rto * 2;
        if !loss_or_long_silence {
            // A clean long silence (no loss event) needs two RTOs to declare outage.
            return false;
        }

        // Re-arming an already-closed epoch refreshes the outage cut without
        // re-seeding sRTT; the fresh post-outage sample still owns that.
        let rearming = self.recovery_start.is_some();
        self.recovery_start = Some(now);
        self.outage_loss_cut = Some(now);
        if !rearming {
            // New epoch: start with a seed sRTT of one RTO and clear prior loss
            // history so the post-outage path is treated as fresh.
            self.rto.reset_to(self.rto.rto());
            self.loss_event_window.reset(now, self.rto.smooth_rtt());
        }
        // Do not touch `cwnd` here; the epoch clamps it inside `set_send_rate`.
        true
    }

    pub fn clear_outage_recovery(&mut self) {
        // Do not clear outage_loss_cut here; it is intentionally left intact so
        // that late echoes of pre-outage packets are still discarded.  However,
        // a second outage-length stall inside an already-closed epoch should
        // refresh the cut.  The caller signals this by calling detect again;
        // detect_outage_recovery will re-arm recovery_start and update the cut.
        self.recovery_start = None;
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
        for p in Self::unacked(&self.send_wnd)
            .map(|(_, v)| v)
            .take(self.cwnd.get())
        {
            let t = p.next_rto_time();
            let t = min_next_poll_time.map(|min| min.min(t)).unwrap_or(t);
            min_next_poll_time = Some(t);
        }
        if let Some(seq) = self.tail_seq()
            && let Some(Some(p)) = self.send_wnd.get(&seq)
        {
            if let Some(t) = self.tlp.next_probe_time(p.sent_time, &self.rto, self.min_rtt) {
                min_next_poll_time = Some(min_next_poll_time.map(|min| min.min(t)).unwrap_or(t));
            }
        }
        min_next_poll_time
    }

    pub fn rto_duration(&self) -> Duration {
        self.rto.rto()
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
    pub rto: Duration,
    /// True only if this packet's RTO was last re-armed by a tail-loss probe
    /// using the tightened post-probe floor.  A subsequent full-RTO retransmit
    /// still fires, but it must not record a congestion loss event because the
    /// probe itself already signalled the tail episode.
    pub rto_from_tail_probe: bool,
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
}

struct SeqOutOfOrder(pub bool);

#[derive(Debug, Clone)]
pub struct Pkt<'a> {
    pub seq: u64,
    pub data: &'a [u8],
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
        space.send(data, stats, now);
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
        space.set_send_rate(PosR::new(crate::reliable_layer::INIT_SEND_RATE).unwrap());
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
        space.set_send_rate(PosR::new(crate::reliable_layer::INIT_SEND_RATE).unwrap());
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
}
