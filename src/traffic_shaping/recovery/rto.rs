use core::time::Duration;

use primitive::ops::float::NonNegR;

/// ref: <https://datatracker.ietf.org/doc/html/rfc6298>
#[derive(Debug, Clone)]
pub struct RtxTimer {
    smooth_rtt: NonNegR<f64>,
    smooth_rtt_var: NonNegR<f64>,
    smooth_rtt_duration: Duration,
    smooth_rtt_var_duration: Duration,
    raw_rto: Duration,
    /// The *corroborated* tail-repair deadline's pre-floor value: the general
    /// RTO with its variance margin replaced by the path's measured reorder
    /// tolerance (see [`Self::corroborated_repair_rto`]).
    corroborated_repair_rto: Duration,
    rto: Duration,
    reorder_window: Duration,
    fast_reorder_window: Duration,
    fast_loss_armed: bool,
    first_measured: bool,
}
impl RtxTimer {
    /// RFC 6298's 1 s minimum RTO floor, kept for the *general* RTO path.
    ///
    /// One scoped departure: once the tail-loss prober's budget for the
    /// current tail episode is spent, the tail loss is corroborated by two
    /// unanswered probes and the send-space retransmission deadline uses the
    /// prober's tightened post-probe floor instead of this one
    /// (`TailLossProber::TAIL_PROBED_MIN_RTO`, see
    /// `PktSendSpace::repair_rto`).  That path also tightens the variance
    /// margin from `K * rttvar` to the path's measured reorder tolerance (see
    /// [`Self::corroborated_repair_rto`]), so it is the floor *and* the margin
    /// that are scoped to the corroborated path.  The floor still governs
    /// every path with no probe evidence, so an unmeasured or merely-idle
    /// connection is unaffected.
    const MIN_RTO: Duration = Duration::from_secs(1);
    const K: f64 = 4.;
    const BETA: f64 = 1. / 4.;
    const ALPHA: f64 = 1. / 8.;

    pub fn new() -> Self {
        let mut timer = Self {
            smooth_rtt: NonNegR::new(Self::MIN_RTO.as_secs_f64()).unwrap(),
            smooth_rtt_var: NonNegR::new(0.0).unwrap(),
            smooth_rtt_duration: Duration::ZERO,
            smooth_rtt_var_duration: Duration::ZERO,
            raw_rto: Duration::ZERO,
            corroborated_repair_rto: Duration::ZERO,
            rto: Duration::ZERO,
            reorder_window: Duration::ZERO,
            fast_reorder_window: Duration::ZERO,
            fast_loss_armed: false,
            first_measured: false,
        };
        timer.recompute_derived();
        timer
    }

    pub fn set(&mut self, rtt: Duration) {
        let rtt_secs = rtt.as_secs_f64();
        if !self.first_measured {
            self.first_measured = true;
            self.smooth_rtt = NonNegR::new(rtt_secs).unwrap();
            self.smooth_rtt_var = NonNegR::new(rtt_secs / 2.).unwrap();
            self.recompute_derived();
            return;
        }

        let rtt_var = (self.smooth_rtt.get() - rtt_secs).abs();
        let smooth_rtt_var = (1. - Self::BETA) * self.smooth_rtt_var.get() + Self::BETA * rtt_var;
        self.smooth_rtt_var = NonNegR::new(smooth_rtt_var).unwrap();

        let smooth_rtt = (1. - Self::ALPHA) * self.smooth_rtt.get() + Self::ALPHA * rtt_secs;
        self.smooth_rtt = NonNegR::new(smooth_rtt).unwrap();
        self.recompute_derived();
    }

    // pub fn rto(&self, granularity: Duration) -> Duration {
    //     let tol = Self::K * self.smooth_rtt_var.get();
    //     let rto = self.smooth_rtt.get() + granularity.as_secs_f64().max(tol);
    //     Duration::from_secs_f64(rto).max(Self::MIN_RTO)
    // }
    pub fn rto(&self) -> Duration {
        self.rto
    }

    /// RTO formula value without any floor applied.
    ///
    /// Consumed only by the measurement probes and pins that separate the
    /// estimator's raw value from the floor — no production path reads it any
    /// more now that the corroborated tail-repair deadline is its own value
    /// ([`Self::corroborated_repair_rto`]).
    #[cfg(test)]
    pub(crate) fn raw_rto(&self) -> Duration {
        self.raw_rto
    }

    /// The variance margin of the *corroborated* tail-repair deadline.
    ///
    /// RFC 6298's `K * rttvar` (`K = 4`) is the ~4-sigma bound that keeps the
    /// general RTO safe while the path's RTT distribution is still unknown.
    /// Once the tail prober's two probes for the current tail episode have
    /// gone unanswered the tail's loss *is* corroborated, so the deadline no
    /// longer has to cover the estimator's tail: it only has to clear the
    /// path's own measured reordering — the same `max(rttvar, srtt / 4)`
    /// margin the reorder window already uses, i.e. `rttvar` *without* the
    /// `K` — the quantity the prober's own probe window is capped by.
    ///
    /// Two structural bounds make the tightening safe in both directions:
    ///
    /// - it is never later than [`Self::raw_rto`].  Where the variance is
    ///   already small relative to sRTT the reorder margin's `srtt / 4` floor
    ///   dominates and would otherwise *loosen* the deadline on a long-RTT
    ///   path; tightening the rung must never push a repair later.  This is
    ///   why the general RTO is the upper bound and not replaced outright.
    /// - it is never earlier than the measured sRTT itself, because the
    ///   margin is added to `srtt`: a corroborated repair is never declared
    ///   before one round trip has demonstrably elapsed.
    pub(crate) fn corroborated_repair_rto(&self) -> Duration {
        self.corroborated_repair_rto
    }

    /// Reset the SRTT filter to a fixed value, keeping the same RTO calculation.
    ///
    /// Used when starting an outage-recovery epoch: the first post-outage RTT
    /// sample should seed the congestion state as if the connection were fresh.
    pub fn reset_to(&mut self, rtt: Duration) {
        self.first_measured = false;
        self.set(rtt);
    }

    /// Reordering window used by the fast-retransmit path.
    ///
    /// RACK-style: `srtt + max(K * rttvar, srtt / 4)`, capped at the full RTO.
    /// No `MIN_RTO` floor so that on stable low-RTT links the window stays tight.
    pub fn reorder_window(&self) -> Duration {
        self.reorder_window
    }

    /// Fast reorder window used only to schedule retransmission of
    /// out-of-order-passed packets when the `RTP_JITTER_CAP` toggle is on:
    /// `srtt + max(rttvar, srtt/4)` (rttvar NOT multiplied by `K`), capped at
    /// the full RTO.  The loss-event accounting deadline still uses
    /// [`reorder_window`].  On low-jitter links (`K*rttvar ≤ srtt/4`) the
    /// `srtt/4` floor dominates both windows, so `fast_reorder_window ==
    /// reorder_window` and the toggle is a no-op there.
    pub fn fast_reorder_window(&self) -> Duration {
        self.fast_reorder_window
    }

    /// Whether the structural low-jitter gate is armed: `K * rttvar <
    /// srtt / 4`, i.e. the `srtt / 4` floor dominates the reorder window
    /// because path jitter is small relative to sRTT.  This is the
    /// srtt-relative safety gate for evidence-gated fast loss declaration —
    /// under high jitter reordering mimics loss, so the fast path must stay
    /// off.  It is only one half of the caller's composite arming decision:
    /// [`crate::traffic_shaping::recovery::rtt_stats::RttStats`] also arms
    /// from the queue-independent lifetime minimum RTT (`rttvar < min_rtt`),
    /// because queueing inflates srtt and rttvar together while `min_rtt`
    /// stays at the uncongested propagation floor.
    pub fn fast_loss_armed(&self) -> bool {
        self.fast_loss_armed
    }

    pub fn smooth_rtt(&self) -> Duration {
        self.smooth_rtt_duration
    }

    pub fn smooth_rtt_var(&self) -> Duration {
        self.smooth_rtt_var_duration
    }

    fn recompute_derived(&mut self) {
        let smooth_rtt = self.smooth_rtt.get();
        let smooth_rtt_var = self.smooth_rtt_var.get();
        let srtt = Duration::from_secs_f64(smooth_rtt);
        let rttvar = Duration::from_secs_f64(smooth_rtt_var);
        let raw_rto = Duration::from_secs_f64(smooth_rtt + Self::K * smooth_rtt_var);
        let rto = raw_rto.max(Self::MIN_RTO);
        let quarter = srtt / 4;
        self.smooth_rtt_duration = srtt;
        self.smooth_rtt_var_duration = rttvar;
        self.raw_rto = raw_rto;
        self.corroborated_repair_rto = (srtt + rttvar.max(quarter)).min(raw_rto);
        self.rto = rto;
        self.reorder_window = (srtt + rttvar.mul_f64(Self::K).max(quarter)).min(rto);
        self.fast_reorder_window = (srtt + rttvar.max(quarter)).min(rto);
        self.fast_loss_armed = smooth_rtt_var * Self::K < smooth_rtt / 4.;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RtxTimer;

    /// The reorder window tracks variance with no `MIN_RTO` floor, and so does
    /// the repair deadline once the tail prober's budget for an episode is
    /// spent: the send space arms the *corroborated* deadline — the general RTO
    /// with RFC 6298's `K * rttvar` term replaced by the path's measured
    /// reorder tolerance `srtt + max(rttvar, srtt / 4)` — floored at
    /// `TAIL_PROBED_MIN_RTO` and never later than the general RTO.  On a
    /// jittered link whose raw RTO sits between that floor and the 1 s
    /// `MIN_RTO`, the ladder therefore steps by the tightened margin and not by
    /// `raw_rto`; the general path (no probe budget spent) keeps the 1 s floor.
    #[test]
    fn reorder_window_tracks_variance_and_the_corroborated_repair_deadline_tracks_the_reorder_margin()
     {
        let mut rto = RtxTimer::new();

        // Steady 100 ms samples: variance collapses, so the reorder window is
        // floored by srtt/4. It stays well below MIN_RTO while rto() is still
        // floored at 1 s.
        for _ in 0..20 {
            rto.set(Duration::from_millis(100));
        }
        let rw = rto.reorder_window();
        assert!(rw >= Duration::from_millis(120), "rw={rw:?}");
        assert!(rw < Duration::from_millis(150), "rw={rw:?}");
        assert!(rto.rto() >= Duration::from_secs(1), "rto={:?}", rto.rto());
        assert!(rw < rto.rto(), "rw={rw:?} rto={:?}", rto.rto());

        // Alternating 100 ms / 900 ms samples: variance grows and K*rttvar
        // dominates, pushing the reorder window above 900 ms. It must never
        // exceed the full RTO.
        let mut rto = RtxTimer::new();
        for _ in 0..10 {
            rto.set(Duration::from_millis(100));
            rto.set(Duration::from_millis(900));
        }
        let rw = rto.reorder_window();
        assert!(rw > Duration::from_millis(900), "rw={rw:?}");
        assert!(rw <= rto.rto(), "rw={rw:?} rto={:?}", rto.rto());

        // A jittered link whose raw RTO lands between the post-probe floor and
        // the 1 s `MIN_RTO` floor.  Fed the same sample sequence, the send
        // space's repair ladder must step by the corroborated deadline — the
        // reorder margin, not the raw RTO and not the 1 s floor — while
        // `rto()` still reports the floored general RTO.
        use std::time::Instant;

        use crate::traffic_shaping::recovery::pkt_send_space::PktSendSpace;
        use crate::traffic_shaping::recovery::tlp::TailLossProber;

        let samples = |i: usize| Duration::from_millis(if i.is_multiple_of(2) { 100 } else { 250 });
        let mut jittered = RtxTimer::new();
        let t0 = Instant::now();
        let mut space = PktSendSpace::new();
        for i in 0..40 {
            jittered.set(samples(i));
            space.sample_rtt(samples(i), t0 + Duration::from_millis(i as u64));
        }
        assert!(
            jittered.raw_rto() > TailLossProber::TAIL_PROBED_MIN_RTO,
            "the fixture's raw RTO {:?} must sit above the post-probe floor, or the reparture is not observable",
            jittered.raw_rto()
        );
        assert!(
            jittered.raw_rto() < RtxTimer::MIN_RTO,
            "the fixture's raw RTO {:?} must sit below the 1 s floor, or the departure is not observable",
            jittered.raw_rto()
        );
        assert!(
            jittered.corroborated_repair_rto() < jittered.raw_rto(),
            "the fixture's corroborated deadline {:?} must be tighter than its raw RTO {:?}, or the variance-bound departure is not observable",
            jittered.corroborated_repair_rto(),
            jittered.raw_rto()
        );
        assert!(
            jittered.corroborated_repair_rto() >= jittered.smooth_rtt(),
            "the corroborated deadline {:?} must never fall below the measured sRTT {:?}",
            jittered.corroborated_repair_rto(),
            jittered.smooth_rtt()
        );
        let post_probe = jittered
            .corroborated_repair_rto()
            .max(TailLossProber::TAIL_PROBED_MIN_RTO);
        let send_t = t0 + Duration::from_secs(1);
        let no_packets_in_flight = space.no_pkts_in_flight();
        let mut connection_state = dre::ConnectionState::new(send_t);
        space.send(
            vec![0u8; 1],
            connection_state.send_packet_2(send_t, no_packets_in_flight),
            None,
            send_t,
        );
        let mut rungs: Vec<u64> = Vec::new();
        for step in 0..4_000u64 {
            let now = send_t + Duration::from_millis(step);
            if space.has_rtx(now) && space.rtx(now).is_some() {
                rungs.push(step);
            } else {
                let _ = space.tail_probe(now);
            }
        }
        assert!(
            rungs.len() >= 3,
            "the replay produced {} full-RTO rungs, too few to measure the steady spacing: rungs={rungs:?}",
            rungs.len()
        );
        let steady_spacing = rungs
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .next_back()
            .unwrap_or(0);
        assert_eq!(
            steady_spacing,
            u64::try_from(post_probe.as_nanos().div_ceil(1_000_000)).unwrap(),
            "M1: with the probe budget spent the repair ladder's steady rung must step by the corroborated deadline ({post_probe:?}), not the raw RTO ({:?}) and not the 1 s `MIN_RTO` floor. rungs={rungs:?}",
            jittered.raw_rto()
        );
        assert!(
            space.rto_duration() >= RtxTimer::MIN_RTO,
            "the general RTO path keeps the 1 s floor, got {:?}",
            space.rto_duration()
        );
    }

    #[test]
    fn fast_reorder_window_equals_stock_on_low_jitter() {
        // On low-jitter links K*rttvar ≤ srtt/4, the srtt/4 floor dominates
        // both windows, so fast_reorder_window == reorder_window (no behavior
        // change with the toggle on).
        let mut rto = RtxTimer::new();
        for _ in 0..20 {
            rto.set(Duration::from_millis(100));
        }
        assert!(rto.fast_loss_armed(), "low-jitter gate must be armed");
        assert_eq!(
            rto.fast_reorder_window(),
            rto.reorder_window(),
            "fast window must equal stock window on low-jitter links"
        );
    }

    #[test]
    fn fast_reorder_window_is_below_stock_on_high_jitter() {
        // On high-jitter links K*rttvar dominates, so the fast window (which
        // uses rttvar NOT multiplied by K) is below the stock window but never
        // below srtt + srtt/4.
        let mut rto = RtxTimer::new();
        for _ in 0..10 {
            rto.set(Duration::from_millis(100));
            rto.set(Duration::from_millis(900));
        }
        assert!(!rto.fast_loss_armed(), "high-jitter gate must be disarmed");
        let fast = rto.fast_reorder_window();
        let stock = rto.reorder_window();
        assert!(fast < stock, "fast={fast:?} must be below stock={stock:?}");
        let srtt = rto.smooth_rtt();
        assert!(
            fast >= srtt + srtt / 4,
            "fast={fast:?} must be ≥ srtt+srtt/4={:?}",
            srtt + srtt / 4
        );
        assert!(
            fast <= rto.rto(),
            "fast={fast:?} must be ≤ rto={:?}",
            rto.rto()
        );
    }

    /// The structural fast-loss gate is a strict comparison: `K * rttvar`
    /// exactly equal to `srtt / 4` leaves the gate disarmed (the jitter is not
    /// *below* the quarter-SRTT margin), so the evidence-gated path stays off
    /// at the boundary.  0.4 and 0.025 are both exactly representable and
    /// `0.025 * 4` is the same `f64` as `0.4 / 4`.
    #[test]
    fn fast_loss_gate_is_disarmed_at_exactly_the_quarter_srtt_boundary() {
        use primitive::ops::float::NonNegR;

        let mut timer = RtxTimer::new();
        timer.smooth_rtt = NonNegR::new(0.4).unwrap();
        timer.smooth_rtt_var = NonNegR::new(0.025).unwrap();
        timer.recompute_derived();
        assert_eq!(
            timer.smooth_rtt_var().mul_f64(RtxTimer::K),
            timer.smooth_rtt() / 4,
            "the sample must land exactly on the engage boundary"
        );
        assert!(
            !timer.fast_loss_armed(),
            "K*rttvar exactly at srtt/4 must leave the structural gate disarmed"
        );
    }

    /// The first RTT sample seeds the filters at `SRTT = R` and
    /// `RTTVAR = R / 2` (RFC 6298), so the initial RTO is `R + 4 * R/2 = 3R`
    /// before the 1 s floor and the initial reorder window is
    /// `R + max(2R, R/4) = 3R`.  Seeding the variance from the sample itself
    /// instead doubles the variance term: it takes the initial RTO to `5R`
    /// and the reorder window to `5R`, which on a 200-400 ms path is a
    /// materially later first retransmission.  The second sample must then
    /// follow the RFC EWMA (`RTTVAR = 3/4 * RTTVAR + 1/4 * |SRTT - R|`) rather
    /// than re-seeding.
    #[test]
    fn the_first_sample_seeds_srtt_and_half_rttvar_then_the_ewma_takes_over() {
        let mut timer = RtxTimer::new();
        let rtt = Duration::from_millis(400);
        timer.set(rtt);
        assert_eq!(timer.smooth_rtt(), rtt, "the first sample IS the sRTT");
        assert_eq!(
            timer.smooth_rtt_var(),
            rtt / 2,
            "the first sample seeds the variance at half the RTT"
        );
        assert_eq!(
            timer.raw_rto(),
            Duration::from_millis(1_200),
            "the pre-floor RTO after one sample is 3R"
        );

        // A repeat sample leaves sRTT where it is and decays the variance by
        // 3/4 toward |sRTT - R| = 0: the seed no longer applies.
        timer.set(rtt);
        assert_eq!(timer.smooth_rtt(), rtt);
        assert_eq!(
            timer.smooth_rtt_var(),
            Duration::from_millis(150),
            "the second sample must follow the 3/4 smoothing, not re-seed"
        );
    }
}
