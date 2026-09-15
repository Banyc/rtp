use std::collections::VecDeque;
use std::time::Duration;

use super::rto::RtxTimer;

/// Number of most recent RTT samples spanned by the rolling minimum — the
/// current-path floor used as the observed-reordering suspicion window, so a
/// single early outlier (or a stale lifetime minimum recorded under
/// congestion) cannot pin that window for the connection's life.
const RECENT_MIN_RTT_SAMPLES: usize = 8;

/// Smoothing weight for the gate's one-sided RTT-deviation estimator.  It
/// matches the RTO filter's `BETA` so the gate reacts on the same timescale as
/// the smoothed RTT it is compared against.
const GATE_RTT_VAR_BETA: f64 = 0.25;

/// Bundle of RTT statistics: the smoothed-RTT / RTO timer, the lifetime
/// minimum RTT, and a rolling minimum over the most recent samples.
#[derive(Debug)]
pub(crate) struct RttStats {
    min_rtt: Option<Duration>,
    /// Minimum RTT over only the most recent [`RECENT_MIN_RTT_SAMPLES`]
    /// samples — the *current* estimator floor.  Unlike the lifetime
    /// `min_rtt` it recovers when the path improves (or an early outlier ages
    /// out), so the reorder-suspicion check does not stay pinned by stale
    /// history.
    recent_min_rtt: Option<Duration>,
    /// The most recent [`RECENT_MIN_RTT_SAMPLES`] RTT samples, oldest first.
    recent_rtt_samples: VecDeque<Duration>,
    /// One-sided (upward) EWMA of the raw RTT deviation from the pre-update
    /// smoothed RTT, consumed only by the delay-based queue gate.
    ///
    /// A queue can only raise RTT *above* the path baseline, so a downward
    /// excursion (a reordered packet echoing a stale timestamp, or the
    /// smoothed RTT overshooting while a queue drains) is not evidence of
    /// queue growth.  The RTO's two-sided `smooth_rtt_var` counts those
    /// downward excursions and so inflates the gate tolerance on a
    /// reorder-tolerant lane; [`Self::gate_rtt_var`] caps the gate at twice
    /// this upward component so a downward-only overshoot cannot license a
    /// larger queue than the upward evidence justifies, while symmetric
    /// jitter and a genuine latency step keep the full two-sided margin.
    gate_up_rtt_var: Duration,
    /// The floating-point accumulator behind [`Self::gate_up_rtt_var`].
    gate_up_rtt_var_secs: f64,
    rto: RtxTimer,
}

impl RttStats {
    pub(crate) fn new() -> Self {
        Self {
            min_rtt: None,
            recent_min_rtt: None,
            recent_rtt_samples: VecDeque::new(),
            gate_up_rtt_var: Duration::ZERO,
            gate_up_rtt_var_secs: 0.0,
            rto: RtxTimer::new(),
        }
    }

    /// Maintain the rolling minimum from the most recent samples.
    fn refresh_recent_min_rtt(&mut self) {
        self.recent_min_rtt = Some(
            self.recent_rtt_samples
                .iter()
                .copied()
                .min()
                .expect("the sample ring is never empty while refreshing"),
        );
    }

    /// Record an RTT sample: update the SRTT filter, the lifetime minimum,
    /// and the rolling recent minimum.
    pub(crate) fn record_rtt(&mut self, rtt: Duration) {
        // The gate's one-sided deviation is measured against the smoothed RTT
        // *before* this sample is folded in, exactly like the RTO's `rtt_var`.
        let pre_srtt = self.rto.smooth_rtt();
        let upward = rtt.saturating_sub(pre_srtt).as_secs_f64();
        self.gate_up_rtt_var_secs =
            (1. - GATE_RTT_VAR_BETA) * self.gate_up_rtt_var_secs + GATE_RTT_VAR_BETA * upward;
        self.gate_up_rtt_var = Duration::from_secs_f64(self.gate_up_rtt_var_secs);
        self.rto.set(rtt);
        self.min_rtt = Some(match self.min_rtt {
            Some(m) => m.min(rtt),
            None => rtt,
        });
        self.recent_rtt_samples.push_back(rtt);
        if self.recent_rtt_samples.len() > RECENT_MIN_RTT_SAMPLES {
            self.recent_rtt_samples.pop_front();
        }
        self.refresh_recent_min_rtt();
    }

    /// Record the lifetime minimum and re-seed the SRTT filter from scratch.
    ///
    /// Used when an outage-recovery epoch closes: the fresh post-outage RTT
    /// reflects a new network state, so we must not merge it with the stale
    /// pre-outage EWMA via `record_rtt`.  The rolling recent minimum is
    /// rebuilt from that fresh sample for the same reason.
    pub(crate) fn record_min_and_reseed_rto(&mut self, rtt: Duration) {
        self.min_rtt = Some(match self.min_rtt {
            Some(m) => m.min(rtt),
            None => rtt,
        });
        self.rto.reset_to(rtt);
        self.recent_rtt_samples.clear();
        self.recent_rtt_samples.push_back(rtt);
        self.recent_min_rtt = Some(rtt);
        self.gate_up_rtt_var = Duration::ZERO;
        self.gate_up_rtt_var_secs = 0.0;
    }

    pub(crate) fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    /// The *current* estimator floor for the reorder-suspicion check: the
    /// minimum over the most recent [`RECENT_MIN_RTT_SAMPLES`] samples.
    /// `None` before any sample exists (the check then abstains).
    pub(crate) fn recent_min_rtt(&self) -> Option<Duration> {
        self.recent_min_rtt
    }

    pub(crate) fn smooth_rtt(&self) -> Duration {
        self.rto.smooth_rtt()
    }

    pub(crate) fn smooth_rtt_var(&self) -> Duration {
        self.rto.smooth_rtt_var()
    }

    /// Robust RTT variance for the delay-based queue gate: the two-sided
    /// `smooth_rtt_var`, capped at twice the one-sided upward component.
    ///
    /// A queue raises RTT above the baseline; the downward half of the
    /// two-sided variance is therefore not queue evidence.  On the
    /// reorder-tolerant lane those downward excursions (reorder low echoes,
    /// and the smoothed RTT overshooting while a queue drains) can dominate
    /// the variance and inflate the gate until it tolerates the standing queue
    /// it is meant to detect.  Capping at `2 * up` keeps the full margin for
    /// symmetric jitter and a genuine step (where `up >= down`) but discards a
    /// downward-only overshoot.  See the [`Self::gate_up_rtt_var`] field docs.
    pub(crate) fn gate_rtt_var(&self) -> Duration {
        self.gate_up_rtt_var
            .mul_f64(2.0)
            .min(self.rto.smooth_rtt_var())
    }

    pub(crate) fn rto_duration(&self) -> Duration {
        self.rto.rto()
    }

    pub(crate) fn raw_rto(&self) -> Duration {
        self.rto.raw_rto()
    }

    pub(crate) fn reorder_window(&self) -> Duration {
        self.rto.reorder_window()
    }

    /// Fast reorder window used only to schedule retransmission when the
    /// `RTP_JITTER_CAP` toggle is on (see [`RtxTimer::fast_reorder_window`]).
    pub(crate) fn fast_reorder_window(&self) -> Duration {
        self.rto.fast_reorder_window()
    }

    /// Whether the structural low-jitter gate for evidence-gated fast loss
    /// is armed (see [`RtxTimer::fast_loss_armed`]).
    pub(crate) fn fast_loss_armed(&self) -> bool {
        self.rto.fast_loss_armed()
    }

    /// Queue-independent fast-loss arming: the smoothed RTT variation is
    /// below the lifetime minimum RTT — the propagation floor recorded while
    /// the path was still uncongested — so a SACK gap is evidence of loss
    /// rather than jitter-driven reordering.
    ///
    /// The srtt-relative gate [`Self::fast_loss_armed`] (`K*rttvar <
    /// srtt/4`) is disarmed exactly when a bulk sender fills the bottleneck
    /// queue, because queueing inflates both `srtt` and `rttvar`.  The
    /// lifetime `min_rtt` does not inflate with queue depth (it is a minimum
    /// over samples, including the uncongested ones), so this gate keeps the
    /// evidence-gated fast-loss path armed under the bulk + loss conditions
    /// where repair latency matters most.  `false` before any sample exists
    /// (the gate abstains; the caller keeps the structural gate).
    pub(crate) fn fast_loss_armed_against_min_rtt(&self) -> bool {
        self.min_rtt
            .is_some_and(|min_rtt| self.smooth_rtt_var() < min_rtt)
    }

    pub(crate) fn reset_rto(&mut self, rtt: Duration) {
        self.rto.reset_to(rtt);
    }
}

impl Default for RttStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RttStats;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Bulk + loss fills the bottleneck queue: echoed-timestamp RTT samples
    /// climb to 60-90 ms, inflating srtt to ~75 ms and rttvar to ~16 ms.  The
    /// srtt-relative gate (`K*rttvar < srtt/4`) disarms, but the lifetime
    /// min_rtt (30 ms, captured while the path was uncongested) does not
    /// inflate, so the queue-independent gate must still arm.
    #[test]
    fn min_rtt_gate_arms_under_queue_inflation_where_the_srtt_gate_disarms() {
        let mut stats = RttStats::new();
        // Propagation floor recorded on the empty queue.
        stats.record_rtt(ms(30));
        for _ in 0..30 {
            stats.record_rtt(ms(90));
            stats.record_rtt(ms(60));
        }

        assert_eq!(stats.min_rtt(), Some(ms(30)), "propagation floor must hold");
        let srtt = stats.smooth_rtt();
        let rttvar = stats.smooth_rtt_var();
        assert!(srtt >= ms(70) && srtt <= ms(80), "srtt={srtt:?}");
        assert!(rttvar >= ms(12) && rttvar <= ms(20), "rttvar={rttvar:?}");

        assert!(
            !stats.fast_loss_armed(),
            "srtt-relative gate must be disarmed by queue inflation"
        );
        assert!(
            stats.fast_loss_armed_against_min_rtt(),
            "queue-independent min-RTT gate must be armed"
        );
    }

    #[test]
    fn min_rtt_gate_stays_disarmed_on_a_high_jitter_link() {
        let mut stats = RttStats::new();
        for _ in 0..20 {
            stats.record_rtt(ms(100));
            stats.record_rtt(ms(900));
        }
        assert!(!stats.fast_loss_armed(), "srtt-relative gate disarmed");
        assert!(
            !stats.fast_loss_armed_against_min_rtt(),
            "min-RTT gate must stay off when jitter dwarfs the propagation floor"
        );
    }

    #[test]
    fn min_rtt_gate_abstains_before_any_sample() {
        let stats = RttStats::new();
        assert!(stats.min_rtt().is_none());
        assert!(!stats.fast_loss_armed_against_min_rtt());
    }

    /// A genuine latency step is upward-only: every sample sits above the
    /// lagging smoothed RTT, so the one-sided upward component is at least the
    /// downward one and the gate keeps the full two-sided margin.  This is the
    /// property that lets the reorder lane's gate stay robust without
    /// abandoning a sustained step.
    #[test]
    fn gate_variance_keeps_the_full_margin_on_an_upward_step() {
        let mut stats = RttStats::new();
        for _ in 0..20 {
            stats.record_rtt(ms(50));
        }
        for _ in 0..3 {
            stats.record_rtt(ms(200));
        }
        assert_eq!(
            stats.gate_rtt_var(),
            stats.smooth_rtt_var(),
            "an upward step must keep the two-sided gate margin"
        );
    }

    /// A downward overshoot (the smoothed RTT left high while the queue
    /// drains, or a reordered packet echoing a low RTT) inflates the two-sided
    /// `smooth_rtt_var` but carries no queue evidence.  The gate caps at twice
    /// the upward component, so it collapses back toward the upward-only
    /// deviation and does not license a larger queue than the path shows.
    #[test]
    fn gate_variance_discounts_a_downward_overshoot() {
        let mut stats = RttStats::new();
        for _ in 0..20 {
            stats.record_rtt(ms(50));
        }
        // Push the smoothed RTT high, then let the samples fall back below it.
        for _ in 0..8 {
            stats.record_rtt(ms(200));
        }
        for _ in 0..8 {
            stats.record_rtt(ms(50));
        }
        assert!(
            stats.smooth_rtt_var() > ms(40),
            "the two-sided variance must stay inflated by the downward return: {:?}",
            stats.smooth_rtt_var()
        );
        assert!(
            stats.gate_rtt_var() < stats.smooth_rtt_var(),
            "the gate must discount the downward overshoot: gate={:?} two-sided={:?}",
            stats.gate_rtt_var(),
            stats.smooth_rtt_var()
        );
    }
}
