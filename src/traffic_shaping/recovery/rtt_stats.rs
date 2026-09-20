use std::collections::VecDeque;
use std::time::Duration;

use super::reorder_tolerance::gate_variance::GateVarianceState;
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

/// Number of most recent two-sided variance estimates spanned by the
/// reorder lane's *steady-state jitter* floor.  A self-inflicted queue raises
/// the variance with the very growth the delay gate must detect, so the gate's
/// jitter margin cannot be the trending variance itself; it is the minimum
/// over a short sliding window instead.  The window must be short enough that
/// a genuinely jittery path (where every window contains an excursion) keeps a
/// real margin, and long enough to bridge the quiet stretch before a queue
/// builds.
const GATE_VAR_WINDOW_SAMPLES: usize = 16;

/// Number of RTT samples after a step-class jump during which the gate falls
/// back to the trending variance.  A path step and an instantaneous queue fill
/// are indistinguishable in a single sample, so the steady-state window must
/// not be discarded on the jump (that would let a queue fill seed its own
/// inflated margin).  The fallback only bridges the first samples, before the
/// floor window has begun to move; once the floor is rising the caller's
/// [`crate::traffic_shaping::core::QueueGrowth`] uses the trending margin for
/// the rest of the transition.
const GATE_STEP_TRANSIENT_SAMPLES: usize = 3;

/// Number of RTT samples after which the windowed steady-state jitter is
/// trusted.  A freshly-established connection has no jitter history: the
/// window fills with the smooth slow-start ramp (near-zero variance), so its
/// minimum reads ~0 and a first reorder/jitter excursion would be mistaken for
/// a queue.  Until the window has this much history the trending margin is
/// used, exactly as before the steady-state estimate existed.  The interactive
/// lane's early history is sparse, so this spans well past connection setup.
///
/// Consumed by the reorder-tolerance policy
/// ([`GateVarianceState::steady`]).
pub(crate) const GATE_VAR_MATURE_SAMPLES: usize = 128;

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
    /// reorder-tolerant lane; [`GateVarianceState::steady`] caps the gate at twice
    /// this upward component so a downward-only overshoot cannot license a
    /// larger queue than the upward evidence justifies, while symmetric
    /// jitter and a genuine latency step keep the full two-sided margin.
    gate_up_rtt_var: Duration,
    /// The floating-point accumulator behind [`Self::gate_up_rtt_var`].
    gate_up_rtt_var_secs: f64,
    /// Most recent [`GATE_VAR_WINDOW_SAMPLES`] two-sided variance estimates,
    /// oldest first.  The reorder lane's gate margin is their minimum: the
    /// steady-state jitter of the quietest recent stretch, which a transient
    /// queue can raise for at most one window before the quiet samples age
    /// out.  See [`GateVarianceState::steady`].
    gate_var_samples: VecDeque<Duration>,
    /// Samples remaining in a raw step transient, during which the windowed
    /// steady-state jitter is not trusted because the floor window has not yet
    /// tracked the new path RTT.  See [`GATE_STEP_TRANSIENT_SAMPLES`].
    step_transient_remaining: usize,
    /// Total RTT samples recorded; the steady-state window is only trusted
    /// after [`GATE_VAR_MATURE_SAMPLES`].
    samples_recorded: usize,
    rto: RtxTimer,
}

/// The delay gate's jitter evidence for one interval.
///
/// Two estimates of the same jitter are offered because they fail in opposite
/// directions: the trending variance reacts immediately to a path step but is
/// inflated by a self-inflicted queue, while the windowed steady-state floor
/// is immune to the queue but lags a step.  [`QueueGrowth`](crate::traffic_shaping::core::QueueGrowth)
/// uses the steady-state value for its ordinary gate (picking the trending one
/// while its own RTT floor is stepping), and the trending value for the
/// persistent-queue timer -- the drain trigger -- because the windowed minimum
/// is not a common-mode estimate across flows sharing a bottleneck.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GateJitter {
    /// Jitter for a settled path: the windowed steady-state minimum, or the
    /// trending value during a raw step transient.
    pub(crate) steady: Duration,
    /// The trending two-sided margin (with the `2 * up` cap applied).
    pub(crate) trending: Duration,
}

impl GateJitter {
    /// A single jitter value for a lane that does not distinguish the two
    /// estimates (the stock/bulk lane).
    pub(crate) fn uniform(rttvar: Duration) -> Self {
        Self {
            steady: rttvar,
            trending: rttvar,
        }
    }
}

impl RttStats {
    pub(crate) fn new() -> Self {
        Self {
            min_rtt: None,
            recent_min_rtt: None,
            recent_rtt_samples: VecDeque::new(),
            gate_up_rtt_var: Duration::ZERO,
            gate_up_rtt_var_secs: 0.0,
            gate_var_samples: VecDeque::new(),
            step_transient_remaining: 0,
            samples_recorded: 0,
            rto: RtxTimer::new(),
        }
    }

    /// Record the two-sided variance estimate into the steady-state window.
    fn record_gate_var_sample(&mut self) {
        self.gate_var_samples.push_back(self.rto.smooth_rtt_var());
        if self.gate_var_samples.len() > GATE_VAR_WINDOW_SAMPLES {
            self.gate_var_samples.pop_front();
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
        self.record_gate_var_sample();
        self.samples_recorded = self.samples_recorded.saturating_add(1);
        // A step-class jump arms a short fallback to the trending variance; the
        // steady-state window is deliberately *not* discarded, so an
        // instantaneous queue fill cannot seed its own inflated margin.  A
        // queue ramp grows by a fraction of the smoothed RTT per sample, so it
        // stops re-arming this after the first jump.
        self.step_transient_remaining = self.step_transient_remaining.saturating_sub(1);
        if rtt.saturating_sub(pre_srtt) > pre_srtt {
            self.step_transient_remaining = GATE_STEP_TRANSIENT_SAMPLES;
        }
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
        self.gate_var_samples.clear();
        self.record_gate_var_sample();
        self.step_transient_remaining = 0;
        self.samples_recorded = 0;
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

    /// Both jitter estimates for the delay gate; see [`GateJitter`].
    ///
    /// The selection lives in the reorder-tolerance policy
    /// ([`GateVarianceState::steady`] / [`GateVarianceState::trending`]); the
    /// raw estimator stays here because the same filter feeds the RTO.
    pub(crate) fn gate_jitter(&self) -> GateJitter {
        let smooth_rtt_var = self.rto.smooth_rtt_var();
        let state = self.gate_variance_state();
        GateJitter {
            steady: state.steady(smooth_rtt_var),
            trending: state.trending(smooth_rtt_var),
        }
    }

    /// A read-only view of the raw gate estimators handed to the reorder-lane
    /// policy ([`GateVarianceState`]).
    fn gate_variance_state(&self) -> GateVarianceState<'_> {
        GateVarianceState {
            upward: self.gate_up_rtt_var,
            step_transient_remaining: self.step_transient_remaining,
            samples_recorded: self.samples_recorded,
            window: &self.gate_var_samples,
        }
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

    use super::{GATE_VAR_MATURE_SAMPLES, RttStats};
    use crate::traffic_shaping::recovery::fast_loss::armed_against_min_rtt;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// The steady-state gate variance the policy selects (see
    /// [`GateVarianceState::steady`]).
    fn gate_rtt_var(stats: &RttStats) -> Duration {
        stats.gate_variance_state().steady(stats.smooth_rtt_var())
    }

    /// The trending gate variance (see [`GateVarianceState::trending`]).
    fn trending_gate_rtt_var(stats: &RttStats) -> Duration {
        stats.gate_variance_state().trending(stats.smooth_rtt_var())
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
            armed_against_min_rtt(stats.min_rtt(), stats.smooth_rtt(), stats.smooth_rtt_var()),
            "queue-premised min-RTT gate must be armed under queue inflation - \
             srtt clears the floor by the jitter tolerance and beyond"
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
            !armed_against_min_rtt(stats.min_rtt(), stats.smooth_rtt(), stats.smooth_rtt_var()),
            "min-RTT gate must stay off when jitter dwarfs the propagation floor"
        );
    }

    #[test]
    fn min_rtt_gate_abstains_before_any_sample() {
        let stats = RttStats::new();
        assert!(stats.min_rtt().is_none());
        assert!(!armed_against_min_rtt(
            stats.min_rtt(),
            stats.smooth_rtt(),
            stats.smooth_rtt_var()
        ));
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
            gate_rtt_var(&stats),
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
            gate_rtt_var(&stats) < stats.smooth_rtt_var(),
            "the gate must discount the downward overshoot: gate={:?} two-sided={:?}",
            gate_rtt_var(&stats),
            stats.smooth_rtt_var()
        );
    }

    /// A self-inflicted queue raises the trending variance, but the gate must
    /// use the windowed steady-state floor instead: after a quiet mature
    /// stretch, a ramp that inflates the trending variance must not raise the
    /// gate toward it.
    #[test]
    fn steady_state_window_ignores_a_transient_variance_spike() {
        let mut stats = RttStats::new();
        for _ in 0..(GATE_VAR_MATURE_SAMPLES + 32) {
            stats.record_rtt(ms(100));
        }
        // Ramp up without any single jump doubling the smoothed RTT, so no
        // step transient is armed; the trending variance inflates with it and
        // the quiet pre-ramp samples stay in the window.
        for rtt in [130u64, 160, 190, 220, 250] {
            stats.record_rtt(ms(rtt));
        }
        assert!(
            stats.smooth_rtt_var() > ms(30),
            "the ramp must inflate the trending variance: {:?}",
            stats.smooth_rtt_var()
        );
        assert!(
            trending_gate_rtt_var(&stats) > ms(30),
            "the trending margin is what the queue would inflate: {:?}",
            trending_gate_rtt_var(&stats)
        );
        assert!(
            gate_rtt_var(&stats) < trending_gate_rtt_var(&stats) / 2,
            "the windowed steady-state floor must ignore the ramp: gate={:?} trending={:?}",
            gate_rtt_var(&stats),
            trending_gate_rtt_var(&stats)
        );
    }

    /// Before the window has enough history it is not trusted: the gate keeps
    /// the trending margin, so a first jitter excursion cannot be mistaken for
    /// a queue.
    #[test]
    fn steady_state_window_is_not_trusted_before_maturity() {
        let mut stats = RttStats::new();
        for _ in 0..8 {
            stats.record_rtt(ms(50));
        }
        assert_eq!(
            gate_rtt_var(&stats),
            trending_gate_rtt_var(&stats),
            "an immature connection must keep the trending margin"
        );
    }
}
