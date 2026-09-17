//! The reorder lane's gate-jitter policy: the two jitter estimates the delay
//! gate chooses between, derived from the raw RTT estimators.
//!
//! The estimates fail in opposite directions — the trending variance reacts
//! immediately to a path step but is inflated by a self-inflicted queue, while
//! the windowed steady-state floor is immune to the queue but lags a step — so
//! the policy offers both and the caller (whose own RTT-floor-rise check knows
//! whether the path is stepping) picks.  The raw state is owned by
//! [`RttStats`](crate::traffic_shaping::recovery::rtt_stats::RttStats) and
//! handed over as a read-only [`GateVarianceState`].

use std::collections::VecDeque;
use std::time::Duration;

use super::super::rtt_stats::GATE_VAR_MATURE_SAMPLES;

/// A read-only view of the raw gate estimators the reorder policy decides over.
/// `RttStats` owns the state and hands the policy a borrow; the policy never
/// mutates it.
#[derive(Debug)]
pub(crate) struct GateVarianceState<'a> {
    /// One-sided (upward) EWMA of the raw RTT deviation from the pre-update
    /// smoothed RTT.  A queue can only raise RTT *above* the path baseline, so
    /// a downward excursion (a reordered packet echoing a stale timestamp, or
    /// the smoothed RTT overshooting while a queue drains) is not evidence of
    /// queue growth.
    pub(crate) upward: Duration,
    /// Samples remaining in a raw step transient, during which the windowed
    /// steady-state jitter is not trusted because the floor window has not yet
    /// tracked the new path RTT.
    pub(crate) step_transient_remaining: usize,
    /// Total RTT samples recorded; the steady-state window is only trusted
    /// after [`GATE_VAR_MATURE_SAMPLES`].
    pub(crate) samples_recorded: usize,
    /// Most recent two-sided variance estimates, oldest first.  The reorder
    /// lane's gate margin is their minimum: the steady-state jitter of the
    /// quietest recent stretch, which a transient queue can raise for at most
    /// one window before the quiet samples age out.
    pub(crate) window: &'a VecDeque<Duration>,
}

impl GateVarianceState<'_> {
    /// The trending two-sided margin with the `2 * up` cap applied: reacts
    /// immediately to a path step, but is inflated by a self-inflicted queue.
    pub(crate) fn trending(&self, smooth_rtt_var: Duration) -> Duration {
        self.upward.mul_f64(2.0).min(smooth_rtt_var)
    }

    /// Robust RTT variance for the delay-based queue gate: the *steady-state*
    /// jitter floor, capped at twice the one-sided upward component.
    ///
    /// A self-inflicted queue raises the smoothed variance with the very
    /// backlog the delay gate must detect, so the trending two-sided
    /// `smooth_rtt_var` lets the gate hold a queue at the depth that inflated
    /// its own tolerance.  Instead the margin is the minimum two-sided variance
    /// over the recent sample window: the quietest recent stretch, which a
    /// rising queue cannot raise until its own inflation has displaced every
    /// quiet sample in the window.  On a genuinely jittery path (the reorder
    /// lane's reorder echoes) every window still contains an excursion, so the
    /// minimum stays at the real jitter and no spurious drain appears.  The
    /// separate `2 * up` cap still discounts a downward-only overshoot.
    ///
    /// During a raw step transient the trending value is returned because the
    /// floor window has not begun to move yet; the caller's floor-rise check
    /// covers the rest of the transition.
    pub(crate) fn steady(&self, smooth_rtt_var: Duration) -> Duration {
        if self.step_transient_remaining > 0 || self.samples_recorded < GATE_VAR_MATURE_SAMPLES {
            return self.trending(smooth_rtt_var);
        }
        let steady_state = self.window.iter().copied().min().unwrap_or(smooth_rtt_var);
        self.upward.mul_f64(2.0).min(steady_state)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::time::Duration;

    use super::{GATE_VAR_MATURE_SAMPLES, GateVarianceState};

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn trending_caps_at_twice_the_upward_component() {
        let window = VecDeque::new();
        let state = GateVarianceState {
            upward: ms(10),
            step_transient_remaining: 0,
            samples_recorded: 0,
            window: &window,
        };
        assert_eq!(state.trending(ms(100)), ms(20));
        assert_eq!(state.trending(ms(15)), ms(15));
    }

    #[test]
    fn steady_falls_back_to_trending_until_mature_and_out_of_transient() {
        let window = VecDeque::from([ms(50), ms(60)]);
        let immature = GateVarianceState {
            upward: ms(10),
            step_transient_remaining: 0,
            samples_recorded: GATE_VAR_MATURE_SAMPLES - 1,
            window: &window,
        };
        assert_eq!(immature.steady(ms(100)), immature.trending(ms(100)));

        let transient = GateVarianceState {
            step_transient_remaining: 2,
            ..immature
        };
        assert_eq!(transient.steady(ms(100)), transient.trending(ms(100)));
    }

    #[test]
    fn steady_uses_the_window_minimum_once_mature() {
        let window = VecDeque::from([ms(50), ms(60), ms(90)]);
        let state = GateVarianceState {
            upward: ms(10),
            step_transient_remaining: 0,
            samples_recorded: GATE_VAR_MATURE_SAMPLES,
            window: &window,
        };
        assert_eq!(
            state.steady(ms(100)),
            ms(20),
            "2*up caps the window minimum"
        );

        let wide = GateVarianceState {
            upward: ms(40),
            ..state
        };
        assert_eq!(
            wide.steady(ms(100)),
            ms(50),
            "the quietest recent sample wins"
        );
    }
}
