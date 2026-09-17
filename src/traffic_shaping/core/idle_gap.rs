//! Idle-gap continuity authority.
//!
//! Every stateful controller timer measures a *continuous* stretch of an
//! observed condition (a standing queue, an ineffective drain).  A sample that
//! arrives after the lane went quiet must not count the quiet stretch as part
//! of that condition: a lane that stopped sending was not holding a queue or
//! draining.  Historically each timer carried its own "last observation"
//! timestamp and reset itself, so a newly added timer could forget the reset
//! (or duplicate a diverging threshold) and report a multi-second condition
//! built out of an idle stretch.
//!
//! This module owns the single definition of "the observation gap that voids
//! continuity" and the observation clock that gates every timer.  The gap
//! verdict and the resets are one operation ([`IdleGap::observe`]), so a timer
//! cannot be observed without being reset; registering a new timer is adding
//! it to the reset bundle.

use std::time::{Duration, Instant};

use super::gentle::{GENTLE_ENTER_MIN, GENTLE_ENTER_RTTS};

/// The observation cadence gap long enough to void the continuity of any
/// controller timer: the larger of three control RTTs and one second, the same
/// stretch a genuine standing queue must survive to enter gentle mode.  A
/// backlogged lane samples far more often than this, so a real condition is
/// unaffected; a lane that went quiet for at least this long was not
/// continuously in the condition.
pub(crate) fn idle_gap_threshold(control_rtt: Duration) -> Duration {
    control_rtt.mul_f64(GENTLE_ENTER_RTTS).max(GENTLE_ENTER_MIN)
}

/// A stateful episode whose time accumulation is voided by an idle gap.
///
/// Implementors clear only the episode's start/latch; they never touch the
/// mode or cooldown the episode was feeding, so only the accumulated time is
/// discarded.
pub(crate) trait IdleContinuity {
    fn break_idle_continuity(&mut self);
}

impl IdleContinuity for Option<Instant> {
    fn break_idle_continuity(&mut self) {
        *self = None;
    }
}

impl<T: IdleContinuity + ?Sized> IdleContinuity for &mut T {
    fn break_idle_continuity(&mut self) {
        (**self).break_idle_continuity();
    }
}

impl<A: IdleContinuity, B: IdleContinuity> IdleContinuity for (A, B) {
    fn break_idle_continuity(&mut self) {
        self.0.break_idle_continuity();
        self.1.break_idle_continuity();
    }
}

/// The single observation clock a controller uses to detect an idle gap and
/// void continuity in the same step.
#[derive(Debug, Default)]
pub(crate) struct IdleGap {
    last: Option<Instant>,
}

impl IdleGap {
    pub(crate) fn new() -> Self {
        Self { last: None }
    }

    pub(crate) fn reset(&mut self) {
        self.last = None;
    }

    /// Record an observation at `now` and, when it followed an idle gap, void
    /// the continuity of every registered timer.  Returns whether the gap
    /// occurred.
    pub(crate) fn observe(
        &mut self,
        timers: &mut impl IdleContinuity,
        now: Instant,
        control_rtt: Duration,
    ) -> bool {
        let gapped = self.last.is_some_and(|previous| {
            now.saturating_duration_since(previous) >= idle_gap_threshold(control_rtt)
        });
        self.last = Some(now);
        if gapped {
            timers.break_idle_continuity();
        }
        gapped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gap threshold is the one authority: three control RTTs floored at
    /// one second.
    #[test]
    fn threshold_is_three_control_rtts_floored_at_one_second() {
        assert_eq!(
            idle_gap_threshold(Duration::from_millis(50)),
            Duration::from_secs(1),
            "a low-RTT path uses the one-second floor"
        );
        assert_eq!(
            idle_gap_threshold(Duration::from_millis(600)),
            Duration::from_millis(1800),
            "a high-RTT path uses three control RTTs"
        );
    }

    /// A representative *new* timer registered with the shared clock resets
    /// uniformly with the existing one: both are voided by the same verdict,
    /// so a future timer cannot be observed without being reset.
    #[test]
    fn every_registered_timer_is_reset_on_the_same_verdict() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let mut gap = IdleGap::new();
        let mut first_since: Option<Instant> = Some(t0);
        let mut second_since: Option<Instant> = Some(t0);

        // Inside the threshold neither timer is reset.
        assert!(!gap.observe(
            &mut (&mut first_since, &mut second_since),
            t0 + control_rtt,
            control_rtt
        ));
        assert!(first_since.is_some() && second_since.is_some());

        // One gap at the threshold voids both, in one step.
        let resumed = t0 + control_rtt + idle_gap_threshold(control_rtt);
        assert!(gap.observe(
            &mut (&mut first_since, &mut second_since),
            resumed,
            control_rtt
        ));
        assert!(first_since.is_none(), "the first timer must reset");
        assert!(
            second_since.is_none(),
            "the registered second timer must reset"
        );
    }

    /// The first observation of a connection has no predecessor, so it is
    /// never a gap.
    #[test]
    fn first_observation_is_never_a_gap() {
        let t0 = Instant::now();
        let mut gap = IdleGap::new();
        let mut timer: Option<Instant> = Some(t0);
        assert!(!gap.observe(&mut &mut timer, t0, Duration::from_millis(100)));
        assert!(timer.is_some());
    }
}
