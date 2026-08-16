use std::time::{Duration, Instant};

const BW_PROBE_GAIN: f64 = 1.0;

/// Ordinary low-loss bandwidth probing paced by transport feedback.
///
/// A delivery sample may offer many probe opportunities inside one RTT.  Only
/// the first increase is applied; later opportunities keep the current rate
/// until the previous increase has had one current control RTT to affect the
/// peer's feedback.  A larger current RTT therefore extends an outstanding
/// wait instead of letting an increase through on stale timing.
#[derive(Debug)]
pub(crate) struct OrdinaryBandwidthProbe {
    last_increase_at: Option<Instant>,
}

impl OrdinaryBandwidthProbe {
    pub(crate) fn new() -> Self {
        Self {
            last_increase_at: None,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.last_increase_at = None;
    }

    pub(crate) fn proposed_rate(delivery_rate: f64) -> f64 {
        delivery_rate + delivery_rate * BW_PROBE_GAIN
    }

    /// Return the rate target for one ordinary probe decision.
    ///
    /// Targets that cannot raise the current rate do not consume the feedback
    /// interval.  An accepted increase starts a new interval at `now`.
    pub(crate) fn target(
        &mut self,
        current: f64,
        delivery_rate: f64,
        control_rtt: Duration,
        now: Instant,
    ) -> f64 {
        let probed = Self::proposed_rate(delivery_rate);
        if probed <= current {
            return current;
        }
        if self
            .last_increase_at
            .is_some_and(|last| now.saturating_duration_since(last) < control_rtt)
        {
            return current;
        }
        if probed > current {
            self.last_increase_at = Some(now);
        }
        probed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increase_waits_for_one_current_feedback_rtt() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(100.0, 100.0, Duration::from_millis(100), t0),
            200.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Duration::from_millis(100),
                t0 + Duration::from_millis(99),
            ),
            140.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Duration::from_millis(100),
                t0 + Duration::from_millis(100),
            ),
            200.0
        );
    }

    #[test]
    fn rising_control_rtt_extends_the_outstanding_wait() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(100.0, 100.0, Duration::from_millis(10), t0),
            200.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Duration::from_millis(30),
                t0 + Duration::from_millis(10),
            ),
            140.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Duration::from_millis(30),
                t0 + Duration::from_millis(30),
            ),
            200.0
        );
    }

    #[test]
    fn nonincrease_does_not_consume_the_feedback_interval() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(300.0, 100.0, Duration::from_secs(1), t0),
            300.0
        );
        assert_eq!(
            probe.target(100.0, 100.0, Duration::from_secs(1), t0),
            200.0
        );
    }

    #[test]
    fn outage_reset_allows_an_immediate_probe() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(100.0, 100.0, Duration::from_secs(1), t0),
            200.0
        );
        probe.reset();
        assert_eq!(
            probe.target(100.0, 100.0, Duration::from_secs(1), t0),
            200.0
        );
    }
}
