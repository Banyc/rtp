use std::time::{Duration, Instant};

const BW_PROBE_GAIN: f64 = 0.5;

/// Scale a multiplicative probe gain by the survival fraction `(1 - loss)`.
///
/// A loss-suppressed delivery rate `D ~= (1 - lr) * R` must not compound the
/// full loss-free gain: the probe would keep proposing `1.5 * D` with no
/// ceiling and ratchet the accepted rate above the link's real deliverable
/// rate.  A missing loss sample (no evidence of loss) keeps the historical
/// full gain; the clamp keeps a malformed rate from inverting the gain.
pub(super) fn loss_scaled_gain(gain: f64, loss_event_rate: Option<f64>) -> f64 {
    let survival = (1.0 - loss_event_rate.unwrap_or(0.0)).clamp(0.0, 1.0);
    gain * survival
}

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

    pub(crate) fn proposed_rate(delivery_rate: f64, loss_event_rate: Option<f64>) -> f64 {
        delivery_rate + delivery_rate * loss_scaled_gain(BW_PROBE_GAIN, loss_event_rate)
    }

    /// Return the rate target for one ordinary probe decision.
    ///
    /// Targets that cannot raise the current rate do not consume the feedback
    /// interval.  An accepted increase starts a new interval at `now`.
    pub(crate) fn target(
        &mut self,
        current: f64,
        delivery_rate: f64,
        loss_event_rate: Option<f64>,
        control_rtt: Duration,
        now: Instant,
    ) -> f64 {
        let probed = Self::proposed_rate(delivery_rate, loss_event_rate);
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
            probe.target(100.0, 100.0, Some(0.0), Duration::from_millis(100), t0),
            150.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Some(0.0),
                Duration::from_millis(100),
                t0 + Duration::from_millis(99),
            ),
            140.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Some(0.0),
                Duration::from_millis(100),
                t0 + Duration::from_millis(100),
            ),
            150.0
        );
    }

    #[test]
    fn rising_control_rtt_extends_the_outstanding_wait() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.0), Duration::from_millis(10), t0),
            150.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Some(0.0),
                Duration::from_millis(30),
                t0 + Duration::from_millis(10),
            ),
            140.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Some(0.0),
                Duration::from_millis(30),
                t0 + Duration::from_millis(30),
            ),
            150.0
        );
    }

    #[test]
    fn nonincrease_does_not_consume_the_feedback_interval() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(300.0, 100.0, Some(0.0), Duration::from_secs(1), t0),
            300.0
        );
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.0), Duration::from_secs(1), t0),
            150.0
        );
    }

    #[test]
    fn outage_reset_allows_an_immediate_probe() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.0), Duration::from_secs(1), t0),
            150.0
        );
        probe.reset();
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.0), Duration::from_secs(1), t0),
            150.0
        );
    }

    #[test]
    fn nonzero_loss_scales_down_the_probe_target() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        // At zero loss the historical full gain holds: target = D * 1.5.
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.0), Duration::from_secs(1), t0),
            150.0
        );
        // A missing loss sample means no evidence of loss: same full gain.
        probe.reset();
        assert_eq!(
            probe.target(100.0, 100.0, None, Duration::from_secs(1), t0),
            150.0
        );
        // 10% loss: target = D * (1 + 0.5 * 0.9) = 1.45 D.
        probe.reset();
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.1), Duration::from_secs(1), t0),
            145.0
        );
        // Loss at the delay-control block threshold: target = 1.4 D.
        probe.reset();
        assert_eq!(
            probe.target(100.0, 100.0, Some(0.2), Duration::from_secs(1), t0),
            140.0
        );
        // A malformed rate above 1 cannot invert the gain: target stays >= D.
        probe.reset();
        assert_eq!(
            probe.target(100.0, 100.0, Some(2.0), Duration::from_secs(1), t0),
            100.0
        );
        assert_eq!(
            OrdinaryBandwidthProbe::proposed_rate(100.0, Some(0.1)),
            145.0
        );
    }
}
