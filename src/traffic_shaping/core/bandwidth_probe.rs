use std::time::{Duration, Instant};

const BW_PROBE_GAIN: f64 = 0.5;

/// The delivery-scaled multiplicative probe's largest no-loss gain factor,
/// `1 + BW_PROBE_GAIN`: the biggest per-probe rate increase the
/// [`ProbeIncrease::Multiplicative`] shape can propose from a delivery sample.
/// Exposed as the single authority for the reorder lane's spurious-sample cap.
/// The cap bounds that delivery-scaled part and adds a lane's additive step
/// back afterwards, so this factor remains exactly the multiplicative probe's
/// maximum and the cap cannot silently fall below (and clip) it.
pub(crate) const ORDINARY_PROBE_MAX_GAIN: f64 = 1.0 + BW_PROBE_GAIN;

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

/// The shape of the ordinary probe's per-probe increase.
///
/// The shape is the lane's, not the sample's: a contention lane must increase
/// by an absolute amount so two flows sharing a bottleneck converge to equal
/// rates, while a lane with no competing flow keeps the delivery-scaled
/// multiplicative probe.  Keeping the choice here (rather than inside the
/// arithmetic) documents that the multiplicative and additive probes are two
/// distinct policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeIncrease {
    /// Delivery-scaled multiplicative probe (`delivery * 1.5`): the dedicated
    /// and reorder lanes' shape.  It grows a flow in proportion to what it
    /// already gets, so it has no convergence force between contending flows.
    Multiplicative,
    /// Absolute additive-increase probe (`max(current, delivery) + step`): the
    /// shared contention lane's shape.  The step is independent of the flow's
    /// current share, so two flows on one bottleneck converge to equal rates
    /// under the multiplicative drain.
    Additive,
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
    /// `increase` selects the lane's shape.  [`ProbeIncrease::Multiplicative`]
    /// proposes `delivery * 1.5` (scaled by the survival fraction) lifted to at
    /// least `current + additive`; [`ProbeIncrease::Additive`] proposes the
    /// larger of the current and delivered rates plus the lane's absolute
    /// `additive` step, so a starved flow climbs by a fixed amount per unit
    /// time instead of by a fraction of a share it does not have.  The
    /// feedback interval still gates every accepted increase; a dedicated lane
    /// passes `Multiplicative` and `additive = 0.0`, keeping the historical
    /// purely multiplicative probe.
    ///
    /// For the multiplicative shape the target can exceed `current + additive`
    /// (it is `max(delivery * 1.5, current + additive)`); for the additive
    /// shape it is exactly `max(current, delivery) + additive`.
    ///
    /// Targets that cannot raise the current rate do not consume the feedback
    /// interval.  An accepted increase starts a new interval at `now`.
    // The arguments are the policy's whole input set; bundling them into a
    // request struct is a separate house-style change.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn target(
        &mut self,
        current: f64,
        delivery_rate: f64,
        loss_event_rate: Option<f64>,
        control_rtt: Duration,
        increase: ProbeIncrease,
        additive: f64,
        now: Instant,
    ) -> f64 {
        let probed = match increase {
            ProbeIncrease::Multiplicative => {
                Self::proposed_rate(delivery_rate, loss_event_rate).max(current + additive)
            }
            ProbeIncrease::Additive => current.max(delivery_rate) + additive,
        };
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
    fn ordinary_probe_max_gain_is_the_probe_own_no_loss_gain() {
        // The exported cap authority must describe the probe exactly: a
        // no-loss probe of `d` proposes `d * ORDINARY_PROBE_MAX_GAIN`, so a
        // caller bounding a probe target from this constant can never clip the
        // legitimate probe it exists to allow.
        let d = 137.0;
        assert_eq!(
            OrdinaryBandwidthProbe::proposed_rate(d, Some(0.0)),
            d * ORDINARY_PROBE_MAX_GAIN
        );
        assert!(OrdinaryBandwidthProbe::proposed_rate(d, Some(0.9)) <= d * ORDINARY_PROBE_MAX_GAIN);
    }

    #[test]
    fn shared_additive_step_raises_the_target_past_the_multiplicative_proposal() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        // At zero loss the multiplicative shape proposes 1.5 * delivery.  A
        // starved shared flow's absolute additive step must win over it: the
        // target is `max(current, delivery) + step`, not a fraction of the
        // share it already holds.
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_millis(100),
                ProbeIncrease::Additive,
                200.0,
                t0,
            ),
            300.0,
        );
        // The same inputs on the multiplicative shape keep the historical
        // 1.5x delivery proposal.
        probe.reset();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_millis(100),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0,
        );
    }

    #[test]
    fn additive_shape_refills_to_the_delivered_rate_before_adding_the_step() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        // Post-drain recovery: the flow sends 100 but the path still delivers
        // 400.  The additive shape refills to delivery (not a multiple of it)
        // and adds the absolute step.
        assert_eq!(
            probe.target(
                100.0,
                400.0,
                Some(0.0),
                Duration::from_millis(100),
                ProbeIncrease::Additive,
                50.0,
                t0,
            ),
            450.0,
        );
    }

    #[test]
    fn increase_waits_for_one_current_feedback_rtt() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_millis(100),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Some(0.0),
                Duration::from_millis(100),
                ProbeIncrease::Multiplicative,
                0.0,
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
                ProbeIncrease::Multiplicative,
                0.0,
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
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_millis(10),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
        assert_eq!(
            probe.target(
                140.0,
                100.0,
                Some(0.0),
                Duration::from_millis(30),
                ProbeIncrease::Multiplicative,
                0.0,
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
                ProbeIncrease::Multiplicative,
                0.0,
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
            probe.target(
                300.0,
                100.0,
                Some(0.0),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            300.0
        );
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
    }

    #[test]
    fn outage_reset_allows_an_immediate_probe() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
        probe.reset();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
    }

    #[test]
    fn nonzero_loss_scales_down_the_probe_target() {
        let t0 = Instant::now();
        let mut probe = OrdinaryBandwidthProbe::new();
        // At zero loss the historical full gain holds: target = D * 1.5.
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.0),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
        // A missing loss sample means no evidence of loss: same full gain.
        probe.reset();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                None,
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            150.0
        );
        // 10% loss: target = D * (1 + 0.5 * 0.9) = 1.45 D.
        probe.reset();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.1),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            145.0
        );
        // Loss at the delay-control block threshold: target = 1.4 D.
        probe.reset();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(0.2),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            140.0
        );
        // A malformed rate above 1 cannot invert the gain: target stays >= D.
        probe.reset();
        assert_eq!(
            probe.target(
                100.0,
                100.0,
                Some(2.0),
                Duration::from_secs(1),
                ProbeIncrease::Multiplicative,
                0.0,
                t0,
            ),
            100.0
        );
        assert_eq!(
            OrdinaryBandwidthProbe::proposed_rate(100.0, Some(0.1)),
            145.0
        );
    }
}
