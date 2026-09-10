//! Persistent-delay drain response with stale-peak floor hysteresis.
//!
//! The drain floor protects a small flow from being drained below a fair share
//! of its own recent delivery peak; after a grace period of continuous binding
//! it decays so a genuine long-term capacity drop can drain past a stale peak.
use std::time::{Duration, Instant};

use super::CongestionDecision;

/// Fraction of the recent peak delivery rate used as a drain-floor target.
pub(crate) const DRAIN_FLOOR_PEAK_FRACTION: f64 = 0.25;

/// Grace period, in control RTTs, before a continuously binding drain floor
/// starts to decay.  This prevents transient capacity dips from collapsing the
/// floor, while allowing a genuine long-term capacity drop to drain past a
/// stale windowed peak.
const DRAIN_FLOOR_GRACE_RTTS: f64 = 3.0;

/// Transport evidence consumed by one drain decision.
#[derive(Debug, Clone, Copy)]
pub(super) struct DrainInput {
    pub(super) delivery_rate: f64,
    pub(super) drain_fraction: f64,
    pub(super) peak_delivery: f64,
    pub(super) current_rate: f64,
    pub(super) minimum_rate: f64,
    pub(super) initial_rate: f64,
    pub(super) loss_event_rate: Option<f64>,
    pub(super) persistent_for: Option<Duration>,
    pub(super) control_rtt: Duration,
    pub(super) now: Instant,
}

/// Stale-peak protection floor hysteresis for the drain response.
#[derive(Debug, Default)]
pub(super) struct QueueResponse {
    floor_binding_since: Option<Instant>,
}

impl QueueResponse {
    pub(super) fn decide_drain(&mut self, input: DrainInput) -> CongestionDecision {
        // Clamp over the ordered range so a misconfigured minimum above the
        // initial rate cannot panic `f64::clamp` (which requires min <= max).
        let lo = input.minimum_rate.min(input.initial_rate);
        let hi = input.minimum_rate.max(input.initial_rate);
        let base = (input.peak_delivery * DRAIN_FLOOR_PEAK_FRACTION).clamp(lo, hi);
        let binding = input.delivery_rate * input.drain_fraction < base;
        if binding {
            self.floor_binding_since.get_or_insert(input.now);
        } else {
            self.floor_binding_since = None;
        }
        let binding_for = self
            .floor_binding_since
            .map(|start| input.now.saturating_duration_since(start));
        let pinned_for = binding_for
            .zip(input.persistent_for)
            .map(|(binding, closed)| binding.min(closed));
        let grace = input.control_rtt.mul_f64(DRAIN_FLOOR_GRACE_RTTS);
        let floor =
            if input.loss_event_rate.is_none() || pinned_for.is_none_or(|pinned| pinned <= grace) {
                base
            } else {
                let pinned = pinned_for.unwrap();
                // Guard the division: a zero control RTT (no measurement yet)
                // means no decay — the floor stays at `base`.
                let rtt = input.control_rtt.as_secs_f64();
                let excess_rtts = if rtt > 0.0 {
                    (pinned.as_secs_f64() - grace.as_secs_f64()) / rtt
                } else {
                    0.0
                };
                (base * 0.5f64.powf(excess_rtts)).max(input.minimum_rate)
            };
        let target = (input.delivery_rate * input.drain_fraction)
            .max(floor)
            .min(input.current_rate)
            .max(input.minimum_rate);
        CongestionDecision::Drain { floor, target }
    }

    pub(super) fn reset(&mut self) {
        self.floor_binding_since = None;
    }

    pub(super) fn floor_binding(&self) -> bool {
        self.floor_binding_since.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        delivery_rate: f64,
        persistent_for: Option<Duration>,
        loss_event_rate: Option<f64>,
        now: Instant,
    ) -> DrainInput {
        DrainInput {
            delivery_rate,
            drain_fraction: 0.9,
            peak_delivery: 1000.0,
            current_rate: 500.0,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            loss_event_rate,
            persistent_for,
            control_rtt: Duration::from_millis(100),
            now,
        }
    }

    #[test]
    fn binding_floor_decays_only_after_continuous_queue_and_grace() {
        let t0 = Instant::now();
        let mut qr = QueueResponse::default();
        let base = (1000.0 * DRAIN_FLOOR_PEAK_FRACTION).clamp(1.0, 128.0);
        assert_eq!(
            base, 128.0,
            "the test setup must bind at the initial-rate cap"
        );

        // With no loss sample the floor stays at base no matter how long the
        // queue persists: blind-flow decay needs the loss sample present.  The
        // binding latch opens at t0 here.
        let decision = qr.decide_drain(input(10.0, Some(Duration::from_secs(100)), None, t0));
        let CongestionDecision::Drain { floor, target } = decision else {
            panic!("drain expected");
        };
        assert_eq!(floor, base, "no loss sample must keep the base floor");
        assert!(target <= 500.0, "target must never exceed current_rate");
        assert!(target >= 1.0, "target must never fall below minimum_rate");

        // Binding with a loss sample while still inside the 3-control-RTT
        // grace window (pinned <= grace) keeps the base floor.
        let decision = qr.decide_drain(input(
            10.0,
            Some(Duration::from_millis(300)),
            Some(0.0),
            t0 + Duration::from_millis(300),
        ));
        let CongestionDecision::Drain { floor, target } = decision else {
            panic!("drain expected");
        };
        assert_eq!(
            floor, base,
            "binding within the grace window must not decay"
        );
        assert!(target <= 500.0);
        assert!(target >= 1.0);

        // Continuous binding past grace with a loss sample halves the floor
        // once per excess control RTT (0.5 ^ excess_rtts).
        let decision = qr.decide_drain(input(
            10.0,
            Some(Duration::from_millis(400)),
            Some(0.0),
            t0 + Duration::from_millis(400),
        ));
        let CongestionDecision::Drain { floor, target } = decision else {
            panic!("drain expected");
        };
        let expected = (base * 0.5f64.powf(1.0)).max(1.0);
        assert!(
            (floor - expected).abs() <= f64::EPSILON * base,
            "one excess RTT must halve the floor once: got {floor}, expected {expected}"
        );
        assert!(target <= 500.0);
        assert!(target >= 1.0);

        let decision = qr.decide_drain(input(
            10.0,
            Some(Duration::from_millis(500)),
            Some(0.0),
            t0 + Duration::from_millis(500),
        ));
        let CongestionDecision::Drain { floor, target } = decision else {
            panic!("drain expected");
        };
        let expected = (base * 0.5f64.powf(2.0)).max(1.0);
        assert!(
            (floor - expected).abs() <= f64::EPSILON * base,
            "two excess RTTs must halve the floor twice: got {floor}, expected {expected}"
        );
        assert!(target <= 500.0);
        assert!(target >= 1.0);

        // An interrupt breaks the binding continuity: the decay clock restarts
        // and the floor returns to base for the whole grace window.
        qr.decide_drain(input(
            1000.0,
            Some(Duration::from_millis(600)),
            Some(0.0),
            t0 + Duration::from_millis(600),
        ));
        let decision = qr.decide_drain(input(
            10.0,
            Some(Duration::from_millis(700)),
            Some(0.0),
            t0 + Duration::from_millis(700),
        ));
        let CongestionDecision::Drain { floor, .. } = decision else {
            panic!("drain expected");
        };
        assert_eq!(
            floor, base,
            "a binding gap must reset the decay clock back to the grace window"
        );
    }
}
