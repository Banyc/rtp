//! Adaptive loss floor for the loss-backoff response.
//!
//! The floor tracks a small fraction of the recent delivery peak (capped by
//! the current rate and the ordered minimum/initial-rate clamp) so a loss
//! response never turns into a rate increase, and the raw target follows the
//! delivery rate with a minimum-rate floor.  When the floor BINDS for more
//! than a grace period — the flow's delivery keeps sitting below the stale
//! pre-loss peak-derived floor — the floor decays toward the actual delivery
//! (halving per excess control RTT, floored at the minimum rate) so a flow
//! under sustained loss converges to the link's real capacity instead of
//! pumping at the stale peak forever.
use std::time::{Duration, Instant};

use super::CongestionDecision;

/// Fraction of the recent peak delivery rate used as the loss-backoff floor.
const LOSS_BACKOFF_PEAK_FRACTION: f64 = 0.05;

/// Grace period, in control RTTs, before a continuously binding loss floor
/// starts to decay.  This prevents a one-shot delivery dip from collapsing the
/// floor while still converging a chronically loss-suppressed flow.
const LOSS_BACKOFF_FLOOR_GRACE_RTTS: f64 = 1.0;

/// Transport evidence consumed by one loss-backoff decision.
#[derive(Debug, Clone, Copy)]
pub(super) struct LossBackoffInput {
    pub(super) current_rate: f64,
    pub(super) delivery_rate: f64,
    pub(super) peak_delivery: f64,
    pub(super) minimum_rate: f64,
    pub(super) initial_rate: f64,
    pub(super) control_rtt: Duration,
    pub(super) now: Instant,
}

/// Adaptive loss floor with stale-peak binding hysteresis.
#[derive(Debug, Default)]
pub(super) struct LossBackoff {
    floor_binding_since: Option<Instant>,
}

impl LossBackoff {
    pub(super) fn decide(&mut self, input: LossBackoffInput) -> CongestionDecision {
        let base_floor = (input.peak_delivery * LOSS_BACKOFF_PEAK_FRACTION)
            // Clamp over the ordered range so a misconfigured minimum above the
            // initial rate cannot panic `f64::clamp` (which requires min <= max).
            .clamp(
                input.minimum_rate.min(input.initial_rate),
                input.minimum_rate.max(input.initial_rate),
            )
            .min(input.current_rate);
        let raw = input
            .delivery_rate
            .min(input.current_rate)
            .max(input.minimum_rate);

        // Binding: the raw target is being held up by the stale peak floor, not
        // by what the link actually delivers.  Track the continuous binding
        // duration so a persistent bind can decay the floor.
        let binding = input.delivery_rate < base_floor;
        if binding {
            self.floor_binding_since.get_or_insert(input.now);
        } else {
            self.floor_binding_since = None;
        }
        let binding_for = self
            .floor_binding_since
            .map(|start| input.now.saturating_duration_since(start));
        let grace = input.control_rtt.mul_f64(LOSS_BACKOFF_FLOOR_GRACE_RTTS);
        let floor = match binding_for {
            Some(bound) if bound > grace => {
                // Guard the division: a zero control RTT (no measurement yet)
                // means no decay — the floor stays at the peak-derived value.
                let rtt = input.control_rtt.as_secs_f64();
                let excess_rtts = if rtt > 0.0 {
                    (bound.as_secs_f64() - grace.as_secs_f64()) / rtt
                } else {
                    0.0
                };
                (base_floor * 0.5f64.powf(excess_rtts)).max(input.minimum_rate)
            }
            _ => base_floor,
        };
        CongestionDecision::LossBackoff {
            raw,
            floor,
            target: raw.max(floor),
        }
    }

    pub(super) fn reset(&mut self) {
        self.floor_binding_since = None;
    }
}

/// Linear backoff of the send rate toward `target`.
///
/// Returns `None` if `current` is already at or below `target`. Otherwise steps
/// down by at most `gap = current - target`, where the natural step is
/// `current * interval / rtt` and a `probe_floor` of
/// `interval / (cwnd_send_rate_scale * rtt^2)` guarantees at least one packet's
/// worth of headway over the RTT window (`cwnd = send_rate * rtt *
/// CWND_SEND_RATE_SCALE`).
pub(crate) fn linear_backoff_step(
    current: f64,
    target: f64,
    interval: Duration,
    control_rtt: Duration,
    cwnd_send_rate_scale: usize,
) -> Option<f64> {
    let gap = current - target;
    if gap <= 0.0 {
        return None;
    }
    let rtt_secs = control_rtt.as_secs_f64();
    let interval_secs = interval.as_secs_f64();
    let probe_floor = interval_secs / (cwnd_send_rate_scale as f64 * rtt_secs * rtt_secs);
    let step = (current * interval_secs / rtt_secs)
        .max(probe_floor)
        .min(gap);
    Some((current - step).max(target))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        current_rate: f64,
        delivery_rate: f64,
        peak_delivery: f64,
        control_rtt: Duration,
        now: Instant,
    ) -> LossBackoffInput {
        LossBackoffInput {
            current_rate,
            delivery_rate,
            peak_delivery,
            minimum_rate: 1.0,
            initial_rate: 128.0,
            control_rtt,
            now,
        }
    }

    #[test]
    fn loss_floor_tracks_capacity_without_raising_current_rate() {
        let t0 = Instant::now();
        let mut lb = LossBackoff::default();
        let control_rtt = Duration::from_millis(100);

        // floor = (peak * 0.05).clamp(minimum_rate, initial_rate).min(current):
        // peak 10000 -> 500, clamped to 128, then capped at current 100.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            lb.decide(input(100.0, 200.0, 10_000.0, control_rtt, t0))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 100.0, "the floor must be capped by the current rate");
        assert_eq!(raw, 100.0, "raw = delivery.min(current).max(minimum_rate)");
        assert_eq!(target, 100.0, "target = raw.max(floor)");
        assert!(
            target <= 100.0,
            "the loss response must never raise the rate"
        );

        // Delivery below current pulls the raw target down while the floor
        // holds at the initial-rate cap.  This sample opens a binding latch
        // inside the grace window, so the floor does not decay yet.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            lb.decide(input(200.0, 50.0, 10_000.0, control_rtt, t0))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 128.0, "clamp(500, 1, 128) = 128, min(current) = 128");
        assert_eq!(raw, 50.0);
        assert_eq!(target, 128.0);
        assert!(target <= 200.0);

        // A current rate below the capacity floor caps the floor at current:
        // the target can never exceed the current rate.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            lb.decide(input(40.0, 10.0, 10_000.0, control_rtt, t0))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 40.0, "min(128, current 40)");
        assert_eq!(raw, 10.0);
        assert_eq!(target, 40.0);
        assert!(target <= 40.0);

        // The minimum-rate floor holds both the raw target and the floor.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            lb.decide(input(50.0, 0.1, 10.0, control_rtt, t0))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 1.0, "clamp(0.5, 1, 128) = 1, min(current) = 1");
        assert_eq!(raw, 1.0, "0.1.min(50).max(1) = 1");
        assert_eq!(target, 1.0);
        assert!(target <= 50.0);
    }

    #[test]
    fn sustained_binding_decays_the_floor_toward_delivery() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let mut lb = LossBackoff::default();

        // A flow whose pre-loss peak (10_000) saturates the floor at the 128
        // initial-rate cap, but which now delivers only ~50 pps: the floor is
        // binding.  Inside the grace window (1 control RTT) the floor holds.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            lb.decide(input(200.0, 50.0, 10_000.0, control_rtt, t0))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 128.0);
        assert_eq!(raw, 50.0);
        assert_eq!(target, 128.0);

        // Binding persists exactly one control RTT: grace is fully consumed,
        // no excess RTT yet, so the floor still holds.
        let CongestionDecision::LossBackoff { floor, .. } =
            lb.decide(input(200.0, 50.0, 10_000.0, control_rtt, t0 + control_rtt))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(
            floor, 128.0,
            "one control RTT of binding is still within grace"
        );

        // Two control RTTs of continuous binding: one excess RTT -> the floor
        // halves once, toward the real ~50 pps delivery.
        let CongestionDecision::LossBackoff { floor, target, .. } = lb.decide(input(
            200.0,
            50.0,
            10_000.0,
            control_rtt,
            t0 + control_rtt * 2,
        )) else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 64.0, "one excess control RTT must halve the floor");
        assert_eq!(
            target, 64.0,
            "the decaying floor still pins the target at 64"
        );
        assert!(
            target > 50.0,
            "the floor must keep binding until it passes delivery"
        );

        // Three control RTTs: two excess RTTs -> 32.  The target converges
        // toward the real delivery rate instead of pinning at the stale peak.
        let CongestionDecision::LossBackoff { floor, target, .. } = lb.decide(input(
            200.0,
            50.0,
            10_000.0,
            control_rtt,
            t0 + control_rtt * 3,
        )) else {
            panic!("loss backoff expected");
        };
        let expected = (128.0 * 0.5f64.powf(2.0)).max(1.0);
        assert!(
            (floor - expected).abs() <= f64::EPSILON * 128.0,
            "two excess control RTTs must halve twice: got {floor}, expected {expected}"
        );
        assert_eq!(
            target, 50.0,
            "the raw delivery now wins over the decayed floor"
        );

        // Delivery recovers above the (decayed) floor: binding breaks, the
        // latch clears and the floor returns to the full peak-derived value —
        // the floor stays a floor for the normal case.
        let CongestionDecision::LossBackoff { floor, .. } = lb.decide(input(
            200.0,
            300.0,
            10_000.0,
            control_rtt,
            t0 + control_rtt * 4,
        )) else {
            panic!("loss backoff expected");
        };
        assert_eq!(
            floor, 128.0,
            "a non-binding sample must restore the peak-derived floor"
        );

        // A fresh binding episode must re-earn the grace window before decaying.
        let CongestionDecision::LossBackoff { floor, .. } = lb.decide(input(
            200.0,
            40.0,
            10_000.0,
            control_rtt,
            t0 + control_rtt * 5,
        )) else {
            panic!("loss backoff expected");
        };
        assert_eq!(
            floor, 128.0,
            "the grace window restarts after a binding gap"
        );
    }

    #[test]
    fn binding_floor_decay_is_floored_at_minimum_rate() {
        let t0 = Instant::now();
        let control_rtt = Duration::from_millis(100);
        let mut lb = LossBackoff::default();

        // Delivery at the minimum rate: no matter how long the binding lasts,
        // the decayed floor can never drop below the minimum rate.  The first
        // call opens the binding latch; the second measures 10 control RTTs of
        // continuous binding.
        lb.decide(input(200.0, 2.0, 10_000.0, control_rtt, t0));
        let base_at = t0 + control_rtt * 10;
        let CongestionDecision::LossBackoff { floor, target, .. } =
            lb.decide(input(200.0, 2.0, 10_000.0, control_rtt, base_at))
        else {
            panic!("loss backoff expected");
        };
        let expected = (128.0 * 0.5f64.powf(9.0)).max(1.0);
        assert!(
            (floor - expected).abs() <= f64::EPSILON * 128.0,
            "the decay must floor at minimum_rate: got {floor}, expected {expected}"
        );
        assert_eq!(target, 2.0, "raw = max(delivery, minimum_rate) = 2");

        // A zero control RTT (no measurement yet) must not divide by zero:
        // the floor stays at the peak-derived value.
        let mut stale = LossBackoff::default();
        let CongestionDecision::LossBackoff { floor, .. } =
            stale.decide(input(200.0, 50.0, 10_000.0, Duration::ZERO, t0))
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 128.0, "zero control RTT means no decay");
    }
}
