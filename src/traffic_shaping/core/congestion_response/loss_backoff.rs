//! Adaptive loss floor for the loss-backoff response.
//!
//! The floor tracks a small fraction of the recent delivery peak (capped by
//! the current rate) so a loss response never turns into a rate increase, and
//! the raw target follows the delivery rate with a minimum-rate floor.
use std::time::Duration;

use super::CongestionDecision;

/// Fraction of the recent peak delivery rate used as the loss-backoff floor.
const LOSS_BACKOFF_PEAK_FRACTION: f64 = 0.05;

pub(super) fn decide(
    current: f64,
    delivery_rate: f64,
    peak_delivery: f64,
    minimum_rate: f64,
    initial_rate: f64,
) -> CongestionDecision {
    let floor = (peak_delivery * LOSS_BACKOFF_PEAK_FRACTION)
        .clamp(minimum_rate, initial_rate)
        .min(current);
    let raw = delivery_rate.min(current).max(minimum_rate);
    CongestionDecision::LossBackoff {
        raw,
        floor,
        target: raw.max(floor),
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

    #[test]
    fn loss_floor_tracks_capacity_without_raising_current_rate() {
        // floor = (peak * 0.05).clamp(minimum_rate, initial_rate).min(current):
        // peak 10000 -> 500, clamped to 128, then capped at current 100.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            decide(100.0, 200.0, 10_000.0, 1.0, 128.0)
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
        // holds at the initial-rate cap.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            decide(200.0, 50.0, 10_000.0, 1.0, 128.0)
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
            decide(40.0, 10.0, 10_000.0, 1.0, 128.0)
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 40.0, "min(128, current 40)");
        assert_eq!(raw, 10.0);
        assert_eq!(target, 40.0);
        assert!(target <= 40.0);

        // The minimum-rate floor holds both the raw target and the floor.
        let CongestionDecision::LossBackoff { raw, floor, target } =
            decide(50.0, 0.1, 10.0, 1.0, 128.0)
        else {
            panic!("loss backoff expected");
        };
        assert_eq!(floor, 1.0, "clamp(0.5, 1, 128) = 1, min(current) = 1");
        assert_eq!(raw, 1.0, "0.1.min(50).max(1) = 1");
        assert_eq!(target, 1.0);
        assert!(target <= 50.0);
    }
}
