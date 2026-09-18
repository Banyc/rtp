//! Reorder-tolerant lane probe-target cap.

use crate::traffic_shaping::core::ORDINARY_PROBE_MAX_GAIN;

/// Bound one probe target on the reorder-tolerant lane.  The cap exists
/// because a reorder-inflated delivery-rate sample could otherwise raise the
/// rate by more than the ordinary probe's own delivery-scaled maximum per-probe
/// gain ([`ORDINARY_PROBE_MAX_GAIN`], derived from the probe's gain).
///
/// It bounds only the *delivery-scaled* part of the target: `additive` is the
/// lane's absolute, RTT-scaled probe step (zero on a lane without one), which
/// is not derived from the delivery sample and so is added after the bound
/// rather than clipped by it.  The stock/bulk lane returns the target
/// unchanged.  Keeping the additive step outside the bound is what makes the
/// cap's maximum meaningful: `ORDINARY_PROBE_MAX_GAIN` is exactly the largest
/// delivery-scaled target the probe it bounds can propose.
pub(crate) fn cap_probe_target(
    reorder_tolerant: bool,
    current: f64,
    target: f64,
    additive: f64,
) -> f64 {
    if reorder_tolerant {
        target.min(current * ORDINARY_PROBE_MAX_GAIN + additive)
    } else {
        target
    }
}
