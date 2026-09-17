//! Reorder-tolerant lane probe-target cap.

use super::ORDINARY_PROBE_MAX_GAIN;

/// Bound one probe target.  On the reorder-tolerant interactive lane a
/// reorder-inflated delivery-rate sample may not raise the rate by more than
/// the ordinary probe's own maximum per-probe gain
/// ([`ORDINARY_PROBE_MAX_GAIN`], derived from the probe's gain), so a spurious
/// sample can only step the rate; the stock/bulk lane returns the target
/// unchanged.  Deriving the cap from the gain keeps it from silently falling
/// below (and clipping) the legitimate probe it exists to allow.
pub(crate) fn cap_probe_target(reorder_tolerant: bool, current: f64, target: f64) -> f64 {
    if reorder_tolerant {
        target.min(current * ORDINARY_PROBE_MAX_GAIN)
    } else {
        target
    }
}
