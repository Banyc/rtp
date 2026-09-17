//! Peak-scaled gentle probe base.
//!
//! The instantaneous delivery sample is depressed by the controller's own
//! drain, so probing from it ramps the lane back to line rate one feedback
//! sample at a time and leaves the pipe idle for most of the recovery lag.
//! The recent delivery peak still remembers the established capacity, so the
//! probe scales from the peak and refills the pipe within a sample.  This is
//! queue-depth-neutral (the peak and average standing-queue depth are
//! unchanged), so it applies to every lane.

/// Delivery rate the gentle probe scales toward its next target.
pub(crate) fn peak_scaled_probe_base(delivery_rate: f64, peak_delivery: f64) -> f64 {
    delivery_rate.max(peak_delivery)
}
