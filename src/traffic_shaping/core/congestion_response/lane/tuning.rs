//! Per-lane delay-controller tuning.
//!
//! Every knob the delay controller reads from the declared lane lives here, so
//! the lane value is the one place a lane-specific policy is selected.  A
//! caller asks the lane for the value it needs instead of comparing the lane
//! against a variant.

use super::CongestionLane;
use std::time::Duration;

/// Reference path RTT the shared lane's additive step is calibrated at.
pub(crate) const SHARED_ADDITIVE_PROBE_REFERENCE_RTT: Duration = Duration::from_millis(100);

/// Gentle-mode multiplicative probe gain for the shared lane.  A shared lane
/// keeps the conservative cross-traffic-protecting gain.
pub(crate) const GENTLE_BW_PROBE_GAIN: f64 = 0.20;

/// Gentle-mode multiplicative probe gain for the dedicated bulk lane.  The
/// lane has no cross-traffic to protect, so it can afford to creep toward
/// capacity instead of probing `1.2x` every cycle: a smaller overshoot
/// stretches each probe phase, spending less of the window in the drain/
/// transition overhead that costs link time at every sawtooth turn while
/// leaving the standing queue's peak and average depth unchanged.
pub(crate) const DEDICATED_GENTLE_BW_PROBE_GAIN: f64 = 0.02;

/// Gentle-mode drain fraction for the shared lane.
pub(crate) const GENTLE_DRAIN_FRAC: f64 = 0.75;

/// The ordinary drain fraction every lane settles at when it is not in gentle
/// mode, and that a dedicated lane keeps even while gentle mode is active.
pub(crate) const DRAIN_RATE_FRACTION: f64 = 0.9;

/// The additive-increase step the shared lane adds to one accepted ordinary
/// probe, in `pkt/s`, at [`SHARED_ADDITIVE_PROBE_REFERENCE_RTT`].
///
/// The step is an absolute rate, so two flows contending for one bottleneck
/// converge to equal rates under the multiplicative drain instead of keeping
/// whatever ratio the delivery-scaled multiplicative probe first gave them.
/// It is deliberately independent of the flow's current rate (a
/// `current`-proportional step grows with the share it exists to redistribute).
///
/// It is scaled by the *square root* of the path's control RTT.  A step
/// proportional to `control_rtt` would be right only if every flow's probe
/// fired exactly once per RTT; under a shared queue the accepted-probe cadence
/// is instead set by the common queue drain/hold cycle, so a full linear
/// compensation over-rewards a high-RTT flow (measured: it wins ~2.4x).  A
/// constant step over-rewards a low-RTT flow (measured: it wins ~2.6x).  The
/// geometric scaling matches the measured cadence and converges the pair.  A
/// dedicated lane has no competing flow to converge against, so it keeps the
/// historical purely multiplicative probe.
pub(crate) const SHARED_ADDITIVE_PROBE_STEP: f64 = 150.0;

impl CongestionLane {
    /// The additive rate step this lane adds to one accepted ordinary probe.
    ///
    /// An absolute rate scaled by the *square root* of the path's control RTT:
    /// the shared queue gates the accepted-probe cadence more than each flow's
    /// own RTT does, so a full linear RTT compensation over-rewards the
    /// high-RTT flow.  A dedicated lane adds nothing.
    pub(crate) fn ordinary_additive_probe_step(self, control_rtt: Duration) -> f64 {
        match self {
            Self::Dedicated => 0.0,
            Self::Shared => {
                let ratio =
                    control_rtt.as_secs_f64() / SHARED_ADDITIVE_PROBE_REFERENCE_RTT.as_secs_f64();
                SHARED_ADDITIVE_PROBE_STEP * ratio.max(0.0).sqrt()
            }
        }
    }

    /// The multiplicative gain the gentle probe creeps at on this lane.
    pub(crate) fn gentle_probe_gain(self) -> f64 {
        match self {
            Self::Dedicated => DEDICATED_GENTLE_BW_PROBE_GAIN,
            Self::Shared => GENTLE_BW_PROBE_GAIN,
        }
    }

    /// The fraction of the delivery rate this lane drains toward while the
    /// delay gate is active.
    ///
    /// A dedicated lane has no cross-traffic to protect, so it drains at the
    /// ordinary fraction even while gentle mode is active; a shared lane uses
    /// gentle mode's deeper fraction while it is active.
    pub(crate) fn drain_fraction(self, gentle: bool) -> f64 {
        match self {
            Self::Dedicated => DRAIN_RATE_FRACTION,
            Self::Shared if gentle => GENTLE_DRAIN_FRAC,
            Self::Shared => DRAIN_RATE_FRACTION,
        }
    }

    /// Whether an application-limited sample on this lane can be attributed to
    /// a competing flow's standing queue rather than this lane's own.
    ///
    /// A shared lane cannot tell its own (absent) queue from another flow's, so
    /// a standing delay on an application-limited sample must not drain its
    /// send rate.  A dedicated lane has no competing traffic over its queue, so
    /// a standing delay there is its own and the ordinary drain applies.
    pub(crate) fn app_limited_means_cross_traffic(self, app_limited: bool) -> bool {
        app_limited && matches!(self, Self::Shared)
    }

    /// Whether this lane owns the windowed ACK-clock fast start.  Only the
    /// dedicated bulk lane does; the shared lane keeps the stock
    /// accumulator/exit.
    pub(crate) fn owns_fast_start(self) -> bool {
        matches!(self, Self::Dedicated)
    }
}
