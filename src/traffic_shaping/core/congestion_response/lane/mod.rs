//! Congestion-lane intent and its per-lane controller tuning.
//!
//! The connection's owner declares a [`CongestionLane`] once.  Every
//! lane-specific decision the delay controller makes -- how deep to drain, how
//! hard to probe, whether an application-limited sample can be attributed to
//! competing traffic, and whether this lane owns the windowed fast start -- is
//! a method on the lane value.  No caller compares the lane against a variant,
//! so a changed knob or a new lane cannot leave a caller behind.

mod probe_base;
mod tuning;

/// Congestion-controller lane intent declared by the connection's owner.
///
/// The delay controller tunes its cross-traffic protection from this intent,
/// never from the delivery mode: the two are orthogonal (a frame-delivery lane
/// can be either shared or dedicated).  A [`Dedicated`](Self::Dedicated) lane
/// has no competing traffic over this connection's queue, so it may creep
/// toward capacity and drain shallower; a [`Shared`](Self::Shared) lane keeps
/// the conservative cross-traffic-protecting behaviour so it cannot push
/// interactive packets out of a shared bottleneck.
///
/// `rtp_mux` declares the intent from its lane class (bulk lanes are dedicated,
/// the interactive lane is shared).  Callers that do not declare an intent get
/// [`Shared`](Self::Shared).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CongestionLane {
    /// A link shared with competing traffic: keep the conservative,
    /// cross-traffic-protecting controller tuning.  The default.
    #[default]
    Shared,
    /// A dedicated pipe with no competing traffic over this connection's
    /// queue: the bulk-lane tuning.
    Dedicated,
}

pub(crate) use probe_base::peak_scaled_probe_base;

#[cfg(test)]
pub(crate) use tuning::{
    DEDICATED_GENTLE_BW_PROBE_GAIN, DRAIN_RATE_FRACTION, GENTLE_BW_PROBE_GAIN, GENTLE_DRAIN_FRAC,
    SHARED_ADDITIVE_PROBE_REFERENCE_RTT, SHARED_ADDITIVE_PROBE_STEP,
};
