mod bandwidth_probe;
mod congestion_response;
mod fast_start;
mod gentle;
mod pacing;
mod queue_growth;
mod rate_window;

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

pub(crate) use bandwidth_probe::OrdinaryBandwidthProbe;
pub(crate) use congestion_response::{
    CongestionDecision, CongestionInput, CongestionResponse, ProbeKind, linear_backoff_step,
};
pub(crate) use fast_start::{FastStart, FastStartStep};
pub(crate) use gentle::GentleExitCause;
pub(crate) use pacing::{SendPacer, SendWake};
pub(crate) use queue_growth::QueueGrowth;
pub(crate) use rate_window::WindowedDeliveryMax;

#[cfg(test)]
pub(crate) use congestion_response::DRAIN_FLOOR_PEAK_FRACTION;
#[cfg(test)]
pub(crate) use gentle::{
    GENTLE_DRAIN_GAP_SHRINK, GENTLE_ENTER_RTTS, GENTLE_REENTRY_COOLDOWN,
    GENTLE_REENTRY_COOLDOWN_RTTS,
};
#[cfg(test)]
pub(crate) use queue_growth::{
    PERSISTENT_QUEUE_RTTVAR_FACTOR, QUEUE_RTT_FACTOR, QUEUE_RTT_FLOOR, QUEUE_TOL_RTT_FRACTION,
    RTT_MIN_BUCKET, RTT_MIN_BUCKET_RTT_SCALE, WindowedRttMin,
};
