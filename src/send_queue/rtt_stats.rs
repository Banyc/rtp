use std::time::Duration;

use super::rto::RtxTimer;

/// Bundle of RTT statistics: the smoothed-RTT / RTO timer and the lifetime
/// minimum RTT.
#[derive(Debug)]
pub(crate) struct RttStats {
    min_rtt: Option<Duration>,
    rto: RtxTimer,
}

impl RttStats {
    pub(crate) fn new() -> Self {
        Self {
            min_rtt: None,
            rto: RtxTimer::new(),
        }
    }

    /// Record an RTT sample: update the SRTT filter and the lifetime minimum.
    pub(crate) fn record_rtt(&mut self, rtt: Duration) {
        self.rto.set(rtt);
        self.min_rtt = Some(match self.min_rtt {
            Some(m) => m.min(rtt),
            None => rtt,
        });
    }

    /// Record the lifetime minimum and re-seed the SRTT filter from scratch.
    ///
    /// Used when an outage-recovery epoch closes: the fresh post-outage RTT
    /// reflects a new network state, so we must not merge it with the stale
    /// pre-outage EWMA via `record_rtt`.
    pub(crate) fn record_min_and_reseed_rto(&mut self, rtt: Duration) {
        self.min_rtt = Some(match self.min_rtt {
            Some(m) => m.min(rtt),
            None => rtt,
        });
        self.rto.reset_to(rtt);
    }

    pub(crate) fn min_rtt(&self) -> Option<Duration> {
        self.min_rtt
    }

    pub(crate) fn smooth_rtt(&self) -> Duration {
        self.rto.smooth_rtt()
    }

    pub(crate) fn smooth_rtt_var(&self) -> Duration {
        self.rto.smooth_rtt_var()
    }

    pub(crate) fn rto_duration(&self) -> Duration {
        self.rto.rto()
    }

    pub(crate) fn raw_rto(&self) -> Duration {
        self.rto.raw_rto()
    }

    pub(crate) fn reorder_window(&self) -> Duration {
        self.rto.reorder_window()
    }

    /// Whether the structural low-jitter gate for evidence-gated fast loss
    /// is armed (see [`RtxTimer::fast_loss_armed`]).
    pub(crate) fn fast_loss_armed(&self) -> bool {
        self.rto.fast_loss_armed()
    }

    pub(crate) fn reset_rto(&mut self, rtt: Duration) {
        self.rto.reset_to(rtt);
    }
}

impl Default for RttStats {
    fn default() -> Self {
        Self::new()
    }
}
